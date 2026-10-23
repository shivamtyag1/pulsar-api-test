# vx-pulsar-social-ingest

Daily extract of Pulsar social-listening data into S3, attributed to the
client each search was built for, for loading into the VX agency Snowflake
schemas.

**Scope boundary:** this repo ends at writing objects to S3. Moving them into
Snowflake runs through VaynerX's config-driven Streaming ETL framework and is
owned separately.

Architecture study: `pulsar-api-study.html`, kept one directory up and
**deliberately outside this repo** — it cites real client names and search
IDs, which do not belong in a git history.

## Status

The extraction library is built and tested; the AWS deployment is not. What
works today is the operator tooling in `tools/` — enough to validate the API
and to land a single client's data in the S3 layout by hand.

| | |
|---|---|
| API fidelity | Reconciles exactly with the Pulsar UI — 42/42 and 2,940/2,940 on two searches |
| Throughput | 0.80 req/s measured, bound by the bearer token, not by compute |
| Library | `src/pulsar/` — client, pagination guard, field generation, attribution |
| Tests | 110, 99.6% coverage, all against a mock transport |

## What this is trying to find out

| Question | Answered by | Why it matters |
|---|---|---|
| Is the documented schema the real schema? | `01` | Docs are thin. Introspection is authoritative |
| Which searches exist, and how big? | `02` | Sizes the backfill; gives the reconciliation baseline |
| Does pagination and rate limiting behave as documented? | `03` | 5 req/s and 100/page are claims until measured |
| Do row counts match what the vendor reports? | `03` | Catches silent drops before they reach a dashboard |
| Which fields are actually populated, per platform? | `04` | Drives the NULL-vs-zero handling in the warehouse |
| Does the data answer the client's question? | `04` | The only one that decides whether we build at all |

## Setup

```bash
cd vx-pulsar-social-ingest
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Token comes from Keeper. Set it in your shell — never in a file here:

```bash
export PULSAR_TOKEN='paste-from-keeper'
```

If the token stops working, renewal goes through the Pulsar account manager.
It is not self-service.

## Run order

```bash
python tools/introspect.py
```
Dumps every endpoint's real schema to `schema/*.txt`. **Read these before
running anything else** — the queries in steps 02 and 03 are best guesses
from thin documentation and are the most likely thing to need correcting.

```bash
python tools/discover.py
```
Lists searches with IDs, statuses, and mention counts. Pick the one for this
client. Note its `totalContents` — that number sizes the backfill.

```bash
python tools/extract.py --search-id <ID> --from 2026-07-31 --dry-run
```
**Always run this first.** One API call, writes nothing. Prints the vendor's
total mention count for the window and an estimated page count for the full
pull — know the size before you spend any more calls.

```bash
python tools/extract.py --search-id <ID>
```
Default window is 3 days, capped at 5 pages — a small real pull to confirm
pagination and reconciliation work before going further. Lands raw responses
under `out/raw/`, flattens to `out/mentions.jsonl`, prints measured
throughput and reconciles the loaded row count against the vendor's `total`.

```bash
python tools/extract.py --search-id <ID> --from 2026-07-31 --max-pages 43
```
Full backfill, once the dry-run has told you how many pages to expect. Set
`--max-pages` from that estimate rather than leaving it open-ended.

```bash
python tools/profile.py
```
Field coverage per source, daily volume table and chart, data quality flags.

## Pull a brand in the Lambda's format (manual S3 dump)

`tools/pull.py` produces exactly what the deployed Lambda writes: same code,
same folder layout, same columns. So files pulled here and uploaded by hand
load through the same Streaming ETL and land in the same Snowflake tables.

```bash
export PULSAR_TOKEN='...'                      # from Keeper. Never commit it.
python tools/pull.py --list-folders Lovesac    # find a folder's exact name
python tools/pull.py --client perdue --dry-run # what would be pulled, calls, minutes
python tools/pull.py --client perdue --all-history --local out/s3   # write to disk first
```

Brands and agencies come from `config/client_map.json` (agencies are
placeholders until confirmed). `--all-history` starts each search at its
oldest mention; without it the window is the last 90 days.

Output under `out/s3/pulsar/`:

| Path | Holds |
|---|---|
| `inventory/agency=<A>/client=<c>/snapshot=<day>/` | the folder and every search, with status and dates |
| `mentions/agency=<A>/client=<c>/search=<id>/snapshot=<day>/` | one row per post (author personal data excluded) |
| `aggregates/agency=<A>/client=<c>/snapshot=<day>/` | 30 breakdowns, 10 headline stats, 15 daily series, audience reports |
| `state/runs/run=<run>/manifest.json` | the run record, written last |

Check the manifest says `"ok": true` and `"reconciled": true`, then upload
the whole `pulsar/` folder yourself, keeping the paths:

```bash
aws s3 cp out/s3/pulsar s3://vx-analytics/pulsar --recursive
```

`export_client.py` and `export_facets.py` still work; they run `pull.py`
with `--only mentions` / `--only facets,timeseries,audience`.

## Phase 0 — production prerequisites

The spike is done and the production design is approved (see the plan file).
Steps 05 and 06 are the first pieces of that build. They still run locally,
because both are human-in-the-loop steps that must happen before any AWS
infrastructure is worth standing up.

```bash
python tools/validate.py
```
**4 API calls.** Proves the two assumptions the production design rests on:

1. **`updatedFrom` semantics** — the incremental contract assumes it filters
   on the same clock as the returned `updatedAt`. If it does not, daily runs
   fall back to expensive date-window re-reads.
2. **Is `pulsarId` unique across searches?** Decides whether the primary key
   is `pulsarId` or `(search, pulsarId)`.

It also exercises the full 123-field selection for the first time — the POC
only ever requested 10 of them.

```bash
python tools/seed_mapping.py
```
**3-4 API calls.** Produces `out/search_client_map.seed.csv`, the first draft
of the search → client mapping table.

Search names cannot be the categorisation key: of 88 searches only 3 contain
underscores (none as separators), 9 are null, and several are placeholders.
So VaynerX owns a mapping table instead, and this seeds it — pulling every
grouping signal the API exposes (team, folders, creator, volume, status) as
evidence columns, pre-filling a conservative client guess, and flagging names
that look like tests.

**This CSV needs a human to finish it.** Rows left blank stay in quarantine
and are never extracted, which is also how the nomenclature ask gets enforced
going forward: a new search is invisible to reporting until someone claims it.
`backfill_from` is the column that matters most for cost — 3.7M of the
account's 15.1M mentions currently sit in searches whose names look like
tests, and nobody should spend a day of API quota backfilling those.

## What to record while running

These are the numbers that turn the study's estimates from derived into
measured:

- **Actual sustained request rate** before throttling, and whether a 429
  carries a `Retry-After` header
- **Backfill size** — total mentions from 31 July
- **Field coverage gaps** — which metrics are absent per platform
- **Whether the reconciliation check matches**, and if not, by how much

## The one thing this POC cannot answer

Whether API reads draw against the **1M mentions/search/month fair use cap**.
Nothing in the API tells us, and finding out empirically means risking the
client's allowance. That question goes to Pulsar (Ben) before any sustained
polling, because a shared pool would rule out the daily metric-refresh design
entirely.

## Deliberately not here

Snowflake loading, orchestration, secrets management, IaC, incremental
watermarks, dbt models. All of that is P1/P2 work and all of it depends on
what these four scripts find out. Adding it now would be building on
assumptions this spike exists to test.

## Layout

```
src/pulsar/          the library - the only code that will be deployed
  client.py          rate-limited GraphQL client; the error taxonomy
  paging.py          pagination termination + reconciliation
  fields.py          field list generated from the introspected schema
  mapping.py         search -> client attribution heuristics

tools/               operator scripts. Run from the repo root. These spend
                     API quota and are never packaged into a deployment.
  introspect.py      schema dump          -> schema/
  discover.py        search inventory     -> out/searches.json
  extract.py         mention pull         -> out/raw/, out/mentions.jsonl
  profile.py         field profiling      -> out/profile.txt
  validate.py        phase 0 checks       -> out/validate_*.json
  seed_mapping.py    attribution seed     -> out/search_client_map.seed.csv
  export_client.py   one client -> the S3 layout, for the viz team

tests/               mock transport only. CI must never touch the live API.
schema/              gitignored. Large, regenerable vendor artifact.
out/                 gitignored. CLIENT DATA - never commit, never share.
```

`src/` layout is deliberate: `import pulsar` resolves to the installed
package, not to whatever happens to be in the working directory, so a test
passing locally means the packaged code works.

## Data protection

`out/` holds client-confidential mentions subject to client Walls. Three
independent mechanisms refuse to let it leave the machine — `.gitignore`, a
pre-commit hook, and a CI job that fails if any is tracked. One is not
enough, because the failure is unrecoverable: you cannot un-publish a repo.

The bearer token lives only in your shell, sourced from Keeper. It is never
committed, never logged, and never written to a file in this repo.
