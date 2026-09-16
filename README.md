# Pulsar API - POC

A four-step spike to find out how the Pulsar API actually behaves before we
commit to building anything. Runs locally. **No Lambda, no Snowpipe** — none of that proves the data is useful, and all of it is
expensive to unpick if the answers here change the design.

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
cd pulsar-poc
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
python 01_introspect.py
```
Dumps every endpoint's real schema to `schema/*.txt`. **Read these before
running anything else** — the queries in steps 02 and 03 are best guesses
from thin documentation and are the most likely thing to need correcting.

```bash
python 02_discover.py
```
Lists searches with IDs, statuses, and mention counts. Pick the one for this
client. Note its `totalContents` — that number sizes the backfill.

```bash
python 03_extract.py --search-id <ID> --from 2026-07-31 --dry-run
```
**Always run this first.** One API call, writes nothing. Prints the vendor's
total mention count for the window and an estimated page count for the full
pull — know the size before you spend any more calls.

```bash
python 03_extract.py --search-id <ID>
```
Default window is 3 days, capped at 5 pages — a small real pull to confirm
pagination and reconciliation work before going further. Lands raw responses
under `out/raw/`, flattens to `out/mentions.jsonl`, prints measured
throughput and reconciles the loaded row count against the vendor's `total`.

```bash
python 03_extract.py --search-id <ID> --from 2026-07-31 --max-pages 43
```
Full backfill, once the dry-run has told you how many pages to expect. Set
`--max-pages` from that estimate rather than leaving it open-ended.

```bash
python 04_profile.py
```
Field coverage per source, daily volume table and chart, data quality flags.

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
pulsar/client.py    rate-limited GraphQL client, retries, error handling
01_introspect.py    schema dump      -> schema/
02_discover.py      search inventory -> out/searches.json
03_extract.py       mention pull     -> out/raw/, out/mentions.jsonl
04_profile.py       profiling        -> out/profile.txt, out/daily_volume.png
```
