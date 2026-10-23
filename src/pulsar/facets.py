"""Aggregate breakdowns and headline metrics for one search.

The mention export answers "what was said". This answers "by whom, where,
in what language, how much" - and it does so in a handful of calls no
matter how large the search is, because these are aggregates computed by
the vendor rather than rows we page through.

That asymmetry is the whole point. A search with 3,000 mentions and one
with 3,000,000 both cost ~5 calls here, where the mention pull costs 30
and 30,000. For a dashboard that needs a gender split and a country
breakdown, this is the cheap path and the mention rows are the expensive
one.

The catalogue is DERIVED FROM THE SCHEMA, not hand-listed: a query
qualifies as a facet if it takes a `filter`, requires nothing else, and
returns a type carrying both `label` and `value`. That rule picks up the
33 real breakdowns and correctly rejects `results`, `topContent` and
`resultsByOutletName` (which return mentions, not facets), `userData`
(a paginated author roster), and `sidebarSummary` / `validateFilter`
(neither is data). Re-running `make introspect` is the whole upgrade path.

AUTHOR PII
----------
Three of the qualifying queries are personal data despite being shaped
like aggregates, and they are excluded by default:

    userNames, userRealNames   the bucket LABEL is a person's name, so
                               grouping does not anonymise - it produces a
                               roster with a post count beside each person.
                               `include_pii=True` admits these two.
    userData                   never emitted, with or without the flag. It
                               is not an aggregate at all: a paginated list
                               of userIdentifier + userScreenName, two of
                               the twelve fields the mention export strips.
                               It fails the label/value rule, and pulling it
                               would need pagination this module does not do.

Aggregation is only a privacy control when the grouping key is a category.
`genders -> {female: 265}` is a category. `userNames -> {<a name>: 4}` is a
person. Pulling "every query that only needs a filter" would defeat the
mention-level PII policy through a side door, which is why the gate lives
here rather than in the caller.
"""

from __future__ import annotations

import dataclasses
import json
import pathlib
from typing import TYPE_CHECKING, Any

from pulsar.client import AuthError, PulsarError, short_error

if TYPE_CHECKING:  # pragma: no cover - typing only
    from collections.abc import Callable, Iterable, Sequence

    from pulsar.export import Executor

SCHEMA = pathlib.Path(__file__).resolve().parents[2] / "schema" / "trac_data.json"

# See the module docstring. These carry author identity and are gated with
# the same `include_pii` switch as the mention-level fields.
PII_FACETS = frozenset({"userNames", "userRealNames", "userData"})

# Pulled by pulsar.timeseries instead, with an explicit granularity and a
# chunked window. Left here, `timeIntervals` has no granularity option to
# set, and MEASURED on a 177-day window the vendor silently answered WEEKLY
# (26 points) - mixing weekly buckets into what every consumer reads as a
# daily series.
TIMESERIES_OWNED = frozenset({"timeIntervals"})

# One level of nesting, where the nested breakdown is the point rather than
# a convenience. Pulsar only exposes author age *within* gender - there is
# no top-level `ages` query - so without this the age split is unreachable.
NESTED = {"genders": "userAge"}

# Fields on the nested breakdown type (NestedLabelAsString). `overIndex` is
# the index against baseline, which is what makes a demographic bucket
# interpretable rather than just a count.
NESTED_FIELDS = ("label", "value", "sampleSize", "overIndex")

# How a headline metric is aggregated. SUM is right for volume-like values
# (reach, impressions, circulation) and meaningless for scores, so the two
# score-like metrics average instead. The choice is recorded in every row's
# `metric` column, so a consumer never has to guess which was applied.
STAT_METRIC = {
    "credibilityScore": "AVG",
    "reputation": "AVG",
    "users": "UNIQUE",  # UniqueEnum, not MetricEnum - distinct authors
}
DEFAULT_STAT_METRIC = "SUM"

# Facets per GraphQL document.
#
# ONE, deliberately. Batching looked like the obvious win - 31 facets in 4
# requests instead of 31 - but the arithmetic does not support it. At the
# measured 0.80 req/s the whole catalogue costs ~40 seconds unbatched,
# against ~11 batched. Thirty seconds is not worth anything here, and
# batching bought two real failure modes for it:
#
#   * 8 facets in one document returns HTTP 500 outright
#   * 4 worked in one dry run and 500'd in a full pull, because the cost
#     depends on which facets share the document and how many buckets each
#     returns - so the safe batch size is not knowable in advance
#
# One per request makes every failure local to the facet that caused it, no
# bisecting required, and the run degrades one breakdown at a time instead
# of four. Raise `batch_size` only with evidence.
DEFAULT_BATCH = 1

# MEASURED, and counter-intuitive enough to be worth stating plainly: an
# EXPLICIT limit is rejected above 100 ("Maximum value allowed for limit is
# 100"), but omitting it altogether returns 250. The server's own default is
# therefore larger than the largest value a caller is allowed to ask for.
#
# The consequence is that passing `limit` can only ever return LESS than not
# passing it, so this module does not set one by default. It exists solely
# so a caller can deliberately ask for fewer.
MAX_LIMIT = 100
IMPLICIT_LIMIT = 250

# MEASURED: the server suppresses small buckets unless told not to. On one
# real search `cities` returned ONE bucket by default and 139 with
# mincount=1; `bioEmojis` went from 1 to 187. Left alone, a city breakdown
# would have shipped showing New York as the only city in the corpus.
#
# So the default here is 1 - land the whole distribution, because a
# warehouse can always aggregate up and can never recover what was never
# written. Note that this turns OFF what was effectively small-cell
# suppression: a bucket of 1 narrows to a single author. That is a
# presentation-layer concern (suppress small cells in anything client-facing
# or published) and it is consistent with the mention rows we already land,
# which carry far more identifying detail than a city with a count of one.
DEFAULT_MINCOUNT = 1

# A facet row is MEASURED at 274 bytes against a mention row's 4,241 - 15x
# smaller, because it is a label and a number rather than 111 fields and a
# transcript. The mention sink's 10,000-row roll would therefore split a
# client's breakdowns across files for no reason, so facets roll far later:
# 100,000 rows is ~27 MB, which keeps every realistic client in ONE object
# while staying well inside the function's memory.
FACET_ROWS_PER_FILE = 100_000

JsonDict = dict[str, Any]


@dataclasses.dataclass(frozen=True)
class Facet:
    """A `{label, value}` breakdown.

    `accepts` is which of `limit` / `mincount` this facet's options input
    actually takes. It is NOT uniform: the 31 facets are spread across five
    different options types, and sending `mincount` to one that does not
    declare it is a hard GraphQL error, not an ignored argument. Three
    types accept it; `TimeIntervalsOptionsInput` (timeIntervals, velocity)
    accepts neither option.
    """

    name: str
    fields: tuple[str, ...]
    nested: str | None = None
    accepts: frozenset[str] = frozenset()


@dataclasses.dataclass(frozen=True)
class Stat:
    """A single headline number.

    Stats take `ForceOptionsInput`, which accepts neither limit nor
    mincount, so they are always requested bare.
    """

    name: str
    metric: str


def _load(schema_path: pathlib.Path | None = None) -> list[JsonDict]:
    raw = json.loads((schema_path or SCHEMA).read_text())
    schema = raw["data"]["__schema"] if "data" in raw else raw["__schema"]
    types: list[JsonDict] = schema["types"]
    return types


def _named(type_ref: JsonDict) -> str | None:
    while type_ref.get("ofType"):
        type_ref = type_ref["ofType"]
    return type_ref.get("name")


def _scalars(by_name: dict[str, JsonDict], type_name: str | None) -> set[str]:
    node = by_name.get(type_name or "") or {}
    out = set()
    for field in node.get("fields") or []:
        ref = field["type"]
        while ref.get("ofType"):
            ref = ref["ofType"]
        if ref["kind"] == "SCALAR":
            out.add(field["name"])
    return out


def _accepted_options(by_name: dict[str, JsonDict], field: JsonDict) -> frozenset[str]:
    """Which of limit/mincount this query's `options` input declares.

    Read per field rather than assumed: `mincount` on a type that does not
    declare it fails the query outright with `argumentNotAccepted`.
    """
    arg = next((a for a in field["args"] if a["name"] == "options"), None)
    if arg is None:
        return frozenset()
    node = by_name.get(_named(arg["type"]) or "") or {}
    declared = {i["name"] for i in node.get("inputFields") or []}
    return frozenset(declared & {"limit", "mincount"})


def catalogue(
    *, include_pii: bool = False, schema_path: pathlib.Path | None = None
) -> tuple[list[Facet], list[Stat]]:
    """The facets and stats worth pulling, read out of the vendor schema."""
    types = _load(schema_path)
    by_name = {t["name"]: t for t in types}
    query = by_name["Query"]

    facets: list[Facet] = []
    stats: list[Stat] = []

    for field in query["fields"]:
        name = field["name"]
        args = {a["name"] for a in field["args"]}
        if "filter" not in args:
            continue
        if not include_pii and name in PII_FACETS:
            continue
        if name in TIMESERIES_OWNED:
            continue

        returns = _named(field["type"])
        required = {
            a["name"]
            for a in field["args"]
            if a["type"]["kind"] == "NON_NULL" and a["name"] != "filter"
        }

        if returns == "StatReturn" and required <= {"metric"}:
            stats.append(Stat(name, STAT_METRIC.get(name, DEFAULT_STAT_METRIC)))
            continue

        if required:
            # Needs a segment, dimension or graph key - a choice this module
            # has no basis for making. Skipped deliberately.
            continue

        scalars = _scalars(by_name, returns)
        if not {"label", "value"} <= scalars:
            continue

        selected = ["label", "value"]
        selected += [f for f in ("sampleSize", "overIndex") if f in scalars]
        facets.append(
            Facet(
                name,
                tuple(selected),
                NESTED.get(name),
                _accepted_options(by_name, field),
            )
        )

    return facets, stats


def build_facet_query(
    facets: Sequence[Facet],
    *,
    limit: int | None = None,
    mincount: int | None = None,
) -> str:
    """One document covering several facets - they share the filter.

    The schema declares NO default for `limit` or `mincount`, but the server
    applies its own and they are worth understanding before trusting a
    breakdown:

      limit     omit it. An explicit value is capped at 100, while omitting
                it returns 250 - see MAX_LIMIT.
      mincount  the server suppresses small buckets, so `cities` returns one
                bucket for a corpus where ~8% of mentions carry a city.
                `fetch` defaults it to 1 - see DEFAULT_MINCOUNT.

    A pull that sets neither cannot distinguish an absent bucket from a
    suppressed one.
    """
    if limit is not None and limit > MAX_LIMIT:
        raise ValueError(
            f"limit must be <= {MAX_LIMIT} (the vendor rejects more). Omitting "
            f"it entirely returns {IMPLICIT_LIMIT}, which is more than any "
            "explicit limit can ask for."
        )
    lines = []
    for facet in facets:
        # Per facet, not per query: they do not share an options type.
        opts = []
        if limit and "limit" in facet.accepts:
            opts.append(f"limit: {limit}")
        if mincount is not None and "mincount" in facet.accepts:
            opts.append(f"mincount: {mincount}")
        options = f", options: {{{', '.join(opts)}}}" if opts else ""

        selection = " ".join(facet.fields)
        if facet.nested:
            selection += f" {facet.nested} {{ {' '.join(NESTED_FIELDS)} }}"
        lines.append(f"  {facet.name}(filter: $filter{options}) {{ {selection} }}")
    return "query Facets($filter: FilterInput!) {\n" + "\n".join(lines) + "\n}"


def build_stat_query(stats: Sequence[Stat]) -> str:
    lines = [
        f"  {s.name}(filter: $filter, metric: {s.metric}) {{ metric value }}"
        for s in stats
    ]
    return "query Stats($filter: FilterInput!) {\n" + "\n".join(lines) + "\n}"


def _normalise_label(label: Any) -> tuple[str | None, str | None]:
    """Flatten a structured bucket label into (label, label_type).

    Most facets label a bucket with a string. `entityNames` labels it with
    an object - {"type": "ORGANIZATION", "name": "lovesac"} - which would
    otherwise land in the column as a stringified Python dict, single
    quotes and all, so not even parseable back as JSON.
    """
    if isinstance(label, dict):
        name = label.get("name")
        if name is not None:
            return str(name), (
                str(label["type"]) if label.get("type") is not None else None
            )
        return json.dumps(label, sort_keys=True), None
    return (None if label is None else str(label)), None


def _row(
    facet: str,
    label: Any,
    value: float | None,
    *,
    metric: str | None = None,
    sample_size: int | None = None,
    over_index: float | None = None,
    parent_facet: str | None = None,
    parent_label: str | None = None,
) -> JsonDict:
    """One uniform shape for every bucket and every metric.

    Deliberately flat: a loader lands these as columns and a dashboard
    groups on `facet` without unpacking nested JSON.
    """
    text, label_type = _normalise_label(label)
    return {
        "facet": facet,
        "label": text,
        "label_type": label_type,
        "value": value,
        "metric": metric,
        "sample_size": sample_size,
        "over_index": over_index,
        "parent_facet": parent_facet,
        "parent_label": parent_label,
    }


def flatten(payload: JsonDict, facets: Iterable[Facet]) -> list[JsonDict]:
    """Turn a facet or stat response into uniform rows."""
    nested_of = {f.name: f.nested for f in facets}
    rows: list[JsonDict] = []

    for name, block in payload.items():
        if block is None:
            continue

        # A StatReturn: one number, no label.
        if isinstance(block, dict):
            rows.append(
                _row(name, None, block.get("value"), metric=block.get("metric"))
            )
            continue

        for bucket in block:
            if not isinstance(bucket, dict):
                continue
            label = bucket.get("label")
            rows.append(
                _row(
                    name,
                    label,
                    bucket.get("value"),
                    sample_size=bucket.get("sampleSize"),
                    over_index=bucket.get("overIndex"),
                )
            )
            child = nested_of.get(name)
            if not child:
                continue
            for sub in bucket.get(child) or []:
                rows.append(
                    _row(
                        child,
                        sub.get("label"),
                        sub.get("value"),
                        sample_size=sub.get("sampleSize"),
                        over_index=sub.get("overIndex"),
                        parent_facet=name,
                        parent_label=label,
                    )
                )
    return rows


def _fetch_batch(
    client: Executor,
    batch: Sequence[Facet],
    variables: JsonDict,
    *,
    limit: int | None,
    mincount: int | None,
    say: Callable[[str], None],
    failed: list[str],
) -> list[JsonDict]:
    """One batch, halving on failure to isolate the facet that broke.

    Unlike the mention pull, a partial result here is safe *and* useful:
    the facets are independent, so losing `domains` costs nothing in the
    `genders` rows. What is not acceptable is losing them silently, so
    every dropped facet is named in `failed` and lands in the manifest.
    """
    try:
        payload = client.execute(
            build_facet_query(batch, limit=limit, mincount=mincount), variables
        )
    except AuthError:
        # A dead token is not a broken facet. Dropping it would report every
        # remaining facet as "dropped" and hide the one thing that is wrong.
        raise
    except PulsarError as exc:
        if len(batch) == 1:
            failed.append(batch[0].name)
            say(f"  DROPPED {batch[0].name}: {short_error(exc)}")
            return []
        mid = len(batch) // 2
        say(f"  batch of {len(batch)} failed, splitting")
        left = _fetch_batch(
            client,
            batch[:mid],
            variables,
            limit=limit,
            mincount=mincount,
            say=say,
            failed=failed,
        )
        right = _fetch_batch(
            client,
            batch[mid:],
            variables,
            limit=limit,
            mincount=mincount,
            say=say,
            failed=failed,
        )
        return left + right
    return flatten(payload, batch)


def search_filter(
    search_ids: Sequence[str], date_from: str, date_to: str, **extra: Any
) -> JsonDict:
    """The FilterInput every aggregate call shares.

    A list of ids is the folder rollup: the vendor aggregates across all of
    them server-side, which is the only way to get a folder-wide DISTINCT
    count (unique authors) - per-search values cannot be summed for that.
    """
    return {
        "searchIds": [str(s) for s in search_ids],
        "dateFrom": date_from,
        "dateTo": date_to,
        **extra,
    }


def fetch(
    client: Executor,
    *,
    search_id: str | None = None,
    search_ids: Sequence[str] | None = None,
    date_from: str,
    date_to: str,
    include_pii: bool = False,
    limit: int | None = None,
    mincount: int | None = DEFAULT_MINCOUNT,
    batch_size: int = DEFAULT_BATCH,
    schema_path: pathlib.Path | None = None,
    progress: Callable[[str], None] | None = None,
) -> tuple[list[JsonDict], list[str]]:
    """Every facet and stat for one search (or several, aggregated).

    ~41 calls, volume-independent. Pass `search_ids` for a folder rollup.

    Returns (rows, facets_that_failed). A caller that ignores the second
    element will silently ship an incomplete breakdown.
    """
    say = progress or (lambda _msg: None)
    ids = list(search_ids or ([search_id] if search_id else []))
    if not ids:
        raise ValueError("fetch needs search_id or search_ids")
    facets, stats = catalogue(include_pii=include_pii, schema_path=schema_path)
    variables = {"filter": search_filter(ids, date_from, date_to)}

    rows: list[JsonDict] = []
    failed: list[str] = []
    batches = [facets[i : i + batch_size] for i in range(0, len(facets), batch_size)]
    for n, batch in enumerate(batches, 1):
        say(f"  facets {n}/{len(batches)}: {', '.join(f.name for f in batch)}")
        rows.extend(
            _fetch_batch(
                client,
                batch,
                variables,
                limit=limit,
                mincount=mincount,
                say=say,
                failed=failed,
            )
        )

    # One stat per request, for the same reason as the facets: all ten in
    # one document returns HTTP 500, and a single document means one bad
    # metric takes the other nine with it.
    for n, stat in enumerate(stats, 1):
        say(f"  stat {n}/{len(stats)}: {stat.name} ({stat.metric})")
        try:
            payload = client.execute(build_stat_query([stat]), variables)
        except AuthError:
            raise
        except PulsarError as exc:
            failed.append(stat.name)
            say(f"  DROPPED {stat.name}: {short_error(exc)}")
            continue
        rows.extend(flatten(payload, ()))

    say(
        f"  {len(rows)} row(s)"
        + (f", {len(failed)} facet(s) dropped" if failed else "")
    )
    return rows, failed
