"""Over-time series: the charts along the top of a Pulsar dashboard.

Every over-time series lives here, including plain volume per day: the
facet catalogue hands `timeIntervals` over (facets.TIMESERIES_OWNED) because
only here is the granularity set explicitly and a long window chunked:

  volume           timeIntervals(options: {granularity: DAY})
  metric series    timeIntervals(stat: {type: X, metric: SUM})
                   impressions, reach, visibility, likes, reactions per day
  breakdown series timeIntervals { sources { label value } }
                   one nested dimension per call - "volume by source by day"
  sentiment series timeIntervals with filter.sentiments = [X]
                   TimeInterval has no nested `sentiments`, so the split is
                   three filtered calls instead of one nested one

Every series lands in the facet row shape (`facets._row`), so it goes into
the same aggregates dataset with no new table:

  metric     facet=timeIntervals  label=<date>  metric=SOCIAL_IMPRESSIONS:SUM
  breakdown  facet=sources        label=twitter parent_facet=timeIntervals
                                               parent_label=<date>
  sentiment  facet=sentiments     label=POSITIVE parent_facet=timeIntervals
                                               parent_label=<date>

Like facets, each series is one request and fails alone: a vendor 500 on
the `topics` breakdown costs that series, is named in the returned failure
list, and leaves the rest intact.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
from typing import TYPE_CHECKING, Any

from pulsar.client import AuthError, PulsarError, short_error
from pulsar.facets import NESTED_FIELDS, _row, search_filter

if TYPE_CHECKING:  # pragma: no cover - typing only
    from collections.abc import Callable, Sequence

    from pulsar.export import Executor

JsonDict = dict[str, Any]

# GranularityEnum. DAY is the vendor default too, but a default nobody
# wrote down is one the vendor can change; stating it pins the grain.
DEFAULT_GRANULARITY = "DAY"
GRANULARITIES = ("HOUR", "DAY", "WEEK", "MONTH")

# StatEnum members worth a series, each summed per interval. The score-like
# members (REPUTATION, CREDIBILITY) are left out: a per-day SUM of a score
# is meaningless, and on the searches measured so far both are null anyway.
METRIC_SERIES = ("SOCIAL_IMPRESSIONS", "REACH", "VISIBILITY", "LIKE", "REACTION")

# Nested breakdowns on TimeInterval, one per call for the same reason facets
# are one per call. All six are NestedLabelAsString and are validated
# against the schema in the tests. entityNames is excluded: its label is a
# JSON object, and entities over time are better built from mention rows.
NESTED_SERIES = ("sources", "emotions", "genders", "countries", "languages", "topics")

# SentimentEnum.
SENTIMENTS = ("POSITIVE", "NEGATIVE", "NEUTRAL")

# MEASURED: the vendor refuses DAY granularity on long windows - "Cannot use
# DAY granularity on an interval of 177 days" - while 91 days worked. So a
# long window is pulled in chunks of at most this many days and stitched
# back together, which keeps every series DAILY whatever the window. Only
# DAY is chunked; HOUR's own ceiling is unmeasured.
MAX_DAY_SPAN = 90


@dataclasses.dataclass(frozen=True)
class Series:
    """One request's worth of time series."""

    name: str  # stable id, used in failure lists and the manifest
    kind: str  # "metric" | "nested" | "sentiment"
    arg: str  # the StatEnum member, nested field, or SentimentEnum member


def plan() -> list[Series]:
    """Every series this module pulls, in request order."""
    return (
        [Series("volume", "volume", "")]
        + [Series(f"metric:{m}", "metric", m) for m in METRIC_SERIES]
        + [Series(f"by:{n}", "nested", n) for n in NESTED_SERIES]
        + [Series(f"sentiment:{s}", "sentiment", s) for s in SENTIMENTS]
    )


def build_query(
    series: Series,
    *,
    granularity: str = DEFAULT_GRANULARITY,
    mincount: int | None = 1,
) -> str:
    """One document for one series. Enum values are inlined, not variables:
    they come from the constants above, never from a caller."""
    if granularity not in GRANULARITIES:
        raise ValueError(f"granularity must be one of {GRANULARITIES}")
    args = f"filter: $filter, options: {{granularity: {granularity}}}"
    selection = "label value"

    if series.kind == "metric":
        args += f", stat: {{type: {series.arg}, metric: SUM}}"
        selection += " metric"
    elif series.kind == "nested":
        # mincount for the same reason as the facets: the server suppresses
        # small buckets otherwise. DistinctNestedOptionsInput declares it.
        opts = f"(options: {{mincount: {mincount}}})" if mincount is not None else ""
        selection += f" {series.arg}{opts} {{ {' '.join(NESTED_FIELDS)} }}"

    return (
        "query Series($filter: FilterInput!) {\n"
        f"  timeIntervals({args}) {{ {selection} }}\n"
        "}"
    )


def flatten(payload: JsonDict, series: Series) -> list[JsonDict]:
    """Turn one series response into facet-shaped rows."""
    rows: list[JsonDict] = []
    for point in payload.get("timeIntervals") or []:
        if not isinstance(point, dict):
            continue
        when = point.get("label")

        if series.kind == "volume":
            rows.append(_row("timeIntervals", when, point.get("value")))
        elif series.kind == "metric":
            rows.append(
                _row(
                    "timeIntervals",
                    when,
                    point.get("value"),
                    metric=f"{series.arg}:SUM",
                )
            )
        elif series.kind == "sentiment":
            rows.append(
                _row(
                    "sentiments",
                    series.arg,
                    point.get("value"),
                    parent_facet="timeIntervals",
                    parent_label=when,
                )
            )
        else:
            # The parent point's own value is plain volume, which the
            # `volume` series already lands. Only children here, so a volume
            # chart never double counts.
            for sub in point.get(series.arg) or []:
                rows.append(
                    _row(
                        series.arg,
                        sub.get("label"),
                        sub.get("value"),
                        sample_size=sub.get("sampleSize"),
                        over_index=sub.get("overIndex"),
                        parent_facet="timeIntervals",
                        parent_label=when,
                    )
                )
    return rows


def windows(
    date_from: str, date_to: str, granularity: str = DEFAULT_GRANULARITY
) -> list[tuple[str, str]]:
    """The window split into [from, to) chunks the vendor accepts.

    Consecutive chunks share a boundary instant, and `fetch` keeps only the
    points that fall inside each chunk's own [from, to) - so a boundary day
    is counted once whether the vendor treats dateTo as inclusive or not.
    """
    if granularity != "DAY":
        return [(date_from, date_to)]
    start = dt.datetime.fromisoformat(date_from.replace("Z", "+00:00"))
    end = dt.datetime.fromisoformat(date_to.replace("Z", "+00:00"))
    step = dt.timedelta(days=MAX_DAY_SPAN)
    out: list[tuple[str, str]] = []
    while start < end:
        stop = min(start + step, end)
        out.append((_iso(start), _iso(stop)))
        start = stop
    return out or [(date_from, date_to)]


def _iso(moment: dt.datetime) -> str:
    return moment.astimezone(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _within(row: JsonDict, lo: str, hi: str) -> bool:
    """Is this row's day inside [lo, hi)? Compared on the date part."""
    when = row["label"] if row["facet"] == "timeIntervals" else row["parent_label"]
    day = str(when or "")[:10]
    return lo[:10] <= day < hi[:10] if day else True


def fetch(
    client: Executor,
    *,
    search_ids: Sequence[str],
    date_from: str,
    date_to: str,
    granularity: str = DEFAULT_GRANULARITY,
    mincount: int | None = 1,
    progress: Callable[[str], None] | None = None,
) -> tuple[list[JsonDict], list[str]]:
    """Every series for one search, or several aggregated.

    A window longer than MAX_DAY_SPAN days is fetched in chunks, so the cost
    is len(plan()) x chunks calls. A series that fails in ANY chunk is
    dropped whole - half a series would chart as a cliff, not a gap.

    Returns (rows, series_that_failed), the same contract as `facets.fetch`.
    """
    say = progress or (lambda _msg: None)
    rows: list[JsonDict] = []
    failed: list[str] = []
    todo = plan()
    chunks = windows(date_from, date_to, granularity)
    if len(chunks) > 1:
        say(f"  window split into {len(chunks)} chunks of <= {MAX_DAY_SPAN} days")

    for n, series in enumerate(todo, 1):
        extra = {"sentiments": [series.arg]} if series.kind == "sentiment" else {}
        say(f"  series {n}/{len(todo)}: {series.name}")
        got: list[JsonDict] = []
        try:
            for lo, hi in chunks:
                variables = {"filter": search_filter(search_ids, lo, hi, **extra)}
                payload = client.execute(
                    build_query(series, granularity=granularity, mincount=mincount),
                    variables,
                )
                part = flatten(payload, series)
                got.extend(r for r in part if len(chunks) == 1 or _within(r, lo, hi))
        except AuthError:
            raise
        except PulsarError as exc:
            failed.append(series.name)
            say(f"  DROPPED {series.name}: {short_error(exc)}")
            continue
        rows.extend(got)

    say(
        f"  {len(rows)} series row(s)"
        + (f", {len(failed)} series dropped" if failed else "")
    )
    return rows, failed
