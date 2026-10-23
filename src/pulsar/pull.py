"""One brand, every search in its folder, everything Pulsar will give us.

This is the single entry point the operator tool and the Lambda share, so
what a laptop run writes is byte-for-byte what production writes:

  1. discover   folder -> searches (pulsar.discovery), status-filtered
  2. inventory  one row per search -> pulsar/inventory/...
  3. per search, each step isolated so one failure costs only that step:
       mentions    paged rows                 -> pulsar/mentions/...
       facets      30 breakdowns + 10 stats   -> pulsar/aggregates/...
       timeseries  metric/breakdown/sentiment -> pulsar/aggregates/...
       audience    Audiense/StatSocial report -> pulsar/aggregates/...
  4. rollup     facets + series once more across ALL the folder's searches,
                tagged scope=folder, for numbers that cannot be summed
                (unique authors)
  5. manifest   last, so its existence means the run finished

Every row carries `_agency`, `_client`, `_run_id`, `_snapshot_date`: the
Snowflake promotion routes on `_agency` and must never have to parse a
filename to decide which agency's database a row belongs in.

Failure policy: a vendor error on one search or one step is recorded in the
manifest and the run carries on - the rest of the data is independent and
still useful. A rejected token (AuthError) stops everything, because every
later call would fail the same way.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import json
import math
from typing import TYPE_CHECKING, Any

from pulsar import audience, discovery, facets, fields, timeseries
from pulsar.client import AuthError, PulsarError
from pulsar.export import build_query, export_search, snapshot_of
from pulsar.mapping import AGENCIES, CLIENT_TOKEN
from pulsar.paging import RunawayPagination
from pulsar.sink import RollingSink

if TYPE_CHECKING:  # pragma: no cover - typing only
    from collections.abc import Callable, Iterable

    from pulsar.export import Executor
    from pulsar.sink import Target

JsonDict = dict[str, Any]

DATASETS = ("mentions", "facets", "timeseries", "audience")
DEFAULT_PREFIX = "pulsar"

# MEASURED throughput of the bearer token. Used only for the dry-run
# estimate; the client's own limiter is what actually paces requests.
MEASURED_RATE = 0.80


@dataclasses.dataclass(frozen=True)
class PullSpec:
    """Everything that decides what a run pulls. No I/O in here."""

    client_token: str
    agency: str
    date_from: str  # ISO 8601, e.g. 2026-07-01T00:00:00Z
    date_to: str
    folder: str | None = None
    search_ids: tuple[str, ...] = ()
    only_search: str | None = None
    datasets: frozenset[str] = frozenset(DATASETS)
    statuses: tuple[str, ...] | None = None
    all_history: bool = False
    rollup: bool = True
    include_pii: bool = False
    max_pages: int | None = None
    limit: int | None = None
    mincount: int | None = facets.DEFAULT_MINCOUNT
    batch_size: int = facets.DEFAULT_BATCH
    granularity: str = timeseries.DEFAULT_GRANULARITY

    def validate(self) -> None:
        problems = []
        if not CLIENT_TOKEN.match(self.client_token):
            problems.append(f"client must match {CLIENT_TOKEN.pattern}")
        if self.agency not in AGENCIES:
            problems.append(f"agency must be one of {sorted(AGENCIES)}")
        unknown = self.datasets - set(DATASETS)
        if unknown or not self.datasets:
            problems.append(f"datasets must be a non-empty subset of {DATASETS}")
        if not self.folder and not self.search_ids and not self.only_search:
            problems.append("need a folder or a search id")
        if self.limit is not None and self.limit > facets.MAX_LIMIT:
            problems.append(f"limit must be <= {facets.MAX_LIMIT}")
        if self.date_from >= self.date_to:
            problems.append("date_from must be before date_to")
        if problems:
            raise ValueError("; ".join(problems))


def iso_day(value: str | dt.date) -> str:
    """YYYY-MM-DD (or a date) -> the ISO datetime the API filters on."""
    text = value.isoformat() if isinstance(value, dt.date) else str(value)
    return text if len(text) > 10 else f"{text}T00:00:00Z"


def dataset_of(row: JsonDict) -> str:
    """Which of the four aggregate kinds a row is. Stamped on every row so
    a view never has to re-derive it from facet names."""
    facet = str(row.get("facet") or "")
    if facet.startswith("audience."):
        return "audience"
    if facet == "timeIntervals" or row.get("parent_facet") == "timeIntervals":
        return "timeseries"
    if row.get("label") is None and row.get("metric") is not None:
        return "stat"
    return "facet"


def _calls(client: Executor) -> int:
    return int(getattr(client, "request_count", 0) or 0)


# How far back `--all-history` looks for a search's oldest mention. Only a
# floor for the one probe call below - the window that is actually pulled
# starts at the oldest mention found, not here.
HISTORY_FLOOR = "2006-01-01T00:00:00Z"

OLDEST_MENTION = """
query Oldest($filter: FilterInput!) {
  results(filter: $filter, options: {limit: 1, sortBy: TIME, sort: ASC}) {
    total
    results { publishedAt }
  }
}
"""


def oldest_mention(client: Executor, search_id: str, date_to: str) -> str | None:
    """The publish date (YYYY-MM-DD) of a search's oldest mention, or None."""
    data = client.execute(
        OLDEST_MENTION,
        {"filter": facets.search_filter([search_id], HISTORY_FLOOR, date_to)},
    )
    rows = (data.get("results") or {}).get("results") or []
    published = rows[0].get("publishedAt") if rows else None
    return str(published)[:10] if published else None


def _window_from(
    spec: PullSpec, search: discovery.SearchInfo, client: Executor
) -> tuple[str, str]:
    """Where a search's window starts, and why.

    `--all-history` does NOT mean "since startDate". MEASURED on Lovesac:
    startDate was 2026-08-27 (reset when the search was edited) while its
    mentions went back to June, so a startDate window pulled 1,741 of 3,505.
    The oldest mention is asked for directly instead, and the window takes
    the EARLIEST of that, startDate and --from - so however the vendor
    sorts, --all-history can never pull less than the default window.
    """
    if not spec.all_history:
        return spec.date_from, "--from"
    return history_start(client, search, spec.date_from, spec.date_to)


def history_start(
    client: Executor,
    search: discovery.SearchInfo,
    date_from: str,
    date_to: str,
    *,
    default_basis: str = "--from",
) -> tuple[str, str]:
    """The earliest of `date_from`, the search's startDate and its oldest
    mention, and which of the three it was. Shared by `--all-history` and
    the scheduled pipeline's backfill so both start from the same day."""
    candidates = {date_from: default_basis}
    if search.start_date:
        candidates.setdefault(iso_day(search.start_date[:10]), "search startDate")
    try:
        oldest = oldest_mention(client, search.id, date_to)
    except AuthError:
        raise
    except PulsarError:
        oldest = None
    if oldest:
        candidates[iso_day(oldest)] = "oldest mention"
    start = min(candidates)
    return start, candidates[start]


def estimate(
    inv: discovery.Inventory, spec: PullSpec, *, schema_path: Any = None
) -> JsonDict:
    """Upper-bound call count and wall clock, before anything is pulled.

    Mention pages are estimated from `totalContents`, which is the search's
    LIFETIME count - so for a 90-day window this over-estimates, never
    under. That is the right direction for a number used to decide whether
    to start.
    """
    found, stats = facets.catalogue(
        include_pii=spec.include_pii, schema_path=schema_path
    )
    per_facets = math.ceil(len(found) / spec.batch_size) + len(stats)
    series_per_chunk = len(timeseries.plan())
    chunks = len(timeseries.windows(spec.date_from, spec.date_to, spec.granularity))
    per_series = series_per_chunk * chunks
    per_search: dict[str, int] = {}
    for s in inv.included:
        n = 0
        if "mentions" in spec.datasets:
            n += max(1, math.ceil((s.total_contents or 0) / 100))
        if "facets" in spec.datasets:
            n += per_facets
        if "timeseries" in spec.datasets:
            n += per_series
        if "audience" in spec.datasets:
            n += 1
        if spec.all_history:
            n += 1  # the oldest-mention probe
        per_search[s.id] = n
    rollup = 0
    if spec.rollup and inv.folder and len(inv.included) > 1:
        rollup += per_facets if "facets" in spec.datasets else 0
        rollup += per_series if "timeseries" in spec.datasets else 0
    total = sum(per_search.values()) + rollup
    return {
        "per_search": per_search,
        "rollup": rollup,
        "calls": total,
        "minutes": round(total / MEASURED_RATE / 60, 1),
    }


class Stamper:
    """Adds the provenance columns every row carries."""

    def __init__(
        self, *, client_token: str, agency: str, run_id: str, snapshot: str
    ) -> None:
        self.common = {
            "_client": client_token,
            "_agency": agency,
            "_run_id": run_id,
            "_snapshot_date": snapshot,
            "_ingested_at": run_id,
        }

    def aggregate(
        self,
        row: JsonDict,
        *,
        scope: str,
        search_id: str | None,
        folder_id: Any,
        window_from: str,
        window_to: str,
    ) -> JsonDict:
        # setdefault: audience rows arrive carrying their REPORT's window,
        # which is the true one and must not be overwritten by the pull's.
        row.setdefault("window_from", window_from)
        row.setdefault("window_to", window_to)
        row["dataset"] = dataset_of(row)
        row["scope"] = scope
        row["_search_id"] = search_id
        row["folder_id"] = None if folder_id is None else str(folder_id)
        row.update(self.common)
        return row

    def inventory(self, row: JsonDict) -> JsonDict:
        row.update(self.common)
        return row


def run(
    spec: PullSpec,
    *,
    meta: Executor,
    data: Executor,
    target: Target,
    run_id: str,
    prefix: str = DEFAULT_PREFIX,
    progress: Callable[[str], None] | None = None,
    schema_path: Any = None,
) -> JsonDict:
    """Pull everything `spec` names and write it through `target`.

    Returns the manifest, which is also written last to
    `<prefix>/state/runs/run=<run_id>/manifest.json`.
    """
    spec.validate()
    say = progress or (lambda _msg: None)
    snapshot = snapshot_of(run_id, fallback=spec.date_to)
    agency, token = spec.agency, spec.client_token
    part = f"agency={agency}/client={token}/snapshot={snapshot}"
    stamp = Stamper(client_token=token, agency=agency, run_id=run_id, snapshot=snapshot)
    started = dt.datetime.now(dt.UTC).isoformat(timespec="seconds")

    # 1. discover
    say(f"discovering searches for {token}")
    pinned = tuple(spec.search_ids) + ((spec.only_search,) if spec.only_search else ())
    inv = discovery.discover(
        meta,
        folder=spec.folder,
        search_ids=pinned,
        statuses=spec.statuses,
        progress=say,
    )
    if spec.only_search:
        for s in inv.searches:
            if s.id != spec.only_search and s.included:
                s.included, s.skip_reason = False, "not selected (--search-id)"
    folder_id = (inv.folder or {}).get("id")

    # 2. inventory
    with RollingSink(
        target,
        prefix=f"{prefix}/inventory/{part}",
        stem=f"pulsar_trac_inventory_{agency}_{token}_{run_id}",
    ) as inv_sink:
        for row in inv.rows():
            inv_sink.write(stamp.inventory(row))

    # 3 + 4. per search, then rollup
    mention_query = build_query(fields.results_selection(include_pii=spec.include_pii))
    results: list[JsonDict] = []
    windows: list[str] = []
    agg_sink = RollingSink(
        target,
        prefix=f"{prefix}/aggregates/{part}",
        stem=f"pulsar_trac_aggregates_{agency}_{token}_{run_id}",
        rows_per_file=facets.FACET_ROWS_PER_FILE,
    )

    def write_aggs(
        rows: Iterable[JsonDict], *, scope: str, sid: str | None, wf: str
    ) -> int:
        n = 0
        for row in rows:
            agg_sink.write(
                stamp.aggregate(
                    row,
                    scope=scope,
                    search_id=sid,
                    folder_id=folder_id,
                    window_from=wf,
                    window_to=spec.date_to,
                )
            )
            n += 1
        return n

    def aggregates_for(ids: list[str], wf: str, entry: JsonDict) -> list[JsonDict]:
        out: list[JsonDict] = []
        if "facets" in spec.datasets:
            try:
                rows, dropped = facets.fetch(
                    data,
                    search_ids=ids,
                    date_from=wf,
                    date_to=spec.date_to,
                    include_pii=spec.include_pii,
                    limit=spec.limit,
                    mincount=spec.mincount,
                    batch_size=spec.batch_size,
                    schema_path=schema_path,
                    progress=say,
                )
                out += rows
                entry["facets_dropped"] = dropped
            except AuthError:
                raise
            except PulsarError as exc:
                entry["errors"].append(f"facets: {exc}")
        if "timeseries" in spec.datasets:
            try:
                rows, dropped = timeseries.fetch(
                    data,
                    search_ids=ids,
                    date_from=wf,
                    date_to=spec.date_to,
                    granularity=spec.granularity,
                    mincount=spec.mincount,
                    progress=say,
                )
                out += rows
                entry["series_dropped"] = dropped
            except AuthError:
                raise
            except PulsarError as exc:
                entry["errors"].append(f"timeseries: {exc}")
        return out

    for n, search in enumerate(inv.included, 1):
        wf, basis = _window_from(spec, search, data)
        windows.append(wf)
        say(
            f"\n[{n}/{len(inv.included)}] {search.label}  "
            f"{wf[:10]} ({basis}) -> {spec.date_to[:10]}"
        )
        entry: JsonDict = {
            "search_id": search.id,
            "search_name": search.name,
            "status": search.status,
            "realtime_status": search.realtime_status,
            "window": {"from": wf, "to": spec.date_to, "from_basis": basis},
            "errors": [],
        }

        if "mentions" in spec.datasets:
            try:
                exp = export_search(
                    data,
                    target,
                    query=mention_query,
                    search_id=search.id,
                    client_token=token,
                    agency=agency,
                    date_from=wf,
                    date_to=spec.date_to,
                    run_id=run_id,
                    prefix=f"{prefix}/mentions",
                    snapshot_date=snapshot,
                    max_pages=spec.max_pages,
                    progress=say,
                )
                entry["mentions"] = exp.manifest_entry()
            except AuthError:
                raise
            except (PulsarError, RunawayPagination) as exc:
                entry["errors"].append(f"mentions: {exc}")

        rows = aggregates_for([search.id], wf, entry)
        entry["aggregate_rows"] = write_aggs(rows, scope="search", sid=search.id, wf=wf)

        if "audience" in spec.datasets:
            try:
                aud = audience.fetch(meta, search_id=search.id, progress=say)
                entry["audience"] = aud.status
                entry["audience_reports"] = aud.reports
                entry["aggregate_rows"] += write_aggs(
                    aud.rows, scope="search", sid=search.id, wf=wf
                )
            except AuthError:
                raise
            except PulsarError as exc:
                entry["errors"].append(f"audience: {exc}")

        results.append(entry)

    rollup: JsonDict | None = None
    wants_rollup = spec.datasets & {"facets", "timeseries"}
    if spec.rollup and inv.folder and len(inv.included) > 1 and wants_rollup:
        ids = [s.id for s in inv.included]
        wf = min(windows)
        say(f"\n[rollup] folder {inv.folder.get('name')!r}: {len(ids)} searches")
        rollup = {
            "search_ids": ids,
            "window": {"from": wf, "to": spec.date_to},
            "errors": [],
        }
        rows = aggregates_for(ids, wf, rollup)
        rollup["aggregate_rows"] = write_aggs(rows, scope="folder", sid=None, wf=wf)

    agg_sink.close()

    # 5. manifest
    dropped = sum(
        len(r.get("facets_dropped") or []) + len(r.get("series_dropped") or [])
        for r in [*results, *([rollup] if rollup else [])]
    )
    errors = sum(len(r["errors"]) for r in results) + len(
        (rollup or {}).get("errors", [])
    )
    mentions_rows = sum(
        (r.get("mentions") or {}).get("rows_written", 0) for r in results
    )
    manifest: JsonDict = {
        "run_id": run_id,
        "action": "pull",
        "client": token,
        "agency": agency,
        "folder": (
            {"id": folder_id, "name": inv.folder.get("name")} if inv.folder else None
        ),
        "snapshot_date": snapshot,
        "window": {"from": spec.date_from, "to": spec.date_to},
        "all_history": spec.all_history,
        "datasets": sorted(spec.datasets),
        "started_at": started,
        "finished_at": dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
        "searches": results,
        "skipped": [
            {"search_id": s.id, "search_name": s.name, "reason": s.skip_reason}
            for s in inv.skipped
        ],
        "not_visible": inv.missing,
        "rollup": rollup,
        "rows": {
            "inventory": inv_sink.rows_written,
            "mentions": mentions_rows,
            "aggregates": agg_sink.rows_written,
        },
        "reconciled": all(
            (r.get("mentions") or {}).get("reconciled", True) for r in results
        ),
        "errors": errors,
        "dropped": dropped,
        "ok": errors == 0,
        "pii_included": spec.include_pii,
        "api_calls": {"trac_meta": _calls(meta), "trac_data": _calls(data)},
        "keys": {
            "inventory": inv_sink.keys,
            "mentions": [
                k for r in results for k in (r.get("mentions") or {}).get("keys", [])
            ],
            "aggregates": agg_sink.keys,
        },
    }
    key = f"{prefix}/state/runs/run={run_id}/manifest.json"
    target.put(key, json.dumps(manifest, indent=2, default=str).encode())
    manifest["manifest_key"] = key
    return manifest
