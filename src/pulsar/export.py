"""Pull one search into one partition.

This lives in the library rather than in the tool because the operator tool
and the deployed function must produce the *same bytes in the same keys*.
An earlier version of this repo kept a second copy of `RollingSink` inside
the tool, and the consequence was that verifying a local export told you
nothing reliable about what production would write. One pull loop, two
callers, no drift.

A client's searches are pulled one at a time rather than aggregated into a
single `filter.searchIds` call. The API supports the aggregate form, but:

  * throughput is capped at 0.80 req/s by the bearer token, so N searches
    cost the same wall clock either way;
  * `search=<id>` stays honest as a path segment;
  * reconciliation stays per-search, so a shortfall names the search that
    dropped rows instead of the client;
  * one sink is in memory at a time, which is what keeps a part inside the
    function's memory budget (see pulsar.sink).
"""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING, Any, Protocol

from pulsar.paging import PaginationGuard
from pulsar.sink import DEFAULT_ROWS_PER_FILE, RollingSink

if TYPE_CHECKING:  # pragma: no cover - typing only
    from collections.abc import Callable

    from pulsar.sink import Target


class Executor(Protocol):
    """The one thing this needs from a client: run a query, get JSON back.

    Structural rather than concrete for the same reason `sink.Target` is -
    it lets a test replay canned pages without a transport, which is what
    keeps the suite off the metered API.
    """

    def execute(self, query: str, variables: dict[str, Any]) -> dict[str, Any]: ...


RESULTS_TEMPLATE = """
query Results($filter: FilterInput!, $options: ResultsOptionsInput) {{
  results(filter: $filter, options: $options) {{
    total
    nextCursor
    results {{
      {selection}
    }}
  }}
}}
"""

PAGE_LIMIT = 100


def build_query(selection: str) -> str:
    return RESULTS_TEMPLATE.format(selection=selection)


@dataclasses.dataclass
class ExportCheckpoint:
    """Where a resumable export got to: enough to carry on in a new process.

    Saved only at page boundaries, after every buffered row has been written
    to a part, so resuming from `cursor` neither skips nor repeats a row.
    """

    cursor: str | None = None
    pages: int = 0
    rows: int = 0
    parts: int = 0
    keys: list[str] = dataclasses.field(default_factory=list)
    bytes_written: int = 0
    vendor_total: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, doc: dict[str, Any] | None) -> ExportCheckpoint | None:
        if not doc:
            return None
        known = {f.name for f in dataclasses.fields(cls)}
        return cls(**{k: v for k, v in doc.items() if k in known})


@dataclasses.dataclass
class SearchExport:
    """What one search's pull produced."""

    search_id: str
    vendor_total: int | None
    rows_written: int
    pulsar_ids: set[str]
    reconciliation: str
    reconciled: bool
    keys: list[str]
    bytes_written: int
    # False when a resumable export stopped at its deadline; `checkpoint`
    # then says where the next invocation carries on.
    finished: bool = True
    checkpoint: ExportCheckpoint | None = None

    def manifest_entry(self) -> dict[str, Any]:
        """The JSON-safe view. `pulsar_ids` is a set and stays out of it."""
        return {
            "search_id": self.search_id,
            "vendor_total": self.vendor_total,
            "rows_written": self.rows_written,
            "distinct_pulsar_ids": len(self.pulsar_ids),
            "reconciliation": self.reconciliation,
            "reconciled": self.reconciled,
            "keys": self.keys,
            "bytes": self.bytes_written,
        }


def snapshot_of(run_id: str, *, fallback: str) -> str:
    """The YYYY-MM-DD a run id was stamped on, e.g. 20260928T101500Z.

    Falls back to `fallback[:10]` for an id that is not a UTC stamp, so a
    caller passing its own run id still gets a valid partition.
    """
    if len(run_id) >= 9 and run_id[:8].isdigit() and run_id[8] == "T":
        return f"{run_id[:4]}-{run_id[4:6]}-{run_id[6:8]}"
    return fallback[:10]


def partition(
    prefix: str, agency: str, client_token: str, search_id: str, snapshot: str
) -> str:
    """The key prefix for one search in one run.

    Source at the top level, then Hive-style partitions. Agency is the first
    partition because it is the hard boundary downstream - VM, CM and TG data
    land in separate Snowflake databases - and a path that leads with it lets
    a bucket policy or a loader pattern separate agencies without parsing
    filenames.

    `snapshot` is the RUN date, not the window end. The window can span
    months; the snapshot says when this copy of it was taken, which is what
    a re-pull needs to supersede.
    """
    return (
        f"{prefix}/agency={agency}/client={client_token}"
        f"/search={search_id}/snapshot={snapshot}"
    )


def export_search(
    client: Executor,
    target: Target,
    *,
    query: str,
    search_id: str,
    client_token: str,
    agency: str,
    date_from: str,
    date_to: str,
    run_id: str,
    prefix: str,
    snapshot_date: str | None = None,
    max_pages: int | None = None,
    page_limit: int = PAGE_LIMIT,
    rows_per_file: int = DEFAULT_ROWS_PER_FILE,
    progress: Callable[[str], None] | None = None,
    extra_filter: dict[str, Any] | None = None,
    start: ExportCheckpoint | None = None,
    deadline: Callable[[], bool] | None = None,
    on_checkpoint: Callable[[ExportCheckpoint], None] | None = None,
) -> SearchExport:
    """Page one search to exhaustion, writing parts as they fill.

    Resumable mode (the scheduled pipeline) is switched on by passing
    `on_checkpoint`. Parts are then cut only at page boundaries, and after
    each cut the checkpoint is handed back to be saved. `deadline()` is
    polled before every page; once it says time is up the export flushes,
    checkpoints and returns `finished=False` instead of being killed by the
    Lambda wall mid-page. `start` resumes from a saved checkpoint.

    `extra_filter` merges into the vendor filter - the incremental pull
    passes `{"updatedFrom": ...}` through it.
    """
    say = progress or (lambda _msg: None)
    resumable = on_checkpoint is not None
    snapshot = snapshot_date or snapshot_of(run_id, fallback=date_to)
    key_prefix = partition(prefix, agency, client_token, search_id, snapshot)
    stem = f"pulsar_trac_mentions_{agency}_{client_token}_{search_id}_{run_id}"

    sink = RollingSink(
        target,
        prefix=key_prefix,
        stem=stem,
        rows_per_file=rows_per_file,
        start_part=start.parts if start else 0,
        auto_flush=not resumable,
    )
    prior_keys = list(start.keys) if start else []
    prior_rows = start.rows if start else 0
    prior_bytes = start.bytes_written if start else 0
    guard: PaginationGuard | None = None
    seen: set[str] = set()
    finished = False

    def checkpoint() -> ExportCheckpoint:
        return ExportCheckpoint(
            cursor=guard.cursor if guard else (start.cursor if start else None),
            pages=guard.pages if guard else (start.pages if start else 0),
            rows=prior_rows + sink.rows_written,
            parts=sink.part,
            keys=prior_keys + sink.keys,
            bytes_written=prior_bytes + sink.bytes_written,
            vendor_total=(
                guard.vendor_total if guard else (start.vendor_total if start else None)
            ),
        )

    try:
        while True:
            if deadline is not None and deadline():
                say("  deadline reached - checkpointing")
                break
            options: dict[str, object] = {"limit": page_limit}
            if guard is not None and guard.cursor:
                options["cursor"] = guard.cursor
            elif guard is None and start is not None and start.cursor:
                options["cursor"] = start.cursor

            flt: dict[str, Any] = {
                "searchIds": [search_id],
                "dateFrom": date_from,
                "dateTo": date_to,
            }
            if extra_filter:
                flt.update(extra_filter)
            data = client.execute(query, {"filter": flt, "options": options})
            block = data.get("results") or {}
            batch = block.get("results") or []

            if guard is None:
                guard = PaginationGuard(
                    vendor_total=(
                        start.vendor_total
                        if start and start.vendor_total is not None
                        else block.get("total")
                    ),
                    page_limit=page_limit,
                    max_pages=max_pages,
                )
                if start is not None:
                    # Carry the counters over so the runaway ceiling and the
                    # "reached vendor total" exit see the WHOLE sweep.
                    guard.pages, guard.rows_seen = start.pages, start.rows
                    guard.cursor = start.cursor
                say(f"  vendor total = {guard.vendor_total}")

            for row in batch:
                pid = row.get("pulsarId")
                if pid is not None:
                    seen.add(str(pid))
                # Our own provenance columns, underscore-prefixed so they are
                # never mistaken for vendor fields downstream. `_search_id`
                # prefers the vendor's own per-row `search` field, so the
                # column stays true even if a pull is ever aggregated.
                row["_search_id"] = str(row.get("search") or search_id)
                row["_client"] = client_token
                row["_agency"] = agency
                row["_run_id"] = run_id
                row["_snapshot_date"] = snapshot
                row["_ingested_at"] = run_id
                sink.write(row)

            decision = guard.observe(len(batch), block.get("nextCursor"))
            say(
                f"  page {guard.pages:>4}: {len(batch):>3} rows "
                f"(running {prior_rows + sink.rows_written:,})"
            )
            if decision:
                say(f"  stopped: {decision.reason}")
                finished = True
                break
            if resumable and sink.buffered >= rows_per_file:
                sink.flush()
                assert on_checkpoint is not None
                on_checkpoint(checkpoint())
    finally:
        # Legacy mode always closes: a raise leaves the tail unflushed by
        # design, but the parts that already filled are out and the caller
        # must know the keys. Resumable mode does NOT flush on a raise - the
        # retry resumes from the last checkpoint and rewrites those rows.
        if not resumable:
            sink.close()

    if resumable:
        sink.flush()
        assert on_checkpoint is not None
        on_checkpoint(checkpoint())

    cp = checkpoint()
    if not finished:
        return SearchExport(
            search_id=search_id,
            vendor_total=cp.vendor_total,
            rows_written=cp.rows,
            pulsar_ids=seen,
            reconciliation="unfinished - resumes from checkpoint",
            reconciled=False,
            keys=cp.keys,
            bytes_written=cp.bytes_written,
            finished=False,
            checkpoint=cp,
        )

    # A resumed sweep only saw part of the ids, so distinct counting would
    # under-report; it reconciles on rows instead.
    distinct = None if start is not None else (len(seen) or None)
    ok, verdict = (
        guard.reconcile(distinct_rows=distinct) if guard else (False, "no pages")
    )
    say(f"  reconciliation: {verdict}")

    return SearchExport(
        search_id=search_id,
        vendor_total=guard.vendor_total if guard else None,
        rows_written=cp.rows,
        pulsar_ids=seen,
        reconciliation=verdict,
        reconciled=ok,
        keys=cp.keys,
        bytes_written=cp.bytes_written,
        finished=True,
        checkpoint=cp,
    )
