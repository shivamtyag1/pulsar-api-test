"""The shared pull loop.

This is the code both the operator tool and the deployed function run, so
a defect here is a defect in every export. The cases worth pinning are the
ones that are silent: a provenance column that says the wrong search, a key
that does not match the partition a loader globs for, or a short pull that
reports success.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from pulsar.export import (
    SearchExport,
    build_query,
    export_search,
    partition,
    snapshot_of,
)


class FakeClient:
    """Replays canned pages. Records the filters it was asked for, because
    "did we actually request the search we claim to have exported?" is one
    of the things these tests exist to answer."""

    def __init__(self, pages: list[dict[str, Any]]) -> None:
        self._pages = pages
        self.filters: list[dict[str, Any]] = []
        self.request_count = 0

    def execute(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        self.filters.append(variables["filter"])
        self.request_count += 1
        return {"results": self._pages[self.request_count - 1]}


class FakeTarget:
    def __init__(self) -> None:
        self.puts: list[tuple[str, bytes]] = []

    def put(self, key: str, body: bytes) -> None:
        self.puts.append((key, body))

    def rows(self) -> list[dict[str, Any]]:
        return [
            json.loads(line)
            for _, body in self.puts
            for line in body.decode().splitlines()
        ]


def page(rows: list[dict[str, Any]], total: int, cursor: str | None) -> dict[str, Any]:
    return {"total": total, "nextCursor": cursor, "results": rows}


def mention(i: int, search: int | None = 176652) -> dict[str, Any]:
    row: dict[str, Any] = {"pulsarId": f"p{i}", "content": f"row {i}"}
    if search is not None:
        row["search"] = search
    return row


def run(client: FakeClient, target: FakeTarget, **kw: Any) -> SearchExport:
    params: dict[str, Any] = {
        "query": build_query("pulsarId content search"),
        "search_id": "176652",
        "client_token": "raisin-bran",
        "agency": "VM",
        "date_from": "2026-01-01T00:00:00Z",
        "date_to": "2026-09-16T00:00:00Z",
        "run_id": "20260921T192448Z",
        "prefix": "pulsar",
        "page_limit": 2,
    }
    params.update(kw)
    return export_search(client, target, **params)


class TestPartition:
    def test_layout_leads_with_agency_and_uses_the_run_date(self) -> None:
        """Agency first: it is the hard boundary downstream (separate
        Snowflake databases), so a path-level rule can separate it."""
        assert (
            partition("pulsar/mentions", "VM", "raisin-bran", "176652", "2026-09-21")
            == "pulsar/mentions/agency=VM/client=raisin-bran/search=176652"
            "/snapshot=2026-09-21"
        )

    def test_the_key_a_pull_writes_uses_that_partition(self) -> None:
        """The snapshot is the RUN date (from the run id), not the window
        end - a 90-day window must not be filed under a future day."""
        client = FakeClient([page([mention(1)], total=1, cursor=None)])
        target = FakeTarget()
        result = run(client, target)
        assert result.keys == [
            "pulsar/agency=VM/client=raisin-bran/search=176652/snapshot=2026-09-21/"
            "pulsar_trac_mentions_VM_raisin-bran_176652_20260921T192448Z_p0001.json"
        ]

    def test_an_explicit_snapshot_wins(self) -> None:
        client = FakeClient([page([mention(1)], total=1, cursor=None)])
        result = run(client, FakeTarget(), snapshot_date="2026-01-02")
        assert "/snapshot=2026-01-02/" in result.keys[0]

    def test_a_run_id_that_is_not_a_stamp_falls_back_to_the_window(self) -> None:
        assert snapshot_of("manual-run", fallback="2026-09-16T00:00:00Z") == (
            "2026-09-16"
        )
        assert snapshot_of("20260921T192448Z", fallback="x") == "2026-09-21"

    def test_every_row_carries_the_snapshot(self) -> None:
        client = FakeClient([page([mention(1)], total=1, cursor=None)])
        target = FakeTarget()
        run(client, target)
        assert target.rows()[0]["_snapshot_date"] == "2026-09-21"


class TestPaging:
    def test_follows_the_cursor_to_exhaustion(self) -> None:
        client = FakeClient(
            [
                page([mention(1), mention(2)], total=5, cursor="c1"),
                page([mention(3), mention(4)], total=5, cursor="c2"),
                page([mention(5)], total=5, cursor=None),
            ]
        )
        target = FakeTarget()
        result = run(client, target)
        assert result.rows_written == 5
        assert result.reconciled
        assert client.request_count == 3

    def test_every_call_filters_to_the_one_search(self) -> None:
        """Aggregation across searches is a deliberate non-feature here; if
        a filter ever carried two ids the `search=` partition would lie."""
        client = FakeClient(
            [
                page([mention(1), mention(2)], total=3, cursor="c1"),
                page([mention(3)], total=3, cursor=None),
            ]
        )
        run(client, FakeTarget())
        assert all(f["searchIds"] == ["176652"] for f in client.filters)
        assert {f["dateFrom"] for f in client.filters} == {"2026-01-01T00:00:00Z"}

    def test_max_pages_stops_the_pull(self) -> None:
        client = FakeClient(
            [
                page([mention(i), mention(i + 1)], total=100, cursor=f"c{i}")
                for i in range(10)
            ]
        )
        result = run(client, FakeTarget(), max_pages=2)
        assert client.request_count == 2
        assert not result.reconciled  # stopped early: 4 of 100


class TestProvenanceColumns:
    def test_search_id_comes_from_the_vendor_field(self) -> None:
        """`TracContent.search` is the vendor's own per-row origin. Using it
        rather than the requested id keeps the column true if a pull is ever
        widened to several searches."""
        client = FakeClient([page([mention(1, search=999999)], total=1, cursor=None)])
        target = FakeTarget()
        run(client, target)
        assert target.rows()[0]["_search_id"] == "999999"

    def test_search_id_falls_back_to_the_requested_id(self) -> None:
        """A row without the vendor field still has to be attributable."""
        client = FakeClient([page([mention(1, search=None)], total=1, cursor=None)])
        target = FakeTarget()
        run(client, target)
        assert target.rows()[0]["_search_id"] == "176652"

    def test_client_and_agency_are_stamped_on_every_row(self) -> None:
        client = FakeClient(
            [
                page([mention(1), mention(2)], total=3, cursor="c1"),
                page([mention(3)], total=3, cursor=None),
            ]
        )
        target = FakeTarget()
        run(client, target)
        rows = target.rows()
        assert len(rows) == 3
        assert {r["_client"] for r in rows} == {"raisin-bran"}
        assert {r["_agency"] for r in rows} == {"VM"}
        assert {r["_run_id"] for r in rows} == {"20260921T192448Z"}


class TestReconciliation:
    def test_a_complete_pull_reconciles(self) -> None:
        client = FakeClient([page([mention(1), mention(2)], total=2, cursor=None)])
        result = run(client, FakeTarget())
        assert result.reconciled
        assert result.vendor_total == 2
        assert len(result.pulsar_ids) == 2

    def test_a_short_pull_does_not(self) -> None:
        """The failure this catches: the vendor says 50, we wrote 2, and
        nothing else in the run would have noticed."""
        client = FakeClient([page([mention(1), mention(2)], total=50, cursor=None)])
        result = run(client, FakeTarget())
        assert not result.reconciled
        assert result.rows_written == 2

    def test_duplicate_ids_are_counted_once(self) -> None:
        client = FakeClient([page([mention(1), mention(1)], total=2, cursor=None)])
        result = run(client, FakeTarget())
        assert result.rows_written == 2
        assert len(result.pulsar_ids) == 1


class TestManifestEntry:
    def test_the_id_set_stays_out_of_the_json(self) -> None:
        """`pulsar_ids` is a set and would not serialise; the manifest
        carries its size instead."""
        client = FakeClient([page([mention(1)], total=1, cursor=None)])
        entry = run(client, FakeTarget()).manifest_entry()
        assert "pulsar_ids" not in entry
        assert entry["distinct_pulsar_ids"] == 1
        json.dumps(entry)  # must not raise

    def test_it_reports_the_keys_it_wrote(self) -> None:
        client = FakeClient([page([mention(1)], total=1, cursor=None)])
        target = FakeTarget()
        entry = run(client, target).manifest_entry()
        assert entry["keys"] == [k for k, _ in target.puts]
        assert entry["search_id"] == "176652"


def test_a_failing_pull_still_reports_the_parts_already_written() -> None:
    """A raise mid-stream suppresses the unflushed tail, but parts that
    already filled are in the bucket. The caller has to be told about them,
    or a failed run leaves objects nobody knows to clean up."""

    class Boom(FakeClient):
        def execute(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
            if self.request_count >= 1:
                raise RuntimeError("vendor blew up")
            return super().execute(query, variables)

    client = Boom([page([mention(1), mention(2)], total=100, cursor="c1")])
    target = FakeTarget()
    with pytest.raises(RuntimeError, match="vendor blew up"):
        run(client, target, rows_per_file=2)
    assert len(target.puts) == 1  # the full part made it out
