"""The orchestrator: one brand's folder in, the S3 layout out.

What these pin: every file lands under the agency-first layout, every row
carries the routing columns Snowflake promotes on, one broken search does
not sink the run, a dead token does, and the manifest is written last.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from pulsar import pull, timeseries
from pulsar.client import AuthError, PulsarError

RUN_ID = "20260928T101500Z"
FOLDER = {"id": 9, "name": "Lovesac", "searchIds": ["1", "2"]}


def search(sid: int, **kw: Any) -> dict[str, Any]:
    node = {
        "id": sid,
        "name": f"Search {sid}",
        "status": "READY",
        "realtimeStatus": "STARTED",
        "historicStatus": "PRESENT",
        "type": "TRAC",
        "startDate": "2025-03-01",
        "endDate": None,
        "totalContents": 150,
        "folderIds": [9],
    }
    node.update(kw)
    return node


class FakeMeta:
    def __init__(self, searches: list[dict[str, Any]] | None = None) -> None:
        self.searches = searches if searches is not None else [search(1), search(2)]
        self.request_count = 0

    def execute(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        self.request_count += 1
        done = {"hasNextPage": False, "endCursor": None}
        if "query Folders" in query:
            return {"folders": {"nodes": [FOLDER], "pageInfo": done}}
        if "query Searches" in query:
            nodes = self.searches
            if variables.get("ids"):
                nodes = [s for s in nodes if str(s["id"]) in variables["ids"]]
            return {"searches": {"nodes": nodes, "pageInfo": done}}
        if "query Audience" in query:
            nodes = (
                [
                    {
                        "id": 5,
                        "provider": "AUDIENSE",
                        "status": "READY",
                        "dateFrom": "2026-01-01T00:00:00Z",
                        "dateTo": "2026-02-01T00:00:00Z",
                        "fullAudienceSize": 10,
                        "communityInfoSegments": [
                            {"name": "fans", "size": 4, "sizeRel": 0.4}
                        ],
                    }
                ]
                if variables["searchId"] == "1"
                else []
            )
            return {"audienceReports": {"nodes": nodes, "pageInfo": done}}
        raise AssertionError(f"unexpected meta query: {query[:40]}")


class FakeData:
    def __init__(
        self,
        fail_mentions: set[str] | None = None,
        auth_on: str | None = None,
        oldest: str | None = None,
        oldest_fails: bool = False,
    ) -> None:
        self.fail_mentions = fail_mentions or set()
        self.auth_on = auth_on
        self.oldest = oldest
        self.oldest_fails = oldest_fails
        self.filters: list[dict[str, Any]] = []
        self.request_count = 0

    def execute(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        self.request_count += 1
        flt = variables.get("filter") or {}
        self.filters.append(flt)
        ids = flt.get("searchIds") or []
        if self.auth_on and self.auth_on in query:
            raise AuthError("HTTP 401 - token rejected")
        if "query Oldest" in query:
            if self.oldest_fails:
                raise PulsarError("HTTP 500")
            rows = [{"publishedAt": self.oldest}] if self.oldest else []
            return {"results": {"total": len(rows), "results": rows}}
        if "results(" in query:
            if ids[0] in self.fail_mentions:
                raise PulsarError("HTTP 500")
            return {
                "results": {
                    "total": 1,
                    "nextCursor": None,
                    "results": [{"pulsarId": f"p{ids[0]}", "search": int(ids[0])}],
                }
            }
        if "query Series" in query:
            point: dict[str, Any] = {"label": "2026-09-01T00:00:00Z", "value": 2.0}
            for name in timeseries.NESTED_SERIES:
                point[name] = [{"label": "x", "value": 1.0}]
            return {"timeIntervals": [point]}
        if "query Facets" in query and "genders(" in query:
            return {"genders": [{"label": "female", "value": 3.0, "userAge": []}]}
        if "query Facets" in query and "timeIntervals(" in query:
            return {"timeIntervals": [{"label": "2026-09-01T00:00:00Z", "value": 2}]}
        if "query Stats" in query and "users(" in query:
            return {"users": {"metric": "unique", "value": 7.0}}
        return {}


class FakeTarget:
    def __init__(self) -> None:
        self.puts: list[tuple[str, bytes]] = []

    def put(self, key: str, body: bytes) -> None:
        self.puts.append((key, body))

    def written(self) -> list[str]:
        return [k for k, _ in self.puts]

    def rows(self, dataset: str) -> list[dict[str, Any]]:
        return [
            json.loads(line)
            for k, body in self.puts
            if f"pulsar/{dataset}/" in k
            for line in body.splitlines()
            if line.strip()
        ]


def spec(**kw: Any) -> pull.PullSpec:
    base: dict[str, Any] = {
        "client_token": "lovesac",
        "agency": "VM",
        "folder": "Lovesac",
        "date_from": "2026-07-01T00:00:00Z",
        "date_to": "2026-09-29T00:00:00Z",
    }
    base.update(kw)
    return pull.PullSpec(**base)


def run(s: pull.PullSpec, **kw: Any) -> tuple[dict[str, Any], FakeTarget, FakeData]:
    target = FakeTarget()
    data = kw.pop("data", FakeData())
    manifest = pull.run(
        s,
        meta=kw.pop("meta", FakeMeta()),
        data=data,
        target=target,
        run_id=RUN_ID,
        **kw,
    )
    return manifest, target, data


class TestLayout:
    def test_every_dataset_lands_agency_first_under_one_parent(self) -> None:
        _, target, _ = run(spec())
        part = "agency=VM/client=lovesac"
        keys = target.written()
        assert any(
            k.startswith(f"pulsar/inventory/{part}/snapshot=2026-09-28/") for k in keys
        )
        assert any(
            k.startswith(f"pulsar/mentions/{part}/search=1/snapshot=2026-09-28/")
            for k in keys
        )
        assert any(
            k.startswith(f"pulsar/aggregates/{part}/snapshot=2026-09-28/") for k in keys
        )

    def test_manifest_is_written_last(self) -> None:
        manifest, target, _ = run(spec())
        assert target.written()[-1] == f"pulsar/state/runs/run={RUN_ID}/manifest.json"
        assert manifest["manifest_key"] == target.written()[-1]
        assert json.loads(target.puts[-1][1])["run_id"] == RUN_ID

    def test_a_custom_prefix(self) -> None:
        _, target, _ = run(spec(), prefix="sandbox/pulsar")
        assert all(k.startswith("sandbox/pulsar/") for k in target.written())


class TestRows:
    def test_every_row_carries_the_routing_columns(self) -> None:
        _, target, _ = run(spec())
        for dataset in ("inventory", "mentions", "aggregates"):
            rows = target.rows(dataset)
            assert rows, dataset
            for row in rows:
                assert row["_agency"] == "VM"
                assert row["_client"] == "lovesac"
                assert row["_run_id"] == RUN_ID
                assert row["_snapshot_date"] == "2026-09-28"

    def test_aggregate_rows_are_classified_and_scoped(self) -> None:
        _, target, _ = run(spec())
        rows = target.rows("aggregates")
        kinds = {r["dataset"] for r in rows}
        assert kinds == {"facet", "stat", "timeseries", "audience"}
        search_rows = [r for r in rows if r["scope"] == "search"]
        assert {r["_search_id"] for r in search_rows} == {"1", "2"}
        assert all(r["folder_id"] == "9" for r in rows)

    def test_audience_rows_keep_their_own_window(self) -> None:
        _, target, _ = run(spec())
        aud = [r for r in target.rows("aggregates") if r["dataset"] == "audience"]
        assert aud[0]["window_from"] == "2026-01-01T00:00:00Z"
        other = [r for r in target.rows("aggregates") if r["dataset"] == "facet"]
        assert other[0]["window_from"] == "2026-07-01T00:00:00Z"

    def test_dataset_of(self) -> None:
        assert pull.dataset_of({"facet": "audience.ages"}) == "audience"
        assert pull.dataset_of({"facet": "timeIntervals"}) == "timeseries"
        assert (
            pull.dataset_of({"facet": "sources", "parent_facet": "timeIntervals"})
            == "timeseries"
        )
        assert pull.dataset_of({"facet": "reach", "label": None, "metric": "sum"}) == (
            "stat"
        )
        assert pull.dataset_of({"facet": "genders", "label": "female"}) == "facet"


class TestRollup:
    def test_folder_rollup_asks_for_every_search_at_once(self) -> None:
        manifest, target, data = run(spec())
        assert any(f.get("searchIds") == ["1", "2"] for f in data.filters)
        folder_rows = [r for r in target.rows("aggregates") if r["scope"] == "folder"]
        assert folder_rows
        assert all(r["_search_id"] is None for r in folder_rows)
        assert manifest["rollup"]["search_ids"] == ["1", "2"]

    def test_no_rollup_for_a_single_search(self) -> None:
        manifest, _, _ = run(spec(), meta=FakeMeta([search(1)]))
        assert manifest["rollup"] is None

    def test_rollup_can_be_turned_off(self) -> None:
        manifest, _, data = run(spec(rollup=False))
        assert manifest["rollup"] is None
        assert not any(len(f.get("searchIds") or []) > 1 for f in data.filters)


class TestFailures:
    def test_one_broken_search_does_not_sink_the_run(self) -> None:
        manifest, _, _ = run(spec(), data=FakeData(fail_mentions={"1"}))
        first, second = manifest["searches"]
        assert first["errors"] and "mentions" in first["errors"][0]
        assert "mentions" not in first
        assert first["aggregate_rows"] > 0  # facets for search 1 still landed
        assert second["mentions"]["rows_written"] == 1
        assert manifest["ok"] is False
        assert manifest["errors"] == 1

    def test_dropped_series_are_counted_not_hidden(self) -> None:
        """The Perdue run said "status: OK" with 28 series dropped."""

        class DropsReach(FakeData):
            def execute(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
                if "type: REACH" in query:
                    self.request_count += 1
                    raise PulsarError("HTTP 500")
                return super().execute(query, variables)

        manifest, _, _ = run(spec(), data=DropsReach())
        assert manifest["ok"] is True  # no errors - the data that came is sound
        assert manifest["dropped"] == 3  # two searches + the folder rollup
        assert manifest["rollup"]["series_dropped"] == ["metric:REACH"]

    def test_a_dead_token_stops_everything_and_writes_no_manifest(self) -> None:
        target = FakeTarget()
        with pytest.raises(AuthError):
            pull.run(
                spec(),
                meta=FakeMeta(),
                data=FakeData(auth_on="query Series"),
                target=target,
                run_id=RUN_ID,
            )
        assert not any("manifest" in k for k in target.written())

    def test_invalid_specs_are_refused_before_any_call(self) -> None:
        for bad in (
            {"agency": "XX"},
            {"client_token": "Bad Name"},
            {"datasets": frozenset({"everything"})},
            {"folder": None},
            {"limit": 500},
            {"date_from": "2026-10-01T00:00:00Z"},
        ):
            with pytest.raises(ValueError):
                run(spec(**bad))


class TestScope:
    def test_only_search_narrows_to_one(self) -> None:
        manifest, _, _ = run(spec(only_search="2"))
        assert [r["search_id"] for r in manifest["searches"]] == ["2"]
        assert {"search_id": "1", "search_name": "Search 1"}.items() <= manifest[
            "skipped"
        ][0].items()
        assert manifest["rollup"] is None

    def test_datasets_subset(self) -> None:
        manifest, target, data = run(spec(datasets=frozenset({"mentions"})))
        assert target.rows("aggregates") == []
        assert all(r["mentions"]["rows_written"] == 1 for r in manifest["searches"])
        assert manifest["rollup"] is None
        assert data.request_count == 2  # one page per search, nothing else

    def test_all_history_starts_at_the_oldest_mention(self) -> None:
        data = FakeData(oldest="2024-11-05T08:00:00Z")
        manifest, _, _ = run(spec(all_history=True), data=data)
        window = manifest["searches"][0]["window"]
        assert window["from"] == "2024-11-05T00:00:00Z"
        assert window["from_basis"] == "oldest mention"
        oldest_calls = [
            f for f in data.filters if f.get("dateFrom") == pull.HISTORY_FLOOR
        ]
        assert len(oldest_calls) == 2  # one probe per search

    def test_a_late_start_date_never_narrows_the_window(self) -> None:
        """The Lovesac regression: startDate 2026-08-27 (reset by an edit)
        while mentions went back to June. The window must not shrink."""
        meta = FakeMeta([search(1, startDate="2026-08-27")])
        manifest, _, _ = run(spec(all_history=True), meta=meta)
        window = manifest["searches"][0]["window"]
        assert window["from"] == "2026-07-01T00:00:00Z"
        assert window["from_basis"] == "--from"

    def test_start_date_wins_when_the_probe_finds_nothing_earlier(self) -> None:
        manifest, _, _ = run(spec(all_history=True))
        window = manifest["searches"][0]["window"]
        assert window["from"] == "2025-03-01T00:00:00Z"
        assert window["from_basis"] == "search startDate"

    def test_a_failed_probe_falls_back_instead_of_failing(self) -> None:
        manifest, _, _ = run(spec(all_history=True), data=FakeData(oldest_fails=True))
        assert manifest["searches"][0]["window"]["from"] == "2025-03-01T00:00:00Z"
        assert manifest["ok"] is True

    def test_a_dead_token_on_the_probe_stops_everything(self) -> None:
        with pytest.raises(AuthError):
            run(spec(all_history=True), data=FakeData(auth_on="query Oldest"))

    def test_without_all_history_there_is_no_probe(self) -> None:
        _, _, data = run(spec())
        assert not any(f.get("dateFrom") == pull.HISTORY_FLOOR for f in data.filters)

    def test_skipped_searches_are_in_the_manifest(self) -> None:
        meta = FakeMeta([search(1), search(2, status="DRAFT")])
        manifest, _, _ = run(spec(), meta=meta)
        assert manifest["skipped"] == [
            {"search_id": "2", "search_name": "Search 2", "reason": "status DRAFT"}
        ]


class TestEstimate:
    def test_counts_every_dataset_plus_rollup(self) -> None:
        from pulsar import discovery, facets

        inv = discovery.discover(FakeMeta(), folder="Lovesac")
        est = pull.estimate(inv, spec())
        found, stats = facets.catalogue()
        per = 2 + len(found) + len(stats) + len(timeseries.plan()) + 1
        assert est["per_search"] == {"1": per, "2": per}
        assert est["rollup"] == len(found) + len(stats) + len(timeseries.plan())
        assert est["calls"] == 2 * per + est["rollup"]
        assert est["minutes"] > 0

    def test_all_history_adds_one_probe_per_search(self) -> None:
        from pulsar import discovery

        inv = discovery.discover(FakeMeta(), folder="Lovesac")
        base = pull.estimate(inv, spec())["calls"]
        assert pull.estimate(inv, spec(all_history=True))["calls"] == base + 2

    def test_the_probe_sort_is_real(self) -> None:
        from pulsar import facets

        types = {t["name"]: t for t in facets._load()}
        sort_by = {v["name"] for v in types["SortByEnum"]["enumValues"]}
        sort = {v["name"] for v in types["SortEnum"]["enumValues"]}
        assert "TIME" in sort_by
        assert "ASC" in sort

    def test_iso_day(self) -> None:
        import datetime as dt

        assert pull.iso_day("2026-01-02") == "2026-01-02T00:00:00Z"
        assert pull.iso_day(dt.date(2026, 1, 2)) == "2026-01-02T00:00:00Z"
        assert pull.iso_day("2026-01-02T05:00:00Z") == "2026-01-02T05:00:00Z"
