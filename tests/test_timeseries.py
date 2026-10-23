"""Time series: what the dashboard plots over time.

The hand-written parts of this module (stat types, nested field names,
sentiment values) are checked against the real schema dump, because a typo
there is not a failing test - it is a GraphQL error on every run, or worse,
a series that silently never lands.
"""

from __future__ import annotations

from typing import Any

import pytest

from pulsar import facets, timeseries
from pulsar.client import AuthError, PulsarError


def _types() -> dict[str, dict[str, Any]]:
    return {t["name"]: t for t in facets._load()}


def _enum(name: str) -> set[str]:
    return {v["name"] for v in _types()[name]["enumValues"]}


class FakeClient:
    def __init__(self, fail: set[str] | None = None, auth: bool = False) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.fail = fail or set()
        self.auth = auth
        self.request_count = 0

    def execute(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        self.calls.append((query, variables))
        self.request_count += 1
        if self.auth:
            raise AuthError("HTTP 401 - token rejected")
        if any(f in query for f in self.fail):
            raise PulsarError("HTTP 500")
        point: dict[str, Any] = {
            "label": "2026-09-01T00:00:00Z",
            "value": 5.0,
            "metric": "x",
        }
        for name in timeseries.NESTED_SERIES:
            point[name] = [{"label": f"{name}-a", "value": 3.0, "sampleSize": 3}]
        return {"timeIntervals": [point]}


class TestAgainstSchema:
    def test_metric_series_are_real_stat_types(self) -> None:
        assert set(timeseries.METRIC_SERIES) <= _enum("StatEnum")

    def test_sentiments_are_real(self) -> None:
        assert set(timeseries.SENTIMENTS) == _enum("SentimentEnum")

    def test_granularities_are_real(self) -> None:
        assert set(timeseries.GRANULARITIES) == _enum("GranularityEnum")

    def test_nested_series_exist_on_time_interval_with_the_fields_we_select(
        self,
    ) -> None:
        types = _types()
        on_interval = {f["name"]: f for f in types["TimeInterval"]["fields"]}
        for name in timeseries.NESTED_SERIES:
            assert name in on_interval, name
            returns = facets._named(on_interval[name]["type"])
            declared = {f["name"] for f in types[str(returns)]["fields"]}
            assert set(facets.NESTED_FIELDS) <= declared, name
            opts = next(a for a in on_interval[name]["args"] if a["name"] == "options")
            opt_type = types[str(facets._named(opts["type"]))]
            assert "mincount" in {i["name"] for i in opt_type["inputFields"]}

    def test_time_interval_has_no_nested_sentiments(self) -> None:
        """The reason sentiment is three filtered calls. If the vendor adds
        it, this fails and the three calls can become one."""
        names = {f["name"] for f in _types()["TimeInterval"]["fields"]}
        assert "sentiments" not in names

    def test_time_intervals_accepts_stat_and_options(self) -> None:
        query = {f["name"]: f for f in _types()["Query"]["fields"]}
        args = {a["name"] for a in query["timeIntervals"]["args"]}
        assert {"filter", "options", "stat"} <= args


class TestPlanAndQuery:
    def test_plan_is_fifteen_uniquely_named_series_volume_first(self) -> None:
        plan = timeseries.plan()
        assert len(plan) == 15
        assert len({s.name for s in plan}) == 15
        assert plan[0] == timeseries.Series("volume", "volume", "")

    def test_metric_query_carries_the_stat(self) -> None:
        q = timeseries.build_query(timeseries.Series("m", "metric", "REACH"))
        assert "stat: {type: REACH, metric: SUM}" in q
        assert "granularity: DAY" in q

    def test_nested_query_asks_for_small_buckets(self) -> None:
        q = timeseries.build_query(timeseries.Series("n", "nested", "sources"))
        assert "sources(options: {mincount: 1})" in q
        assert "stat:" not in q

    def test_nested_without_mincount(self) -> None:
        q = timeseries.build_query(
            timeseries.Series("n", "nested", "sources"), mincount=None
        )
        assert "sources {" in q

    def test_bad_granularity_is_refused(self) -> None:
        with pytest.raises(ValueError, match="granularity"):
            timeseries.build_query(timeseries.plan()[0], granularity="YEAR")


class TestFetch:
    def test_every_series_is_one_request_with_the_shared_filter(self) -> None:
        client = FakeClient()
        _rows, failed = timeseries.fetch(
            client,
            search_ids=["1", "2"],
            date_from="2026-07-01T00:00:00Z",
            date_to="2026-09-29T00:00:00Z",
        )
        assert failed == []
        assert client.request_count == 15
        for _, variables in client.calls:
            assert variables["filter"]["searchIds"] == ["1", "2"]

    def test_sentiment_series_filter_on_sentiment(self) -> None:
        client = FakeClient()
        timeseries.fetch(
            client,
            search_ids=["1"],
            date_from="2026-07-01T00:00:00Z",
            date_to="2026-09-29T00:00:00Z",
        )
        sentiments = [v["filter"].get("sentiments") for _, v in client.calls[-3:]]
        assert sentiments == [["POSITIVE"], ["NEGATIVE"], ["NEUTRAL"]]

    def test_row_shapes(self) -> None:
        rows, _ = timeseries.fetch(
            FakeClient(),
            search_ids=["1"],
            date_from="2026-07-01T00:00:00Z",
            date_to="2026-09-29T00:00:00Z",
        )
        metric = [r for r in rows if r["metric"] == "REACH:SUM"]
        assert metric == [
            facets._row(
                "timeIntervals", "2026-09-01T00:00:00Z", 5.0, metric="REACH:SUM"
            )
        ]
        by_source = [r for r in rows if r["facet"] == "sources"]
        assert by_source[0]["parent_facet"] == "timeIntervals"
        assert by_source[0]["parent_label"] == "2026-09-01T00:00:00Z"
        assert by_source[0]["sample_size"] == 3
        positive = [r for r in rows if r["label"] == "POSITIVE"]
        assert positive[0]["facet"] == "sentiments"
        assert positive[0]["value"] == 5.0

    def test_volume_lands_once(self) -> None:
        """Volume comes from the volume series only - the nested series'
        parent values are not repeated."""
        rows, _ = timeseries.fetch(
            FakeClient(),
            search_ids=["1"],
            date_from="2026-07-01T00:00:00Z",
            date_to="2026-09-29T00:00:00Z",
        )
        volume = [
            r for r in rows if r["facet"] == "timeIntervals" and r["metric"] is None
        ]
        assert volume == [facets._row("timeIntervals", "2026-09-01T00:00:00Z", 5.0)]

    def test_a_failing_series_is_dropped_and_named(self) -> None:
        rows, failed = timeseries.fetch(
            FakeClient(fail={"topics"}),
            search_ids=["1"],
            date_from="2026-07-01T00:00:00Z",
            date_to="2026-09-29T00:00:00Z",
        )
        assert failed == ["by:topics"]
        assert rows

    def test_a_dead_token_stops_everything(self) -> None:
        with pytest.raises(AuthError):
            timeseries.fetch(
                FakeClient(auth=True),
                search_ids=["1"],
                date_from="2026-07-01T00:00:00Z",
                date_to="2026-09-29T00:00:00Z",
            )

    def test_junk_points_are_ignored(self) -> None:
        series = timeseries.Series("m", "metric", "REACH")
        assert timeseries.flatten({"timeIntervals": [None, "x"]}, series) == []
        assert timeseries.flatten({}, series) == []


class DayClient:
    """Answers each chunk with one point per day of [dateFrom, dateTo],
    INCLUSIVE of dateTo - the worst case for double counting."""

    def __init__(self) -> None:
        self.windows: list[tuple[str, str]] = []

    def execute(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        import datetime as dt

        f = variables["filter"]
        self.windows.append((f["dateFrom"], f["dateTo"]))
        day = dt.date.fromisoformat(f["dateFrom"][:10])
        end = dt.date.fromisoformat(f["dateTo"][:10])
        points = []
        while day <= end:
            point: dict[str, Any] = {"label": f"{day}T00:00:00Z", "value": 1.0}
            for name in timeseries.NESTED_SERIES:
                point[name] = [{"label": "x", "value": 1.0}]
            points.append(point)
            day += dt.timedelta(days=1)
        return {"timeIntervals": points}


class TestLongWindows:
    """MEASURED: DAY granularity is refused past ~90 days ("Cannot use DAY
    granularity on an interval of 177 days"). Long windows are chunked."""

    def test_short_window_is_one_chunk(self) -> None:
        assert timeseries.windows("2026-07-01T00:00:00Z", "2026-09-29T00:00:00Z") == [
            ("2026-07-01T00:00:00Z", "2026-09-29T00:00:00Z")
        ]

    def test_long_window_is_split_into_contiguous_chunks(self) -> None:
        chunks = timeseries.windows("2026-04-07T00:00:00Z", "2026-09-30T00:00:00Z")
        assert len(chunks) == 2
        assert chunks[0] == ("2026-04-07T00:00:00Z", "2026-07-06T00:00:00Z")
        assert chunks[1] == ("2026-07-06T00:00:00Z", "2026-09-30T00:00:00Z")

    def test_coarse_granularity_is_never_split(self) -> None:
        span = ("2025-01-01T00:00:00Z", "2026-09-30T00:00:00Z")
        assert timeseries.windows(*span, "WEEK") == [span]

    def test_every_day_once_across_chunks(self) -> None:
        """177 days, two chunks, and a vendor that includes dateTo: each
        day must still appear exactly once, and stay daily."""
        client = DayClient()
        rows, failed = timeseries.fetch(
            client,
            search_ids=["1"],
            date_from="2026-04-07T00:00:00Z",
            date_to="2026-10-01T00:00:00Z",
        )
        assert failed == []
        volume = [
            r["label"][:10]
            for r in rows
            if r["facet"] == "timeIntervals" and r["metric"] is None
        ]
        assert len(volume) == len(set(volume)) == 177
        assert volume[0] == "2026-04-07" and volume[-1] == "2026-09-30"
        by_source = [r["parent_label"][:10] for r in rows if r["facet"] == "sources"]
        assert len(by_source) == len(set(by_source)) == 177
        assert len(client.windows) == 15 * 2

    def test_a_series_failing_in_one_chunk_is_dropped_whole(self) -> None:
        class FailSecond(DayClient):
            def execute(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
                if "REACH" in query and variables["filter"]["dateFrom"] > "2026-06":
                    raise PulsarError("HTTP 500")
                return super().execute(query, variables)

        rows, failed = timeseries.fetch(
            FailSecond(),
            search_ids=["1"],
            date_from="2026-04-07T00:00:00Z",
            date_to="2026-10-01T00:00:00Z",
        )
        assert failed == ["metric:REACH"]
        assert not [r for r in rows if r["metric"] == "REACH:SUM"]

    def test_errors_are_reported_in_one_line(self) -> None:
        from pulsar.client import short_error

        body = (
            'GraphQL errors:\n[\n  {\n    "message": "Cannot use DAY granularity '
            'on an interval of 177 days. Please use a coarser granularity",\n'
            '    "extensions": {"traceId": "abc"}\n  }\n]'
        )
        assert short_error(PulsarError(body)) == (
            "Cannot use DAY granularity on an interval of 177 days. "
            "Please use a coarser granularity"
        )
        assert short_error(PulsarError("HTTP 500: boom\nmore")) == "HTTP 500: boom"
