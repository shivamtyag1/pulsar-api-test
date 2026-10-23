"""Facet catalogue and flattening.

Two things here are worth pinning hard. The PII gate, because the queries
it blocks look identical in shape to the ones it allows and a regression
would be invisible in the output. And the catalogue's selection rule,
because it is derived from the vendor schema rather than hand-listed - so
the test is what says the rule still picks the right queries after a
`make introspect`.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from pulsar import facets
from pulsar.client import PulsarError


class FakeClient:
    """Answers every call with an empty payload.

    Deliberately not a fixed list: the number of calls follows from the
    catalogue size and the batch size, and a test that hard-codes it breaks
    every time either changes - which is noise, not signal.
    """

    def __init__(self, payloads: list[dict[str, Any]] | None = None) -> None:
        self._payloads = payloads or []
        self.queries: list[str] = []
        self.request_count = 0

    def execute(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        self.queries.append(query)
        self.request_count += 1
        if self.request_count <= len(self._payloads):
            return self._payloads[self.request_count - 1]
        return {}


class TestCatalogue:
    def test_real_breakdowns_are_picked_up(self) -> None:
        found, _ = facets.catalogue()
        names = {f.name for f in found}
        for expected in ("genders", "countries", "languages", "bioKeywords", "topics"):
            assert expected in names

    def test_non_facets_are_rejected(self) -> None:
        """`results` and `topContent` return mentions, `validateFilter`
        returns a verdict, `userData` is an author roster. All four take a
        filter, so only the label/value rule keeps them out."""
        found, _ = facets.catalogue(include_pii=True)
        names = {f.name for f in found}
        for rejected in (
            "results",
            "topContent",
            "resultsByOutletName",
            "validateFilter",
            "sidebarSummary",
            "userData",
        ):
            assert rejected not in names

    def test_queries_needing_a_choice_are_skipped(self) -> None:
        """A segment or dimension is a judgement the module cannot make."""
        found, _ = facets.catalogue()
        names = {f.name for f in found}
        for skipped in ("wordCloudCluster", "resultsBy", "summary", "clustering"):
            assert skipped not in names

    def test_selection_only_asks_for_fields_the_type_has(self) -> None:
        """The live failure this prevents: `sampleSize` exists on BioKeyword
        but not on Gender, and asking for it anyway fails the whole batch."""
        found, _ = facets.catalogue()
        by_name = {f.name: f for f in found}
        assert "sampleSize" in by_name["bioKeywords"].fields
        assert "sampleSize" not in by_name["genders"].fields
        assert by_name["countries"].fields == ("label", "value")

    def test_age_is_reachable_only_under_gender(self) -> None:
        """Pulsar has no top-level ages query, so without the nesting the
        age split cannot be retrieved at all."""
        found, _ = facets.catalogue()
        assert {f.name for f in found if f.nested} == {"genders"}
        assert next(f for f in found if f.name == "genders").nested == "userAge"


class TestPiiGate:
    def test_author_name_facets_are_excluded_by_default(self) -> None:
        """These are `{label, value}` like every safe facet - but the label
        is a person, so grouping produces a roster, not an aggregate."""
        found, _ = facets.catalogue()
        names = {f.name for f in found}
        assert "userNames" not in names
        assert "userRealNames" not in names

    def test_the_flag_admits_them(self) -> None:
        default = {f.name for f in facets.catalogue()[0]}
        opted_in = {f.name for f in facets.catalogue(include_pii=True)[0]}
        assert opted_in - default == {"userNames", "userRealNames"}

    def test_user_data_is_never_emitted(self) -> None:
        """Not even with the flag: it is a paginated list of userIdentifier
        and userScreenName, two of the twelve fields the mention export
        strips. It fails the label/value rule and stays out."""
        assert "userData" not in {f.name for f in facets.catalogue(include_pii=True)[0]}


class TestStats:
    def test_scores_average_and_volumes_sum(self) -> None:
        """Summing a 0-100 score produces a number that renders fine and
        means nothing."""
        _, stats = facets.catalogue()
        by_name = {s.name: s.metric for s in stats}
        assert by_name["reach"] == "SUM"
        assert by_name["mediaImpressions"] == "SUM"
        assert by_name["reputation"] == "AVG"
        assert by_name["credibilityScore"] == "AVG"

    def test_users_uses_its_own_enum(self) -> None:
        """`users` takes UniqueEnum, not MetricEnum - SUM would be rejected."""
        _, stats = facets.catalogue()
        assert next(s for s in stats if s.name == "users").metric == "UNIQUE"


class TestQueryBuilding:
    def test_a_batch_is_one_document_sharing_one_filter(self) -> None:
        found, _ = facets.catalogue()
        query = facets.build_facet_query(found[:3])
        assert query.count("$filter") == 4  # one declaration + three uses
        assert query.startswith("query Facets($filter: FilterInput!)")

    def test_nesting_is_expanded(self) -> None:
        genders = next(f for f in facets.catalogue()[0] if f.name == "genders")
        query = facets.build_facet_query([genders])
        assert "userAge { label value sampleSize overIndex }" in query

    def test_limit_is_applied_when_given(self) -> None:
        found, _ = facets.catalogue()
        assert "options: {limit: 50}" in facets.build_facet_query(found[:1], limit=50)
        assert "options" not in facets.build_facet_query(found[:1])

    def test_stat_queries_carry_their_metric(self) -> None:
        _, stats = facets.catalogue()
        query = facets.build_stat_query(stats)
        assert "reach(filter: $filter, metric: SUM)" in query
        assert "users(filter: $filter, metric: UNIQUE)" in query


class TestFlatten:
    def test_buckets_become_one_row_each(self) -> None:
        rows = facets.flatten(
            {"countries": [{"label": "US", "value": 1973.0}]},
            [facets.Facet("countries", ("label", "value"))],
        )
        assert rows == [
            {
                "facet": "countries",
                "label": "US",
                "label_type": None,
                "value": 1973.0,
                "metric": None,
                "sample_size": None,
                "over_index": None,
                "parent_facet": None,
                "parent_label": None,
            }
        ]

    def test_nested_rows_carry_their_parent(self) -> None:
        """Without the parent the age buckets are unattributable - "25 = 94"
        is meaningless unless you know it is 94 men."""
        rows = facets.flatten(
            {
                "genders": [
                    {
                        "label": "male",
                        "value": 619.0,
                        "userAge": [
                            {"label": "25", "value": 94.0, "sampleSize": 300},
                        ],
                    }
                ]
            },
            [facets.Facet("genders", ("label", "value"), nested="userAge")],
        )
        assert len(rows) == 2
        child = rows[1]
        assert child["facet"] == "userAge"
        assert child["parent_facet"] == "genders"
        assert child["parent_label"] == "male"
        assert child["sample_size"] == 300

    def test_a_stat_becomes_a_labelless_row(self) -> None:
        rows = facets.flatten({"reach": {"metric": "SUM", "value": 42.0}}, [])
        assert rows == [
            {
                "facet": "reach",
                "label": None,
                "label_type": None,
                "value": 42.0,
                "metric": "SUM",
                "sample_size": None,
                "over_index": None,
                "parent_facet": None,
                "parent_label": None,
            }
        ]

    def test_an_empty_facet_contributes_nothing(self) -> None:
        """A facet Pulsar has no data for must not become a phantom row."""
        assert facets.flatten({"cities": [], "states": None}, []) == []

    def test_rows_are_json_serialisable(self) -> None:
        rows = facets.flatten(
            {"genders": [{"label": "female", "value": 265.0}]},
            [facets.Facet("genders", ("label", "value"))],
        )
        json.dumps(rows)

    def test_every_row_has_the_same_keys(self) -> None:
        """A loader lands these as columns; a ragged row shifts them."""
        rows = facets.flatten(
            {
                "genders": [
                    {
                        "label": "male",
                        "value": 1.0,
                        "userAge": [{"label": "25", "value": 1.0}],
                    }
                ],
                "reach": {"metric": "SUM", "value": 9.0},
            },
            [facets.Facet("genders", ("label", "value"), nested="userAge")],
        )
        assert len({tuple(sorted(r)) for r in rows}) == 1
        assert len(rows) == 3


class TestFetch:
    def test_every_facet_and_every_stat_gets_a_request(self) -> None:
        found, stats = facets.catalogue()
        expected = -(-len(found) // facets.DEFAULT_BATCH) + len(stats)
        client = FakeClient()
        _rows, _failed = facets.fetch(
            client,
            search_id="176652",
            date_from="2026-01-01T00:00:00Z",
            date_to="2026-09-26T00:00:00Z",
        )
        assert client.request_count == expected
        assert "query Stats" in client.queries[-1]

    def test_cost_does_not_depend_on_search_size(self) -> None:
        """The property that makes this worth running daily: aggregates are
        computed vendor-side, so a 3M-mention search costs the same as a
        3k one."""
        found, _ = facets.catalogue()
        calls = -(-len(found) // facets.DEFAULT_BATCH) + 1
        # ~40s at the measured 0.80 req/s, for a search of any size.
        assert calls <= 40

    def test_the_filter_names_the_search_and_window(self) -> None:
        captured: list[dict[str, Any]] = []

        class Capturing(FakeClient):
            def execute(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
                captured.append(variables)
                return super().execute(query, variables)

        client = Capturing()
        _rows, _failed = facets.fetch(
            client,
            search_id="224320",
            date_from="2026-01-01T00:00:00Z",
            date_to="2026-09-26T00:00:00Z",
        )
        assert all(v["filter"]["searchIds"] == ["224320"] for v in captured)
        assert all(v["filter"]["dateTo"] == "2026-09-26T00:00:00Z" for v in captured)

    def test_several_ids_are_one_aggregated_filter(self) -> None:
        """The folder rollup: the vendor aggregates across the list, which
        is the only way to get a folder-wide distinct author count."""
        captured: list[dict[str, Any]] = []

        class Capturing(FakeClient):
            def execute(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
                captured.append(variables)
                return super().execute(query, variables)

        facets.fetch(Capturing(), search_ids=["1", "2"], date_from="a", date_to="b")
        assert all(v["filter"]["searchIds"] == ["1", "2"] for v in captured)

    def test_needs_an_id(self) -> None:
        with pytest.raises(ValueError, match="search_id"):
            facets.fetch(FakeClient(), date_from="a", date_to="b")

    @pytest.mark.parametrize("where", ["query Facets", "query Stats"])
    def test_a_dead_token_is_not_reported_as_dropped_facets(self, where: str) -> None:
        from pulsar.client import AuthError

        class Dead(FakeClient):
            def execute(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
                if where in query:
                    raise AuthError("HTTP 401 - token rejected")
                return super().execute(query, variables)

        with pytest.raises(AuthError):
            facets.fetch(Dead(), search_id="1", date_from="a", date_to="b")

    @pytest.mark.parametrize("include_pii", [False, True])
    def test_pii_choice_reaches_the_wire(self, include_pii: bool) -> None:
        client = FakeClient()
        _rows, _failed = facets.fetch(
            client,
            search_id="176652",
            date_from="2026-01-01T00:00:00Z",
            date_to="2026-09-26T00:00:00Z",
            include_pii=include_pii,
        )
        asked = "userNames" in " ".join(client.queries)
        assert asked is include_pii


class TestHandWrittenBitsAgainstTheSchema:
    """The catalogue derives its own field lists, so those cannot drift.
    NESTED, NESTED_FIELDS and STAT_METRIC are hand-written and can - and a
    wrong one fails the whole batch at runtime, which is how the first
    version of this module broke (`sampleSize` asked for on `Gender`).
    """

    @staticmethod
    def _schema() -> dict[str, Any]:
        raw = json.loads(facets.SCHEMA.read_text())
        node = raw["data"]["__schema"] if "data" in raw else raw["__schema"]
        return {t["name"]: t for t in node["types"]}

    @staticmethod
    def _named(ref: dict[str, Any]) -> str | None:
        while ref.get("ofType"):
            ref = ref["ofType"]
        return ref.get("name")

    def test_every_nested_field_exists_on_its_parent(self) -> None:
        by_name = self._schema()
        for parent, child in facets.NESTED.items():
            query_field = next(
                f for f in by_name["Query"]["fields"] if f["name"] == parent
            )
            parent_type = by_name[self._named(query_field["type"]) or ""]
            assert child in {f["name"] for f in parent_type["fields"]}, (
                f"{parent}.{child} is not in the schema"
            )

    def test_every_nested_selection_exists_on_the_nested_type(self) -> None:
        by_name = self._schema()
        for parent, child in facets.NESTED.items():
            query_field = next(
                f for f in by_name["Query"]["fields"] if f["name"] == parent
            )
            parent_type = by_name[self._named(query_field["type"]) or ""]
            child_field = next(f for f in parent_type["fields"] if f["name"] == child)
            child_type = by_name[self._named(child_field["type"]) or ""]
            available = {f["name"] for f in child_type["fields"]}
            missing = set(facets.NESTED_FIELDS) - available
            assert not missing, f"{child} has no {missing}"

    def test_every_stat_metric_is_a_member_of_that_query_s_enum(self) -> None:
        """`users` takes UniqueEnum and the rest take MetricEnum; sending
        SUM to `users` is rejected by the server, not by us."""
        by_name = self._schema()
        _, stats = facets.catalogue()
        for stat in stats:
            query_field = next(
                f for f in by_name["Query"]["fields"] if f["name"] == stat.name
            )
            arg = next(a for a in query_field["args"] if a["name"] == "metric")
            enum = by_name[self._named(arg["type"]) or ""]
            allowed = {e["name"] for e in enum["enumValues"]}
            assert stat.metric in allowed, (
                f"{stat.name} metric {stat.metric} not in {enum['name']} {allowed}"
            )

    def test_every_selected_field_exists_on_its_return_type(self) -> None:
        by_name = self._schema()
        found, _ = facets.catalogue(include_pii=True)
        for facet in found:
            query_field = next(
                f for f in by_name["Query"]["fields"] if f["name"] == facet.name
            )
            rtype = by_name[self._named(query_field["type"]) or ""]
            available = {f["name"] for f in rtype["fields"]}
            missing = set(facet.fields) - available
            assert not missing, f"{facet.name} has no {missing}"


class TestDegradation:
    """A batch the vendor refuses must cost the run that batch, not the run.

    Facets are independent - losing `domains` does not corrupt `genders` -
    so partial is safe here in a way it is not for the mention pull. What
    is unacceptable is losing one quietly.
    """

    class Flaky(FakeClient):
        """500s on any document mentioning `poison`, like the real vendor
        does when a batch is too expensive to compute."""

        def __init__(self, poison: str) -> None:
            super().__init__([])
            self.poison = poison

        def execute(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
            self.queries.append(query)
            self.request_count += 1
            if self.poison in query:
                raise PulsarError("gave up after 5 attempts")
            return {}

    def test_one_bad_facet_does_not_lose_the_others(self) -> None:
        client = self.Flaky("domains")
        rows, failed = facets.fetch(
            client,
            search_id="176652",
            date_from="2026-01-01T00:00:00Z",
            date_to="2026-09-26T00:00:00Z",
        )
        assert "domains" in failed
        # Only the offender and whatever shares its exact name are dropped;
        # the rest of the catalogue still ran.
        assert len(failed) < 4
        assert isinstance(rows, list)

    def test_a_failing_batch_halves_itself(self) -> None:
        """Only reachable when a caller raises batch_size above the default
        of 1 - but it must still work, because that is the escape hatch for
        anyone who decides the call saving is worth the risk."""
        client = self.Flaky("domainsCredibility")
        _rows, failed = facets.fetch(
            client,
            search_id="176652",
            date_from="2026-01-01T00:00:00Z",
            date_to="2026-09-26T00:00:00Z",
            batch_size=4,
        )
        found, _ = facets.catalogue()
        clean = -(-len(found) // 4) + 1
        assert client.request_count > clean  # halving costs extra calls
        assert "domainsCredibility" in failed

    def test_stats_failing_does_not_lose_the_facets(self) -> None:
        client = self.Flaky("query Stats")
        rows, failed = facets.fetch(
            client,
            search_id="176652",
            date_from="2026-01-01T00:00:00Z",
            date_to="2026-09-26T00:00:00Z",
        )
        _, stats = facets.catalogue()
        assert set(failed) == {s.name for s in stats}
        assert isinstance(rows, list)


class TestBucketSuppression:
    """The server applies undocumented limit/mincount defaults, so a pull
    that does not set them cannot distinguish an absent bucket from a
    suppressed one. Both have to reach the wire when asked for."""

    def test_mincount_reaches_the_query(self) -> None:
        found, _ = facets.catalogue()
        query = facets.build_facet_query(found[:1], mincount=1)
        assert "mincount: 1" in query

    def test_mincount_zero_is_not_swallowed_as_falsey(self) -> None:
        found, _ = facets.catalogue()
        assert "mincount: 0" in facets.build_facet_query(found[:1], mincount=0)

    def test_both_options_combine(self) -> None:
        found, _ = facets.catalogue()
        query = facets.build_facet_query(found[:1], limit=100, mincount=1)
        assert "options: {limit: 100, mincount: 1}" in query

    def test_neither_means_no_options_block(self) -> None:
        found, _ = facets.catalogue()
        assert "options" not in facets.build_facet_query(found[:1])

    def test_fetch_passes_mincount_to_the_facets_that_take_it(self) -> None:
        client = FakeClient()
        facets.fetch(
            client,
            search_id="176652",
            date_from="2026-01-01T00:00:00Z",
            date_to="2026-09-26T00:00:00Z",
            mincount=1,
        )
        accepting = {f.name for f in facets.catalogue()[0] if "mincount" in f.accepts}
        assert accepting  # the rule is worth nothing if it matches nothing
        for query in (q for q in client.queries if q.startswith("query Facets")):
            name = query.splitlines()[1].strip().split("(")[0]
            assert ("mincount: 1" in query) is (name in accepting), name


class TestLimitCeiling:
    """The vendor's limit handling is backwards: an explicit value is
    rejected above 100, while omitting it returns 250. So asking for a
    limit can only ever narrow the result, never widen it."""

    def test_over_the_ceiling_is_refused_before_the_call(self) -> None:
        found, _ = facets.catalogue()
        with pytest.raises(ValueError, match="must be <= 100"):
            facets.build_facet_query(found[:1], limit=facets.MAX_LIMIT + 1)

    def test_the_ceiling_itself_is_allowed(self) -> None:
        found, _ = facets.catalogue()
        assert "limit: 100" in facets.build_facet_query(
            found[:1], limit=facets.MAX_LIMIT
        )

    def test_the_implicit_default_exceeds_the_explicit_ceiling(self) -> None:
        """Documenting the surprise, so nobody 'fixes' the default by
        setting limit=100 and silently loses 150 buckets."""
        assert facets.IMPLICIT_LIMIT > facets.MAX_LIMIT


class TestMincountDefault:
    """Measured on a real search: default mincount gave `cities` ONE bucket
    where mincount=1 gave 139. Shipping the default would have put a city
    breakdown in front of a client showing New York as the only city."""

    def test_fetch_asks_for_every_bucket_by_default(self) -> None:
        client = FakeClient()
        facets.fetch(
            client,
            search_id="176652",
            date_from="2026-01-01T00:00:00Z",
            date_to="2026-09-26T00:00:00Z",
        )
        accepting = {f.name for f in facets.catalogue()[0] if "mincount" in f.accepts}
        asked = {
            q.splitlines()[1].strip().split("(")[0]
            for q in client.queries
            if f"mincount: {facets.DEFAULT_MINCOUNT}" in q
        }
        assert asked == accepting

    def test_the_default_is_one_not_absent(self) -> None:
        """None would mean 'let the server decide', which is the behaviour
        that silently truncated the breakdown."""
        assert facets.DEFAULT_MINCOUNT == 1

    def test_it_can_still_be_raised_deliberately(self) -> None:
        """Small-cell suppression is a legitimate reason to raise it."""
        client = FakeClient()
        facets.fetch(
            client,
            search_id="176652",
            date_from="2026-01-01T00:00:00Z",
            date_to="2026-09-26T00:00:00Z",
            mincount=5,
        )
        assert any("mincount: 5" in q for q in client.queries)
        assert not any("mincount: 1" in q for q in client.queries)


class TestTimeIntervalsIsOwnedByTimeseries:
    def test_not_in_the_catalogue(self) -> None:
        """Left in, it has no granularity to set and the vendor silently
        answers weekly on a long window. pulsar.timeseries pulls it."""
        names = {f.name for f in facets.catalogue()[0]}
        assert "timeIntervals" not in names
        assert "velocity" in names


class TestOptionsAreNotUniform:
    """The 31 facets are spread across five different options input types.
    Sending `mincount` to one that does not declare it is a hard GraphQL
    error (`argumentNotAccepted`), not an ignored argument - so which
    options each facet takes has to be read from the schema, per facet.
    """

    def test_facets_disagree_about_which_options_they_take(self) -> None:
        by_name = {f.name: f for f in facets.catalogue()[0]}
        assert "mincount" in by_name["genders"].accepts
        assert "mincount" not in by_name["contentTypes"].accepts  # MultipleOptions
        assert "mincount" not in by_name["domainsCredibility"].accepts  # ResultsBy
        assert by_name["velocity"].accepts == frozenset()  # neither

    def test_mincount_is_omitted_where_it_is_not_accepted(self) -> None:
        by_name = {f.name: f for f in facets.catalogue()[0]}
        query = facets.build_facet_query(
            [by_name["genders"], by_name["contentTypes"], by_name["velocity"]],
            mincount=1,
        )
        lines = {
            line.strip().split("(")[0]: line
            for line in query.splitlines()
            if "(" in line
        }
        assert "mincount: 1" in lines["genders"]
        assert "mincount" not in lines["contentTypes"]
        assert "options" not in lines["velocity"]

    def test_limit_is_omitted_where_it_is_not_accepted(self) -> None:
        by_name = {f.name: f for f in facets.catalogue()[0]}
        query = facets.build_facet_query([by_name["velocity"]], limit=50)
        assert "options" not in query

    def test_every_accepted_option_really_is_on_that_input_type(self) -> None:
        """Guards the derivation itself against a schema refresh."""
        raw = json.loads(facets.SCHEMA.read_text())
        node = raw["data"]["__schema"] if "data" in raw else raw["__schema"]
        by_name = {t["name"]: t for t in node["types"]}

        def named(ref: dict[str, Any]) -> str | None:
            while ref.get("ofType"):
                ref = ref["ofType"]
            return ref.get("name")

        for facet in facets.catalogue(include_pii=True)[0]:
            field = next(
                f for f in by_name["Query"]["fields"] if f["name"] == facet.name
            )
            arg = next((a for a in field["args"] if a["name"] == "options"), None)
            declared = (
                {i["name"] for i in by_name[named(arg["type"]) or ""]["inputFields"]}
                if arg
                else set()
            )
            assert facet.accepts <= declared, f"{facet.name} claims {facet.accepts}"


class TestStatsAreRequestedSeparately:
    def test_one_request_per_stat(self) -> None:
        """Ten in one document returns HTTP 500, and a single document
        means one bad metric takes the other nine with it."""
        client = FakeClient()
        facets.fetch(
            client,
            search_id="176652",
            date_from="2026-01-01T00:00:00Z",
            date_to="2026-09-26T00:00:00Z",
        )
        stat_queries = [q for q in client.queries if q.startswith("query Stats")]
        _, stats = facets.catalogue()
        assert len(stat_queries) == len(stats)
        assert all(q.count("(filter:") == 1 for q in stat_queries)

    def test_one_failing_stat_does_not_drop_the_rest(self) -> None:
        client = TestDegradation.Flaky("reputation")
        _rows, failed = facets.fetch(
            client,
            search_id="176652",
            date_from="2026-01-01T00:00:00Z",
            date_to="2026-09-26T00:00:00Z",
        )
        _, stats = facets.catalogue()
        assert failed == ["reputation"]
        assert len(failed) < len(stats)


class TestStructuredLabels:
    """`entityNames` labels buckets with an object, not a string. Left
    alone it lands in the column as a stringified Python dict - single
    quotes and all, so not even parseable back as JSON."""

    def test_an_object_label_is_split_into_name_and_type(self) -> None:
        rows = facets.flatten(
            {
                "entityNames": [
                    {
                        "label": {"type": "ORGANIZATION", "name": "lovesac"},
                        "value": 2170.0,
                    }
                ]
            },
            [facets.Facet("entityNames", ("label", "value"))],
        )
        assert rows[0]["label"] == "lovesac"
        assert rows[0]["label_type"] == "ORGANIZATION"

    def test_a_plain_label_is_untouched(self) -> None:
        rows = facets.flatten(
            {"countries": [{"label": "US", "value": 1.0}]},
            [facets.Facet("countries", ("label", "value"))],
        )
        assert rows[0]["label"] == "US"
        assert rows[0]["label_type"] is None

    def test_an_unexpected_object_survives_as_json(self) -> None:
        """A shape we have not seen must not crash the run, and must stay
        parseable rather than becoming repr() output."""
        rows = facets.flatten(
            {"topics": [{"label": {"a": 1}, "value": 1.0}]},
            [facets.Facet("topics", ("label", "value"))],
        )
        assert json.loads(rows[0]["label"]) == {"a": 1}

    def test_every_row_still_has_identical_keys(self) -> None:
        rows = facets.flatten(
            {
                "entityNames": [{"label": {"type": "X", "name": "y"}, "value": 1.0}],
                "countries": [{"label": "US", "value": 2.0}],
                "reach": {"metric": "SUM", "value": 3.0},
            },
            [facets.Facet("entityNames", ("label", "value"))],
        )
        assert len({tuple(sorted(r)) for r in rows}) == 1


def test_facet_rows_roll_far_later_than_mention_rows() -> None:
    """A facet row is 274 bytes against a mention row's 4,241. Rolling both
    at the same count would split a client's breakdowns across files for no
    reason - the whole point is one object per client."""
    from pulsar.sink import DEFAULT_ROWS_PER_FILE

    assert facets.FACET_ROWS_PER_FILE > DEFAULT_ROWS_PER_FILE
    part_mb = facets.FACET_ROWS_PER_FILE * 274 / 1e6
    assert part_mb < 50, f"a facets part is {part_mb:.0f} MB"
