"""Audience reports: aggregate-only rows, and "no report" as a normal result."""

from __future__ import annotations

from typing import Any

from pulsar import audience, facets


def _types() -> dict[str, dict[str, Any]]:
    import json
    import pathlib

    path = pathlib.Path(facets.SCHEMA).with_name("trac_meta.json")
    raw = json.loads(path.read_text())
    schema = raw["data"]["__schema"] if "data" in raw else raw["__schema"]
    return {t["name"]: t for t in schema["types"]}


REPORT = {
    "id": 77,
    "name": "Q3 audience",
    "provider": "AUDIENSE",
    "status": "READY",
    "segmentation": "INTERCONNECTIONS",
    "dateFrom": "2026-06-01T00:00:00Z",
    "dateTo": "2026-09-01T00:00:00Z",
    "fullAudienceSize": 1000,
    "audiencePercentage": 0.4,
    "createdAt": "2026-09-02T00:00:00Z",
    "communityInfoSegments": [
        {
            "id": 1,
            "name": "Home decor fans",
            "size": 400,
            "sizeRel": 0.4,
            "bioKeywords": ["mom", "design"],
            "ages": [{"key": "25-34", "value": 0.5}],
            "genders": [{"key": "female", "value": 0.7}],
        }
    ],
}


class FakeMeta:
    def __init__(self, pages: list[dict[str, Any]]) -> None:
        self.pages = pages
        self.variables: list[dict[str, Any]] = []

    def execute(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        self.variables.append(variables)
        return {"audienceReports": self.pages[len(self.variables) - 1]}


class TestAgainstSchema:
    def test_selected_segment_fields_exist(self) -> None:
        seg = {f["name"] for f in _types()["CommunityInfoSegment"]["fields"]}
        assert set(audience.KV_SEGMENTS) | {"name", "size", "sizeRel"} <= seg

    def test_report_fields_exist(self) -> None:
        rep = {f["name"] for f in _types()["AudienceReport"]["fields"]}
        for name in ("fullAudienceSize", "dateFrom", "dateTo", "provider", "status"):
            assert name in rep

    def test_named_account_lists_are_not_selected(self) -> None:
        """brands/media/personalities name people and accounts - kept out
        for the same reason userNames is kept out of the facets."""
        for field in ("brands", "media", "personalities", "link", "summary"):
            assert f" {field} " not in audience.QUERY


class TestFlatten:
    def test_rows(self) -> None:
        rows = audience.flatten(REPORT)
        report = rows[0]
        assert report["facet"] == "audience.report"
        assert report["label"] == "77"
        assert report["label_type"] == "AUDIENSE"
        assert report["value"] == 1000

        seg = [r for r in rows if r["facet"] == "audience.segment"]
        assert {r["metric"] for r in seg} == {"size", "sizeRel"}
        assert seg[0]["parent_label"] == "77"

        ages = [r for r in rows if r["facet"] == "audience.ages"]
        assert ages[0]["label"] == "25-34"
        assert ages[0]["parent_label"] == "77/Home decor fans"

        kw = [r for r in rows if r["facet"] == "audience.bioKeywords"]
        assert [r["label"] for r in kw] == ["mom", "design"]
        assert kw[0]["value"] is None

    def test_every_row_carries_the_reports_own_window(self) -> None:
        for row in audience.flatten(REPORT):
            assert row["window_from"] == "2026-06-01T00:00:00Z"
            assert row["window_to"] == "2026-09-01T00:00:00Z"


class TestFetch:
    def test_no_report_is_a_result_not_an_error(self) -> None:
        meta = FakeMeta([{"nodes": [], "pageInfo": {"hasNextPage": False}}])
        result = audience.fetch(meta, search_id="1")
        assert result.status == "none"
        assert result.rows == []

    def test_pages_and_summarises(self) -> None:
        meta = FakeMeta(
            [
                {
                    "nodes": [REPORT],
                    "pageInfo": {"hasNextPage": True, "endCursor": "c1"},
                },
                {
                    "nodes": [dict(REPORT, id=78, status="FAILED"), None],
                    "pageInfo": {"hasNextPage": False},
                },
            ]
        )
        result = audience.fetch(meta, search_id="1")
        assert meta.variables[1]["after"] == "c1"
        assert result.status == "2 report(s), 1 ready"
        assert "communityInfoSegments" not in result.reports[0]
        assert result.rows
