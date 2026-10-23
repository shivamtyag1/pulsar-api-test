"""Folder -> searches.

The case these tests exist for: a brand's folder holding a project the
pipeline never pulls because nobody typed its id. Discovery must find every
search in the folder, say why it skipped any, and never resolve "Lovesac"
to some other folder that merely contains the word.
"""

from __future__ import annotations

from typing import Any

import pytest

from pulsar import discovery


def search(sid: int, **kw: Any) -> dict[str, Any]:
    node = {
        "id": sid,
        "name": f"Search {sid}",
        "status": "READY",
        "realtimeStatus": "STARTED",
        "historicStatus": "PRESENT",
        "type": "TRAC",
        "startDate": "2025-01-01",
        "endDate": None,
        "totalContents": 100,
        "folderIds": [9],
    }
    node.update(kw)
    return node


class FakeMeta:
    """Answers the discovery queries from a small in-memory account."""

    def __init__(
        self,
        folders: list[dict[str, Any]],
        in_folder: list[dict[str, Any]],
        by_id: dict[str, dict[str, Any]] | None = None,
        page_size: int = 50,
    ) -> None:
        self.folders = folders
        self.in_folder = in_folder
        self.by_id = by_id or {}
        self.page_size = page_size
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def _page(self, root: str, nodes: list[Any], after: str | None) -> dict[str, Any]:
        start = int(after or 0)
        chunk = nodes[start : start + self.page_size]
        more = start + self.page_size < len(nodes)
        return {
            root: {
                "nodes": chunk,
                "pageInfo": {
                    "hasNextPage": more,
                    "endCursor": str(start + self.page_size) if more else None,
                },
            }
        }

    def execute(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        self.calls.append((query, variables))
        if "query Folders" in query:
            return self._page("folders", self.folders, variables.get("after"))
        if "query Folder(" in query:
            match = [f for f in self.folders if str(f["id"]) == variables["id"]]
            return {"folder": match[0] if match else None}
        if variables.get("folderId"):
            return self._page("searches", self.in_folder, variables.get("after"))
        nodes = [self.by_id[i] for i in variables.get("ids") or [] if i in self.by_id]
        return self._page("searches", nodes, variables.get("after"))


LOVESAC = {"id": 9, "name": "Lovesac", "searchIds": ["1", "2", "3", "4"]}


class TestResolveFolder:
    def test_exact_name_case_insensitive(self) -> None:
        meta = FakeMeta([{"id": 8, "name": "Lovesac - Archive"}, LOVESAC], [])
        assert discovery.resolve_folder(meta, "lovesac")["id"] == 9

    def test_a_substring_match_is_not_good_enough(self) -> None:
        meta = FakeMeta([{"id": 8, "name": "Lovesac - Archive"}], [])
        with pytest.raises(discovery.DiscoveryError, match="Close matches"):
            discovery.resolve_folder(meta, "Lovesac")

    def test_nothing_at_all(self) -> None:
        with pytest.raises(discovery.DiscoveryError, match="No matches"):
            discovery.resolve_folder(FakeMeta([], []), "Nope")

    def test_two_folders_with_one_name_needs_the_id(self) -> None:
        meta = FakeMeta([LOVESAC, dict(LOVESAC, id=10)], [])
        with pytest.raises(discovery.DiscoveryError, match="Pass the id"):
            discovery.resolve_folder(meta, "Lovesac")

    def test_by_id(self) -> None:
        meta = FakeMeta([LOVESAC], [])
        assert discovery.resolve_folder(meta, "9")["name"] == "Lovesac"
        with pytest.raises(discovery.DiscoveryError, match="no folder with id"):
            discovery.resolve_folder(meta, "404")


class TestDiscover:
    def account(self) -> FakeMeta:
        return FakeMeta(
            [LOVESAC],
            in_folder=[
                search(1),
                search(2, realtimeStatus="STOPPED"),  # paused
                search(4, status="DRAFT"),
            ],
            # 3 is in the folder's searchIds but searches(folderId) left it
            # out - the archived-hidden-by-default case.
            by_id={"3": search(3, status="ARCHIVED"), "50": search(50)},
        )

    def test_every_search_in_the_folder_including_paused_and_archived(self) -> None:
        inv = discovery.discover(self.account(), folder="Lovesac")
        assert [s.id for s in inv.included] == ["1", "2", "3"]
        assert [(s.id, s.skip_reason) for s in inv.skipped] == [("4", "status DRAFT")]
        assert inv.missing == []

    def test_labels_show_state(self) -> None:
        inv = discovery.discover(self.account(), folder="Lovesac")
        labels = {s.id: s.label for s in inv.searches}
        assert labels["2"] == "Search 2 (2, paused)"
        assert labels["3"] == "Search 3 (3, archived)"
        assert labels["1"] == "Search 1 (1)"

    def test_pinned_ids_are_added_not_substituted(self) -> None:
        inv = discovery.discover(self.account(), folder="Lovesac", search_ids=["50"])
        assert "50" in [s.id for s in inv.included]
        assert "1" in [s.id for s in inv.included]

    def test_ids_the_token_cannot_see_are_reported(self) -> None:
        meta = self.account()
        meta.by_id.pop("3")
        inv = discovery.discover(meta, folder="Lovesac")
        assert inv.missing == ["3"]
        rows = inv.rows()
        missing = [r for r in rows if r["search_id"] == "3"]
        assert missing[0]["included"] is False
        assert "not visible" in missing[0]["skip_reason"]

    def test_empty_searches_are_skipped(self) -> None:
        meta = FakeMeta([LOVESAC], [search(1, totalContents=0)])
        inv = discovery.discover(meta, folder="Lovesac")
        assert inv.skipped[0].skip_reason == "no data (totalContents = 0)"

    def test_status_filter(self) -> None:
        inv = discovery.discover(
            self.account(), folder="Lovesac", statuses=["archived"]
        )
        assert [s.id for s in inv.included] == ["3"]
        assert any(s.skip_reason == "excluded by --status" for s in inv.skipped)

    def test_ids_only_without_a_folder(self) -> None:
        inv = discovery.discover(self.account(), search_ids=["50"])
        assert inv.folder is None
        assert [s.id for s in inv.included] == ["50"]

    def test_needs_something(self) -> None:
        with pytest.raises(discovery.DiscoveryError):
            discovery.discover(self.account())

    def test_paginates(self) -> None:
        meta = FakeMeta([LOVESAC], [search(i) for i in range(1, 8)], page_size=3)
        meta.folders[0] = dict(LOVESAC, searchIds=[str(i) for i in range(1, 8)])
        inv = discovery.discover(meta, folder="Lovesac")
        assert len(inv.included) == 7

    def test_inventory_rows_carry_the_folder(self) -> None:
        inv = discovery.discover(self.account(), folder="Lovesac")
        row = inv.rows()[0]
        assert row["folder_id"] == 9
        assert row["folder_name"] == "Lovesac"
        assert row["search_id"] == "1"
        assert row["search_name"] == "Search 1"

    def test_runaway_pagination_is_refused(self) -> None:
        class Loop:
            def execute(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
                return {
                    "folders": {
                        "nodes": [],
                        "pageInfo": {"hasNextPage": True, "endCursor": "same"},
                    }
                }

        with pytest.raises(discovery.DiscoveryError, match="refusing"):
            discovery.list_folders(Loop())


class TestAgainstSchema:
    """The hand-written selections, checked against schema/trac_meta.json."""

    @staticmethod
    def types() -> dict[str, dict[str, Any]]:
        import json
        import pathlib

        from pulsar import facets

        path = pathlib.Path(facets.SCHEMA).with_name("trac_meta.json")
        raw = json.loads(path.read_text())
        schema = raw["data"]["__schema"] if "data" in raw else raw["__schema"]
        return {t["name"]: t for t in schema["types"]}

    def test_search_fields_exist_on_the_search_interface(self) -> None:
        declared = {f["name"] for f in self.types()["Search"]["fields"]}
        assert set(discovery.SEARCH_FIELDS.split()) <= declared

    def test_folder_fields_exist(self) -> None:
        declared = {f["name"] for f in self.types()["Folder"]["fields"]}
        assert {"id", "name", "searchIds", "teamId", "isPublic"} <= declared

    def test_query_arguments_exist(self) -> None:
        query = {f["name"]: f for f in self.types()["Query"]["fields"]}

        def args(name: str) -> set[str]:
            return {a["name"] for a in query[name]["args"]}

        assert {"name", "first", "after"} <= args("folders")
        assert "id" in args("folder")
        assert {"folderId", "ids", "first", "after"} <= args("searches")
        assert {"searchId", "first", "after"} <= args("audienceReports")

    def test_skip_statuses_are_real(self) -> None:
        values = {v["name"] for v in self.types()["SearchStatusEnum"]["enumValues"]}
        assert values >= discovery.SKIP_STATUSES
