"""Folder -> searches: which Pulsar searches a brand's pull covers.

A brand's folder in Pulsar holds several searches (projects) - active,
paused (realtime STOPPED) and archived side by side. Hand-typing their ids
into the client map is how a new project silently goes unpulled, so the
folder is resolved live on every run instead.

Two lookups, both on `trac_meta`:

  folders(name:)             -> the folder's id and its `searchIds`
  searches(folderId:)        -> name, status, dates, totalContents per search

and a cross-check between them: any id in `Folder.searchIds` that
`searches(folderId:)` did not return is fetched by id, and anything still
missing is reported as "not visible to this token" rather than dropped
quietly. The two lists disagreeing is exactly the case where a silent
filter (archived searches hidden by default, say) would lose a project.

Which searches are pulled is a STATUS decision, made here and recorded per
search in the inventory:

  pulled     everything with data, whatever its realtime state - a paused
             or archived search still holds the history a dashboard shows
  skipped    DRAFT / DELETING / PERMANENTLY_DELETED (no data, or data on its
             way out), and totalContents == 0 (nothing to pull; ~55 calls
             saved per empty search)
"""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - typing only
    from collections.abc import Callable, Iterable, Sequence

    from pulsar.export import Executor

JsonDict = dict[str, Any]

PAGE_SIZE = 50
MAX_PAGES = 100  # 5,000 folders or searches - far beyond the account

SKIP_STATUSES = frozenset({"DRAFT", "DELETING", "PERMANENTLY_DELETED"})

SEARCH_FIELDS = (
    "id name status realtimeStatus historicStatus type "
    "startDate endDate totalContents folderIds"
)

FOLDERS = """
query Folders($name: String, $first: Int!, $after: String) {
  folders(name: $name, first: $first, after: $after) {
    pageInfo { hasNextPage endCursor }
    nodes { id name searchIds teamId isPublic }
  }
}
"""

FOLDER = """
query Folder($id: ID!) {
  folder(id: $id) { id name searchIds teamId isPublic }
}
"""

SEARCHES = f"""
query Searches($folderId: ID, $ids: [ID!], $first: Int!, $after: String) {{
  searches(folderId: $folderId, ids: $ids, first: $first, after: $after) {{
    pageInfo {{ hasNextPage endCursor }}
    nodes {{ {SEARCH_FIELDS} }}
  }}
}}
"""


class DiscoveryError(ValueError):
    """The folder could not be resolved to exactly one folder."""


@dataclasses.dataclass
class SearchInfo:
    """One search as the inventory records it."""

    id: str
    name: str | None
    status: str | None
    realtime_status: str | None
    historic_status: str | None
    type: str | None
    start_date: str | None
    end_date: str | None
    total_contents: int | None
    folder_ids: list[str]
    included: bool = True
    skip_reason: str | None = None

    @classmethod
    def from_node(cls, node: JsonDict) -> SearchInfo:
        return cls(
            id=str(node.get("id")),
            name=node.get("name"),
            status=node.get("status"),
            realtime_status=node.get("realtimeStatus"),
            historic_status=node.get("historicStatus"),
            type=node.get("type"),
            start_date=node.get("startDate"),
            end_date=node.get("endDate"),
            total_contents=node.get("totalContents"),
            folder_ids=[str(f) for f in node.get("folderIds") or []],
        )

    @property
    def label(self) -> str:
        """Human label for logs: `Lovesac - Brand (224320, paused)`."""
        state = "paused" if self.realtime_status == "STOPPED" else None
        state = "archived" if self.status == "ARCHIVED" else state
        tail = f", {state}" if state else ""
        return f"{self.name or '?'} ({self.id}{tail})"


@dataclasses.dataclass
class Inventory:
    """What a run covers: the folder (if any) and every search considered."""

    folder: JsonDict | None
    searches: list[SearchInfo]
    missing: list[str]  # ids the folder lists but the token cannot see

    @property
    def included(self) -> list[SearchInfo]:
        return [s for s in self.searches if s.included]

    @property
    def skipped(self) -> list[SearchInfo]:
        return [s for s in self.searches if not s.included]

    def rows(self) -> list[JsonDict]:
        """One JSON row per search, folder fields repeated on each."""
        folder = self.folder or {}
        out = []
        for s in self.searches:
            row = dataclasses.asdict(s)
            row["search_id"] = row.pop("id")
            row["search_name"] = row.pop("name")
            row["folder_id"] = folder.get("id")
            row["folder_name"] = folder.get("name")
            out.append(row)
        for sid in self.missing:
            out.append(
                {
                    "search_id": sid,
                    "folder_id": folder.get("id"),
                    "folder_name": folder.get("name"),
                    "included": False,
                    "skip_reason": "listed in folder but not visible to this token",
                }
            )
        return out


def paginate(
    client: Executor, query: str, root: str, variables: JsonDict
) -> list[JsonDict]:
    """Relay pagination to exhaustion, bounded."""
    nodes: list[JsonDict] = []
    after: str | None = None
    for _ in range(MAX_PAGES):
        data = client.execute(query, {**variables, "first": PAGE_SIZE, "after": after})
        block = data.get(root) or {}
        nodes.extend(n for n in block.get("nodes") or [] if isinstance(n, dict))
        info = block.get("pageInfo") or {}
        after = info.get("endCursor")
        if not info.get("hasNextPage") or not after:
            return nodes
    raise DiscoveryError(f"{root}: more than {MAX_PAGES} pages - refusing to loop")


def list_folders(client: Executor, name: str | None = None) -> list[JsonDict]:
    """Every folder the token can see, optionally filtered by the vendor's
    own name match (which may be fuzzy - see `resolve_folder`)."""
    return paginate(client, FOLDERS, "folders", {"name": name})


def resolve_folder(client: Executor, folder: str) -> JsonDict:
    """A folder by numeric id or exact (case-insensitive) name.

    Exact, not "best", match: the vendor's `name` argument may match
    substrings, and "Lovesac" silently resolving to "Lovesac - Archive" is
    a wall problem, not a convenience.
    """
    if folder.isdigit():
        data = client.execute(FOLDER, {"id": folder})
        node = data.get("folder")
        if not isinstance(node, dict):
            raise DiscoveryError(f"no folder with id {folder} visible to this token")
        return node

    candidates = list_folders(client, folder)
    exact = [
        f for f in candidates if str(f.get("name", "")).casefold() == folder.casefold()
    ]
    if len(exact) == 1:
        return exact[0]
    shown = ", ".join(f"{f.get('name')!r} (id {f.get('id')})" for f in candidates)
    if not exact:
        raise DiscoveryError(
            f"no folder named {folder!r}. "
            + (f"Close matches: {shown}" if shown else "No matches at all.")
            + " Pass the folder id, or run with --list-folders."
        )
    raise DiscoveryError(
        f"{len(exact)} folders are named {folder!r}: {shown}. Pass the id instead."
    )


def _classify(search: SearchInfo, statuses: Iterable[str] | None) -> None:
    """Decide included/skip_reason in place."""
    allowed = {s.upper() for s in statuses} if statuses else None
    if search.status in SKIP_STATUSES:
        search.included, search.skip_reason = False, f"status {search.status}"
    elif allowed is not None and (search.status or "").upper() not in allowed:
        search.included, search.skip_reason = False, "excluded by --status"
    elif search.total_contents == 0:
        search.included, search.skip_reason = False, "no data (totalContents = 0)"


def discover(
    client: Executor,
    *,
    folder: str | None = None,
    search_ids: Sequence[str] = (),
    statuses: Iterable[str] | None = None,
    progress: Callable[[str], None] | None = None,
) -> Inventory:
    """Resolve a folder and/or pinned ids to the searches a run covers.

    Pinned ids are added to the folder's, not a replacement for it - that is
    how a search that lives outside the brand's folder still gets pulled.
    """
    say = progress or (lambda _msg: None)
    if not folder and not search_ids:
        raise DiscoveryError("need a folder or at least one search id")

    folder_node: JsonDict | None = None
    by_id: dict[str, SearchInfo] = {}

    if folder:
        folder_node = resolve_folder(client, folder)
        say(f"  folder {folder_node.get('name')!r} (id {folder_node.get('id')})")
        for node in paginate(
            client, SEARCHES, "searches", {"folderId": str(folder_node["id"])}
        ):
            info = SearchInfo.from_node(node)
            by_id[info.id] = info

    wanted = [str(s) for s in (folder_node or {}).get("searchIds") or []]
    wanted += [str(s) for s in search_ids]
    unseen = [s for s in dict.fromkeys(wanted) if s not in by_id]
    if unseen:
        for node in paginate(client, SEARCHES, "searches", {"ids": unseen}):
            info = SearchInfo.from_node(node)
            by_id[info.id] = info

    missing = [s for s in unseen if s not in by_id]
    searches = sorted(by_id.values(), key=lambda s: int(s.id) if s.id.isdigit() else 0)
    for s in searches:
        _classify(s, statuses)

    inv = Inventory(folder=folder_node, searches=searches, missing=missing)
    say(
        f"  {len(inv.included)} search(es) to pull, {len(inv.skipped)} skipped"
        + (f", {len(missing)} not visible" if missing else "")
    )
    return inv
