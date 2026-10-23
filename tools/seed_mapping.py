"""Phase 1 - seed the search -> client mapping table.

Ijon's plan categorises searches by a `Client_Purpose_Other` naming
convention. That does not survive contact with the account: of 88 searches
only 3 names contain underscores (none as separators), 9 names are null,
and several are junk. So the mapping is a table VaynerX owns, and this
script produces the first draft of it for a human to finish.

It pulls every grouping signal the API exposes - team, folders, creator,
volume, status - and writes a CSV where the `client` / `agency` / `project`
columns are pre-filled where the evidence is unambiguous and left blank
where a person needs to decide.

Budget: 3-4 API calls (2 pages of searches + 1 of folders).

Output: out/search_client_map.seed.csv   <- review, fill in, then promote
        out/searches_full.json           <- raw discovery snapshot
        out/folders.json                 <- raw folder snapshot
"""

import csv
import json
import pathlib
from typing import Any

from pulsar.client import PulsarClient, PulsarError
from pulsar.mapping import guess_client, looks_like_junk

OUT = pathlib.Path("out")
PAGE_SIZE = 50

# Everything on the Search interface worth having for categorisation or
# sizing. The POC fetched 6 of these; the rest are free on the same call.
SEARCHES = """
query Searches($first: Int!, $after: String) {
  searches(first: $first, after: $after) {
    totalCount
    nodes {
      id
      name
      status
      type
      searchHash
      totalContents
      startDate
      endDate
      createdAt
      updatedAt
      lastEditedAt
      rtStartedAt
      rtStoppedAt
      realtimeStatus
      historicStatus
      visibility
      teamId
      teamName
      folderIds
      stats { historicContents realtimeContents totalContents }
      volume { day week month }
      user { id email firstname lastname }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

FOLDERS = """
query Folders($first: Int!, $after: String) {
  folders(first: $first, after: $after) {
    totalCount
    nodes {
      id
      name
      description
      teamId
      isPublic
      searchIds
      createdBy { id email }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""


def paginate(client: PulsarClient, query: str, key: str) -> list[dict[str, Any]]:
    collected, after, page = [], None, 0
    while True:
        page += 1
        data = client.execute(query, {"first": PAGE_SIZE, "after": after})
        block = data.get(key) or {}
        nodes = [n for n in (block.get("nodes") or []) if n]
        collected.extend(nodes)
        print(f"  {key} page {page}: {len(nodes)} (total {block.get('totalCount')})")

        info = block.get("pageInfo") or {}
        if not info.get("hasNextPage") or not info.get("endCursor"):
            break
        after = info["endCursor"]
    return collected


def main() -> int:
    OUT.mkdir(exist_ok=True)

    try:
        with PulsarClient("trac_meta") as client:
            print("discovering searches...")
            searches = paginate(client, SEARCHES, "searches")
            print("discovering folders...")
            try:
                folders = paginate(client, FOLDERS, "folders")
            except PulsarError as exc:
                # Folders are evidence, not a hard dependency.
                print(f"  folders unavailable ({exc}); continuing without them")
                folders = []
            calls = client.request_count
    except PulsarError as exc:
        print(f"FAILED: {exc}")
        return 1

    (OUT / "searches_full.json").write_text(json.dumps(searches, indent=2))
    (OUT / "folders.json").write_text(json.dumps(folders, indent=2))

    # search_id -> [folder names]
    folder_of: dict[str, list[str]] = {}
    for f in folders:
        for sid in f.get("searchIds") or []:
            folder_of.setdefault(str(sid), []).append(
                f.get("name") or f"folder:{f.get('id')}"
            )

    rows: list[dict[str, Any]] = []
    for s in searches:
        sid = str(s.get("id"))
        name = s.get("name")
        total = s.get("totalContents") or 0
        user = s.get("user") or {}
        volume = s.get("volume") or {}

        rows.append(
            {
                # --- to be filled in by a human ---
                "search_id": sid,
                "client": guess_client(name),
                "agency": "",
                "project": "",
                "backfill_from": "",
                "active": "",
                "mapped_by": "",
                "notes": "",
                # --- evidence, read-only ---
                "ev_name": name or "",
                "ev_status": s.get("status") or "",
                "ev_type": s.get("type") or "",
                "ev_total_contents": total,
                "ev_volume_day": volume.get("day") or 0,
                "ev_team_name": s.get("teamName") or "",
                "ev_team_id": s.get("teamId") or "",
                "ev_folders": "; ".join(folder_of.get(sid, [])),
                "ev_created_by": user.get("email") or "",
                "ev_start_date": s.get("startDate") or "",
                "ev_created_at": s.get("createdAt") or "",
                "ev_updated_at": s.get("updatedAt") or "",
                "ev_looks_like_junk": "YES" if looks_like_junk(name) else "",
            }
        )

    # Biggest and most active first - that is the order a human should
    # work through them in, because that is where the API budget goes.
    rows.sort(key=lambda r: (-r["ev_total_contents"], r["ev_name"]))

    path = OUT / "search_client_map.seed.csv"
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    with_data = [r for r in rows if r["ev_total_contents"] > 0]
    junk = [r for r in with_data if r["ev_looks_like_junk"]]
    guessed = [r for r in with_data if r["client"]]
    teams = {r["ev_team_name"] for r in rows if r["ev_team_name"]}
    foldered = [r for r in with_data if r["ev_folders"]]

    print(f"\n{len(rows)} searches -> {path}")
    print(f"{calls} API calls used\n")
    print(f"  with data (totalContents > 0):  {len(with_data)}")
    print(f"  client pre-filled by guess:     {len(guessed)}")
    print(f"  in at least one folder:         {len(foldered)}")
    print(f"  distinct teams seen:            {len(teams)} {sorted(teams) or '(none)'}")
    print(f"  name looks like a test/junk:    {len(junk)}")

    if with_data:
        top = with_data[:5]
        print("\n  biggest searches - these decide the backfill budget:")
        for r in top:
            flag = "  <-- looks like a test" if r["ev_looks_like_junk"] else ""
            print(f"    {r['ev_total_contents']:>10,}  {r['ev_name'][:40]:<40}{flag}")

    print("\nNEXT: open the CSV, fill in client / agency / project / backfill_from /")
    print("active for every row that matters, then promote it to the config path.")
    print("Rows left blank stay in quarantine and are never extracted.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
