"""Step 2 - find out what searches exist and how big they are.

We cannot pull TRAC data without a searchId, and totalContents tells us
whether the backfill is a 10-minute job or an hour-long one. This also
gives us the reconciliation baseline: if we later load fewer rows than
totalContents, something was dropped.

Output: out/searches.json
"""

import json
import pathlib
from typing import Any

from pulsar.client import PulsarClient, PulsarError

OUT = pathlib.Path("out")

# Corrected against schema/trac_meta.txt (real introspection, 14 Sep 2026).
# searches() returns a Relay-style SearchInterfaceConnection - cursor
# pagination via first/after, not a page number - and pageInfo tells us
# when to stop rather than comparing counts against totalCount.
SEARCHES = """
query Searches($first: Int!, $after: String) {
  searches(first: $first, after: $after) {
    totalCount
    nodes {
      id
      name
      status
      searchHash
      totalContents
      startDate
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

PAGE_SIZE = 50


def main() -> int:
    OUT.mkdir(exist_ok=True)
    collected: list[dict[str, Any]] = []

    try:
        with PulsarClient("trac_meta") as client:
            after = None
            page_no = 0
            while True:
                page_no += 1
                data = client.execute(SEARCHES, {"first": PAGE_SIZE, "after": after})
                block = data.get("searches") or {}
                nodes = block.get("nodes") or []
                collected.extend(nodes)

                total = block.get("totalCount")
                print(f"  page {page_no}: {len(nodes)} searches (total {total})")

                page_info = block.get("pageInfo") or {}
                if not page_info.get("hasNextPage"):
                    break
                after = page_info.get("endCursor")
                if not after:
                    break
    except PulsarError as exc:
        print(f"FAILED: {exc}")
        print("\nIf this is a field error, open schema/trac_meta.txt and")
        print("correct the SEARCHES query above to match the real schema.")
        return 1

    (OUT / "searches.json").write_text(json.dumps(collected, indent=2))

    print(f"\n{len(collected)} searches -> out/searches.json\n")
    print(f"{'ID':<12} {'STATUS':<12} {'MENTIONS':>12}  NAME")
    print("-" * 72)

    for s in sorted(collected, key=lambda x: x.get("totalContents") or 0, reverse=True):
        total = s.get("totalContents")
        print(
            f"{s.get('id')!s:<12} "
            f"{s.get('status')!s:<12} "
            f"{(f'{total:,}' if isinstance(total, int) else '?'):>12}  "
            f"{s.get('name')}"
        )

    print("\nPick the search for this client and use its ID in step 03.")
    print("Note the mention count - that sizes the backfill (see study §8.1).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
