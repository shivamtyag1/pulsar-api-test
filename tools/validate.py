"""Phase 0 - prove the two assumptions the production design rests on.

The incremental contract in the plan assumes `updatedFrom` filters on the
same clock as the returned `updatedAt`, and that `pulsarId` is a usable
primary key. Both are unverified. If either is wrong the design changes,
so they get tested before anything is built on them.

Budget: 4 API calls total.

  test 1  full field set          - does requesting all 123 fields work?   (free, shares call with test 2)
  test 2  updatedFrom semantics   - does the watermark filter actually work? (2 calls)
  test 3  pulsarId across searches - is the PK `pulsarId` or (search, pulsarId)? (2 calls)

Usage:
  python tools/validate.py              # all tests
  python tools/validate.py --test 2     # one test
"""

import argparse
import datetime as dt
import json
import pathlib
import statistics
from typing import Any

from pulsar.client import PulsarClient, PulsarError
from pulsar.fields import results_selection

OUT = pathlib.Path("out")

# Kotex Rush Rally - 42 mentions, one page. Small enough that a boundary
# test costs nothing and we already know its true total from the UI.
SMALL_SEARCH = "226781"

# Two searches on the same topic with overlapping date ranges. If one piece
# of content can sit in two searches, these are where it shows up.
OVERLAP_A = "221479"  # Google Australia
OVERLAP_B = "221712"  # Google Australia - Test


def build_query() -> str:
    return f"""
query Results($filter: FilterInput!, $options: ResultsOptionsInput) {{
  results(filter: $filter, options: $options) {{
    total
    nextCursor
    results {{
        {results_selection()}
    }}
  }}
}}
"""


def fetch(
    client: PulsarClient, query: str, filt: dict[str, Any], options: dict[str, Any]
) -> dict[str, Any]:
    data = client.execute(query, {"filter": filt, "options": options})
    return data.get("results") or {}


def test_field_set_and_updated_from(client: PulsarClient, query: str) -> dict[str, Any]:
    """Tests 1 and 2. Two calls."""
    print("\n" + "=" * 68)
    print("TEST 1+2  full field set, then updatedFrom boundary")
    print("=" * 68)

    window = {
        "searchIds": [SMALL_SEARCH],
        "dateFrom": "2026-01-01T00:00:00Z",
        "dateTo": "2026-12-31T00:00:00Z",
    }

    # --- Test 1: does the full 123-field selection even parse? ---
    block = fetch(client, query, window, {"limit": 100})
    rows = block.get("results") or []
    total = block.get("total")

    print(f"\n[test 1] full field set: OK - {len(rows)} rows, vendor total {total}")
    if not rows:
        print("  no rows returned; cannot continue to test 2")
        return {"test1": "PASS (no rows)", "test2": "SKIPPED"}

    populated = sum(1 for k, v in rows[0].items() if v not in (None, "", [], {}))
    print(f"  fields returned per row: {len(rows[0])}, populated on row 1: {populated}")

    # The whole design assumes this is present and stable.
    missing_key = [r for r in rows if not r.get("pulsarId")]
    print(
        f"  rows missing pulsarId: {len(missing_key)} "
        f"({'OK' if not missing_key else 'PROBLEM - pulsarId is not reliable'})"
    )

    stamped = [r for r in rows if r.get("updatedAt")]
    print(f"  rows with updatedAt:   {len(stamped)}/{len(rows)}")
    if not stamped:
        print("  no updatedAt values - updatedFrom cannot be tested, and the")
        print("  incremental design must fall back to date-window re-reads")
        return {"test1": "PASS", "test2": "FAIL (no updatedAt)"}

    # --- Test 2: pick a boundary and check both sides of it ---
    stamps = sorted(r["updatedAt"] for r in stamped)
    boundary = statistics.median_low(stamps)
    expected_at_or_after = [s for s in stamps if s >= boundary]

    print(f"\n[test 2] boundary = {boundary}")
    print(
        f"  baseline: {len(stamps)} rows, {len(expected_at_or_after)} at/after boundary"
    )

    filtered_block = fetch(
        client, query, {**window, "updatedFrom": boundary}, {"limit": 100}
    )
    filtered = filtered_block.get("results") or []
    got = sorted(r["updatedAt"] for r in filtered if r.get("updatedAt"))

    below = [s for s in got if s < boundary]
    print(f"  with updatedFrom: {len(filtered)} rows returned")
    print(f"  rows below the boundary (should be 0): {len(below)}")

    if below:
        verdict = "FAIL - updatedFrom does not filter on the updatedAt clock"
    elif len(got) == len(stamps):
        verdict = "INCONCLUSIVE - filter returned everything; it may be ignored"
    elif set(got) == set(expected_at_or_after):
        verdict = "PASS - exact match on the expected set"
    else:
        verdict = (
            f"PARTIAL - filtered to {len(got)}, expected "
            f"{len(expected_at_or_after)}; usable but not exact"
        )

    print(f"  VERDICT: {verdict}")

    (OUT / "validate_updated_from.json").write_text(
        json.dumps(
            {
                "boundary": boundary,
                "baseline_count": len(stamps),
                "expected_at_or_after": len(expected_at_or_after),
                "returned_with_filter": len(got),
                "below_boundary": len(below),
                "verdict": verdict,
            },
            indent=2,
        )
    )

    return {"test1": "PASS", "test2": verdict}


def test_key_across_searches(client: PulsarClient, query: str) -> dict[str, Any]:
    """Test 3. Two calls."""
    print("\n" + "=" * 68)
    print("TEST 3  is pulsarId unique across searches?")
    print("=" * 68)

    # Same narrow window on both so we are comparing the same period.
    today = dt.datetime.now(dt.UTC).date()
    window = {
        "dateFrom": (today - dt.timedelta(days=7)).strftime("%Y-%m-%dT00:00:00Z"),
        "dateTo": (today + dt.timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z"),
    }
    options = {"limit": 100, "sortBy": "TIME", "sort": "DESC"}

    seen = {}
    for label, search_id in (("A", OVERLAP_A), ("B", OVERLAP_B)):
        block = fetch(client, query, {**window, "searchIds": [search_id]}, options)
        rows = block.get("results") or []
        ids = {r.get("pulsarId") for r in rows if r.get("pulsarId")}
        seen[search_id] = ids

        echoed = {r.get("search") for r in rows}
        print(
            f"\n  search {search_id} ({label}): {len(rows)} rows, "
            f"{len(ids)} distinct pulsarId"
        )
        print(f"    `search` field echoes: {echoed or 'not populated'}")

    overlap = seen[OVERLAP_A] & seen[OVERLAP_B]
    print(f"\n  shared pulsarId between the two searches: {len(overlap)}")

    if overlap:
        verdict = "PK MUST BE (search, pulsarId) - ids are shared across searches"
        print(f"  sample shared id: {sorted(overlap)[0]}")
    else:
        verdict = (
            "no overlap in this sample - pulsarId looks global, but 100 "
            "rows is not proof; use (search, pulsarId) defensively"
        )

    print(f"  VERDICT: {verdict}")

    (OUT / "validate_key.json").write_text(
        json.dumps(
            {
                "search_a": OVERLAP_A,
                "search_b": OVERLAP_B,
                "distinct_a": len(seen[OVERLAP_A]),
                "distinct_b": len(seen[OVERLAP_B]),
                "shared": len(overlap),
                "verdict": verdict,
            },
            indent=2,
        )
    )

    return {"test3": verdict}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--test",
        type=int,
        choices=[2, 3],
        help="run only one test (2 = updatedFrom, 3 = key)",
    )
    args = ap.parse_args()

    OUT.mkdir(exist_ok=True)
    query = build_query()
    results = {}

    try:
        with PulsarClient("trac_data") as client:
            if args.test in (None, 2):
                results.update(test_field_set_and_updated_from(client, query))
            if args.test in (None, 3):
                results.update(test_key_across_searches(client, query))
            calls = client.request_count
    except PulsarError as exc:
        print(f"\nFAILED: {exc}")
        print("\nIf this is a field error, the full field set is the likely cause.")
        print("Re-run tools/introspect.py - the schema may have changed.")
        return 1

    print("\n" + "=" * 68)
    print("SUMMARY")
    print("=" * 68)
    for name, verdict in results.items():
        print(f"  {name}: {verdict}")
    print(f"\n{calls} API calls used")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
