"""Step 3 - pull mentions for one search and land them raw.

Writes unmodified API responses as JSONL. That raw file is the whole
point: once we hold it, every downstream mistake is fixable with a
re-read of local data instead of another trip through a rate-limited
(and possibly fair-use-metered) API.

Usage:
  # Always start here - one call, no data written. Tells you the total
  # mention count for the window before you spend any more calls.
  python tools/extract.py --search-id 12345 --from 2026-07-31 --dry-run

  # Then a small real pull (default window is 3 days, 5 pages max).
  python tools/extract.py --search-id 12345

  # Full pull, once the dry-run total tells you how big this is going
  # to be - set --max-pages deliberately rather than relying on the
  # default guard rail.
  python tools/extract.py --search-id 12345 --from 2026-07-31 --max-pages 40

Output: out/raw/trac/search=<id>/ingest=<ts>/page_NNNN.json
        out/mentions.jsonl   (flattened, for step 04)
        --dry-run writes nothing and makes exactly one API call.
"""

import argparse
import datetime as dt
import json
import pathlib
import time
from typing import Any

from pulsar.client import PulsarClient, PulsarError
from pulsar.paging import PaginationGuard, RunawayPagination

OUT = pathlib.Path("out")

# Corrected against schema/trac_data.txt (real introspection, 14 Sep 2026).
# The docs implied flat args (searchIds/dateFrom/dateTo/page) - the real
# schema nests everything under filter/options, and pagination is cursor-
# based, not page-number-based:
#   results(filter: FilterInput!, options: ResultsOptionsInput): ResultsReturn!
#   FilterInput.dateFrom / dateTo are ISO8601DateTime! (required)
#   ResultsOptionsInput.cursor (not "page"), .limit
#   ResultsReturn.nextCursor (not "nextPage"), .total, .results: [TracContent!]!
RESULTS = """
query Results($filter: FilterInput!, $options: ResultsOptionsInput) {
  results(filter: $filter, options: $options) {
    total
    nextCursor
    results {
      content
      source
      userName
      publishedAt
      likesCount
      sharesCount
      commentsCount
      engagement
      impressions
      potentialReach
    }
  }
}
"""

PAGE_LIMIT = 100  # matches the documented "100 per page" ceiling


def iso(d: dt.date) -> str:
    return d.strftime("%Y-%m-%dT00:00:00Z")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--search-id", required=True)
    ap.add_argument(
        "--days",
        type=int,
        default=3,
        help="default kept small so an unqualified run can't "
        "accidentally sweep a huge window (default: 3)",
    )
    ap.add_argument("--from", dest="date_from")
    ap.add_argument("--to", dest="date_to")
    ap.add_argument(
        "--max-pages",
        type=int,
        default=5,
        help="POC guard rail - stops a runaway pull. Raise "
        "deliberately for a full pull once --dry-run has "
        "told you the expected size (default: 5)",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="fetch page 1 only (1 API call), print the "
        "vendor's total and a sample record, write "
        "nothing. Always run this before a real pull",
    )
    args = ap.parse_args()

    # UTC, not local: the API's dateFrom/dateTo are UTC, so a run late in
    # the local evening would otherwise ask for the wrong day.
    today = dt.datetime.now(dt.UTC).date()
    date_from = args.date_from or iso(today - dt.timedelta(days=args.days))
    date_to = args.date_to or iso(today + dt.timedelta(days=1))
    if len(date_from) == 10:
        date_from += "T00:00:00Z"
    if len(date_to) == 10:
        date_to += "T00:00:00Z"

    stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")

    print(f"search {args.search_id}  window {date_from} -> {date_to}")

    if args.dry_run:
        print("dry run - 1 call, nothing written to disk\n")
        try:
            with PulsarClient("trac_data") as client:
                data = client.execute(
                    RESULTS,
                    {
                        "filter": {
                            "searchIds": [args.search_id],
                            "dateFrom": date_from,
                            "dateTo": date_to,
                        },
                        "options": {"limit": PAGE_LIMIT},
                    },
                )
        except PulsarError as exc:
            print(f"FAILED: {exc}")
            print("\nIf this is a field error, open schema/trac_data.txt and")
            print("correct the RESULTS query above to match the real schema.")
            return 1

        block = data.get("results") or {}
        total = block.get("total")
        page1 = block.get("results") or []

        print(f"vendor reports total = {total}")
        print(f"page size = {len(page1)}")
        if isinstance(total, int) and page1:
            est_pages = -(-total // len(page1))  # ceil
            print(f"estimated pages for full window ~= {est_pages}")
            print(f"suggested: --max-pages {est_pages} for the full pull")
        if page1:
            print("\nsample record:")
            print(json.dumps(page1[0], indent=2))
        print("\nno files written - this was a dry run")
        return 0

    raw_dir = OUT / "raw" / "trac" / f"search={args.search_id}" / f"ingest={stamp}"
    raw_dir.mkdir(parents=True, exist_ok=True)

    rows: list[dict[str, Any]] = []
    guard: PaginationGuard | None = None
    started = time.monotonic()

    try:
        with PulsarClient("trac_data") as client:
            while True:
                options: dict[str, object] = {"limit": PAGE_LIMIT}
                if guard is not None and guard.cursor:
                    options["cursor"] = guard.cursor

                data = client.execute(
                    RESULTS,
                    {
                        "filter": {
                            "searchIds": [args.search_id],
                            "dateFrom": date_from,
                            "dateTo": date_to,
                        },
                        "options": options,
                    },
                )

                block = data.get("results") or {}
                batch = block.get("results") or []

                # The vendor's total is only trustworthy from the first page;
                # the guard is built once we have seen it.
                if guard is None:
                    guard = PaginationGuard(
                        vendor_total=block.get("total"),
                        page_limit=PAGE_LIMIT,
                        max_pages=args.max_pages,
                    )
                    print(f"  vendor reports total = {guard.vendor_total}")

                # Land the response untouched, before we interpret anything.
                (raw_dir / f"page_{guard.pages + 1:04d}.json").write_text(
                    json.dumps(data, indent=2)
                )

                for r in batch:
                    r["_search_id"] = args.search_id
                    r["_ingested_at"] = stamp
                    rows.append(r)

                decision = guard.observe(len(batch), block.get("nextCursor"))
                print(f"  page {guard.pages}: {len(batch)} rows (running {len(rows)})")

                if decision:
                    print(f"  stopped: {decision.reason}")
                    break

            requests_made = client.request_count

    except RunawayPagination as exc:
        print(f"\nRUNAWAY: {exc}")
        print(f"\n{len(rows):,} rows were fetched but are NOT reconciled.")
        print(f"Raw responses are in {raw_dir} if you want to inspect them.")
        return 1
    except PulsarError as exc:
        print(f"\nFAILED: {exc}")
        print("\nIf this is a field error, open schema/trac_data.txt and")
        print("correct the RESULTS query above to match the real schema.")
        return 1

    elapsed = time.monotonic() - started
    path = OUT / "mentions.jsonl"
    with path.open("w") as fh:
        for r in rows:
            fh.write(json.dumps(r) + "\n")

    print(f"\n{len(rows):,} rows -> {path}")
    print(f"raw responses -> {raw_dir}")
    print(
        f"{requests_made} requests in {elapsed:.1f}s "
        f"({requests_made / max(elapsed, 0.001):.2f} req/s)"
    )

    # Reconciliation against the vendor's own total. Deduplicated on the
    # primary key, because paging can legitimately re-serve rows.
    distinct = len({r.get("pulsarId") for r in rows if r.get("pulsarId")}) or None
    ok, verdict = guard.reconcile(distinct_rows=distinct)
    print(f"reconciliation: {verdict}")

    if not ok:
        print("  rows were dropped - do not trust or publish this extract")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
