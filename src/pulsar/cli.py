"""The `tools/pull.py` command line, kept in the package so the old tool
names can delegate to it and the tests can drive it without a subprocess.

See tools/pull.py for usage.
"""

from __future__ import annotations

import argparse
import datetime as dt
import pathlib
import time
from typing import Any

from pulsar import discovery, pull
from pulsar.client import PulsarClient, PulsarError
from pulsar.mapping import AGENCIES, CLIENT_TOKEN, ClientMapError, resolve_scope
from pulsar.sink import LocalTarget, S3Target, Target

DEFAULT_BUCKET = "vx-analytics"


def _utc_stamp() -> str:
    return dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")


def _args(argv: list[str] | None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Pull a brand's Pulsar folder into S3.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--client", help="lower-case token, e.g. lovesac")
    ap.add_argument("--folder", help="Pulsar folder name or id (overrides the map)")
    ap.add_argument("--agency", choices=sorted(AGENCIES), help="overrides the map")
    ap.add_argument("--search-id", help="pull only this search")
    ap.add_argument("--map", help="client map path (default config/client_map.json)")
    ap.add_argument("--from", dest="date_from", help="YYYY-MM-DD (default: 90d ago)")
    ap.add_argument("--to", dest="date_to", help="YYYY-MM-DD (default: tomorrow)")
    ap.add_argument(
        "--all-history",
        action="store_true",
        help="start each search at its oldest mention (never later than --from)",
    )
    ap.add_argument(
        "--only",
        default=",".join(pull.DATASETS),
        help=f"comma list from {','.join(pull.DATASETS)} (default: all)",
    )
    ap.add_argument(
        "--status",
        help="comma list of SearchStatusEnum values to include (default: every "
        "status except DRAFT/DELETING/PERMANENTLY_DELETED)",
    )
    ap.add_argument(
        "--no-rollup", action="store_true", help="skip the folder-wide pass"
    )
    ap.add_argument(
        "--local",
        metavar="DIR",
        help="write under DIR (e.g. out/s3) instead of S3",
    )
    ap.add_argument("--bucket", default=DEFAULT_BUCKET)
    ap.add_argument("--prefix", default=pull.DEFAULT_PREFIX)
    ap.add_argument("--max-pages", type=int, help="mention page cap per search")
    ap.add_argument("--limit", type=int, help="facet bucket cap, <= 100 (leave unset)")
    ap.add_argument(
        "--mincount", type=int, default=1, help="smallest bucket (default 1)"
    )
    ap.add_argument("--batch", type=int, default=1, help="facets per request")
    ap.add_argument("--granularity", default="DAY", help="HOUR/DAY/WEEK/MONTH")
    ap.add_argument(
        "--include-pii",
        action="store_true",
        help="author PII fields/facets. Pending Legal/InfoSec sign-off",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="discovery + estimate only; pulls and writes nothing",
    )
    ap.add_argument(
        "--list-folders",
        nargs="?",
        const="",
        metavar="NAME",
        help="list folders the token can see (optionally matching NAME) and exit",
    )
    return ap.parse_args(argv)


def list_folders(name: str) -> int:
    with PulsarClient("trac_meta") as meta:
        folders = discovery.list_folders(meta, name or None)
    print(f"{'id':>8}  {'searches':>8}  name")
    for f in sorted(folders, key=lambda f: str(f.get("name", "")).casefold()):
        print(
            f"{f.get('id')!s:>8}  {len(f.get('searchIds') or []):>8}  {f.get('name')}"
        )
    print(f"\n{len(folders)} folder(s)")
    return 0


def print_inventory(inv: discovery.Inventory, est: dict[str, Any] | None) -> None:
    if inv.folder:
        print(f"\nfolder: {inv.folder.get('name')!r} (id {inv.folder.get('id')})")
    print(f"\n  {'search':>8}  {'status':<12} {'realtime':<10} {'contents':>10}  name")
    for s in inv.searches:
        mark = " " if s.included else "x"
        print(
            f"{mark} {s.id:>8}  {s.status or '-':<12} {s.realtime_status or '-':<10} "
            f"{s.total_contents if s.total_contents is not None else '-':>10}  "
            f"{s.name or '-'}"
            + (f"   <- skipped: {s.skip_reason}" if not s.included else "")
        )
    for sid in inv.missing:
        print(f"x {sid:>8}  listed in the folder but not visible to this token")
    if est:
        print(
            f"\nestimate: <= {est['calls']:,} calls, ~{est['minutes']} min at "
            f"{pull.MEASURED_RATE} req/s (mention pages from lifetime "
            "totalContents, so an upper bound)"
        )


def print_summary(manifest: dict[str, Any], where: str, elapsed: float) -> None:
    print("\n" + "=" * 72)
    for r in manifest["searches"]:
        m = r.get("mentions") or {}
        print(
            f"  {r['search_id']:>8}  mentions {m.get('rows_written', '-'):>7}  "
            f"aggregates {r.get('aggregate_rows', 0):>6}  "
            f"audience: {r.get('audience', '-')}"
        )
        dropped = (r.get("facets_dropped") or []) + (r.get("series_dropped") or [])
        if dropped:
            print(f"            dropped: {', '.join(dropped)}")
        for e in r["errors"]:
            print(f"            ERROR {e[:200]}")
    if manifest["rollup"]:
        rollup = manifest["rollup"]
        print(f"  folder rollup: {rollup['aggregate_rows']} rows")
        rdropped = (rollup.get("facets_dropped") or []) + (
            rollup.get("series_dropped") or []
        )
        if rdropped:
            print(f"            dropped: {', '.join(rdropped)}")
    rows = manifest["rows"]
    calls = manifest["api_calls"]
    print(
        f"\n  rows: {rows['mentions']:,} mentions, {rows['aggregates']:,} aggregates, "
        f"{rows['inventory']} inventory"
    )
    print(
        f"  api calls: {calls['trac_data'] + calls['trac_meta']:,} "
        f"in {elapsed / 60:.1f} min"
    )
    print(f"  written to {where}/{manifest['manifest_key'].rsplit('/state/', 1)[0]}/")
    print(f"  manifest:  {where}/{manifest['manifest_key']}")
    print(
        "  status:    "
        + (
            f"{manifest['errors']} error(s)"
            if not manifest["ok"]
            else f"COMPLETE, but {manifest['dropped']} facet/series pull(s) dropped"
            if manifest.get("dropped")
            else "OK"
        )
    )


def translate_legacy(argv: list[str], only: str) -> list[str]:
    """Flags of the old export_client / export_facets tools -> this CLI.

    The old tools wrote locally unless given --s3, so that stays the default
    for anyone still calling them by the old name.
    """
    args = [a for a in argv if a != "--s3"]
    if "--s3" not in argv and "--local" not in argv:
        args += ["--local", "out/s3"]
    return ["--only", only, *args]


def main(argv: list[str] | None = None) -> int:
    args = _args(argv)
    if args.list_folders is not None:
        return list_folders(args.list_folders)

    if not args.client or not CLIENT_TOKEN.match(args.client):
        print(f"--client is required and must match {CLIENT_TOKEN.pattern}")
        return 2

    try:
        scope = resolve_scope(
            args.client,
            folder=args.folder,
            search_id=args.search_id,
            agency=args.agency,
            map_path=args.map,
        )
    except ClientMapError as exc:
        print(f"FAILED: {exc}")
        return 2

    today = dt.datetime.now(dt.UTC).date()
    try:
        spec = pull.PullSpec(
            client_token=args.client,
            agency=scope.agency,
            folder=scope.folder,
            search_ids=scope.search_ids,
            only_search=scope.only_search,
            date_from=pull.iso_day(args.date_from or today - dt.timedelta(days=90)),
            date_to=pull.iso_day(args.date_to or today + dt.timedelta(days=1)),
            datasets=frozenset(d.strip() for d in args.only.split(",") if d.strip()),
            statuses=(
                tuple(s.strip() for s in args.status.split(","))
                if args.status
                else None
            ),
            all_history=args.all_history,
            rollup=not args.no_rollup,
            include_pii=args.include_pii,
            max_pages=args.max_pages,
            limit=args.limit,
            mincount=args.mincount,
            batch_size=args.batch,
            granularity=args.granularity.upper(),
        )
        spec.validate()
    except ValueError as exc:
        print(f"FAILED: {exc}")
        return 2

    print(f"client {spec.client_token} ({spec.agency}), scope from {scope.provenance}")
    print(
        f"window {spec.date_from[:10]} -> {spec.date_to[:10]}"
        + (" (or each search's oldest mention, if earlier)" if spec.all_history else "")
    )
    print(f"datasets {', '.join(sorted(spec.datasets))}")
    if spec.include_pii:
        print(
            "WARNING: --include-pii. Pending Legal/InfoSec sign-off; not for "
            "anything client-facing."
        )

    if args.dry_run:
        try:
            with PulsarClient("trac_meta") as meta:
                inv = discovery.discover(
                    meta,
                    folder=spec.folder,
                    search_ids=spec.search_ids
                    + ((spec.only_search,) if spec.only_search else ()),
                    statuses=spec.statuses,
                )
        except (PulsarError, discovery.DiscoveryError) as exc:
            print(f"FAILED: {exc}")
            return 1
        if spec.only_search:
            for s in inv.searches:
                if s.id != spec.only_search and s.included:
                    s.included, s.skip_reason = False, "not selected (--search-id)"
        print_inventory(inv, pull.estimate(inv, spec))
        print("\ndry run: nothing pulled, nothing written")
        return 0

    run_id = _utc_stamp()
    target: Target
    if args.local:
        root = pathlib.Path(args.local)
        target = LocalTarget(root)
        where = str(root)
    else:
        try:
            s3 = S3Target(args.bucket)
            # Fail in one second on missing credentials, not after twenty
            # minutes of API calls when the first part tries to upload.
            s3.put(f"{args.prefix}/state/preflight/{run_id}.json", b'{"ok":true}\n')
        except Exception as exc:  # boto raises many unrelated types
            print(f"FAILED: cannot write to s3://{args.bucket}/{args.prefix}/: {exc}")
            print("Log in first (aws sso login --profile <profile>) or use --local.")
            return 2
        target = s3
        where = f"s3://{args.bucket}"

    started = time.monotonic()
    try:
        with PulsarClient("trac_meta") as meta, PulsarClient("trac_data") as data:
            manifest = pull.run(
                spec,
                meta=meta,
                data=data,
                target=target,
                run_id=run_id,
                prefix=args.prefix,
                progress=print,
            )
    except (PulsarError, discovery.DiscoveryError) as exc:
        print(f"\nFAILED: {exc}")
        print(
            "No manifest was written, so this run is incomplete. Parts already "
            f"uploaded carry run id {run_id}."
        )
        return 1

    print_summary(manifest, where, time.monotonic() - started)
    return 0 if manifest["ok"] else 1
