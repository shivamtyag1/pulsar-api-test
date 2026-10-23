"""Pull one brand's Pulsar folder - every search, every dataset - into S3.

The one command for landing data the viz team can build on:

  1. find the brand's folder and every search in it (active, paused, archived)
  2. per search: mentions, 30 facets + 10 stats, 15 daily time series, audience report
  3. once more across the whole folder for folder-wide totals
  4. write it all to s3://<bucket>/pulsar/ and a manifest last

Author PII is EXCLUDED by default and stays so pending Legal/InfoSec sign-off.

Usage:
  # 0. Don't know the folder's exact name? List what the token can see.
  python tools/pull.py --list-folders
  python tools/pull.py --list-folders Lovesac

  # 1. Always start here: resolves the folder, lists every search with its
  #    status, estimates calls and minutes. Pulls nothing, writes nothing.
  python tools/pull.py --client lovesac --folder "Lovesac" --dry-run

  # 2. Try it locally first, written into out/s3/ as a mirror of the bucket.
  python tools/pull.py --client lovesac --folder "Lovesac" --local out/s3

  # 3. The real run: straight to S3 (needs an AWS SSO session).
  python tools/pull.py --client lovesac --folder "Lovesac"

  # Other knobs
  python tools/pull.py --client lovesac --from 2026-01-01 --to 2026-09-28
  python tools/pull.py --client lovesac --all-history      # back to each search's oldest mention
  python tools/pull.py --client lovesac --only facets,timeseries,audience   # skip mentions
  python tools/pull.py --client lovesac --search-id 224320 # re-run one search
  python tools/pull.py --client new-brand --folder "New Brand" --agency CM  # unmapped brand

Once a folder name is confirmed, record it in config/client_map.json so the
command is just `--client lovesac`:

  "lovesac": {"agency": "VM", "folder": "Lovesac"}
"""

import sys

from pulsar.cli import main

if __name__ == "__main__":
    sys.exit(main())
