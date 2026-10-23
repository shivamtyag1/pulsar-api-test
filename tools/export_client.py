"""Superseded by tools/pull.py - kept so existing commands keep working.

Runs `tools/pull.py --only mentions` with the same flags. Two differences
from the old tool, both deliberate:

  * output goes to the new layout
      pulsar/mentions/agency=<A>/client=<c>/search=<id>/snapshot=<run date>/
  * without --s3 it still writes locally (out/s3/), as before; with --s3 it
    uploads directly instead of printing `aws s3 cp` commands

Prefer:  python tools/pull.py --client <c> --only mentions
"""

import sys

from pulsar.cli import main, translate_legacy

if __name__ == "__main__":
    sys.exit(main(translate_legacy(sys.argv[1:], "mentions")))
