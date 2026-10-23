"""Superseded by tools/pull.py - kept so existing commands keep working.

Runs `tools/pull.py --only facets,timeseries,audience` with the same flags.
Output goes to the new layout, pulsar/aggregates/agency=<A>/client=<c>/
snapshot=<run date>/, and now includes the time series and audience rows
alongside the facets. Without --s3 it writes locally (out/s3/), as before.

Prefer:  python tools/pull.py --client <c> --only facets,timeseries,audience
"""

import sys

from pulsar.cli import main, translate_legacy

if __name__ == "__main__":
    sys.exit(main(translate_legacy(sys.argv[1:], "facets,timeseries,audience")))
