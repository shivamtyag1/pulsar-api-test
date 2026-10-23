"""Audience reports: the demographic panels that come from Audiense/StatSocial.

These live on `trac_meta`, not `trac_data`, and they exist only if someone
has already run a report in the Pulsar UI - the API has no mutation to
start one. So "no report" is a normal outcome here, reported rather than
raised, and it tells the team which brands need a report run by hand.

What is kept is aggregate only: per community segment, its size and the
key/value distributions (ages, genders, countries, cities, languages,
interests) plus the bio keyword list. What is dropped, deliberately:

  link, summary            free text / URLs about the segment
  brands, media,           named accounts and outlets the segment follows -
  personalities, sources   public figures, but a list of named people is
                           exactly what the author-PII policy keeps out of
                           the facets, so it stays out here too

Rows use the facet row shape, so they go into the aggregates dataset:

  facet=audience.report   label=<report id>  label_type=<provider>
                          value=fullAudienceSize
  facet=audience.segment  label=<segment>    value=size | sizeRel (metric says which)
                          parent_facet=audience.report parent_label=<report id>
  facet=audience.<dim>    label=<key>        value=<share>
                          parent_facet=audience.segment
                          parent_label=<report id>/<segment>

Each row also carries the REPORT's window (`window_from`/`window_to`), not
the pull's: a report is computed over its own date range, and stamping it
with the pull window would misdate it.
"""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING, Any

from pulsar.facets import _row

if TYPE_CHECKING:  # pragma: no cover - typing only
    from collections.abc import Callable

    from pulsar.export import Executor

JsonDict = dict[str, Any]

# KeyValuePair lists on CommunityInfoSegment.
KV_SEGMENTS = ("ages", "genders", "countries", "cities", "languages", "interests")

PAGE_SIZE = 20
MAX_PAGES = 50  # a search with 1,000 reports is a bug, not a workload

QUERY = (
    """
query Audience($searchId: ID, $first: Int!, $after: String) {
  audienceReports(searchId: $searchId, first: $first, after: $after) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id name provider status segmentation
      dateFrom dateTo fullAudienceSize audiencePercentage createdAt
      communityInfoSegments {
        id name size sizeRel bioKeywords
        """
    + " ".join(f"{k} {{ key value }}" for k in KV_SEGMENTS)
    + """
      }
    }
  }
}
"""
)


@dataclasses.dataclass
class AudienceResult:
    """What one search's audience lookup found."""

    search_id: str
    reports: list[JsonDict]  # id, name, provider, status, window - no segments
    rows: list[JsonDict]

    @property
    def status(self) -> str:
        if not self.reports:
            return "none"
        ready = sum(1 for r in self.reports if r.get("status") == "READY")
        return f"{len(self.reports)} report(s), {ready} ready"


def flatten(report: JsonDict) -> list[JsonDict]:
    """One report's segments as facet-shaped rows."""
    rid = str(report.get("id"))
    window = {"window_from": report.get("dateFrom"), "window_to": report.get("dateTo")}
    rows = [
        _row(
            "audience.report",
            rid,
            report.get("fullAudienceSize"),
            metric="full_audience_size",
        )
        | {"label_type": report.get("provider")}
    ]

    for seg in report.get("communityInfoSegments") or []:
        name = seg.get("name")
        seg_key = f"{rid}/{name}"
        for metric in ("size", "sizeRel"):
            rows.append(
                _row(
                    "audience.segment",
                    name,
                    seg.get(metric),
                    metric=metric,
                    parent_facet="audience.report",
                    parent_label=rid,
                )
            )
        for dim in KV_SEGMENTS:
            for pair in seg.get(dim) or []:
                rows.append(
                    _row(
                        f"audience.{dim}",
                        pair.get("key"),
                        pair.get("value"),
                        parent_facet="audience.segment",
                        parent_label=seg_key,
                    )
                )
        for keyword in seg.get("bioKeywords") or []:
            rows.append(
                _row(
                    "audience.bioKeywords",
                    keyword,
                    None,
                    parent_facet="audience.segment",
                    parent_label=seg_key,
                )
            )

    return [r | window for r in rows]


def fetch(
    client: Executor,
    *,
    search_id: str,
    progress: Callable[[str], None] | None = None,
) -> AudienceResult:
    """Every audience report on one search. Usually one call."""
    say = progress or (lambda _msg: None)
    reports: list[JsonDict] = []
    rows: list[JsonDict] = []
    after: str | None = None

    for _ in range(MAX_PAGES):
        data = client.execute(
            QUERY, {"searchId": search_id, "first": PAGE_SIZE, "after": after}
        )
        block = data.get("audienceReports") or {}
        for node in block.get("nodes") or []:
            if not isinstance(node, dict):
                continue
            reports.append(
                {
                    k: node.get(k)
                    for k in (
                        "id",
                        "name",
                        "provider",
                        "status",
                        "segmentation",
                        "dateFrom",
                        "dateTo",
                        "fullAudienceSize",
                        "createdAt",
                    )
                }
            )
            rows.extend(flatten(node))
        info = block.get("pageInfo") or {}
        after = info.get("endCursor")
        if not info.get("hasNextPage") or not after:
            break

    result = AudienceResult(search_id=search_id, reports=reports, rows=rows)
    say(f"  audience: {result.status}")
    return result
