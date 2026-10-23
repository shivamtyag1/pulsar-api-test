"""Pagination termination logic for Pulsar `results` queries.

This exists as its own module because getting it wrong is expensive in a
way that is not obvious from reading the API docs.

Measured on search 226781: the vendor returned `nextCursor` as a non-null
value on a *complete* final page - 42 of 42 rows delivered, cursor still
populated. Code that loops while `nextCursor is not None` therefore never
terminates. Against a rate-limited, possibly fair-use-metered API, an
infinite loop is not a hung process; it is a bill and a burned quota.

So termination is a decision made from several independent signals, any
one of which is sufficient to stop. The guard is a pure state machine with
no I/O, which is what makes it testable without spending API calls.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

# Pulsar's documented maximum page size.
DEFAULT_PAGE_LIMIT = 100

# How far past the predicted page count we tolerate before declaring a
# runaway. The vendor's `total` can drift slightly during a sweep, so a
# little slack is legitimate; 20% of it is not.
DEFAULT_SLACK = 1.2


class RunawayPagination(RuntimeError):
    """Raised when paging exceeds its predicted bound by more than the slack.

    This is deliberately an exception rather than a quiet stop: it means the
    vendor's behaviour has diverged from what we measured, and continuing to
    spend calls on that assumption is worse than failing loudly.
    """


@dataclass(frozen=True)
class StopDecision:
    """Why paging stopped - carried into logs and run history."""

    stop: bool
    reason: str

    def __bool__(self) -> bool:
        return self.stop


@dataclass
class PaginationGuard:
    """Tracks paging state and decides when to stop.

    Usage::

        guard = PaginationGuard(vendor_total=block["total"])
        while True:
            decision = guard.observe(len(batch), block.get("nextCursor"))
            if decision:
                break
            ...fetch next page with guard.cursor...

    Every stop condition is checked on every page; the first to fire wins,
    and its reason is recorded.
    """

    vendor_total: int | None = None
    page_limit: int = DEFAULT_PAGE_LIMIT
    slack: float = DEFAULT_SLACK
    max_pages: int | None = None

    pages: int = field(default=0, init=False)
    rows_seen: int = field(default=0, init=False)
    cursor: str | None = field(default=None, init=False)
    _previous_cursor: str | None = field(default=None, init=False)

    @property
    def predicted_pages(self) -> int | None:
        """Pages we expect from the vendor's own total, before slack."""
        if self.vendor_total is None or self.page_limit <= 0:
            return None
        return max(1, math.ceil(self.vendor_total / self.page_limit))

    @property
    def page_ceiling(self) -> int | None:
        """Hard upper bound on pages, including slack and any explicit cap."""
        bounds = [
            b
            for b in (
                math.ceil(self.predicted_pages * self.slack)
                if self.predicted_pages
                else None,
                self.max_pages,
            )
            if b is not None
        ]
        return min(bounds) if bounds else None

    def observe(self, batch_size: int, next_cursor: str | None) -> StopDecision:
        """Record one page of results and decide whether to continue.

        Call exactly once per fetched page, before fetching the next.
        """
        self.pages += 1
        self.rows_seen += batch_size
        self._previous_cursor, self.cursor = self.cursor, next_cursor

        # 1. An empty page is the vendor's clearest end-of-stream signal.
        if batch_size == 0:
            return StopDecision(True, "empty page")

        # 2. We have everything the vendor said existed. This is the normal
        #    exit, and the one `nextCursor` alone would miss.
        if self.vendor_total is not None and self.rows_seen >= self.vendor_total:
            return StopDecision(
                True, f"reached vendor total ({self.rows_seen}/{self.vendor_total})"
            )

        # 3. No cursor to continue with.
        if not next_cursor:
            return StopDecision(True, "no next cursor")

        # 4. A repeated cursor means the vendor is serving the same page
        #    forever. Without this check that is an infinite loop.
        if self._previous_cursor is not None and next_cursor == self._previous_cursor:
            return StopDecision(True, "cursor did not advance")

        # 5. Runaway guard - we are well past what the total predicted.
        ceiling = self.page_ceiling
        if ceiling is not None and self.pages >= ceiling:
            if self.max_pages is not None and self.pages >= self.max_pages:
                return StopDecision(True, f"hit max_pages ({self.max_pages})")
            raise RunawayPagination(
                f"paged {self.pages} times against a predicted "
                f"{self.predicted_pages} (total={self.vendor_total}); "
                "vendor paging behaviour has diverged - stopping before this "
                "burns more quota"
            )

        return StopDecision(False, "continue")

    def reconcile(self, distinct_rows: int | None = None) -> tuple[bool, str]:
        """Compare what we wrote against what the vendor claimed.

        `distinct_rows` is the count after de-duplicating on the primary key;
        pass it when available, since paging can legitimately re-serve rows.
        Returns (ok, human-readable verdict).
        """
        loaded = distinct_rows if distinct_rows is not None else self.rows_seen

        if self.vendor_total is None:
            return True, f"no vendor total to reconcile against ({loaded:,} loaded)"

        delta = loaded - self.vendor_total
        if delta == 0:
            return True, f"MATCH ({loaded:,})"
        if delta > 0:
            # Over-count is expected on an updatedFrom sweep: the total moves
            # as content indexes underneath us. Under-count never is.
            return True, (
                f"OVER by {delta:+,} (vendor {self.vendor_total:,}, loaded "
                f"{loaded:,}) - expected on incremental sweeps"
            )
        return False, (
            f"UNDER by {delta:+,} (vendor {self.vendor_total:,}, loaded "
            f"{loaded:,}) - rows were dropped; do not publish"
        )
