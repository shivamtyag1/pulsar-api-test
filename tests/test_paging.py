"""Pagination termination - the logic most likely to cost real money.

Every test here encodes a failure mode we either measured against the live
API or reasoned about from it. The headline case is `test_stops_when_vendor
_total_reached_despite_live_cursor`, which reproduces the exact behaviour
observed on search 226781.
"""

from __future__ import annotations

import pytest

from pulsar.paging import (
    DEFAULT_PAGE_LIMIT,
    PaginationGuard,
    RunawayPagination,
)


class TestTermination:
    def test_stops_when_vendor_total_reached_despite_live_cursor(self) -> None:
        """The measured bug: a complete final page still carries a cursor.

        Search 226781 returned 42 of 42 rows with `nextCursor` populated.
        Looping on the cursor alone never terminates.
        """
        guard = PaginationGuard(vendor_total=42)

        decision = guard.observe(batch_size=42, next_cursor="still-here")

        assert decision.stop is True
        assert "vendor total" in decision.reason
        assert guard.rows_seen == 42

    def test_stops_on_empty_page(self) -> None:
        guard = PaginationGuard(vendor_total=1000)
        assert guard.observe(0, "cursor").stop is True
        assert guard.observe(0, "cursor").reason == "empty page"

    def test_stops_when_cursor_absent(self) -> None:
        guard = PaginationGuard(vendor_total=1000)
        decision = guard.observe(100, None)
        assert decision.stop is True
        assert decision.reason == "no next cursor"

    def test_stops_when_cursor_does_not_advance(self) -> None:
        """A repeated cursor means the vendor is re-serving one page."""
        guard = PaginationGuard(vendor_total=10_000)

        assert guard.observe(100, "cursor-a").stop is False
        decision = guard.observe(100, "cursor-a")

        assert decision.stop is True
        assert decision.reason == "cursor did not advance"

    def test_continues_through_a_normal_multi_page_pull(self) -> None:
        guard = PaginationGuard(vendor_total=250)

        assert guard.observe(100, "c1").stop is False
        assert guard.observe(100, "c2").stop is False
        final = guard.observe(50, "c3")

        assert final.stop is True
        assert guard.rows_seen == 250
        assert guard.pages == 3

    def test_handles_unknown_vendor_total(self) -> None:
        """Without a total we lean entirely on cursor and empty-page signals."""
        guard = PaginationGuard(vendor_total=None)

        assert guard.predicted_pages is None
        assert guard.observe(100, "c1").stop is False
        assert guard.observe(100, None).stop is True


class TestRunawayGuard:
    def test_raises_when_paging_far_exceeds_prediction(self) -> None:
        """Divergence from measured behaviour should fail loudly, not quietly.

        A silent stop would look like a successful short read and publish
        incomplete data.
        """
        guard = PaginationGuard(vendor_total=100)  # predicts 1 page

        with pytest.raises(RunawayPagination, match="diverged"):
            for _ in range(10):
                # Never reaches the total, cursor keeps advancing: the shape
                # of an infinite loop.
                guard.observe(1, f"cursor-{guard.pages}")

    def test_max_pages_stops_cleanly_rather_than_raising(self) -> None:
        """An operator-set cap is a deliberate limit, not an anomaly."""
        guard = PaginationGuard(vendor_total=1_000_000, max_pages=3)

        assert guard.observe(100, "c1").stop is False
        assert guard.observe(100, "c2").stop is False
        decision = guard.observe(100, "c3")

        assert decision.stop is True
        assert "max_pages" in decision.reason

    def test_page_ceiling_prefers_the_tighter_bound(self) -> None:
        generous = PaginationGuard(vendor_total=100_000, max_pages=5)
        assert generous.page_ceiling == 5

        tight = PaginationGuard(vendor_total=100, max_pages=5000)
        assert tight.page_ceiling == pytest.approx(2, abs=1)

    @pytest.mark.parametrize(
        ("total", "expected"),
        [(0, 1), (1, 1), (100, 1), (101, 2), (2770, 28), (5_882_217, 58823)],
    )
    def test_predicted_pages(self, total: int, expected: int) -> None:
        guard = PaginationGuard(vendor_total=total, page_limit=DEFAULT_PAGE_LIMIT)
        assert guard.predicted_pages == expected


class TestReconciliation:
    def test_exact_match(self) -> None:
        guard = PaginationGuard(vendor_total=2770)
        guard.observe(2770, None)

        ok, verdict = guard.reconcile()

        assert ok is True
        assert "MATCH" in verdict

    def test_undercount_blocks_publication(self) -> None:
        """Dropped rows are the failure this whole check exists to catch."""
        guard = PaginationGuard(vendor_total=2770)
        guard.observe(500, None)

        ok, verdict = guard.reconcile()

        assert ok is False
        assert "UNDER" in verdict
        assert "do not publish" in verdict

    def test_overcount_is_tolerated_on_sweeps(self) -> None:
        """During an updatedFrom sweep the vendor total moves underneath us."""
        guard = PaginationGuard(vendor_total=100)
        guard.observe(105, None)

        ok, verdict = guard.reconcile()

        assert ok is True
        assert "OVER" in verdict

    def test_reconciles_on_distinct_rows_when_given(self) -> None:
        """Paging can re-serve rows; the PK-deduplicated count is the truth."""
        guard = PaginationGuard(vendor_total=100)
        guard.observe(120, None)  # 20 duplicates re-served

        ok, verdict = guard.reconcile(distinct_rows=100)

        assert ok is True
        assert "MATCH" in verdict

    def test_no_vendor_total_is_not_a_failure(self) -> None:
        guard = PaginationGuard(vendor_total=None)
        guard.observe(42, None)

        ok, verdict = guard.reconcile()

        assert ok is True
        assert "no vendor total" in verdict
