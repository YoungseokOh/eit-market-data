from __future__ import annotations

from eit_market_data.kr.short_ban import (
    KR_SHORT_BAN_WINDOWS,
    is_kr_short_ban_month,
)


def test_covid_window_flagged():
    for m in ("2020-03", "2020-09", "2021-05"):
        assert is_kr_short_ban_month(m) is True


def test_second_window_flagged():
    for m in ("2023-11", "2024-06", "2025-03"):
        assert is_kr_short_ban_month(m) is True


def test_non_ban_months():
    for m in ("2019-01", "2020-02", "2021-06", "2023-10", "2025-04", "2026-06"):
        assert is_kr_short_ban_month(m) is False


def test_window_boundaries_inclusive():
    for lo, hi in KR_SHORT_BAN_WINDOWS:
        assert is_kr_short_ban_month(lo) is True
        assert is_kr_short_ban_month(hi) is True
