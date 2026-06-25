"""Tests for the holiday-aware trading-day calendar.

These pin the correctness fix: execution_date (next trading day after a month-end
decision_date) must skip exchange holidays, not just weekends.
"""

from __future__ import annotations

from datetime import date

import pytest

from eit_market_data.core.calendar import (
    _first_business_day,
    _last_business_day,
    _next_business_day,
    is_trading_day,
)


def test_2023_12_execution_skips_new_years_day():
    """The headline regression: 2023-12 decision stays 2023-12-29,
    execution becomes 2024-01-02 (NOT 2024-01-01, a market holiday)."""
    decision = _last_business_day(2023, 12)
    assert decision == date(2023, 12, 29)
    execution = _next_business_day(decision)
    assert execution == date(2024, 1, 2)


@pytest.mark.parametrize(
    "year,month,decision,execution",
    [
        # New Year boundaries (US)
        (2023, 12, date(2023, 12, 29), date(2024, 1, 2)),
        (2022, 12, date(2022, 12, 30), date(2023, 1, 3)),  # Jan 2 2023 observed NY
        (2015, 12, date(2015, 12, 31), date(2016, 1, 4)),  # Jan 1 Fri + weekend
        # July 4 boundary
        (2024, 6, date(2024, 6, 28), date(2024, 7, 1)),
        (2021, 6, date(2021, 6, 30), date(2021, 7, 1)),
        # Labor Day boundary (first Monday Sept is a holiday)
        (2024, 8, date(2024, 8, 30), date(2024, 9, 3)),  # Sep 2 = Labor Day
        # Thanksgiving / Christmas-adjacent month interiors handled by holiday set
        (2020, 11, date(2020, 11, 30), date(2020, 12, 1)),
    ],
)
def test_us_month_boundaries(year, month, decision, execution):
    d = _last_business_day(year, month)
    assert d == decision
    assert _next_business_day(d) == execution


def test_us_holidays_are_not_trading_days():
    # New Year, Good Friday (not federal), Juneteenth, Christmas
    assert not is_trading_day(date(2024, 1, 1))
    assert not is_trading_day(date(2024, 3, 29))  # Good Friday
    assert not is_trading_day(date(2023, 6, 19))  # Juneteenth
    assert not is_trading_day(date(2023, 12, 25))  # Christmas
    # A normal weekday trades
    assert is_trading_day(date(2024, 3, 28))


def test_weekends_never_trade():
    assert not is_trading_day(date(2024, 3, 30))  # Saturday
    assert not is_trading_day(date(2024, 3, 31))  # Sunday


def test_kr_calendar_skips_krx_holidays():
    # KR year-end closure: 2023-12-29 is a KRX holiday, so the last KR trading
    # day of Dec 2023 is 2023-12-28; execution skips New Year to 2024-01-02.
    d = _last_business_day(2023, 12, "XKRX")
    assert d == date(2023, 12, 28)
    assert _next_business_day(d, "XKRX") == date(2024, 1, 2)
    # Chuseok 2024 closure block (Sep 16-18)
    assert not is_trading_day(date(2024, 9, 17), "XKRX")


def test_market_aliases():
    assert _last_business_day(2023, 12, "US") == _last_business_day(2023, 12, "XNYS")
    assert _last_business_day(2023, 12, "KR") == _last_business_day(2023, 12, "XKRX")


def test_first_business_day_skips_holiday():
    # Jan 1 2024 is a holiday -> first trading day is Jan 2.
    assert _first_business_day(2024, 1) == date(2024, 1, 2)


def test_out_of_window_falls_back_to_weekday_only():
    # Beyond coverage window: weekday-only fallback, no crash.
    far = date(2030, 7, 4)  # would be a holiday if modeled, but out of window
    assert is_trading_day(far)  # Thursday weekday -> True under fallback
