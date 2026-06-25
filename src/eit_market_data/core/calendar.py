"""Exchange-holiday-aware trading-day calendar helpers.

Previously these helpers used a *weekday-only* (Mon-Fri) calendar that ignored
exchange holidays. That was incorrect: ``execution_date`` (the next trading day
after a month-end ``decision_date``) could land on a market holiday. The most
visible failure was 2023-12, where the weekday-only logic produced an
``execution_date`` of 2024-01-01 (New Year's Day, markets closed) instead of the
correct 2024-01-02.

Calendar choice
---------------
``pandas_market_calendars`` would be the ideal source of truth but is **not** an
available dependency (and adding it pulls a heavy dependency tree). ``pandas`` is
present, but ``pandas.tseries.holiday.USFederalHolidayCalendar`` is the *federal*
calendar, which is wrong for exchanges (e.g. Good Friday closes NYSE but is not a
federal holiday; Columbus Day / Veterans Day are federal but markets stay open).

So this module ships a **minimal, hand-maintained holiday set** for the two
exchanges this project needs:

* ``XNYS`` — NYSE / Nasdaq (US)
* ``XKRX`` — Korea Exchange (KR)

Coverage window: **2015-01-01 .. 2026-12-31**. This spans every month the
backfill touches. Dates outside this window fall back to a weekday-only
calendar with a logged warning, so the helpers never raise.

The holiday sets below are full-day market closures only. Early-close /
half-day sessions (e.g. day after Thanksgiving) are *not* modeled because they
are still trading days and do not affect decision/execution date selection.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta

logger = logging.getLogger(__name__)

DEFAULT_MARKET = "XNYS"

# Inclusive coverage window for the curated holiday sets.
_COVERAGE_START = date(2015, 1, 1)
_COVERAGE_END = date(2026, 12, 31)


def _d(y: int, m: int, day: int) -> date:
    return date(y, m, day)


# ---------------------------------------------------------------------------
# XNYS (NYSE / Nasdaq) full-day closures, 2015-2026.
# Sources: NYSE published holiday calendars. Observed dates already applied
# (e.g. a Saturday holiday observed Friday, a Sunday holiday observed Monday).
# Good Friday and Juneteenth (from 2022) are included; these are NOT federal.
# ---------------------------------------------------------------------------
_XNYS_HOLIDAYS: frozenset[date] = frozenset(
    {
        # 2015
        _d(2015, 1, 1), _d(2015, 1, 19), _d(2015, 2, 16), _d(2015, 4, 3),
        _d(2015, 5, 25), _d(2015, 7, 3), _d(2015, 9, 7), _d(2015, 11, 26),
        _d(2015, 12, 25),
        # 2016
        _d(2016, 1, 1), _d(2016, 1, 18), _d(2016, 2, 15), _d(2016, 3, 25),
        _d(2016, 5, 30), _d(2016, 7, 4), _d(2016, 9, 5), _d(2016, 11, 24),
        _d(2016, 12, 26),
        # 2017
        _d(2017, 1, 2), _d(2017, 1, 16), _d(2017, 2, 20), _d(2017, 4, 14),
        _d(2017, 5, 29), _d(2017, 7, 4), _d(2017, 9, 4), _d(2017, 11, 23),
        _d(2017, 12, 25),
        # 2018
        _d(2018, 1, 1), _d(2018, 1, 15), _d(2018, 2, 19), _d(2018, 3, 30),
        _d(2018, 5, 28), _d(2018, 7, 4), _d(2018, 9, 3), _d(2018, 11, 22),
        _d(2018, 12, 5),  # National day of mourning (G.H.W. Bush)
        _d(2018, 12, 25),
        # 2019
        _d(2019, 1, 1), _d(2019, 1, 21), _d(2019, 2, 18), _d(2019, 4, 19),
        _d(2019, 5, 27), _d(2019, 7, 4), _d(2019, 9, 2), _d(2019, 11, 28),
        _d(2019, 12, 25),
        # 2020
        _d(2020, 1, 1), _d(2020, 1, 20), _d(2020, 2, 17), _d(2020, 4, 10),
        _d(2020, 5, 25), _d(2020, 7, 3), _d(2020, 9, 7), _d(2020, 11, 26),
        _d(2020, 12, 25),
        # 2021
        _d(2021, 1, 1), _d(2021, 1, 18), _d(2021, 2, 15), _d(2021, 4, 2),
        _d(2021, 5, 31), _d(2021, 7, 5), _d(2021, 9, 6), _d(2021, 11, 25),
        _d(2021, 12, 24),
        # 2022 (Juneteenth observed from this year)
        _d(2022, 1, 17), _d(2022, 2, 21), _d(2022, 4, 15), _d(2022, 5, 30),
        _d(2022, 6, 20), _d(2022, 7, 4), _d(2022, 9, 5), _d(2022, 11, 24),
        _d(2022, 12, 26),
        # 2023
        _d(2023, 1, 2), _d(2023, 1, 16), _d(2023, 2, 20), _d(2023, 4, 7),
        _d(2023, 5, 29), _d(2023, 6, 19), _d(2023, 7, 4), _d(2023, 9, 4),
        _d(2023, 11, 23), _d(2023, 12, 25),
        # 2024
        _d(2024, 1, 1), _d(2024, 1, 15), _d(2024, 2, 19), _d(2024, 3, 29),
        _d(2024, 5, 27), _d(2024, 6, 19), _d(2024, 7, 4), _d(2024, 9, 2),
        _d(2024, 11, 28), _d(2024, 12, 25),
        # 2025
        _d(2025, 1, 1), _d(2025, 1, 9),  # Carter national day of mourning
        _d(2025, 1, 20), _d(2025, 2, 17), _d(2025, 4, 18), _d(2025, 5, 26),
        _d(2025, 6, 19), _d(2025, 7, 4), _d(2025, 9, 1), _d(2025, 11, 27),
        _d(2025, 12, 25),
        # 2026
        _d(2026, 1, 1), _d(2026, 1, 19), _d(2026, 2, 16), _d(2026, 4, 3),
        _d(2026, 5, 25), _d(2026, 6, 19), _d(2026, 7, 3), _d(2026, 9, 7),
        _d(2026, 11, 26), _d(2026, 12, 25),
    }
)


# ---------------------------------------------------------------------------
# XKRX (Korea Exchange) full-day closures, 2015-2026.
# KR holidays include lunar-calendar holidays (Seollal, Chuseok) whose Gregorian
# dates shift each year, substitute holidays, and the year-end market closure
# (typically Dec 31). These are enumerated explicitly since they are not derivable
# from simple weekday rules.
# ---------------------------------------------------------------------------
_XKRX_HOLIDAYS: frozenset[date] = frozenset(
    {
        # 2015
        _d(2015, 1, 1), _d(2015, 2, 18), _d(2015, 2, 19), _d(2015, 2, 20),
        _d(2015, 3, 1), _d(2015, 5, 1), _d(2015, 5, 5), _d(2015, 5, 25),
        _d(2015, 6, 6), _d(2015, 8, 14), _d(2015, 8, 15), _d(2015, 9, 28),
        _d(2015, 9, 29), _d(2015, 10, 3), _d(2015, 10, 9), _d(2015, 12, 25),
        _d(2015, 12, 31),
        # 2016
        _d(2016, 1, 1), _d(2016, 2, 8), _d(2016, 2, 9), _d(2016, 2, 10),
        _d(2016, 3, 1), _d(2016, 4, 13), _d(2016, 5, 5), _d(2016, 5, 6),
        _d(2016, 6, 6), _d(2016, 8, 15), _d(2016, 9, 14), _d(2016, 9, 15),
        _d(2016, 9, 16), _d(2016, 10, 3), _d(2016, 12, 30),
        # 2017
        _d(2017, 1, 27), _d(2017, 1, 30), _d(2017, 3, 1), _d(2017, 5, 1),
        _d(2017, 5, 3), _d(2017, 5, 5), _d(2017, 5, 9), _d(2017, 6, 6),
        _d(2017, 8, 15), _d(2017, 10, 2), _d(2017, 10, 3), _d(2017, 10, 4),
        _d(2017, 10, 5), _d(2017, 10, 6), _d(2017, 10, 9), _d(2017, 12, 25),
        _d(2017, 12, 29),
        # 2018
        _d(2018, 1, 1), _d(2018, 2, 15), _d(2018, 2, 16), _d(2018, 3, 1),
        _d(2018, 5, 1), _d(2018, 5, 7), _d(2018, 5, 22), _d(2018, 6, 6),
        _d(2018, 6, 13), _d(2018, 8, 15), _d(2018, 9, 24), _d(2018, 9, 25),
        _d(2018, 9, 26), _d(2018, 10, 3), _d(2018, 10, 9), _d(2018, 12, 25),
        _d(2018, 12, 31),
        # 2019
        _d(2019, 1, 1), _d(2019, 2, 4), _d(2019, 2, 5), _d(2019, 2, 6),
        _d(2019, 3, 1), _d(2019, 5, 1), _d(2019, 5, 6), _d(2019, 6, 6),
        _d(2019, 8, 15), _d(2019, 9, 12), _d(2019, 9, 13), _d(2019, 10, 3),
        _d(2019, 10, 9), _d(2019, 12, 25), _d(2019, 12, 31),
        # 2020
        _d(2020, 1, 1), _d(2020, 1, 24), _d(2020, 1, 27), _d(2020, 4, 15),
        _d(2020, 4, 30), _d(2020, 5, 1), _d(2020, 5, 5), _d(2020, 8, 15),
        _d(2020, 8, 17), _d(2020, 9, 30), _d(2020, 10, 1), _d(2020, 10, 2),
        _d(2020, 10, 9), _d(2020, 12, 25), _d(2020, 12, 31),
        # 2021
        _d(2021, 1, 1), _d(2021, 2, 11), _d(2021, 2, 12), _d(2021, 3, 1),
        _d(2021, 5, 5), _d(2021, 5, 19), _d(2021, 8, 16), _d(2021, 9, 20),
        _d(2021, 9, 21), _d(2021, 9, 22), _d(2021, 10, 4), _d(2021, 10, 11),
        _d(2021, 12, 31),
        # 2022
        _d(2022, 1, 31), _d(2022, 2, 1), _d(2022, 2, 2), _d(2022, 3, 1),
        _d(2022, 3, 9), _d(2022, 5, 5), _d(2022, 6, 1), _d(2022, 6, 6),
        _d(2022, 8, 15), _d(2022, 9, 9), _d(2022, 9, 12), _d(2022, 10, 3),
        _d(2022, 10, 10), _d(2022, 12, 30),
        # 2023
        _d(2023, 1, 23), _d(2023, 1, 24), _d(2023, 3, 1), _d(2023, 5, 1),
        _d(2023, 5, 5), _d(2023, 5, 29), _d(2023, 6, 6), _d(2023, 8, 15),
        _d(2023, 9, 28), _d(2023, 9, 29), _d(2023, 10, 2), _d(2023, 10, 3),
        _d(2023, 10, 9), _d(2023, 12, 25), _d(2023, 12, 29),
        # 2024
        _d(2024, 1, 1), _d(2024, 2, 9), _d(2024, 2, 12), _d(2024, 3, 1),
        _d(2024, 4, 10), _d(2024, 5, 1), _d(2024, 5, 6), _d(2024, 5, 15),
        _d(2024, 6, 6), _d(2024, 8, 15), _d(2024, 9, 16), _d(2024, 9, 17),
        _d(2024, 9, 18), _d(2024, 10, 1), _d(2024, 10, 3), _d(2024, 10, 9),
        _d(2024, 12, 25), _d(2024, 12, 31),
        # 2025
        _d(2025, 1, 1), _d(2025, 1, 28), _d(2025, 1, 29), _d(2025, 1, 30),
        _d(2025, 3, 1), _d(2025, 3, 3), _d(2025, 5, 1), _d(2025, 5, 5),
        _d(2025, 5, 6), _d(2025, 6, 3), _d(2025, 6, 6), _d(2025, 8, 15),
        _d(2025, 10, 3), _d(2025, 10, 6), _d(2025, 10, 7), _d(2025, 10, 8),
        _d(2025, 10, 9), _d(2025, 12, 25), _d(2025, 12, 31),
        # 2026
        _d(2026, 1, 1), _d(2026, 2, 16), _d(2026, 2, 17), _d(2026, 2, 18),
        _d(2026, 3, 1), _d(2026, 3, 2), _d(2026, 5, 1), _d(2026, 5, 5),
        _d(2026, 5, 25), _d(2026, 6, 6), _d(2026, 8, 15), _d(2026, 8, 17),
        _d(2026, 9, 24), _d(2026, 9, 25), _d(2026, 10, 3), _d(2026, 10, 5),
        _d(2026, 10, 9), _d(2026, 12, 25), _d(2026, 12, 31),
    }
)


_HOLIDAY_SETS: dict[str, frozenset[date]] = {
    "XNYS": _XNYS_HOLIDAYS,
    "XKRX": _XKRX_HOLIDAYS,
}

# Friendly aliases.
_MARKET_ALIASES: dict[str, str] = {
    "US": "XNYS",
    "NYSE": "XNYS",
    "NASDAQ": "XNYS",
    "KR": "XKRX",
    "KRX": "XKRX",
    "KOSPI": "XKRX",
    "KOSDAQ": "XKRX",
}


def _resolve_market(market: str) -> str:
    key = market.upper()
    return _MARKET_ALIASES.get(key, key)


def _is_trading_day(value: date, market: str) -> bool:
    """Return True if ``value`` is a trading day on ``market``.

    Weekends are always non-trading. Within the curated coverage window a date is
    a trading day iff it is a weekday and not in the exchange holiday set. Outside
    the coverage window we degrade to weekday-only (with a one-time warning) so
    callers never crash on out-of-range dates.
    """
    if value.weekday() >= 5:
        return False

    resolved = _resolve_market(market)
    holidays = _HOLIDAY_SETS.get(resolved)
    if holidays is None:
        logger.warning(
            "unknown market %r for trading calendar; using weekday-only fallback",
            market,
        )
        return True

    if not (_COVERAGE_START <= value <= _COVERAGE_END):
        logger.warning(
            "date %s is outside trading-calendar coverage window %s..%s for %s; "
            "using weekday-only fallback",
            value.isoformat(), _COVERAGE_START.isoformat(),
            _COVERAGE_END.isoformat(), resolved,
        )
        return True

    return value not in holidays


def is_trading_day(value: date, market: str = DEFAULT_MARKET) -> bool:
    """Public predicate: is ``value`` a trading day on ``market``?"""
    return _is_trading_day(value, market)


def _last_business_day(year: int, month: int, market: str = DEFAULT_MARKET) -> date:
    """Return the last *trading* day of the given month (holiday-aware)."""
    if month == 12:
        last = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        last = date(year, month + 1, 1) - timedelta(days=1)
    while not _is_trading_day(last, market):
        last -= timedelta(days=1)
    return last


def _first_business_day(year: int, month: int, market: str = DEFAULT_MARKET) -> date:
    """Return the first *trading* day of the given month (holiday-aware)."""
    first = date(year, month, 1)
    while not _is_trading_day(first, market):
        first += timedelta(days=1)
    return first


def _next_business_day(value: date, market: str = DEFAULT_MARKET) -> date:
    """Return the next *trading* day strictly after ``value`` (holiday-aware)."""
    next_day = value + timedelta(days=1)
    while not _is_trading_day(next_day, market):
        next_day += timedelta(days=1)
    return next_day
