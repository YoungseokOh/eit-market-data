"""Weekday-only business-day helpers.

These intentionally use a *weekday-only* calendar (Mon-Fri); they do NOT apply
exchange holidays. The logic is byte-identical to the originals that lived in
``snapshot.py``.
"""

from __future__ import annotations

from datetime import date, timedelta


def _last_business_day(year: int, month: int) -> date:
    """Return the last business day of the given month."""
    # Go to last day of month
    if month == 12:
        last = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        last = date(year, month + 1, 1) - timedelta(days=1)
    # Walk back to a weekday
    while last.weekday() >= 5:
        last -= timedelta(days=1)
    return last


def _first_business_day(year: int, month: int) -> date:
    """Return the first business day of the given month."""
    first = date(year, month, 1)
    while first.weekday() >= 5:
        first += timedelta(days=1)
    return first


def _next_business_day(value: date) -> date:
    """Return the next weekday after the given date."""
    next_day = value + timedelta(days=1)
    while next_day.weekday() >= 5:
        next_day += timedelta(days=1)
    return next_day
