from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from eit_market_data.fred_provider import (
    _latest_value,
    _mom_change,
    _vintage_series,
    _yoy_change,
)


class _FakeFred:
    """Minimal fredapi.Fred stand-in recording get_series call kwargs.

    ``series`` maps series_id -> pandas Series to return (or an Exception
    instance to raise). Every call's kwargs are appended to ``calls`` so tests
    can assert the point-in-time (ALFRED vintage) realtime window is passed.
    """

    def __init__(self, series: dict[str, object]) -> None:
        self._series = series
        self.calls: list[dict] = []

    def get_series(self, series_id, **kwargs):  # noqa: ANN001, ANN003
        self.calls.append({"series_id": series_id, **kwargs})
        value = self._series.get(series_id)
        if isinstance(value, Exception):
            raise value
        if value is None:
            return pd.Series(dtype="float64")
        return value


def _monthly_series(values: list[float], end: date) -> pd.Series:
    """Build a monthly-indexed Series ending at month-start of ``end``."""
    idx = pd.date_range(end=pd.Timestamp(end.replace(day=1)), periods=len(values), freq="MS")
    return pd.Series(values, index=idx)


# ---------------------------------------------------------------------------
# _vintage_series: PIT realtime window + fallback
# ---------------------------------------------------------------------------


def test_vintage_series_passes_asof_as_realtime_window() -> None:
    as_of = date(2026, 3, 31)
    start = date(2026, 1, 1)
    fred = _FakeFred({"CPIAUCSL": _monthly_series([300.0, 301.0, 302.0], as_of)})

    out = _vintage_series(fred, "CPIAUCSL", start, as_of)

    assert out is not None and not out.empty
    call = fred.calls[0]
    # Vintage/PIT: realtime_start == realtime_end == as_of (ALFRED as-known-on).
    assert call["realtime_start"] == as_of.isoformat()
    assert call["realtime_end"] == as_of.isoformat()
    # observation window is honored too.
    assert call["observation_start"] == start
    assert call["observation_end"] == as_of


def test_vintage_series_falls_back_to_plain_pull_on_vintage_error() -> None:
    as_of = date(2026, 3, 31)
    start = date(2026, 1, 1)
    fallback = _monthly_series([1.0, 2.0], as_of)

    class _FallbackFred:
        def __init__(self) -> None:
            self.calls: list[dict] = []

        def get_series(self, series_id, **kwargs):  # noqa: ANN001, ANN003
            self.calls.append(kwargs)
            # First call uses the realtime vintage window -> raise.
            if "realtime_start" in kwargs:
                raise RuntimeError("no real-time history for series")
            return fallback

    fred = _FallbackFred()
    out = _vintage_series(fred, "X", start, as_of)

    assert out is not None
    assert list(out) == [1.0, 2.0]
    # Two calls: vintage attempt then plain fallback (no realtime kwargs).
    assert len(fred.calls) == 2
    assert "realtime_start" not in fred.calls[1]


def test_vintage_series_drops_nan_rows() -> None:
    as_of = date(2026, 3, 31)
    idx = pd.date_range(end=pd.Timestamp("2026-03-01"), periods=3, freq="MS")
    fred = _FakeFred({"X": pd.Series([1.0, float("nan"), 3.0], index=idx)})

    out = _vintage_series(fred, "X", date(2026, 1, 1), as_of)

    assert list(out) == [1.0, 3.0]


def test_vintage_series_returns_none_when_all_pulls_empty() -> None:
    fred = _FakeFred({"X": pd.Series(dtype="float64")})
    assert _vintage_series(fred, "X", date(2026, 1, 1), date(2026, 3, 31)) is None


# ---------------------------------------------------------------------------
# _latest_value: rounding + lookback window
# ---------------------------------------------------------------------------


def test_latest_value_returns_last_observation_rounded_to_4dp() -> None:
    as_of = date(2026, 3, 31)
    fred = _FakeFred({"DFF": _monthly_series([5.123456, 5.234561], as_of)})

    val = _latest_value(fred, "DFF", as_of)

    assert val == round(5.234561, 4)
    # lookback default 90 days -> observation_start = as_of - 90d.
    assert fred.calls[0]["observation_start"] == as_of - timedelta(days=90)
    assert fred.calls[0]["realtime_start"] == as_of.isoformat()


def test_latest_value_honors_custom_lookback() -> None:
    as_of = date(2026, 3, 31)
    fred = _FakeFred({"GDP": _monthly_series([1.0, 2.0], as_of)})

    _latest_value(fred, "GDP", as_of, lookback_days=180)

    assert fred.calls[0]["observation_start"] == as_of - timedelta(days=180)


def test_latest_value_none_when_series_empty() -> None:
    fred = _FakeFred({"DFF": pd.Series(dtype="float64")})
    assert _latest_value(fred, "DFF", date(2026, 3, 31)) is None


# ---------------------------------------------------------------------------
# _yoy_change: 12-month-ago nearest lookup, rounding to 1dp
# ---------------------------------------------------------------------------


def test_yoy_change_uses_observation_nearest_one_year_back() -> None:
    as_of = date(2026, 3, 31)
    # 13 monthly points: value 100 a year ago, 110 now -> +10.0% YoY.
    values = [100.0 + i for i in range(13)]
    # Force first point ~12 months before as_of and last == as_of month.
    idx = pd.date_range(end=pd.Timestamp("2026-03-01"), periods=13, freq="MS")
    series = pd.Series(values, index=idx)
    fred = _FakeFred({"CPIAUCSL": series})

    yoy = _yoy_change(fred, "CPIAUCSL", as_of)

    # current=112 (last). target = as_of - 365d = 2025-03-31; the nearest index
    # is the 2025-04-01 observation (value 101), not 2025-03-01.
    assert yoy == round((112.0 - 101.0) / 101.0 * 100, 1)
    # 400-day lookback window for YoY.
    assert fred.calls[0]["observation_start"] == as_of - timedelta(days=400)


def test_yoy_change_none_with_single_point() -> None:
    as_of = date(2026, 3, 31)
    fred = _FakeFred({"X": _monthly_series([100.0], as_of)})
    assert _yoy_change(fred, "X", as_of) is None


def test_yoy_change_none_when_past_is_zero() -> None:
    as_of = date(2026, 3, 31)
    idx = pd.date_range(end=pd.Timestamp("2026-03-01"), periods=13, freq="MS")
    # Put 0.0 at index 1 (the 2025-04-01 point that "nearest" selects).
    values = [1.0] * 13
    values[1] = 0.0
    series = pd.Series(values, index=idx)
    fred = _FakeFred({"X": series})
    # The nearest-to-1y-ago point is 0 -> division guard returns None.
    assert _yoy_change(fred, "X", as_of) is None


# ---------------------------------------------------------------------------
# _mom_change: last two observations, rounding to 1dp
# ---------------------------------------------------------------------------


def test_mom_change_uses_last_two_observations() -> None:
    as_of = date(2026, 3, 31)
    fred = _FakeFred({"CPIAUCSL": _monthly_series([200.0, 210.0], as_of)})

    mom = _mom_change(fred, "CPIAUCSL", as_of)

    assert mom == round((210.0 - 200.0) / 200.0 * 100, 1)
    assert fred.calls[0]["observation_start"] == as_of - timedelta(days=90)
    assert fred.calls[0]["realtime_start"] == as_of.isoformat()


def test_mom_change_none_with_single_point() -> None:
    as_of = date(2026, 3, 31)
    fred = _FakeFred({"X": _monthly_series([100.0], as_of)})
    assert _mom_change(fred, "X", as_of) is None


def test_mom_change_none_when_prev_is_zero() -> None:
    as_of = date(2026, 3, 31)
    fred = _FakeFred({"X": _monthly_series([0.0, 5.0], as_of)})
    assert _mom_change(fred, "X", as_of) is None
