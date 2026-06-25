"""OHLCV DataFrame -> ``list[PriceBar]`` conversion shared across providers.

Folds the eight duplicated OHLCV loops (yfinance prices/benchmark, pykrx
prices/benchmark, ci-safe FDR prices/benchmark) into one helper. The behaviour
reproduced exactly is:

* per-row date = ``idx.date()`` when available else ``idx``;
* skip rows whose date is not a ``date`` or is after ``as_of`` (look-ahead);
* OHLC rounded to 2 decimals via ``round(float(cell), 2)``;
* missing/falsy cells fall back to ``0`` (``row.get(col, 0) or 0``);
* volume = ``float(row.get(col, 0) or 0)`` (unrounded);
* result trimmed to the last ``lookback_days`` rows (``[-lookback_days:]``).

Korean column names are tried first, then English; on an English-only frame the
Korean lookups simply miss and the English fallback is used — a no-op.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from eit_market_data.schemas.snapshot import PriceBar

# (korean, english) column-name pairs. Korean is tried first; English is the
# fallback. On an English frame the Korean key is absent so English wins.
_DEFAULT_KOREAN_COLUMNS: dict[str, tuple[str, str]] = {
    "open": ("시가", "Open"),
    "high": ("고가", "High"),
    "low": ("저가", "Low"),
    "close": ("종가", "Close"),
    "volume": ("거래량", "Volume"),
}


def _resolve(df: Any, korean: str, english: str) -> str:
    """Pick the Korean column if present on the frame, else English.

    Frames without a ``columns`` attribute (e.g. lightweight test doubles that
    only implement ``iterrows``) resolve to the English name, matching the
    English-only access pattern of the original yfinance loop.
    """
    columns = getattr(df, "columns", ())
    return korean if korean in columns else english


def price_bars_from_frame(
    df: Any,
    as_of: date,
    lookback_days: int,
    *,
    korean_columns: dict[str, tuple[str, str]] | None = None,
) -> list[PriceBar]:
    """Convert an OHLCV frame to point-in-time ``PriceBar`` rows.

    Returns an empty list when ``df`` is ``None`` or empty.
    """
    if df is None or df.empty:
        return []

    columns = korean_columns or _DEFAULT_KOREAN_COLUMNS
    open_col = _resolve(df, *columns["open"])
    high_col = _resolve(df, *columns["high"])
    low_col = _resolve(df, *columns["low"])
    close_col = _resolve(df, *columns["close"])
    volume_col = _resolve(df, *columns["volume"])

    bars: list[PriceBar] = []
    for idx, row in df.iterrows():
        bar_date = idx.date() if hasattr(idx, "date") else idx
        if not isinstance(bar_date, date) or bar_date > as_of:
            continue
        bars.append(
            PriceBar(
                date=bar_date,
                open=round(float(row.get(open_col, 0) or 0), 2),
                high=round(float(row.get(high_col, 0) or 0), 2),
                low=round(float(row.get(low_col, 0) or 0), 2),
                close=round(float(row.get(close_col, 0) or 0), 2),
                volume=float(row.get(volume_col, 0) or 0),
            )
        )

    return bars[-lookback_days:]
