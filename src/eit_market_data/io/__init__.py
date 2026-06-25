"""I/O helpers for materializing on-disk artifacts from snapshot bundles.

Currently houses the per-ticker daily OHLCV price store builder. The output CSV
and ``_manifest.json`` layout is a consumer contract (``eit-research`` reads the
store), so changes here must preserve byte-identical output for existing data.
"""

from __future__ import annotations

from eit_market_data.io.daily_prices import (
    MANIFEST_FILENAME,
    PRICE_STORE_COLUMNS,
    STORE_SUBDIR,
    Bar,
    StoreResult,
    build_daily_price_store,
    merge_bars,
    normalize_store_ticker,
)

__all__ = [
    "Bar",
    "StoreResult",
    "PRICE_STORE_COLUMNS",
    "STORE_SUBDIR",
    "MANIFEST_FILENAME",
    "build_daily_price_store",
    "merge_bars",
    "normalize_store_ticker",
]
