#!/usr/bin/env python3
# ruff: noqa: E402
"""Materialize a per-ticker daily OHLCV price store for a market.

Thin CLI wrapper over :mod:`eit_market_data.io.daily_prices`, which holds the
Bar/seed/merge/build logic. The store is the daily-cadence companion to the
month-end snapshot bundles and writes ONE file per ticker,
``artifacts/<market>/daily_prices/<TICKER>.csv``, with the consumer schema::

    date,open,high,low,close,volume

The on-disk CSV and ``_manifest.json`` layout is a consumer contract
(``eit-research`` reads it); see the module docstring for the consistency
guarantees and merge semantics.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from dotenv import load_dotenv

load_dotenv(PROJECT_ROOT / ".env")

# Re-export the public surface so existing importers (and tests that load this
# script as a module) keep working unchanged.
from eit_market_data.io.daily_prices import (  # noqa: F401
    MANIFEST_FILENAME,
    PRICE_STORE_COLUMNS,
    SNAPSHOTS_SUBDIR,
    STORE_SUBDIR,
    Bar,
    StoreResult,
    build_daily_price_store,
    fetch_provider_bars,
    iter_bundle_snapshots,
    load_universe,
    merge_bars,
    normalize_store_ticker,
    read_existing_store,
    seed_from_bundles,
    write_store,
)

logger = logging.getLogger(__name__)

ARTIFACTS_ROOT = PROJECT_ROOT / "artifacts"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build the per-ticker daily OHLCV price store for a market."
    )
    parser.add_argument("--market", required=True, choices=["kr", "us"])
    parser.add_argument(
        "--universe",
        default=None,
        help="Universe CSV. Default: every ticker present in the monthly bundles.",
    )
    parser.add_argument("--start", default=None, help="Earliest date (YYYY-MM-DD).")
    parser.add_argument(
        "--end",
        default=None,
        help="Latest date (YYYY-MM-DD). Default: last business day.",
    )
    parser.add_argument(
        "--artifacts-root",
        default=str(ARTIFACTS_ROOT),
        help="Bundle/store root (contains <market>/snapshots and <market>/daily_prices).",
    )
    parser.add_argument(
        "--source",
        default="auto",
        choices=["auto", "bundle", "provider"],
        help="auto: bundle seed + provider tail; bundle: offline; provider: live only.",
    )
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Recompute every row instead of incrementally appending new days.",
    )
    parser.add_argument(
        "--min-rows",
        type=int,
        default=20,
        help="Report tickers with fewer than this many rows as short history.",
    )
    parser.add_argument(
        "--tail-lookback-days",
        type=int,
        default=400,
        help="Trading-day lookback requested from the live provider for the tail.",
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    result = build_daily_price_store(
        market=args.market,
        artifacts_root=Path(args.artifacts_root),
        universe_csv=Path(args.universe) if args.universe else None,
        start=date.fromisoformat(args.start) if args.start else None,
        end=date.fromisoformat(args.end) if args.end else None,
        source=args.source,
        rebuild=args.rebuild,
        min_rows=args.min_rows,
        tail_lookback_days=args.tail_lookback_days,
    )

    print(
        f"[DONE] market={result.market} store={result.store_dir} "
        f"tickers={len(result.tickers)} written={len(result.tickers) - len(result.missing)} "
        f"missing={len(result.missing)} short={len(result.short)} "
        f"rows={result.total_rows} range={result.min_date}..{result.max_date}"
    )
    if result.missing:
        print(f"[WARN] missing history: {', '.join(result.missing[:20])}")


if __name__ == "__main__":
    main()
