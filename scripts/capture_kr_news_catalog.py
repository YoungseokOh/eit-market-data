#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import bootstrap, load_tickers as _load_tickers

PROJECT_ROOT = bootstrap()

from eit_market_data.kr.news_catalog import KrNewsCatalogStore
from eit_market_data.kr.naver_news_provider import NaverArchiveNewsProvider


def load_tickers(path: Path) -> list[str]:
    return _load_tickers(path, "kr")


async def capture_catalog(
    *,
    storage_root: Path,
    universe_csv: Path,
    as_of: date,
    lookback_days: int,
) -> None:
    store = KrNewsCatalogStore(storage_root)
    provider = NaverArchiveNewsProvider(
        max_pages=200,
        page_delay_seconds=0.1,
        require_full_coverage=False,
        raise_on_error=True,
    )
    tickers = load_tickers(universe_csv)
    for ticker in tickers:
        await store.capture_archive_window(
            provider=provider,
            ticker=ticker,
            as_of=as_of,
            lookback_days=lookback_days,
        )
        print(ticker)


def main() -> None:
    parser = argparse.ArgumentParser(description="Capture day-level KR news catalogs.")
    parser.add_argument("--storage-root", required=True, help="Persistent root for local catalogs.")
    parser.add_argument("--as-of", required=True, help="Capture date in YYYY-MM-DD.")
    parser.add_argument(
        "--universe-csv",
        default=str(PROJECT_ROOT / "universes" / "kr_universe.csv"),
        help="Ticker universe CSV with a ticker column.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=30,
        help="Archive lookback window used while seeding daily catalogs.",
    )
    args = parser.parse_args()

    asyncio.run(
        capture_catalog(
            storage_root=Path(args.storage_root),
            universe_csv=Path(args.universe_csv),
            as_of=date.fromisoformat(args.as_of),
            lookback_days=args.lookback_days,
        )
    )


if __name__ == "__main__":
    main()
