#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import csv
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from dotenv import load_dotenv

load_dotenv(PROJECT_ROOT / ".env")

from eit_market_data.kr.market_helpers import normalize_ticker
from eit_market_data.kr.news_catalog import KrNewsCatalogStore
from eit_market_data.kr.naver_news_provider import NaverArchiveNewsProvider


def load_tickers(path: Path) -> list[str]:
    with path.open(newline="", encoding="utf-8") as handle:
        return [
            normalize_ticker(str(row.get("ticker", "")).strip())
            for row in csv.DictReader(handle)
            if str(row.get("ticker", "")).strip()
        ]


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
