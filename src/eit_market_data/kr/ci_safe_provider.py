"""CI-safe Korean providers that avoid KRX-authenticated endpoints."""

from __future__ import annotations

import asyncio
import csv
import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from eit_market_data.core.price_frame import price_bars_from_frame
from eit_market_data.core.sector_math import compute_sector_averages
from eit_market_data.kr.market_helpers import fetch_index_ohlcv_frame, normalize_ticker
from eit_market_data.schemas.snapshot import (
    FilingData,
    FundamentalData,
    MacroData,
    NewsItem,
    PriceBar,
    SectorAverages,
)

logger = logging.getLogger(__name__)


class FdrNaverPriceProvider:
    """Fetch Korean stock prices via FinanceDataReader's NAVER route."""

    def __init__(self) -> None:
        self._semaphore = asyncio.Semaphore(16)
        # Per-instance cache keyed by (ticker, as_of).
        # NAVER sise.nhn always returns count=6000 bars regardless of start date,
        # so we store the full history once and slice on return.
        self._cache: dict[tuple[str, date], list[PriceBar]] = {}

    async def fetch_prices(
        self, ticker: str, as_of: date, lookback_days: int = 300
    ) -> list[PriceBar]:
        norm_ticker = normalize_ticker(ticker)
        key = (norm_ticker, as_of)

        # Fast path: already cached — no semaphore needed
        if key in self._cache:
            return self._cache[key][-lookback_days:]

        async with self._semaphore:
            # Re-check inside semaphore (another coroutine may have populated it)
            if key in self._cache:
                return self._cache[key][-lookback_days:]
            try:
                # Fetch complete history so all subsequent callers get cache hits
                all_bars = await asyncio.to_thread(
                    self._fetch_prices_sync,
                    norm_ticker,
                    as_of,
                    6000,
                )
                self._cache[key] = all_bars
                return all_bars[-lookback_days:]
            except Exception as exc:
                logger.warning("FDR NAVER price fetch failed for %s: %s", norm_ticker, exc)
                self._cache[key] = []
                return []

    def _fetch_prices_sync(
        self,
        ticker: str,
        as_of: date,
        lookback_days: int,
    ) -> list[PriceBar]:
        import FinanceDataReader as fdr

        start = as_of - timedelta(days=max(int(lookback_days * 1.8), 30))
        df = fdr.DataReader(f"NAVER:{ticker}", start.strftime("%Y-%m-%d"), as_of.strftime("%Y-%m-%d"))
        return price_bars_from_frame(df, as_of, lookback_days)


class SeedSectorProvider:
    """Static sector provider seeded from a universe CSV."""

    def __init__(
        self,
        universe_csv: str | Path | None = None,
        fundamental_provider: Any | None = None,
    ) -> None:
        self._universe_csv = Path(universe_csv) if universe_csv is not None else None
        self._fundamental_provider = fundamental_provider
        self._sector_map = self._load_sector_seed(self._universe_csv)

    async def fetch_sector_map(
        self, universe: list[str], as_of: date | None = None
    ) -> dict[str, str]:
        _ = as_of
        result: dict[str, str] = {}
        for ticker in universe:
            norm = normalize_ticker(ticker)
            result[ticker] = self._sector_map.get(norm, "General")
        return result

    async def fetch_sector_averages(
        self, sector: str, tickers: list[str], as_of: date
    ) -> SectorAverages:
        provider = self._fundamental_provider
        if provider is None or not hasattr(provider, "fetch_fundamentals"):
            return SectorAverages(sector=sector)

        tasks = [
            provider.fetch_fundamentals(normalize_ticker(ticker), as_of, n_quarters=4)
            for ticker in tickers
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        funds = [
            result
            for result in results
            if not isinstance(result, Exception)
            and isinstance(result, FundamentalData)
        ]
        return compute_sector_averages(sector, funds)

    @staticmethod
    def _load_sector_seed(path: Path | None) -> dict[str, str]:
        if path is None or not path.exists():
            return {}
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            result: dict[str, str] = {}
            for row in reader:
                ticker = normalize_ticker(str(row.get("ticker", "")).strip())
                sector = str(row.get("sector", "")).strip()
                if ticker and sector:
                    result[ticker] = sector
            return result


class NullNewsProvider:
    """Placeholder news provider for KR CI-safe bundles."""

    async def fetch_news(
        self, ticker: str, as_of: date, lookback_days: int = 30
    ) -> list[NewsItem]:
        _ = (ticker, as_of, lookback_days)
        return []


class NullDartProvider:
    """Placeholder DART provider when API key or dependency is unavailable."""

    async def fetch_fundamentals(
        self,
        ticker: str,
        as_of: date,
        n_quarters: int = 8,
    ) -> FundamentalData:
        _ = (as_of, n_quarters)
        return FundamentalData(ticker=normalize_ticker(ticker))

    async def fetch_filing(self, ticker: str, as_of: date) -> FilingData:
        _ = as_of
        return FilingData(ticker=normalize_ticker(ticker))


class NullMacroProvider:
    """Placeholder macro provider when ECOS API key is unavailable."""

    async def fetch_macro(self, as_of: date) -> MacroData:
        _ = as_of
        return MacroData()


class FdrBenchmarkProvider:
    """Fetch Korean benchmark (KOSPI/KOSDAQ) index prices via FinanceDataReader."""

    _DEFAULT_INDEX = "1001"  # KOSPI

    async def fetch_benchmark(self, as_of: date, lookback_days: int = 300) -> list[PriceBar]:
        start = as_of - timedelta(days=lookback_days + 60)
        try:
            frame, _source = await asyncio.to_thread(
                fetch_index_ohlcv_frame,
                self._DEFAULT_INDEX,
                start,
                as_of,
            )
        except Exception as exc:
            logger.warning("FDR benchmark fetch failed: %s", exc)
            return []
        return price_bars_from_frame(frame, as_of, lookback_days)


class NullBenchmarkProvider:
    """Placeholder benchmark provider for KR CI-safe bundles."""

    async def fetch_benchmark(self, as_of: date, lookback_days: int = 300) -> list[PriceBar]:
        _ = (as_of, lookback_days)
        return []
