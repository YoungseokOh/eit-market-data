"""CI-safe Korean providers that avoid KRX-authenticated endpoints."""

from __future__ import annotations

import asyncio
import csv
import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np

from eit_market_data.kr.market_helpers import normalize_ticker
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
        self._semaphore = asyncio.Semaphore(4)

    async def fetch_prices(
        self, ticker: str, as_of: date, lookback_days: int = 300
    ) -> list[PriceBar]:
        norm_ticker = normalize_ticker(ticker)
        async with self._semaphore:
            try:
                return await asyncio.to_thread(
                    self._fetch_prices_sync,
                    norm_ticker,
                    as_of,
                    lookback_days,
                )
            except Exception as exc:
                logger.warning("FDR NAVER price fetch failed for %s: %s", norm_ticker, exc)
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
        if df is None or df.empty:
            return []

        bars: list[PriceBar] = []
        for idx, row in df.iterrows():
            bar_date = idx.date() if hasattr(idx, "date") else idx
            if not isinstance(bar_date, date) or bar_date > as_of:
                continue
            bars.append(
                PriceBar(
                    date=bar_date,
                    open=round(float(row.get("Open", 0) or 0), 2),
                    high=round(float(row.get("High", 0) or 0), 2),
                    low=round(float(row.get("Low", 0) or 0), 2),
                    close=round(float(row.get("Close", 0) or 0), 2),
                    volume=float(row.get("Volume", 0) or 0),
                )
            )
        return bars[-lookback_days:]


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

        metrics: dict[str, list[float]] = {}
        for result in results:
            if isinstance(result, Exception) or not getattr(result, "quarters", None):
                continue
            quarter = result.quarters[0]
            revenue = quarter.revenue
            total_assets = quarter.total_assets
            if not revenue or not total_assets or total_assets == 0:
                continue

            def _add(key: str, value: float | None) -> None:
                if value is not None:
                    metrics.setdefault(key, []).append(value)

            _add("roa", (quarter.net_income or 0) / total_assets)
            _add(
                "roe",
                (quarter.net_income or 0) / quarter.total_equity
                if quarter.total_equity
                else None,
            )
            _add("gross_margin", (quarter.gross_profit or 0) / revenue)
            _add("operating_margin", (quarter.operating_income or 0) / revenue)
            _add("net_margin", (quarter.net_income or 0) / revenue)
            if quarter.current_liabilities and quarter.current_liabilities > 0:
                _add("current_ratio", (quarter.current_assets or 0) / quarter.current_liabilities)
            if quarter.total_equity and quarter.total_equity > 0:
                _add("debt_to_equity", (quarter.total_debt or 0) / quarter.total_equity)
            _add("asset_turnover", revenue / total_assets)
            if result.last_close_price and quarter.eps and quarter.eps > 0:
                _add("pe_ttm", result.last_close_price / (quarter.eps * 4))

        avg_metrics = {
            key: round(float(np.mean(values)), 4)
            for key, values in metrics.items()
            if values
        }
        return SectorAverages(sector=sector, avg_metrics=avg_metrics)

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


class NullBenchmarkProvider:
    """Placeholder benchmark provider for KR CI-safe bundles."""

    async def fetch_benchmark(self, as_of: date, lookback_days: int = 300) -> list[PriceBar]:
        _ = (as_of, lookback_days)
        return []
