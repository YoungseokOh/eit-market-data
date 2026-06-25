"""Korean market data providers."""

from __future__ import annotations

import asyncio
import logging
from datetime import date, timedelta
from typing import Any, Callable

from eit_market_data.kr.market_helpers import (
    date_to_yyyymmdd,
    fetch_stock_ohlcv_frame,
    fetch_index_ohlcv_frame,
    fetch_live_sector_classification_map,
    load_sector_snapshot_map,
    normalize_ticker,
    fetch_market_cap_frame,
    fetch_market_ticker_list,
)
from eit_market_data.core.price_frame import price_bars_from_frame
from eit_market_data.core.sector_math import compute_sector_averages
from eit_market_data.kr.krx_auth import KrxAuthRequired
from eit_market_data.schemas.snapshot import (
    FundamentalData,
    NewsItem,
    PriceBar,
    SectorAverages,
)

logger = logging.getLogger(__name__)

_PYKRX_DELAY_SECONDS = 0.5


def _normalize_ticker(ticker: str) -> str:
    """Normalize ticker to 6-digit KRX stock code."""
    return normalize_ticker(ticker)


class PykrxProvider:
    """Korean market data provider backed by pykrx.

    Implements:
    - PriceProvider
    - SectorProvider
    - BenchmarkProvider
    - NewsProvider (delegates to Naver Finance)
    """

    def __init__(
        self,
        fundamental_provider: Any | None = None,
        official_only: bool = True,
    ) -> None:
        self._fundamental_provider = fundamental_provider
        self._fundamental_provider_init_failed = False
        self._official_only = official_only
        self._sector_cache: dict[tuple[str, str], str] = {}
        self._logged_sector_snapshots: set[tuple[str, str]] = set()
        self._semaphore = asyncio.Semaphore(2)

        # Initialize news provider
        from eit_market_data.kr.naver_news_provider import NaverNewsProvider

        self._news_provider = NaverNewsProvider()

    async def _run_limited(
        self, fn: Callable[..., Any], *args: Any, **kwargs: Any
    ) -> Any:
        """Run a sync market-data call in thread with concurrency/rate limits."""
        async with self._semaphore:
            try:
                return await asyncio.to_thread(fn, *args, **kwargs)
            finally:
                await asyncio.sleep(_PYKRX_DELAY_SECONDS)

    # ------------------------------------------------------------------
    # PriceProvider
    # ------------------------------------------------------------------

    async def fetch_prices(
        self, ticker: str, as_of: date, lookback_days: int = 300
    ) -> list[PriceBar]:
        """Fetch Korean stock OHLCV prices for a ticker."""
        norm_ticker = _normalize_ticker(ticker)
        try:
            return await self._run_limited(
                self._fetch_prices_sync, norm_ticker, as_of, lookback_days
            )
        except Exception as e:
            logger.warning("KR price fetch failed for %s: %s", norm_ticker, e)
            return []

    def _fetch_prices_sync(
        self, ticker: str, as_of: date, lookback_days: int
    ) -> list[PriceBar]:
        start = as_of - timedelta(days=max(int(lookback_days * 1.8), 30))
        df, _source = fetch_stock_ohlcv_frame(
            ticker,
            start,
            as_of,
            logger_=logger,
        )
        return price_bars_from_frame(df, as_of, lookback_days)

    # ------------------------------------------------------------------
    # SectorProvider
    # ------------------------------------------------------------------

    async def fetch_sector_map(
        self, universe: list[str], as_of: date | None = None
    ) -> dict[str, str]:
        """Fetch sector names for the Korean universe."""
        result: dict[str, str] = {}
        tickers = [_normalize_ticker(t) for t in universe]
        effective_as_of = as_of or date.today()
        date_str = date_to_yyyymmdd(effective_as_of)

        unresolved = [t for t in tickers if (t, date_str) not in self._sector_cache]
        if unresolved:
            for market in ("KOSPI", "KOSDAQ"):
                snapshot_map, snapshot_path = load_sector_snapshot_map(
                    market,
                    effective_as_of,
                    logger_=logger,
                    official_only=self._official_only,
                )
                if snapshot_path is not None:
                    snapshot_key = (market, snapshot_path.name)
                    if snapshot_key not in self._logged_sector_snapshots:
                        logger.warning(
                            "Using cached sector snapshot %s for %s as of %s",
                            snapshot_path.name,
                            market,
                            effective_as_of,
                        )
                        self._logged_sector_snapshots.add(snapshot_key)
                for t in unresolved:
                    if t in snapshot_map:
                        self._sector_cache[(t, date_str)] = snapshot_map[t]
                unresolved = [
                    t for t in unresolved if (t, date_str) not in self._sector_cache
                ]
                if not unresolved:
                    break

            if unresolved:
                kospi_map = await self._run_limited(
                    self._fetch_sector_classification_sync, "KOSPI", effective_as_of
                )
                for t in unresolved:
                    if t in kospi_map:
                        self._sector_cache[(t, date_str)] = kospi_map[t]

            unresolved = [
                t for t in unresolved if (t, date_str) not in self._sector_cache
            ]
            if unresolved:
                kosdaq_map = await self._run_limited(
                    self._fetch_sector_classification_sync, "KOSDAQ", effective_as_of
                )
                for t in unresolved:
                    if t in kosdaq_map:
                        self._sector_cache[(t, date_str)] = kosdaq_map[t]

        for raw, norm in zip(universe, tickers, strict=True):
            result[raw] = self._sector_cache.get((norm, date_str), "General")

        return result

    def _fetch_sector_classification_sync(
        self, market: str, as_of: date
    ) -> dict[str, str]:
        sector_map, _query_day = fetch_live_sector_classification_map(
            market,
            as_of,
            logger_=logger,
        )
        return sector_map

    async def fetch_sector_averages(
        self, sector: str, tickers: list[str], as_of: date
    ) -> SectorAverages:
        """Compute sector average metrics from quarterly fundamentals."""
        provider = self._fundamental_provider
        if provider is None and not self._fundamental_provider_init_failed:
            try:
                from eit_market_data.kr.fundamental_provider import (
                    CompositeKrFundamentalProvider,
                )

                provider = CompositeKrFundamentalProvider()
                self._fundamental_provider = provider
            except Exception:
                self._fundamental_provider_init_failed = True
                provider = None
        if provider is None or not hasattr(provider, "fetch_fundamentals"):
            return SectorAverages(sector=sector)

        tasks = [
            provider.fetch_fundamentals(_normalize_ticker(t), as_of, n_quarters=4)
            for t in tickers
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        funds = [
            result
            for result in results
            if not isinstance(result, Exception)
            and isinstance(result, FundamentalData)
        ]
        return compute_sector_averages(sector, funds)

    # ------------------------------------------------------------------
    # BenchmarkProvider
    # ------------------------------------------------------------------

    async def fetch_benchmark(
        self, as_of: date, lookback_days: int = 300
    ) -> list[PriceBar]:
        """Fetch KOSPI index (code 1001) as benchmark."""
        try:
            return await self._run_limited(
                self._fetch_benchmark_sync, as_of, lookback_days
            )
        except KrxAuthRequired:
            if self._official_only:
                raise
            return []
        except Exception as e:
            logger.warning("KR benchmark fetch failed: %s", e)
            return []

    def _fetch_benchmark_sync(self, as_of: date, lookback_days: int) -> list[PriceBar]:
        start = as_of - timedelta(days=max(int(lookback_days * 1.8), 30))
        df, _source = fetch_index_ohlcv_frame(
            "1001",
            start,
            as_of,
            logger_=logger,
            official_only=self._official_only,
        )
        return price_bars_from_frame(df, as_of, lookback_days)

    # ------------------------------------------------------------------
    # NewsProvider
    # ------------------------------------------------------------------

    async def fetch_news(
        self, ticker: str, as_of: date, lookback_days: int = 30
    ) -> list[NewsItem]:
        """Fetch Korean stock news from Naver Finance."""
        return await self._news_provider.fetch_news(ticker, as_of, lookback_days)


def get_kr_universe(
    as_of: date,
    top_n: int = 50,
    markets: list[str] | None = None,
) -> list[str]:
    """Return top_n Korean tickers by market cap as of the given date.

    Uses pykrx listings to dynamically fetch KOSPI + KOSDAQ tickers.
    Returns 6-digit stock codes sorted by market cap descending.
    """
    if markets is None:
        markets = ["KOSPI", "KOSDAQ"]

    all_tickers: list[str] = []
    for market in markets:
        try:
            tickers = fetch_market_ticker_list(as_of, market)
            all_tickers.extend(tickers)
        except Exception:
            pass

    caps: dict[str, int] = {}
    for market in markets:
        try:
            df_cap = fetch_market_cap_frame(as_of, market)
            for ticker, row in df_cap.iterrows():
                cap = row.get("시가총액", 0)
                if cap and cap > 0:
                    caps[normalize_ticker(str(ticker))] = int(cap)
        except Exception:
            pass

    filtered = [t for t in all_tickers if t in caps]
    if not filtered:
        logger.warning(
            "get_kr_universe: market cap data unavailable, returning unsorted tickers"
        )
        return all_tickers[:top_n]

    sorted_tickers = sorted(filtered, key=lambda t: caps.get(t, 0), reverse=True)
    return sorted_tickers[:top_n]
