"""pykrx-based Korean market data providers."""

from __future__ import annotations

import asyncio
import logging
from datetime import date, timedelta
from typing import Any, Callable

from eit_market_data.kr.market_helpers import (
    date_to_yyyymmdd,
    fetch_index_ohlcv_frame,
    fetch_live_sector_classification_map,
    load_sector_snapshot_map,
    normalize_ticker,
)
from eit_market_data.kr.krx_auth import KrxAuthRequired, install_pykrx_krx_session_hooks
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
    """Korean market data provider based on pykrx.

    Implements:
    - PriceProvider
    - SectorProvider
    - BenchmarkProvider
    - NewsProvider (stub: pykrx has no news API)
    """

    def __init__(
        self,
        fundamental_provider: Any | None = None,
        official_only: bool = True,
    ) -> None:
        try:
            from pykrx import stock  # noqa: F401
        except ImportError as e:
            raise ImportError(
                "pykrx is required for Korean market data. "
                "Install with: pip install -e '.[kr]'"
            ) from e
        install_pykrx_krx_session_hooks()
        self._fundamental_provider = fundamental_provider
        self._fundamental_provider_init_failed = False
        self._official_only = official_only
        self._sector_cache: dict[tuple[str, str], str] = {}
        self._logged_sector_snapshots: set[tuple[str, str]] = set()
        self._semaphore = asyncio.Semaphore(2)

    async def _run_limited(
        self, fn: Callable[..., Any], *args: Any, **kwargs: Any
    ) -> Any:
        """Run a sync pykrx call in thread with concurrency/rate limits."""
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
        """Fetch KRX OHLCV prices for a stock ticker."""
        norm_ticker = _normalize_ticker(ticker)
        try:
            return await self._run_limited(
                self._fetch_prices_sync, norm_ticker, as_of, lookback_days
            )
        except Exception as e:
            logger.warning("pykrx price fetch failed for %s: %s", norm_ticker, e)
            return []

    def _fetch_prices_sync(
        self, ticker: str, as_of: date, lookback_days: int
    ) -> list[PriceBar]:
        from pykrx import stock

        start = as_of - timedelta(days=max(int(lookback_days * 1.8), 30))
        df = stock.get_market_ohlcv_by_date(
            date_to_yyyymmdd(start),
            date_to_yyyymmdd(as_of),
            ticker,
        )
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
                    open=round(float(row.get("시가", 0) or 0), 2),
                    high=round(float(row.get("고가", 0) or 0), 2),
                    low=round(float(row.get("저가", 0) or 0), 2),
                    close=round(float(row.get("종가", 0) or 0), 2),
                    volume=float(row.get("거래량", 0) or 0),
                )
            )

        return bars[-lookback_days:]

    # ------------------------------------------------------------------
    # SectorProvider
    # ------------------------------------------------------------------

    async def fetch_sector_map(
        self, universe: list[str], as_of: date | None = None
    ) -> dict[str, str]:
        """Fetch sector names for Korean universe from pykrx."""
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
        import numpy as np

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

        metrics: dict[str, list[float]] = {}
        for result in results:
            if isinstance(result, Exception) or not isinstance(result, FundamentalData):
                continue
            if not result.quarters:
                continue
            q = result.quarters[0]
            rev = q.revenue
            ta = q.total_assets
            if not rev or not ta or ta == 0:
                continue

            def _add(key: str, val: float | None) -> None:
                if val is not None:
                    metrics.setdefault(key, []).append(val)

            _add("roa", (q.net_income or 0) / ta if ta else None)
            _add(
                "roe", (q.net_income or 0) / q.total_equity if q.total_equity else None
            )
            _add("gross_margin", (q.gross_profit or 0) / rev)
            _add("operating_margin", (q.operating_income or 0) / rev)
            _add("net_margin", (q.net_income or 0) / rev)
            if q.current_liabilities and q.current_liabilities > 0:
                _add("current_ratio", (q.current_assets or 0) / q.current_liabilities)
            if q.total_equity and q.total_equity > 0:
                _add("debt_to_equity", (q.total_debt or 0) / q.total_equity)
            _add("asset_turnover", rev / ta)
            if result.last_close_price and q.eps and q.eps > 0:
                _add("pe_ttm", result.last_close_price / (q.eps * 4))

        avg_metrics: dict[str, float] = {}
        for key, values in metrics.items():
            if values:
                avg_metrics[key] = round(float(np.mean(values)), 4)

        return SectorAverages(sector=sector, avg_metrics=avg_metrics)

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
            logger.warning("pykrx benchmark fetch failed: %s", e)
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
        if df is None or df.empty:
            return []

        open_col = "시가" if "시가" in df.columns else "Open"
        high_col = "고가" if "고가" in df.columns else "High"
        low_col = "저가" if "저가" in df.columns else "Low"
        close_col = "종가" if "종가" in df.columns else "Close"
        volume_col = "거래량" if "거래량" in df.columns else "Volume"

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

    # ------------------------------------------------------------------
    # NewsProvider
    # ------------------------------------------------------------------

    async def fetch_news(
        self, ticker: str, as_of: date, lookback_days: int = 30
    ) -> list[NewsItem]:
        """pykrx does not provide news API."""
        _ = (ticker, as_of, lookback_days)
        return []


def get_kr_universe(
    as_of: date,
    top_n: int = 50,
    markets: list[str] | None = None,
) -> list[str]:
    """Return top_n Korean tickers by market cap as of the given date.

    Uses pykrx to dynamically fetch KOSPI + KOSDAQ tickers.
    Returns 6-digit stock codes sorted by market cap descending.
    """
    from pykrx import stock

    install_pykrx_krx_session_hooks()

    if markets is None:
        markets = ["KOSPI", "KOSDAQ"]

    date_str = as_of.strftime("%Y%m%d")
    all_tickers: list[str] = []
    for market in markets:
        try:
            tickers = stock.get_market_ticker_list(date_str, market=market)
            all_tickers.extend(tickers)
        except Exception:
            pass

    caps: dict[str, int] = {}
    for market in markets:
        try:
            df_cap = stock.get_market_cap(date_str, market=market)
            for ticker, row in df_cap.iterrows():
                cap = row.get("시가총액", 0)
                if cap and cap > 0:
                    caps[str(ticker)] = int(cap)
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
