"""Composite Korean fundamental provider.

Combines DART quarterly statements with public market snapshots so the
result matches the fields expected by ``eit-research``.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, timedelta
from typing import Any

from eit_market_data.kr.market_helpers import (
    date_to_yyyymmdd,
    fetch_market_cap_frame,
    fetch_stock_ohlcv_frame,
    latest_krx_trading_day,
    normalize_ticker,
)
from eit_market_data.schemas.snapshot import FundamentalData

logger = logging.getLogger(__name__)


class CompositeKrFundamentalProvider:
    """Merge DART fundamentals with recent market snapshot fields."""

    def __init__(
        self,
        dart_provider: Any | None = None,
        price_provider: Any | None = None,
        *,
        use_market_snapshot: bool = True,
    ) -> None:
        if dart_provider is None:
            from eit_market_data.kr.dart_provider import DartProvider

            dart_provider = DartProvider()

        self._dart = dart_provider
        self._price_provider = price_provider
        self._use_market_snapshot = use_market_snapshot
        self._market_cap_cache: dict[str, Any] = {}
        self._semaphore = asyncio.Semaphore(2)

    async def fetch_fundamentals(
        self,
        ticker: str,
        as_of: date,
        n_quarters: int = 8,
    ) -> FundamentalData:
        norm_ticker = normalize_ticker(ticker)
        async with self._semaphore:
            dart_task = asyncio.create_task(
                self._dart.fetch_fundamentals(norm_ticker, as_of, n_quarters=n_quarters)
            )
            market_task = asyncio.create_task(self._fetch_market_snapshot(norm_ticker, as_of))
            price_task = asyncio.create_task(self._fetch_price_snapshot(norm_ticker, as_of))
            try:
                dart_fundamentals, market_snapshot, price_snapshot = await asyncio.gather(
                    dart_task,
                    market_task,
                    price_task,
                )
            except Exception as e:
                logger.warning(
                    "Composite KR fundamentals fetch failed for %s: %s",
                    norm_ticker,
                    e,
                )
                return FundamentalData(ticker=norm_ticker)

        return self._merge_fundamentals(dart_fundamentals, market_snapshot, price_snapshot)

    async def _fetch_market_snapshot(
        self, ticker: str, as_of: date
    ) -> dict[str, float | None]:
        if not self._use_market_snapshot:
            return {
                "last_close_price": None,
                "market_cap": None,
                "issued_shares": None,
            }
        return await asyncio.to_thread(self._fetch_market_snapshot_sync, ticker, as_of)

    async def _fetch_price_snapshot(self, ticker: str, as_of: date) -> dict[str, float | None]:
        if self._price_provider is None or not hasattr(self._price_provider, "fetch_prices"):
            return {"last_close_price": None}
        try:
            bars = await self._price_provider.fetch_prices(ticker, as_of, lookback_days=10)
        except Exception as exc:
            logger.warning("KR price snapshot fetch failed for %s: %s", ticker, exc)
            return {"last_close_price": None}
        if not bars:
            return {"last_close_price": None}
        return {"last_close_price": bars[-1].close}

    def _fetch_market_snapshot_sync(self, ticker: str, as_of: date) -> dict[str, float | None]:
        # Clamp to today so future decision dates (end-of-month) can still
        # fetch the most recently available market data.
        effective_as_of = min(as_of, date.today())
        trade_date = latest_krx_trading_day(ticker, effective_as_of)
        if trade_date is None:
            return {
                "last_close_price": None,
                "market_cap": None,
                "issued_shares": None,
            }

        start = trade_date - timedelta(days=1)
        price_df, _source = fetch_stock_ohlcv_frame(
            ticker,
            start,
            trade_date,
        )
        last_close: float | None = None
        if price_df is not None and not price_df.empty:
            row = price_df.iloc[-1]
            close_val = row.get("종가", row.get("Close", 0)) or 0
            last_close = float(close_val) if close_val else None

        market_cap = None
        issued_shares = None
        frame = self._market_cap_frame(trade_date)
        if frame is not None and ticker in frame.index:
            row = frame.loc[ticker]
            cap_val = row.get("시가총액", 0) or 0
            shares_val = row.get("상장주식수", 0) or 0
            market_cap = float(cap_val) if cap_val else None
            issued_shares = float(shares_val) if shares_val else None

        return {
            "last_close_price": last_close,
            "market_cap": market_cap,
            "issued_shares": issued_shares,
        }

    def _market_cap_frame(self, trade_date: date):  # noqa: ANN202
        cache_key = date_to_yyyymmdd(trade_date)
        if cache_key not in self._market_cap_cache:
            frames: list[Any] = []
            for market in ("KOSPI", "KOSDAQ"):
                try:
                    frame = fetch_market_cap_frame(trade_date, market)
                except Exception:
                    frame = None
                if frame is None or frame.empty:
                    continue
                normalized = frame.copy()
                normalized.index = normalized.index.map(lambda value: normalize_ticker(str(value)))
                frames.append(normalized)
            self._market_cap_cache[cache_key] = None
            if frames:
                try:
                    import pandas as pd

                    self._market_cap_cache[cache_key] = pd.concat(frames)
                except Exception:
                    self._market_cap_cache[cache_key] = frames[0]
        return self._market_cap_cache[cache_key]

    def _merge_fundamentals(
        self,
        dart_fundamentals: FundamentalData,
        market_snapshot: dict[str, float | None],
        price_snapshot: dict[str, float | None] | None = None,
    ) -> FundamentalData:
        issued_shares = market_snapshot.get("issued_shares")
        quarters = dart_fundamentals.quarters
        if issued_shares is not None and quarters:
            quarters = [
                quarter
                if quarter.issued_shares is not None
                else quarter.model_copy(update={"issued_shares": issued_shares})
                for quarter in quarters
            ]

            # Calculate EPS from net_income / issued_shares if EPS is missing
            updated_quarters = []
            for quarter in quarters:
                if quarter.eps is None and quarter.net_income and issued_shares and issued_shares > 0:
                    # net_income is in KRW millions, issued_shares is in units
                    # eps = (net_income * 1,000,000) / issued_shares
                    eps = round((quarter.net_income * 1_000_000) / issued_shares, 0)
                    updated_quarters.append(quarter.model_copy(update={"eps": eps}))
                else:
                    updated_quarters.append(quarter)
            quarters = updated_quarters

        return dart_fundamentals.model_copy(
            update={
                "ticker": normalize_ticker(dart_fundamentals.ticker),
                "quarters": quarters,
                "market_cap": (
                    dart_fundamentals.market_cap
                    if dart_fundamentals.market_cap is not None
                    else market_snapshot.get("market_cap")
                ),
                "last_close_price": (
                    dart_fundamentals.last_close_price
                    if dart_fundamentals.last_close_price is not None
                    else (
                        market_snapshot.get("last_close_price")
                        or (price_snapshot or {}).get("last_close_price")
                    )
                ),
            }
        )
