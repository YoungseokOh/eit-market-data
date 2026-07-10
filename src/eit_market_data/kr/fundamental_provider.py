"""Composite Korean fundamental provider.

Combines DART quarterly statements with public market snapshots so the
result matches the fields expected by ``eit-research``.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from datetime import date
from typing import Any

from eit_market_data.kr.market_helpers import (
    date_to_yyyymmdd,
    fetch_market_cap_frame,
    fetch_market_fundamental_frame,
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
        raise_on_error: bool = False,
    ) -> None:
        if dart_provider is None:
            from eit_market_data.kr.dart_provider import DartProvider

            dart_provider = DartProvider()

        self._dart = dart_provider
        self._price_provider = price_provider
        self._use_market_snapshot = use_market_snapshot
        self._raise_on_error = raise_on_error
        self._market_cap_cache: dict[str, Any] = {}
        self._market_cap_cache_lock = threading.Lock()
        # Per-date KRX market-fundamental frame cache (carries the DPS column).
        self._dps_frame_cache: dict[str, Any] = {}
        self._dps_frame_cache_lock = threading.Lock()
        self._semaphore = asyncio.Semaphore(16)
        # Cache final FundamentalData by (ticker, as_of) so sector_avg_tasks
        # that call fetch_fundamentals() a second time get instant hits.
        self._fundamentals_cache: dict[tuple[str, date], FundamentalData] = {}

    async def fetch_fundamentals(
        self,
        ticker: str,
        as_of: date,
        n_quarters: int = 8,
    ) -> FundamentalData:
        norm_ticker = normalize_ticker(ticker)
        key = (norm_ticker, as_of)

        if key in self._fundamentals_cache:
            return self._fundamentals_cache[key]

        async with self._semaphore:
            # Re-check inside semaphore in case another coroutine just populated it
            if key in self._fundamentals_cache:
                return self._fundamentals_cache[key]

            dart_task = asyncio.create_task(
                self._dart.fetch_fundamentals(norm_ticker, as_of, n_quarters=n_quarters)
            )
            market_task = asyncio.create_task(self._fetch_market_snapshot(norm_ticker, as_of))
            price_task = asyncio.create_task(self._fetch_price_snapshot(norm_ticker, as_of))
            dps_task = asyncio.create_task(self._fetch_dps_snapshot(norm_ticker, as_of))
            try:
                dart_fundamentals, market_snapshot, price_snapshot, dps_snapshot = (
                    await asyncio.gather(
                        dart_task,
                        market_task,
                        price_task,
                        dps_task,
                    )
                )
            except Exception as e:
                logger.warning(
                    "Composite KR fundamentals fetch failed for %s: %s",
                    norm_ticker,
                    e,
                )
                if self._raise_on_error:
                    raise
                return FundamentalData(ticker=norm_ticker)

            if market_snapshot.get("issued_shares") is None:
                issued_shares = await self._fetch_dart_issued_shares(norm_ticker, as_of)
                last_close = (
                    market_snapshot.get("last_close_price")
                    or price_snapshot.get("last_close_price")
                )
                if issued_shares is not None:
                    market_snapshot["issued_shares"] = issued_shares
                    if market_snapshot.get("market_cap") is None and last_close:
                        market_snapshot["market_cap"] = float(last_close) * issued_shares

        result = self._merge_fundamentals(
            dart_fundamentals, market_snapshot, price_snapshot, dps_snapshot
        )
        self._fundamentals_cache[key] = result
        return result

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

    async def _fetch_dps_snapshot(self, ticker: str, as_of: date) -> dict[str, float | None]:
        """PIT annual cash dividend-per-share from the KRX market-fundamental snapshot.

        pykrx ``get_market_fundamental(date, market)`` exposes a ``DPS`` column —
        the trailing annual cash dividend per share reflected in the DIV yield as
        of ``date``. Reading it at ``as_of`` is point-in-time safe (it is the
        value published/known on that date). Best-effort: a missing/failed KRX
        snapshot yields ``None`` rather than raising, since DPS is additive.
        """
        if not self._use_market_snapshot:
            return {"dividends_per_share": None}
        return await asyncio.to_thread(self._fetch_dps_snapshot_sync, ticker, as_of)

    def _fetch_dps_snapshot_sync(self, ticker: str, as_of: date) -> dict[str, float | None]:
        effective_as_of = min(as_of, date.today())
        frame = self._market_fundamental_frame(effective_as_of)
        if frame is None or ticker not in frame.index:
            return {"dividends_per_share": None}
        try:
            dps_val = frame.loc[ticker, "DPS"]
            # A duplicate ticker across KOSPI/KOSDAQ frames yields a Series; take
            # the first finite value.
            if hasattr(dps_val, "iloc"):
                dps_val = next((v for v in dps_val if v is not None), None)
            dps = float(dps_val) if dps_val is not None else None
        except Exception:
            return {"dividends_per_share": None}
        # DPS 0 means "no cash dividend for the trailing year" — a real datum, not
        # missing — but store None so a genuine zero is not confused downstream
        # with a paid dividend; the consumer treats absent DPS as no dividend.
        return {"dividends_per_share": dps if dps and dps > 0 else None}

    def _market_fundamental_frame(self, trade_date: date):  # noqa: ANN202
        cache_key = date_to_yyyymmdd(trade_date)
        with self._dps_frame_cache_lock:
            if cache_key in self._dps_frame_cache:
                return self._dps_frame_cache[cache_key]
            frames: list[Any] = []
            for market in ("KOSPI", "KOSDAQ"):
                try:
                    frame = fetch_market_fundamental_frame(trade_date, market)
                except Exception as exc:
                    logger.warning(
                        "KR market-fundamental (DPS) fetch failed for %s: %s", market, exc
                    )
                    frame = None
                if frame is None or frame.empty or "DPS" not in frame.columns:
                    continue
                normalized = frame.copy()
                normalized.index = normalized.index.map(lambda value: normalize_ticker(str(value)))
                frames.append(normalized)
            result = None
            if frames:
                try:
                    import pandas as pd

                    result = pd.concat(frames)
                    result = result[~result.index.duplicated(keep="first")]
                except Exception:
                    result = frames[0]
            self._dps_frame_cache[cache_key] = result
            return result

    def _fetch_market_snapshot_sync(self, ticker: str, as_of: date) -> dict[str, float | None]:
        # Market-cap snapshots are month/day keyed already, so use the snapshot
        # date directly instead of re-fetching per-ticker OHLCV to discover the
        # last trading day. Last close is supplied by the price provider path.
        effective_as_of = min(as_of, date.today())
        market_cap = None
        issued_shares = None
        frame = self._market_cap_frame(effective_as_of)
        row = None
        if frame is not None and ticker in frame.index:
            row = frame.loc[ticker]
        elif self._use_market_snapshot:
            row = self._fetch_remote_market_cap_row(ticker, effective_as_of)

        if row is not None:
            cap_val = row.get("시가총액", 0) or 0
            shares_val = row.get("상장주식수", 0) or 0
            market_cap = float(cap_val) if cap_val else None
            issued_shares = float(shares_val) if shares_val else None

        return {
            "last_close_price": None,
            "market_cap": market_cap,
            "issued_shares": issued_shares,
        }

    def _market_cap_frame(self, trade_date: date):  # noqa: ANN202
        cache_key = date_to_yyyymmdd(trade_date)
        with self._market_cap_cache_lock:
            if cache_key in self._market_cap_cache:
                return self._market_cap_cache[cache_key]
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
            result = None
            if frames:
                try:
                    import pandas as pd

                    result = pd.concat(frames)
                except Exception:
                    result = frames[0]
            self._market_cap_cache[cache_key] = result
            return result

    def _fetch_remote_market_cap_row(self, ticker: str, as_of: date):  # noqa: ANN202
        for market in ("KOSPI", "KOSDAQ"):
            try:
                frame = fetch_market_cap_frame(as_of, market, use_local=False)
            except Exception:
                frame = None
            if frame is None or frame.empty:
                continue

            normalized = frame.copy()
            if "종목코드" in normalized.columns:
                normalized["종목코드"] = normalized["종목코드"].map(
                    lambda value: normalize_ticker(str(value))
                )
                normalized = normalized.set_index("종목코드", drop=True)
            else:
                normalized.index = normalized.index.map(
                    lambda value: normalize_ticker(str(value))
                )

            if ticker in normalized.index:
                return normalized.loc[ticker]
        return None

    async def _fetch_dart_issued_shares(
        self,
        ticker: str,
        as_of: date,
    ) -> float | None:
        fetcher = getattr(self._dart, "fetch_issued_shares", None)
        if not callable(fetcher):
            return None
        try:
            value = await fetcher(ticker, as_of)
        except Exception as exc:
            logger.warning("DART issued shares fallback failed for %s: %s", ticker, exc)
            return None
        return float(value) if value else None

    def _merge_fundamentals(
        self,
        dart_fundamentals: FundamentalData,
        market_snapshot: dict[str, float | None],
        price_snapshot: dict[str, float | None] | None = None,
        dps_snapshot: dict[str, float | None] | None = None,
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

            # Prefer DART's natively-reported EPS (already populated on the
            # quarter in native KRW/share); only DERIVE a fallback when it is
            # missing. net_income is stored in raw KRW (won) and issued_shares
            # is a raw share count, so the derived EPS is simply
            # net_income / issued_shares with NO unit factor.
            #
            # (The previous code multiplied by 1_000_000 assuming net_income was
            # in KRW millions; combined with the parser actually storing KRW
            # thousands, the derived EPS came out ~1000x too large. Both the
            # parser unit and this factor are now corrected to raw won.)
            updated_quarters = []
            for quarter in quarters:
                if quarter.eps is None and quarter.net_income and issued_shares and issued_shares > 0:
                    eps = round(quarter.net_income / issued_shares, 2)
                    updated_quarters.append(quarter.model_copy(update={"eps": eps}))
                else:
                    updated_quarters.append(quarter)
            quarters = updated_quarters

        # Attach the PIT annual cash DPS to the most recent visible quarter only.
        # DART quarters arrive sorted newest-first; the annual figure belongs to
        # the latest report, and filling every quarter would misread as a
        # per-quarter dividend. Additive: only set when currently unpopulated.
        dps = (dps_snapshot or {}).get("dividends_per_share")
        if dps is not None and quarters and quarters[0].dividends_per_share is None:
            quarters = [
                quarters[0].model_copy(update={"dividends_per_share": dps}),
                *quarters[1:],
            ]

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
