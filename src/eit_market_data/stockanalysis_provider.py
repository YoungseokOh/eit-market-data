"""Supplementary US price provider backed by stockanalysis.com.

Purpose: recover daily OHLCV history for US large-caps that yfinance
retroactively **purges** once they delist / are acquired / go private (Yahoo
returns HTTP 404 "Quote not found" for the entire history, including the
pre-delisting years). This is the survivorship-bias hole documented in
``docs/us_delisted_supplementary_source.md``. stockanalysis.com retains a
material share of these delisted symbols behind a free, key-less JSON API and
is used **only as a fallback** for tickers the primary provider (yfinance)
returns empty for — see :class:`FallbackPriceProvider`.

Bar schema / adjustment convention (must match the yfinance ``auto_adjust=True``
path that produced the existing bundles, so output stays byte-compatible):

* The API row exposes raw ``o/h/l/c``, an **adjusted close** ``a`` (split +
  dividend, total-return), and raw volume ``v``. Empirically ``a`` matches
  yfinance's ``auto_adjust=True`` close to <0.08% on still-listed controls
  (AAPL/MSFT/JPM/XOM/KO/JNJ, 2019-2022), while raw ``c`` diverges up to ~40%.
* We reproduce yfinance's proportional back-adjustment by scaling OHL by the
  per-row factor ``a / c`` and setting ``close = a``. Volume is left raw
  (yfinance ``auto_adjust=True`` does not adjust volume).
* OHLC rounded to 2 decimals, exactly like
  :func:`eit_market_data.core.price_frame.price_bars_from_frame`.

Note: this endpoint's ticker keys are the **historical delisted symbols**
(e.g. ``TWTR``, ``SIVB``, ``ATVI``). Some acquired names are not retrievable
(symbol reused by a newer listing, or purged) and are reported as the residual
survivorship gap; the provider returns ``[]`` for them and never raises.
"""

from __future__ import annotations

import asyncio
import json
import logging
import urllib.error
import urllib.request
from datetime import date  # noqa: TC003
from typing import Any

from eit_market_data.providers import PriceProvider  # noqa: TC001
from eit_market_data.schemas.snapshot import PriceBar

logger = logging.getLogger(__name__)

# Politeness: stockanalysis.com is a free source. Cap concurrency and space
# requests so a batch backfill never hammers it.
_SEMAPHORE = asyncio.Semaphore(2)
_DELAY_SECONDS = 0.5
_HTTP_TIMEOUT = 25.0
_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)
_API_TEMPLATE = (
    "https://stockanalysis.com/api/symbol/s/{ticker}/history"
    "?range={range}&period=Daily"
)
# 10 years back from the symbol's last available bar; for a delisted symbol that
# is 10Y ending at its delisting date, which fully covers a 2019-2022 window and
# any 300-trading-day lookback.
_DEFAULT_RANGE = "10Y"


class StockAnalysisPriceProvider:
    """PriceProvider backed by the free stockanalysis.com history API.

    Intended as a *fallback* for delisted US tickers. Uses only the standard
    library (no optional dependency). All blocking I/O is wrapped in
    ``asyncio.to_thread`` and rate-limited by a module-level semaphore. Errors
    return an empty list and log a warning; exceptions never propagate.
    """

    def __init__(self, *, history_range: str = _DEFAULT_RANGE) -> None:
        self._range = history_range

    async def fetch_prices(
        self, ticker: str, as_of: date, lookback_days: int = 300
    ) -> list[PriceBar]:
        """Return up to ``lookback_days`` adjusted daily bars on/before ``as_of``."""
        async with _SEMAPHORE:
            try:
                rows = await asyncio.to_thread(self._fetch_raw, ticker)
            except Exception as exc:  # noqa: BLE001 - never propagate to caller
                logger.warning("stockanalysis price fetch failed for %s: %s", ticker, exc)
                return []
            finally:
                await asyncio.sleep(_DELAY_SECONDS)
        return self._bars_from_rows(rows, as_of, lookback_days)

    # ------------------------------------------------------------------
    # Blocking HTTP (monkeypatched in tests)
    # ------------------------------------------------------------------

    def _fetch_raw(self, ticker: str) -> list[dict[str, Any]]:
        """Fetch the raw ``data`` rows for ``ticker`` (newest-first). ``[]`` if absent."""
        url = _API_TEMPLATE.format(ticker=ticker, range=self._range)
        request = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
        try:
            with urllib.request.urlopen(request, timeout=_HTTP_TIMEOUT) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            # 400/404 = symbol not present on this source (residual gap).
            logger.warning("stockanalysis HTTP %s for %s", exc.code, ticker)
            return []
        data = payload.get("data")
        return data if isinstance(data, list) else []

    # ------------------------------------------------------------------
    # Pure parsing (adjustment convention lives here)
    # ------------------------------------------------------------------

    @staticmethod
    def _bars_from_rows(
        rows: list[dict[str, Any]], as_of: date, lookback_days: int
    ) -> list[PriceBar]:
        bars: list[PriceBar] = []
        for row in rows:
            try:
                bar_date = date.fromisoformat(str(row["t"]))
            except (KeyError, ValueError, TypeError):
                continue
            if bar_date > as_of:  # point-in-time: drop future bars (look-ahead)
                continue
            raw_close = _as_float(row.get("c"))
            adj_close = _as_float(row.get("a"))
            if raw_close is None or adj_close is None or raw_close == 0:
                continue
            # Proportional back-adjustment factor (split + dividend), matching
            # yfinance auto_adjust=True: scale OHL by a/c, set close = a.
            factor = adj_close / raw_close
            open_ = _as_float(row.get("o"))
            high = _as_float(row.get("h"))
            low = _as_float(row.get("l"))
            bars.append(
                PriceBar(
                    date=bar_date,
                    open=round((open_ or raw_close) * factor, 2),
                    high=round((high or raw_close) * factor, 2),
                    low=round((low or raw_close) * factor, 2),
                    close=round(adj_close, 2),
                    volume=float(row.get("v") or 0),
                )
            )
        # API rows are newest-first; emit ascending and cap to the lookback.
        bars.sort(key=lambda b: b.date)
        return bars[-lookback_days:]


class FallbackPriceProvider:
    """Chain a primary PriceProvider with a supplementary one.

    ``fetch_prices`` returns the primary result unless it is empty, in which
    case it falls through to the supplementary provider. This keeps still-listed
    tickers (which yfinance serves fine) on the primary source untouched — so
    already-built bundles are unaffected — and only reaches the supplementary
    source for the delisted names yfinance purges.
    """

    def __init__(self, primary: PriceProvider, supplementary: PriceProvider) -> None:
        self._primary = primary
        self._supplementary = supplementary

    async def fetch_prices(
        self, ticker: str, as_of: date, lookback_days: int = 300
    ) -> list[PriceBar]:
        try:
            primary_bars = await self._primary.fetch_prices(ticker, as_of, lookback_days)
        except Exception as exc:  # noqa: BLE001 - primary errors fall through
            logger.warning("primary price provider failed for %s: %s", ticker, exc)
            primary_bars = []
        if primary_bars:
            return primary_bars
        supplementary_bars = await self._supplementary.fetch_prices(
            ticker, as_of, lookback_days
        )
        if supplementary_bars:
            logger.info(
                "supplementary source filled %d bars for delisted %s",
                len(supplementary_bars),
                ticker,
            )
        return supplementary_bars


def _as_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None
