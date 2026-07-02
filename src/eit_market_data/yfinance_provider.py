"""YFinance-based data providers.

Implements PriceProvider, FundamentalProvider, SectorProvider,
NewsProvider, and BenchmarkProvider using the free yfinance library.

All methods are point-in-time safe: data is filtered to ``as_of`` date.
yfinance is a synchronous library, so all calls are wrapped with
``asyncio.to_thread`` for async compatibility.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from datetime import date, timedelta
from typing import Any

import numpy as np

from eit_market_data.core.price_frame import price_bars_from_frame
from eit_market_data.core.sector_math import compute_sector_averages
from eit_market_data.schemas.snapshot import (
    FilingData,
    FundamentalData,
    MacroData,
    NewsItem,
    PriceBar,
    QuarterlyFinancials,
    SectorAverages,
)

logger = logging.getLogger(__name__)


def _install_curl_timeout(default_timeout: float = 30.0) -> None:
    """Force a hard per-request timeout into yfinance's curl_cffi backend.

    yfinance (curl_cffi) hangs indefinitely when a Yahoo keepalive socket stalls
    inside ``curl_easy_perform`` because some yfinance code paths pass
    ``timeout=None`` (e.g. ``.info`` / financial statements), and the blocking
    call runs in an uninterruptible thread that ``asyncio.wait_for`` cannot
    cancel. ``setdefault`` is not enough because the key is already present, so
    we *cap* every request: a ``None``/missing/oversized timeout is forced down
    to ``default_timeout``; a smaller explicit yfinance timeout is kept.
    """
    try:
        import curl_cffi.requests as _ccr
    except Exception:  # noqa: BLE001 - optional dependency / API drift
        return
    if getattr(_ccr.Session.request, "_eit_timeout_patched", False):
        return
    _orig_request = _ccr.Session.request

    def _request_with_timeout(self, *args, **kwargs):  # noqa: ANN001, ANN202
        t = kwargs.get("timeout")
        if t is None or not isinstance(t, (int, float)) or t > default_timeout:
            kwargs["timeout"] = default_timeout
        return _orig_request(self, *args, **kwargs)

    _request_with_timeout._eit_timeout_patched = True  # type: ignore[attr-defined]
    _ccr.Session.request = _request_with_timeout


_install_curl_timeout()

# ---------------------------------------------------------------------------
# Column name mappings: yfinance → QuarterlyFinancials schema
# ---------------------------------------------------------------------------

_INCOME_MAP: dict[str, str] = {
    "Total Revenue": "revenue",
    "Operating Revenue": "revenue",
    "Cost Of Revenue": "cost_of_goods_sold",
    "Reconciled Cost Of Revenue": "cost_of_goods_sold",
    "Gross Profit": "gross_profit",
    "Operating Income": "operating_income",
    "Total Operating Income As Reported": "operating_income",
    "Net Income": "net_income",
    "Net Income Common Stockholders": "net_income",
    "EBITDA": "ebitda",
    "Normalized EBITDA": "ebitda",
    "Basic EPS": "eps",
    "Diluted EPS": "eps",
    "Interest Expense": "interest_expense",
}

_BALANCE_MAP: dict[str, str] = {
    "Total Assets": "total_assets",
    "Total Liabilities Net Minority Interest": "total_liabilities",
    "Stockholders Equity": "total_equity",
    "Total Equity Gross Minority Interest": "total_equity",
    "Current Assets": "current_assets",
    "Current Liabilities": "current_liabilities",
    "Total Debt": "total_debt",
    "Cash And Cash Equivalents": "cash_and_equivalents",
    "Cash Cash Equivalents And Short Term Investments": "cash_and_equivalents",
    "Inventory": "inventory",
    "Accounts Receivable": "accounts_receivable",
    "Receivables": "accounts_receivable",
    "Ordinary Shares Number": "issued_shares",
    "Share Issued": "issued_shares",
}

_CASHFLOW_MAP: dict[str, str] = {
    "Operating Cash Flow": "operating_cash_flow",
    "Capital Expenditure": "capital_expenditure",
    "Free Cash Flow": "free_cash_flow",
    "Cash Dividends Paid": "dividends_paid",
}

# Concurrency limiter for yfinance (avoid rate-limiting)
_SEMAPHORE = asyncio.Semaphore(1)
# Hard wall-clock budget for a single blocking yfinance call. yfinance (curl_cffi)
# does not honour socket.setdefaulttimeout, so a stuck Yahoo socket can hang
# indefinitely; this bounds every call at the async boundary instead.
_YF_CALL_TIMEOUT = 90.0
_YF_THROTTLE_LOCK = threading.Lock()
_YF_LAST_REQUEST_AT = 0.0
_YF_MIN_REQUEST_INTERVAL_SECONDS = 1.0


def _safe_float(val: Any) -> float | None:
    """Convert a value to float, returning None for NaN / missing."""
    if val is None:
        return None
    try:
        f = float(val)
        return None if np.isnan(f) else f
    except (ValueError, TypeError):
        return None


def _date_from_timestamp(ts: Any) -> date | None:
    """Convert pandas Timestamp or similar to date."""
    try:
        return ts.date() if hasattr(ts, "date") else None
    except Exception:
        return None


def _respect_yf_rate_limit() -> None:
    global _YF_LAST_REQUEST_AT
    with _YF_THROTTLE_LOCK:
        now = time.monotonic()
        wait_seconds = _YF_MIN_REQUEST_INTERVAL_SECONDS - (now - _YF_LAST_REQUEST_AT)
        if wait_seconds > 0:
            time.sleep(wait_seconds)
            now = time.monotonic()
        _YF_LAST_REQUEST_AT = now


def _is_rate_limited_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return (
        "too many requests" in text
        or "rate limited" in text
        or "invalid crumb" in text
        or "unauthorized" in text
    )


class YFinanceProvider:
    """Unified provider backed by yfinance for prices, fundamentals,
    sectors, news, and benchmark data.

    Install with: ``pip install yfinance``
    """

    def __init__(self) -> None:
        try:
            import yfinance  # noqa: F401
        except ImportError as e:
            raise ImportError(
                "yfinance is required for real data. "
                "Install with: pip install -e '.[real-data]'"
            ) from e

        # Cache for sector map (avoids repeated .info calls)
        self._sector_cache: dict[str, str] = {}
        self._info_cache: dict[str, dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_ticker(self, symbol: str):  # noqa: ANN202
        import yfinance as yf

        return yf.Ticker(symbol)

    def _get_info(self, symbol: str, *, refresh: bool = False) -> dict[str, Any]:
        if not refresh and symbol in self._info_cache:
            return self._info_cache[symbol]

        last_error: Exception | None = None
        for attempt in range(3):
            ticker = self._get_ticker(symbol)
            try:
                _respect_yf_rate_limit()
                info = dict(ticker.info)
            except Exception as exc:
                last_error = exc
                info = {}

            if info:
                self._info_cache[symbol] = info
                return info

            if attempt < 2:
                time.sleep(1.5 * (attempt + 1))

        if last_error is not None:
            logger.warning("Info fetch failed for %s: %s", symbol, last_error)
        return {}

    def _get_statement(self, symbol: str, attr: str):  # noqa: ANN202
        last_error: Exception | None = None
        for attempt in range(3):
            ticker = self._get_ticker(symbol)
            try:
                _respect_yf_rate_limit()
                return getattr(ticker, attr)
            except Exception as exc:
                last_error = exc
                if attempt < 2:
                    backoff = 1.5 * (attempt + 1) if _is_rate_limited_error(exc) else 0.5
                    time.sleep(backoff)
        if last_error is not None:
            logger.warning("%s fetch failed for %s: %s", attr, symbol, last_error)
        return None

    async def _run_limited(self, fn, *args, default):  # noqa: ANN001, ANN202
        """Run a blocking yfinance call under the shared semaphore with a hard
        wall-clock timeout. On timeout/error, log and return ``default()`` (a
        fresh empty value) so one stuck Yahoo socket cannot freeze the batch."""
        name = getattr(fn, "__name__", str(fn))
        async with _SEMAPHORE:
            try:
                return await asyncio.wait_for(
                    asyncio.to_thread(fn, *args), _YF_CALL_TIMEOUT
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "yfinance %s exceeded %.0fs timeout", name, _YF_CALL_TIMEOUT
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("yfinance %s failed: %s", name, exc)
            return default()

    # ------------------------------------------------------------------
    # PriceProvider
    # ------------------------------------------------------------------

    async def fetch_prices(
        self, ticker: str, as_of: date, lookback_days: int = 300
    ) -> list[PriceBar]:
        """Fetch daily OHLCV from yfinance, filtered to <= as_of."""
        return await self._run_limited(
            self._fetch_prices_sync, ticker, as_of, lookback_days, default=list
        )

    def _fetch_prices_sync(
        self, ticker: str, as_of: date, lookback_days: int
    ) -> list[PriceBar]:
        start = as_of - timedelta(days=int(lookback_days * 1.6))
        end = as_of + timedelta(days=1)  # yfinance end is exclusive

        last_error: Exception | None = None
        df = None
        for attempt in range(3):
            t = self._get_ticker(ticker)
            try:
                _respect_yf_rate_limit()
                df = t.history(start=str(start), end=str(end), auto_adjust=True)
            except Exception as exc:
                last_error = exc
                if attempt < 2:
                    backoff = 1.5 * (attempt + 1) if _is_rate_limited_error(exc) else 0.5
                    time.sleep(backoff)
                continue
            if df is not None and not df.empty:
                break
            # Empty with no exception = Yahoo genuinely has no data for this
            # ticker (delisted / not-yet-listed). yfinance already retried
            # internally, so re-calling won't change the result — stop now
            # instead of burning two more round-trips + sleeps per empty ticker
            # (there are ~130 such names in a PIT 2019 universe).
            break

        if df is None or df.empty:
            if last_error is not None:
                logger.warning("Price fetch failed for %s: %s", ticker, last_error)
            logger.warning("No price data for %s", ticker)
            return []

        # Cap to lookback_days trading days
        return price_bars_from_frame(df, as_of, lookback_days)

    # ------------------------------------------------------------------
    # FundamentalProvider
    # ------------------------------------------------------------------

    async def fetch_fundamentals(
        self, ticker: str, as_of: date, n_quarters: int = 8
    ) -> FundamentalData:
        """Fetch quarterly financials from yfinance."""
        return await self._run_limited(
            self._fetch_fundamentals_sync,
            ticker,
            as_of,
            n_quarters,
            default=lambda: FundamentalData(
                ticker=ticker, quarters=[], market_cap=None, last_close_price=None
            ),
        )

    def _fetch_fundamentals_sync(
        self, ticker: str, as_of: date, n_quarters: int
    ) -> FundamentalData:
        # Get quarterly statements
        income = self._get_statement(ticker, "quarterly_income_stmt")
        balance = self._get_statement(ticker, "quarterly_balance_sheet")
        cashflow = self._get_statement(ticker, "quarterly_cashflow")

        quarters: list[QuarterlyFinancials] = []

        # yfinance returns columns as Timestamps representing fiscal period
        # end dates. Reports are typically filed 45-60 days after period end.
        # We use a 60-day buffer to approximate point-in-time availability.
        filing_lag_days = 60

        if income is not None and not income.empty:
            for col in income.columns:
                col_date = _date_from_timestamp(col)
                if col_date is None:
                    continue
                # Point-in-time: period end + ~60 days filing lag
                estimated_available = col_date + timedelta(days=filing_lag_days)
                if estimated_available > as_of:
                    continue

                q_data: dict[str, Any] = {}

                def _set_if_missing(data: dict, key: str, val: Any) -> None:
                    """Set key only if not already populated."""
                    v = _safe_float(val)
                    if v is not None and key not in data:
                        data[key] = v

                # Income statement
                for yf_key, schema_key in _INCOME_MAP.items():
                    if yf_key in income.index:
                        _set_if_missing(q_data, schema_key, income.loc[yf_key, col])

                # Balance sheet
                if balance is not None and col in balance.columns:
                    for yf_key, schema_key in _BALANCE_MAP.items():
                        if yf_key in balance.index:
                            _set_if_missing(
                                q_data, schema_key, balance.loc[yf_key, col]
                            )

                # Cash flow
                if cashflow is not None and col in cashflow.columns:
                    for yf_key, schema_key in _CASHFLOW_MAP.items():
                        if yf_key in cashflow.index:
                            _set_if_missing(
                                q_data, schema_key, cashflow.loc[yf_key, col]
                            )

                # Determine fiscal quarter label
                q_num = (col_date.month - 1) // 3 + 1
                fiscal_q = f"{col_date.year}Q{q_num}"

                quarters.append(
                    QuarterlyFinancials(
                        fiscal_quarter=fiscal_q,
                        report_date=col_date,
                        **q_data,
                    )
                )

        # Sort by report_date descending, cap to n_quarters
        quarters.sort(key=lambda q: q.report_date, reverse=True)
        quarters = quarters[:n_quarters]

        # Point-in-time market_cap / last_close.
        #
        # yfinance's .info["marketCap"]/["previousClose"] are *current* values, so
        # using them in a historical snapshot injects look-ahead (the same class of
        # bug found on the KR side). Reconstruct as-of decision_date instead:
        #   last_close   = unadjusted close on/just before as_of
        #   market_cap   = last_close x issued shares from the most recent PIT
        #                  quarter (report_date <= as_of). Unadjusted close is used
        #                  so market_cap is a true as-of value rather than a
        #                  dividend/split-adjusted figure.
        last_close = self._asof_close(ticker, as_of)
        issued_shares = next((q.issued_shares for q in quarters if q.issued_shares), None)
        market_cap = (
            round(last_close * issued_shares, 2)
            if (last_close is not None and issued_shares)
            else None
        )

        return FundamentalData(
            ticker=ticker,
            quarters=quarters,
            market_cap=market_cap,
            last_close_price=last_close,
        )

    def _asof_close(self, ticker: str, as_of: date) -> float | None:
        """Unadjusted closing price on or just before ``as_of`` (point-in-time)."""
        start = as_of - timedelta(days=10)
        end = as_of + timedelta(days=1)  # yfinance end is exclusive
        try:
            t = self._get_ticker(ticker)
            _respect_yf_rate_limit()
            df = t.history(start=str(start), end=str(end), auto_adjust=False)
        except Exception as exc:  # noqa: BLE001
            logger.warning("as-of close fetch failed for %s: %s", ticker, exc)
            return None
        if df is None or df.empty:
            return None
        last: float | None = None
        for idx, row in df.iterrows():
            bar_date = idx.date() if hasattr(idx, "date") else idx
            if bar_date > as_of:
                continue
            close = _safe_float(row.get("Close"))
            if close is not None:
                last = round(close, 2)
        return last

    # ------------------------------------------------------------------
    # SectorProvider
    # ------------------------------------------------------------------

    async def fetch_sector_map(
        self, universe: list[str], as_of: date | None = None
    ) -> dict[str, str]:
        """Get sector for each ticker from yfinance .info.

        The as_of parameter is accepted for protocol compatibility but not used,
        as yfinance sector mappings are not point-in-time specific.
        """
        _ = as_of
        return await self._run_limited(
            self._fetch_sector_map_sync, universe, default=dict
        )

    def _fetch_sector_map_sync(self, universe: list[str]) -> dict[str, str]:
        result: dict[str, str] = {}
        for ticker in universe:
            if ticker in self._sector_cache:
                result[ticker] = self._sector_cache[ticker]
                continue
            info = self._get_info(ticker)
            sector = info.get("sector", "General")
            self._sector_cache[ticker] = sector
            result[ticker] = sector
        return result

    async def fetch_sector_averages(
        self, sector: str, tickers: list[str], as_of: date
    ) -> SectorAverages:
        """Compute sector average metrics from yfinance fundamentals."""
        funds = await asyncio.gather(
            *[self.fetch_fundamentals(t, as_of, n_quarters=4) for t in tickers]
        )

        return compute_sector_averages(sector, list(funds))

    # ------------------------------------------------------------------
    # NewsProvider
    # ------------------------------------------------------------------

    async def fetch_news(
        self, ticker: str, as_of: date, lookback_days: int = 30
    ) -> list[NewsItem]:
        """Fetch news from yfinance."""
        return await self._run_limited(
            self._fetch_news_sync, ticker, as_of, lookback_days, default=list
        )

    def _fetch_news_sync(
        self, ticker: str, as_of: date, lookback_days: int
    ) -> list[NewsItem]:
        from datetime import datetime

        t = self._get_ticker(ticker)
        try:
            raw_news = t.news or []
        except Exception:
            raw_news = []

        cutoff = as_of - timedelta(days=lookback_days)
        items: list[NewsItem] = []

        for article in raw_news:
            # yfinance news has 'providerPublishTime' (unix), 'title', 'publisher'
            pub_ts = article.get("providerPublishTime")
            if pub_ts:
                pub_date = datetime.utcfromtimestamp(pub_ts).date()
            else:
                # Try content.pubDate
                content = article.get("content", {})
                pub_str = content.get("pubDate", "")
                if pub_str:
                    try:
                        pub_date = datetime.fromisoformat(
                            pub_str.replace("Z", "+00:00")
                        ).date()
                    except Exception:
                        pub_date = as_of
                else:
                    pub_date = as_of

            if pub_date > as_of or pub_date < cutoff:
                continue

            title = article.get("title", "")
            if not title:
                content = article.get("content", {})
                title = content.get("title", "")

            publisher = article.get("publisher", "")
            if not publisher:
                content = article.get("content", {})
                provider = content.get("provider", {})
                publisher = provider.get("displayName", "")

            summary = article.get("summary", "")
            if not summary:
                content = article.get("content", {})
                summary = content.get("summary", "")

            if title:
                items.append(
                    NewsItem(
                        date=pub_date,
                        source=publisher,
                        headline=title,
                        summary=summary or "",
                    )
                )

        items.sort(key=lambda x: x.date, reverse=True)
        return items[:15]

    # ------------------------------------------------------------------
    # BenchmarkProvider
    # ------------------------------------------------------------------

    async def fetch_benchmark(
        self, as_of: date, lookback_days: int = 300
    ) -> list[PriceBar]:
        """Fetch S&P 500 (^GSPC) prices as benchmark."""
        return await self.fetch_prices("^GSPC", as_of, lookback_days)

    # ------------------------------------------------------------------
    # FilingProvider stub (SEC EDGAR is separate)
    # ------------------------------------------------------------------

    async def fetch_filing(self, ticker: str, as_of: date) -> FilingData:
        """YFinance does not provide full filing text.

        Use ``EdgarFilingProvider`` instead. This stub returns
        the company description from yfinance .info as a fallback.
        """
        return await self._run_limited(
            self._fetch_filing_stub,
            ticker,
            as_of,
            default=lambda: FilingData(
                ticker=ticker,
                filing_date=as_of,
                filing_type="info",
                business_overview=None,
                risks=None,
                mda=None,
                governance=None,
            ),
        )

    def _fetch_filing_stub(self, ticker: str, as_of: date) -> FilingData:
        info = self._get_info(ticker)
        desc = info.get("longBusinessSummary", "")
        return FilingData(
            ticker=ticker,
            filing_date=as_of,
            filing_type="info",
            business_overview=desc or None,
            risks=None,
            mda=None,
            governance=None,
        )

    # ------------------------------------------------------------------
    # MacroProvider stub (FRED is separate)
    # ------------------------------------------------------------------

    async def fetch_macro(self, as_of: date) -> MacroData:
        """YFinance does not provide macro data.

        Use ``FredMacroProvider`` instead. This stub returns empty data.
        """
        return MacroData()
