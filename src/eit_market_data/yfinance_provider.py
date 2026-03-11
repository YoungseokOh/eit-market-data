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
from datetime import date, timedelta
from typing import Any

import numpy as np

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
_SEMAPHORE = asyncio.Semaphore(3)


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

    def _get_info(self, symbol: str) -> dict[str, Any]:
        if symbol not in self._info_cache:
            ticker = self._get_ticker(symbol)
            try:
                self._info_cache[symbol] = dict(ticker.info)
            except Exception:
                self._info_cache[symbol] = {}
        return self._info_cache[symbol]

    # ------------------------------------------------------------------
    # PriceProvider
    # ------------------------------------------------------------------

    async def fetch_prices(
        self, ticker: str, as_of: date, lookback_days: int = 300
    ) -> list[PriceBar]:
        """Fetch daily OHLCV from yfinance, filtered to <= as_of."""
        async with _SEMAPHORE:
            return await asyncio.to_thread(
                self._fetch_prices_sync, ticker, as_of, lookback_days
            )

    def _fetch_prices_sync(
        self, ticker: str, as_of: date, lookback_days: int
    ) -> list[PriceBar]:
        import yfinance as yf

        start = as_of - timedelta(days=int(lookback_days * 1.6))
        end = as_of + timedelta(days=1)  # yfinance end is exclusive

        t = yf.Ticker(ticker)
        df = t.history(start=str(start), end=str(end), auto_adjust=True)

        if df.empty:
            logger.warning("No price data for %s", ticker)
            return []

        bars: list[PriceBar] = []
        for idx, row in df.iterrows():
            bar_date = idx.date() if hasattr(idx, "date") else idx
            if bar_date > as_of:
                continue
            bars.append(
                PriceBar(
                    date=bar_date,
                    open=round(float(row["Open"]), 2),
                    high=round(float(row["High"]), 2),
                    low=round(float(row["Low"]), 2),
                    close=round(float(row["Close"]), 2),
                    volume=float(row.get("Volume", 0)),
                )
            )

        # Cap to lookback_days trading days
        return bars[-lookback_days:]

    # ------------------------------------------------------------------
    # FundamentalProvider
    # ------------------------------------------------------------------

    async def fetch_fundamentals(
        self, ticker: str, as_of: date, n_quarters: int = 8
    ) -> FundamentalData:
        """Fetch quarterly financials from yfinance."""
        async with _SEMAPHORE:
            return await asyncio.to_thread(
                self._fetch_fundamentals_sync, ticker, as_of, n_quarters
            )

    def _fetch_fundamentals_sync(
        self, ticker: str, as_of: date, n_quarters: int
    ) -> FundamentalData:
        t = self._get_ticker(ticker)

        # Get quarterly statements
        try:
            income = t.quarterly_income_stmt
        except Exception:
            income = None
        try:
            balance = t.quarterly_balance_sheet
        except Exception:
            balance = None
        try:
            cashflow = t.quarterly_cashflow
        except Exception:
            cashflow = None

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

        # Market cap and last close price
        info = self._get_info(ticker)
        market_cap = _safe_float(info.get("marketCap"))
        last_close = _safe_float(info.get("previousClose"))

        return FundamentalData(
            ticker=ticker,
            quarters=quarters,
            market_cap=market_cap,
            last_close_price=last_close,
        )

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
        async with _SEMAPHORE:
            return await asyncio.to_thread(self._fetch_sector_map_sync, universe)

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

        metrics: dict[str, list[float]] = {}
        for fund in funds:
            if not fund.quarters:
                continue
            q = fund.quarters[0]
            rev = q.revenue
            ta = q.total_assets
            if not rev or not ta or ta == 0:
                continue

            def _add(key: str, val: float | None) -> None:
                if val is not None:
                    metrics.setdefault(key, []).append(val)

            _add("roa", (q.net_income or 0) / ta if ta else None)
            _add(
                "roe",
                (q.net_income or 0) / q.total_equity if q.total_equity else None,
            )
            _add("gross_margin", (q.gross_profit or 0) / rev)
            _add("operating_margin", (q.operating_income or 0) / rev)
            _add("net_margin", (q.net_income or 0) / rev)
            if q.current_liabilities and q.current_liabilities > 0:
                _add("current_ratio", (q.current_assets or 0) / q.current_liabilities)
            if q.total_equity and q.total_equity > 0:
                _add("debt_to_equity", (q.total_debt or 0) / q.total_equity)
            _add("asset_turnover", rev / ta)
            if fund.last_close_price and q.eps and q.eps > 0:
                _add("pe_ttm", fund.last_close_price / (q.eps * 4))

        avg: dict[str, float] = {}
        for k, vals in metrics.items():
            if vals:
                avg[k] = round(float(np.mean(vals)), 4)

        return SectorAverages(sector=sector, avg_metrics=avg)

    # ------------------------------------------------------------------
    # NewsProvider
    # ------------------------------------------------------------------

    async def fetch_news(
        self, ticker: str, as_of: date, lookback_days: int = 30
    ) -> list[NewsItem]:
        """Fetch news from yfinance."""
        async with _SEMAPHORE:
            return await asyncio.to_thread(
                self._fetch_news_sync, ticker, as_of, lookback_days
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
        async with _SEMAPHORE:
            return await asyncio.to_thread(self._fetch_filing_stub, ticker, as_of)

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
