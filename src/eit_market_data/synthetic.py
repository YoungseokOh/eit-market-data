"""Synthetic data provider for testing and demonstration.

Generates deterministic, reproducible market data using seeded random
number generators.  No external data sources are required.

Prices follow Geometric Brownian Motion (GBM).
Fundamentals are randomized within realistic ranges.
"""

from __future__ import annotations

import hashlib
from datetime import date, timedelta

import numpy as np
import pandas as pd

from eit_market_data.schemas.snapshot import (
    FilingData,
    FundamentalData,
    MacroData,
    NewsItem,
    PriceBar,
    QuarterlyFinancials,
    SectorAverages,
)

# ---------------------------------------------------------------------------
# Universe & sector definitions
# ---------------------------------------------------------------------------

DEFAULT_UNIVERSE: list[str] = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "BRK.B",
    "JPM", "JNJ", "V", "PG", "UNH", "HD", "MA",
    "DIS", "PYPL", "BAC", "INTC", "CMCSA", "NFLX", "VZ", "ADBE", "CRM", "ABT",
    "T", "MRK", "PFE", "WMT", "KO",
]

SECTOR_MAP: dict[str, str] = {
    "AAPL": "Technology", "MSFT": "Technology", "GOOGL": "Technology",
    "AMZN": "Consumer Discretionary", "NVDA": "Technology", "META": "Technology",
    "TSLA": "Consumer Discretionary", "BRK.B": "Financials",
    "JPM": "Financials", "JNJ": "Healthcare", "V": "Financials",
    "PG": "Consumer Staples", "UNH": "Healthcare", "HD": "Consumer Discretionary",
    "MA": "Financials", "DIS": "Communication Services", "PYPL": "Financials",
    "BAC": "Financials", "INTC": "Technology", "CMCSA": "Communication Services",
    "NFLX": "Communication Services", "VZ": "Communication Services",
    "ADBE": "Technology", "CRM": "Technology", "ABT": "Healthcare",
    "T": "Communication Services", "MRK": "Healthcare", "PFE": "Healthcare",
    "WMT": "Consumer Staples", "KO": "Consumer Staples",
}


def _ticker_seed(ticker: str) -> int:
    """Deterministic seed from ticker name."""
    return int(hashlib.md5(ticker.encode()).hexdigest()[:8], 16) % (2**31)


def _month_seed(month: str) -> int:
    """Deterministic seed from month string."""
    return int(hashlib.md5(month.encode()).hexdigest()[:8], 16) % (2**31)


# ---------------------------------------------------------------------------
# GBM price simulation
# ---------------------------------------------------------------------------

def _generate_prices(
    ticker: str,
    end_date: date,
    n_days: int = 300,
    mu: float = 0.0003,
    sigma: float = 0.02,
) -> list[PriceBar]:
    """Generate synthetic daily OHLCV data via Geometric Brownian Motion.

    Deterministic for a given (ticker, end_date) pair.
    """
    seed = _ticker_seed(ticker) + end_date.toordinal()
    rng = np.random.default_rng(seed)

    # Initial price varies by ticker
    base_price = 50.0 + (seed % 400)

    # Generate daily returns
    daily_returns = rng.normal(mu, sigma, n_days)
    log_prices = np.log(base_price) + np.cumsum(daily_returns)
    closes = np.exp(log_prices)

    # Generate business days
    all_dates = pd.bdate_range(
        end=end_date, periods=n_days, freq="B"
    )

    bars: list[PriceBar] = []
    for i, d in enumerate(all_dates):
        c = float(closes[i])
        spread = c * rng.uniform(0.005, 0.025)
        h = c + spread * rng.uniform(0.3, 1.0)
        lo = c - spread * rng.uniform(0.3, 1.0)
        o = c + spread * rng.uniform(-0.5, 0.5)
        vol = float(rng.uniform(1e6, 5e7))
        bars.append(PriceBar(
            date=d.date(),
            open=round(o, 2),
            high=round(h, 2),
            low=round(lo, 2),
            close=round(c, 2),
            volume=round(vol),
        ))

    return bars


# ---------------------------------------------------------------------------
# Synthetic fundamentals
# ---------------------------------------------------------------------------

def _generate_fundamentals(
    ticker: str,
    as_of: date,
    n_quarters: int = 8,
) -> FundamentalData:
    """Generate plausible quarterly financials."""
    seed = _ticker_seed(ticker)
    rng = np.random.default_rng(seed)

    base_revenue = rng.uniform(1_000, 50_000)  # millions
    growth = rng.uniform(-0.05, 0.15)

    quarters: list[QuarterlyFinancials] = []
    for q in range(n_quarters):
        # Quarters going back from as_of
        q_end = as_of - timedelta(days=90 * q + 30)
        q_label = f"{q_end.year}Q{(q_end.month - 1) // 3 + 1}"

        rev = base_revenue * (1 + growth) ** (n_quarters - q) * rng.uniform(0.9, 1.1)
        cogs = rev * rng.uniform(0.4, 0.7)
        gp = rev - cogs
        op_inc = gp * rng.uniform(0.3, 0.7)
        ni = op_inc * rng.uniform(0.6, 0.9)
        ebitda = op_inc * rng.uniform(1.1, 1.4)
        shares = rng.uniform(500, 5000)  # millions
        eps = ni / shares if shares else 0

        total_assets = rev * rng.uniform(2.0, 5.0)
        total_liab = total_assets * rng.uniform(0.3, 0.7)
        equity = total_assets - total_liab
        current_assets = total_assets * rng.uniform(0.15, 0.4)
        current_liab = total_liab * rng.uniform(0.2, 0.5)
        debt = total_liab * rng.uniform(0.3, 0.8)
        cash = current_assets * rng.uniform(0.2, 0.5)
        inventory = current_assets * rng.uniform(0.05, 0.3)
        ar = current_assets * rng.uniform(0.1, 0.4)
        interest = debt * rng.uniform(0.02, 0.06) / 4

        ocf = ni * rng.uniform(0.8, 1.5)
        capex = rev * rng.uniform(0.03, 0.15)
        fcf = ocf - capex
        divs = ni * rng.uniform(0.0, 0.5)

        quarters.append(QuarterlyFinancials(
            fiscal_quarter=q_label,
            report_date=q_end + timedelta(days=int(rng.uniform(20, 60))),
            revenue=round(rev, 1),
            cost_of_goods_sold=round(cogs, 1),
            gross_profit=round(gp, 1),
            operating_income=round(op_inc, 1),
            net_income=round(ni, 1),
            ebitda=round(ebitda, 1),
            eps=round(eps, 4),
            interest_expense=round(interest, 1),
            dividends_per_share=round(divs / shares if shares else 0, 4),
            total_assets=round(total_assets, 1),
            total_liabilities=round(total_liab, 1),
            total_equity=round(equity, 1),
            current_assets=round(current_assets, 1),
            current_liabilities=round(current_liab, 1),
            total_debt=round(debt, 1),
            cash_and_equivalents=round(cash, 1),
            inventory=round(inventory, 1),
            accounts_receivable=round(ar, 1),
            issued_shares=round(shares, 1),
            operating_cash_flow=round(ocf, 1),
            capital_expenditure=round(capex, 1),
            free_cash_flow=round(fcf, 1),
            dividends_paid=round(divs, 1),
        ))

    prices = _generate_prices(ticker, as_of, 5)
    last_close = prices[-1].close if prices else 100.0
    market_cap = last_close * (quarters[0].issued_shares or 1000) if quarters else None

    return FundamentalData(
        ticker=ticker,
        quarters=quarters,
        market_cap=round(market_cap, 1) if market_cap else None,
        last_close_price=last_close,
    )


# ---------------------------------------------------------------------------
# Synthetic filings
# ---------------------------------------------------------------------------

_FILING_TEMPLATES: dict[str, str] = {
    "business_overview": (
        "{ticker} is a leading company in the {sector} sector. "
        "The company's core business involves developing and marketing innovative "
        "products and services. Key markets include North America and Asia-Pacific. "
        "Revenue is diversified across multiple segments with a focus on sustainable growth."
    ),
    "risks": (
        "Key risk factors include: (1) Intense competition in {sector}, "
        "(2) Regulatory changes that may impact operations, "
        "(3) Foreign exchange exposure from international operations, "
        "(4) Supply chain disruptions, and (5) Dependence on key personnel."
    ),
    "mda": (
        "Management views the current quarter as demonstrating {trend} performance. "
        "Revenue growth was driven by strong demand in core segments. "
        "Operating margins {margin_trend} due to {margin_driver}. "
        "The company continues to invest in R&D and expects moderate growth in coming quarters."
    ),
    "governance": (
        "The Board of Directors consists of 9 members, 7 of whom are independent. "
        "The CEO has served since {ceo_year}. Executive compensation is tied to "
        "long-term shareholder value creation through a mix of base salary, "
        "annual bonus, and equity-based incentives."
    ),
}


def _generate_filing(ticker: str, as_of: date) -> FilingData:
    """Generate synthetic filing text sections."""
    seed = _ticker_seed(ticker)
    rng = np.random.default_rng(seed + as_of.year)
    sector = SECTOR_MAP.get(ticker, "General")

    trends = ["solid", "mixed", "strong", "improving"]
    margin_trends = ["improved", "remained stable", "declined slightly"]
    margin_drivers = ["cost optimization", "input cost pressures", "scale efficiencies"]

    return FilingData(
        ticker=ticker,
        filing_date=as_of - timedelta(days=int(rng.uniform(30, 120))),
        filing_type="10-K",
        business_overview=_FILING_TEMPLATES["business_overview"].format(
            ticker=ticker, sector=sector
        ),
        risks=_FILING_TEMPLATES["risks"].format(sector=sector),
        mda=_FILING_TEMPLATES["mda"].format(
            trend=rng.choice(trends),
            margin_trend=rng.choice(margin_trends),
            margin_driver=rng.choice(margin_drivers),
        ),
        governance=_FILING_TEMPLATES["governance"].format(
            ceo_year=int(rng.uniform(2010, 2022))
        ),
    )


# ---------------------------------------------------------------------------
# Synthetic news
# ---------------------------------------------------------------------------

_NEWS_TEMPLATES: list[str] = [
    "{ticker} reports quarterly earnings {direction} analyst expectations",
    "{ticker} announces {action} in {area}",
    "Analysts {sentiment} on {ticker} following {event}",
    "{ticker} shares {movement} amid {context}",
    "{sector} sector faces {challenge} as {ticker} {reaction}",
    "{ticker} CEO discusses growth strategy at investor conference",
    "New product launch by {ticker} receives {reception} reviews",
    "{ticker} expands operations in {region}",
]


def _generate_news(
    ticker: str, as_of: date, n_items: int = 15
) -> list[NewsItem]:
    """Generate synthetic news headlines."""
    seed = _ticker_seed(ticker) + as_of.toordinal()
    rng = np.random.default_rng(seed)
    sector = SECTOR_MAP.get(ticker, "General")

    directions = ["above", "below", "in line with"]
    actions = ["expansion", "partnership", "restructuring", "new investment"]
    areas = ["AI technology", "cloud services", "emerging markets", "sustainability"]
    sentiments = ["turn bullish", "remain cautious", "upgrade outlook"]
    events = ["strong earnings", "market volatility", "sector rotation"]
    movements = ["rise", "dip", "hold steady"]
    contexts = ["broader market trends", "sector momentum", "policy changes"]
    challenges = ["headwinds", "regulatory pressure", "competition"]
    reactions = ["adapts strategy", "maintains course", "pivots approach"]
    receptions = ["positive", "mixed", "enthusiastic"]
    regions = ["Asia-Pacific", "Europe", "Latin America"]

    items: list[NewsItem] = []
    for _i in range(n_items):
        template = rng.choice(_NEWS_TEMPLATES)
        headline = template.format(
            ticker=ticker,
            sector=sector,
            direction=rng.choice(directions),
            action=rng.choice(actions),
            area=rng.choice(areas),
            sentiment=rng.choice(sentiments),
            event=rng.choice(events),
            movement=rng.choice(movements),
            context=rng.choice(contexts),
            challenge=rng.choice(challenges),
            reaction=rng.choice(reactions),
            reception=rng.choice(receptions),
            region=rng.choice(regions),
        )
        news_date = as_of - timedelta(days=int(rng.uniform(0, 30)))
        items.append(NewsItem(
            date=news_date,
            source=rng.choice(["Reuters", "Bloomberg", "CNBC", "WSJ", "FT"]),
            headline=headline,
            summary=f"Details regarding {ticker} in the {sector} sector.",
        ))

    items.sort(key=lambda x: x.date, reverse=True)
    return items


# ---------------------------------------------------------------------------
# Synthetic macro
# ---------------------------------------------------------------------------

def _generate_macro(as_of: date) -> MacroData:
    """Generate synthetic macro indicators."""
    seed = as_of.toordinal()
    rng = np.random.default_rng(seed)

    return MacroData(
        rates_policy={
            "fed_funds_rate": round(rng.uniform(4.0, 5.5), 2),
            "treasury_10y": round(rng.uniform(3.5, 5.0), 2),
            "treasury_2y": round(rng.uniform(3.8, 5.2), 2),
            "yield_curve_spread_10y_2y": round(rng.uniform(-0.5, 1.0), 2),
            "policy_stance": rng.choice(["hawkish", "neutral", "dovish"]),
        },
        inflation_commodities={
            "cpi_yoy": round(rng.uniform(2.0, 5.0), 1),
            "cpi_mom": round(rng.uniform(-0.2, 0.6), 1),
            "ppi_yoy": round(rng.uniform(1.0, 6.0), 1),
            "oil_wti": round(rng.uniform(65, 95), 1),
            "gold": round(rng.uniform(1800, 2400), 0),
            "copper": round(rng.uniform(3.5, 4.8), 2),
        },
        growth_economy={
            "gdp_growth_yoy": round(rng.uniform(1.0, 4.0), 1),
            "unemployment_rate": round(rng.uniform(3.4, 4.5), 1),
            "ism_manufacturing": round(rng.uniform(46, 56), 1),
            "consumer_confidence": round(rng.uniform(95, 115), 1),
            "nonfarm_payrolls_k": round(rng.uniform(100, 350), 0),
        },
        market_risk={
            "vix": round(rng.uniform(12, 30), 1),
            "sp500_level": round(rng.uniform(4200, 5500), 0),
            "sp500_monthly_return": round(rng.uniform(-5, 8), 1),
            "ig_credit_spread": round(rng.uniform(0.8, 2.0), 2),
            "hy_credit_spread": round(rng.uniform(3.0, 6.0), 2),
        },
    )


# ---------------------------------------------------------------------------
# Synthetic sector averages
# ---------------------------------------------------------------------------

def _compute_sector_averages(
    sector: str,
    tickers: list[str],
    as_of: date,
) -> SectorAverages:
    """Compute average fundamental metrics for a sector from synthetic data."""
    metrics: dict[str, list[float]] = {}
    for ticker in tickers:
        fund = _generate_fundamentals(ticker, as_of, n_quarters=4)
        if not fund.quarters:
            continue
        q = fund.quarters[0]
        if q.revenue and q.total_assets and q.revenue > 0:
            metrics.setdefault("roa", []).append(
                (q.net_income or 0) / q.total_assets if q.total_assets else 0
            )
            metrics.setdefault("roe", []).append(
                (q.net_income or 0) / q.total_equity if q.total_equity else 0
            )
            metrics.setdefault("gross_margin", []).append(
                (q.gross_profit or 0) / q.revenue
            )
            metrics.setdefault("operating_margin", []).append(
                (q.operating_income or 0) / q.revenue
            )
            metrics.setdefault("net_margin", []).append(
                (q.net_income or 0) / q.revenue
            )
            if q.current_liabilities and q.current_liabilities > 0:
                metrics.setdefault("current_ratio", []).append(
                    (q.current_assets or 0) / q.current_liabilities
                )
            if q.total_equity and q.total_equity > 0:
                metrics.setdefault("debt_to_equity", []).append(
                    (q.total_debt or 0) / q.total_equity
                )
            metrics.setdefault("asset_turnover", []).append(
                q.revenue / q.total_assets
            )
            if fund.last_close_price and q.eps and q.eps > 0:
                metrics.setdefault("pe_ttm", []).append(
                    fund.last_close_price / (q.eps * 4)  # Annualized
                )

    avg: dict[str, float] = {}
    for k, vals in metrics.items():
        if vals:
            avg[k] = round(float(np.mean(vals)), 4)

    return SectorAverages(sector=sector, avg_metrics=avg)


# ---------------------------------------------------------------------------
# Unified synthetic provider
# ---------------------------------------------------------------------------

class SyntheticProvider:
    """Synthetic data provider implementing all data provider interfaces.

    All methods are deterministic given the same ticker + date inputs.
    No external data sources required.
    """

    def __init__(self, universe: list[str] | None = None):
        self.universe = universe or DEFAULT_UNIVERSE
        self._sector_map = {t: SECTOR_MAP.get(t, "General") for t in self.universe}

    async def fetch_prices(
        self, ticker: str, as_of: date, lookback_days: int = 300
    ) -> list[PriceBar]:
        return _generate_prices(ticker, as_of, lookback_days)

    async def fetch_fundamentals(
        self, ticker: str, as_of: date, n_quarters: int = 8
    ) -> FundamentalData:
        return _generate_fundamentals(ticker, as_of, n_quarters)

    async def fetch_filing(self, ticker: str, as_of: date) -> FilingData:
        return _generate_filing(ticker, as_of)

    async def fetch_news(
        self, ticker: str, as_of: date, lookback_days: int = 30
    ) -> list[NewsItem]:
        return _generate_news(ticker, as_of, min(lookback_days, 30))

    async def fetch_macro(self, as_of: date) -> MacroData:
        return _generate_macro(as_of)

    async def fetch_sector_map(
        self, universe: list[str], as_of: date | None = None
    ) -> dict[str, str]:
        _ = as_of
        return {t: self._sector_map.get(t, "General") for t in universe}

    async def fetch_sector_averages(
        self, sector: str, tickers: list[str], as_of: date
    ) -> SectorAverages:
        return _compute_sector_averages(sector, tickers, as_of)

    async def fetch_benchmark(
        self, as_of: date, lookback_days: int = 300
    ) -> list[PriceBar]:
        return _generate_prices("__BENCHMARK__", as_of, lookback_days)
