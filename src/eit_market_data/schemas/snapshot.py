"""Point-in-time snapshot schemas.

A MonthlySnapshot freezes all data available at the decision date to enforce
strict point-in-time discipline (no look-ahead bias).
"""

from __future__ import annotations

from datetime import date  # noqa: TC003
from typing import Any

from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Price data
# ---------------------------------------------------------------------------

class PriceBar(BaseModel):
    """Single OHLCV bar."""

    date: date
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0


# ---------------------------------------------------------------------------
# Fundamental data
# ---------------------------------------------------------------------------

class QuarterlyFinancials(BaseModel):
    """One quarter of financial data (P&L, B/S, CF items).

    All monetary values in the reporting currency (millions).
    None means the value is unavailable.
    """

    fiscal_quarter: str = Field(description="e.g. '2024Q1'")
    report_date: date = Field(description="Filing / announcement date")

    # Income Statement
    revenue: float | None = None
    cost_of_goods_sold: float | None = None
    gross_profit: float | None = None
    operating_income: float | None = None
    net_income: float | None = None
    ebitda: float | None = None
    eps: float | None = None
    interest_expense: float | None = None
    dividends_per_share: float | None = None

    # Balance Sheet (point-in-time, not flow)
    total_assets: float | None = None
    total_liabilities: float | None = None
    total_equity: float | None = None
    current_assets: float | None = None
    current_liabilities: float | None = None
    total_debt: float | None = None
    cash_and_equivalents: float | None = None
    inventory: float | None = None
    accounts_receivable: float | None = None
    issued_shares: float | None = None

    # Cash Flow Statement
    operating_cash_flow: float | None = None
    capital_expenditure: float | None = None
    free_cash_flow: float | None = None
    dividends_paid: float | None = None


class FundamentalData(BaseModel):
    """Collection of quarterly financials for a stock (point-in-time)."""

    ticker: str
    quarters: list[QuarterlyFinancials] = Field(
        default_factory=list,
        description="Most recent quarters first, ordered by report_date descending.",
    )
    market_cap: float | None = Field(
        default=None, description="As of decision date",
    )
    last_close_price: float | None = Field(
        default=None, description="Closing price at decision date",
    )


# ---------------------------------------------------------------------------
# Filing / qualitative data
# ---------------------------------------------------------------------------

class FilingData(BaseModel):
    """Extracted text sections from the most recent filing (10-K/10-Q or equivalent)."""

    ticker: str
    filing_date: date | None = None
    filing_type: str | None = Field(default=None, description="e.g. '10-K', '有価証券報告書'")
    business_overview: str | None = None
    risks: str | None = None
    mda: str | None = Field(default=None, description="Management Discussion & Analysis")
    governance: str | None = None


# ---------------------------------------------------------------------------
# News data
# ---------------------------------------------------------------------------

class NewsItem(BaseModel):
    """A single news headline/article."""

    date: date
    source: str = ""
    headline: str
    summary: str = ""


# ---------------------------------------------------------------------------
# Macro data
# ---------------------------------------------------------------------------

class MacroData(BaseModel):
    """Macro indicators grouped by category.

    Each category maps indicator name → value (float or str).
    """

    rates_policy: dict[str, Any] = Field(default_factory=dict)
    inflation_commodities: dict[str, Any] = Field(default_factory=dict)
    growth_economy: dict[str, Any] = Field(default_factory=dict)
    market_risk: dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Sector data
# ---------------------------------------------------------------------------

class SectorAverages(BaseModel):
    """Average metrics for a sector, used for stock-vs-sector comparison."""

    sector: str
    avg_metrics: dict[str, float] = Field(
        default_factory=dict,
        description="e.g. {'pe_ttm': 18.5, 'roa': 0.08, ...}",
    )


# ---------------------------------------------------------------------------
# Snapshot metadata
# ---------------------------------------------------------------------------

class SnapshotMetadata(BaseModel):
    """Metadata for reproducibility tracking."""

    created_at: str = ""
    config_hash: str = ""
    price_hash: str = ""
    fundamental_hash: str = ""
    filing_hash: str = ""
    news_hash: str = ""
    macro_hash: str = ""


# ---------------------------------------------------------------------------
# Root snapshot
# ---------------------------------------------------------------------------

class MonthlySnapshot(BaseModel):
    """Point-in-time frozen data snapshot for a single monthly decision.

    decision_date: last business day of the month
    execution_date: first business day of the following month
    """

    model_config = {"frozen": True}

    decision_date: date
    execution_date: date
    universe: list[str]
    prices: dict[str, list[PriceBar]]
    fundamentals: dict[str, FundamentalData]
    filings: dict[str, FilingData]
    news: dict[str, list[NewsItem]]
    macro: MacroData
    sector_map: dict[str, str] = Field(
        default_factory=dict, description="{ticker: sector_name}"
    )
    sector_averages: dict[str, SectorAverages] = Field(
        default_factory=dict, description="{sector_name: SectorAverages}"
    )
    benchmark_prices: list[PriceBar] = Field(default_factory=list)
    input_hash: str = ""
    metadata: SnapshotMetadata = Field(default_factory=SnapshotMetadata)
