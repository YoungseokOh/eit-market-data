"""Point-in-time snapshot schemas.

A MonthlySnapshot freezes all data available at the decision date to enforce
strict point-in-time discipline (no look-ahead bias).
"""

from __future__ import annotations

from datetime import date, datetime  # noqa: TC003
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

    Aggregate monetary values (revenue, net_income, total_assets, ...) are in
    the reporting currency's *raw native units* — KRW (won) for KR via DART and
    USD for US via SEC XBRL. This matches ``FundamentalData.market_cap`` (also
    raw native currency), so consumer ratios such as ``net_income/market_cap``,
    ROA, ROE and ``market_cap/total_equity`` are unitless and need no per-market
    scaling. Per-share fields (``eps``, ``dividends_per_share``) and
    ``last_close_price`` are in native currency per share. ``issued_shares`` is
    a raw share count. None means the value is unavailable.
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
        default=None,
        description=(
            "As of decision date, in raw native currency (KRW won for KR, USD "
            "for US) — same unit as the quarterly aggregate fields."
        ),
    )
    last_close_price: float | None = Field(
        default=None, description="Closing price at decision date",
    )

    def _ttm_flow(self, field: str) -> float | None:
        """Sum an income/cash-flow field over the latest four quarters.

        Returns None unless at least four quarters are present and all four
        carry a value for ``field``. This is a derived convenience accessor: it
        is a plain property (NOT a Pydantic ``computed_field``), so it is never
        serialized and does not change the on-disk JSON shape — existing bundles
        round-trip unchanged. ``quarters`` is stored report_date-descending, so
        the first four are the trailing twelve months.
        """
        recent = self.quarters[:4]
        if len(recent) < 4:
            return None
        values = [getattr(q, field, None) for q in recent]
        if any(v is None for v in values):
            return None
        return float(sum(values))

    @property
    def ttm_revenue(self) -> float | None:
        """Trailing-twelve-month revenue, or None if the window is incomplete."""
        return self._ttm_flow("revenue")

    @property
    def ttm_net_income(self) -> float | None:
        """Trailing-twelve-month net income, or None if the window is incomplete."""
        return self._ttm_flow("net_income")


# ---------------------------------------------------------------------------
# Filing / qualitative data
# ---------------------------------------------------------------------------

class FilingData(BaseModel):
    """Extracted text sections from the most recent filing (10-K/10-Q or equivalent).

    Backward-compatible "trailing annual filing history" extension (Option A):
    the single top-level fields below always describe the most-recent filing and
    are mirrored by ``history[0]`` when ``history`` is populated. ``history`` is a
    newest-first list of the current plus up to two prior annual filings. Each
    ``history`` entry is itself a :class:`FilingData` but carries an empty nested
    ``history`` to avoid unbounded recursion. Existing consumers that read the
    single top-level fields (e.g. ``filing.risks``) are unaffected.
    """

    ticker: str
    filing_date: date | None = None
    filing_type: str | None = Field(default=None, description="e.g. '10-K', '有価証券報告書'")
    business_overview: str | None = None
    risks: str | None = None
    mda: str | None = Field(default=None, description="Management Discussion & Analysis")
    governance: str | None = None
    fiscal_year: int | None = Field(
        default=None,
        description="Fiscal year the filing reports on (history entries); None for the legacy single record",
    )
    history: list[FilingData] = Field(
        default_factory=list,
        description="Newest-first trailing annual filings (current + up to 2 prior); history[0] == top-level fields",
    )


# Resolve the self-referential ``history`` forward reference (PEP 563 strings).
FilingData.model_rebuild()


# ---------------------------------------------------------------------------
# News data
# ---------------------------------------------------------------------------

class NewsItem(BaseModel):
    """A single news headline/article."""

    date: date
    published_at: datetime | None = None
    source: str = ""
    headline: str
    summary: str = ""
    url: str | None = None


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
    news: dict[str, list[NewsItem]] = Field(default_factory=dict)
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
