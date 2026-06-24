"""SEC EDGAR XBRL fundamentals provider (point-in-time).

Implements ``FundamentalProvider`` from the SEC EDGAR XBRL ``companyfacts`` API
(https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json). Unlike yfinance —
which only retains a rolling ~5 quarters — every XBRL fact carries the actual
``filed`` date and reporting period, so arbitrary *historical* months can be
reconstructed point-in-time:

* a fact is visible at ``as_of`` only when ``filed <= as_of``;
* when a period was restated, the latest filing known by ``as_of`` wins;
* ``report_date`` is the real SEC ``filed`` date (no 60-day approximation).

One ``companyfacts`` document per ticker (cached on disk) covers the company's
whole history, so multi-month / multi-year backfills are cheap once warm.

``market_cap`` is reconstructed as-of: as-of shares outstanding (dei cover-page
count, ``filed <= as_of``) times the unadjusted close on ``decision_date`` (via
the injected price provider). Requires ``SEC_EDGAR_USER_AGENT``.
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import date, timedelta
from typing import Any

from eit_market_data.edgar_provider import (
    _get_httpx_client,
    _rate_limited_get,
    _ticker_to_cik,
)
from eit_market_data.schemas.snapshot import FundamentalData, QuarterlyFinancials

logger = logging.getLogger(__name__)

_XBRL_CACHE_DIR = os.environ.get(
    "EIT_EDGAR_XBRL_CACHE_DIR",
    os.path.join(os.getcwd(), "data", "edgar_xbrl_cache"),
)
_XBRL_CACHE_SIZE_LIMIT = int(
    os.environ.get("EIT_EDGAR_XBRL_CACHE_SIZE_LIMIT_BYTES", str(20 * 1024 * 1024 * 1024))
)

# A flow fact spanning roughly one fiscal quarter (vs YTD / annual cumulants).
_MIN_QUARTER_DAYS = 80
_MAX_QUARTER_DAYS = 100
# If the newest available quarter ends more than this many days before as_of, the
# ticker is treated as a coverage gap and its fundamentals are dropped (avoids
# feeding a years-stale quarter to the value screen). ~9 months tolerates normal
# filing lag and an occasional missed quarter.
_MAX_STALE_DAYS = 270

# us-gaap tag preference lists. First populated tag wins.
_FLOW_TAGS: dict[str, list[str]] = {
    "revenue": [
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "Revenues",
        "RevenueFromContractWithCustomerIncludingAssessedTax",
        "SalesRevenueNet",
        # Banks / insurers report top line differently:
        "RevenuesNetOfInterestExpense",
        "InterestAndDividendIncomeOperating",
    ],
    "cost_of_goods_sold": ["CostOfGoodsAndServicesSold", "CostOfRevenue", "CostOfGoodsSold"],
    "gross_profit": ["GrossProfit"],
    "operating_income": ["OperatingIncomeLoss"],
    "net_income": ["NetIncomeLoss", "ProfitLoss"],
    "interest_expense": ["InterestExpense", "InterestExpenseNonoperating"],
    "operating_cash_flow": [
        "NetCashProvidedByUsedInOperatingActivities",
        "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations",
    ],
    "capital_expenditure": [
        "PaymentsToAcquirePropertyPlantAndEquipment",
        "PaymentsToAcquireProductiveAssets",
        "PaymentsForCapitalImprovements",
    ],
    "dividends_paid": ["PaymentsOfDividendsCommonStock", "PaymentsOfDividends"],
}
# Depreciation & amortization, used to synthesize EBITDA = operating_income + D&A.
_DEP_AMORT_TAGS = [
    "DepreciationDepletionAndAmortization",
    "DepreciationAmortizationAndAccretionNet",
    "DepreciationAndAmortization",
    "DepreciationDepletionAndAmortizationPropertyPlantAndEquipment",
    "DepreciationAmortizationAndOther",
    "CostsAndExpensesDepreciationAndAmortization",
    "Depreciation",
]
_EPS_TAGS = ["EarningsPerShareDiluted", "EarningsPerShareBasic"]
_INSTANT_TAGS: dict[str, list[str]] = {
    "total_assets": ["Assets"],
    "total_liabilities": ["Liabilities"],
    "total_equity": [
        "StockholdersEquity",
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
    ],
    "current_assets": ["AssetsCurrent"],
    "current_liabilities": ["LiabilitiesCurrent"],
    "cash_and_equivalents": [
        "CashAndCashEquivalentsAtCarryingValue",
        "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
    ],
    "inventory": ["InventoryNet"],
    "accounts_receivable": ["AccountsReceivableNetCurrent", "ReceivablesNetCurrent"],
    "issued_shares": ["CommonStockSharesOutstanding", "CommonStockSharesIssued"],
}
_SHARES_TAGS = [
    ("dei", "EntityCommonStockSharesOutstanding"),
    ("us-gaap", "CommonStockSharesOutstanding"),
    ("us-gaap", "CommonStockSharesIssued"),
    # Fallback for issuers that report shares only via class-dimensioned tags the
    # flattened companyfacts omits (e.g. META, BRK-B, FOX, NWS): the EPS-driver
    # weighted-average diluted/basic share count is present for virtually every
    # filer. A period-average count is a sound market-cap proxy and far better
    # than dropping a mega-cap from the universe.
    ("us-gaap", "WeightedAverageNumberOfDilutedSharesOutstanding"),
    ("us-gaap", "WeightedAverageNumberOfSharesOutstandingBasic"),
]


def _as_date(value: Any) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except (ValueError, TypeError):
        return None


def _pick_pit(entries: list[dict], as_of: date, *, instant: bool) -> dict[str, dict]:
    """Return {period_key: chosen_fact} honouring point-in-time visibility.

    Only facts with ``filed <= as_of`` are eligible. For each period the fact
    with the latest ``filed`` (most recent restatement known by ``as_of``) wins.
    Flow facts are restricted to ~one-quarter durations; instants keyed by end.
    """
    chosen: dict[str, dict] = {}
    for e in entries:
        filed = _as_date(e.get("filed"))
        end = _as_date(e.get("end"))
        if filed is None or end is None or filed > as_of:
            continue
        if instant:
            key = end.isoformat()
        else:
            start = _as_date(e.get("start"))
            if start is None:
                continue
            dur = (end - start).days
            if not (_MIN_QUARTER_DAYS <= dur <= _MAX_QUARTER_DAYS):
                continue
            key = f"{start.isoformat()}:{end.isoformat()}"
        prev = chosen.get(key)
        if prev is None or _as_date(prev.get("filed")) < filed:
            chosen[key] = e
    return chosen


def _fiscal_label(end_d: date, fy: Any, fp: Any) -> str:
    """True fiscal-quarter label from the NATIVE (earliest-filed) fy/fp.

    The caller passes fy/fp taken from the period's *original* filing (not a later
    comparative), so they are uncontaminated and uniquely identify the fiscal
    quarter: Q1/Q2/Q3 map directly and FY (the annual standalone = annual - 9M)
    maps to Q4. Falls back to the period-end calendar quarter only when fp is
    missing.
    """
    fp_s = str(fp).upper() if fp else ""
    if fp_s in ("Q1", "Q2", "Q3"):
        year = int(fy) if fy else end_d.year
        return f"{year}{fp_s}"
    if fp_s == "FY":
        year = int(fy) if fy else end_d.year
        return f"{year}Q4"
    q_num = (end_d.month - 1) // 3 + 1
    return f"{end_d.year}Q{q_num}"


def _standalone_flows(entries: list[dict], as_of: date) -> dict[str, dict]:
    """Decompose flow facts into STANDALONE quarter values keyed by period end.

    Cash-flow and many income items are filed as fiscal-year-to-date cumulants
    (3M, 6M, 9M, FY) rather than standalone quarters. Within a fiscal year all
    cumulants share the same ``start`` (the FY start), so the standalone quarter
    ending at E is ``cum(start, E) - cum(start, prev_end)``. This also yields the
    accounting Q4 automatically (annual - 9-month YTD). A genuine ~one-quarter
    fact (80-100 days) is used directly when present.

    Returns ``{end_iso: {"val", "filed", "fy", "fp"}}`` where fy/fp come from the
    end fact so the caller can label the true fiscal quarter (FY -> Q4).
    """
    from collections import defaultdict

    # Per (start, end): latest-filed wins for the VALUE (restatements), but the
    # EARLIEST-filed fact carries the native fy/fp label. companyfacts fy/fp
    # reflect the *report* a fact came from, so a comparative re-filing contaminates
    # the label; the original filing (earliest filed) labels the period correctly.
    chosen: dict[tuple[date, date], dict] = {}
    native_label: dict[tuple[date, date], tuple[Any, Any, date]] = {}
    for e in entries:
        filed = _as_date(e.get("filed"))
        start = _as_date(e.get("start"))
        end = _as_date(e.get("end"))
        if filed is None or start is None or end is None or filed > as_of:
            continue
        if (end - start).days < _MIN_QUARTER_DAYS:
            continue  # drop sub-quarter / partial slivers
        key = (start, end)
        prev = chosen.get(key)
        if prev is None or _as_date(prev.get("filed")) < filed:
            chosen[key] = e
        lbl = native_label.get(key)
        if lbl is None or filed < lbl[2]:
            native_label[key] = (e.get("fy"), e.get("fp"), filed)

    by_start: dict[date, list[tuple[date, dict]]] = defaultdict(list)
    for (start, end), e in chosen.items():
        by_start[start].append((end, e))

    out: dict[str, dict] = {}
    for start, items in by_start.items():
        items.sort(key=lambda x: x[0])  # by end ascending = cumulative chain
        prev_cum: float | None = None
        for end, e in items:
            try:
                cum = float(e["val"])
            except (KeyError, ValueError, TypeError):
                prev_cum = None
                continue
            dur = (end - start).days
            if _MIN_QUARTER_DAYS <= dur <= _MAX_QUARTER_DAYS:
                standalone = cum  # genuine single quarter
            elif prev_cum is not None:
                standalone = cum - prev_cum  # YTD diff (incl. annual - 9M = Q4)
            else:
                standalone = None  # first period of FY but not ~quarter-long
            prev_cum = cum
            if standalone is None:
                continue
            end_iso = end.isoformat()
            # Prefer a direct single-quarter fact over a derived diff for the
            # same end; otherwise latest-filed wins.
            existing = out.get(end_iso)
            is_direct = _MIN_QUARTER_DAYS <= dur <= _MAX_QUARTER_DAYS
            if (
                existing is None
                or (is_direct and not existing.get("_direct"))
                or _as_date(existing.get("filed")) < _as_date(e.get("filed"))
            ):
                nat_fy, nat_fp, _f = native_label.get((start, end), (None, None, None))
                out[end_iso] = {
                    "val": standalone,
                    "filed": e.get("filed"),
                    "fy": nat_fy,
                    "fp": nat_fp,
                    "_direct": is_direct,
                }
    return out


class EdgarXbrlFundamentalProvider:
    """FundamentalProvider backed by SEC EDGAR XBRL companyfacts."""

    def __init__(self, price_provider: Any | None = None) -> None:
        if not os.environ.get("SEC_EDGAR_USER_AGENT"):
            raise ValueError(
                "SEC_EDGAR_USER_AGENT is required for EdgarXbrlFundamentalProvider"
            )
        self._price_provider = price_provider
        self._semaphore = asyncio.Semaphore(5)
        try:
            import diskcache

            self._cache: Any = diskcache.Cache(
                _XBRL_CACHE_DIR, size_limit=_XBRL_CACHE_SIZE_LIMIT
            )
        except Exception as exc:  # noqa: BLE001 - cache optional
            logger.warning("XBRL disk cache unavailable, running uncached: %s", exc)
            self._cache = None

    # ------------------------------------------------------------------
    # companyfacts retrieval (cached)
    # ------------------------------------------------------------------

    async def _companyfacts(self, cik: str) -> dict | None:
        cache_key = f"companyfacts:{cik}"
        if self._cache is not None:
            cached = self._cache.get(cache_key)
            if cached is not None:
                return cached or None
        url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
        async with self._semaphore:
            async with _get_httpx_client() as client:
                text = await _rate_limited_get(client, url)
        if not text:
            if self._cache is not None:
                self._cache.set(cache_key, {}, expire=60 * 60 * 24)  # short negative cache
            return None
        import json

        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            return None
        if self._cache is not None:
            self._cache.set(cache_key, data)
        return data

    # ------------------------------------------------------------------
    # FundamentalProvider
    # ------------------------------------------------------------------

    async def fetch_fundamentals(
        self, ticker: str, as_of: date, n_quarters: int = 8
    ) -> FundamentalData:
        try:
            return await self._fetch_impl(ticker, as_of, n_quarters)
        except Exception as exc:  # noqa: BLE001 - never propagate to caller
            logger.warning("XBRL fundamentals failed for %s: %s", ticker, exc)
            return FundamentalData(ticker=ticker)

    async def _fetch_impl(
        self, ticker: str, as_of: date, n_quarters: int
    ) -> FundamentalData:
        async with _get_httpx_client() as client:
            cik = await _ticker_to_cik(client, ticker)
        if not cik:
            return FundamentalData(ticker=ticker)
        facts = await self._companyfacts(cik)
        if not facts:
            return FundamentalData(ticker=ticker)

        gaap = facts.get("facts", {}).get("us-gaap", {})
        dei = facts.get("facts", {}).get("dei", {})

        def units(node: dict) -> list[dict]:
            u = node.get("units", {}) if isinstance(node, dict) else {}
            out: list[dict] = []
            for arr in u.values():
                out.extend(arr)
            return out

        def merged_units(tags: list[str]) -> list[dict]:
            """Union the units of every candidate tag that is present.

            Companies migrate XBRL tags but keep the deprecated key present
            (populated only through ~2019-2020). First-present-wins would lock
            onto the dead tag; merging + per-period latest-filed-wins lets the
            current tag's recent periods surface naturally.
            """
            out: list[dict] = []
            for tag in tags:
                if tag in gaap:
                    out.extend(units(gaap[tag]))
            return out

        # Flow metrics: decompose YTD cumulants into standalone quarters keyed by
        # end -> {val, filed, fy, fp}. This recovers accounting Q4 (annual - 9M).
        flow_pit: dict[str, dict[str, dict]] = {}
        for schema_key, tags in _FLOW_TAGS.items():
            u = merged_units(tags)
            if u:
                flow_pit[schema_key] = _standalone_flows(u, as_of)
        eps_u = merged_units(_EPS_TAGS)
        eps_pit: dict[str, dict] = _standalone_flows(eps_u, as_of) if eps_u else {}
        dep_u = merged_units(_DEP_AMORT_TAGS)
        dep_pit: dict[str, dict] = _standalone_flows(dep_u, as_of) if dep_u else {}

        # Instant (balance-sheet) metrics keyed by period-end.
        instant_pit: dict[str, dict[str, dict]] = {}
        for schema_key, tags in _INSTANT_TAGS.items():
            u = merged_units(tags)
            if u:
                instant_pit[schema_key] = _pick_pit(u, as_of, instant=True)

        # Candidate quarter ends = end dates of revenue / net_income / OCF flows.
        # Cluster ends within a few days (issuers report the same fiscal quarter
        # end as e.g. 03-30 vs 03-31 across statements / 52-53 week calendars);
        # collapse each cluster to its latest end so one fiscal quarter isn't
        # split into duplicates.
        raw_ends: set[date] = set()
        for key in ("revenue", "net_income", "operating_cash_flow"):
            for k in flow_pit.get(key, {}):
                d = _as_date(k)
                if d is not None:
                    raw_ends.add(d)
        clusters: list[date] = []
        for d in sorted(raw_ends):
            if clusters and (d - clusters[-1]).days <= 7:
                clusters[-1] = d  # extend cluster to the later end
            else:
                clusters.append(d)

        def _near(period_map: dict[str, dict], end_d: date) -> dict | None:
            """Fetch a metric fact whose end is within the quarter's cluster."""
            best: dict | None = None
            best_end: date | None = None
            for k, fact in period_map.items():
                kd = _as_date(k)
                if kd is None or abs((kd - end_d).days) > 7:
                    continue
                if best_end is None or abs((kd - end_d).days) < abs((best_end - end_d).days):
                    best, best_end = fact, kd
            return best

        dated_quarters: list[tuple[date, QuarterlyFinancials]] = []
        for end_d in clusters:
            end_iso = end_d.isoformat()
            q_data: dict[str, Any] = {}
            report_filed: date | None = None

            def _consume(fact: dict | None) -> float | None:
                nonlocal report_filed
                if not fact:
                    return None
                filed = _as_date(fact.get("filed"))
                if filed is not None and (report_filed is None or filed > report_filed):
                    report_filed = filed
                try:
                    return float(fact["val"])
                except (KeyError, ValueError, TypeError):
                    return None

            # Fiscal label comes from a single authoritative fact: net_income
            # preferred, else revenue. Mixing sources lets a stale fy from one
            # metric mislabel the quarter.
            label_fact = _near(flow_pit.get("net_income", {}), end_d) or _near(
                flow_pit.get("revenue", {}), end_d
            )
            fy_label = label_fact.get("fy") if label_fact else None
            fp_label = label_fact.get("fp") if label_fact else None

            for schema_key, period_map in flow_pit.items():
                v = _consume(_near(period_map, end_d))
                if v is not None:
                    q_data[schema_key] = v
            eps_v = _consume(_near(eps_pit, end_d))
            if eps_v is not None:
                q_data["eps"] = eps_v
            for schema_key, period_map in instant_pit.items():
                v = _consume(_near(period_map, end_d))
                if v is not None:
                    q_data[schema_key] = v

            # EBITDA = operating income + D&A (synthesized; XBRL has no single
            # EBITDA tag). Only when both legs are present for this quarter.
            da_v = _consume(_near(dep_pit, end_d))
            if q_data.get("operating_income") is not None and da_v is not None:
                q_data["ebitda"] = q_data["operating_income"] + da_v

            # Free cash flow = OCF - capex (capex stored as a positive outflow).
            if "operating_cash_flow" in q_data and "capital_expenditure" in q_data:
                q_data["free_cash_flow"] = (
                    q_data["operating_cash_flow"] - q_data["capital_expenditure"]
                )

            if report_filed is None or report_filed > as_of:
                continue
            if "revenue" not in q_data and "net_income" not in q_data:
                continue

            dated_quarters.append(
                (
                    end_d,
                    QuarterlyFinancials(
                        fiscal_quarter=_fiscal_label(end_d, fy_label, fp_label),
                        report_date=report_filed,
                        **q_data,
                    ),
                )
            )

        # Order by the economic period END (most recent quarter first), NOT by
        # filing date. Old periods reappear as *comparative* columns in recent
        # 10-K/10-Q filings, so their latest ``filed`` date looks recent; sorting
        # by filed date would float a stale quarter to quarters[0] and drive the
        # value screen off ancient fundamentals. End-date ordering is the true
        # point-in-time recency (each kept quarter still has filed <= as_of).
        dated_quarters.sort(key=lambda item: item[0], reverse=True)
        # Staleness guard: if the newest available quarter ends far before as_of,
        # this ticker has a tag-coverage gap (its recent quarters aren't tagged
        # under the us-gaap tags we read), and end-date ordering would otherwise
        # surface a years-old quarter as "most recent". Drop the ticker's
        # fundamentals entirely rather than feed the value screen stale data.
        if dated_quarters and dated_quarters[0][0] < as_of - timedelta(days=_MAX_STALE_DAYS):
            dated_quarters = []
        quarters = [q for _end, q in dated_quarters[:n_quarters]]

        # As-of shares outstanding (cover-page count, filed <= as_of).
        shares = self._asof_shares(gaap, dei, as_of)
        last_close = self._asof_close(ticker, as_of)
        market_cap = (
            round(last_close * shares, 2)
            if (last_close is not None and shares)
            else None
        )

        return FundamentalData(
            ticker=ticker,
            quarters=quarters,
            market_cap=market_cap,
            last_close_price=last_close,
        )

    def _asof_shares(self, gaap: dict, dei: dict, as_of: date) -> float | None:
        """As-of shares outstanding for the market-cap reconstruction.

        Pools every share tag (dei cover-page + us-gaap) and picks the count with
        the most recent period end (filed<=as_of, end<=as_of), tie-broken by
        latest filing then largest count. A **staleness guard** rejects counts
        whose end is far before as_of (prevents using a decade-old cover-page
        value, e.g. a 2011 Class-A count for a 2023 market cap). For dual-class
        issuers the same end can carry multiple per-class counts; taking the
        largest selects the primary common class. (Dual-class total economic
        shares are not fully reconstructable from companyfacts; this avoids the
        gross error rather than perfectly modelling every class.)
        """
        # Tag PRIORITY matters: cover-page *outstanding* before us-gaap
        # *outstanding* before *issued* (issued includes treasury stock and would
        # overstate the count). Return the first tag that yields a non-stale
        # recent value; only break ties WITHIN a tag (same end -> larger count,
        # which for dual-class issuers selects the primary common class).
        for ns, tag in _SHARES_TAGS:
            node = (dei if ns == "dei" else gaap).get(tag)
            if not isinstance(node, dict):
                continue
            best_key: tuple[date, date, float] | None = None
            best_val: float | None = None
            for arr in node.get("units", {}).values():
                for e in arr:
                    filed = _as_date(e.get("filed"))
                    end = _as_date(e.get("end"))
                    if filed is None or end is None or filed > as_of or end > as_of:
                        continue
                    if end < as_of - timedelta(days=_MAX_STALE_DAYS):
                        continue  # staleness guard
                    try:
                        val = float(e["val"])
                    except (KeyError, ValueError, TypeError):
                        continue
                    key = (end, filed, val)
                    if best_key is None or key > best_key:
                        best_key, best_val = key, val
            if best_val:
                return best_val
        return None

    def _price_prov(self) -> Any | None:
        if self._price_provider is None:
            try:
                from eit_market_data.yfinance_provider import YFinanceProvider

                self._price_provider = YFinanceProvider()
            except Exception:  # noqa: BLE001
                return None
        return self._price_provider

    def _asof_close(self, ticker: str, as_of: date) -> float | None:
        """As-*traded* (split-consistent) close on/just before as_of.

        yfinance's auto_adjust=False close is still split-adjusted to today, but
        XBRL share counts are as-reported (pre-split). Multiplying them directly
        understates market_cap by the split factor (e.g. NVDA 10:1 in 2024). We
        undo splits that occurred *after* as_of so price and shares share one
        basis, making market_cap = close x shares split-invariant.
        """
        provider = self._price_prov()
        if provider is None:
            return None
        asof_close = getattr(provider, "_asof_close", None)
        if not callable(asof_close):
            return None
        try:
            close = asof_close(ticker, as_of)
        except Exception:  # noqa: BLE001
            return None
        if close is None:
            return None
        return round(close * self._split_factor_after(ticker, as_of), 4)

    def _split_factor_after(self, ticker: str, as_of: date) -> float:
        """Product of split ratios with split date > as_of (1.0 if none)."""
        provider = self._price_prov()
        get_ticker = getattr(provider, "_get_ticker", None)
        if not callable(get_ticker):
            return 1.0
        try:
            splits = get_ticker(ticker).splits
        except Exception:  # noqa: BLE001
            return 1.0
        factor = 1.0
        try:
            for ts, ratio in splits.items():
                d = ts.date() if hasattr(ts, "date") else ts
                if d > as_of and ratio:
                    factor *= float(ratio)
        except Exception:  # noqa: BLE001
            return 1.0
        return factor
