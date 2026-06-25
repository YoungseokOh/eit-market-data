"""Sector-average metric computation shared across providers.

The metric formulas, guards, ``np.mean`` aggregation, and 4-decimal rounding
here are extracted verbatim from the duplicated bodies in
``yfinance_provider.fetch_sector_averages``, ``kr.pykrx_provider``,
``kr.ci_safe_provider``, and ``local_collection.compute_sector_averages_from_state``.

Only the *metric math* lives here. Each call site keeps its own gathering /
exception-handling policy (KR uses ``gather(return_exceptions=True)`` +
``isinstance`` filtering; yfinance uses a plain ``gather``); they pass the
already-resolved list of ``FundamentalData`` in and receive a ``SectorAverages``.
"""

from __future__ import annotations

import numpy as np

from eit_market_data.schemas.snapshot import FundamentalData, SectorAverages


def compute_sector_averages(
    sector: str, fundamentals: list[FundamentalData]
) -> SectorAverages:
    """Compute average sector metrics from each fund's most recent quarter.

    Uses the latest quarter (``quarters[0]``) of every fund that has revenue and
    non-zero total assets. Each metric is the mean across qualifying funds,
    rounded to 4 decimals. Funds with no quarters or missing revenue/total
    assets are skipped.
    """
    metrics: dict[str, list[float]] = {}
    for fund in fundamentals:
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
        _add("roe", (q.net_income or 0) / q.total_equity if q.total_equity else None)
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

    avg_metrics: dict[str, float] = {}
    for key, values in metrics.items():
        if values:
            avg_metrics[key] = round(float(np.mean(values)), 4)

    return SectorAverages(sector=sector, avg_metrics=avg_metrics)
