from __future__ import annotations

from datetime import date

from eit_market_data.schemas.snapshot import FundamentalData, QuarterlyFinancials


def _q(label: str, report: date, revenue: float | None, net: float | None) -> QuarterlyFinancials:
    return QuarterlyFinancials(
        fiscal_quarter=label,
        report_date=report,
        revenue=revenue,
        net_income=net,
    )


def _four_quarters() -> list[QuarterlyFinancials]:
    # report_date-descending (most recent first), as the builder stores them.
    return [
        _q("2025Q4", date(2026, 3, 10), 40.0, 4.0),
        _q("2025Q3", date(2025, 11, 14), 30.0, 3.0),
        _q("2025Q2", date(2025, 8, 14), 20.0, 2.0),
        _q("2025Q1", date(2025, 5, 14), 10.0, 1.0),
    ]


def test_ttm_sums_latest_four_quarters() -> None:
    fund = FundamentalData(ticker="AAPL", quarters=_four_quarters())
    assert fund.ttm_revenue == 100.0
    assert fund.ttm_net_income == 10.0


def test_ttm_uses_only_the_four_most_recent_quarters() -> None:
    quarters = _four_quarters() + [_q("2024Q4", date(2025, 3, 10), 999.0, 999.0)]
    fund = FundamentalData(ticker="AAPL", quarters=quarters)
    # The trailing 4 (first 4) are summed; the older 5th is ignored.
    assert fund.ttm_revenue == 100.0


def test_ttm_none_when_fewer_than_four_quarters() -> None:
    fund = FundamentalData(ticker="AAPL", quarters=_four_quarters()[:3])
    assert fund.ttm_revenue is None
    assert fund.ttm_net_income is None


def test_ttm_none_when_a_quarter_field_is_missing() -> None:
    quarters = _four_quarters()
    quarters[1] = _q("2025Q3", date(2025, 11, 14), None, 3.0)
    fund = FundamentalData(ticker="AAPL", quarters=quarters)
    assert fund.ttm_revenue is None
    # net_income is fully populated, so its TTM still resolves.
    assert fund.ttm_net_income == 10.0


def test_ttm_helper_is_not_serialized() -> None:
    """The TTM accessors are plain properties: JSON shape must be unchanged."""
    fund = FundamentalData(ticker="AAPL", quarters=_four_quarters())
    dumped = fund.model_dump()
    assert "ttm_revenue" not in dumped
    assert "ttm_net_income" not in dumped
    assert set(dumped.keys()) == {
        "ticker",
        "quarters",
        "market_cap",
        "last_close_price",
    }
    # JSON round-trips back to an equal model (no new required fields).
    restored = FundamentalData.model_validate_json(fund.model_dump_json())
    assert restored.ticker == "AAPL"
    assert restored.ttm_revenue == 100.0
