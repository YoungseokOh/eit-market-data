from __future__ import annotations
from datetime import date

import pandas as pd

from eit_market_data.kr.dart_provider import (
    _normalize_quarter_values,
    _report_entries_from_list,
)
from eit_market_data.kr.fundamental_provider import CompositeKrFundamentalProvider
from eit_market_data.schemas.snapshot import FundamentalData, QuarterlyFinancials


def test_report_entries_use_actual_receipt_dates() -> None:
    report_list = pd.DataFrame(
        [
            {
                "bsns_year": "2024",
                "reprt_code": "11012",
                "rcept_dt": "20240814",
                "rcept_no": "202408140001",
            },
            {
                "bsns_year": "2024",
                "reprt_code": "11011",
                "rcept_dt": "20250310",
                "rcept_no": "202503100001",
            },
        ]
    )

    entries = _report_entries_from_list(report_list, date(2025, 3, 10))

    assert entries[0]["fiscal_quarter"] == "2024Q4"
    assert entries[0]["report_date"] == date(2025, 3, 10)
    assert entries[1]["fiscal_quarter"] == "2024Q2"
    assert entries[1]["report_date"] == date(2024, 8, 14)


def test_normalize_quarter_values_converts_cumulative_flow_fields() -> None:
    raw_quarter_map = {
        "2024Q1": {"revenue": 100.0, "eps": 10.0, "total_assets": 300.0},
        "2024Q2": {"revenue": 260.0, "eps": 26.0, "total_assets": 320.0},
        "2024Q3": {"revenue": 420.0, "eps": 42.0, "total_assets": 340.0},
        "2024Q4": {"revenue": 600.0, "eps": 60.0, "total_assets": 360.0},
    }

    q2 = _normalize_quarter_values("2024Q2", raw_quarter_map["2024Q2"], raw_quarter_map)
    q4 = _normalize_quarter_values("2024Q4", raw_quarter_map["2024Q4"], raw_quarter_map)

    assert q2["revenue"] == 160.0
    assert q2["eps"] == 16.0
    assert q2["total_assets"] == 320.0
    assert q4["revenue"] == 180.0
    assert q4["eps"] == 18.0


def test_composite_provider_merges_market_snapshot_fields() -> None:
    provider = object.__new__(CompositeKrFundamentalProvider)
    fundamentals = provider._merge_fundamentals(
        FundamentalData(
            ticker="005930",
            quarters=[
                QuarterlyFinancials(
                    fiscal_quarter="2024Q4",
                    report_date=date(2025, 3, 10),
                    revenue=120.0,
                    issued_shares=None,
                )
            ],
        ),
        {
            "last_close_price": 70000.0,
            "market_cap": 450000000000.0,
            "issued_shares": 6430000.0,
        },
    )

    assert fundamentals.market_cap == 450000000000.0
    assert fundamentals.last_close_price == 70000.0
    assert fundamentals.quarters[0].issued_shares == 6430000.0


def test_composite_provider_uses_price_snapshot_when_market_price_missing() -> None:
    provider = object.__new__(CompositeKrFundamentalProvider)
    fundamentals = provider._merge_fundamentals(
        FundamentalData(ticker="005930", quarters=[]),
        {
            "last_close_price": None,
            "market_cap": None,
            "issued_shares": None,
        },
        {"last_close_price": 71200.0},
    )

    assert fundamentals.last_close_price == 71200.0
