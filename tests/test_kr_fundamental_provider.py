from __future__ import annotations
from datetime import date

import pandas as pd
import pytest

from eit_market_data.kr.dart_provider import (
    DartProvider,
    _extract_issued_shares_from_document,
    _normalize_ticker,
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


def test_dart_provider_normalize_ticker_preserves_alphanumeric_krx_code() -> None:
    assert _normalize_ticker("0126Z0") == "0126Z0"
    assert _normalize_ticker("5930") == "005930"


def test_dart_provider_reraises_report_list_failure_when_strict() -> None:
    provider = object.__new__(DartProvider)
    provider._raise_on_error = True
    provider._ticker_to_corp_code = lambda ticker: "00105855"  # type: ignore[method-assign]
    provider._fetch_report_list = lambda corp_code, as_of: (_ for _ in ()).throw(  # type: ignore[method-assign]
        TimeoutError("ConnectTimeout")
    )

    with pytest.raises(TimeoutError, match="ConnectTimeout"):
        provider._fetch_fundamentals_sync("010120", date(2026, 5, 4), 8)


def test_dart_provider_filing_falls_back_when_latest_document_is_missing() -> None:
    provider = object.__new__(DartProvider)
    provider._ticker_to_corp_code = lambda ticker: "00139214"  # type: ignore[method-assign]
    provider._fetch_report_list = lambda corp_code, as_of: pd.DataFrame(  # type: ignore[method-assign]
        [
            {
                "rcept_dt": "20260313",
                "report_nm": "[첨부정정]사업보고서 (2025.12)",
                "rcept_no": "20260313001226",
            },
            {
                "rcept_dt": "20250328",
                "report_nm": "[기재정정]사업보고서 (2024.12)",
                "rcept_no": "20250328001868",
            },
        ]
    )

    def fake_fetch_document(rcept_no: str) -> str:
        if rcept_no == "20260313001226":
            raise ValueError("{'status': '014', 'message': '파일이 존재하지 않습니다.'}")
        return "<DOCUMENT><SECTION>Ⅱ. 사업의 내용\n정상 사업 개요 텍스트입니다. 충분히 긴 본문입니다. " * 5

    provider._fetch_document = fake_fetch_document  # type: ignore[method-assign]

    filing = provider._fetch_filing_sync("000810", date(2026, 4, 30))

    assert filing.filing_date == date(2025, 3, 28)
    assert filing.business_overview is not None


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


def test_extract_issued_shares_from_dart_share_count_table() -> None:
    doc_text = """
    4. 주식의 총수 등
    가. 주식의 총수 현황
    구 분
    주식의 종류
    보통주
    합계
    Ⅳ. 발행주식의 총수 (Ⅱ-Ⅲ)
    97,801,344
    97,801,344
    -
    """

    assert _extract_issued_shares_from_document(doc_text) == 97801344.0


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


def test_composite_provider_falls_back_to_dart_shares_for_market_cap() -> None:
    class DummyDartProvider:
        async def fetch_fundamentals(self, ticker, as_of, n_quarters=8):  # noqa: ANN001
            return FundamentalData(
                ticker=ticker,
                quarters=[
                    QuarterlyFinancials(
                        fiscal_quarter="2025Q3",
                        report_date=date(2025, 11, 14),
                        net_income=120.0,
                    )
                ],
            )

        async def fetch_issued_shares(self, ticker, as_of):  # noqa: ANN001
            _ = (ticker, as_of)
            return 1000.0

    class DummyPriceProvider:
        async def fetch_prices(self, ticker, as_of, lookback_days=10):  # noqa: ANN001
            from eit_market_data.schemas.snapshot import PriceBar

            return [PriceBar(date=as_of, open=10, high=10, low=10, close=12, volume=1)]

    provider = CompositeKrFundamentalProvider(
        dart_provider=DummyDartProvider(),
        price_provider=DummyPriceProvider(),
    )
    provider._fetch_market_snapshot_sync = lambda ticker, as_of: {  # type: ignore[method-assign]
        "last_close_price": None,
        "market_cap": None,
        "issued_shares": None,
    }

    import asyncio

    fundamentals = asyncio.run(provider.fetch_fundamentals("247540", date(2025, 12, 31)))

    assert fundamentals.market_cap == 12000.0
    assert fundamentals.last_close_price == 12.0
    assert fundamentals.quarters[0].issued_shares == 1000.0


def test_composite_provider_refetches_remote_market_cap_when_local_snapshot_misses_ticker(
    monkeypatch,
) -> None:
    provider = CompositeKrFundamentalProvider(
        dart_provider=object(),
        price_provider=None,
    )

    local_frame = pd.DataFrame(
        {
            "시가총액": [100.0],
            "상장주식수": [10.0],
        },
        index=pd.Index(["005930"], name="종목코드"),
    )
    remote_frame = pd.DataFrame(
        {
            "시가총액": [450000000000.0],
            "상장주식수": [6430000.0],
        },
        index=pd.Index(["247540"], name="티커"),
    )
    calls: list[tuple[str, bool]] = []

    def fake_fetch_market_cap_frame(as_of, market, *, use_local=True):  # noqa: ANN001
        _ = as_of
        calls.append((market, use_local))
        if use_local:
            return local_frame
        if market == "KOSDAQ":
            return remote_frame
        return pd.DataFrame(columns=["시가총액", "상장주식수"])

    monkeypatch.setattr(
        "eit_market_data.kr.fundamental_provider.fetch_market_cap_frame",
        fake_fetch_market_cap_frame,
    )

    snapshot = provider._fetch_market_snapshot_sync("247540", date(2025, 12, 31))

    assert snapshot["market_cap"] == 450000000000.0
    assert snapshot["issued_shares"] == 6430000.0
    assert ("KOSDAQ", False) in calls
