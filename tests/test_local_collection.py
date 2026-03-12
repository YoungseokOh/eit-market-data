from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from eit_market_data.kr.naver_news_provider import NaverArchiveNewsRecord
from eit_market_data.local_collection import (
    ValidationCheck,
    _is_sorted_dates,
    build_local_universe_manifest,
    build_run_root,
    summarize_checks,
    validate_kr_final_snapshot,
)
from eit_market_data.schemas.snapshot import (
    FilingData,
    FundamentalData,
    MacroData,
    MonthlySnapshot,
    NewsItem,
    PriceBar,
    SectorAverages,
    SnapshotMetadata,
)


def _snapshot_with_news(news_items: list[NewsItem]) -> MonthlySnapshot:
    ticker = "005930"
    return MonthlySnapshot(
        decision_date=date(2026, 3, 31),
        execution_date=date(2026, 4, 1),
        universe=[ticker],
        prices={
            ticker: [
                PriceBar(
                    date=date(2026, 3, 31),
                    open=1,
                    high=1,
                    low=1,
                    close=1,
                    volume=1,
                )
            ]
        },
        fundamentals={ticker: FundamentalData(ticker=ticker)},
        filings={ticker: FilingData(ticker=ticker, business_overview="overview")},
        news={ticker: news_items},
        macro=MacroData(rates_policy={"base_rate": 2.75}),
        sector_map={ticker: "IT"},
        sector_averages={"IT": SectorAverages(sector="IT", avg_metrics={})},
        benchmark_prices=[],
        input_hash="hash",
        metadata=SnapshotMetadata(created_at="2026-03-31T00:00:00"),
    )


def test_build_run_root_is_stable() -> None:
    path = build_run_root(
        storage_root=Path("/tmp/storage"),
        as_of=date(2026, 3, 31),
        market="both",
        phase="all",
        full_universe_kind="top300",
    )
    assert str(path) == "/tmp/storage/runs/2026-03-31/both_all_top300"


def test_validate_kr_final_snapshot_rejects_news_outside_target_month() -> None:
    ticker = "005930"
    snapshot = _snapshot_with_news(
        [
            NewsItem(date=date(2026, 2, 28), source="Naver", headline="이전달 기사"),
        ]
    )

    checks = validate_kr_final_snapshot(
        snapshot=snapshot,
        news_audit={
            ticker: [
                NaverArchiveNewsRecord(
                    date=date(2026, 2, 28),
                    headline="이전달 기사",
                    url="https://finance.naver.com/item/news_read.naver?article_id=1",
                )
            ]
        },
        as_of=date(2026, 3, 31),
    )

    assert any(
        check.name == f"kr:final_news:{ticker}" and check.detail == "date_out_of_month"
        for check in checks
    )


def test_summarize_checks_counts_failed_and_degraded() -> None:
    summary = summarize_checks(
        [
            ValidationCheck("ok", "ok", ""),
            ValidationCheck("warn", "degraded", "empty_macro"),
            ValidationCheck("bad", "failed", "missing_prices"),
        ]
    )

    assert summary["failed"] == 1
    assert summary["degraded"] == 1


def test_is_sorted_dates_accepts_both_directions() -> None:
    assert _is_sorted_dates([date(2026, 3, 1), date(2026, 3, 2), date(2026, 3, 3)])
    assert _is_sorted_dates([date(2026, 3, 3), date(2026, 3, 2), date(2026, 3, 1)])
    assert not _is_sorted_dates([date(2026, 3, 1), date(2026, 3, 3), date(2026, 3, 2)])


def test_build_local_universe_manifest_top_kind_keeps_market_cap(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "eit_market_data.local_collection._listing_metadata_frame",
        lambda: pd.DataFrame(
            [
                {"ticker": "005930", "name": "삼성전자", "market": "KOSPI", "sector": "IT"},
                {"ticker": "000660", "name": "SK하이닉스", "market": "KOSPI", "sector": "IT"},
            ]
        ),
    )

    def fake_cap_frame(as_of, market):  # noqa: ANN001, ANN202
        _ = as_of
        if market == "KOSPI":
            return pd.DataFrame(
                {
                    "종목코드": ["005930", "000660"],
                    "시가총액": [500.0, 300.0],
                }
            )
        return pd.DataFrame(columns=["종목코드", "시가총액"])

    monkeypatch.setattr("eit_market_data.local_collection.fetch_market_cap_frame", fake_cap_frame)

    output_path = tmp_path / "top100.csv"
    build_local_universe_manifest(
        as_of=date(2026, 3, 12),
        kind="top100",
        output_path=output_path,
    )

    frame = pd.read_csv(output_path)
    assert list(frame.columns) == ["ticker", "market", "sector", "name", "market_cap", "rank", "as_of"]
    assert frame["market_cap"].tolist() == [500.0, 300.0]
    assert frame["rank"].tolist() == [1, 2]
