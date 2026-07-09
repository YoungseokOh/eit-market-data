from __future__ import annotations

import asyncio
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest
from helpers import FakeCache

from eit_market_data.kr.news_catalog import KrNewsWindowCoverage
from eit_market_data.kr.naver_news_provider import NaverArchiveNewsRecord
import eit_market_data.local_collection as local_collection
from eit_market_data.local_collection import (
    CheckpointPolicy,
    KrCollectionState,
    ValidationCheck,
    _next_kr_execution_date,
    _run_kr_phase,
    _is_sorted_dates,
    _is_dart_transient_error,
    build_local_universe_manifest,
    build_run_root,
    summarize_checks,
    validate_kr_checkpoint,
    validate_kr_final_snapshot,
)
from eit_market_data.schemas.snapshot import (
    FilingData,
    FundamentalData,
    MacroData,
    MonthlySnapshot,
    NewsItem,
    PriceBar,
    QuarterlyFinancials,
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


def test_next_kr_execution_date_skips_weekend() -> None:
    # Friday 2026-05-08 -> next trading day is Monday 2026-05-11.
    assert _next_kr_execution_date(date(2026, 5, 8)) == date(2026, 5, 11)


def test_next_kr_execution_date_is_xkrx_holiday_aware() -> None:
    # Month-end decision dates whose naive next-weekday lands on a KRX holiday
    # must skip to the genuine next trading day.
    # 2024-12-30 -> 2024-12-31 is the year-end closure -> 2025-01-02.
    assert _next_kr_execution_date(date(2024, 12, 30)) == date(2025, 1, 2)
    # 2023-12-28 -> 2023-12-29 closure + New Year -> 2024-01-02.
    assert _next_kr_execution_date(date(2023, 12, 28)) == date(2024, 1, 2)
    # 2024-09-30 -> 2024-10-01 개천절 -> 2024-10-02.
    assert _next_kr_execution_date(date(2024, 9, 30)) == date(2024, 10, 2)
    # 2023-09-27 -> 추석 연휴(09-28,09-29) + 개천절(10-02,10-03) -> 2023-10-04.
    assert _next_kr_execution_date(date(2023, 9, 27)) == date(2023, 10, 4)


def test_run_kr_phase_uses_official_pykrx_raw_crawler(monkeypatch, tmp_path: Path) -> None:
    commands: list[list[str]] = []

    def fake_run_subprocess_stage(*, name, command, log_path):  # noqa: ANN001
        _ = (name, log_path)
        commands.append(command)

    class DummyCollector:
        def __init__(self, **kwargs):  # noqa: ANN003
            self.kwargs = kwargs

        async def collect(self, *, universe_csv: Path, resume: bool):  # noqa: ANN001
            _ = (universe_csv, resume)
            return {"summary_path": "summary.json"}

    monkeypatch.setattr(local_collection.runner, "run_subprocess_stage", fake_run_subprocess_stage)
    monkeypatch.setattr(
        local_collection.runner,
        "validate_kr_raw_outputs",
        lambda raw_root: [ValidationCheck("cap_daily", "ok", str(raw_root))],
    )
    monkeypatch.setattr(local_collection.runner, "LocalKrCollector", DummyCollector)

    run_root = tmp_path / "run"
    universe_csv = tmp_path / "universe.csv"
    universe_csv.write_text("ticker\n005930\n", encoding="utf-8")

    asyncio.run(
        _run_kr_phase(
            stage_name="full",
            as_of=date(2026, 5, 4),
            raw_start=date(2026, 1, 1),
            run_root=run_root,
            universe_csv=universe_csv,
            progress_path=run_root / "progress.json",
            resume=False,
            policy=CheckpointPolicy(every_tickers=25, every_seconds=600),
            dart_mode="live",
        )
    )

    assert len(commands) == 1
    command = commands[0]
    assert "scripts/crawl_kr_data_pykrx.py" in command
    assert "scripts/crawl_kr_data_fallback.py" not in command
    assert "--skip-meta" in command
    assert "--skip-ohlcv" in command
    assert "--universe-csv" not in command


def test_local_kr_collector_skips_inter_ticker_delay_in_cache_only(tmp_path: Path) -> None:
    collector = local_collection.LocalKrCollector(
        as_of=date(2026, 5, 4),
        storage_root=tmp_path,
        bundle_root=tmp_path / "bundles",
        partial_root=tmp_path / "partials",
        checkpoint_root=tmp_path / "reports",
        policy=CheckpointPolicy(every_tickers=25, every_seconds=600),
        progress_path=tmp_path / "progress.json",
        dart_mode="cache_only",
    )

    assert collector.inter_ticker_delay_seconds == 0.0


def test_local_kr_collector_keeps_inter_ticker_delay_for_live_dart(
    tmp_path: Path,
    monkeypatch,
) -> None:
    # Live DART mode eagerly constructs DartProvider, which validates the API
    # key over the network. Stub it (and the ECOS macro provider) so the
    # collector builds offline; the assertion below only inspects the
    # configured inter-ticker delay.
    from types import SimpleNamespace

    monkeypatch.setattr(local_collection.collector, "DartProvider", lambda **kwargs: object())
    monkeypatch.setattr(
        local_collection.collector, "PykrxProvider", lambda **kwargs: SimpleNamespace()
    )
    monkeypatch.setattr(
        local_collection.collector,
        "CompositeKrFundamentalProvider",
        lambda **kwargs: object(),
    )
    monkeypatch.setattr(local_collection.collector, "EcosMacroProvider", lambda: object())
    collector = local_collection.LocalKrCollector(
        as_of=date(2026, 5, 4),
        storage_root=tmp_path,
        bundle_root=tmp_path / "bundles",
        partial_root=tmp_path / "partials",
        checkpoint_root=tmp_path / "reports",
        policy=CheckpointPolicy(every_tickers=25, every_seconds=600),
        progress_path=tmp_path / "progress.json",
        dart_mode="live",
    )

    assert collector.inter_ticker_delay_seconds == 4.0


def test_validate_kr_checkpoint_degrades_missing_filing_text() -> None:
    ticker = "012450"
    checks = asyncio.run(
        validate_kr_checkpoint(
            state=KrCollectionState(
                prices={
                    ticker: [
                        PriceBar(
                            date=date(2026, 5, 4),
                            open=1,
                            high=1,
                            low=1,
                            close=1,
                            volume=1,
                        )
                    ]
                },
                fundamentals={
                    ticker: FundamentalData(
                        ticker=ticker,
                        quarters=[
                            QuarterlyFinancials(
                                fiscal_quarter="2025Q4",
                                report_date=date(2026, 3, 10),
                            )
                        ],
                    )
                },
                filings={ticker: FilingData(ticker=ticker)},
            ),
            as_of=date(2026, 5, 4),
        )
    )

    filing_checks = [check for check in checks if check.name == f"kr:filing:{ticker}"]
    assert len(filing_checks) == 1
    assert filing_checks[0].status == "degraded"
    assert filing_checks[0].detail == "missing_business_overview"


def test_validate_kr_checkpoint_degrades_missing_quarters_with_market_fields() -> None:
    ticker = "010120"
    checks = asyncio.run(
        validate_kr_checkpoint(
            state=KrCollectionState(
                prices={
                    ticker: [
                        PriceBar(
                            date=date(2026, 5, 4),
                            open=1,
                            high=1,
                            low=1,
                            close=1,
                            volume=1,
                        )
                    ]
                },
                fundamentals={
                    ticker: FundamentalData(
                        ticker=ticker,
                        market_cap=1,
                        last_close_price=1,
                    )
                },
                filings={ticker: FilingData(ticker=ticker, business_overview="ok")},
            ),
            as_of=date(2026, 5, 4),
        )
    )

    fundamental_checks = [
        check for check in checks if check.name == f"kr:fundamentals:{ticker}"
    ]
    assert len(fundamental_checks) == 1
    assert fundamental_checks[0].status == "degraded"
    assert fundamental_checks[0].detail == "missing_quarters"


def test_dart_transient_error_detection() -> None:
    assert _is_dart_transient_error(RuntimeError("ConnectTimeoutError: timed out"))
    assert _is_dart_transient_error(RuntimeError("Max retries exceeded with url"))
    assert not _is_dart_transient_error(RuntimeError("DART fundamentals returned empty"))


def test_validate_kr_final_snapshot_rejects_news_outside_target_window() -> None:
    ticker = "005930"
    snapshot = _snapshot_with_news(
        [
            NewsItem(date=date(2026, 2, 28), source="Naver", headline="창밖 기사"),
        ]
    )

    checks = validate_kr_final_snapshot(
        snapshot=snapshot,
        news_audit={
            ticker: [
                NaverArchiveNewsRecord(
                    date=date(2026, 2, 28),
                    published_at=datetime(2026, 2, 28, 9, 0, tzinfo=timezone(timedelta(hours=9))),
                    headline="창밖 기사",
                    url="https://finance.naver.com/item/news_read.naver?article_id=1",
                )
            ]
        },
        news_coverage={
            ticker: KrNewsWindowCoverage(
                ticker=ticker,
                window_start=date(2026, 3, 2),
                window_end=date(2026, 3, 31),
                raw_count=1,
                captured_days=30,
                missing_capture_days=[],
                page_cap_hit_days=[],
                status="ok",
            )
        },
        as_of=date(2026, 3, 31),
    )

    assert any(
        check.name == f"kr:final_news:{ticker}" and check.detail == "date_out_of_window"
        for check in checks
    )


def test_validate_kr_final_snapshot_marks_missing_capture_days_degraded() -> None:
    ticker = "005930"
    snapshot = _snapshot_with_news(
        [
            NewsItem(date=date(2026, 3, 31), source="Naver", headline="당일 기사"),
        ]
    )

    checks = validate_kr_final_snapshot(
        snapshot=snapshot,
        news_audit={
            ticker: [
                NaverArchiveNewsRecord(
                    date=date(2026, 3, 31),
                    published_at=datetime(2026, 3, 31, 9, 0, tzinfo=timezone(timedelta(hours=9))),
                    headline="당일 기사",
                    url="https://finance.naver.com/item/news_read.naver?article_id=1",
                )
            ]
        },
        news_coverage={
            ticker: KrNewsWindowCoverage(
                ticker=ticker,
                window_start=date(2026, 3, 2),
                window_end=date(2026, 3, 31),
                raw_count=1,
                captured_days=29,
                missing_capture_days=["2026-03-02"],
                page_cap_hit_days=[],
                status="degraded",
            )
        },
        as_of=date(2026, 3, 31),
    )

    assert any(
        check.name == f"kr:final_news_coverage:{ticker}" and check.status == "degraded"
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
    monkeypatch.setattr("eit_market_data.local_collection.universe.PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        "eit_market_data.local_collection.universe._listing_metadata_frame",
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

    monkeypatch.setattr("eit_market_data.local_collection.universe.fetch_market_cap_frame", fake_cap_frame)

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


def test_build_local_universe_manifest_uses_snapshot_market_cap_fallback(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr("eit_market_data.local_collection.universe.PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        "eit_market_data.local_collection.universe._listing_metadata_frame",
        lambda: pd.DataFrame(
            [
                {"ticker": "005930", "name": "삼성전자", "market": "KOSPI", "sector": "IT"},
                {"ticker": "247540", "name": "에코프로비엠", "market": "KOSDAQ", "sector": "Materials"},
            ]
        ),
    )

    def fake_cap_frame(as_of, market):  # noqa: ANN001, ANN202
        _ = as_of
        if market == "KOSPI":
            return pd.DataFrame(
                {
                    "종목코드": ["005930"],
                    "시가총액": [500.0],
                }
            )
        return pd.DataFrame(columns=["종목코드", "시가총액"])

    snapshot_dir = tmp_path / "artifacts/kr/snapshots/2025-12"
    snapshot_dir.mkdir(parents=True)
    (snapshot_dir / "snapshot.json").write_text(
        json.dumps(
            {
                "fundamentals": {
                    "247540": {
                        "market_cap": 900.0,
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr("eit_market_data.local_collection.universe.fetch_market_cap_frame", fake_cap_frame)

    output_path = tmp_path / "top200.csv"
    build_local_universe_manifest(
        as_of=date(2025, 12, 31),
        kind="top200",
        output_path=output_path,
    )

    frame = pd.read_csv(output_path, dtype={"ticker": str})
    assert frame["ticker"].tolist() == ["247540", "005930"]
    assert frame["market_cap"].tolist() == [900.0, 500.0]


def test_build_local_universe_manifest_kospi200_uses_official_order(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        "eit_market_data.local_collection.universe._listing_metadata_frame",
        lambda: pd.DataFrame(
            [
                {"ticker": "005930", "name": "삼성전자", "market": "KOSPI", "sector": "IT"},
                {"ticker": "0126Z0", "name": "삼성에피스홀딩스", "market": "KOSPI", "sector": "Healthcare"},
            ]
        ),
    )
    tickers = ["005930", "0126Z0"] + [f"{idx:06d}" for idx in range(1, 199)]
    monkeypatch.setattr(
        "eit_market_data.local_collection.universe._fetch_kospi200_tickers_from_pykrx",
        lambda as_of: tickers,
    )

    def fake_cap_frame(as_of, market):  # noqa: ANN001, ANN202
        _ = as_of
        assert market == "KOSPI"
        return pd.DataFrame(
            {
                "종목코드": ["005930", "0126Z0"],
                "종목명": ["삼성전자", "삼성에피스홀딩스"],
                "시가총액": [500.0, 300.0],
            }
        )

    monkeypatch.setattr("eit_market_data.local_collection.universe.fetch_market_cap_frame", fake_cap_frame)

    output_path = tmp_path / "kospi200.csv"
    build_local_universe_manifest(
        as_of=date(2025, 12, 31),
        kind="kospi200",
        output_path=output_path,
    )

    frame = pd.read_csv(output_path, dtype={"ticker": str})
    assert len(frame) == 200
    assert frame["ticker"].head(2).tolist() == ["005930", "0126Z0"]
    assert frame["rank"].head(2).tolist() == [1, 2]
    assert frame["source"].unique().tolist() == ["krx_pykrx"]
    assert frame.loc[1, "market_cap"] == 300.0


def test_build_local_universe_manifest_kospi200_falls_back_to_naver_current(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        "eit_market_data.local_collection.universe._listing_metadata_frame",
        lambda: pd.DataFrame(columns=["ticker", "name", "market", "sector"]),
    )
    monkeypatch.setattr(
        "eit_market_data.local_collection.universe._fetch_kospi200_tickers_from_pykrx",
        lambda as_of: [],
    )
    rows = [{"ticker": "0126Z0", "name": "삼성에피스홀딩스"}] + [
        {"ticker": f"{idx:06d}", "name": f"name-{idx}"}
        for idx in range(1, 200)
    ]
    monkeypatch.setattr(
        "eit_market_data.local_collection.universe._fetch_kospi200_rows_from_naver_current",
        lambda: rows,
    )
    monkeypatch.setattr(
        "eit_market_data.local_collection.universe.fetch_market_cap_frame",
        lambda as_of, market: pd.DataFrame(columns=["종목코드", "시가총액"]),
    )

    output_path = tmp_path / "kospi200.csv"
    # The Naver *current* membership fallback is only PIT-safe for the current
    # month (it is stamped with date.today()); a historical as_of must never use
    # it. Exercise the fallback with a current-month as_of.
    build_local_universe_manifest(
        as_of=date.today(),
        kind="kospi200",
        output_path=output_path,
    )

    frame = pd.read_csv(output_path, dtype={"ticker": str})
    assert len(frame) == 200
    assert frame.loc[0, "ticker"] == "0126Z0"
    assert frame.loc[0, "name"] == "삼성에피스홀딩스"
    assert frame["source"].unique().tolist() == ["naver_current_fallback"]


def test_build_local_universe_manifest_kospi200_historical_no_prev_refuses_naver(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        "eit_market_data.local_collection.universe._listing_metadata_frame",
        lambda: pd.DataFrame(columns=["ticker", "name", "market", "sector"]),
    )
    monkeypatch.setattr(
        "eit_market_data.local_collection.universe._fetch_kospi200_tickers_from_pykrx",
        lambda as_of: [],
    )
    monkeypatch.setattr(
        "eit_market_data.local_collection.universe._fetch_kospi200_rows_from_naver_current",
        lambda: [{"ticker": f"{idx:06d}", "name": f"name-{idx}"} for idx in range(1, 201)],
    )

    # Historical month with an empty pykrx list and no previous membership: refuse
    # the today()-stamped Naver fallback rather than backfill past with present.
    with pytest.raises(RuntimeError, match="look-ahead"):
        build_local_universe_manifest(
            as_of=date(2024, 9, 30),
            kind="kospi200",
            output_path=tmp_path / "kospi200.csv",
        )


def test_build_local_universe_manifest_kospi200_offcycle_churn_carries_forward(
    monkeypatch,
    tmp_path: Path,
) -> None:
    base = [f"{idx:06d}" for idx in range(1, 201)]
    monkeypatch.setattr(
        "eit_market_data.local_collection.universe._listing_metadata_frame",
        lambda: pd.DataFrame(columns=["ticker", "name", "market", "sector"]),
    )
    monkeypatch.setattr(
        "eit_market_data.local_collection.universe.fetch_market_cap_frame",
        lambda as_of, market: pd.DataFrame(columns=["종목코드", "시가총액"]),
    )
    # Off-cycle (September) pykrx list swaps 19 names vs the prior month.
    suspect = base[:181] + [f"9{idx:05d}" for idx in range(19)]
    monkeypatch.setattr(
        "eit_market_data.local_collection.universe._fetch_kospi200_tickers_from_pykrx",
        lambda as_of: suspect,
    )
    previous_members = [{"ticker": t, "name": f"name-{t}"} for t in base]

    output_path = tmp_path / "kospi200.csv"
    build_local_universe_manifest(
        as_of=date(2024, 9, 30),
        kind="kospi200",
        output_path=output_path,
        previous_members=previous_members,
    )

    frame = pd.read_csv(output_path, dtype={"ticker": str})
    assert set(frame["ticker"]) == set(base)
    assert frame["source"].unique().tolist() == ["carry_forward_offcycle"]


def _cache_only_provider(data: dict[str, object]) -> local_collection.CacheOnlyDartProvider:
    provider = local_collection.CacheOnlyDartProvider.__new__(
        local_collection.CacheOnlyDartProvider
    )
    provider._cache = FakeCache(data)
    return provider


def test_cache_only_filing_drops_future_dated_record():
    # Cache holds ONLY the FY2025 annual report physically filed 2026-03-18,
    # bucketed under collection months 202603..202606 (no January bucket).
    future_filing = FilingData(
        ticker="000080",
        filing_date=date(2026, 3, 18),
        filing_type="사업보고서",
        business_overview="FY2025 annual report body",
    )
    data = {f"filing:000080:2026{mm:02d}": future_filing for mm in (3, 4, 5, 6)}
    provider = _cache_only_provider(data)

    # As of 2026-01-30 the March filing is in the FUTURE -> must NOT be returned.
    jan = asyncio.run(provider.fetch_filing("000080", date(2026, 1, 30)))
    assert jan.filing_date is None
    assert jan.business_overview is None
    assert jan.ticker == "000080"

    # As of a date on/after the real filing_date it becomes valid again.
    apr = asyncio.run(provider.fetch_filing("000080", date(2026, 4, 30)))
    assert apr.filing_date == date(2026, 3, 18)
    assert apr.business_overview == "FY2025 annual report body"


def test_cache_only_filing_prefers_latest_valid_filing():
    older = FilingData(
        ticker="000080",
        filing_date=date(2025, 3, 20),
        filing_type="사업보고서",
        business_overview="FY2024 annual report",
    )
    newer = FilingData(
        ticker="000080",
        filing_date=date(2026, 3, 18),
        filing_type="사업보고서",
        business_overview="FY2025 annual report",
    )
    data = {
        "filing:000080:202503": older,
        "filing:000080:202603": newer,
    }
    provider = _cache_only_provider(data)

    # 2026-02: only the FY2024 report (2025-03-20) qualifies.
    feb = asyncio.run(provider.fetch_filing("000080", date(2026, 2, 27)))
    assert feb.filing_date == date(2025, 3, 20)

    # 2026-04: the newer FY2025 report (2026-03-18) is now valid and preferred.
    apr = asyncio.run(provider.fetch_filing("000080", date(2026, 4, 30)))
    assert apr.filing_date == date(2026, 3, 18)


def test_cache_only_fundamentals_drops_future_quarters():
    quarters = [
        QuarterlyFinancials(fiscal_quarter="2025Q4", report_date=date(2026, 5, 15), revenue=10.0),
        QuarterlyFinancials(fiscal_quarter="2025Q3", report_date=date(2025, 11, 14), revenue=9.0),
        QuarterlyFinancials(fiscal_quarter="2025Q2", report_date=date(2025, 8, 13), revenue=8.0),
    ]
    fund = FundamentalData(ticker="000080", quarters=quarters, market_cap=123.0)
    provider = _cache_only_provider({"fundamental:000080:202601": fund})

    out = asyncio.run(provider.fetch_fundamentals("000080", date(2026, 1, 30)))
    report_dates = [q.report_date for q in out.quarters]
    assert date(2026, 5, 15) not in report_dates
    assert report_dates == [date(2025, 11, 14), date(2025, 8, 13)]
    assert out.market_cap == 123.0  # non-date fields preserved
