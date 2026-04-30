from __future__ import annotations

import asyncio
import importlib.util
import json
import logging
import sys
from datetime import date
from pathlib import Path

import pandas as pd
from eit_market_data.schemas.snapshot import (
    FilingData,
    FundamentalData,
    MacroData,
    MonthlySnapshot,
    NewsItem,
    PriceBar,
    SnapshotMetadata,
)


def _load_module(path: Path, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_fetch_pykrx_all_index_data_uses_shared_fallback(monkeypatch, tmp_path: Path) -> None:
    module = _load_module(
        Path("scripts/fetch_pykrx_all.py"),
        "fetch_pykrx_all_test",
    )
    frame = pd.DataFrame(
        {
            "Open": [1.0],
            "High": [2.0],
            "Low": [0.5],
            "Close": [1.5],
            "Volume": [10.0],
        },
        index=pd.to_datetime(["2024-01-31"]),
    )
    saved: list[tuple[str, int]] = []

    monkeypatch.setattr(
        module,
        "fetch_index_ohlcv_frame",
        lambda code, start, end, logger_=None, official_only=True: (frame, "yahoo:test"),
    )
    monkeypatch.setattr(module, "_call", lambda fn, *args, **kwargs: pd.DataFrame())
    monkeypatch.setattr(
        module,
        "_save_parquet",
        lambda df, path: saved.append((path.as_posix(), 0 if df is None else len(df))),
    )

    class DummyStock:
        def get_index_fundamental(self, *args, **kwargs):  # noqa: ANN202
            return pd.DataFrame()

    module.fetch_index_data(DummyStock(), date(2024, 1, 1), date(2024, 1, 31), tmp_path)

    assert any("index/ohlcv/KOSPI_20240131.parquet" in path and rows == 1 for path, rows in saved)
    assert any("index/fundamental/KOSPI_20240131.parquet" in path for path, _rows in saved)


def test_crawl_kr_data_keeps_existing_sector_snapshot_on_failure(monkeypatch, tmp_path: Path, capsys) -> None:
    module = _load_module(
        Path("scripts/crawl_kr_data.py"),
        "crawl_kr_data_test",
    )
    snapshot_path = tmp_path / "market/sector/KOSPI_20241227.parquet"
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_path.write_text("keep-me")

    monkeypatch.setattr(module, "OUTPUT_ROOT", tmp_path)
    monkeypatch.setattr(module, "safe_call", lambda fn, *args, **kwargs: None)

    module.fetch_sector_classification()

    captured = capsys.readouterr()
    assert "keeping existing sector snapshot" in captured.out
    assert snapshot_path.read_text() == "keep-me"


def test_preflight_dart_marks_missing_market_fields_as_degraded(monkeypatch) -> None:
    module = _load_module(
        Path("scripts/preflight_kr_data.py"),
        "preflight_kr_data_test",
    )
    monkeypatch.setenv("DART_API_KEY", "test")

    class DummyProvider:
        async def fetch_fundamentals(self, ticker, as_of, n_quarters=4):  # noqa: ANN001
            from eit_market_data.schemas.snapshot import FundamentalData, QuarterlyFinancials

            return FundamentalData(
                ticker=ticker,
                quarters=[
                    QuarterlyFinancials(
                        fiscal_quarter="2024Q4",
                        report_date=date(2025, 3, 10),
                        revenue=100.0,
                        operating_income=10.0,
                        net_income=8.0,
                        total_assets=200.0,
                        total_equity=120.0,
                    )
                    for _ in range(4)
                ],
            )

    monkeypatch.setattr(module, "CompositeKrFundamentalProvider", lambda: DummyProvider())

    result = asyncio.run(module._check_dart(date(2026, 3, 6), "005930"))

    assert result.status == "degraded"
    assert "market_cap" in result.detail
    assert "last_close_price" in result.detail


def test_preflight_news_ok_when_provider_returns_items(monkeypatch) -> None:
    module = _load_module(
        Path("scripts/preflight_kr_data.py"),
        "preflight_kr_data_news_ok_test",
    )

    class DummyProvider:
        async def fetch_news(self, ticker, as_of, lookback_days=30):  # noqa: ANN001
            _ = (ticker, as_of, lookback_days)
            return [NewsItem(date=date(2026, 3, 12), source="Naver", headline="headline")]

    monkeypatch.setattr(module, "NaverNewsProvider", lambda: DummyProvider())

    result = asyncio.run(module._check_news(date(2026, 3, 12), "005930"))

    assert result.status == "ok"
    assert "items=1" in result.detail


def test_preflight_news_fails_when_provider_empty_but_raw_links_exist(monkeypatch) -> None:
    module = _load_module(
        Path("scripts/preflight_kr_data.py"),
        "preflight_kr_data_news_failed_test",
    )

    class DummyProvider:
        async def fetch_news(self, ticker, as_of, lookback_days=30):  # noqa: ANN001
            _ = (ticker, as_of, lookback_days)
            return []

    monkeypatch.setattr(module, "NaverNewsProvider", lambda: DummyProvider())
    monkeypatch.setattr(module, "_probe_naver_news_links", lambda ticker: 5)

    result = asyncio.run(module._check_news(date(2026, 3, 12), "005930"))

    assert result.status == "failed"
    assert "raw_links=5" in result.detail


def test_preflight_news_degrades_when_raw_page_is_empty(monkeypatch) -> None:
    module = _load_module(
        Path("scripts/preflight_kr_data.py"),
        "preflight_kr_data_news_degraded_test",
    )

    class DummyProvider:
        async def fetch_news(self, ticker, as_of, lookback_days=30):  # noqa: ANN001
            _ = (ticker, as_of, lookback_days)
            return []

    monkeypatch.setattr(module, "NaverNewsProvider", lambda: DummyProvider())
    monkeypatch.setattr(module, "_probe_naver_news_links", lambda ticker: 0)

    result = asyncio.run(module._check_news(date(2026, 3, 12), "005930"))

    assert result.status == "degraded"
    assert "raw_links=0" in result.detail


def test_crawl_kr_data_fallback_extracts_daily_cap(monkeypatch) -> None:
    module = _load_module(
        Path("scripts/crawl_kr_data_fallback.py"),
        "crawl_kr_data_fallback_extract_test",
    )
    monkeypatch.setattr(
        module,
        "_fnguide_get",
        lambda path: {
            "CHART": [
                {"TRD_DT": "2024-01-31", "J_PRC": "70,000", "MKT_CAP": "4200000"},
                {"TRD_DT": "2024-02-01", "J_PRC": "0", "MKT_CAP": "4210000"},
            ]
        },
    )
    meta = module.TickerMeta(ticker="005930", market="KOSPI", name="삼성전자")

    rows = module._extract_daily_cap(
        meta,
        pd.Timestamp("2024-01-01"),
        pd.Timestamp("2024-01-31"),
        {pd.Period("2024-01", freq="M"): pd.Timestamp("2024-01-31")},
    )

    assert len(rows) == 1
    assert rows[0]["종목코드"] == "005930"
    assert rows[0]["종가"] == 70000
    assert rows[0]["시가총액"] == 4200000 * 100_000_000
    assert rows[0]["상장주식수"] == round((4200000 * 100_000_000) / 70000)


def test_crawl_kr_data_fallback_maps_month_label_to_month_end(monkeypatch) -> None:
    module = _load_module(
        Path("scripts/crawl_kr_data_fallback.py"),
        "crawl_kr_data_fallback_month_end_test",
    )
    monkeypatch.setattr(
        module,
        "_fnguide_get",
        lambda path: {
            "CHART": [
                {"TRD_DT": "2024/02/01", "J_PRC": "71,000", "MKT_CAP": "4210000"},
            ]
        },
    )
    meta = module.TickerMeta(ticker="005930", market="KOSPI", name="삼성전자")

    rows = module._extract_daily_cap(
        meta,
        pd.Timestamp("2024-02-01"),
        pd.Timestamp("2024-02-29"),
        {pd.Period("2024-02", freq="M"): pd.Timestamp("2024-02-29")},
    )

    assert len(rows) == 1
    assert rows[0]["source_trade_date"] == pd.Timestamp("2024-02-29")


def test_crawl_kr_data_fallback_saves_daily_cap_grouped_files(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_module(
        Path("scripts/crawl_kr_data_fallback.py"),
        "crawl_kr_data_fallback_save_test",
    )
    rows = [
        {
            "종목코드": "005930",
            "종목명": "삼성전자",
            "시장": "KOSPI",
            "종가": 70000,
            "시가총액": 420000000000000,
            "상장주식수": 5960000000,
            "source_trade_date": pd.Timestamp("2024-01-31"),
        },
        {
            "종목코드": "000660",
            "종목명": "SK하이닉스",
            "시장": "KOSPI",
            "종가": 120000,
            "시가총액": 87360000000000,
            "상장주식수": 728000000,
            "source_trade_date": pd.Timestamp("2024-01-31"),
        },
    ]

    saved: dict[Path, pd.DataFrame] = {}

    def _fake_to_parquet(self, path, index=False):  # noqa: ANN001
        _ = index
        saved[Path(path)] = self.copy()

    monkeypatch.setattr(module.pd.DataFrame, "to_parquet", _fake_to_parquet)

    module._save_market_daily(rows, tmp_path)

    output = tmp_path / "KOSPI_20240131.parquet"
    assert output in saved
    frame = saved[output]
    assert frame["종목코드"].tolist() == ["000660", "005930"]
    assert set(frame.columns) >= {"종목코드", "시가총액", "상장주식수", "source_trade_date"}


def test_crawl_kr_data_fallback_default_uses_full_public_listings() -> None:
    module = _load_module(
        Path("scripts/crawl_kr_data_fallback.py"),
        "crawl_kr_data_fallback_full_listing_test",
    )

    class DummyFdr:
        @staticmethod
        def StockListing(market):  # noqa: N802
            frames = {
                "KOSPI-DESC": pd.DataFrame(
                    {
                        "Code": ["005930"],
                        "Name": ["삼성전자"],
                        "Market": ["KOSPI"],
                    }
                ),
                "KOSDAQ-DESC": pd.DataFrame(
                    {
                        "Code": ["000250"],
                        "Name": ["삼천당제약"],
                        "Market": ["KOSDAQ"],
                    }
                ),
            }
            return frames[market]

    module.fdr = DummyFdr

    tickers = module._load_tickers(None)

    assert [(item.ticker, item.market) for item in tickers] == [
        ("000250", "KOSDAQ"),
        ("005930", "KOSPI"),
    ]


def test_crawl_kr_data_fallback_reports_missing_cap_daily_files(tmp_path: Path) -> None:
    module = _load_module(
        Path("scripts/crawl_kr_data_fallback.py"),
        "crawl_kr_data_fallback_missing_cap_test",
    )
    month_ends = [pd.Timestamp("2022-01-28")]
    (tmp_path / "market/cap_daily").mkdir(parents=True)
    (tmp_path / "market/cap_daily/KOSPI_20220128.parquet").write_text("stub")

    missing = module.missing_cap_daily_files(tmp_path, month_ends)

    assert missing == [tmp_path / "market/cap_daily/KOSDAQ_20220128.parquet"]


def test_build_us_snapshot_supports_external_artifacts_root(monkeypatch, tmp_path: Path) -> None:
    module = _load_module(
        Path("scripts/build_us_snapshot.py"),
        "build_us_snapshot_test",
    )

    class DummyBuilder:
        def __init__(self, **providers):  # noqa: ANN003
            self.providers = providers

        async def build(self, month, universe, config):  # noqa: ANN001, ANN202
            _ = (month, config)
            ticker = universe[0]
            return MonthlySnapshot(
                decision_date=date(2026, 3, 31),
                execution_date=date(2026, 4, 1),
                universe=universe,
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
                news={ticker: []},
                macro=MacroData(rates_policy={"base_rate": 2.75}),
                benchmark_prices=[],
                input_hash="hash",
                metadata=SnapshotMetadata(created_at="2026-03-31T00:00:00"),
            )

    monkeypatch.setattr(module, "create_real_providers", lambda: {})
    monkeypatch.setattr(module, "SnapshotBuilder", DummyBuilder)

    result = asyncio.run(
        module.build_us_snapshot(
            as_of=date(2026, 3, 12),
            universe=["AAPL"],
            artifacts_root=tmp_path,
        )
    )

    assert result["status"] == "ok"
    summary = result["summary"]
    assert str(tmp_path) in summary["files"]["snapshot"]
    assert (tmp_path / "snapshots" / "2026-03" / "summary.json").exists()


def test_backfill_filter_drops_known_pykrx_malformed_record() -> None:
    module = _load_module(
        Path("scripts/backfill_all.py"),
        "backfill_all_filter_test",
    )
    malformed = logging.LogRecord(
        name="root",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg=("KOSPI",),
        args=({},),
        exc_info=None,
    )
    valid = logging.LogRecord(
        name="backfill",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="hello %s",
        args=("world",),
        exc_info=None,
    )

    log_filter = module._DropMalformedFilter()

    assert log_filter.filter(malformed) is False
    assert log_filter.filter(valid) is True


def test_backfill_capture_output_lines_collects_stdout_and_stderr() -> None:
    module = _load_module(
        Path("scripts/backfill_all.py"),
        "backfill_all_capture_test",
    )

    def _noisy():  # noqa: ANN202
        print('"000104" invalid symbol or has no data')
        print("temporary stderr noise", file=sys.stderr)
        return "ok"

    result, lines = module._capture_output_lines(_noisy)

    assert result == "ok"
    assert '"000104" invalid symbol or has no data' in lines
    assert "temporary stderr noise" in lines


def test_backfill_phase1_summary_aggregates_failures(monkeypatch) -> None:
    module = _load_module(
        Path("scripts/backfill_all.py"),
        "backfill_all_summary_test",
    )
    summary = module._Phase1Summary()
    collector = module._Phase1MarketCollector("KOSPI", summary)
    lines: list[str] = []

    collector.warning(
        "Ticker %s fetch failed in %s: %s",
        "000104",
        "fdr",
        '"000104" invalid symbol or has no data',
    )
    summary.record_ohlcv_message("KOSPI", '"000104" invalid symbol or has no data')
    summary.record_ohlcv_message("KOSPI", "Ticker 000660 fetch failed in naver: timeout")
    summary.record_pykrx_failure(
        "get_market_cap",
        RuntimeError("KRX POST https://data.krx.co.kr: status=400 LOGOUT"),
        "2022-02 KOSPI",
    )

    monkeypatch.setattr(module.tqdm, "write", lines.append)

    summary.emit_ohlcv_summary("KOSPI")
    summary.emit_pykrx_summary()

    assert any("OHLCV KOSPI failures:" in line for line in lines)
    assert any("invalid symbol or no data=1 tickers (e.g. 000104)" in line for line in lines)
    assert any("timeout=1 tickers (e.g. 000660)" in line for line in lines)
    assert any("get_market_cap: status=400 LOGOUT x1 (2022-02 KOSPI)" in line for line in lines)


def test_backfill_pykrx_call_uses_error_reporter(monkeypatch) -> None:
    module = _load_module(
        Path("scripts/backfill_all.py"),
        "backfill_all_pykrx_call_test",
    )
    events: list[tuple[str, str, str | None]] = []

    def _boom():  # noqa: ANN202
        raise RuntimeError("status=400 LOGOUT")

    monkeypatch.setattr(module.time, "sleep", lambda *_args, **_kwargs: None)

    result = module._pykrx_call(
        _boom,
        on_error=lambda fn_name, exc, context: events.append((fn_name, str(exc), context)),
        error_context="2022-02 KOSPI",
    )

    assert result is None
    assert events == [("_boom", "status=400 LOGOUT", "2022-02 KOSPI")]


# ---------------------------------------------------------------------------
# BackfillDartProvider tests
# ---------------------------------------------------------------------------


def _make_backfill_json(ticker: str, quarters: list[dict], filing: dict | None = None) -> dict:
    """Build a Phase 2 backfill JSON payload for testing."""
    return {
        "ticker": ticker,
        "as_of": "2026-03-18",
        "fundamentals": {"ticker": ticker, "quarters": quarters},
        "filing": filing or {"ticker": ticker},
    }


def test_backfill_dart_provider_returns_fundamentals(tmp_path: Path) -> None:
    module = _load_module(
        Path("scripts/backfill_all.py"),
        "backfill_all_dart_test",
    )
    quarters = [
        {"fiscal_quarter": "2025Q4", "report_date": "2026-03-10", "revenue": 100.0},
        {"fiscal_quarter": "2025Q3", "report_date": "2025-11-14", "revenue": 80.0},
    ]
    backfill_dir = tmp_path / "dart"
    backfill_dir.mkdir()
    (backfill_dir / "005930.json").write_text(
        json.dumps(_make_backfill_json("005930", quarters)), encoding="utf-8",
    )

    provider = module.BackfillDartProvider(backfill_dir)
    result = asyncio.run(provider.fetch_fundamentals("005930", date(2026, 3, 18)))

    assert result.ticker == "005930"
    assert len(result.quarters) == 2
    assert result.quarters[0].fiscal_quarter == "2025Q4"


def test_backfill_dart_provider_filters_look_ahead(tmp_path: Path) -> None:
    module = _load_module(
        Path("scripts/backfill_all.py"),
        "backfill_all_dart_pit_test",
    )
    quarters = [
        {"fiscal_quarter": "2025Q4", "report_date": "2026-03-10", "revenue": 100.0},
        {"fiscal_quarter": "2025Q3", "report_date": "2025-11-14", "revenue": 80.0},
        {"fiscal_quarter": "2025Q2", "report_date": "2025-08-14", "revenue": 70.0},
    ]
    backfill_dir = tmp_path / "dart"
    backfill_dir.mkdir()
    (backfill_dir / "005930.json").write_text(
        json.dumps(_make_backfill_json("005930", quarters)), encoding="utf-8",
    )

    provider = module.BackfillDartProvider(backfill_dir)
    # as_of = 2025-12-31 → only Q3 and Q2 should be included (report_date <= as_of)
    result = asyncio.run(provider.fetch_fundamentals("005930", date(2025, 12, 31)))

    assert len(result.quarters) == 2
    assert result.quarters[0].fiscal_quarter == "2025Q3"
    assert result.quarters[1].fiscal_quarter == "2025Q2"


def test_backfill_dart_provider_missing_ticker_returns_empty(tmp_path: Path) -> None:
    module = _load_module(
        Path("scripts/backfill_all.py"),
        "backfill_all_dart_empty_test",
    )
    backfill_dir = tmp_path / "dart"
    backfill_dir.mkdir()

    provider = module.BackfillDartProvider(backfill_dir)
    fund = asyncio.run(provider.fetch_fundamentals("999999", date(2026, 3, 18)))
    filing = asyncio.run(provider.fetch_filing("999999", date(2026, 3, 18)))

    assert fund.ticker == "999999"
    assert fund.quarters == []
    assert filing.ticker == "999999"


def test_backfill_dart_provider_filing_point_in_time(tmp_path: Path) -> None:
    module = _load_module(
        Path("scripts/backfill_all.py"),
        "backfill_all_dart_filing_pit_test",
    )
    filing = {
        "ticker": "005930",
        "filing_date": "2026-03-10",
        "business_overview": "Samsung overview",
    }
    backfill_dir = tmp_path / "dart"
    backfill_dir.mkdir()
    (backfill_dir / "005930.json").write_text(
        json.dumps(_make_backfill_json("005930", [], filing)), encoding="utf-8",
    )

    provider = module.BackfillDartProvider(backfill_dir)
    # as_of before filing_date → should return empty
    result = asyncio.run(provider.fetch_filing("005930", date(2025, 12, 31)))
    assert result.business_overview is None

    # as_of after filing_date → should return full filing
    result = asyncio.run(provider.fetch_filing("005930", date(2026, 3, 18)))
    assert result.business_overview == "Samsung overview"
