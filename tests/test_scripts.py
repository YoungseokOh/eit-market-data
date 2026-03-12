from __future__ import annotations

import asyncio
import importlib.util
import sys
from datetime import date
from pathlib import Path

import pandas as pd
from eit_market_data.schemas.snapshot import NewsItem


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
