from __future__ import annotations

import asyncio
import importlib.util
import sys
from datetime import date
from pathlib import Path

import pandas as pd


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
