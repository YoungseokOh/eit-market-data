from __future__ import annotations

import json
import importlib.util
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

from eit_market_data.schemas.snapshot import FundamentalData, QuarterlyFinancials

_SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "backfill_dart_cache_controlled.py"
_SPEC = importlib.util.spec_from_file_location("backfill_dart_cache_controlled", _SCRIPT_PATH)
assert _SPEC and _SPEC.loader
backfill = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(backfill)


def _args(tmp_path, *, filing_mode: str = "optional") -> SimpleNamespace:
    universe = tmp_path / "universe.csv"
    universe.write_text("ticker\n000810\n", encoding="utf-8")
    return SimpleNamespace(
        universe_csv=universe,
        as_of="2026-04-30",
        progress=tmp_path / "progress.json",
        cache_dir=tmp_path / "dart_cache",
        delay=0.0,
        quarters=1,
        max_tickers=None,
        continue_on_empty=False,
        max_consecutive_empty=8,
        filing_mode=filing_mode,
    )


def test_controlled_backfill_records_empty_filing_without_blocking_fundamentals(
    monkeypatch,
    tmp_path,
) -> None:
    class FakeDartProvider:
        def __init__(self, **kwargs):  # noqa: ANN003
            self.kwargs = kwargs

        async def fetch_fundamentals(self, ticker, as_of, n_quarters=8):  # noqa: ANN001
            assert ticker == "000810"
            assert as_of == date(2026, 4, 30)
            return FundamentalData(
                ticker=ticker,
                quarters=[
                    QuarterlyFinancials(
                        fiscal_quarter="2025Q4",
                        report_date=date(2026, 3, 14),
                    )
                ],
            )

        async def fetch_filing(self, ticker, as_of):  # noqa: ANN001
            _ = (ticker, as_of)
            raise RuntimeError("DART filing returned empty for 000810")

    monkeypatch.setattr(backfill, "DartProvider", FakeDartProvider)

    assert backfill.run(_args(tmp_path, filing_mode="optional")) == 0

    progress = json.loads((tmp_path / "progress.json").read_text(encoding="utf-8"))
    assert progress["completed"] == ["000810"]
    assert progress["fundamental_completed"] == ["000810"]
    assert progress["filing_empty"] == ["000810"]
    assert progress["failed"][0]["field"] == "filing"
    assert progress["stopped"] is None


def test_controlled_backfill_strict_filing_still_stops(monkeypatch, tmp_path) -> None:
    class FakeDartProvider:
        def __init__(self, **kwargs):  # noqa: ANN003
            self.kwargs = kwargs

        async def fetch_fundamentals(self, ticker, as_of, n_quarters=8):  # noqa: ANN001
            _ = (ticker, as_of, n_quarters)
            return FundamentalData(
                ticker=ticker,
                quarters=[
                    QuarterlyFinancials(
                        fiscal_quarter="2025Q4",
                        report_date=date(2026, 3, 14),
                    )
                ],
            )

        async def fetch_filing(self, ticker, as_of):  # noqa: ANN001
            _ = (ticker, as_of)
            raise RuntimeError("DART filing returned empty for 000810")

    monkeypatch.setattr(backfill, "DartProvider", FakeDartProvider)

    assert backfill.run(_args(tmp_path, filing_mode="strict")) == 2

    progress = json.loads((tmp_path / "progress.json").read_text(encoding="utf-8"))
    assert progress["fundamental_completed"] == ["000810"]
    assert progress["stopped"]["field"] == "filing"
    assert progress["completed"] == []


def test_controlled_backfill_still_stops_on_empty_fundamentals(monkeypatch, tmp_path) -> None:
    class FakeDartProvider:
        def __init__(self, **kwargs):  # noqa: ANN003
            self.kwargs = kwargs

        async def fetch_fundamentals(self, ticker, as_of, n_quarters=8):  # noqa: ANN001
            _ = (ticker, as_of, n_quarters)
            raise RuntimeError("DART fundamentals returned empty for 000810")

    monkeypatch.setattr(backfill, "DartProvider", FakeDartProvider)

    assert backfill.run(_args(tmp_path, filing_mode="optional")) == 2

    progress = json.loads((tmp_path / "progress.json").read_text(encoding="utf-8"))
    assert progress["stopped"]["field"] == "fundamental"


def test_controlled_backfill_rejects_unknown_filing_mode(tmp_path) -> None:
    args = _args(tmp_path, filing_mode="wat")

    with pytest.raises(ValueError, match="--filing-mode"):
        backfill.run(args)


def test_load_tickers_preserves_alphanumeric_krx_codes(tmp_path) -> None:
    universe = tmp_path / "universe.csv"
    universe.write_text("ticker\n0126Z0\n5930\n", encoding="utf-8")

    assert backfill._load_tickers(universe) == ["005930", "0126Z0"]
