from __future__ import annotations

import asyncio
from datetime import date
from pathlib import Path

import pandas as pd

from eit_market_data.kr.pykrx_provider import PykrxProvider


def test_fetch_benchmark_uses_fallback_frame(monkeypatch) -> None:
    provider = PykrxProvider()
    frame = pd.DataFrame(
        {
            "Open": [2500.0, 2520.0],
            "High": [2550.0, 2560.0],
            "Low": [2490.0, 2510.0],
            "Close": [2540.0, 2555.0],
            "Volume": [100.0, 120.0],
        },
        index=pd.to_datetime(["2024-01-03", "2024-01-04"]),
    )

    monkeypatch.setattr(
        "eit_market_data.kr.pykrx_provider.fetch_index_ohlcv_frame",
        lambda index_code, start, end, logger_=None: (frame, "yahoo:test"),
    )

    bars = provider._fetch_benchmark_sync(date(2024, 1, 4), lookback_days=10)

    assert len(bars) == 2
    assert bars[-1].close == 2555.0
    assert bars[-1].date == date(2024, 1, 4)


def test_fetch_sector_map_uses_cached_snapshot_before_live(monkeypatch) -> None:
    provider = PykrxProvider()
    live_calls: list[str] = []

    async def fake_run_limited(fn, *args, **kwargs):  # noqa: ANN001
        live_calls.append(args[0])
        return {}

    monkeypatch.setattr(
        "eit_market_data.kr.pykrx_provider.load_sector_snapshot_map",
        lambda market, as_of, logger_=None: (
            {"005930": "반도체 제조업"} if market == "KOSPI" else {},
            Path("/tmp/KOSPI_20241231.parquet") if market == "KOSPI" else None,
        ),
    )
    monkeypatch.setattr(provider, "_run_limited", fake_run_limited)

    result = asyncio.run(provider.fetch_sector_map(["005930"], as_of=date(2026, 3, 6)))

    assert result == {"005930": "반도체 제조업"}
    assert live_calls == []


def test_fetch_sector_map_falls_back_to_live_when_snapshot_missing(monkeypatch) -> None:
    provider = PykrxProvider()

    async def fake_run_limited(fn, *args, **kwargs):  # noqa: ANN001
        return {"000660": "반도체 제조업"} if args[0] == "KOSPI" else {}

    monkeypatch.setattr(
        "eit_market_data.kr.pykrx_provider.load_sector_snapshot_map",
        lambda market, as_of, logger_=None: ({}, None),
    )
    monkeypatch.setattr(provider, "_run_limited", fake_run_limited)

    result = asyncio.run(provider.fetch_sector_map(["000660"], as_of=date(2026, 3, 6)))

    assert result == {"000660": "반도체 제조업"}
