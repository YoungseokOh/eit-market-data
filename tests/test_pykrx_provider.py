from __future__ import annotations

import asyncio
from datetime import date
from pathlib import Path

import pandas as pd

from eit_market_data.kr.krx_auth import KrxAuthRequired
from eit_market_data.kr.pykrx_provider import PykrxProvider


def test_fetch_benchmark_rejects_fallback_frame_in_official_mode(monkeypatch) -> None:
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
        lambda index_code, start, end, logger_=None, official_only=True: (
            frame if not official_only else None,
            "yahoo:test" if not official_only else "",
        ),
    )

    bars = provider._fetch_benchmark_sync(date(2024, 1, 4), lookback_days=10)

    assert bars == []


def test_fetch_benchmark_tr_uses_explicit_index_override(monkeypatch) -> None:
    provider = PykrxProvider(benchmark_index="5042", benchmark_tr_index="9999")
    frame = pd.DataFrame(
        {
            "Open": [2500.0],
            "High": [2550.0],
            "Low": [2490.0],
            "Close": [2540.0],
            "Volume": [100.0],
        },
        index=pd.to_datetime(["2024-01-04"]),
    )
    seen: dict[str, str] = {}

    def fake_index(index_code, start, end, logger_=None, official_only=True):  # noqa: ANN001, ANN202
        seen["index"] = index_code
        return frame, "pykrx"

    monkeypatch.setattr(
        "eit_market_data.kr.pykrx_provider.fetch_index_ohlcv_frame", fake_index
    )
    # Explicit override must be used verbatim; no name resolution attempted.
    def _boom(*a, **k):  # noqa: ANN002, ANN003, ANN202
        raise AssertionError("resolve_tr_index_code should not be called with override")

    monkeypatch.setattr("eit_market_data.kr.pykrx_provider.resolve_tr_index_code", _boom)

    bars = provider._fetch_benchmark_tr_sync(date(2024, 1, 4), lookback_days=10)
    assert seen["index"] == "9999"
    assert [bar.close for bar in bars] == [2540.0]


def test_fetch_benchmark_tr_returns_empty_when_code_unresolved(monkeypatch) -> None:
    provider = PykrxProvider(benchmark_index="1001")
    monkeypatch.setattr(
        "eit_market_data.kr.pykrx_provider.resolve_tr_index_code",
        lambda as_of, base_index, logger_=None: None,
    )
    bars = provider._fetch_benchmark_tr_sync(date(2024, 1, 4), lookback_days=10)
    assert bars == []


def test_resolve_tr_index_code_matches_by_name(monkeypatch) -> None:
    import eit_market_data.kr.market_helpers as mh

    mh._tr_index_code_cache.clear()

    class FakeStock:
        def get_index_ticker_list(self, as_of_str, market=None):  # noqa: ANN001, ANN202
            return {"KOSPI": ["1028", "1035"], "KRX": [], "KOSDAQ": []}.get(market, [])

        def get_index_ticker_name(self, code):  # noqa: ANN001, ANN202
            return {"1028": "코스피 200", "1035": "코스피 200 TR"}[code]

    monkeypatch.setattr(
        "eit_market_data.kr.pykrx_loader.load_pykrx_stock", lambda: FakeStock()
    )
    monkeypatch.setattr(mh, "install_pykrx_krx_session_hooks", lambda: None)
    monkeypatch.setattr(mh, "ensure_krx_authenticated_session", lambda interactive=False: None)

    code = mh.resolve_tr_index_code(date(2024, 6, 28), "1028")
    assert code == "1035"


def test_fetch_benchmark_raises_auth_error_in_official_mode(monkeypatch) -> None:
    provider = PykrxProvider()

    async def fake_run_limited(fn, *args, **kwargs):  # noqa: ANN001, ANN202
        raise KrxAuthRequired("KRX login required")

    monkeypatch.setattr(provider, "_run_limited", fake_run_limited)

    try:
        asyncio.run(provider.fetch_benchmark(date(2026, 3, 6), lookback_days=20))
    except KrxAuthRequired as exc:
        assert "KRX login required" in str(exc)
    else:
        raise AssertionError("expected KrxAuthRequired")


def test_fetch_sector_map_uses_cached_snapshot_before_live(monkeypatch) -> None:
    provider = PykrxProvider()
    live_calls: list[str] = []

    async def fake_run_limited(fn, *args, **kwargs):  # noqa: ANN001
        live_calls.append(args[0])
        return {}

    monkeypatch.setattr(
        "eit_market_data.kr.pykrx_provider.load_sector_snapshot_map",
        lambda market, as_of, logger_=None, official_only=True: (
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
        lambda market, as_of, logger_=None, official_only=True: ({}, None),
    )
    monkeypatch.setattr(provider, "_run_limited", fake_run_limited)

    result = asyncio.run(provider.fetch_sector_map(["000660"], as_of=date(2026, 3, 6)))

    assert result == {"000660": "반도체 제조업"}


def test_fetch_sector_map_skips_non_authoritative_snapshot_in_official_mode(
    monkeypatch,
) -> None:
    provider = PykrxProvider()

    async def fake_run_limited(fn, *args, **kwargs):  # noqa: ANN001
        return {"005930": "반도체 제조업"} if args[0] == "KOSPI" else {}

    monkeypatch.setattr(
        "eit_market_data.kr.pykrx_provider.load_sector_snapshot_map",
        lambda market, as_of, logger_=None, official_only=True: ({}, None),
    )
    monkeypatch.setattr(provider, "_run_limited", fake_run_limited)

    result = asyncio.run(provider.fetch_sector_map(["005930"], as_of=date(2026, 3, 6)))

    assert result == {"005930": "반도체 제조업"}
