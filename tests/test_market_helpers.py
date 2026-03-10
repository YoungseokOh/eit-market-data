from __future__ import annotations

import sys
import types
from datetime import date

import pandas as pd

from eit_market_data.kr.krx_auth import KrxAuthRequired
from eit_market_data.kr.market_helpers import (
    fetch_index_ohlcv_frame,
    fetch_market_cap_frame,
    load_sector_snapshot_map,
)


def test_load_sector_snapshot_map_uses_index_as_ticker_column(tmp_path) -> None:
    frame = pd.DataFrame(
        {
            "종목명": ["삼성전자"],
            "업종명": ["반도체 제조업"],
        },
        index=pd.Index(["005930"], name="종목코드"),
    )
    path = tmp_path / "KOSPI_20241231.parquet"
    frame.to_parquet(path)

    sector_map, snapshot_path = load_sector_snapshot_map(
        "KOSPI",
        date(2026, 3, 6),
        snapshot_dir=tmp_path,
        official_only=True,
    )

    assert snapshot_path == path
    assert sector_map == {"005930": "반도체 제조업"}


def test_load_sector_snapshot_map_skips_non_authoritative_snapshot_in_official_mode(
    tmp_path,
) -> None:
    frame = pd.DataFrame(
        {
            "종목코드": ["005930"],
            "업종명": ["반도체 제조업"],
            "Industry": ["Semiconductor"],
            "ListingDate": ["1975-06-11"],
        }
    )
    frame.to_parquet(tmp_path / "KOSPI_20241231.parquet", index=False)

    sector_map, snapshot_path = load_sector_snapshot_map(
        "KOSPI",
        date(2026, 3, 6),
        snapshot_dir=tmp_path,
        official_only=True,
    )

    assert sector_map == {}
    assert snapshot_path is None


def test_fetch_index_ohlcv_frame_uses_name_display_false(monkeypatch) -> None:
    calls: dict[str, object] = {}

    class DummyStock:
        @staticmethod
        def get_index_ohlcv_by_date(*args, **kwargs):  # noqa: ANN001, ANN205
            calls["args"] = args
            calls["kwargs"] = kwargs
            return pd.DataFrame(
                {
                    "시가": [1.0],
                    "고가": [2.0],
                    "저가": [0.5],
                    "종가": [1.5],
                    "거래량": [10],
                },
                index=pd.to_datetime(["2026-03-06"]),
            )

    monkeypatch.setitem(sys.modules, "pykrx", types.SimpleNamespace(stock=DummyStock()))
    monkeypatch.setattr(
        "eit_market_data.kr.market_helpers.install_pykrx_krx_session_hooks",
        lambda: None,
    )
    monkeypatch.setattr(
        "eit_market_data.kr.market_helpers.ensure_krx_authenticated_session",
        lambda interactive=False: None,
    )

    frame, source = fetch_index_ohlcv_frame("1001", date(2026, 3, 1), date(2026, 3, 6))

    assert source == "pykrx"
    assert frame is not None and not frame.empty
    assert calls["kwargs"] == {"name_display": False}


def test_fetch_market_cap_frame_raises_on_unexpected_columns(monkeypatch) -> None:
    class DummyStock:
        @staticmethod
        def get_market_cap(*args, **kwargs):  # noqa: ANN001, ANN205
            return pd.DataFrame({"foo": [1]})

    monkeypatch.setitem(sys.modules, "pykrx", types.SimpleNamespace(stock=DummyStock()))
    monkeypatch.setattr(
        "eit_market_data.kr.market_helpers.install_pykrx_krx_session_hooks",
        lambda: None,
    )
    monkeypatch.setattr(
        "eit_market_data.kr.market_helpers.ensure_krx_authenticated_session",
        lambda interactive=False: None,
    )

    try:
        fetch_market_cap_frame(date(2026, 3, 6), "KOSPI")
    except KrxAuthRequired as exc:
        assert "unexpected columns" in str(exc)
    else:
        raise AssertionError("expected KrxAuthRequired")
