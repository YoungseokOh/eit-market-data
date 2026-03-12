from __future__ import annotations

from datetime import date

import pandas as pd

from eit_market_data.kr.market_helpers import (
    fetch_index_ohlcv_frame,
    fetch_live_sector_classification_map,
    fetch_market_cap_frame,
    load_sector_snapshot_map,
)


def test_load_sector_snapshot_map_uses_index_as_ticker_column(tmp_path, monkeypatch) -> None:
    frame = pd.DataFrame(
        {
            "종목명": ["삼성전자"],
            "업종명": ["반도체 제조업"],
        },
        index=pd.Index(["005930"], name="종목코드"),
    )
    path = tmp_path / "KOSPI_20241231.parquet"
    path.write_text("stub")
    monkeypatch.setattr("pandas.read_parquet", lambda target: frame if target == path else None)

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
    monkeypatch,
) -> None:
    frame = pd.DataFrame(
        {
            "종목코드": ["005930"],
            "업종명": ["반도체 제조업"],
            "Industry": ["Semiconductor"],
            "ListingDate": ["1975-06-11"],
        }
    )
    path = tmp_path / "KOSPI_20241231.parquet"
    path.write_text("stub")
    monkeypatch.setattr("pandas.read_parquet", lambda target: frame if target == path else None)

    sector_map, snapshot_path = load_sector_snapshot_map(
        "KOSPI",
        date(2026, 3, 6),
        snapshot_dir=tmp_path,
        official_only=True,
    )

    assert sector_map == {}
    assert snapshot_path is None


def test_fetch_index_ohlcv_frame_uses_public_symbol_mapping(monkeypatch) -> None:
    calls: list[str] = []
    frame = pd.DataFrame(
        {
            "Open": [1.0],
            "High": [2.0],
            "Low": [0.5],
            "Close": [1.5],
            "Volume": [10],
        },
        index=pd.to_datetime(["2026-03-06"]),
    )

    class DummyFdrModule:
        @staticmethod
        def DataReader(symbol, start, end):  # noqa: N802
            calls.append(symbol)
            _ = (start, end)
            return frame

    monkeypatch.setattr("eit_market_data.kr.market_helpers._load_fdr", lambda: DummyFdrModule())

    result, source = fetch_index_ohlcv_frame("1001", date(2026, 3, 1), date(2026, 3, 6))

    assert source == "fdr"
    assert result is not None and not result.empty
    assert calls == ["KS11"]


def test_fetch_market_cap_frame_raises_on_unexpected_columns(monkeypatch) -> None:
    class DummyFdrModule:
        @staticmethod
        def StockListing(*args, **kwargs):  # noqa: ANN001, ANN205
            return pd.DataFrame({"Code": ["005930"], "foo": [1]})

    monkeypatch.setattr("eit_market_data.kr.market_helpers._load_fdr", lambda: DummyFdrModule())
    monkeypatch.setattr(
        "eit_market_data.kr.market_helpers._load_local_market_cap_snapshot",
        lambda as_of, market: None,
    )
    monkeypatch.setattr(
        "eit_market_data.kr.market_helpers.ensure_krx_authenticated_session",
        lambda interactive=False: None,
    )

    try:
        fetch_market_cap_frame(date(2026, 3, 6), "KOSPI")
    except RuntimeError as exc:
        assert "unexpected columns" in str(exc)
    else:
        raise AssertionError("expected RuntimeError")


def test_fetch_market_cap_frame_uses_local_cap_daily_snapshot_with_monkeypatched_dir(
    tmp_path,
    monkeypatch,
) -> None:
    frame = pd.DataFrame(
        {
            "종목코드": ["005930"],
            "종목명": ["삼성전자"],
            "시장": ["KOSPI"],
            "종가": [70000],
            "시가총액": [420000000000000],
            "상장주식수": [5960000000],
            "source_trade_date": [pd.Timestamp("2024-01-31")],
        }
    )
    path = tmp_path / "KOSPI_20240131.parquet"
    path.write_text("stub")
    monkeypatch.setattr("eit_market_data.kr.market_helpers.CAP_DAILY_DIR", tmp_path)
    monkeypatch.setattr("pandas.read_parquet", lambda target: frame if target == path else None)

    result = fetch_market_cap_frame(date(2024, 1, 31), "KOSPI")

    assert result is not None
    assert int(result.loc["005930", "시가총액"]) == 420000000000000
    assert int(result.loc["005930", "상장주식수"]) == 5960000000


def test_fetch_market_cap_frame_uses_previous_local_trading_day(tmp_path, monkeypatch) -> None:
    frame = pd.DataFrame(
        {
            "종목코드": ["005930"],
            "종목명": ["삼성전자"],
            "시장": ["KOSPI"],
            "종가": [70200],
            "시가총액": [421000000000000],
            "상장주식수": [5997150997],
            "source_trade_date": [pd.Timestamp("2024-02-02")],
        }
    )
    path = tmp_path / "KOSPI_20240202.parquet"
    path.write_text("stub")
    monkeypatch.setattr("eit_market_data.kr.market_helpers.CAP_DAILY_DIR", tmp_path)
    monkeypatch.setattr("pandas.read_parquet", lambda target: frame if target == path else None)

    result = fetch_market_cap_frame(date(2024, 2, 4), "KOSPI")

    assert result is not None
    assert int(result.loc["005930", "종가"]) == 70200


def test_fetch_market_cap_frame_returns_none_for_old_dates_without_local_snapshot(monkeypatch) -> None:
    monkeypatch.setattr(
        "eit_market_data.kr.market_helpers._load_fdr",
        lambda: (_ for _ in ()).throw(AssertionError("should not load fdr")),
    )

    assert fetch_market_cap_frame(date(2024, 1, 31), "KOSPI") is None


def test_fetch_live_sector_classification_map_uses_krx_desc_industry(monkeypatch) -> None:
    class DummyFdrModule:
        @staticmethod
        def StockListing(market):  # noqa: N802
            assert market == "KRX-DESC"
            return pd.DataFrame(
                {
                    "Code": ["005930", "247540", "000660"],
                    "Market": ["KOSPI", "KOSDAQ GLOBAL", "KOSPI"],
                    "Industry": ["통신 및 방송 장비 제조업", "일차전지 및 이차전지 제조업", "반도체 제조업"],
                    "Sector": ["", "우량기업부", ""],
                }
            )

    monkeypatch.setattr("eit_market_data.kr.market_helpers._load_fdr", lambda: DummyFdrModule())

    sector_map, query_day = fetch_live_sector_classification_map("KOSDAQ", date(2026, 3, 12))

    assert query_day == date(2026, 3, 12)
    assert sector_map == {"247540": "일차전지 및 이차전지 제조업"}
