from __future__ import annotations

from datetime import date
from pathlib import Path

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


def test_fetch_index_ohlcv_frame_uses_pykrx(monkeypatch) -> None:
    calls: list[str] = []

    def _mock_load_fdr() -> type:
        class _Fdr:
            @staticmethod
            def DataReader(symbol: str, start: str, end: str):  # noqa: ANN001,ANN204
                assert symbol == "^KS11"
                assert start == "2026-03-01"
                assert end == "2026-03-06"
                return None

        return _Fdr

    class DummyStock:
        @staticmethod
        def get_index_ohlcv_by_date(start, end, code, name_display=False):  # noqa: ANN001
            calls.append(code)
            _ = (start, end)
            return frame

    frame = pd.DataFrame(
        {
            "시가": [1.0],
            "고가": [2.0],
            "저가": [0.5],
            "종가": [1.5],
            "거래량": [10],
        },
        index=pd.to_datetime(["2026-03-06"]),
    )

    monkeypatch.setattr("eit_market_data.kr.market_helpers._load_fdr", _mock_load_fdr)
    monkeypatch.setattr(
        "eit_market_data.kr.market_helpers.install_pykrx_krx_session_hooks",
        lambda: None,
    )
    monkeypatch.setattr(
        "eit_market_data.kr.market_helpers.ensure_krx_authenticated_session",
        lambda interactive: None,
    )
    monkeypatch.setattr(
        "eit_market_data.kr.pykrx_loader.load_pykrx_stock",
        lambda: DummyStock,
    )

    result, source = fetch_index_ohlcv_frame("1001", date(2026, 3, 1), date(2026, 3, 6))

    assert source == "pykrx"
    assert result is not None and not result.empty
    assert calls == ["1001"]


def test_fetch_market_cap_frame_raises_on_unexpected_columns(monkeypatch) -> None:
    from datetime import timedelta

    class DummyStock:
        @staticmethod
        def get_market_cap(*args, **kwargs):  # noqa: ANN001, ANN205
            return pd.DataFrame({"foo": [1]}, index=pd.Index(["005930"], name="티커"))

    monkeypatch.setattr("eit_market_data.kr.market_helpers.CAP_DAILY_DIR", Path("/tmp/does-not-exist"))
    monkeypatch.setattr(
        "eit_market_data.kr.market_helpers._load_fdr",
        lambda: type("_Fdr", (), {"StockListing": staticmethod(lambda *args, **kwargs: pd.DataFrame())})(),
    )
    monkeypatch.setattr("eit_market_data.kr.market_helpers.install_pykrx_krx_session_hooks", lambda: None)
    monkeypatch.setattr("eit_market_data.kr.market_helpers.ensure_krx_authenticated_session", lambda interactive: None)
    monkeypatch.setattr(
        "eit_market_data.kr.pykrx_loader.load_pykrx_stock",
        lambda: DummyStock,
    )

    as_of = date.today() - timedelta(days=1)

    try:
        fetch_market_cap_frame(as_of, "KOSPI")
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


def test_fetch_market_cap_frame_uses_pykrx_for_recent_dates_when_fdr_fails(
    tmp_path,
    monkeypatch,
) -> None:
    frame = pd.DataFrame(
        {
            "종가": [70000],
            "시가총액": [420000000000000],
            "거래량": [100],
            "거래대금": [7000000],
            "상장주식수": [5960000000],
        },
        index=pd.Index(["005930"], name="티커"),
    )

    class DummyStock:
        @staticmethod
        def get_market_cap(date_str, market):  # noqa: ANN001
            assert market == "KOSPI"
            assert date_str == as_of.strftime("%Y%m%d")
            return frame

    from datetime import timedelta

    as_of = date.today() - timedelta(days=10)

    monkeypatch.setattr("eit_market_data.kr.market_helpers.CAP_DAILY_DIR", tmp_path)
    monkeypatch.setattr(
        "eit_market_data.kr.market_helpers._load_fdr",
        lambda: type("_Fdr", (), {"StockListing": staticmethod(lambda *args, **kwargs: pd.DataFrame())})(),
    )
    monkeypatch.setattr("eit_market_data.kr.market_helpers.install_pykrx_krx_session_hooks", lambda: None)
    monkeypatch.setattr("eit_market_data.kr.market_helpers.ensure_krx_authenticated_session", lambda interactive: None)
    monkeypatch.setattr(
        "eit_market_data.kr.pykrx_loader.load_pykrx_stock",
        lambda: DummyStock,
    )

    result = fetch_market_cap_frame(as_of, "KOSPI")

    assert result is not None
    assert int(result.loc["005930", "시가총액"]) == 420000000000000


def test_fetch_live_sector_classification_map_uses_fdr_desc(monkeypatch) -> None:
    class DummyFdr:
        @staticmethod
        def StockListing(market: str) -> pd.DataFrame:
            assert market == "KRX-DESC"
            return pd.DataFrame(
                {
                    "Code": ["247540"],
                    "Market": ["KOSDAQ"],
                    "Industry": ["일차전지 및 이차전지 제조업"],
                    "Sector": ["TestSector"],
                    "ListingDate": ["1975-06-11"],
                }
            )

    monkeypatch.setattr("eit_market_data.kr.market_helpers._load_fdr", lambda: DummyFdr())

    sector_map, query_day = fetch_live_sector_classification_map("KOSDAQ", date(2026, 3, 12))

    assert query_day == date(2026, 3, 12)
    assert sector_map == {"247540": "일차전지 및 이차전지 제조업"}
