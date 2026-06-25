from __future__ import annotations

from datetime import date

from eit_market_data.yfinance_provider import YFinanceProvider


def test_get_info_does_not_cache_empty_response(monkeypatch) -> None:
    provider = object.__new__(YFinanceProvider)
    provider._sector_cache = {}
    provider._info_cache = {}

    calls = {"count": 0}

    class DummyTicker:
        @property
        def info(self):  # noqa: ANN201
            calls["count"] += 1
            if calls["count"] == 1:
                raise RuntimeError("temporary failure")
            return {"marketCap": 123.0, "previousClose": 45.0}

    monkeypatch.setattr(provider, "_get_ticker", lambda symbol: DummyTicker())

    first = provider._get_info("MSFT")
    second = provider._get_info("MSFT")

    assert first == {"marketCap": 123.0, "previousClose": 45.0}
    assert second == {"marketCap": 123.0, "previousClose": 45.0}
    assert provider._info_cache["MSFT"]["marketCap"] == 123.0
    assert calls["count"] == 2


def test_fetch_fundamentals_market_cap_none_when_no_quarters(monkeypatch) -> None:
    # Point-in-time contract: with no quarterly statements there are no PIT
    # issued shares, so market_cap must be None rather than the *current*
    # info.marketCap (which would leak look-ahead). last_close_price is the
    # as-of unadjusted close reconstructed from price history.
    provider = object.__new__(YFinanceProvider)
    provider._sector_cache = {}
    provider._info_cache = {"MSFT": {}}

    monkeypatch.setattr(provider, "_get_statement", lambda symbol, attr: None)
    monkeypatch.setattr(provider, "_asof_close", lambda ticker, as_of: 78.0)

    fundamentals = provider._fetch_fundamentals_sync("MSFT", date(2025, 12, 31), 8)

    assert fundamentals.quarters == []
    assert fundamentals.market_cap is None
    assert fundamentals.last_close_price == 78.0


def test_fetch_prices_retries_after_rate_limit(monkeypatch) -> None:
    provider = object.__new__(YFinanceProvider)
    provider._sector_cache = {}
    provider._info_cache = {}

    class DummyFrame:
        empty = False

        @staticmethod
        def iterrows():
            class _Idx:
                @staticmethod
                def date():
                    return date(2025, 12, 31)

            yield _Idx(), {"Open": 1.0, "High": 1.0, "Low": 1.0, "Close": 1.0, "Volume": 1.0}

    calls = {"count": 0}

    class DummyTicker:
        def history(self, **kwargs):  # noqa: ANN003
            _ = kwargs
            calls["count"] += 1
            if calls["count"] == 1:
                raise RuntimeError("Too Many Requests")
            return DummyFrame()

    monkeypatch.setattr(provider, "_get_ticker", lambda symbol: DummyTicker())

    bars = provider._fetch_prices_sync("MSFT", date(2025, 12, 31), 300)

    assert len(bars) == 1
    assert calls["count"] == 2
