from __future__ import annotations

import asyncio
from datetime import date

from eit_market_data.stockanalysis_provider import (
    FallbackPriceProvider,
    StockAnalysisPriceProvider,
)

# Newest-first rows as the stockanalysis.com API returns them. Row for
# 2020-06-02 has a != c (a dividend/split-adjusted close) to exercise the
# proportional back-adjustment; the 2099 row must be dropped by the as_of
# filter; the zero-close row must be skipped (guards divide-by-zero).
_ROWS = [
    {"t": "2099-01-01", "o": 999.0, "h": 999.0, "l": 999.0, "c": 999.0, "a": 999.0, "v": 1},
    {"t": "2020-06-03", "o": 100.0, "h": 110.0, "l": 90.0, "c": 100.0, "a": 100.0, "v": 500},
    {"t": "2020-06-02", "o": 100.0, "h": 120.0, "l": 80.0, "c": 100.0, "a": 50.0, "v": 400},
    {"t": "2020-06-01", "o": 0.0, "h": 0.0, "l": 0.0, "c": 0.0, "a": 0.0, "v": 0},
]


def _provider(rows: list[dict]) -> StockAnalysisPriceProvider:
    provider = StockAnalysisPriceProvider()
    provider._fetch_raw = lambda ticker: rows  # type: ignore[method-assign]
    return provider


def test_parses_rows_ascending_and_adjusts_ohlc() -> None:
    provider = _provider(_ROWS)
    bars = asyncio.run(provider.fetch_prices("TWTR", date(2020, 12, 31)))

    # Zero-close row dropped; two valid bars, ascending by date.
    assert [b.date for b in bars] == [date(2020, 6, 2), date(2020, 6, 3)]

    # 2020-06-02: factor a/c = 50/100 = 0.5 applied to OHL, close = a.
    adjusted = bars[0]
    assert adjusted.open == 50.0
    assert adjusted.high == 60.0
    assert adjusted.low == 40.0
    assert adjusted.close == 50.0
    assert adjusted.volume == 400.0  # volume left raw (auto_adjust convention)

    # 2020-06-03: a == c so no scaling.
    assert bars[1].close == 100.0
    assert bars[1].high == 110.0


def test_as_of_filter_drops_future_bars() -> None:
    provider = _provider(_ROWS)
    bars = asyncio.run(provider.fetch_prices("TWTR", date(2020, 6, 2)))
    # Only bars on/before 2020-06-02 survive (the 2099 and 2020-06-03 dropped).
    assert [b.date for b in bars] == [date(2020, 6, 2)]


def test_lookback_caps_to_last_n_bars() -> None:
    provider = _provider(_ROWS)
    bars = asyncio.run(provider.fetch_prices("TWTR", date(2020, 12, 31), lookback_days=1))
    assert len(bars) == 1
    assert bars[0].date == date(2020, 6, 3)  # most recent kept


def test_empty_on_error_never_raises() -> None:
    provider = StockAnalysisPriceProvider()

    def _boom(ticker: str):  # noqa: ANN202
        raise RuntimeError("network down")

    provider._fetch_raw = _boom  # type: ignore[method-assign]
    bars = asyncio.run(provider.fetch_prices("SIVB", date(2020, 12, 31)))
    assert bars == []


def test_missing_symbol_returns_empty() -> None:
    provider = _provider([])
    assert asyncio.run(provider.fetch_prices("XLNX", date(2020, 12, 31))) == []


class _StubProvider:
    def __init__(self, bars: list) -> None:
        self._bars = bars
        self.calls = 0

    async def fetch_prices(self, ticker, as_of, lookback_days=300):  # noqa: ANN001, ANN201
        self.calls += 1
        return list(self._bars)


def test_fallback_prefers_primary_when_nonempty() -> None:
    primary = _StubProvider(["PRIMARY_BAR"])
    supplementary = _StubProvider(["FALLBACK_BAR"])
    chain = FallbackPriceProvider(primary, supplementary)

    result = asyncio.run(chain.fetch_prices("AAPL", date(2020, 12, 31)))
    assert result == ["PRIMARY_BAR"]
    assert supplementary.calls == 0  # supplementary untouched for listed names


def test_fallback_uses_supplementary_when_primary_empty() -> None:
    primary = _StubProvider([])
    supplementary = _StubProvider(["FALLBACK_BAR"])
    chain = FallbackPriceProvider(primary, supplementary)

    result = asyncio.run(chain.fetch_prices("TWTR", date(2020, 12, 31)))
    assert result == ["FALLBACK_BAR"]
    assert supplementary.calls == 1
