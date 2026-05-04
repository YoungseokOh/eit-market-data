from __future__ import annotations

import asyncio
from datetime import date

import pytest

from eit_market_data.schemas.snapshot import (
    FilingData,
    FundamentalData,
    MacroData,
    PriceBar,
    SectorAverages,
)
from eit_market_data.snapshot import SnapshotBuilder


class _AsOfRecorderProvider:
    def __init__(self) -> None:
        self.seen_as_of: list[date] = []

    async def fetch_prices(
        self,
        ticker: str,
        as_of: date,
        lookback_days: int = 300,
    ) -> list[PriceBar]:
        _ = lookback_days
        self.seen_as_of.append(as_of)
        return [PriceBar(date=as_of, open=1, high=1, low=1, close=1, volume=1)]

    async def fetch_fundamentals(
        self,
        ticker: str,
        as_of: date,
        n_quarters: int = 8,
    ) -> FundamentalData:
        _ = n_quarters
        self.seen_as_of.append(as_of)
        return FundamentalData(ticker=ticker, last_close_price=1)

    async def fetch_filing(self, ticker: str, as_of: date) -> FilingData:
        self.seen_as_of.append(as_of)
        return FilingData(ticker=ticker, filing_date=as_of, business_overview="ok")

    async def fetch_macro(self, as_of: date) -> MacroData:
        self.seen_as_of.append(as_of)
        return MacroData(rates_policy={"base_rate": 1})

    async def fetch_sector_map(
        self,
        universe: list[str],
        as_of: date | None = None,
    ) -> dict[str, str]:
        assert as_of is not None
        self.seen_as_of.append(as_of)
        return {ticker: "Tech" for ticker in universe}

    async def fetch_sector_averages(
        self,
        sector: str,
        tickers: list[str],
        as_of: date,
    ) -> SectorAverages:
        _ = tickers
        self.seen_as_of.append(as_of)
        return SectorAverages(sector=sector)

    async def fetch_benchmark(
        self,
        as_of: date,
        lookback_days: int = 300,
    ) -> list[PriceBar]:
        _ = lookback_days
        self.seen_as_of.append(as_of)
        return [PriceBar(date=as_of, open=1, high=1, low=1, close=1, volume=1)]


def test_snapshot_builder_accepts_partial_month_decision_date() -> None:
    provider = _AsOfRecorderProvider()
    builder = SnapshotBuilder(
        price_provider=provider,
        fundamental_provider=provider,
        filing_provider=provider,
        macro_provider=provider,
        sector_provider=provider,
        benchmark_provider=provider,
    )

    snapshot = asyncio.run(
        builder.build("2026-05", ["005930"], decision_date=date(2026, 5, 4))
    )

    assert snapshot.decision_date == date(2026, 5, 4)
    assert snapshot.execution_date == date(2026, 5, 5)
    assert set(provider.seen_as_of) == {date(2026, 5, 4)}


def test_snapshot_builder_rejects_decision_date_outside_month() -> None:
    provider = _AsOfRecorderProvider()
    builder = SnapshotBuilder(
        price_provider=provider,
        fundamental_provider=provider,
        filing_provider=provider,
        macro_provider=provider,
        sector_provider=provider,
        benchmark_provider=provider,
    )

    with pytest.raises(ValueError, match="outside snapshot month"):
        asyncio.run(
            builder.build("2026-05", ["005930"], decision_date=date(2026, 4, 30))
        )
