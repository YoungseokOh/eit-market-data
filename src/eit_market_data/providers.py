"""Abstract data provider interfaces.

All providers return point-in-time data: results must only include
information available on or before the requested `as_of` date.

Implement concrete providers by sub-classing these protocols.
The default implementation is SyntheticProvider (see synthetic.py).
"""

from __future__ import annotations

from datetime import date  # noqa: TC003
from typing import Protocol, runtime_checkable

from eit_market_data.schemas.snapshot import (  # noqa: TC001
    FilingData,
    FundamentalData,
    MacroData,
    NewsItem,
    PriceBar,
    SectorAverages,
)


@runtime_checkable
class PriceProvider(Protocol):
    """Provides historical OHLCV price data."""

    async def fetch_prices(
        self, ticker: str, as_of: date, lookback_days: int = 300
    ) -> list[PriceBar]:
        """Return up to `lookback_days` trading-day bars ending on or before `as_of`."""
        ...


@runtime_checkable
class FundamentalProvider(Protocol):
    """Provides quarterly financial data."""

    async def fetch_fundamentals(
        self, ticker: str, as_of: date, n_quarters: int = 8
    ) -> FundamentalData:
        """Return the most recent `n_quarters` quarters filed on or before `as_of`."""
        ...


@runtime_checkable
class FilingProvider(Protocol):
    """Provides qualitative text sections from corporate filings."""

    async def fetch_filing(self, ticker: str, as_of: date) -> FilingData:
        """Return the most recent filing text sections available on or before `as_of`."""
        ...


@runtime_checkable
class NewsProvider(Protocol):
    """Provides news headlines and summaries."""

    async def fetch_news(
        self, ticker: str, as_of: date, lookback_days: int = 30
    ) -> list[NewsItem]:
        """Return news items from `[as_of - lookback_days, as_of]`."""
        ...


@runtime_checkable
class MacroProvider(Protocol):
    """Provides macro-economic indicators."""

    async def fetch_macro(self, as_of: date) -> MacroData:
        """Return macro indicators available on or before `as_of`."""
        ...


@runtime_checkable
class SectorProvider(Protocol):
    """Provides sector mapping and averages."""

    async def fetch_sector_map(
        self, universe: list[str], as_of: date | None = None
    ) -> dict[str, str]:
        """Return {ticker: sector_name} for the given universe, as of the given date."""
        ...

    async def fetch_sector_averages(
        self, sector: str, tickers: list[str], as_of: date
    ) -> SectorAverages:
        """Return average fundamental metrics for the sector."""
        ...


@runtime_checkable
class BenchmarkProvider(Protocol):
    """Provides benchmark index prices."""

    async def fetch_benchmark(self, as_of: date, lookback_days: int = 300) -> list[PriceBar]:
        """Return benchmark index OHLCV bars ending on or before `as_of`."""
        ...
