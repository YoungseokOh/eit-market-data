"""Shared test helpers.

Centralizes fixtures that were duplicated across several test modules so a
single signature change does not have to be mirrored into N stubs:

- ``load_script_module``: importlib shim to load a ``scripts/*.py`` entry point
  as a module (the entry points are not importable as packages).
- ``FakeCache``: minimal diskcache stand-in (``get`` + ``iterkeys``).
- ``flow_fact`` / ``instant_fact``: XBRL fact builders for EDGAR tests.
- ``StubProvider``: a Protocol-conforming provider base returning empty values,
  so a test only overrides the one method it cares about and stays valid when
  the provider protocols gain parameters.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from datetime import date

from eit_market_data.schemas.snapshot import (
    FilingData,
    FundamentalData,
    MacroData,
    SectorAverages,
)


def load_script_module(path: str | Path, module_name: str):  # noqa: ANN201
    """Load a standalone ``scripts/*.py`` file as an importable module."""
    path = Path(path)
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


class FakeCache:
    """Minimal diskcache stand-in exposing ``get()`` and ``iterkeys()``."""

    def __init__(self, data: dict[str, object]) -> None:
        self._data = dict(data)

    def get(self, key: str) -> object:
        return self._data.get(key)

    def iterkeys(self):  # noqa: ANN201 - mirrors diskcache.Cache API
        return iter(self._data.keys())


def flow_fact(start: str, end: str, val: float, filed: str, form: str = "10-Q") -> dict:
    """Build a flow (duration) XBRL fact for EDGAR XBRL tests."""
    return {"start": start, "end": end, "val": val, "filed": filed, "form": form}


def instant_fact(end: str, val: float, filed: str, form: str = "10-Q") -> dict:
    """Build an instant (point-in-time) XBRL fact for EDGAR XBRL tests."""
    return {"end": end, "val": val, "filed": filed, "form": form}


class StubProvider:
    """Protocol-conforming provider base returning empty point-in-time values.

    Subclass and override only the method under test. ``**kwargs`` on every
    method keeps the stub valid if a provider protocol gains a parameter
    (e.g. ``n_quarters``, ``lookback_days``), so tests do not break en masse.
    """

    async def fetch_prices(self, ticker: str, as_of: date, **kwargs: Any) -> list:
        return []

    async def fetch_fundamentals(
        self, ticker: str, as_of: date, **kwargs: Any
    ) -> FundamentalData:
        return FundamentalData(ticker=ticker, quarters=[])

    async def fetch_filing(self, ticker: str, as_of: date, **kwargs: Any) -> FilingData:
        return FilingData(ticker=ticker)

    async def fetch_news(self, ticker: str, as_of: date, **kwargs: Any) -> list:
        return []

    async def fetch_macro(self, as_of: date, **kwargs: Any) -> MacroData:
        return MacroData()

    async def fetch_sector_map(
        self, tickers: list[str], as_of: date, **kwargs: Any
    ) -> dict[str, str]:
        return {}

    async def fetch_sector_averages(
        self, as_of: date, **kwargs: Any
    ) -> dict[str, SectorAverages]:
        return {}

    async def fetch_benchmark(self, as_of: date, **kwargs: Any) -> list:
        return []
