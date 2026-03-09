"""MonthlySnapshot builder.

Assembles point-in-time data from all providers into a frozen
MonthlySnapshot, enforcing strict look-ahead prevention.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

from eit_market_data.synthetic import SyntheticProvider
from eit_market_data.schemas.snapshot import MonthlySnapshot, SnapshotMetadata


@dataclass
class SnapshotConfig:
    artifacts_dir: str = "artifacts"


def config_hash(config: SnapshotConfig) -> str:
    blob = json.dumps({"artifacts_dir": config.artifacts_dir}).encode()
    return hashlib.sha256(blob).hexdigest()[:16]


def create_real_providers() -> dict:
    """Create real data providers (yfinance + FRED + EDGAR).

    Returns dict of keyword arguments for SnapshotBuilder.
    Requires: ``pip install -e '.[real-data]'``
    """
    from eit_market_data.edgar_provider import EdgarFilingProvider
    from eit_market_data.fred_provider import FredMacroProvider
    from eit_market_data.yfinance_provider import YFinanceProvider

    yf = YFinanceProvider()
    return {
        "price_provider": yf,
        "fundamental_provider": yf,
        "filing_provider": EdgarFilingProvider(),
        "news_provider": yf,
        "macro_provider": FredMacroProvider(),
        "sector_provider": yf,
        "benchmark_provider": yf,
    }


def create_kr_providers() -> dict:
    """Create Korean market data providers (pykrx + DART + ECOS).

    Returns dict of keyword arguments for SnapshotBuilder.
    Requires: pip install -e '.[kr]'
    """
    from eit_market_data.kr.pykrx_provider import PykrxProvider
    from eit_market_data.kr.dart_provider import DartProvider
    from eit_market_data.kr.ecos_provider import EcosMacroProvider

    dart = DartProvider()
    pykrx = PykrxProvider(fundamental_provider=dart)
    return {
        "price_provider": pykrx,
        "fundamental_provider": dart,
        "filing_provider": dart,
        "news_provider": pykrx,
        "macro_provider": EcosMacroProvider(),
        "sector_provider": pykrx,
        "benchmark_provider": pykrx,
    }


def _last_business_day(year: int, month: int) -> date:
    """Return the last business day of the given month."""
    # Go to last day of month
    if month == 12:
        last = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        last = date(year, month + 1, 1) - timedelta(days=1)
    # Walk back to a weekday
    while last.weekday() >= 5:
        last -= timedelta(days=1)
    return last


def _first_business_day(year: int, month: int) -> date:
    """Return the first business day of the given month."""
    first = date(year, month, 1)
    while first.weekday() >= 5:
        first += timedelta(days=1)
    return first


def _next_month(year: int, month: int) -> tuple[int, int]:
    """Return (year, month) for the next calendar month."""
    if month == 12:
        return year + 1, 1
    return year, month + 1


def _content_hash(obj: object) -> str:
    """SHA-256 of the JSON-serializable object."""
    blob = json.dumps(obj, sort_keys=True, default=str).encode()
    return hashlib.sha256(blob).hexdigest()[:16]


class SnapshotBuilder:
    """Builds a MonthlySnapshot from configured providers.

    Uses SyntheticProvider by default; swap in real providers by
    passing them to the constructor.
    """

    def __init__(
        self,
        price_provider=None,
        fundamental_provider=None,
        filing_provider=None,
        news_provider=None,
        macro_provider=None,
        sector_provider=None,
        benchmark_provider=None,
    ):
        # Default to synthetic for all
        synth = SyntheticProvider()
        self.price = price_provider or synth
        self.fundamental = fundamental_provider or synth
        self.filing = filing_provider or synth
        self.news = news_provider or synth
        self.macro = macro_provider or synth
        self.sector = sector_provider or synth
        self.benchmark = benchmark_provider or synth

    async def build(
        self,
        month: str,
        universe: list[str],
        config: SnapshotConfig | None = None,
    ) -> MonthlySnapshot:
        """Build a complete point-in-time snapshot for a given month.

        Args:
            month: Decision month in "YYYY-MM" format.
            universe: List of tickers in the investment universe.
            config: Full configuration.

        Returns:
            Frozen MonthlySnapshot.
        """
        config = config or SnapshotConfig()
        year, mon = int(month[:4]), int(month[5:7])
        decision_date = _last_business_day(year, mon)
        ny, nm = _next_month(year, mon)
        execution_date = _first_business_day(ny, nm)

        # Fetch all data concurrently
        price_tasks = {t: self.price.fetch_prices(t, decision_date) for t in universe}
        fund_tasks = {t: self.fundamental.fetch_fundamentals(t, decision_date) for t in universe}
        filing_tasks = {t: self.filing.fetch_filing(t, decision_date) for t in universe}
        news_tasks = {t: self.news.fetch_news(t, decision_date) for t in universe}
        macro_task = self.macro.fetch_macro(decision_date)
        sector_map_task = self.sector.fetch_sector_map(universe)
        benchmark_task = self.benchmark.fetch_benchmark(decision_date)

        # Gather all per-stock data
        all_prices = await asyncio.gather(*price_tasks.values())
        all_funds = await asyncio.gather(*fund_tasks.values())
        all_filings = await asyncio.gather(*filing_tasks.values())
        all_news = await asyncio.gather(*news_tasks.values())
        macro, sector_map, benchmark_prices = await asyncio.gather(
            macro_task, sector_map_task, benchmark_task
        )

        prices = dict(zip(price_tasks.keys(), all_prices, strict=True))
        fundamentals = dict(zip(fund_tasks.keys(), all_funds, strict=True))
        filings = dict(zip(filing_tasks.keys(), all_filings, strict=True))
        news = dict(zip(news_tasks.keys(), all_news, strict=True))

        # Build sector averages
        sectors: dict[str, list[str]] = defaultdict(list)
        for t, s in sector_map.items():
            sectors[s].append(t)

        sector_avg_tasks = {
            s: self.sector.fetch_sector_averages(s, tickers, decision_date)
            for s, tickers in sectors.items()
        }
        all_sector_avgs = await asyncio.gather(*sector_avg_tasks.values())
        sector_averages = dict(zip(sector_avg_tasks.keys(), all_sector_avgs, strict=True))

        # Compute content hashes
        input_hash = _content_hash({
            "decision_date": str(decision_date),
            "universe": sorted(universe),
        })

        metadata = SnapshotMetadata(
            created_at=datetime.utcnow().isoformat(),
            config_hash=config_hash(config),
            price_hash=_content_hash({t: len(p) for t, p in prices.items()}),
            fundamental_hash=_content_hash({t: len(f.quarters) for t, f in fundamentals.items()}),
            filing_hash=_content_hash({t: bool(f.business_overview) for t, f in filings.items()}),
            news_hash=_content_hash({t: len(n) for t, n in news.items()}),
            macro_hash=_content_hash(macro.model_dump()),
        )

        return MonthlySnapshot(
            decision_date=decision_date,
            execution_date=execution_date,
            universe=universe,
            prices=prices,
            fundamentals=fundamentals,
            filings=filings,
            news=news,
            macro=macro,
            sector_map=sector_map,
            sector_averages=sector_averages,
            benchmark_prices=benchmark_prices,
            input_hash=input_hash,
            metadata=metadata,
        )

    async def build_and_persist(
        self,
        month: str,
        universe: list[str],
        config: SnapshotConfig | None = None,
    ) -> MonthlySnapshot:
        """Build snapshot and save metadata to artifacts."""
        snapshot = await self.build(month, universe, config)

        artifacts_root = config.artifacts_dir if config is not None else "artifacts"
        artifacts_dir = Path(artifacts_root) / "snapshots" / month
        artifacts_dir.mkdir(parents=True, exist_ok=True)

        meta_path = artifacts_dir / "metadata.json"
        meta_path.write_text(snapshot.metadata.model_dump_json(indent=2))

        return snapshot
