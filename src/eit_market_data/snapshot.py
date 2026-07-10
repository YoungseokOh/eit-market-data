"""MonthlySnapshot builder.

Assembles point-in-time data from all providers into a frozen
MonthlySnapshot, enforcing strict look-ahead prevention.
"""

from __future__ import annotations

import asyncio
import gzip
import hashlib
import json
import logging
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Literal

from eit_market_data.core.calendar import (
    _last_business_day,
    _next_business_day,
)
from eit_market_data.core.hashing import _content_hash
from eit_market_data.synthetic import SyntheticProvider
from eit_market_data.schemas.snapshot import MonthlySnapshot, PriceBar, SnapshotMetadata

MONTHLY_SNAPSHOT_FILENAME = "snapshot.json"
logger = logging.getLogger(__name__)


@dataclass
class SnapshotConfig:
    artifacts_dir: str = "artifacts"


def config_hash(config: SnapshotConfig) -> str:
    blob = json.dumps({"artifacts_dir": config.artifacts_dir}).encode()
    return hashlib.sha256(blob).hexdigest()[:16]


def create_real_providers(fundamentals_source: str = "yfinance") -> dict:
    """Create real data providers (yfinance + FRED + EDGAR).

    Returns dict of keyword arguments for SnapshotBuilder.
    Requires: ``pip install -e '.[real-data]'``

    ``fundamentals_source`` selects the fundamental provider:

    * ``"yfinance"`` (default) — yfinance quarterly statements. Rolling ~5
      quarters only, so it cannot reach far into the past.
    * ``"edgar_xbrl"`` — SEC EDGAR XBRL companyfacts. True point-in-time
      (per-fact ``filed`` dates) with history back to ~2009; required for
      historical backfills. Prices/sector/benchmark stay on yfinance.
    """
    from eit_market_data.edgar_provider import EdgarFilingProvider
    from eit_market_data.fred_provider import FredMacroProvider
    from eit_market_data.yfinance_provider import YFinanceProvider

    yf = YFinanceProvider()
    if fundamentals_source == "edgar_xbrl":
        from eit_market_data.edgar_xbrl_provider import EdgarXbrlFundamentalProvider

        fundamental_provider: Any = EdgarXbrlFundamentalProvider(price_provider=yf)
    elif fundamentals_source == "yfinance":
        fundamental_provider = yf
    else:
        raise ValueError(f"unknown fundamentals_source: {fundamentals_source!r}")

    # Optional survivorship-bias fallback: when EIT_US_DELISTED_FALLBACK is set,
    # wrap the *price* provider so delisted tickers that yfinance purges (HTTP
    # 404) are back-filled from stockanalysis.com. It is fill-empties-only — a
    # still-listed ticker that yfinance serves is never touched, so the existing
    # bundles are byte-identical. Inert (yfinance-only) unless the env var is
    # set. See docs/us_delisted_supplementary_source.md.
    price_provider: Any = yf
    if os.getenv("EIT_US_DELISTED_FALLBACK", "").strip().lower() in ("1", "true", "yes"):
        from eit_market_data.stockanalysis_provider import (
            FallbackPriceProvider,
            StockAnalysisPriceProvider,
        )

        price_provider = FallbackPriceProvider(yf, StockAnalysisPriceProvider())
        logger.info("US delisted-price fallback (stockanalysis.com) ENABLED")

    return {
        "price_provider": price_provider,
        "fundamental_provider": fundamental_provider,
        "filing_provider": EdgarFilingProvider(),
        "macro_provider": FredMacroProvider(),
        "sector_provider": yf,
        "benchmark_provider": yf,
    }


def create_kr_providers(
    profile: Literal["official", "official_enriched", "ci_safe"] = "official",
    *,
    universe_csv: str | Path | None = None,
    dart_override: Any | None = None,
    benchmark_index: str = "1001",
) -> dict:
    """Create Korean market data providers.

    Returns dict of keyword arguments for SnapshotBuilder.
    Requires: pip install -e '.[kr]'

    If *dart_override* is given it replaces the default ``DartProvider``
    for both fundamentals and filings, skipping any DART API initialisation.
    """
    from eit_market_data.kr.fundamental_provider import CompositeKrFundamentalProvider
    from eit_market_data.kr.ci_safe_provider import (
        FdrBenchmarkProvider,
        FdrNaverPriceProvider,
        NullDartProvider,
        NullMacroProvider,
        SeedSectorProvider,
    )
    from eit_market_data.kr.ecos_provider import EcosMacroProvider
    from eit_market_data.kr.dart_provider import DartProvider
    from eit_market_data.kr.pykrx_provider import PykrxProvider

    if dart_override is not None:
        dart = dart_override
    else:
        try:
            dart = DartProvider()
        except (ImportError, ValueError) as exc:
            logger.warning("Falling back to NullDartProvider: %s", exc)
            dart = NullDartProvider()
    try:
        macro = EcosMacroProvider()
    except (ImportError, ValueError) as exc:
        logger.warning("Falling back to NullMacroProvider: %s", exc)
        macro = NullMacroProvider()

    if profile == "ci_safe":
        price_provider = FdrNaverPriceProvider()
        fundamentals = CompositeKrFundamentalProvider(
            dart_provider=dart,
            price_provider=price_provider,
            use_market_snapshot=True,
        )
        sector_provider = SeedSectorProvider(
            universe_csv=universe_csv,
            fundamental_provider=fundamentals,
        )
        return {
            "price_provider": price_provider,
            "fundamental_provider": fundamentals,
            "filing_provider": dart,
            "macro_provider": macro,
            "sector_provider": sector_provider,
            "benchmark_provider": FdrBenchmarkProvider(),
        }

    pykrx = PykrxProvider(official_only=True, benchmark_index=benchmark_index)
    fundamentals = CompositeKrFundamentalProvider(dart_provider=dart, price_provider=pykrx)
    pykrx._fundamental_provider = fundamentals
    return {
        "price_provider": pykrx,
        "fundamental_provider": fundamentals,
        "filing_provider": dart,
        "macro_provider": macro,
        "sector_provider": pykrx,
        "benchmark_provider": pykrx,
    }


def _next_month(year: int, month: int) -> tuple[int, int]:
    """Return (year, month) for the next calendar month."""
    if month == 12:
        return year + 1, 1
    return year, month + 1


def serialize_snapshot(snapshot: MonthlySnapshot) -> dict[str, object]:
    """Serialize a snapshot while omitting empty legacy news fields."""
    exclude: dict[str, object] = {}
    if not snapshot.news:
        exclude["news"] = True
    # Omit the TR benchmark when unfetched so bundles that don't carry it stay
    # byte-identical to their pre-TR serialization (contract: existing values
    # immutable). Present only when the build actually populated it.
    if not snapshot.benchmark_tr_prices:
        exclude["benchmark_tr_prices"] = True

    metadata_exclude: dict[str, bool] = {}
    if not snapshot.metadata.news_hash:
        metadata_exclude["news_hash"] = True
    if metadata_exclude:
        exclude["metadata"] = metadata_exclude

    return snapshot.model_dump(mode="json", exclude=exclude)


def serialize_snapshot_metadata(metadata: SnapshotMetadata) -> dict[str, object]:
    """Serialize metadata while omitting empty legacy news hashes."""
    exclude: dict[str, bool] | None = {"news_hash": True} if not metadata.news_hash else None
    return metadata.model_dump(mode="json", exclude=exclude)


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
        self.news = news_provider
        self.macro = macro_provider or synth
        self.sector = sector_provider or synth
        self.benchmark = benchmark_provider or synth

    async def build(
        self,
        month: str,
        universe: list[str],
        config: SnapshotConfig | None = None,
        decision_date: date | None = None,
        market: str = "XNYS",
    ) -> MonthlySnapshot:
        """Build a complete point-in-time snapshot for a given month.

        Args:
            month: Decision month in "YYYY-MM" format.
            universe: List of tickers in the investment universe.
            config: Full configuration.
            decision_date: Optional explicit decision date for partial-month
                local runs. Defaults to the month's last *trading* day.
            market: Exchange MIC for the trading calendar used to pick
                decision/execution dates. Defaults to ``"XNYS"`` (US). KR
                callers should pass ``"XKRX"`` so execution_date skips KRX
                holidays.

        Returns:
            Frozen MonthlySnapshot.
        """
        config = config or SnapshotConfig()
        year, mon = int(month[:4]), int(month[5:7])
        if decision_date is None:
            decision_date = _last_business_day(year, mon, market)
        elif decision_date.strftime("%Y-%m") != month:
            raise ValueError(
                f"decision_date {decision_date.isoformat()} is outside snapshot month {month}"
            )
        execution_date = _next_business_day(decision_date, market)

        # Throttle concurrent API calls to avoid rate limits
        sem = asyncio.Semaphore(32)
        completed = 0
        total = len(universe) * 3

        async def _limited(coro, label: str = ""):
            nonlocal completed
            async with sem:
                result = await coro
            completed += 1
            if completed % 500 == 0 or completed == total:
                logger.info(
                    "  [build %s] %d/%d stock tasks done (%.0f%%)",
                    month, completed, total, completed / total * 100,
                )
            return result

        # Fetch all data with concurrency limit
        price_tasks = {t: _limited(self.price.fetch_prices(t, decision_date), "price") for t in universe}
        fund_tasks = {t: _limited(self.fundamental.fetch_fundamentals(t, decision_date), "fund") for t in universe}
        filing_tasks = {t: _limited(self.filing.fetch_filing(t, decision_date), "filing") for t in universe}
        macro_task = self.macro.fetch_macro(decision_date)
        sector_map_task = self.sector.fetch_sector_map(universe, as_of=decision_date)
        benchmark_task = self.benchmark.fetch_benchmark(decision_date)
        # TR benchmark is optional: only providers that implement fetch_benchmark_tr
        # (yfinance ^SP500TR, pykrx KR TR index) supply it; others yield [].
        benchmark_tr_fetch = getattr(self.benchmark, "fetch_benchmark_tr", None)

        async def _benchmark_tr() -> list[PriceBar]:
            if benchmark_tr_fetch is None:
                return []
            return await benchmark_tr_fetch(decision_date)

        benchmark_tr_task = _benchmark_tr()

        # Gather price / fundamental / filing all in parallel.
        # FdrNaverPriceProvider caches by (ticker, as_of), so fund_tasks and
        # sector_avg_tasks that also call fetch_prices() get cache hits instead
        # of duplicate NAVER HTTP requests.
        n = len(universe)
        all_stock_results = await asyncio.gather(
            *price_tasks.values(),
            *fund_tasks.values(),
            *filing_tasks.values(),
        )
        all_prices = list(all_stock_results[:n])
        all_funds = list(all_stock_results[n : 2 * n])
        all_filings = list(all_stock_results[2 * n :])

        macro, sector_map, benchmark_prices, benchmark_tr_prices = await asyncio.gather(
            macro_task, sector_map_task, benchmark_task, benchmark_tr_task
        )

        prices = dict(zip(price_tasks.keys(), all_prices, strict=True))
        fundamentals = dict(zip(fund_tasks.keys(), all_funds, strict=True))
        filings = dict(zip(filing_tasks.keys(), all_filings, strict=True))
        news = {}
        if self.news is not None:
            news_tasks = {t: self.news.fetch_news(t, decision_date) for t in universe}
            all_news = await asyncio.gather(*news_tasks.values())
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
            news_hash=_content_hash({t: len(n) for t, n in news.items()}) if news else "",
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
            benchmark_tr_prices=benchmark_tr_prices,
            input_hash=input_hash,
            metadata=metadata,
        )

    async def build_and_persist(
        self,
        month: str,
        universe: list[str],
        config: SnapshotConfig | None = None,
        decision_date: date | None = None,
        market: str = "XNYS",
    ) -> MonthlySnapshot:
        """Build snapshot and save metadata to artifacts."""
        snapshot = await self.build(
            month, universe, config, decision_date=decision_date, market=market
        )

        artifacts_root = config.artifacts_dir if config is not None else "artifacts"
        artifacts_dir = Path(artifacts_root) / "snapshots" / month
        artifacts_dir.mkdir(parents=True, exist_ok=True)

        meta_path = artifacts_dir / "metadata.json"
        meta_path.write_text(
            json.dumps(serialize_snapshot_metadata(snapshot.metadata), indent=2, sort_keys=True)
        )
        snapshot_path = artifacts_dir / MONTHLY_SNAPSHOT_FILENAME
        snapshot_text = json.dumps(serialize_snapshot(snapshot), indent=2, sort_keys=True)
        snapshot_path.write_text(snapshot_text)
        (artifacts_dir / "snapshot.json.gz").write_bytes(gzip.compress(snapshot_text.encode("utf-8")))

        return snapshot
