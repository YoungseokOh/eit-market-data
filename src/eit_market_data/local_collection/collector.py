"""KR snapshot collector with checkpoint validation."""

from __future__ import annotations

import asyncio
import gzip
import json
import logging
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any

from eit_market_data.core.hashing import _content_hash
from eit_market_data.core.sector_math import compute_sector_averages
from eit_market_data.kr.ci_safe_provider import NullMacroProvider
from eit_market_data.kr.dart_provider import DartProvider
from eit_market_data.kr.ecos_provider import EcosMacroProvider
from eit_market_data.kr.fundamental_provider import CompositeKrFundamentalProvider
from eit_market_data.kr.pykrx_provider import PykrxProvider
from eit_market_data.local_collection.cache_only_dart import (
    CacheOnlyDartProvider,
    _is_dart_transient_error,
)
from eit_market_data.local_collection.progress import (
    _write_json,
    load_progress,
    load_tickers,
    save_progress,
)
from eit_market_data.local_collection.state import (
    BatchPayload,
    CheckpointPolicy,
    KrCollectionState,
    _load_batch_payload,
    _serialize_batch,
)
from eit_market_data.local_collection.validation import (
    raise_for_failed_checks,
    summarize_checks,
    validate_kr_checkpoint,
    validate_kr_final_snapshot,
)
from eit_market_data.schemas.snapshot import (
    FilingData,
    FundamentalData,
    MacroData,
    MonthlySnapshot,
    PriceBar,
    SectorAverages,
    SnapshotMetadata,
)
from eit_market_data.snapshot import (
    SnapshotConfig,
    config_hash,
    serialize_snapshot,
    serialize_snapshot_metadata,
)

logger = logging.getLogger(__name__)


def _next_kr_execution_date(as_of: date) -> date:
    """Return the next KRX (XKRX) trading day strictly after ``as_of``.

    Uses the curated, holiday-aware exchange calendar in
    :mod:`eit_market_data.core.calendar` so that ``execution_date`` never lands
    on a market holiday (Seollal/Chuseok, year-end closure, substitute
    holidays, etc.). This replaces the previous brittle live-pykrx business-day
    lookup that fell back to a weekend-only calendar and produced non-trading
    execution dates (e.g. 2024-12-31, 2024-10-01 개천절, 2023-09-28 추석).
    """
    from eit_market_data.core.calendar import _next_business_day

    return _next_business_day(as_of, "XKRX")


def compute_sector_averages_from_state(
    sector_map: dict[str, str],
    fundamentals: dict[str, FundamentalData],
) -> dict[str, SectorAverages]:
    grouped: dict[str, list[FundamentalData]] = {}
    for ticker, sector in sector_map.items():
        grouped.setdefault(sector or "General", []).append(
            fundamentals.get(ticker, FundamentalData(ticker=ticker))
        )

    return {
        sector: compute_sector_averages(sector, funds)
        for sector, funds in grouped.items()
    }


class LocalKrCollector:
    """Collect KR snapshot data with checkpoint validation."""

    def __init__(
        self,
        *,
        as_of: date,
        storage_root: Path,
        bundle_root: Path,
        partial_root: Path,
        checkpoint_root: Path,
        policy: CheckpointPolicy,
        progress_path: Path,
        dart_mode: str = "live",
    ) -> None:
        self.as_of = as_of
        self.month = as_of.strftime("%Y-%m")
        self.storage_root = storage_root
        self.bundle_root = bundle_root
        self.partial_root = partial_root
        self.checkpoint_root = checkpoint_root
        self.policy = policy
        self.progress_path = progress_path
        self.inter_ticker_delay_seconds = 0.0 if dart_mode == "cache_only" else 4.0
        if dart_mode == "cache_only":
            self.dart = CacheOnlyDartProvider()
        else:
            self.dart = DartProvider(allow_stale_fallback=False, raise_on_error=True)
        self.pykrx = PykrxProvider(official_only=True)
        self.fundamentals = CompositeKrFundamentalProvider(
            dart_provider=self.dart,
            price_provider=self.pykrx,
            raise_on_error=True,
        )
        self.pykrx._fundamental_provider = self.fundamentals
        try:
            self.macro_provider: Any = EcosMacroProvider()
        except Exception:
            self.macro_provider = NullMacroProvider()

    def _switch_dart_cache_only(self, reason: Exception) -> None:
        if isinstance(self.dart, CacheOnlyDartProvider):
            return
        logger.warning("Switching DART provider to cache-only mode: %s", reason)
        self.dart = CacheOnlyDartProvider()
        self.fundamentals = CompositeKrFundamentalProvider(
            dart_provider=self.dart,
            price_provider=self.pykrx,
            raise_on_error=True,
        )
        self.pykrx._fundamental_provider = self.fundamentals

    async def _fetch_fundamentals_with_fallback(self, ticker: str) -> FundamentalData:
        try:
            return await self.fundamentals.fetch_fundamentals(
                ticker,
                self.as_of,
                n_quarters=8,
            )
        except Exception as exc:
            logger.warning(
                "DART fundamentals unavailable for %s as of %s: %s",
                ticker,
                self.as_of,
                exc,
            )
            if _is_dart_transient_error(exc):
                self._switch_dart_cache_only(exc)
                return await self.fundamentals.fetch_fundamentals(
                    ticker,
                    self.as_of,
                    n_quarters=8,
                )

            cache_only_fundamentals = CompositeKrFundamentalProvider(
                dart_provider=CacheOnlyDartProvider(),
                price_provider=self.pykrx,
                raise_on_error=True,
            )
            return await cache_only_fundamentals.fetch_fundamentals(
                ticker,
                self.as_of,
                n_quarters=8,
            )

    async def collect(
        self,
        *,
        universe_csv: Path,
        resume: bool,
    ) -> dict[str, Any]:
        tickers = load_tickers(universe_csv)
        self.bundle_root.mkdir(parents=True, exist_ok=True)
        self.partial_root.mkdir(parents=True, exist_ok=True)
        self.checkpoint_root.mkdir(parents=True, exist_ok=True)

        progress = load_progress(
            self.progress_path,
            {
                "as_of": self.as_of.isoformat(),
                "month": self.month,
                "next_index": 0,
                "completed_batches": 0,
                "completed_tickers": 0,
            },
        )
        state = KrCollectionState()
        next_index = 0

        if resume:
            batch_files = sorted(self.partial_root.glob("batch_*.json"))
            for batch_file in batch_files:
                state.merge(_load_batch_payload(batch_file))
            # Older partial batches may still contain news payloads.
            state.news.clear()
            state.news_audit.clear()
            state.news_coverage.clear()
            next_index = int(progress.get("next_index", 0) or 0)

        sector_map = await self.pykrx.fetch_sector_map(tickers, as_of=self.as_of)
        last_checkpoint_at = time.monotonic()
        batch_no = int(progress.get("completed_batches", 0) or 0)
        pending = BatchPayload(
            tickers=[],
            prices={},
            fundamentals={},
            filings={},
            news={},
            news_audit={},
            news_coverage={},
        )

        while next_index + len(pending.tickers) < len(tickers):
            ticker = tickers[next_index + len(pending.tickers)]
            payload = await self._collect_ticker(ticker)
            pending.merge(payload)
            preview = KrCollectionState(
                prices=dict(state.prices),
                fundamentals=dict(state.fundamentals),
                filings=dict(state.filings),
                news=dict(state.news),
                news_audit=dict(state.news_audit),
                news_coverage=dict(state.news_coverage),
            )
            preview.merge(pending)
            should_checkpoint = (
                len(pending.tickers) >= self.policy.every_tickers
                or time.monotonic() - last_checkpoint_at >= self.policy.every_seconds
                or next_index + len(pending.tickers) >= len(tickers)
            )
            if not should_checkpoint:
                continue

            checks = await validate_kr_checkpoint(
                state=preview,
                as_of=self.as_of,
            )
            batch_no += 1
            report_path = self.checkpoint_root / f"batch_{batch_no:04d}.json"
            _write_json(
                report_path,
                {
                    "stage": "kr_bundle_checkpoint",
                    "batch": batch_no,
                    "processed_tickers": next_index + len(pending.tickers),
                    "tickers": pending.tickers,
                    "summary": summarize_checks(checks),
                },
            )
            raise_for_failed_checks("kr_bundle_checkpoint", checks, report_path)

            batch_path = self.partial_root / f"batch_{batch_no:04d}.json"
            _write_json(batch_path, _serialize_batch(pending))
            state.merge(pending)
            next_index += len(pending.tickers)
            progress.update(
                {
                    "next_index": next_index,
                    "completed_batches": batch_no,
                    "completed_tickers": next_index,
                    "last_checkpoint_report": str(report_path),
                }
            )
            save_progress(self.progress_path, progress)
            last_checkpoint_at = time.monotonic()
            pending = BatchPayload(
                tickers=[],
                prices={},
                fundamentals={},
                filings={},
                news={},
                news_audit={},
                news_coverage={},
            )

        macro = await self.macro_provider.fetch_macro(self.as_of)
        benchmark_prices = await self.pykrx.fetch_benchmark(self.as_of)
        benchmark_tr_prices = await self.pykrx.fetch_benchmark_tr(self.as_of)
        sector_averages = compute_sector_averages_from_state(sector_map, state.fundamentals)

        snapshot = self._build_snapshot(
            tickers=tickers,
            state=state,
            sector_map=sector_map,
            sector_averages=sector_averages,
            macro=macro,
            benchmark_prices=benchmark_prices,
            benchmark_tr_prices=benchmark_tr_prices,
        )
        summary = self._persist_snapshot(snapshot, state)
        final_checks = validate_kr_final_snapshot(
            snapshot=snapshot,
            news_audit=state.news_audit,
            news_coverage=state.news_coverage,
            as_of=self.as_of,
        )
        final_report_path = self.bundle_root / "snapshots" / self.month / "validation_report.json"
        _write_json(
            final_report_path,
            {
                "stage": "kr_bundle_final",
                "summary": summarize_checks(final_checks),
            },
        )
        raise_for_failed_checks("kr_bundle_final", final_checks, final_report_path)
        progress["status"] = "completed"
        progress["final_summary_path"] = summary["summary_path"]
        save_progress(self.progress_path, progress)
        return summary

    async def _collect_ticker(self, ticker: str) -> BatchPayload:
        payload = BatchPayload(
            tickers=[ticker],
            prices={},
            fundamentals={},
            filings={},
            news={},
            news_audit={},
            news_coverage={},
        )
        prices = await self.pykrx.fetch_prices(ticker, self.as_of, lookback_days=300)
        fundamentals = await self._fetch_fundamentals_with_fallback(ticker)
        try:
            filing = await self.dart.fetch_filing(ticker, self.as_of)
        except Exception as exc:
            logger.warning("DART filing unavailable for %s as of %s: %s", ticker, self.as_of, exc)
            filing = FilingData(ticker=ticker)
        payload.prices[ticker] = prices
        payload.fundamentals[ticker] = fundamentals
        payload.filings[ticker] = filing
        if self.inter_ticker_delay_seconds > 0:
            await asyncio.sleep(self.inter_ticker_delay_seconds)
        return payload

    def _build_snapshot(
        self,
        *,
        tickers: list[str],
        state: KrCollectionState,
        sector_map: dict[str, str],
        sector_averages: dict[str, SectorAverages],
        macro: MacroData,
        benchmark_prices: list[PriceBar],
        benchmark_tr_prices: list[PriceBar] | None = None,
    ) -> MonthlySnapshot:
        execution_date = _next_kr_execution_date(self.as_of)

        metadata = SnapshotMetadata(
            created_at=datetime.utcnow().isoformat(),
            config_hash=config_hash(SnapshotConfig(artifacts_dir=str(self.bundle_root))),
            price_hash=_content_hash({ticker: len(items) for ticker, items in state.prices.items()}),
            fundamental_hash=_content_hash(
                {ticker: len(item.quarters) for ticker, item in state.fundamentals.items()}
            ),
            filing_hash=_content_hash(
                {ticker: bool(item.business_overview) for ticker, item in state.filings.items()}
            ),
            news_hash="",
            macro_hash=_content_hash(macro.model_dump(mode="json")),
        )

        return MonthlySnapshot(
            decision_date=self.as_of,
            execution_date=execution_date,
            universe=tickers,
            prices=state.prices,
            fundamentals=state.fundamentals,
            filings=state.filings,
            macro=macro,
            sector_map=sector_map,
            sector_averages=sector_averages,
            benchmark_prices=benchmark_prices,
            benchmark_tr_prices=benchmark_tr_prices or [],
            input_hash=_content_hash({"decision_date": self.as_of.isoformat(), "universe": tickers}),
            metadata=metadata,
        )

    def _persist_snapshot(self, snapshot: MonthlySnapshot, state: KrCollectionState) -> dict[str, Any]:
        month_dir = self.bundle_root / "snapshots" / self.month
        month_dir.mkdir(parents=True, exist_ok=True)
        snapshot_path = month_dir / "snapshot.json"
        metadata_path = month_dir / "metadata.json"
        manifest_path = month_dir / "manifest.json"
        summary_path = month_dir / "summary.json"
        gzip_path = month_dir / "snapshot.json.gz"

        snapshot_text = json.dumps(serialize_snapshot(snapshot), indent=2, sort_keys=True)
        snapshot_path.write_text(snapshot_text, encoding="utf-8")
        gzip_path.write_bytes(gzip.compress(snapshot_text.encode("utf-8")))
        metadata_path.write_text(
            json.dumps(serialize_snapshot_metadata(snapshot.metadata), indent=2, sort_keys=True),
            encoding="utf-8",
        )

        manifest = {
            "market": "kr",
            "month": self.month,
            "decision_date": snapshot.decision_date.isoformat(),
            "execution_date": snapshot.execution_date.isoformat(),
            "files": {
                "snapshot": "snapshot.json",
                "snapshot_gzip": "snapshot.json.gz",
                "metadata": "metadata.json",
                "summary": "summary.json",
            },
        }
        _write_json(manifest_path, manifest)

        summary = {
            "status": "ok",
            "market": "kr",
            "month": self.month,
            "decision_date": snapshot.decision_date.isoformat(),
            "execution_date": snapshot.execution_date.isoformat(),
            "universe_size": len(snapshot.universe),
            "price_tickers": len(snapshot.prices),
            "fundamental_tickers": len(snapshot.fundamentals),
            "filing_tickers": len(snapshot.filings),
            "benchmark_bars": len(snapshot.benchmark_prices),
            "snapshot_path": str(snapshot_path),
            "summary_path": str(summary_path),
        }
        _write_json(summary_path, summary)
        return summary
