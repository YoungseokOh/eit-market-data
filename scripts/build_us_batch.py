#!/usr/bin/env python3
"""Build US snapshots monthly for S&P 500 + Nasdaq 100.

Usage:
    python scripts/build_us_batch.py --start-month 2025-01 --end-month 2025-12
    python scripts/build_us_batch.py --year 2025 --chunk-size 120
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import sys
import urllib.request
from datetime import datetime
from datetime import date
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Sequence

from io import StringIO

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from dotenv import load_dotenv

load_dotenv(PROJECT_ROOT / ".env")

from eit_market_data.snapshot import (
    SnapshotBuilder,
    SnapshotConfig,
    create_real_providers,
    _content_hash,
    config_hash,
    serialize_snapshot,
    serialize_snapshot_metadata,
)

from eit_market_data.schemas.snapshot import MonthlySnapshot, SnapshotMetadata

DEFAULT_SP500_CSV = PROJECT_ROOT / "universes" / "sp500.csv"
NDX_100_URL = "https://en.wikipedia.org/wiki/Nasdaq-100"
NDX_OFFICIAL_URL = "https://www.nasdaq.com/solutions/global-indexes/nasdaq-100/companies"
NDX_SOURCE_WIKIPEDIA = "wikipedia"
NDX_SOURCE_OFFICIAL = "official"

logger = logging.getLogger(__name__)


def _parse_month(value: str) -> tuple[int, int]:
    return tuple(map(int, value.split("-")))


def _month_range(start: str, end: str) -> list[str]:
    """Generate YYYY-MM strings from start to end (inclusive)."""
    sy, sm = _parse_month(start)
    ey, em = _parse_month(end)

    if (ey, em) < (sy, sm):
        raise ValueError("--start-month must be <= --end-month")

    out = []
    y, m = sy, sm
    while (y, m) <= (ey, em):
        out.append(f"{y:04d}-{m:02d}")
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1
    return out


def _load_csv_tickers(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        tickers: list[str] = []
        for row in reader:
            ticker = str(row.get("ticker", "")).strip().upper().replace(".", "-")
            if ticker:
                tickers.append(ticker)
    return tickers


def _download_table(url: str) -> list[Any]:
    import pandas as pd

    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as response:
        html = response.read().decode("utf-8", errors="ignore")
    tables = pd.read_html(StringIO(html))
    return tables


def _pick_nasdaq100_tickers(tables: list[Any]) -> list[str]:
    import pandas as pd

    for table in tables:
        df = table if isinstance(table, pd.DataFrame) else None
        if df is None:
            continue
        cols = [str(c).lower() for c in df.columns]
        if "ticker" not in cols:
            continue
        if not (("company" in cols) or ("security" in cols)):
            continue
        if len(df) < 90:
            continue

        ticker_col = next(c for c in df.columns if str(c).lower() == "ticker")
        tickers = df[ticker_col]
        parsed = [
            str(v).strip().replace(".", "-").upper()
            for v in tickers.tolist()
            if str(v).strip() not in {"", "Ticker"}
        ]
        if len(parsed) >= 90:
            return parsed
    raise RuntimeError("Unable to locate Nasdaq-100 table from Wikipedia output")


def _pick_nasdaq100_tickers_official(table: list[Any]) -> list[str]:
    import pandas as pd

    if not table:
        raise RuntimeError("Unable to locate Nasdaq-100 table from Nasdaq output")

    df = table[0]
    if isinstance(df, pd.DataFrame) and len(df) >= 90:
        df = df.copy()
        first_row = [str(v).strip() for v in df.iloc[0].tolist()]
        if "Symbol" in first_row and "Company Name" in first_row:
            df.columns = first_row
            df = df.iloc[1:]
        else:
            df.columns = [str(c).strip() for c in df.columns]
            if df.columns[0] == "Symbol":
                # header already present in HTML columns
                pass
            else:
                raise RuntimeError("Unexpected Nasdaq official table format")
        if "Symbol" not in df.columns:
            raise RuntimeError("Unexpected Nasdaq official table format")
        tickers = df["Symbol"].tolist()
        parsed = [
            str(v).strip().replace(".", "-").upper()
            for v in tickers
            if str(v).strip() and str(v).strip() != "Symbol"
        ]
        if len(parsed) >= 90:
            return parsed

    raise RuntimeError("Unable to locate Nasdaq-100 table from Nasdaq output")


def _resolve_sp500_tickers(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(
            f"sp500 universe file not found: {path}. "
            f"Run python scripts/backfill_all.py --refresh-sp500",
        )
    sp500 = _load_csv_tickers(path)
    logger.info("Loaded %d S&P 500 tickers from %s", len(sp500), path)
    return sp500


def _resolve_nasdaq100_tickers(ndx_source: str) -> list[str]:
    if ndx_source == NDX_SOURCE_OFFICIAL:
        try:
            import pandas as pd

            html = urllib.request.urlopen(
                urllib.request.Request(
                    NDX_OFFICIAL_URL,
                    headers={"User-Agent": "Mozilla/5.0"},
                ),
                timeout=30,
            ).read().decode("utf-8", errors="ignore")
            rows = pd.read_html(StringIO(html))
            ndx = _pick_nasdaq100_tickers_official(rows)
            source = "Nasdaq official page"
            logger.info("Loaded %d Nasdaq-100 tickers from %s", len(ndx), source)
            return ndx
        except Exception:
            logger.warning(
                "Official Nasdaq source parsing failed, fallback to Wikipedia",
            )

    rows = _download_table(NDX_100_URL)
    ndx = _pick_nasdaq100_tickers(rows)
    logger.info("Loaded %d Nasdaq-100 tickers from Wikipedia", len(ndx))
    return ndx


def _merge_universe(sp500: Sequence[str], ndx: Sequence[str]) -> list[str]:
    universe: list[str] = []
    seen: set[str] = set()
    for ticker in [*sp500, *ndx]:
        if ticker in seen:
            continue
        seen.add(ticker)
        universe.append(ticker)
    return universe


def _chunks(items: Sequence[str], chunk_size: int) -> Iterable[Sequence[str]]:
    for i in range(0, len(items), chunk_size):
        yield items[i : i + chunk_size]


def _now_iso() -> str:
    return datetime.utcnow().isoformat()


def _write_snapshot_bundle(
    month_dir: Path,
    snapshot: MonthlySnapshot,
    metadata: SnapshotMetadata,
    month: str,
    artifacts_root: Path,
    universe_size: int,
) -> None:
    month_dir.mkdir(parents=True, exist_ok=True)

    snapshot_path = month_dir / "snapshot.json"
    snapshot_json = serialize_snapshot(snapshot)
    snapshot_path.write_text(
        json.dumps(snapshot_json, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    metadata_path = month_dir / "metadata.json"
    metadata_json = serialize_snapshot_metadata(metadata)
    metadata_path.write_text(
        json.dumps(metadata_json, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    manifest_path = month_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "market": "us",
                "month": month,
                "snapshot": "snapshot.json",
                "metadata": "metadata.json",
                "summary": "summary.json",
                "created_at": metadata.created_at,
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    summary_path = month_dir / "summary.json"
    summary_path.write_text(
        json.dumps(
            {
                "status": "ok",
                "market": "us",
                "month": month,
                "universe_size": universe_size,
                "price_tickers": len(snapshot.prices),
                "fundamental_tickers": len(snapshot.fundamentals),
                "filing_tickers": len(snapshot.filings),
                "benchmark_bars": len(snapshot.benchmark_prices),
                "prices_count": {t: len(v) for t, v in snapshot.prices.items()},
                "fundamentals_count": {t: len(v.quarters) for t, v in snapshot.fundamentals.items()},
                "macro_risk_keys": len(snapshot.macro.market_risk),
                "files": {
                    "snapshot": str(snapshot_path.relative_to(artifacts_root)),
                    "metadata": str(metadata_path.relative_to(artifacts_root)),
                    "manifest": str(manifest_path.relative_to(artifacts_root)),
                    "summary": str(summary_path.relative_to(artifacts_root)),
                },
            },
            sort_keys=True,
            indent=2,
        ),
        encoding="utf-8",
    )


async def _build_chunked_snapshot(
    month: str,
    universe: Sequence[str],
    builder: SnapshotBuilder,
    config: SnapshotConfig,
    *,
    chunk_size: int,
) -> MonthlySnapshot:
    if chunk_size <= 0 or chunk_size >= len(universe):
        return await builder.build(month=month, universe=list(universe), config=config)

    chunk_snaps: list[MonthlySnapshot] = []
    for idx, chunk in enumerate(_chunks(universe, chunk_size), start=1):
        logger.info(
            "[%s] chunk %d/%d: %d tickers",
            month,
            idx,
            (len(universe) - 1) // chunk_size + 1,
            len(chunk),
        )
        snapshot = await builder.build(month=month, universe=list(chunk), config=config)
        chunk_snaps.append(snapshot)

    merged = _merge_snapshots(chunk_snaps)

    # Recompute sector averages across the full merged universe.
    sector_map: dict[str, str] = merged.sector_map
    sector_tickers: dict[str, list[str]] = defaultdict(list)
    for ticker, sector in sector_map.items():
        sector_tickers[sector].append(ticker)

    sector_avg_tasks = {
        sector: builder.sector.fetch_sector_averages(
            sector,
            tickers,
            merged.decision_date,
        )
        for sector, tickers in sector_tickers.items()
    }
    all_avgs = await asyncio.gather(*sector_avg_tasks.values())
    sector_averages = dict(zip(sector_avg_tasks.keys(), all_avgs, strict=True))

    return MonthlySnapshot(
        decision_date=merged.decision_date,
        execution_date=merged.execution_date,
        universe=merged.universe,
        prices=merged.prices,
        fundamentals=merged.fundamentals,
        filings=merged.filings,
        news=merged.news,
        macro=merged.macro,
        sector_map=merged.sector_map,
        sector_averages=sector_averages,
        benchmark_prices=merged.benchmark_prices,
        input_hash=_content_hash(
            {"decision_date": str(merged.decision_date), "universe": sorted(merged.universe)}
        ),
        metadata=merged.metadata,
    )


def _merge_snapshots(snaps: list[MonthlySnapshot]) -> MonthlySnapshot:
    if not snaps:
        raise ValueError("No chunk snapshots to merge")

    first = snaps[0]
    universe: list[str] = []
    seen: set[str] = set()
    prices = {}
    fundamentals = {}
    filings = {}
    news = {}
    sector_map = {}
    sector_averages = {}

    for snap in snaps:
        for ticker in snap.universe:
            if ticker not in seen:
                universe.append(ticker)
                seen.add(ticker)
        prices.update(snap.prices)
        fundamentals.update(snap.fundamentals)
        filings.update(snap.filings)
        news.update(snap.news)
        sector_map.update(snap.sector_map)
        sector_averages.update(snap.sector_averages)

    metadata = SnapshotMetadata(
        created_at=first.metadata.created_at,
        config_hash=first.metadata.config_hash,
        price_hash=_content_hash({t: len(bars) for t, bars in prices.items()}),
        fundamental_hash=_content_hash({t: len(data.quarters) for t, data in fundamentals.items()}),
        filing_hash=_content_hash({t: bool(data.business_overview or data.risks or data.mda or data.governance) for t, data in filings.items()}),
        news_hash=first.metadata.news_hash,
        macro_hash=_content_hash(first.macro.model_dump()),
    )

    return MonthlySnapshot(
        decision_date=first.decision_date,
        execution_date=first.execution_date,
        universe=universe,
        prices=prices,
        fundamentals=fundamentals,
        filings=filings,
        news=news,
        macro=first.macro,
        sector_map=sector_map,
        sector_averages=sector_averages,
        benchmark_prices=first.benchmark_prices,
        input_hash=_content_hash({"decision_date": str(first.decision_date), "universe": sorted(universe)}),
        metadata=metadata,
    )


async def build_month_range(
    *,
    start_month: str,
    end_month: str,
    out_root: Path,
    sp500_csv: Path,
    month_batch_size: int,
    month_batch_index: int,
    chunk_size: int,
    ndx_source: str,
    as_of_only: bool,
    pause_seconds: float,
    fail_fast: bool,
) -> None:
    sp500 = _resolve_sp500_tickers(sp500_csv)
    ndx100 = _resolve_nasdaq100_tickers(ndx_source)

    sp_set = set(sp500)
    ndx_set = set(ndx100)
    overlap = sp_set & ndx_set
    logger.info(
        "Universe stats: SP500=%d, NDX=%d, overlap=%d, merged=%d",
        len(sp_set),
        len(ndx_set),
        len(overlap),
        len(sp_set | ndx_set),
    )
    logger.info("NDX-only candidates (%d): %s", len(ndx_set - sp_set), sorted(list(ndx_set - sp_set))[:30])

    universe = _merge_universe(
        sp500,
        ndx100,
    )

    # Optional skip current month/future months for stability.
    months = _month_range(start_month, end_month)
    if as_of_only:
        today = date.today()
        current = today.strftime("%Y-%m")
        months = [m for m in months if m < current]

    if month_batch_size > 0:
        if month_batch_index < 0:
            raise ValueError("--month-batch-index must be >= 0")
        start_idx = month_batch_index * month_batch_size
        months = months[start_idx : start_idx + month_batch_size]
        logger.info(
            "Month split mode: batch_size=%d batch_index=%d => %d months",
            month_batch_size,
            month_batch_index,
            len(months),
        )

    providers = create_real_providers()
    builder = SnapshotBuilder(**providers)
    config = SnapshotConfig(artifacts_dir=str(out_root))
    month_count = len(months)

    logger.info(
        "US batch target: %d months, %d tickers, chunk_size=%d",
        month_count,
        len(universe),
        chunk_size,
    )

    for i, month in enumerate(months, start=1):
        month_dir = out_root / "us" / "snapshots" / month
        if month_dir.exists() and (month_dir / "snapshot.json").exists():
            logger.info("[%s] (%d/%d) already exists, skip", month, i, month_count)
            continue

        try:
            logger.info("[%s] START %d/%d (%d tickers)", month, i, month_count, len(universe))
            snapshot = await _build_chunked_snapshot(
                month=month,
                universe=universe,
                builder=builder,
                config=config,
                chunk_size=chunk_size,
            )

            metadata = SnapshotMetadata(
                created_at=_now_iso(),
                config_hash=config_hash(config),
                price_hash=_content_hash({t: len(v) for t, v in snapshot.prices.items()}),
                fundamental_hash=_content_hash({t: len(f.quarters) for t, f in snapshot.fundamentals.items()}),
                filing_hash=_content_hash({t: bool(f.business_overview or f.risks or f.mda or f.governance) for t, f in snapshot.filings.items()}),
                news_hash=_content_hash({t: len(v) for t, v in snapshot.news.items()}) if snapshot.news else "",
                macro_hash=_content_hash(snapshot.macro.model_dump()),
            )

            merged_snapshot = snapshot.model_copy(
                update={
                    "metadata": metadata,
                    "input_hash": _content_hash(
                        {
                            "decision_date": str(snapshot.decision_date),
                            "universe": sorted(snapshot.universe),
                        }
                    ),
                }
            )
            _write_snapshot_bundle(
                month_dir=month_dir,
                snapshot=merged_snapshot,
                metadata=metadata,
                month=month,
                artifacts_root=out_root,
                universe_size=len(merged_snapshot.universe),
            )
            logger.info("[%s] DONE", month)
            if pause_seconds > 0 and i < month_count:
                logger.info("sleep %.1fs before next month", pause_seconds)
                await asyncio.sleep(pause_seconds)
        except Exception as exc:
            logger.exception("[%s] FAIL: %s", month, exc)
            month_dir.mkdir(parents=True, exist_ok=True)
            (month_dir / "summary.json").write_text(
                json.dumps({"status": "failed", "month": month, "error": str(exc)}),
                encoding="utf-8",
            )
            if fail_fast:
                raise


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build US snapshots monthly for S&P 500 + Nasdaq-100.",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--year", type=int, help="Collect full year, e.g. --year 2025")
    group.add_argument("--start-month", help="Start month in YYYY-MM format")
    parser.add_argument("--end-month", help="End month in YYYY-MM format")

    parser.add_argument(
        "--artifacts-root",
        default=str(PROJECT_ROOT / "artifacts"),
        help="Output root for snapshots (default: ./artifacts)",
    )
    parser.add_argument(
        "--sp500-csv",
        default=str(DEFAULT_SP500_CSV),
        help="Path to S&P 500 universe CSV",
    )
    parser.add_argument(
        "--ndx-source",
        default=NDX_SOURCE_WIKIPEDIA,
        choices=[NDX_SOURCE_WIKIPEDIA, NDX_SOURCE_OFFICIAL],
        help="Nasdaq-100 source. wikipedia gives 516 union, official gives 517 union.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=120,
        help="Split US universe into chunks for stability (0=disable)",
    )
    parser.add_argument(
        "--month-delay",
        type=float,
        default=0.0,
        help="Pause between months (seconds)",
    )
    parser.add_argument(
        "--month-batch-size",
        type=int,
        default=0,
        help="Optional split mode: process only a fixed number of months (0 = all)",
    )
    parser.add_argument(
        "--month-batch-index",
        type=int,
        default=0,
        help="Zero-based index for split mode (--month-batch-size must be > 0)",
    )
    parser.add_argument(
        "--as-of-only",
        action="store_true",
        help="Skip current/future months",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop on first month failure",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    if args.year:
        start_month = f"{args.year:04d}-01"
        end_month = f"{args.year:04d}-12"
    else:
        if not args.end_month:
            parser.error("--end-month is required when --start-month is used")
        start_month = args.start_month
        end_month = args.end_month

    asyncio.run(
        build_month_range(
            start_month=start_month,
            end_month=end_month,
            out_root=Path(args.artifacts_root),
            sp500_csv=Path(args.sp500_csv),
            month_batch_size=args.month_batch_size,
            month_batch_index=args.month_batch_index,
            chunk_size=args.chunk_size,
            ndx_source=args.ndx_source,
            as_of_only=args.as_of_only,
            pause_seconds=args.month_delay,
            fail_fast=args.fail_fast,
        )
    )


if __name__ == "__main__":
    main()
