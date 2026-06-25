#!/usr/bin/env python3
"""Build US market data snapshot for a given month.

Usage:
    python scripts/build_us_snapshot.py --as-of 2026-02-27

This builds a complete US snapshot (YFinance + FRED + EDGAR) and saves it as:
    artifacts/[market-subdir/]
    └── snapshots/YYYY-MM/snapshot.json
        metadata.json
        manifest.json
        summary.json
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import bootstrap, write_json

PROJECT_ROOT = bootstrap()

from eit_market_data.snapshot import (
    SnapshotBuilder,
    SnapshotConfig,
    create_real_providers,
    serialize_snapshot,
    serialize_snapshot_metadata,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path)


async def build_us_snapshot(
    as_of: date,
    universe: list[str],
    artifacts_root: Path | None = None,
    market_subdir: str = "",
) -> dict[str, object]:
    """Build US snapshot and save outputs.

    Args:
        as_of: Decision date (point-in-time)
        universe: List of tickers (e.g., ["AAPL", "MSFT"])
        artifacts_root: Output directory (default: PROJECT_ROOT/artifacts)
        market_subdir: Optional market subdirectory (e.g. "us")

    Returns:
        Summary dict with status and paths
    """
    artifacts_root = artifacts_root or PROJECT_ROOT / "artifacts"
    effective_root = artifacts_root / market_subdir if market_subdir else artifacts_root
    month_dir = effective_root / "snapshots" / as_of.strftime("%Y-%m")
    month_dir.mkdir(parents=True, exist_ok=True)

    logger.info(
        f"Building US snapshot: as_of={as_of}, tickers={universe}, "
        f"output_dir={month_dir}"
    )

    try:
        # Create providers (YFinance + FRED + EDGAR)
        providers = create_real_providers()
        builder = SnapshotBuilder(**providers)
        config = SnapshotConfig(artifacts_dir=str(effective_root))

        # Build snapshot. Pin decision_date to the exact as_of so mid-month
        # runs are not mislabeled as month-end (point-in-time correctness,
        # mirroring scripts/build_kr_snapshot.py).
        month_str = as_of.strftime("%Y-%m")
        snapshot = await builder.build(
            month=month_str,
            universe=universe,
            config=config,
            decision_date=as_of,
        )

        logger.info(f"Snapshot built: {len(universe)} tickers, decision_date={snapshot.decision_date}")

        # Save snapshot.json
        snapshot_path = month_dir / "snapshot.json"
        snapshot_json = serialize_snapshot(snapshot)
        write_json(snapshot_path, snapshot_json)
        logger.info(f"Saved snapshot.json: {snapshot_path}")

        # Save metadata.json
        metadata_path = month_dir / "metadata.json"
        metadata_json = {
            "version": "1.0",
            "market": "us",
            "decision_date": str(snapshot.decision_date),
            "execution_date": str(snapshot.execution_date),
            "universe": snapshot.universe,
            "providers": ["YFinanceProvider", "FredMacroProvider", "EdgarFilingProvider"],
            "snapshot_metadata": serialize_snapshot_metadata(snapshot.metadata),
        }
        write_json(metadata_path, metadata_json)
        logger.info(f"Saved metadata.json: {metadata_path}")

        # Save manifest.json (for eit-research)
        manifest_path = month_dir / "manifest.json"
        manifest = {
            "market": "us",
            "month": month_str,
            "snapshot": "snapshot.json",
            "metadata": "metadata.json",
            "summary": "summary.json",
            "created_at": snapshot.metadata.created_at,
        }
        write_json(manifest_path, manifest)
        logger.info(f"Saved manifest.json: {manifest_path}")

        # Save summary.json
        summary_path = month_dir / "summary.json"
        summary = {
            "status": "ok",
            "market": "us",
            "as_of": as_of.isoformat(),
            "universe": universe,
            "prices_count": {t: len(snapshot.prices.get(t, [])) for t in universe},
            "fundamentals_count": {
                t: len(snapshot.fundamentals.get(t, {}).quarters or []) for t in universe
            },
            "macro_keys": {
                "rates_policy": len(snapshot.macro.rates_policy),
                "inflation_commodities": len(snapshot.macro.inflation_commodities),
                "growth_economy": len(snapshot.macro.growth_economy),
                "market_risk": len(snapshot.macro.market_risk),
            },
            "files": {
                "snapshot": _display_path(snapshot_path),
                "metadata": _display_path(metadata_path),
                "manifest": _display_path(manifest_path),
                "summary": _display_path(summary_path),
            },
        }
        write_json(summary_path, summary)
        logger.info(f"Saved summary.json: {summary_path}")

        logger.info(f"✅ US snapshot built successfully: {month_str}")
        return {
            "status": "ok",
            "as_of": as_of.isoformat(),
            "universe": universe,
            "snapshot_dir": str(month_dir),
            "summary": summary,
        }

    except Exception as e:
        logger.error(f"❌ Failed to build US snapshot: {e}", exc_info=True)
        return {
            "status": "failed",
            "as_of": as_of.isoformat(),
            "error": str(e),
        }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build US market data snapshot for a given month."
    )
    parser.add_argument(
        "--as-of",
        required=True,
        help="Reference date in YYYY-MM-DD (decision date, typically month-end).",
    )
    parser.add_argument(
        "--universe",
        default="AAPL,MSFT,GOOGL,AMZN,NVDA",
        help="Comma-separated list of tickers (default: top 5 US equities).",
    )
    parser.add_argument(
        "--artifacts-root",
        help="Output directory for snapshots (default: PROJECT_ROOT/artifacts).",
    )
    parser.add_argument(
        "--market-subdir",
        default="",
        help="Optional market subdirectory under snapshots/ (e.g. 'us').",
    )

    args = parser.parse_args()

    as_of = date.fromisoformat(args.as_of)
    universe = [t.strip() for t in args.universe.split(",")]
    artifacts_root = Path(args.artifacts_root) if args.artifacts_root else None

    exit_code = asyncio.run(
        build_us_snapshot(
            as_of=as_of,
            universe=universe,
            artifacts_root=artifacts_root,
            market_subdir=args.market_subdir,
        )
    )

    if isinstance(exit_code, dict) and exit_code.get("status") == "ok":
        print(f"[SUMMARY] status=ok as_of={exit_code['as_of']} snapshot_dir={exit_code['snapshot_dir']}")
        sys.exit(0)
    else:
        print(f"[SUMMARY] status=failed as_of={as_of}")
        sys.exit(1)


if __name__ == "__main__":
    main()
