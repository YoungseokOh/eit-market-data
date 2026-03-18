from __future__ import annotations

import argparse
import asyncio
import csv
import json
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

UNIVERSE_CSV = PROJECT_ROOT / "universes/kr_universe.csv"
ARTIFACTS_ROOT = PROJECT_ROOT / "artifacts"
MONTHLY_SNAPSHOT_FILENAME = "snapshot.json"


def load_universe_tickers(path: Path) -> list[str]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return [
            str(row["ticker"]).strip().zfill(6)
            for row in reader
            if row.get("ticker") and str(row["ticker"]).strip()
        ]


def _last_business_day(year: int, month: int) -> date:
    if month == 12:
        last = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        last = date(year, month + 1, 1) - timedelta(days=1)
    while last.weekday() >= 5:
        last -= timedelta(days=1)
    return last


def should_build_monthly_snapshot(as_of: date) -> bool:
    return as_of == _last_business_day(as_of.year, as_of.month)


async def build_snapshot(
    as_of: date,
    universe_csv: Path,
    artifacts_root: Path,
    profile: str,
    market_subdir: str = "",
) -> dict[str, object]:
    from eit_market_data.snapshot import SnapshotBuilder, SnapshotConfig, create_kr_providers

    month = as_of.strftime("%Y-%m")
    tickers = load_universe_tickers(universe_csv)

    # When market_subdir is set, nest artifacts under that subdirectory
    effective_root = artifacts_root / market_subdir if market_subdir else artifacts_root

    builder = SnapshotBuilder(
        **create_kr_providers(profile=profile, universe_csv=universe_csv)
    )
    snapshot = await builder.build_and_persist(
        month,
        tickers,
        SnapshotConfig(artifacts_dir=str(effective_root)),
    )

    snapshot_dir = effective_root / "snapshots" / month
    field_coverage = _field_coverage(snapshot)
    manifest = {
        "bundle_version": 1,
        "month": month,
        "decision_date": snapshot.decision_date.isoformat(),
        "execution_date": snapshot.execution_date.isoformat(),
        "source_profile": profile,
        "universe_csv": str(universe_csv),
        "field_coverage": field_coverage,
        "warnings": _manifest_warnings(profile, field_coverage),
        "files": {
            "snapshot": MONTHLY_SNAPSHOT_FILENAME,
            "snapshot_gzip": "snapshot.json.gz",
            "metadata": "metadata.json",
            "summary": "summary.json",
        },
    }
    (snapshot_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    summary = _summary_payload(snapshot, month, profile, effective_root, field_coverage)
    (snapshot_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return summary


def _summary_payload(
    snapshot,
    month: str,
    profile: str,
    artifacts_root: Path,
    field_coverage: dict[str, Any] | None = None,
) -> dict[str, object]:  # noqa: ANN001
    coverage = field_coverage or _field_coverage(snapshot)
    snapshot_dir = artifacts_root / "snapshots" / month
    return {
        "status": "ok",
        "month": month,
        "decision_date": snapshot.decision_date.isoformat(),
        "execution_date": snapshot.execution_date.isoformat(),
        "universe_size": len(snapshot.universe),
        "price_tickers": len(snapshot.prices),
        "fundamental_tickers": len(snapshot.fundamentals),
        "filing_tickers": len(snapshot.filings),
        "sector_count": len(snapshot.sector_averages),
        "benchmark_bars": len(snapshot.benchmark_prices),
        "source_profile": profile,
        "snapshot_path": str((snapshot_dir / MONTHLY_SNAPSHOT_FILENAME).relative_to(artifacts_root)),
        "manifest_path": str((snapshot_dir / "manifest.json").relative_to(artifacts_root)),
        "metadata_path": str((snapshot_dir / "metadata.json").relative_to(artifacts_root)),
    }


def _field_coverage(snapshot) -> dict[str, Any]:  # noqa: ANN001, ANN401
    universe_size = len(snapshot.universe)
    fundamentals = list(snapshot.fundamentals.values())
    return {
        "universe_size": universe_size,
        "prices": sum(1 for bars in snapshot.prices.values() if bars),
        "fundamentals": sum(1 for item in fundamentals if item.quarters),
        "filings": sum(
            1
            for item in snapshot.filings.values()
            if item.business_overview or item.risks or item.mda or item.governance
        ),
        "last_close_price": sum(1 for item in fundamentals if item.last_close_price is not None),
        "market_cap": sum(1 for item in fundamentals if item.market_cap is not None),
        "sector_map": len(snapshot.sector_map),
        "benchmark_bars": len(snapshot.benchmark_prices),
    }


def _manifest_warnings(profile: str, coverage: dict[str, Any]) -> list[str]:
    warnings: list[str] = []
    if profile == "ci_safe":
        warnings.extend(
            [
                "benchmark_prices are optional in ci_safe profile",
                "market_cap and issued_shares may be null in ci_safe profile",
                "sector_map is sourced from the universe CSV seed sector",
            ]
        )
    if coverage.get("market_cap", 0) == 0:
        warnings.append("market_cap coverage is zero")
    if coverage.get("benchmark_bars", 0) == 0:
        warnings.append("benchmark coverage is zero")
    return warnings


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a KR monthly snapshot when month-end closes.")
    parser.add_argument("--as-of", required=True, help="Reference date in YYYY-MM-DD.")
    parser.add_argument(
        "--universe-csv",
        default=str(UNIVERSE_CSV),
        help="Universe CSV path.",
    )
    parser.add_argument(
        "--artifacts-root",
        default=str(ARTIFACTS_ROOT),
        help="Output directory for snapshot artifacts.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Build even when the reference date is not the month-end business day.",
    )
    parser.add_argument(
        "--profile",
        default="official",
        choices=["official", "official_enriched", "ci_safe"],
        help="Provider profile to use for the KR snapshot build.",
    )
    parser.add_argument(
        "--market-subdir",
        default="",
        help="Optional market subdirectory under snapshots/ (e.g. 'kr').",
    )
    args = parser.parse_args()

    as_of = date.fromisoformat(args.as_of)
    artifacts_root = Path(args.artifacts_root)
    universe_csv = Path(args.universe_csv)

    if not args.force and not should_build_monthly_snapshot(as_of):
        summary = {
            "status": "skipped",
            "reason": "not_month_end_business_day",
            "month": as_of.strftime("%Y-%m"),
            "as_of": as_of.isoformat(),
        }
        skip_dir = artifacts_root / "snapshots" / as_of.strftime("%Y-%m")
        skip_dir.mkdir(parents=True, exist_ok=True)
        (skip_dir / "summary.json").write_text(
            json.dumps(summary, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        print("[SKIP] Snapshot build skipped: not month-end business day.")
        return

    summary = asyncio.run(
        build_snapshot(as_of, universe_csv, artifacts_root, args.profile, args.market_subdir)
    )
    print(
        f"[DONE] Snapshot built month={summary['month']} "
        f"decision_date={summary['decision_date']} universe={summary['universe_size']}"
    )


if __name__ == "__main__":
    main()
