"""Seed the DART diskcache from existing snapshot JSON files.

When opendart.fss.or.kr is inaccessible (e.g. WSL2 IP block), DartProvider
falls back to cached FundamentalData / FilingData objects.  This script
pre-populates that cache from any snapshots already on disk so that
builds can proceed without a live DART connection.

Usage:
    python scripts/seed_dart_cache.py
    python scripts/seed_dart_cache.py --snapshots-dir artifacts/snapshots
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

import diskcache
from eit_market_data.kr.dart_provider import _DART_CACHE_DIR, _DOC_TTL, _FINSTATE_TTL
from eit_market_data.schemas.snapshot import FilingData, FundamentalData


def _load_snapshots(snapshots_dir: Path) -> list[dict]:
    snapshots = []
    for path in sorted(snapshots_dir.glob("*/snapshot.json")):
        try:
            with path.open() as f:
                snapshots.append(json.load(f))
        except Exception as e:
            print(f"  skip {path}: {e}")
    return snapshots


def seed(snapshots_dir: Path) -> None:
    seed_into_cache(snapshots_dir, _DART_CACHE_DIR)


def seed_into_cache(snapshots_dir: Path, cache_dir: Path) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache = diskcache.Cache(str(cache_dir))

    snapshots = _load_snapshots(snapshots_dir)
    if not snapshots:
        print("No snapshot.json files found — nothing to seed.")
        return

    fundamentals_seeded = 0
    filings_seeded = 0

    for snap in snapshots:
        decision_date = snap.get("decision_date", "")
        month = decision_date[:7] if decision_date else ""

        for ticker, fund_dict in snap.get("fundamentals", {}).items():
            try:
                fund = FundamentalData.model_validate(fund_dict)
            except Exception:
                continue
            if not fund.quarters:
                continue
            cache_key = f"fundamental:{ticker}:{month.replace('-', '')}"
            existing = cache.get(cache_key)
            if existing is None:
                cache.set(cache_key, fund, expire=_FINSTATE_TTL)
                fundamentals_seeded += 1

        for ticker, filing_dict in snap.get("filings", {}).items():
            try:
                filing = FilingData.model_validate(filing_dict)
            except Exception:
                continue
            if not filing.business_overview:
                continue
            cache_key = f"filing:{ticker}:{month.replace('-', '')}"
            existing = cache.get(cache_key)
            if existing is None:
                cache.set(cache_key, filing, expire=_DOC_TTL)
                filings_seeded += 1

    cache.close()
    print(f"Seeded {fundamentals_seeded} fundamentals, {filings_seeded} filings from {len(snapshots)} snapshot(s).")
    print(f"Cache dir: {cache_dir}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--snapshots-dir",
        type=Path,
        default=PROJECT_ROOT / "artifacts" / "snapshots",
        help="Directory containing YYYY-MM/snapshot.json files",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        help="Override the DART diskcache directory. Defaults to EIT_DART_CACHE_DIR or repo data/dart_cache.",
    )
    args = parser.parse_args()
    seed_into_cache(args.snapshots_dir, args.cache_dir or _DART_CACHE_DIR)


if __name__ == "__main__":
    main()
