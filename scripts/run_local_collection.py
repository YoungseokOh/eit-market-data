# ruff: noqa: E402
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import bootstrap

PROJECT_ROOT = bootstrap()

from eit_market_data.local_collection import run_local_collection


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run local-only market collection with checkpoint validation.",
    )
    parser.add_argument(
        "--storage-root",
        required=True,
        help="Directory outside the repo for raw data, bundles, logs, and reports.",
    )
    parser.add_argument("--as-of", required=True, help="Reference date in YYYY-MM-DD.")
    parser.add_argument(
        "--market",
        default="both",
        choices=["kr", "us", "both"],
        help="Market scope to collect.",
    )
    parser.add_argument(
        "--phase",
        default="all",
        choices=["pilot", "full", "all"],
        help="Run only the pilot phase, only the full phase, or both.",
    )
    parser.add_argument(
        "--full-universe-kind",
        default="top100",
        choices=["kospi200", "top100", "top200", "top300", "full"],
        help="Universe kind used for the full KR phase.",
    )
    parser.add_argument(
        "--start",
        help="Optional raw backfill start date in YYYY-MM-DD. Defaults to January 1 of as-of year.",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from the last validated checkpoint inside the same run root.",
    )
    parser.add_argument(
        "--dart-mode",
        default="live",
        choices=["live", "cache_only"],
        help="Use live OpenDART calls or local DART cache only for KR bundles.",
    )
    parser.add_argument(
        "--us-universe",
        default="AAPL,MSFT,GOOGL,AMZN,NVDA",
        help="Comma-separated US tickers for the US bundle stages.",
    )
    args = parser.parse_args()

    run_root = run_local_collection(
        storage_root=Path(args.storage_root),
        as_of=date.fromisoformat(args.as_of),
        market=args.market,
        phase=args.phase,
        full_universe_kind=args.full_universe_kind,
        start=date.fromisoformat(args.start) if args.start else None,
        resume=args.resume,
        us_universe=args.us_universe,
        dart_mode=args.dart_mode,
    )
    print(run_root)


if __name__ == "__main__":
    main()
