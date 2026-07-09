# ruff: noqa: E402
"""Resumable KR (KOSPI200) monthly bundle backfill for a date range.

For each month in ``[--start-month, --end-month]`` this driver:

1. Resolves the holiday-aware last KRX trading day (``core.calendar._last_business_day``
   with ``XKRX``) as the decision date — never a calendar month-end, which would
   land on a KRX closure and zero out market_cap.
2. Builds the point-in-time KOSPI200 membership CSV under
   ``universes/kr/kospi200/<YYYY-MM>.csv`` (carrying the prior month forward for
   the off-cycle churn guard) and version-controls it alongside the existing months.
3. Builds the monthly snapshot bundle under ``artifacts/kr/snapshots/<YYYY-MM>/``
   with ``--profile official`` and DART served ``cache_only`` (no OpenDART network;
   see ``.claude/rules/dart-api-limits.md``).

Resumable: a month whose ``snapshot.json`` already exists is skipped, so an
environment kill loses at most the in-flight month. Progress + a status marker are
written to ``data/kr_backfill_2019/``.

Usage:
    python scripts/build_kr_backfill_2019.py --start-month 2019-01 --end-month 2022-12
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import traceback
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import bootstrap, month_tuples as _iter_months

PROJECT_ROOT = bootstrap()

from eit_market_data.core.calendar import _last_business_day
from eit_market_data.local_collection import (
    _load_kospi200_members_from_csv,
    build_local_universe_manifest,
)

from build_kr_snapshot import build_snapshot  # type: ignore[import-not-found]

ARTIFACTS_ROOT = PROJECT_ROOT / "artifacts"
UNIVERSE_DIR = PROJECT_ROOT / "universes" / "kr" / "kospi200"
STATE_DIR = PROJECT_ROOT / "data" / "kr_backfill_2019"


def _prev_month_csv(year: int, month: int) -> Path:
    py, pm = (year - 1, 12) if month == 1 else (year, month - 1)
    return UNIVERSE_DIR / f"{py:04d}-{pm:02d}.csv"


def _write_state(payload: dict) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    payload["updated"] = datetime.now().astimezone().isoformat(timespec="seconds")
    (STATE_DIR / "state.json").write_text(json.dumps(payload, indent=2, ensure_ascii=False))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start-month", required=True, help="YYYY-MM inclusive.")
    ap.add_argument("--end-month", required=True, help="YYYY-MM inclusive.")
    args = ap.parse_args()

    UNIVERSE_DIR.mkdir(parents=True, exist_ok=True)
    months = _iter_months(args.start_month, args.end_month)
    built = skipped = failed = 0
    print(f"[backfill] {len(months)} months {args.start_month}..{args.end_month}", flush=True)

    for (year, month) in months:
        label = f"{year:04d}-{month:02d}"
        snap_json = ARTIFACTS_ROOT / "kr" / "snapshots" / label / "snapshot.json"
        if snap_json.exists():
            skipped += 1
            print(f"[backfill] skip {label} (snapshot exists)", flush=True)
            continue

        as_of = _last_business_day(year, month, "XKRX")
        _write_state({"phase": "running", "month": label, "as_of": as_of.isoformat(),
                      "built": built, "skipped": skipped, "failed": failed})
        try:
            prev_csv = _prev_month_csv(year, month)
            previous = _load_kospi200_members_from_csv(prev_csv) if prev_csv.exists() else None
            universe_csv = UNIVERSE_DIR / f"{label}.csv"
            build_local_universe_manifest(
                as_of=as_of,
                kind="kospi200",
                output_path=universe_csv,
                previous_members=previous,
            )
            summary = asyncio.run(
                build_snapshot(
                    as_of,
                    universe_csv,
                    ARTIFACTS_ROOT,
                    profile="official",
                    market_subdir="kr",
                    dart_mode="cache_only",
                )
            )
            built += 1
            print(
                f"[backfill] DONE {label} as_of={as_of} universe={summary.get('universe_size')} "
                f"prices={summary.get('price_tickers')} funds={summary.get('fundamental_tickers')} "
                f"bench={summary.get('benchmark_bars')}",
                flush=True,
            )
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"[backfill] FAIL {label}: {exc}", flush=True)
            traceback.print_exc()
            _write_state({"phase": "error", "month": label, "as_of": as_of.isoformat(),
                          "error": str(exc), "built": built, "skipped": skipped, "failed": failed})
            return 1

    _write_state({"phase": "done", "built": built, "skipped": skipped, "failed": failed})
    print(f"[backfill] COMPLETE built={built} skipped={skipped} failed={failed}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
