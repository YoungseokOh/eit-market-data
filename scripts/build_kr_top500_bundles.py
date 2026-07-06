#!/usr/bin/env python3
# ruff: noqa: E402
"""Resumable KR top-500 (KOSPI+KOSDAQ, ADV-ranked) monthly bundle build driver.

Consumes the pre-built point-in-time membership CSVs under
``universes/kr/top500/<YYYY-MM>.csv`` (produced by
``scripts/build_kr_top500_universe.py``) and builds a monthly snapshot bundle per
month under ``<--artifacts-root>/kr/snapshots/<YYYY-MM>/`` using:

* the KRX 300 index (code 5042) as the KOSPI+KOSDAQ-spanning benchmark, and
* ``--dart-mode cache_only`` by default (offline-first: reuses ``data/dart_cache``
  for the overlap corps; the new KOSDAQ corps must be backfilled separately with
  ``scripts/backfill_dart_cache_controlled.py`` before their fundamentals appear).

Resumable: a month whose ``snapshot.json`` already exists is skipped. Writes to a
parallel ``--artifacts-root`` (default ``artifacts_top500``) so the existing
KOSPI200 bundles under ``artifacts/kr`` are never clobbered mid-flight; promote
with an explicit swap once verified.

Usage:
    python scripts/build_kr_top500_bundles.py --start-month 2019-01 --end-month 2026-06
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import traceback
from datetime import date
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT / "src"))
sys.path.insert(0, str(_REPO_ROOT / "scripts"))

from build_kr_snapshot import build_snapshot
from eit_market_data.core.calendar import _last_business_day

UNIVERSE_DIR = _REPO_ROOT / "universes" / "kr" / "top500"
KRX300_INDEX = "5042"


def _iter_months(start: str, end: str) -> list[tuple[int, int]]:
    sy, sm = (int(x) for x in start.split("-"))
    ey, em = (int(x) for x in end.split("-"))
    out: list[tuple[int, int]] = []
    y, m = sy, sm
    while (y, m) <= (ey, em):
        out.append((y, m))
        m += 1
        if m > 12:
            m, y = 1, y + 1
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-month", required=True)
    ap.add_argument("--end-month", required=True)
    ap.add_argument("--artifacts-root", default=str(_REPO_ROOT / "artifacts_top500"))
    ap.add_argument("--dart-mode", default="cache_only", choices=["cache_only", "live"])
    ap.add_argument("--profile", default="official")
    args = ap.parse_args()

    artifacts_root = Path(args.artifacts_root)
    state_path = artifacts_root / "kr" / "_top500_build_state.json"
    months = _iter_months(args.start_month, args.end_month)
    built = skipped = failed = 0
    print(f"[top500] {len(months)} months {args.start_month}..{args.end_month} "
          f"root={artifacts_root} dart={args.dart_mode}", flush=True)

    for (year, month) in months:
        label = f"{year:04d}-{month:02d}"
        snap_json = artifacts_root / "kr" / "snapshots" / label / "snapshot.json"
        universe_csv = UNIVERSE_DIR / f"{label}.csv"
        if snap_json.exists():
            skipped += 1
            print(f"[top500] skip {label} (exists)", flush=True)
            continue
        if not universe_csv.exists():
            failed += 1
            print(f"[top500] MISSING universe csv {universe_csv}", flush=True)
            continue

        as_of = _last_business_day(year, month, "XKRX")
        try:
            summary = asyncio.run(
                build_snapshot(
                    as_of,
                    universe_csv,
                    artifacts_root,
                    profile=args.profile,
                    market_subdir="kr",
                    dart_mode=args.dart_mode,
                    benchmark_index=KRX300_INDEX,
                )
            )
            built += 1
            print(
                f"[top500] DONE {label} as_of={as_of} universe={summary.get('universe_size')} "
                f"prices={summary.get('price_tickers')} funds={summary.get('fundamental_tickers')} "
                f"bench={summary.get('benchmark_bars')}",
                flush=True,
            )
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"[top500] FAIL {label}: {exc}", flush=True)
            traceback.print_exc()
            state_path.parent.mkdir(parents=True, exist_ok=True)
            state_path.write_text(json.dumps(
                {"phase": "error", "month": label, "error": str(exc),
                 "built": built, "skipped": skipped, "failed": failed}), encoding="utf-8")
            return 1

    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(
        {"phase": "done", "built": built, "skipped": skipped, "failed": failed}),
        encoding="utf-8")
    print(f"[top500] COMPLETE built={built} skipped={skipped} failed={failed}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
