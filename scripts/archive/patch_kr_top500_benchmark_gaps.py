"""Backfill empty ``benchmark_prices`` in KR top-500 bundles (additive only).

19/90 months shipped with an empty benchmark_prices array because the KRX
session was not authenticated at build time for the KRX 300 index (5042).
This patch re-fetches just those months while a valid session is live and
injects the result into the existing snapshot.json, touching no other field.

Usage:
    python scripts/patch_kr_top500_benchmark_gaps.py [--dry-run]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import bootstrap  # noqa: E402

_REPO_ROOT = bootstrap()

from eit_market_data.core.calendar import _last_business_day  # noqa: E402
from eit_market_data.kr.pykrx_provider import PykrxProvider  # noqa: E402

_SNAPSHOTS = _REPO_ROOT / "artifacts_top500" / "kr" / "snapshots"
KRX300_INDEX = "5042"

MISSING_MONTHS = [
    "2020-07", "2020-08", "2020-09", "2020-10",
    "2021-09", "2021-10", "2021-11", "2021-12",
    "2022-01", "2022-12",
    "2023-01", "2023-02",
    "2024-02", "2024-03", "2024-04",
    "2025-07", "2025-08", "2025-09",
    "2026-06",
]


async def _fetch_one(provider: PykrxProvider, label: str) -> list[dict] | None:
    year, month = (int(x) for x in label.split("-"))
    as_of = _last_business_day(year, month, "XKRX")
    bars = await provider.fetch_benchmark(as_of, lookback_days=300)
    if not bars:
        return None
    return [
        {
            "date": b.date.isoformat(),
            "open": b.open,
            "high": b.high,
            "low": b.low,
            "close": b.close,
            "volume": b.volume,
        }
        for b in bars
    ]


async def main_async(dry_run: bool) -> int:
    provider = PykrxProvider(benchmark_index=KRX300_INDEX)
    filled, still_empty = [], []
    for label in MISSING_MONTHS:
        sp = _SNAPSHOTS / label / "snapshot.json"
        if not sp.exists():
            print(f"MISSING snapshot dir for {label}, skip")
            continue
        bars = await _fetch_one(provider, label)
        if not bars:
            print(f"{label}: still empty after re-fetch")
            still_empty.append(label)
            continue
        data = json.loads(sp.read_text())
        before = len(data.get("benchmark_prices") or [])
        data["benchmark_prices"] = bars
        if not dry_run:
            sp.write_text(json.dumps(data, indent=2, sort_keys=True))
        print(f"{label}: {before} -> {len(bars)} bars" + (" (dry-run)" if dry_run else ""))
        filled.append(label)

    print(f"\nfilled {len(filled)}/{len(MISSING_MONTHS)}, still empty: {still_empty}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    return asyncio.run(main_async(args.dry_run))


if __name__ == "__main__":
    raise SystemExit(main())
