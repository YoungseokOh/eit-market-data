"""Re-adjust drifted backfill tickers to the frozen 2023+ adjustment basis.

The 2019-2022 US backfill fetched prices from yfinance NOW, while the frozen
2023-01+ bundles were built earlier; a corporate action between the two dates
(e.g. a CRWD split) makes yfinance auto_adjust express the SAME calendar date at
a different level. The consumer's adjustment_continuity check (shared-date
comparison) correctly flags these at the 2022-12 -> 2023-01 seam.

Fix: for each named ticker, multiply every OHLC value in the 2019-2022 backfill
bundles by a constant so its latest date shared with the frozen 2023-01 bundle
matches exactly. For a clean split (CRWD, verified constant ratio 4.0 across all
280 overlapping dates) this restores the original basis exactly everywhere. For a
spin-off (DD) the ratio is mildly non-constant (0.277-0.290), so the seam is made
exact and the residual is reported. Volume is not rescaled. The frozen 2023+
bundles are never touched.

Usage:
    python scripts/fix_seam_adjustment_drift.py [--dry-run]
"""

from __future__ import annotations

import argparse
import glob
import json
import statistics
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_US = _REPO_ROOT / "artifacts" / "us" / "snapshots"

TICKERS = ("CRWD", "DD")
_OHLC = ("open", "high", "low", "close")
BACKFILL_MONTHS = tuple(f"{y}-{m:02d}" for y in range(2019, 2023) for m in range(1, 13))


def _seam_factor(ticker: str) -> tuple[float, float]:
    """(factor, ratio_spread) from the 2022-12 backfill vs frozen 2023-01 seam."""
    bf = {r["date"]: r["close"] for r in json.loads((_US / "2022-12" / "snapshot.json").read_text())["prices"].get(ticker, [])}
    orig = {r["date"]: r["close"] for r in json.loads((_US / "2023-01" / "snapshot.json").read_text())["prices"].get(ticker, [])}
    shared = sorted(d for d in set(bf) & set(orig) if bf[d])
    ratios = [orig[d] / bf[d] for d in shared]
    factor = orig[shared[-1]] / bf[shared[-1]]  # anchor on the latest shared date
    return factor, (max(ratios) - min(ratios))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    factors = {t: _seam_factor(t) for t in TICKERS}
    for t, (f, spread) in factors.items():
        kind = "clean split (exact)" if spread < 0.01 else f"spin-off (seam-exact; overlap residual {spread:.1%})"
        print(f"{t}: rescale factor {f:.5f} — {kind}")

    files_changed = 0
    for month in BACKFILL_MONTHS:
        sp = _US / month / "snapshot.json"
        if not sp.exists():
            continue
        data = json.loads(sp.read_text())
        prices = data.get("prices") or {}
        changed = False
        for t in TICKERS:
            bars = prices.get(t)
            if not bars:
                continue
            f = factors[t][0]
            for bar in bars:
                for k in _OHLC:
                    if bar.get(k) is not None:
                        bar[k] = round(bar[k] * f, 6)
            changed = True
        if changed and not args.dry_run:
            sp.write_text(json.dumps(data, indent=2, sort_keys=True))
            files_changed += 1

    print(f"backfill months rescaled: {files_changed}{' (dry-run)' if args.dry_run else ''}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
