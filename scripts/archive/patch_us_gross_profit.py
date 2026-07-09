"""Additively derive US gross_profit = revenue - cost_of_goods_sold.

US fundamentals come from EDGAR XBRL. Many filers tag CostOfRevenue but not
GrossProfit, so gross_profit is null (~38% coverage) while cost_of_goods_sold is
higher (~61%). gross_profit is COGS-definitionally revenue - COGS, computed from
the SAME filing, so filling it where both inputs exist is additive and PIT-safe
(no new information, no look-ahead). Financials with no COGS stay null.

Only null gross_profit is written; existing values and all other fields are
untouched; files are re-serialized with the producer's indent=2/sort_keys.

Usage:
    python scripts/patch_us_gross_profit.py [--dry-run]
"""

from __future__ import annotations

import argparse
import glob
import json
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--snapshots-dir", default=str(_REPO_ROOT / "artifacts" / "us" / "snapshots"))
    args = ap.parse_args()

    snaps = sorted(glob.glob(str(Path(args.snapshots_dir) / "*" / "snapshot.json")))
    tot_q = have_gp = derived = no_cogs = 0
    files_changed = 0

    for sp in snaps:
        data = json.loads(Path(sp).read_text())
        changed = False
        for _ticker, fd in (data.get("fundamentals") or {}).items():
            for q in fd.get("quarters") or []:
                tot_q += 1
                if q.get("gross_profit") is not None:
                    have_gp += 1
                    continue
                rev = q.get("revenue")
                cogs = q.get("cost_of_goods_sold")
                if rev is None or cogs is None:
                    no_cogs += 1
                    continue
                if not args.dry_run:
                    q["gross_profit"] = rev - cogs
                derived += 1
                changed = True
        if changed and not args.dry_run:
            Path(sp).write_text(json.dumps(data, indent=2, sort_keys=True))
            files_changed += 1

    total_gp = have_gp + derived
    print(f"snapshots={len(snaps)} quarters={tot_q}")
    print(f"gross_profit: already={have_gp} derived={derived} still_null(no COGS)={no_cogs}")
    print(f"gross_profit coverage: {100*have_gp/tot_q:.0f}% -> {100*total_gp/tot_q:.0f}%")
    print(f"files_changed={files_changed}{' (dry-run)' if args.dry_run else ''}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
