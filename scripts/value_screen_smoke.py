#!/usr/bin/env python3
"""Value-screen smoke check over US monthly bundles.

Replicates (approximately) the consumer's cheapest-15 value screen so we can
confirm, without importing eit-research, that each evaluable decision month M:

* yields >=15 qualifying names (has as-of market_cap + screenable fundamentals), and
* has a following bundle M+1 so a forward return can resolve.

Yields are computed from the most recent point-in-time quarter (already filtered
to ``report_date <= decision_date`` by the producer):

* earnings yield  = net_income * 4 / market_cap
* fcf yield       = free_cash_flow * 4 / market_cap
* book yield      = total_equity / market_cap
* ebitda yield    = ebitda * 4 / market_cap

A name qualifies if it has market_cap > 0 and at least one finite yield; the
"cheapest 15" are the top 15 by the average of their available yields.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import next_month as _next_month

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ROOT = PROJECT_ROOT / "artifacts" / "us" / "snapshots"


def _yields(fund: dict) -> list[float]:
    mc = fund.get("market_cap")
    quarters = fund.get("quarters") or []
    if not mc or mc <= 0 or not quarters:
        return []
    q = quarters[0]  # most recent PIT quarter (producer sorts desc)
    out: list[float] = []
    for key, mult in (("net_income", 4), ("free_cash_flow", 4), ("ebitda", 4)):
        v = q.get(key)
        if isinstance(v, (int, float)):
            out.append(v * mult / mc)
    eq = q.get("total_equity")
    if isinstance(eq, (int, float)):
        out.append(eq / mc)
    return out


def screen_month(path: Path) -> dict:
    data = json.loads(path.read_text(encoding="utf-8"))
    funds = data.get("fundamentals") or {}
    scored: list[tuple[str, float]] = []
    for ticker, fund in funds.items():
        if not isinstance(fund, dict):
            continue
        ys = _yields(fund)
        if ys:
            scored.append((ticker, sum(ys) / len(ys)))
    scored.sort(key=lambda kv: kv[1], reverse=True)
    top15 = scored[:15]
    return {
        "month": path.parent.name,
        "qualifying": len(scored),
        "top15": [t for t, _ in top15],
        "top15_count": len(top15),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=str(DEFAULT_ROOT))
    args = parser.parse_args()
    root = Path(args.root)
    months = sorted(p.parent.name for p in root.glob("*/snapshot.json"))
    available = set(months)

    ok = True
    evaluable = 0
    for month in months:
        nxt = _next_month(month)
        has_next = nxt in available
        report = screen_month(root / month / "snapshot.json")
        full15 = report["top15_count"] == 15
        if has_next and full15:
            evaluable += 1
        status = "OK  " if (full15 and (has_next or month == months[-1])) else "WARN"
        if not full15:
            ok = False
        print(
            f"{status} {month}: qualifying={report['qualifying']:3d} "
            f"top15={report['top15_count']:2d} M+1={nxt}{'' if has_next else ' (MISSING)'}"
        )

    print(f"\nEvaluable months (15 names + M+1 exists): {evaluable}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
