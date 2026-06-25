#!/usr/bin/env python3
"""Scan KR snapshot bundles for point-in-time (look-ahead) violations.

For every ``artifacts/kr/snapshots/*/snapshot.json`` this asserts the producer
invariant: for a snapshot whose ``decision_date = D``,

* every ``filing.filing_date <= D``
* every fundamental ``quarter.report_date <= D``
* every ``price``/``benchmark`` bar ``date <= D``

Exit code is non-zero when any violation is found, so it can gate CI/rebuilds.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_GLOB_ROOT = PROJECT_ROOT / "artifacts" / "kr" / "snapshots"


def _as_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def scan_snapshot(path: Path) -> dict[str, object]:
    data = json.loads(path.read_text(encoding="utf-8"))
    decision = _as_date(data.get("decision_date"))
    violations: list[str] = []
    if decision is None:
        return {"month": path.parent.name, "decision_date": None, "violations": ["missing_decision_date"]}

    for ticker, filing in (data.get("filings") or {}).items():
        fd = _as_date(filing.get("filing_date")) if isinstance(filing, dict) else None
        if fd is not None and fd > decision:
            violations.append(f"filing {ticker} {fd.isoformat()}")

    for ticker, fund in (data.get("fundamentals") or {}).items():
        if not isinstance(fund, dict):
            continue
        for q in fund.get("quarters") or []:
            rd = _as_date(q.get("report_date")) if isinstance(q, dict) else None
            if rd is not None and rd > decision:
                violations.append(f"fundamental {ticker} {q.get('fiscal_quarter')} {rd.isoformat()}")

    for ticker, bars in (data.get("prices") or {}).items():
        for bar in bars or []:
            bd = _as_date(bar.get("date")) if isinstance(bar, dict) else None
            if bd is not None and bd > decision:
                violations.append(f"price {ticker} {bd.isoformat()}")

    for bar in data.get("benchmark_prices") or []:
        bd = _as_date(bar.get("date")) if isinstance(bar, dict) else None
        if bd is not None and bd > decision:
            violations.append(f"benchmark {bd.isoformat()}")

    return {
        "month": path.parent.name,
        "decision_date": decision.isoformat(),
        "violations": violations,
    }


def run_scan(root: Path, max_show: int) -> int:
    """Scan ``<root>/*/snapshot.json`` bundles; return 1 on any violation.

    Market-agnostic: both the KR and US look-ahead scanners call this with their
    own default root. The exit-code contract (1 on any violation, else 0) gates
    CI/rebuilds, so it must stay stable.
    """
    paths = sorted(root.glob("*/snapshot.json"))
    if not paths:
        print(f"No snapshots found under {root}")
        return 0

    total = 0
    for path in paths:
        report = scan_snapshot(path)
        viols = report["violations"]
        total += len(viols)
        head = f"{report['month']}  decision={report['decision_date']}  violations={len(viols)}"
        if viols:
            examples = ", ".join(viols[:max_show])
            more = f", +{len(viols) - max_show} more" if len(viols) > max_show else ""
            print(f"FAIL  {head}  [{examples}{more}]")
        else:
            print(f"OK    {head}")

    print(f"\nTOTAL violations across {len(paths)} bundle(s): {total}")
    return 1 if total else 0


def main(default_root: Path = DEFAULT_GLOB_ROOT, description: str | None = None) -> int:
    parser = argparse.ArgumentParser(description=description or __doc__)
    parser.add_argument(
        "--root",
        default=str(default_root),
        help="Root dir holding <month>/snapshot.json bundles.",
    )
    parser.add_argument(
        "--max-show",
        type=int,
        default=5,
        help="Max example violations to print per month.",
    )
    args = parser.parse_args()
    return run_scan(Path(args.root), args.max_show)


if __name__ == "__main__":
    sys.exit(main())
