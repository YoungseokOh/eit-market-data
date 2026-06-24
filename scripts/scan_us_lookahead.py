#!/usr/bin/env python3
"""Scan US snapshot bundles for point-in-time (look-ahead) violations.

Thin wrapper over the market-agnostic scanner in ``scan_kr_lookahead.py``: for
every ``artifacts/us/snapshots/*/snapshot.json`` it asserts that, for a snapshot
whose ``decision_date = D``, every ``filing.filing_date``, fundamental
``quarter.report_date``, and price/benchmark bar ``date`` is ``<= D``.

Exit code is non-zero when any violation is found, so it can gate rebuilds.
"""

from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ROOT = PROJECT_ROOT / "artifacts" / "us" / "snapshots"


def _load_scanner():
    path = Path(__file__).resolve().parent / "scan_kr_lookahead.py"
    spec = importlib.util.spec_from_file_location("scan_lookahead_core", path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=str(DEFAULT_ROOT))
    parser.add_argument("--max-show", type=int, default=5)
    args = parser.parse_args()

    scanner = _load_scanner()
    root = Path(args.root)
    paths = sorted(root.glob("*/snapshot.json"))
    if not paths:
        print(f"No snapshots found under {root}")
        return 0

    total = 0
    for path in paths:
        report = scanner.scan_snapshot(path)
        viols = report["violations"]
        total += len(viols)
        head = f"{report['month']}  decision={report['decision_date']}  violations={len(viols)}"
        if viols:
            examples = ", ".join(viols[: args.max_show])
            more = f", +{len(viols) - args.max_show} more" if len(viols) > args.max_show else ""
            print(f"FAIL  {head}  [{examples}{more}]")
        else:
            print(f"OK    {head}")

    print(f"\nTOTAL violations across {len(paths)} bundle(s): {total}")
    return 1 if total else 0


if __name__ == "__main__":
    sys.exit(main())
