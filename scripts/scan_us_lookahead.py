#!/usr/bin/env python3
"""Scan US snapshot bundles for point-in-time (look-ahead) violations.

Thin entrypoint over the market-agnostic scanner in ``scan_kr_lookahead.py``:
for every ``artifacts/us/snapshots/*/snapshot.json`` it asserts that, for a
snapshot whose ``decision_date = D``, every ``filing.filing_date``, fundamental
``quarter.report_date``, and price/benchmark bar ``date`` is ``<= D``.

Exit code is non-zero when any violation is found, so it can gate rebuilds.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from scan_kr_lookahead import main

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ROOT = PROJECT_ROOT / "artifacts" / "us" / "snapshots"


if __name__ == "__main__":
    sys.exit(main(default_root=DEFAULT_ROOT, description=__doc__))
