"""Additively add gross_profit / cost_of_goods_sold / operating_cash_flow to KR
snapshot quarter records, sourced offline from the cached DART finstate_all.

For each KR ``snapshot.json`` quarter (matched by fiscal_quarter + report_date)
we re-derive fundamentals OFFLINE (live DART blocked; cache only) with the
extended decomposition, verify the overlapping revenue/net_income are unchanged
(same decomposition => safe), and write ONLY the three new flow fields when they
are currently null. Existing values and structure are never modified; the file
is re-serialized with the producer's ``indent=2, sort_keys=True`` so the diff is
exactly the added keys.

PIT: re-derivation uses the snapshot's decision_date as as_of, so only
report_date <= as_of quarters are produced and the new fields come from the same
filing as the existing ones — no look-ahead.

Usage:
    python scripts/patch_kr_quality_fields.py --dry-run   # report coverage only
    python scripts/patch_kr_quality_fields.py             # write the fields
"""

from __future__ import annotations

import argparse
import glob
import json
import sys
from datetime import date
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT / "src"))

from eit_market_data.kr.dart_provider import DartProvider  # noqa: E402

NEW_FIELDS = ("gross_profit", "cost_of_goods_sold", "operating_cash_flow")
_REL_TOL = 1e-6


def _close(a, b) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return abs(float(a) - float(b)) <= _REL_TOL * max(1.0, abs(float(a)), abs(float(b)))


def _offline_provider() -> DartProvider:
    """DartProvider whose live DART data endpoints are neutralized so the
    re-derivation is served entirely from the on-disk cache (corp-code map and
    cached finstate/finstate_all/report-list only)."""
    prov = DartProvider()

    def _blocked(*_a, **_k):
        raise RuntimeError("live DART blocked (offline patch)")

    for name in ("finstate", "finstate_all", "list"):
        if hasattr(prov._dart, name):
            setattr(prov._dart, name, _blocked)
    return prov


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--snapshots-dir", default=str(_REPO_ROOT / "artifacts" / "kr" / "snapshots"))
    ap.add_argument("--n-quarters", type=int, default=40)
    args = ap.parse_args()

    prov = _offline_provider()
    rederive: dict[tuple[str, str], dict] = {}

    snaps = sorted(glob.glob(str(Path(args.snapshots_dir) / "*" / "snapshot.json")))
    tot_q = matched = drift = 0
    added = {f: 0 for f in NEW_FIELDS}
    files_changed = 0

    for sp in snaps:
        data = json.loads(Path(sp).read_text())
        decision = date.fromisoformat(data["decision_date"])
        ym = decision.strftime("%Y%m")
        changed = False
        for ticker, fd in (data.get("fundamentals") or {}).items():
            quarters = fd.get("quarters") or []
            if not quarters:
                continue
            key = (ticker, ym)
            if key not in rederive:
                try:
                    rd = prov._fetch_fundamentals_sync(ticker, decision, args.n_quarters)
                    rederive[key] = {
                        (q.fiscal_quarter, q.report_date.isoformat()): q for q in rd.quarters
                    }
                except Exception:
                    rederive[key] = {}
            rmap = rederive[key]
            for q in quarters:
                tot_q += 1
                rq = rmap.get((q.get("fiscal_quarter"), q.get("report_date")))
                if rq is None:
                    continue
                if not _close(q.get("revenue"), rq.revenue) or not _close(
                    q.get("net_income"), rq.net_income
                ):
                    drift += 1
                    continue
                matched += 1
                for f in NEW_FIELDS:
                    val = getattr(rq, f)
                    if q.get(f) is None and val is not None:
                        if not args.dry_run:
                            q[f] = val
                        added[f] += 1
                        changed = True
        if changed and not args.dry_run:
            Path(sp).write_text(json.dumps(data, indent=2, sort_keys=True))
            files_changed += 1

    print(f"snapshots={len(snaps)} quarters={tot_q} matched={matched} drift_skipped={drift}")
    print(f"fields added: " + ", ".join(f"{f}={added[f]}" for f in NEW_FIELDS))
    print(f"files_changed={files_changed}{' (dry-run: none written)' if args.dry_run else ''}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
