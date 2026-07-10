"""Additive in-place patch: KR dividends (DPS) + KODEX 200 TR benchmark.

Injects two additive fields into existing KR snapshot bundles WITHOUT touching
any other field (raw-dict edit, no pydantic round-trip → untouched fields stay
byte-identical):

* ``fundamentals[ticker].quarters[latest].dividends_per_share`` — the PIT annual
  cash DPS from pykrx ``get_market_fundamental`` at the snapshot's decision date,
  attached to the newest quarter only, only when currently unset, DPS>0 (else
  left None: "no dividend" and "no data" both read as absent, so coverage is
  reported as two separate ratios below).
* ``benchmark_tr_prices`` — KODEX 200 TR (278530) adjusted price, the KR
  total-return benchmark (KOSPI200 book only; top-500 KRX300 TR deferred).

Uses only KRX/pykrx (no DART), so it runs safely alongside a DART backfill.
Resumable: a month already carrying both fields is skipped. Reports per-month
and aggregate coverage: lookup-success ratio and DPS>0 ratio.

Usage:
    python scripts/patch_kr_dividends_and_tr.py \
        --snapshots artifacts/kr/snapshots [--tr-etf 278530] [--dry-run]
    # top-500 (DPS only, no TR):
    python scripts/patch_kr_dividends_and_tr.py \
        --snapshots artifacts_top500/kr/snapshots --tr-etf ""
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import threading
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import bootstrap  # noqa: E402

_REPO_ROOT = bootstrap()

from eit_market_data.kr.fundamental_provider import CompositeKrFundamentalProvider  # noqa: E402
from eit_market_data.kr.market_helpers import normalize_ticker  # noqa: E402
from eit_market_data.kr.pykrx_provider import PykrxProvider  # noqa: E402


def _dps_frame_provider() -> CompositeKrFundamentalProvider:
    """A CompositeKrFundamentalProvider usable only for its DPS-frame path."""
    p = object.__new__(CompositeKrFundamentalProvider)
    p._use_market_snapshot = True
    p._dps_frame_cache = {}
    p._dps_frame_cache_lock = threading.Lock()
    return p


def _latest_quarter_index(quarters: list[dict]) -> int | None:
    """Index of the newest quarter by report_date (annual DPS belongs there)."""
    best_idx, best_key = None, None
    for i, q in enumerate(quarters):
        rd = q.get("report_date")
        if not rd:
            continue
        if best_key is None or rd > best_key:
            best_key, best_idx = rd, i
    return best_idx


def _patch_month(
    snap_path: Path,
    dps_provider: CompositeKrFundamentalProvider,
    tr_provider: PykrxProvider | None,
    dry_run: bool,
) -> dict:
    data = json.loads(snap_path.read_text(encoding="utf-8"))
    as_of = date.fromisoformat(str(data["decision_date"]))
    fundamentals = data.get("fundamentals", {})

    universe = list(fundamentals.keys())
    lookup_ok = 0
    dps_positive = 0
    dps_filled = 0

    for ticker, fund in fundamentals.items():
        if not isinstance(fund, dict):
            continue
        snap = dps_provider._fetch_dps_snapshot_sync(normalize_ticker(str(ticker)), as_of)
        # lookup-success = the KRX market-fundamental frame carried this ticker at
        # all (distinct from "paid a dividend"). Recompute against the raw frame.
        frame = dps_provider._market_fundamental_frame(min(as_of, date.today()))
        norm = normalize_ticker(str(ticker))
        if frame is not None and norm in frame.index:
            lookup_ok += 1
        dps = snap.get("dividends_per_share")
        if dps is not None and dps > 0:
            dps_positive += 1
            quarters = fund.get("quarters") or []
            idx = _latest_quarter_index(quarters)
            if idx is not None and quarters[idx].get("dividends_per_share") is None:
                quarters[idx]["dividends_per_share"] = dps
                dps_filled += 1

    tr_bars = 0
    if tr_provider is not None and not data.get("benchmark_tr_prices"):
        bars = asyncio.run(tr_provider.fetch_benchmark_tr(as_of, lookback_days=300))
        if bars:
            data["benchmark_tr_prices"] = [
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
            tr_bars = len(bars)

    changed = dps_filled > 0 or tr_bars > 0
    if changed and not dry_run:
        snap_path.write_text(
            json.dumps(data, indent=2, sort_keys=True), encoding="utf-8"
        )

    return {
        "month": as_of.strftime("%Y-%m"),
        "universe": len(universe),
        "lookup_ok": lookup_ok,
        "dps_positive": dps_positive,
        "dps_filled": dps_filled,
        "tr_bars": tr_bars,
        "changed": changed,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--snapshots", required=True, help="snapshots dir (…/kr/snapshots)")
    ap.add_argument(
        "--tr-etf",
        default="278530",
        help="TR ETF ticker for benchmark_tr_prices (default 278530 KODEX 200 TR; "
        "pass '' to skip the TR field, e.g. for the top-500 book).",
    )
    ap.add_argument("--start-month", default=None, help="YYYY-MM inclusive lower bound")
    ap.add_argument("--end-month", default=None, help="YYYY-MM inclusive upper bound")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    snapshots = Path(args.snapshots)
    if not snapshots.is_dir():
        print(f"not a directory: {snapshots}")
        return 2

    dps_provider = _dps_frame_provider()
    tr_provider = PykrxProvider(benchmark_tr_etf=args.tr_etf) if args.tr_etf else None

    months = sorted(p.name for p in snapshots.iterdir() if (p / "snapshot.json").exists())
    if args.start_month:
        months = [m for m in months if m >= args.start_month]
    if args.end_month:
        months = [m for m in months if m <= args.end_month]

    print(f"[patch] {len(months)} months under {snapshots} tr_etf={args.tr_etf or '(none)'}")
    agg = {"universe": 0, "lookup_ok": 0, "dps_positive": 0, "dps_filled": 0, "tr_months": 0}
    for m in months:
        try:
            r = _patch_month(
                snapshots / m / "snapshot.json", dps_provider, tr_provider, args.dry_run
            )
        except Exception as exc:  # noqa: BLE001
            print(f"[patch] {m}: ERROR {exc.__class__.__name__}: {exc}")
            continue
        agg["universe"] += r["universe"]
        agg["lookup_ok"] += r["lookup_ok"]
        agg["dps_positive"] += r["dps_positive"]
        agg["dps_filled"] += r["dps_filled"]
        agg["tr_months"] += 1 if r["tr_bars"] else 0
        luq = (r["lookup_ok"] / r["universe"] * 100) if r["universe"] else 0.0
        dpq = (r["dps_positive"] / r["universe"] * 100) if r["universe"] else 0.0
        print(
            f"[patch] {r['month']}: univ={r['universe']} "
            f"lookup={r['lookup_ok']}({luq:.0f}%) dps>0={r['dps_positive']}({dpq:.0f}%) "
            f"filled={r['dps_filled']} tr_bars={r['tr_bars']}"
            + (" [dry]" if args.dry_run else "")
        )

    u = agg["universe"] or 1
    print(
        f"\n[patch] AGGREGATE: ticker-months={agg['universe']} "
        f"lookup-success={agg['lookup_ok']/u*100:.1f}% "
        f"DPS>0={agg['dps_positive']/u*100:.1f}% "
        f"DPS-filled={agg['dps_filled']} TR-months={agg['tr_months']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
