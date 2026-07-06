#!/usr/bin/env python3
# ruff: noqa: E402
"""Build a survivorship-free, point-in-time KOSPI+KOSDAQ top-N-by-ADV universe.

For each month's holiday-aware last XKRX business day (the *decision date*) this
script:

1. Lists the *then-listed* KOSPI + KOSDAQ tickers via pykrx (survivorship-free:
   KRX retains delisted tickers, so a name that later delists still appears in
   its historical months and exits at its real delisting date — no look-ahead,
   no retro-applied present-day list).
2. Ranks them by trailing average daily trading value (ADV = mean 거래대금 over a
   sampled trailing window of business days) and takes the top ``--top-n``.
3. Persists per-month membership CSVs under ``universes/kr/top500/YYYY-MM.csv``.

The live pykrx cost is a per-market daily market-cap frame (거래대금 for all
tickers in one call). Those frames are cached under
``data/market/adv_cap_daily/{MARKET}_{yyyymmdd}.parquet`` so nothing is refetched
and the fetch phase is fully resumable. To stay gentle on KRX we sample every
``--stride`` business day of the trailing window rather than every day.

Phases:
    fetch    live pykrx: populate the daily cap-frame cache (resumable)
    compute  offline: rank ADV, write per-month membership CSVs
    both     fetch then compute (default)

Usage:
    python scripts/build_kr_top500_universe.py --phase both \
        --start 2019-01 --end 2026-06 --top-n 500 --window 60 --stride 3 --delay 0.6
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT / "src"))

from eit_market_data.core.calendar import _is_trading_day, _last_business_day
from eit_market_data.kr.market_helpers import (
    fetch_market_cap_frame,
    fetch_market_ticker_list,
    normalize_ticker,
)

ADV_CACHE_DIR = _REPO_ROOT / "data" / "market" / "adv_cap_daily"
UNIVERSE_DIR = _REPO_ROOT / "universes" / "kr" / "top500"
MARKETS = ("KOSPI", "KOSDAQ")


def _iter_months(start: str, end: str):
    sy, sm = (int(x) for x in start.split("-"))
    ey, em = (int(x) for x in end.split("-"))
    y, m = sy, sm
    while (y, m) <= (ey, em):
        yield y, m
        m += 1
        if m > 12:
            m = 1
            y += 1


def _business_days_back(anchor: date, n: int) -> list[date]:
    """Return the ``n`` XKRX business days ending at ``anchor`` (inclusive), ascending."""
    out: list[date] = []
    d = anchor
    while len(out) < n:
        if _is_trading_day(d, "XKRX"):
            out.append(d)
        d -= timedelta(days=1)
        if (anchor - d).days > n * 3 + 40:  # safety guard
            break
    return sorted(out)


def _sampled_days(start: str, end: str, window: int, stride: int) -> list[date]:
    """Union of stride-sampled trailing-window business days across all decision months."""
    needed: set[date] = set()
    for y, m in _iter_months(start, end):
        anchor = _last_business_day(y, m, "XKRX")
        window_days = _business_days_back(anchor, window)
        # sample every `stride`-th day, always keeping the most recent (anchor)
        sampled = window_days[::-1][::stride]
        needed.update(sampled)
    return sorted(needed)


def _cache_path(market: str, d: date) -> Path:
    return ADV_CACHE_DIR / f"{market}_{d:%Y%m%d}.parquet"


def _fetch_phase(days: list[date], delay: float) -> dict[str, int]:
    import pandas as pd

    ADV_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    stats = {"fetched": 0, "cached": 0, "empty": 0}
    total = len(days) * len(MARKETS)
    i = 0
    for d in days:
        for market in MARKETS:
            i += 1
            path = _cache_path(market, d)
            if path.exists():
                stats["cached"] += 1
                continue
            frame = fetch_market_cap_frame(d, market, use_local=False)
            if frame is None or frame.empty or "거래대금" not in frame.columns:
                stats["empty"] += 1
                print(f"[{i}/{total}] EMPTY {market} {d}", flush=True)
                time.sleep(delay)
                continue
            working = frame.reset_index()
            code_col = "종목코드" if "종목코드" in working.columns else working.columns[0]
            out = pd.DataFrame(
                {
                    "ticker": working[code_col].astype(str).map(normalize_ticker),
                    "value": pd.to_numeric(working["거래대금"], errors="coerce"),
                    "close": pd.to_numeric(working.get("종가"), errors="coerce"),
                    "market_cap": pd.to_numeric(working.get("시가총액"), errors="coerce"),
                }
            )
            if "종목명" in working.columns:
                out["name"] = working["종목명"].fillna("").astype(str).str.strip()
            out.to_parquet(path, index=False)
            stats["fetched"] += 1
            print(f"[{i}/{total}] OK {market} {d} rows={len(out)}", flush=True)
            time.sleep(delay)
    return stats


def _load_cached_day(market: str, d: date):
    import pandas as pd

    path = _cache_path(market, d)
    if not path.exists():
        return None
    try:
        return pd.read_parquet(path)
    except Exception:
        return None


def _compute_phase(start: str, end: str, top_n: int, window: int, stride: int) -> dict[str, Any]:
    import pandas as pd

    UNIVERSE_DIR.mkdir(parents=True, exist_ok=True)
    sizes: dict[str, int] = {}
    prev_set: set[str] | None = None
    churn_log: list[dict[str, Any]] = []

    for y, m in _iter_months(start, end):
        anchor = _last_business_day(y, m, "XKRX")
        month = f"{y:04d}-{m:02d}"
        window_days = _business_days_back(anchor, window)
        sampled = window_days[::-1][::stride]

        # PIT membership: then-listed tickers per market
        listed: dict[str, str] = {}  # ticker -> market
        for market in MARKETS:
            for t in fetch_market_ticker_list(anchor, market):
                listed.setdefault(t, market)

        # trailing ADV from cached daily 거래대금
        value_sums: dict[str, float] = {}
        value_counts: dict[str, int] = {}
        latest_close: dict[str, float] = {}
        latest_cap: dict[str, float] = {}
        names: dict[str, str] = {}
        for market in MARKETS:
            for d in sampled:
                frame = _load_cached_day(market, d)
                if frame is None or frame.empty:
                    continue
                for row in frame.itertuples(index=False):
                    t = normalize_ticker(str(row.ticker))
                    v = getattr(row, "value", None)
                    if v is None or pd.isna(v):
                        continue
                    value_sums[t] = value_sums.get(t, 0.0) + float(v)
                    value_counts[t] = value_counts.get(t, 0) + 1
                    c = getattr(row, "close", None)
                    if c is not None and not pd.isna(c):
                        latest_close[t] = float(c)
                    mc = getattr(row, "market_cap", None)
                    if mc is not None and not pd.isna(mc):
                        latest_cap[t] = float(mc)
                    nm = getattr(row, "name", "")
                    if nm:
                        names[t] = str(nm)

        rows: list[dict[str, Any]] = []
        for t, mkt in listed.items():
            cnt = value_counts.get(t, 0)
            if cnt == 0:
                continue  # listed but no traded value in window -> not ADV-rankable
            adv = value_sums[t] / cnt
            rows.append(
                {
                    "ticker": t,
                    "market": mkt,
                    "name": names.get(t, ""),
                    "adv": adv,
                    "adv_days": cnt,
                    "close": latest_close.get(t),
                    "market_cap": latest_cap.get(t),
                }
            )
        if not rows:
            print(f"{month}: NO ADV DATA (cache missing?) anchor={anchor}", flush=True)
            sizes[month] = 0
            continue

        df = pd.DataFrame(rows).sort_values("adv", ascending=False).head(top_n).reset_index(drop=True)
        df["rank"] = range(1, len(df) + 1)
        df["as_of"] = anchor.isoformat()
        df = df[["ticker", "market", "name", "adv", "adv_days", "close", "market_cap", "rank", "as_of"]]
        out_path = UNIVERSE_DIR / f"{month}.csv"
        df.to_csv(out_path, index=False, encoding="utf-8")
        sizes[month] = len(df)

        cur_set = set(df["ticker"])
        if prev_set is not None:
            entrants = cur_set - prev_set
            leavers = prev_set - cur_set
            churn_log.append(
                {
                    "month": month,
                    "size": len(cur_set),
                    "entrants": len(entrants),
                    "leavers": len(leavers),
                    "kosdaq": int((df["market"] == "KOSDAQ").sum()),
                }
            )
        prev_set = cur_set
        n_kosdaq = int((df["market"] == "KOSDAQ").sum())
        print(f"{month}: {len(df)} names (KOSDAQ={n_kosdaq}) anchor={anchor}", flush=True)

    return {"sizes": sizes, "churn": churn_log}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", choices=["fetch", "compute", "both"], default="both")
    ap.add_argument("--start", default="2019-01")
    ap.add_argument("--end", default="2026-06")
    ap.add_argument("--top-n", type=int, default=500)
    ap.add_argument("--window", type=int, default=60, help="trailing window in business days")
    ap.add_argument("--stride", type=int, default=3, help="sample every Nth business day")
    ap.add_argument("--delay", type=float, default=0.6, help="seconds between live pykrx calls")
    args = ap.parse_args()

    if args.phase in ("fetch", "both"):
        days = _sampled_days(args.start, args.end, args.window, args.stride)
        print(f"FETCH phase: {len(days)} distinct sampled days x {len(MARKETS)} markets", flush=True)
        stats = _fetch_phase(days, args.delay)
        print(f"FETCH done: {stats}", flush=True)

    if args.phase in ("compute", "both"):
        result = _compute_phase(args.start, args.end, args.top_n, args.window, args.stride)
        sizes = result["sizes"]
        nonzero = [v for v in sizes.values() if v]
        print(
            f"COMPUTE done: {len(sizes)} months, "
            f"size range {min(nonzero) if nonzero else 0}-{max(nonzero) if nonzero else 0}",
            flush=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
