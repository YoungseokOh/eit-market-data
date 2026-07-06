#!/usr/bin/env python3
# ruff: noqa: E402
"""Scope the DART backfill for the top-500 universe and build the delisting manifest.

Reads the per-month membership CSVs under ``universes/kr/top500/`` and:

1. Computes the union of top-500 tickers across all months, subtracts the corps
   already present in ``data/dart_cache`` (the ~240 KOSPI200 large-cap set), and
   writes the explicit *missing-corp* ticker CSV that the controlled DART backfill
   consumes: ``universes/kr/top500/_dart_missing.csv``.
2. Resolves DART ``corp_code`` for the missing tickers (one corpCode.xml download
   via OpenDartReader — an allowed single fetch; KOSDAQ corps are included).
3. Classifies every top-500 *exit* (a name in the membership at some month that is
   no longer listed as of ``--today``) into the delisting manifest
   ``docs/kr_top500_delisted_manifest.{md,json}``, capturing the terminal OHLCV
   collapse path for failure-to-zero cases (pykrx retains delisted OHLCV).

DART is only touched for the single corpCode.xml resolution (``--resolve-corps``),
never for fundamentals. Run the fundamentals backfill separately with
``scripts/backfill_dart_cache_controlled.py`` + ``scripts/backfill_finstate_all.py``.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT / "src"))

from eit_market_data.kr.market_helpers import (
    fetch_market_ticker_list,
    fetch_stock_ohlcv_frame,
    normalize_ticker,
)

UNIVERSE_DIR = _REPO_ROOT / "universes" / "kr" / "top500"
CACHE_TICKERS_JSON = Path(
    "/private/tmp/claude-501/-Users-ysoh-Projects-eit-market-data/"
    "bce7158b-a205-475b-809c-0931da643d82/scratchpad/cache_tickers.json"
)
DOCS_DIR = _REPO_ROOT / "docs"
MISSING_CSV = UNIVERSE_DIR / "_dart_missing.csv"


def _load_universe() -> tuple[dict[str, dict[str, Any]], list[str]]:
    import pandas as pd

    files = sorted(UNIVERSE_DIR.glob("[0-9]*.csv"))
    per_ticker: dict[str, dict[str, Any]] = {}
    months: list[str] = []
    for f in files:
        month = f.stem
        months.append(month)
        df = pd.read_csv(f, dtype={"ticker": str})
        for row in df.itertuples(index=False):
            t = normalize_ticker(str(row.ticker))
            rec = per_ticker.setdefault(
                t,
                {"ticker": t, "market": row.market, "name": getattr(row, "name", ""),
                 "months": [], "first": month, "last": month},
            )
            rec["months"].append(month)
            rec["last"] = month
            if getattr(row, "name", ""):
                rec["name"] = row.name
            rec["market"] = row.market
    return per_ticker, months


def _cached_tickers() -> set[str]:
    if not CACHE_TICKERS_JSON.exists():
        return set()
    data = json.loads(CACHE_TICKERS_JSON.read_text())
    return {normalize_ticker(t) for t in data.get("fundamental_tickers", [])}


def _resolve_corp_codes(tickers: list[str]) -> dict[str, str | None]:
    """One corpCode.xml download; map tickers -> corp_code offline thereafter."""
    from eit_market_data.kr.dart_provider import DartProvider

    provider = DartProvider(allow_stale_fallback=True, raise_on_error=False)
    out: dict[str, str | None] = {}
    for t in tickers:
        try:
            out[t] = provider._ticker_to_corp_code(t)
        except Exception:
            out[t] = None
    return out


def _classify_exits(
    per_ticker: dict[str, dict[str, Any]],
    months: list[str],
    today: date,
    capture_collapse: bool,
) -> list[dict[str, Any]]:
    """A top-500 name whose universe tenure ended before the final month and that
    is no longer listed today is treated as a delisting exit; capture terminal OHLCV."""
    last_month = months[-1]
    today_listed = set(fetch_market_ticker_list(today, "KOSPI")) | set(
        fetch_market_ticker_list(today, "KOSDAQ")
    )
    exits: list[dict[str, Any]] = []
    for t, rec in per_ticker.items():
        if rec["last"] == last_month:
            continue  # still in the universe at the end -> not an exit
        still_listed = t in today_listed
        entry = {
            "ticker": t,
            "name": rec["name"],
            "market": rec["market"],
            "first_month": rec["first"],
            "last_month": rec["last"],
            "months_in_universe": len(rec["months"]),
            "still_listed": still_listed,
            "classification": "dropped_out" if still_listed else "delisted",
        }
        if not still_listed and capture_collapse:
            ly, lm = (int(x) for x in rec["last"].split("-"))
            end = date(ly, lm, 28) + timedelta(days=120)
            start = date(ly, lm, 1) - timedelta(days=120)
            df, src = fetch_stock_ohlcv_frame(t, start, min(end, today))
            if df is not None and not df.empty and "종가" in df.columns:
                closes = df["종가"].dropna()
                if len(closes) > 0:
                    peak = float(closes.max())
                    last_close = float(closes.iloc[-1])
                    last_dt = df.index[-1]
                    drawdown = 1.0 - (last_close / peak) if peak else 0.0
                    entry["terminal_close"] = last_close
                    entry["window_peak_close"] = peak
                    entry["terminal_drawdown"] = round(drawdown, 4)
                    entry["last_ohlcv_date"] = str(
                        last_dt.date() if hasattr(last_dt, "date") else last_dt
                    )
                    entry["ohlcv_source"] = src
                    # failure-to-zero: deep terminal collapse from window peak
                    entry["failure_to_zero"] = bool(drawdown >= 0.70)
                    if entry["failure_to_zero"]:
                        tail = closes.tail(20)
                        entry["collapse_path_close"] = [float(x) for x in tail.tolist()]
        exits.append(entry)
    exits.sort(key=lambda e: (e["classification"], e["last_month"], e["ticker"]))
    return exits


def _write_manifest(exits: list[dict[str, Any]], months: list[str]) -> None:
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    delisted = [e for e in exits if e["classification"] == "delisted"]
    dropped = [e for e in exits if e["classification"] == "dropped_out"]
    ftz = [e for e in delisted if e.get("failure_to_zero")]
    payload = {
        "generated_universe_months": [months[0], months[-1]] if months else [],
        "counts": {
            "total_exits": len(exits),
            "delisted": len(delisted),
            "dropped_out_still_listed": len(dropped),
            "failure_to_zero": len(ftz),
        },
        "exits": exits,
    }
    (DOCS_DIR / "kr_top500_delisted_manifest.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    lines: list[str] = []
    lines.append("# KR Top-500 (KOSPI+KOSDAQ, ADV-ranked) Delisting Manifest")
    lines.append("")
    lines.append(
        f"Universe span: {months[0] if months else '?'} .. {months[-1] if months else '?'}."
    )
    lines.append("")
    lines.append(
        "An *exit* is a name that was a top-500 member in some month and whose tenure "
        "ended before the final month. `delisted` = no longer listed on KOSPI/KOSDAQ "
        "today (survivorship-relevant); `dropped_out` = still listed but fell out of the "
        "top-500 by ADV. Delisted names have their terminal OHLCV captured from pykrx "
        "(KRX retains delisted history). `failure_to_zero` flags a terminal drawdown "
        ">= 70% from the trailing-window peak close — the collapse path KR uniquely "
        "makes observable."
    )
    lines.append("")
    lines.append("## Counts")
    lines.append("")
    lines.append(f"- total exits: {len(exits)}")
    lines.append(f"- delisted (no longer listed): {len(delisted)}")
    lines.append(f"- dropped out (still listed): {len(dropped)}")
    lines.append(f"- failure-to-zero (collapse captured): {len(ftz)}")
    lines.append("")
    lines.append("## Delisted names")
    lines.append("")
    lines.append("| ticker | name | market | first | last | months | terminal_dd | failure_to_zero | last_ohlcv |")
    lines.append("|---|---|---|---|---|---|---|---|---|")
    for e in delisted:
        lines.append(
            f"| {e['ticker']} | {e.get('name','')} | {e['market']} | {e['first_month']} | "
            f"{e['last_month']} | {e['months_in_universe']} | "
            f"{e.get('terminal_drawdown','')} | {e.get('failure_to_zero','')} | "
            f"{e.get('last_ohlcv_date','')} |"
        )
    lines.append("")
    (DOCS_DIR / "kr_top500_delisted_manifest.md").write_text(
        "\n".join(lines), encoding="utf-8"
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--today", default=date.today().isoformat())
    ap.add_argument("--resolve-corps", action="store_true",
                    help="download corpCode.xml once and map missing tickers -> corp_code")
    ap.add_argument("--no-collapse", action="store_true",
                    help="skip terminal OHLCV capture (avoids pykrx calls)")
    args = ap.parse_args()
    today = date.fromisoformat(args.today)

    per_ticker, months = _load_universe()
    if not months:
        print("No universe CSVs found under", UNIVERSE_DIR)
        return 1
    union = sorted(per_ticker)
    cached = _cached_tickers()
    missing = sorted(t for t in union if t not in cached)

    print(f"universe months: {len(months)} ({months[0]}..{months[-1]})")
    print(f"union tickers: {len(union)}; cached(240-set): {len(cached & set(union))}; "
          f"missing(need DART): {len(missing)}")

    corp_map: dict[str, str | None] = {}
    if args.resolve_corps:
        corp_map = _resolve_corp_codes(missing)
        resolved = sum(1 for v in corp_map.values() if v)
        print(f"corp_code resolved: {resolved}/{len(missing)}")

    import pandas as pd

    rows = []
    for t in missing:
        rec = per_ticker[t]
        rows.append({
            "ticker": t,
            "corp_code": corp_map.get(t, ""),
            "market": rec["market"],
            "name": rec["name"],
            "first_month": rec["first"],
            "last_month": rec["last"],
            "months_in_universe": len(rec["months"]),
        })
    MISSING_CSV.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(MISSING_CSV, index=False, encoding="utf-8")
    print("wrote", MISSING_CSV)

    exits = _classify_exits(per_ticker, months, today, capture_collapse=not args.no_collapse)
    _write_manifest(exits, months)
    print("wrote docs/kr_top500_delisted_manifest.{md,json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
