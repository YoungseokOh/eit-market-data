#!/usr/bin/env python3
"""Additively merge S&P MidCap 400 mid-caps into existing US snapshot bundles.

WHY ADDITIVE (never rebuild): the existing 90 US bundles carry frozen
S&P 500 ∪ Nasdaq-100 data. Re-fetching an existing ticker from yfinance *now*
re-adjusts it to today's split/dividend basis and corrupts the frozen values
(the CRWD/DD 2022-12 seam drift). So for each month this script:

  1. computes the point-in-time S&P MidCap 400 membership (as of that month's
     stored ``decision_date``),
  2. keeps only names NOT already in that month's universe (share-class CIK
     duplicates against the existing universe are dropped too),
  3. fetches ONLY those new mid-caps (prices + fundamentals + filing + sector),
  4. MERGES them into the on-disk ``snapshot.json`` by operating on the raw JSON
     dict — existing per-ticker entries are never re-serialized, so their bytes
     are unchanged. Only ``universe`` (append), the new ``prices``/
     ``fundamentals``/``filings``/``sector_map`` keys, and the derived
     ``sector_averages`` / hashes change.

RESUMABLE + CHECKPOINTED: names already present in a month's ``universe`` are
skipped, and new names are fetched in small chunks; the bundle is re-written
after every chunk (atomic temp+rename). A kill therefore loses at most one
in-flight chunk. Re-running is idempotent.

Usage:
    python scripts/merge_us_sp400.py --start-month 2019-01 --end-month 2026-06

Env:
    EIT_US_DELISTED_FALLBACK=1   recover delisted mid-cap prices (stockanalysis.com)
    EIT_EDGAR_FILING_CACHE=1     cache EDGAR filing text
    SEC_EDGAR_USER_AGENT, FRED_API_KEY   as usual
"""

from __future__ import annotations

import argparse
import asyncio
import gzip
import json
import logging
import os
import sys
from datetime import date
from pathlib import Path
from typing import Any

import socket as _socket

_socket.setdefaulttimeout(90)

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import bootstrap, month_range, write_json

PROJECT_ROOT = bootstrap()

from eit_market_data.core.hashing import _content_hash
from eit_market_data.schemas.snapshot import FundamentalData, SectorAverages
from eit_market_data.snapshot import create_real_providers
from eit_market_data.us_universe import _dedup_by_cik, _ticker_cik_map, pit_sp400

logger = logging.getLogger(__name__)

SP400_CSV_DIR = PROJECT_ROOT / "universes" / "us" / "sp400"


def _sector_averages_from_funds(sector: str, funds: list[FundamentalData]) -> SectorAverages:
    """Sector average metrics from already-frozen fundamentals (no new fetches).

    Mirrors ``scripts/build_us_batch._sector_averages_from_funds`` so the merged
    bundle's ``sector_averages`` are computed the same PIT-safe way, purely from
    the bundle's own stored fundamentals.
    """
    from statistics import mean as _mean

    metrics: dict[str, list[float]] = {}

    def _add(key: str, val: float | None) -> None:
        if val is not None:
            metrics.setdefault(key, []).append(val)

    for fund in funds:
        if not fund or not fund.quarters:
            continue
        q = fund.quarters[0]
        rev = q.revenue
        ta = q.total_assets
        if not rev or not ta or ta == 0:
            continue
        _add("roa", (q.net_income or 0) / ta if ta else None)
        _add("roe", (q.net_income or 0) / q.total_equity if q.total_equity else None)
        _add("gross_margin", (q.gross_profit or 0) / rev)
        _add("operating_margin", (q.operating_income or 0) / rev)
        _add("net_margin", (q.net_income or 0) / rev)
        if q.current_liabilities and q.current_liabilities > 0:
            _add("current_ratio", (q.current_assets or 0) / q.current_liabilities)
        if q.total_equity and q.total_equity > 0:
            _add("debt_to_equity", (q.total_debt or 0) / q.total_equity)
        _add("asset_turnover", rev / ta)
        if fund.last_close_price and q.eps and q.eps > 0:
            _add("pe_ttm", fund.last_close_price / (q.eps * 4))

    avg = {k: round(float(_mean(vals)), 4) for k, vals in metrics.items() if vals}
    return SectorAverages(sector=sector, avg_metrics=avg)


async def _fetch_new_entries(
    providers: dict[str, Any],
    tickers: list[str],
    decision_date: date,
) -> dict[str, dict[str, Any]]:
    """Fetch prices/fundamentals/filing/sector for *new* names only.

    Returns JSON-ready dicts keyed by ticker under 'prices','fundamentals',
    'filings','sector_map'. Only new names are touched — existing bundle data is
    never re-fetched.
    """
    price = providers["price_provider"]
    fund = providers["fundamental_provider"]
    filing = providers["filing_provider"]
    sector = providers["sector_provider"]

    sem = asyncio.Semaphore(16)

    async def _lim(coro: Any) -> Any:
        async with sem:
            return await coro

    price_tasks = [_lim(price.fetch_prices(t, decision_date)) for t in tickers]
    fund_tasks = [_lim(fund.fetch_fundamentals(t, decision_date)) for t in tickers]
    filing_tasks = [_lim(filing.fetch_filing(t, decision_date)) for t in tickers]
    sector_task = sector.fetch_sector_map(tickers, as_of=decision_date)

    results = await asyncio.gather(
        *price_tasks, *fund_tasks, *filing_tasks, sector_task
    )
    n = len(tickers)
    all_prices = results[:n]
    all_funds = results[n : 2 * n]
    all_filings = results[2 * n : 3 * n]
    sector_map = results[3 * n]

    out_prices: dict[str, Any] = {}
    out_funds: dict[str, Any] = {}
    out_filings: dict[str, Any] = {}
    for t, bars, f, fl in zip(tickers, all_prices, all_funds, all_filings, strict=True):
        out_prices[t] = [b.model_dump(mode="json") for b in (bars or [])]
        out_funds[t] = f.model_dump(mode="json")
        out_filings[t] = fl.model_dump(mode="json")

    return {
        "prices": out_prices,
        "fundamentals": out_funds,
        "filings": out_filings,
        "sector_map": {t: sector_map.get(t, "Unknown") for t in tickers},
    }


def _recompute_derived(snap: dict[str, Any]) -> None:
    """Recompute sector_averages + metadata/input hashes in place.

    Everything here is DERIVED from the (now merged) per-ticker data, so
    recomputing does not violate the byte-identity guarantee on existing tickers'
    prices/fundamentals/filings.
    """
    # sector_averages: reconstruct FundamentalData objects from the merged JSON.
    sector_map: dict[str, str] = snap.get("sector_map", {})
    by_sector: dict[str, list[str]] = {}
    for t, s in sector_map.items():
        by_sector.setdefault(s, []).append(t)

    funds_json: dict[str, Any] = snap.get("fundamentals", {})
    sector_averages: dict[str, Any] = {}
    for s, tickers in by_sector.items():
        funds = [
            FundamentalData(**funds_json[t])
            for t in tickers
            if t in funds_json
        ]
        sector_averages[s] = _sector_averages_from_funds(s, funds).model_dump(mode="json")
    snap["sector_averages"] = sector_averages

    # Hashes (derived reproducibility fields).
    prices = snap.get("prices", {})
    fundamentals = snap.get("fundamentals", {})
    filings = snap.get("filings", {})
    snap["input_hash"] = _content_hash(
        {"decision_date": str(snap["decision_date"]), "universe": sorted(snap["universe"])}
    )
    meta = snap.setdefault("metadata", {})
    meta["price_hash"] = _content_hash({t: len(p) for t, p in prices.items()})
    meta["fundamental_hash"] = _content_hash(
        {t: len(f.get("quarters", [])) for t, f in fundamentals.items()}
    )
    meta["filing_hash"] = _content_hash(
        {
            t: bool(
                f.get("business_overview")
                or f.get("risks")
                or f.get("mda")
                or f.get("governance")
            )
            for t, f in filings.items()
        }
    )


def _atomic_write(path: Path, text: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def _persist_snapshot(month_dir: Path, snap: dict[str, Any], had_gz: bool) -> None:
    text = json.dumps(snap, indent=2, sort_keys=True)
    _atomic_write(month_dir / "snapshot.json", text)
    if had_gz:
        tmp = month_dir / "snapshot.json.gz.tmp"
        tmp.write_bytes(gzip.compress(text.encode("utf-8")))
        tmp.replace(month_dir / "snapshot.json.gz")
    # metadata.json (mirror the hashes we recomputed)
    meta_path = month_dir / "metadata.json"
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text())
        except (json.JSONDecodeError, OSError):
            meta = {}
        # metadata.json layout differs between build paths; only refresh the
        # hash block if present, plus a universe echo when the file carries one.
        for k in ("price_hash", "fundamental_hash", "filing_hash"):
            if k in snap.get("metadata", {}):
                # nested (build_us_snapshot) vs flat (build_us_batch)
                if "snapshot_metadata" in meta and isinstance(meta["snapshot_metadata"], dict):
                    meta["snapshot_metadata"][k] = snap["metadata"][k]
                else:
                    meta[k] = snap["metadata"][k]
        if "universe" in meta:
            meta["universe"] = snap["universe"]
        _atomic_write(meta_path, json.dumps(meta, indent=2, sort_keys=True))
    # summary.json: refresh counts if it exists (best-effort, non-contractual)
    summary_path = month_dir / "summary.json"
    if summary_path.exists():
        try:
            summary = json.loads(summary_path.read_text())
            if isinstance(summary, dict) and summary.get("status") == "ok":
                summary["universe_size"] = len(snap["universe"])
                summary["price_tickers"] = len(snap.get("prices", {}))
                summary["fundamental_tickers"] = len(snap.get("fundamentals", {}))
                summary["sp400_merged"] = True
                _atomic_write(summary_path, json.dumps(summary, indent=2, sort_keys=True))
        except (json.JSONDecodeError, OSError):
            pass


def _dedup_new_against_existing(
    members: set[str], existing: list[str]
) -> list[str]:
    """New S&P 400 names not already represented in the existing universe.

    Drops (a) names literally already in the universe and (b) share-class
    duplicates whose CIK is already represented by an existing member (so a
    company is never double-counted across the large-cap + mid-cap union).
    Never removes or reorders existing names.
    """
    existing_set = set(existing)
    cik_map = _ticker_cik_map()
    existing_ciks = {cik_map.get(t) for t in existing_set if cik_map.get(t)}
    # First collapse share-class dupes *within* the new mid-cap set itself.
    new_only = _dedup_by_cik({m for m in members if m not in existing_set})
    out: list[str] = []
    for t in new_only:
        cik = cik_map.get(t)
        if cik is not None and cik in existing_ciks:
            continue  # same issuer already in universe under another symbol
        out.append(t)
    return sorted(out)


def _write_sp400_csv(month: str, members: list[str], decision_date: date) -> None:
    SP400_CSV_DIR.mkdir(parents=True, exist_ok=True)
    lines = ["ticker,as_of,source"]
    for t in sorted(members):
        lines.append(f"{t},{decision_date.isoformat()},wikipedia_sp400_pit")
    _atomic_write(SP400_CSV_DIR / f"{month}.csv", "\n".join(lines) + "\n")


async def merge_month(
    month: str,
    month_dir: Path,
    chunk_size: int,
    fundamentals_source: str,
) -> dict[str, Any]:
    snap_path = month_dir / "snapshot.json"
    snap = json.loads(snap_path.read_text())
    had_gz = (month_dir / "snapshot.json.gz").exists()
    decision_date = date.fromisoformat(snap["decision_date"])

    members = pit_sp400(decision_date)
    deduped_members = _dedup_by_cik(members)
    _write_sp400_csv(month, deduped_members, decision_date)

    new_names = _dedup_new_against_existing(members, snap["universe"])
    already = [t for t in new_names if t in set(snap["universe"])]
    to_fetch = [t for t in new_names if t not in set(snap["universe"])]

    logger.info(
        "[%s] decision=%s sp400=%d new_candidates=%d to_fetch=%d already=%d",
        month, decision_date, len(deduped_members), len(new_names),
        len(to_fetch), len(already),
    )

    if not to_fetch:
        # Still refresh derived fields once in case a prior partial run left them
        # stale, then persist (idempotent).
        _recompute_derived(snap)
        _persist_snapshot(month_dir, snap, had_gz)
        return {"month": month, "added": 0, "already_present": len(already)}

    added = 0
    for i in range(0, len(to_fetch), chunk_size):
        chunk = to_fetch[i : i + chunk_size]
        # Fresh providers per chunk so a transient Yahoo failure does not poison
        # cached ticker metadata for the rest of the run.
        providers = create_real_providers(fundamentals_source=fundamentals_source)
        entries = await _fetch_new_entries(providers, chunk, decision_date)

        for t in chunk:
            if t in snap["universe"]:
                continue
            snap["universe"].append(t)
            snap["prices"][t] = entries["prices"][t]
            snap["fundamentals"][t] = entries["fundamentals"][t]
            snap["filings"][t] = entries["filings"][t]
            snap["sector_map"][t] = entries["sector_map"][t]
            added += 1

        _recompute_derived(snap)
        _persist_snapshot(month_dir, snap, had_gz)
        priced = sum(1 for t in chunk if snap["prices"].get(t))
        logger.info(
            "[%s] chunk %d-%d/%d merged (+%d, %d/%d priced) -> universe=%d",
            month, i, i + len(chunk), len(to_fetch), len(chunk),
            priced, len(chunk), len(snap["universe"]),
        )

    return {"month": month, "added": added, "already_present": len(already)}


async def run(
    start_month: str,
    end_month: str,
    artifacts_root: Path,
    chunk_size: int,
    fundamentals_source: str,
) -> None:
    months = month_range(start_month, end_month)
    logger.info("merge target: %d months (%s..%s)", len(months), start_month, end_month)
    for idx, month in enumerate(months, start=1):
        month_dir = artifacts_root / "us" / "snapshots" / month
        if not (month_dir / "snapshot.json").exists():
            logger.warning("[%s] (%d/%d) no existing snapshot.json, skip", month, idx, len(months))
            continue
        try:
            res = await merge_month(month, month_dir, chunk_size, fundamentals_source)
            logger.info("[%s] (%d/%d) DONE added=%d", month, idx, len(months), res["added"])
        except Exception as exc:  # noqa: BLE001
            logger.exception("[%s] FAIL: %s", month, exc)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-month", default="2019-01")
    parser.add_argument("--end-month", default="2026-06")
    parser.add_argument("--artifacts-root", default=str(PROJECT_ROOT / "artifacts"))
    parser.add_argument("--chunk-size", type=int, default=25)
    parser.add_argument("--fundamentals-source", default="edgar_xbrl",
                        choices=["yfinance", "edgar_xbrl"])
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger.info(
        "delisted_fallback=%s filing_cache=%s",
        os.getenv("EIT_US_DELISTED_FALLBACK"), os.getenv("EIT_EDGAR_FILING_CACHE"),
    )

    asyncio.run(
        run(
            start_month=args.start_month,
            end_month=args.end_month,
            artifacts_root=Path(args.artifacts_root),
            chunk_size=args.chunk_size,
            fundamentals_source=args.fundamentals_source,
        )
    )


if __name__ == "__main__":
    main()
