# ruff: noqa: E402
"""Offline re-bucketing of DART fundamentals to an early 2019 anchor.

Purpose
-------
``CacheOnlyDartProvider._lookup`` serves the LATEST ``fundamental:<ticker>:<YYYYMM>``
bucket whose month is ``<= as_of`` month. The earliest existing fundamental bucket
is ``202301``, so a 2019-2022 ``as_of`` finds no fundamentals at all. This script
re-derives each cached ticker's COMPLETE quarterly history OFFLINE from the raw
``finstate:`` cache (years 2015-2022 are present) and stores it under an early
anchor bucket ``fundamental:<ticker>:<ANCHOR>`` (default ``201901``).

Point-in-time safety
---------------------
The stored blob carries every quarter with ``report_date <= ANCHOR_ASOF`` (default
2022-12-31). The per-record PIT guard in ``CacheOnlyDartProvider.fetch_fundamentals``
(``report_date <= as_of``) does the real filtering at build time, so anchoring the
full blob early leaks no look-ahead information. Only ``fundamental:`` buckets are
written; ``issued_shares`` is intentionally EXCLUDED (it has no per-record date
guard, so an early anchor would be a look-ahead risk).

No rate-limited OpenDART DATA endpoints are called: ``list``/``finstate``/``document``
are hard-blocked and only the local diskcache is read. Idempotent: an existing
anchor bucket is skipped unless ``--force``.

Usage:
    python scripts/rebucket_dart_fundamentals_2019.py [--anchor 201901] [--force]
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import bootstrap

PROJECT_ROOT = bootstrap()

import diskcache

from eit_market_data.kr.dart_provider import (
    DartProvider,
    _DART_CACHE_DIR,
    _DART_CACHE_SIZE_LIMIT,
)
from eit_market_data.kr.market_helpers import normalize_ticker
from eit_market_data.schemas.snapshot import FundamentalData

ANCHOR_ASOF = date(2022, 12, 31)
N_QUARTERS = 60


def _build_offline_maps(cache: diskcache.Cache) -> tuple[dict[str, str], dict[str, object]]:
    """Return (ticker->corp_code, corp_code->richest reports frame), from cache."""
    ticker_to_corp: dict[str, str] = {}
    best_reports: dict[str, tuple[int, object]] = {}
    for key in cache.iterkeys():
        text = str(key)
        if not text.startswith("reports:"):
            continue
        corp = text.split(":")[1]
        frame = cache.get(text)
        if frame is None or not hasattr(frame, "columns"):
            continue
        if "stock_code" in frame.columns and len(frame) > 0:
            try:
                stock = normalize_ticker(str(frame.iloc[0].get("stock_code", "")))
                if stock and stock != "000000":
                    ticker_to_corp[stock] = corp
            except Exception:
                pass
        n = len(frame)
        if corp not in best_reports or n > best_reports[corp][0]:
            best_reports[corp] = (n, frame)
    return ticker_to_corp, {c: fr for c, (_, fr) in best_reports.items()}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--anchor", default="201901", help="Anchor bucket month YYYYMM (default 201901).")
    ap.add_argument("--force", action="store_true", help="Overwrite an existing anchor bucket.")
    args = ap.parse_args()
    anchor = args.anchor

    cache = diskcache.Cache(str(_DART_CACHE_DIR), size_limit=_DART_CACHE_SIZE_LIMIT)
    ticker_to_corp, best_reports = _build_offline_maps(cache)
    print(f"[rebucket] offline maps: {len(ticker_to_corp)} tickers, {len(best_reports)} corp report frames")

    os.environ.setdefault("DART_API_KEY", "offline")
    prov = DartProvider(allow_stale_fallback=True, raise_on_error=False)

    def _blocked(*_a: object, **_k: object) -> object:
        raise RuntimeError("network blocked (offline re-derivation)")

    prov._dart.list = _blocked
    prov._dart.finstate = _blocked
    prov._dart.document = _blocked
    prov._fetch_report_list = lambda corp, as_of: best_reports.get(corp)  # type: ignore[assignment]
    prov._corp_cache = dict(ticker_to_corp)

    written = skipped = empty = 0
    span_min: date | None = None
    span_max: date | None = None
    for ticker in sorted(ticker_to_corp):
        anchor_key = f"fundamental:{ticker}:{anchor}"
        if not args.force and cache.get(anchor_key) is not None:
            skipped += 1
            continue
        fd = prov._fetch_fundamentals_sync(ticker, ANCHOR_ASOF, N_QUARTERS)
        if not fd.quarters:
            empty += 1
            continue
        cache.set(anchor_key, fd, expire=None)
        written += 1
        rds = [q.report_date for q in fd.quarters]
        lo, hi = min(rds), max(rds)
        span_min = lo if span_min is None else min(span_min, lo)
        span_max = hi if span_max is None else max(span_max, hi)

    print(
        f"[rebucket] anchor={anchor} written={written} skipped={skipped} "
        f"empty={empty} report_date_span={span_min}..{span_max}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
