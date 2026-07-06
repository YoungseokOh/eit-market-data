#!/usr/bin/env python3
# ruff: noqa: E402
"""Safely backfill DART diskcache for an explicit ticker universe."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from datetime import date
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import bootstrap

PROJECT_ROOT = bootstrap()

from eit_market_data.kr.dart_provider import DartProvider, _DART_CACHE_SIZE_LIMIT
from eit_market_data.kr.market_helpers import normalize_ticker
from eit_market_data.schemas.snapshot import FilingData, FundamentalData
from eit_market_data.local_collection import _is_dart_transient_error


DEFAULT_CACHE_DIR = PROJECT_ROOT / "data" / "dart_cache"
DEFAULT_DELAY_SECONDS = 5.0
FILING_MODES = {"optional", "strict", "skip"}


def _load_tickers(path: Path) -> list[str]:
    import pandas as pd

    frame = pd.read_csv(path, dtype={"ticker": str})
    if "ticker" not in frame.columns:
        raise ValueError(f"{path} must contain a ticker column")
    tickers = []
    for raw in frame["ticker"]:
        ticker = normalize_ticker(str(raw))
        if ticker.strip():
            tickers.append(ticker)
    return sorted(dict.fromkeys(tickers))


def _load_progress(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"completed": [], "failed": [], "stopped": None}
    return json.loads(path.read_text(encoding="utf-8"))


def _save_progress(path: Path, progress: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(progress, ensure_ascii=False, indent=2), encoding="utf-8")


def _record_once(progress: dict[str, Any], key: str, value: Any) -> None:
    values = progress.setdefault(key, [])
    if value not in values:
        values.append(value)


def _progress_set(progress: dict[str, Any], key: str) -> set[str]:
    return {str(value) for value in progress.get(key, [])}


def _cache_get(cache: Any, key: str) -> Any:
    try:
        return cache.get(key)
    except Exception:
        return None


def _has_fundamental(cache: Any, ticker: str, ym: str, min_quarters: int) -> bool:
    value = _cache_get(cache, f"fundamental:{ticker}:{ym}")
    return isinstance(value, FundamentalData) and len(value.quarters) >= min_quarters


def _has_filing(cache: Any, ticker: str, ym: str) -> bool:
    value = _cache_get(cache, f"filing:{ticker}:{ym}")
    return isinstance(value, FilingData) and bool(value.business_overview)


def _is_empty_filing_error(exc: Exception) -> bool:
    return "DART filing returned empty" in f"{exc.__class__.__name__}: {exc}"


def _is_empty_fundamental_error(exc: Exception) -> bool:
    return "DART fundamentals returned empty" in f"{exc.__class__.__name__}: {exc}"


def _should_stop_live(exc: Exception, *, filing_mode: str = "strict") -> bool:
    if _is_dart_transient_error(exc):
        return True
    text = f"{exc.__class__.__name__}: {exc}"
    if _is_empty_filing_error(exc) and filing_mode != "strict":
        return False
    return (
        "DART fundamentals returned empty" in text
        or "DART filing returned empty" in text
        or "'status': '013'" in text
        or '"status": "013"' in text
    )


def _sleep_after_call(delay_seconds: float) -> None:
    if delay_seconds > 0:
        time.sleep(delay_seconds)


def run(args: argparse.Namespace) -> int:
    import diskcache

    if args.filing_mode not in FILING_MODES:
        raise ValueError(f"--filing-mode must be one of {sorted(FILING_MODES)}")

    as_of = date.fromisoformat(args.as_of)
    # NOTE: DART cache entries are bucketed by the *collection* month (this as_of),
    # not by the filing's real rcept_dt. A bucket can therefore hold a record whose
    # real filing_date/report_date is after as_of (e.g. an annual report first seen
    # during a later collection run). Point-in-time correctness is enforced at read
    # time by DartProvider / CacheOnlyDartProvider, which validate each record's own
    # date against as_of; the bucket month is only a coarse pre-filter. Do not treat
    # the bucket month as the authoritative as-of of the record it contains.
    ym = as_of.strftime("%Y%m")
    tickers = _load_tickers(args.universe_csv)
    cache = diskcache.Cache(str(args.cache_dir), size_limit=_DART_CACHE_SIZE_LIMIT)
    provider = DartProvider(allow_stale_fallback=False, raise_on_error=True)
    progress = _load_progress(args.progress)
    progress["stopped"] = None
    completed = _progress_set(progress, "completed")
    fundamental_completed = _progress_set(progress, "fundamental_completed") | completed
    filing_completed = _progress_set(progress, "filing_completed") | completed
    filing_empty = _progress_set(progress, "filing_empty")

    print(
        f"[DART] universe={len(tickers)} as_of={as_of.isoformat()} "
        f"ym={ym} delay={args.delay}s quarters={args.quarters} "
        f"filing_mode={args.filing_mode}"
    )
    print(f"[DART] progress={args.progress}")

    attempted = 0
    consecutive_empty = 0
    for ticker in tickers:
        if ticker in completed:
            continue
        has_fundamental = ticker in fundamental_completed or _has_fundamental(
            cache,
            ticker,
            ym,
            args.quarters,
        )
        has_filing = (
            args.filing_mode == "skip"
            or ticker in filing_completed
            or (args.filing_mode == "optional" and ticker in filing_empty)
            or _has_filing(cache, ticker, ym)
        )
        need_fundamental = not has_fundamental
        need_filing = not has_filing
        if not need_fundamental and not need_filing:
            consecutive_empty = 0
            _record_once(progress, "completed", ticker)
            completed.add(ticker)
            continue
        if args.max_tickers is not None and attempted >= args.max_tickers:
            break

        attempted += 1
        progress["current_ticker"] = ticker
        _save_progress(args.progress, progress)
        print(
            f"[DART] {ticker} fundamental={'yes' if need_fundamental else 'cached'} "
            f"filing={'yes' if need_filing else 'cached'}"
        )
        try:
            result: dict[str, Any] = {"ticker": ticker}
            if need_fundamental:
                fundamental = asyncio.run(
                    provider.fetch_fundamentals(ticker, as_of, n_quarters=args.quarters)
                )
                result["quarters"] = len(fundamental.quarters)
                _record_once(progress, "fundamental_completed", ticker)
                fundamental_completed.add(ticker)
                _sleep_after_call(args.delay)
            else:
                result["quarters"] = "cached"

            if need_filing:
                try:
                    filing = asyncio.run(provider.fetch_filing(ticker, as_of))
                    result["filing"] = bool(filing.business_overview)
                    if filing.business_overview:
                        _record_once(progress, "filing_completed", ticker)
                        filing_completed.add(ticker)
                    elif args.filing_mode == "optional":
                        _record_once(progress, "filing_empty", ticker)
                        filing_empty.add(ticker)
                    else:
                        raise RuntimeError(f"DART filing returned empty for {ticker}")
                except Exception as filing_exc:
                    if _is_empty_filing_error(filing_exc) and args.filing_mode == "optional":
                        result["filing"] = "empty"
                        _record_once(progress, "filing_empty", ticker)
                        filing_empty.add(ticker)
                        _record_once(
                            progress,
                            "failed",
                            {
                                "ticker": ticker,
                                "field": "filing",
                                "error": f"{filing_exc.__class__.__name__}: {filing_exc}",
                            },
                        )
                    else:
                        raise
                finally:
                    _sleep_after_call(args.delay)
            else:
                result["filing"] = "cached"

            print(f"[DART] ok {result}")
            consecutive_empty = 0
            _record_once(progress, "completed", ticker)
            completed.add(ticker)
            progress["last_ticker"] = ticker
            progress["current_ticker"] = None
            _save_progress(args.progress, progress)
        except Exception as exc:
            item = {
                "ticker": ticker,
                "error": f"{exc.__class__.__name__}: {exc}",
            }
            if _is_empty_fundamental_error(exc):
                item["field"] = "fundamental"
            elif _is_empty_filing_error(exc):
                item["field"] = "filing"
            _record_once(progress, "failed", item)
            progress["last_ticker"] = ticker
            progress["current_ticker"] = None
            if _should_stop_live(exc, filing_mode=args.filing_mode):
                transient = _is_dart_transient_error(exc)
                if args.continue_on_empty and not transient:
                    # Isolated genuine no-data corp (013 / empty fundamental with a
                    # live HTTP 200): record and skip so resume advances past it. A
                    # *run* of consecutive empties may signal an IP throttle/block —
                    # stop then, per .claude/rules/dart-api-limits.md.
                    consecutive_empty += 1
                    _record_once(progress, "empty_skipped", ticker)
                    _record_once(progress, "completed", ticker)
                    completed.add(ticker)
                    progress["current_ticker"] = None
                    _save_progress(args.progress, progress)
                    print(f"[DART] empty-skip {ticker} consec={consecutive_empty}")
                    if consecutive_empty >= args.max_consecutive_empty:
                        progress["stopped"] = {"reason": "consecutive_empty",
                                               "count": consecutive_empty, **item}
                        _save_progress(args.progress, progress)
                        print(f"[DART] STOP consecutive_empty={consecutive_empty} "
                              "(possible throttle/block)")
                        return 2
                    time.sleep(args.delay)
                    continue
                progress["stopped"] = item
                _save_progress(args.progress, progress)
                print(f"[DART] live stop {item}")
                return 2
            print(f"[DART] failed {item}")
            _save_progress(args.progress, progress)
            time.sleep(args.delay)

    _save_progress(args.progress, progress)
    print(
        f"[DART] done completed={len(progress.get('completed', []))} "
        f"failed={len(progress.get('failed', []))}"
    )
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Safely backfill DART diskcache for an explicit ticker universe.",
    )
    parser.add_argument("--universe-csv", type=Path, required=True)
    parser.add_argument("--as-of", required=True)
    parser.add_argument("--progress", type=Path, required=True)
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY_SECONDS)
    parser.add_argument("--quarters", type=int, default=8)
    parser.add_argument("--max-tickers", type=int)
    parser.add_argument(
        "--continue-on-empty",
        action="store_true",
        help=(
            "Treat an isolated empty/013 fundamental as a genuine no-data corp: record "
            "it (empty_skipped + completed) and continue instead of stopping. A run of "
            "--max-consecutive-empty empties still stops (possible throttle/block)."
        ),
    )
    parser.add_argument(
        "--max-consecutive-empty",
        type=int,
        default=8,
        help="With --continue-on-empty, stop after this many consecutive empties.",
    )
    parser.add_argument(
        "--filing-mode",
        choices=sorted(FILING_MODES),
        default="optional",
        help=(
            "strict stops on empty filing text, optional records filing_empty and continues, "
            "skip avoids filing document calls."
        ),
    )
    raise SystemExit(run(parser.parse_args()))


if __name__ == "__main__":
    main()
