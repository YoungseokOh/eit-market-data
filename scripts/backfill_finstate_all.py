"""Controlled DART ``finstate_all`` (full financial statements) backfill.

The cached ``finstate:`` entries are DART's *major-accounts* endpoint
(fnlttSinglAcnt) — BS/IS headline lines only. They lack 매출원가 (COGS),
매출총이익 (gross profit), and the cash-flow statement (영업활동현금흐름 / OCF),
which the quality (Novy-Marx GP/A) and accruals (Sloan) factors need. Those
line items live in the *full-statements* endpoint (fnlttSinglAcntAll /
``finstate_all``). This script fetches ``finstate_all`` for exactly the
(corp, year, reprt) periods already present as ``finstate`` keys and caches
them under ``finstate_all:{corp}:{year}:{reprt}:{fs_div}``.

STRICTLY follows .claude/rules/dart-api-limits.md:
- explicit universe (the existing finstate combos), skip already-cached,
- >= --delay seconds between live calls (default 5.5s),
- resumable via a progress file (--progress),
- STOP immediately on the first DART transient (never hammer),
- one live call per (corp, year, reprt) pair (CFS; OFS fallback only if CFS empty).

Usage:
    python scripts/backfill_finstate_all.py --delay 5.5 --progress data/finstate_all_progress.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT / "src"))

import diskcache  # noqa: E402

from eit_market_data.local_collection import _is_dart_transient_error  # noqa: E402

_CACHE_DIR = os.environ.get("EIT_DART_CACHE_DIR", str(_REPO_ROOT / "data" / "dart_cache"))
_SIZE_LIMIT = int(os.environ.get("EIT_DART_CACHE_SIZE_LIMIT_BYTES", str(50 * 1024 * 1024 * 1024)))


def _combos_from_cache(cache: diskcache.Cache) -> list[tuple[str, str, str]]:
    """Distinct (corp, year, reprt) already present as finstate: keys."""
    combos: set[tuple[str, str, str]] = set()
    for k in cache:
        if isinstance(k, str) and k.startswith("finstate:"):
            parts = k.split(":")
            if len(parts) >= 4:
                combos.add((parts[1], parts[2], parts[3]))
    return sorted(combos)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--delay", type=float, default=5.5, help="Seconds between live DART calls (>=5).")
    ap.add_argument("--progress", default=str(_REPO_ROOT / "data" / "finstate_all_progress.json"))
    ap.add_argument("--limit", type=int, default=0, help="Max live calls this run (0 = no cap).")
    args = ap.parse_args()

    key = os.environ.get("DART_API_KEY")
    if not key:
        print("DART_API_KEY not set (source .env)")
        return 2
    import OpenDartReader

    dart = OpenDartReader(key)
    cache = diskcache.Cache(_CACHE_DIR, size_limit=_SIZE_LIMIT)

    progress_path = Path(args.progress)
    progress = {}
    if progress_path.exists():
        progress = json.loads(progress_path.read_text())
    done = set(progress.get("completed", []))
    empty = set(progress.get("empty", []))

    combos = _combos_from_cache(cache)
    todo = [c for c in combos if f"{c[0]}:{c[1]}:{c[2]}" not in done]
    print(f"[finstate_all] combos={len(combos)} done={len(done)} todo={len(todo)} delay={args.delay}s")

    live_calls = 0
    stopped = None
    for corp, year, reprt in todo:
        tag = f"{corp}:{year}:{reprt}"
        # skip if any finstate_all bucket already cached for this period
        if any(f"finstate_all:{corp}:{year}:{reprt}:{d}" in cache for d in ("CFS", "OFS")):
            done.add(tag)
            continue
        got = False
        for fs_div in ("CFS", "OFS"):
            try:
                df = dart.finstate_all(corp, int(year), reprt_code=reprt, fs_div=fs_div)
            except TypeError:
                try:
                    df = dart.finstate_all(corp, int(year), reprt_code=reprt)
                except Exception as exc:  # noqa: BLE001
                    if _is_dart_transient_error(exc):
                        stopped = f"transient at {tag} ({fs_div}): {exc}"
                        break
                    df = None
            except Exception as exc:  # noqa: BLE001
                if _is_dart_transient_error(exc):
                    stopped = f"transient at {tag} ({fs_div}): {exc}"
                    break
                df = None
            live_calls += 1
            if df is not None and len(df) > 0:
                cache.set(f"finstate_all:{corp}:{year}:{reprt}:{fs_div}", df)
                got = True
                time.sleep(args.delay)
                break
            time.sleep(args.delay)
        if stopped:
            break
        if got:
            done.add(tag)
        else:
            empty.add(tag)
            done.add(tag)  # don't re-probe an empty period every run
        if live_calls % 25 == 0:
            progress["completed"] = sorted(done)
            progress["empty"] = sorted(empty)
            progress_path.write_text(json.dumps(progress, ensure_ascii=False))
            print(f"[finstate_all] live={live_calls} done={len(done)} empty={len(empty)} last={tag}")
        if args.limit and live_calls >= args.limit:
            print(f"[finstate_all] hit --limit {args.limit}")
            break

    progress["completed"] = sorted(done)
    progress["empty"] = sorted(empty)
    progress["stopped"] = stopped
    progress_path.write_text(json.dumps(progress, ensure_ascii=False))
    cache.close()
    if stopped:
        print(f"[finstate_all] STOPPED (transient) — {stopped}")
        print("[finstate_all] Do NOT relaunch immediately; verify DART is reachable first.")
        return 3
    print(f"[finstate_all] pass complete: live_calls={live_calls} done={len(done)} empty={len(empty)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
