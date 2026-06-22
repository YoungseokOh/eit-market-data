#!/usr/bin/env python3
# ruff: noqa: E402
"""Materialize a per-ticker daily OHLCV price store for a market.

The store is the daily-cadence companion to the month-end snapshot bundles.
It writes ONE file per ticker, ``artifacts/<market>/daily_prices/<TICKER>.csv``,
with the exact schema the consumer (``eit.data.daily_prices.DailyPriceStore``)
reads::

    date,open,high,low,close,volume

``date`` is ISO ``YYYY-MM-DD``, rows ascending, one row per trading day. The
consumer filters ``date <= as_of`` itself, so this store just holds correct,
complete daily history.

Consistency with the monthly bundles is guaranteed *by construction*:

1. The store is seeded from the published monthly ``snapshot.json`` ``prices``
   maps (the same bars the consumer already trusts). When two bundles overlap a
   date, the *later* bundle wins, so the store carries the most recent
   split/dividend adjustment basis.
2. The live provider (the SAME provider that produced the bundle prices -
   pykrx for KR, yfinance for US) is used only to APPEND trading days *after*
   the latest bundle date. It never rewrites a bundle-covered bar, so a
   re-fetch whose adjustment basis has drifted can never make the store
   disagree with a published bundle.

The merge is idempotent and incremental: existing rows are preserved verbatim;
re-running only appends genuinely new trading days. Pass ``--rebuild`` to
recompute every row from scratch (e.g. after an intentional basis refresh).
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from dotenv import load_dotenv

load_dotenv(PROJECT_ROOT / ".env")

logger = logging.getLogger(__name__)

ARTIFACTS_ROOT = PROJECT_ROOT / "artifacts"
# Exact header the consumer's DailyPriceStore expects (PRICE_STORE_COLUMNS).
PRICE_STORE_COLUMNS = ["date", "open", "high", "low", "close", "volume"]
STORE_SUBDIR = "daily_prices"
SNAPSHOTS_SUBDIR = "snapshots"
MANIFEST_FILENAME = "_manifest.json"


@dataclass(frozen=True)
class Bar:
    """A single daily OHLCV bar keyed by its trade date."""

    date: date
    open: float
    high: float
    low: float
    close: float
    volume: float

    def as_row(self) -> dict[str, str]:
        return {
            "date": self.date.isoformat(),
            "open": _fmt(self.open),
            "high": _fmt(self.high),
            "low": _fmt(self.low),
            "close": _fmt(self.close),
            "volume": _fmt(self.volume),
        }


def _fmt(value: float) -> str:
    """Render a float without trailing noise, matching bundle rounding."""
    f = float(value)
    if f == int(f):
        return str(int(f))
    return repr(round(f, 6))


# ---------------------------------------------------------------------------
# Universe
# ---------------------------------------------------------------------------


def normalize_store_ticker(market: str, raw: str) -> str:
    """Return the ticker symbol exactly as keyed in the bundle ``prices`` map."""
    ticker = str(raw).strip()
    if market == "kr":
        # KR bundle keys are 6-digit zero-padded codes; preserve alphanumeric
        # KRX codes (e.g. 0126Z0) untouched when they contain non-digits.
        if ticker and ticker.isdigit():
            return ticker.zfill(6)
        return ticker
    return ticker.upper()


def load_universe(market: str, universe_csv: Path | None) -> list[str] | None:
    """Load the target universe, or return None to use whatever the bundles hold."""
    if universe_csv is None:
        return None
    with universe_csv.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames and "ticker" in reader.fieldnames:
            rows = [row.get("ticker", "") for row in reader]
        else:
            handle.seek(0)
            rows = [line.strip() for line in handle if line.strip()]
    tickers = [normalize_store_ticker(market, r) for r in rows if str(r).strip()]
    # Preserve order, drop dupes.
    return list(dict.fromkeys(t for t in tickers if t))


# ---------------------------------------------------------------------------
# Bundle seed (canonical, consumer-visible bars)
# ---------------------------------------------------------------------------


def _bar_from_mapping(payload: dict) -> Bar | None:
    try:
        return Bar(
            date=date.fromisoformat(str(payload["date"])),
            open=float(payload["open"]),
            high=float(payload["high"]),
            low=float(payload["low"]),
            close=float(payload["close"]),
            volume=float(payload.get("volume", 0) or 0),
        )
    except (KeyError, ValueError, TypeError):
        return None


def iter_bundle_snapshots(market_root: Path) -> list[Path]:
    """Return monthly snapshot.json paths in ascending month order."""
    snap_root = market_root / SNAPSHOTS_SUBDIR
    if not snap_root.exists():
        return []
    return [
        month_dir / "snapshot.json"
        for month_dir in sorted(snap_root.iterdir())
        if (month_dir / "snapshot.json").exists()
    ]


def seed_from_bundles(
    market_root: Path,
    *,
    start: date | None,
    end: date | None,
) -> dict[str, dict[date, Bar]]:
    """Build {ticker: {date: Bar}} from monthly bundles, later month wins.

    Iterating snapshots in ascending month order means a later bundle's bar for
    a given date overwrites an earlier one, so the store inherits the most
    recent adjustment basis for every overlapping date.
    """
    by_ticker: dict[str, dict[date, Bar]] = {}
    for snapshot_path in iter_bundle_snapshots(market_root):
        try:
            payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("skip unreadable bundle %s: %s", snapshot_path, exc)
            continue
        prices = payload.get("prices", {})
        for ticker, bars in prices.items():
            target = by_ticker.setdefault(ticker, {})
            for raw_bar in bars:
                bar = _bar_from_mapping(raw_bar)
                if bar is None:
                    continue
                if start is not None and bar.date < start:
                    continue
                if end is not None and bar.date > end:
                    continue
                target[bar.date] = bar
    return by_ticker


# ---------------------------------------------------------------------------
# Provider tail (fresh trading days after the latest bundle)
# ---------------------------------------------------------------------------


def _make_price_provider(market: str):
    if market == "kr":
        from eit_market_data.kr.pykrx_provider import PykrxProvider

        return PykrxProvider(official_only=True)
    from eit_market_data.yfinance_provider import YFinanceProvider

    return YFinanceProvider()


def fetch_provider_bars(
    market: str,
    tickers: Iterable[str],
    *,
    as_of: date,
    lookback_days: int,
) -> dict[str, list[Bar]]:
    """Fetch daily bars (<= as_of) for tickers via the bundle's own provider.

    Returns {} on any failure (no network / KRX auth) - the bundle seed is the
    offline fallback. Reuses the exact provider used to build the bundles, so
    appended bars share their adjustment basis.
    """
    import asyncio

    tickers = list(tickers)
    if not tickers:
        return {}
    try:
        provider = _make_price_provider(market)
    except Exception as exc:  # noqa: BLE001 - optional dependency / auth
        logger.warning("price provider unavailable for %s tail: %s", market, exc)
        return {}

    async def _gather() -> dict[str, list[Bar]]:
        sem = asyncio.Semaphore(4)

        async def _one(ticker: str) -> tuple[str, list[Bar]]:
            async with sem:
                try:
                    raw = await provider.fetch_prices(
                        ticker, as_of, lookback_days=lookback_days
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning("tail fetch failed for %s: %s", ticker, exc)
                    return ticker, []
            bars: list[Bar] = []
            for pb in raw:
                bars.append(
                    Bar(
                        date=pb.date,
                        open=float(pb.open),
                        high=float(pb.high),
                        low=float(pb.low),
                        close=float(pb.close),
                        volume=float(pb.volume),
                    )
                )
            return ticker, bars

        results = await asyncio.gather(*(_one(t) for t in tickers))
        return {ticker: bars for ticker, bars in results}

    try:
        return asyncio.run(_gather())
    except Exception as exc:  # noqa: BLE001
        logger.warning("provider tail aborted for %s: %s", market, exc)
        return {}


# ---------------------------------------------------------------------------
# CSV read / merge / write (idempotent + incremental)
# ---------------------------------------------------------------------------


def read_existing_store(path: Path) -> dict[date, Bar]:
    if not path.exists():
        return {}
    out: dict[date, Bar] = {}
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            bar = _bar_from_mapping(row)
            if bar is not None:
                out[bar.date] = bar
    return out


def merge_bars(
    existing: dict[date, Bar],
    seed: dict[date, Bar],
    tail: list[Bar],
    *,
    rebuild: bool,
) -> tuple[list[Bar], int]:
    """Merge sources into an ascending bar list.

    Precedence (rebuild): bundle seed for every covered date; provider tail only
    for dates strictly after the latest seeded date.
    Incremental (default): keep existing rows verbatim; add only dates missing
    from the file (from seed, then tail-after-latest). Returns (bars, n_added).
    """
    seed_max = max(seed) if seed else None

    if rebuild:
        merged: dict[date, Bar] = dict(seed)
        for bar in tail:
            if seed_max is None or bar.date > seed_max:
                merged.setdefault(bar.date, bar)
        return _sorted(merged), len(merged)

    merged = dict(existing)
    added = 0
    for d, bar in seed.items():
        if d not in merged:
            merged[d] = bar
            added += 1
    cutoff = seed_max
    for bar in tail:
        if cutoff is not None and bar.date <= cutoff:
            continue
        if bar.date not in merged:
            merged[bar.date] = bar
            added += 1
    return _sorted(merged), added


def _sorted(bars: dict[date, Bar]) -> list[Bar]:
    return [bars[d] for d in sorted(bars)]


def write_store(path: Path, bars: list[Bar]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=PRICE_STORE_COLUMNS)
        writer.writeheader()
        for bar in bars:
            writer.writerow(bar.as_row())


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def _last_business_day(reference: date) -> date:
    current = reference
    while current.weekday() >= 5:
        current -= timedelta(days=1)
    return current


@dataclass
class StoreResult:
    market: str
    store_dir: Path
    tickers: list[str]
    missing: list[str]
    short: list[str]
    total_rows: int
    min_date: date | None
    max_date: date | None


def build_daily_price_store(
    *,
    market: str,
    artifacts_root: Path,
    universe_csv: Path | None,
    start: date | None,
    end: date | None,
    source: str,
    rebuild: bool,
    min_rows: int,
    tail_lookback_days: int,
) -> StoreResult:
    market_root = artifacts_root / market
    store_dir = market_root / STORE_SUBDIR

    seed = (
        seed_from_bundles(market_root, start=start, end=end)
        if source in ("auto", "bundle")
        else {}
    )

    universe = load_universe(market, universe_csv)
    if universe is None:
        universe = sorted(seed.keys())
    tickers = list(universe)

    end_date = end or _last_business_day(datetime.now(timezone.utc).date())

    tail: dict[str, list[Bar]] = {}
    if source in ("auto", "provider"):
        tail = fetch_provider_bars(
            market, tickers, as_of=end_date, lookback_days=tail_lookback_days
        )

    missing: list[str] = []
    short: list[str] = []
    total_rows = 0
    overall_min: date | None = None
    overall_max: date | None = None
    manifest_tickers: dict[str, dict] = {}

    for ticker in tickers:
        ticker_seed = seed.get(ticker, {})
        ticker_tail = tail.get(ticker, [])
        if start is not None:
            ticker_tail = [b for b in ticker_tail if b.date >= start]
        if end is not None:
            ticker_tail = [b for b in ticker_tail if b.date <= end]

        path = store_dir / f"{ticker}.csv"
        existing = read_existing_store(path) if not rebuild else {}
        bars, _added = merge_bars(existing, ticker_seed, ticker_tail, rebuild=rebuild)

        if not bars:
            missing.append(ticker)
            continue
        write_store(path, bars)

        total_rows += len(bars)
        t_min, t_max = bars[0].date, bars[-1].date
        overall_min = t_min if overall_min is None else min(overall_min, t_min)
        overall_max = t_max if overall_max is None else max(overall_max, t_max)
        if len(bars) < min_rows:
            short.append(ticker)
        manifest_tickers[ticker] = {
            "rows": len(bars),
            "min_date": t_min.isoformat(),
            "max_date": t_max.isoformat(),
        }

    manifest = {
        "market": market,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "columns": PRICE_STORE_COLUMNS,
        "ticker_count": len(manifest_tickers),
        "total_rows": total_rows,
        "min_date": overall_min.isoformat() if overall_min else None,
        "max_date": overall_max.isoformat() if overall_max else None,
        "missing_tickers": missing,
        "short_tickers": short,
        "tickers": manifest_tickers,
    }
    store_dir.mkdir(parents=True, exist_ok=True)
    (store_dir / MANIFEST_FILENAME).write_text(
        json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8"
    )

    return StoreResult(
        market=market,
        store_dir=store_dir,
        tickers=tickers,
        missing=missing,
        short=short,
        total_rows=total_rows,
        min_date=overall_min,
        max_date=overall_max,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build the per-ticker daily OHLCV price store for a market."
    )
    parser.add_argument("--market", required=True, choices=["kr", "us"])
    parser.add_argument(
        "--universe",
        default=None,
        help="Universe CSV. Default: every ticker present in the monthly bundles.",
    )
    parser.add_argument("--start", default=None, help="Earliest date (YYYY-MM-DD).")
    parser.add_argument(
        "--end",
        default=None,
        help="Latest date (YYYY-MM-DD). Default: last business day.",
    )
    parser.add_argument(
        "--artifacts-root",
        default=str(ARTIFACTS_ROOT),
        help="Bundle/store root (contains <market>/snapshots and <market>/daily_prices).",
    )
    parser.add_argument(
        "--source",
        default="auto",
        choices=["auto", "bundle", "provider"],
        help="auto: bundle seed + provider tail; bundle: offline; provider: live only.",
    )
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Recompute every row instead of incrementally appending new days.",
    )
    parser.add_argument(
        "--min-rows",
        type=int,
        default=20,
        help="Report tickers with fewer than this many rows as short history.",
    )
    parser.add_argument(
        "--tail-lookback-days",
        type=int,
        default=400,
        help="Trading-day lookback requested from the live provider for the tail.",
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    result = build_daily_price_store(
        market=args.market,
        artifacts_root=Path(args.artifacts_root),
        universe_csv=Path(args.universe) if args.universe else None,
        start=date.fromisoformat(args.start) if args.start else None,
        end=date.fromisoformat(args.end) if args.end else None,
        source=args.source,
        rebuild=args.rebuild,
        min_rows=args.min_rows,
        tail_lookback_days=args.tail_lookback_days,
    )

    print(
        f"[DONE] market={result.market} store={result.store_dir} "
        f"tickers={len(result.tickers)} written={len(result.tickers) - len(result.missing)} "
        f"missing={len(result.missing)} short={len(result.short)} "
        f"rows={result.total_rows} range={result.min_date}..{result.max_date}"
    )
    if result.missing:
        print(f"[WARN] missing history: {', '.join(result.missing[:20])}")


if __name__ == "__main__":
    main()
