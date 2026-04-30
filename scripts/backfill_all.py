# ruff: noqa: E402
"""Full historical market data backfill.

Phases:
  1. KR Raw pykrx  — 전종목 OHLCV, cap, fundamental, investor, shorting, foreign, index, etf, meta
  2. KR DART 재무   — 전종목 분기 재무제표 (opendartreader, 5s delay)
  3. KR Snapshots   — 월별 MonthlySnapshot JSON (ci_safe profile)
  4. US Snapshots   — S&P 500 월별 MonthlySnapshot JSON

Usage:
    python scripts/backfill_all.py --start 2022-01 --end 2026-03
    python scripts/backfill_all.py --start 2022-01 --end 2026-03 --phase 2
    python scripts/backfill_all.py --start 2022-01 --end 2026-03 --phase 3 4
    python scripts/backfill_all.py --refresh-sp500  # update universes/sp500.csv from Wikipedia
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import re
import sys
import time
from collections import Counter, defaultdict
from contextlib import redirect_stderr, redirect_stdout
from datetime import date, timedelta
from io import StringIO
from pathlib import Path
from typing import Any

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from dotenv import load_dotenv

load_dotenv(PROJECT_ROOT / ".env")

logger = logging.getLogger("backfill")

_INVALID_SYMBOL_PATTERN = re.compile(r'"?(?P<ticker>[0-9A-Z]+)"?\s+invalid symbol or has no data', re.IGNORECASE)


def _safe_log_message(msg: object, args: object) -> str:
    if not args:
        return str(msg)
    try:
        return str(msg) % args
    except Exception:
        return f"{msg} {args}"


def _is_known_pykrx_malformed_record(record: logging.LogRecord) -> bool:
    return (
        record.name == "root"
        and isinstance(record.msg, tuple)
        and isinstance(record.args, tuple)
        and len(record.args) == 1
        and isinstance(record.args[0], dict)
    )


def _normalize_issue_reason(message: str) -> str:
    lowered = message.lower()
    if "invalid symbol" in lowered or "has no data" in lowered:
        return "invalid symbol or no data"
    if "status=400 logout" in lowered:
        return "status=400 LOGOUT"
    if ":" in message:
        return message.split(":", 1)[1].strip()
    return message.strip()


class _Phase1Summary:
    def __init__(self) -> None:
        self.ohlcv_failures: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
        self.pykrx_failures: Counter[tuple[str, str]] = Counter()
        self.pykrx_examples: dict[tuple[str, str], str] = {}

    def record_ohlcv_failure(self, market: str, ticker: str, issue: str) -> None:
        norm_ticker = str(ticker).strip().strip('"') or "unknown"
        normalized_issue = _normalize_issue_reason(issue)
        self.ohlcv_failures[market][normalized_issue].add(norm_ticker)

    def record_ohlcv_message(self, market: str, message: str) -> None:
        text = message.strip()
        if not text:
            return
        match = re.match(r"Ticker\s+(?P<ticker>\S+)\s+fetch failed in (?P<source>\w+):\s*(?P<reason>.+)", text)
        if match:
            ticker = match.group("ticker")
            source = match.group("source")
            reason = _normalize_issue_reason(match.group("reason"))
            self.record_ohlcv_failure(market, ticker, f"{source}: {reason}")
            return
        match = _INVALID_SYMBOL_PATTERN.search(text)
        if match:
            self.record_ohlcv_failure(market, match.group("ticker"), "fdr: invalid symbol or no data")
            return

    def record_pykrx_failure(self, fn_name: str, exc: Exception, context: str | None = None) -> None:
        reason = _normalize_issue_reason(str(exc))
        key = (fn_name, reason)
        self.pykrx_failures[key] += 1
        if context and key not in self.pykrx_examples:
            self.pykrx_examples[key] = context

    def emit_ohlcv_summary(self, market: str) -> None:
        issue_map = self.ohlcv_failures.get(market)
        if not issue_map:
            return
        parts: list[str] = []
        for issue, tickers in sorted(issue_map.items()):
            if not tickers:
                continue
            example = sorted(tickers)[0]
            parts.append(f"{issue}={len(tickers)} tickers (e.g. {example})")
        if parts:
            tqdm.write(f"[Phase 1] OHLCV {market} failures: " + "; ".join(parts))
        self.ohlcv_failures.pop(market, None)

    def emit_pykrx_summary(self) -> None:
        if not self.pykrx_failures:
            return
        tqdm.write("[Phase 1] KRX call failures:")
        for (fn_name, reason), count in sorted(self.pykrx_failures.items()):
            example = self.pykrx_examples.get((fn_name, reason))
            line = f"  - {fn_name}: {reason} x{count}"
            if example:
                line += f" ({example})"
            tqdm.write(line)
        self.pykrx_failures.clear()
        self.pykrx_examples.clear()


class _Phase1MarketCollector:
    def __init__(self, market: str, summary: _Phase1Summary) -> None:
        self.market = market
        self.summary = summary

    def warning(self, msg: object, *args: object, **kwargs: object) -> None:
        _ = kwargs
        self.summary.record_ohlcv_message(self.market, _safe_log_message(msg, args))


def _capture_output_lines(fn: Any, *args: Any, **kwargs: Any) -> tuple[Any, list[str]]:
    stdout_buffer = StringIO()
    stderr_buffer = StringIO()
    with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
        result = fn(*args, **kwargs)
    combined = stdout_buffer.getvalue().splitlines() + stderr_buffer.getvalue().splitlines()
    return result, [line.strip() for line in combined if line.strip()]


import contextlib
import os


@contextlib.contextmanager
def _suppress_stdout():
    """Redirect stdout to /dev/null to silence FDR's ``print()`` spam."""
    devnull = open(os.devnull, "w")  # noqa: SIM115
    try:
        with redirect_stdout(devnull):
            yield
    finally:
        devnull.close()


class _DropMalformedFilter(logging.Filter):
    """Silently drop log records whose args cause a formatting TypeError.

    pykrx's internal util.py wrapper calls ``logging.info(args, kwargs)``
    directly on the root logger with a tuple as the message and a dict as
    the only positional arg — this makes Python's %-formatter raise
    ``TypeError: not all arguments converted``, which in turn causes tqdm's
    logging redirect handler to print a full traceback on every pykrx call.
    """

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003
        try:
            record.getMessage()
            return True
        except TypeError:
            if _is_known_pykrx_malformed_record(record):
                return False
            return False


logging.getLogger().addFilter(_DropMalformedFilter())

MARKETS = ["KOSPI", "KOSDAQ"]
PYKRX_DELAY = 0.3  # seconds between pykrx calls
DART_DELAY_DEFAULT = 5.0  # seconds between DART requests
DART_QUARTERS_DEFAULT = 32  # enough depth for 2022+ point-in-time replay
KR_MIN_FIELD_COVERAGE_RATIO = 0.50

KR_UNIVERSE_CSV = PROJECT_ROOT / "universes" / "kr_universe.csv"
SP500_CSV = PROJECT_ROOT / "universes" / "sp500.csv"

BACKFILL_ROOT = PROJECT_ROOT / "data" / "backfill"
ARTIFACTS_ROOT = PROJECT_ROOT / "artifacts"


# ---------------------------------------------------------------------------
# BackfillDartProvider — reads Phase 2 JSON instead of calling DART API
# ---------------------------------------------------------------------------


class BackfillDartProvider:
    """DART provider backed by Phase 2 backfill JSON files.

    Reads ``data/backfill/dart/{ticker}.json`` saved by ``phase2_kr_dart()``
    and returns ``FundamentalData`` / ``FilingData`` with point-in-time
    filtering (quarters where ``report_date > as_of`` are excluded).
    """

    def __init__(self, backfill_dir: Path) -> None:
        self._dir = backfill_dir
        self._cache: dict[str, dict] = {}

    def _load(self, ticker: str) -> dict | None:
        norm = ticker.zfill(6)
        if norm in self._cache:
            return self._cache[norm]
        path = self._dir / f"{norm}.json"
        if not path.exists():
            return None
        data = json.loads(path.read_text(encoding="utf-8"))
        self._cache[norm] = data
        return data

    async def fetch_fundamentals(
        self,
        ticker: str,
        as_of: date,
        n_quarters: int = 8,
    ) -> "FundamentalData":
        from eit_market_data.schemas.snapshot import FundamentalData, QuarterlyFinancials

        data = self._load(ticker)
        if data is None or "fundamentals" not in data:
            return FundamentalData(ticker=ticker.zfill(6))

        raw = data["fundamentals"]
        quarters: list[QuarterlyFinancials] = []
        for q in raw.get("quarters", []):
            qf = QuarterlyFinancials.model_validate(q)
            if qf.report_date <= as_of:
                quarters.append(qf)
        quarters.sort(key=lambda q: q.report_date, reverse=True)
        return FundamentalData(
            ticker=ticker.zfill(6),
            quarters=quarters[:n_quarters],
        )

    async def fetch_filing(self, ticker: str, as_of: date) -> "FilingData":
        from eit_market_data.schemas.snapshot import FilingData

        data = self._load(ticker)
        if data is None or "filing" not in data:
            return FilingData(ticker=ticker.zfill(6))

        filing = FilingData.model_validate(data["filing"])
        # Exclude filing if its date is after as_of (point-in-time)
        if filing.filing_date is not None and filing.filing_date > as_of:
            return FilingData(ticker=ticker.zfill(6))
        return filing


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------


def _yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def _month_range(start: str, end: str) -> list[str]:
    """Generate YYYY-MM strings from *start* to *end* inclusive."""
    months: list[str] = []
    y, m = int(start[:4]), int(start[5:7])
    ey, em = int(end[:4]), int(end[5:7])
    while (y, m) <= (ey, em):
        months.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1
    return months


def _last_business_day(year: int, month: int) -> date:
    if month == 12:
        last = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        last = date(year, month + 1, 1) - timedelta(days=1)
    while last.weekday() >= 5:
        last -= timedelta(days=1)
    return last


def _today_capped(d: date) -> date:
    """Return *d* capped to today (prevent future dates)."""
    today = date.today()
    return min(d, today)


def _save_parquet(df: Any, path: Path) -> None:
    from datetime import datetime, timezone

    if df is None or (hasattr(df, "empty") and df.empty):
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    df["collected_at"] = datetime.now(timezone.utc).isoformat()
    df.to_parquet(path, index=True)
    logger.debug("saved %s (%d rows)", path, len(df))


def _save_csv_df(df: Any, path: Path) -> None:
    from datetime import datetime, timezone

    if df is None or (hasattr(df, "empty") and df.empty):
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    df["collected_at"] = datetime.now(timezone.utc).isoformat()
    df.to_csv(path, index=True, encoding="utf-8-sig")
    logger.debug("saved %s (%d rows)", path, len(df))


def _pykrx_call(
    fn: Any,
    *args: Any,
    on_error: Any | None = None,
    error_context: str | None = None,
    **kwargs: Any,
) -> Any:
    """Call pykrx function with delay and error handling."""
    try:
        result = fn(*args, **kwargs)
        time.sleep(PYKRX_DELAY)
        return result
    except Exception as e:
        if on_error is None:
            logger.warning("pykrx call failed %s: %s", fn.__name__, e)
        else:
            on_error(fn.__name__, e, error_context)
        time.sleep(PYKRX_DELAY)
        return None


def _load_csv_tickers(path: Path) -> list[str]:
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [row["ticker"].strip() for row in reader if row.get("ticker")]


def _fdr_all_kr_tickers() -> list[str]:
    """Get full KR market ticker list via FDR StockListing (no KRX auth needed)."""
    import FinanceDataReader as fdr

    tickers: list[str] = []
    for market in ("KOSPI-DESC", "KOSDAQ-DESC"):
        try:
            df = fdr.StockListing(market)
            if df is not None and not df.empty and "Code" in df.columns:
                codes = df["Code"].dropna().astype(str).str.zfill(6).tolist()
                tickers.extend(codes)
                logger.info("FDR %s: %d tickers", market, len(codes))
        except Exception as exc:
            logger.warning("FDR StockListing %s failed: %s", market, exc)
    return sorted(set(tickers))


def _value(obj: Any, key: str, default: Any = None) -> Any:
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _snapshot_field_coverage(snapshot: Any) -> dict[str, Any]:
    universe = _value(snapshot, "universe", []) or []
    prices = _value(snapshot, "prices", {}) or {}
    fundamentals = list((_value(snapshot, "fundamentals", {}) or {}).values())
    filings = list((_value(snapshot, "filings", {}) or {}).values())
    sector_map = _value(snapshot, "sector_map", {}) or {}
    benchmark_prices = _value(snapshot, "benchmark_prices", []) or []

    def has_filing_text(item: Any) -> bool:
        return bool(
            _value(item, "business_overview")
            or _value(item, "risks")
            or _value(item, "mda")
            or _value(item, "governance")
        )

    def has_issued_shares(item: Any) -> bool:
        quarters = _value(item, "quarters", []) or []
        return any(_value(quarter, "issued_shares") is not None for quarter in quarters)

    return {
        "universe_size": len(universe),
        "prices": sum(1 for bars in prices.values() if bars),
        "fundamentals": sum(1 for item in fundamentals if _value(item, "quarters", [])),
        "filings": sum(1 for item in filings if has_filing_text(item)),
        "last_close_price": sum(
            1 for item in fundamentals if _value(item, "last_close_price") is not None
        ),
        "market_cap": sum(1 for item in fundamentals if _value(item, "market_cap") is not None),
        "issued_shares": sum(1 for item in fundamentals if has_issued_shares(item)),
        "sector_map": len(sector_map),
        "benchmark_bars": len(benchmark_prices),
    }


def _read_existing_field_coverage(snapshot_dir: Path) -> dict[str, Any] | None:
    manifest_path = snapshot_dir / "manifest.json"
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return None
        coverage = manifest.get("field_coverage")
        if isinstance(coverage, dict):
            return coverage

    snapshot_path = snapshot_dir / "snapshot.json"
    if not snapshot_path.exists():
        return None
    try:
        snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    return _snapshot_field_coverage(snapshot)


def _snapshot_dir_complete(snapshot_dir: Path, market: str) -> bool:
    if not (snapshot_dir / "snapshot.json").exists():
        return False
    if not (snapshot_dir / "snapshot.json.gz").exists():
        return False
    if not (snapshot_dir / "metadata.json").exists():
        return False
    if not (snapshot_dir / "summary.json").exists():
        return False
    if not (snapshot_dir / "manifest.json").exists():
        return False

    try:
        summary = json.loads((snapshot_dir / "summary.json").read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return False
    if summary.get("status") != "ok":
        return False

    coverage = _read_existing_field_coverage(snapshot_dir)
    if not coverage:
        return False
    universe_size = int(coverage.get("universe_size", 0) or 0)
    if universe_size <= 0:
        return False
    min_kr_fields = int(universe_size * KR_MIN_FIELD_COVERAGE_RATIO)

    if int(coverage.get("prices", 0) or 0) <= 0:
        return False
    if market == "kr" and int(coverage.get("fundamentals", 0) or 0) < min_kr_fields:
        return False
    if market == "kr" and int(coverage.get("market_cap", 0) or 0) < min_kr_fields:
        return False
    if market == "kr" and int(coverage.get("issued_shares", 0) or 0) < min_kr_fields:
        return False
    if market != "kr" and int(coverage.get("fundamentals", 0) or 0) <= 0:
        return False
    return True


def _manifest_warnings(market: str, coverage: dict[str, Any]) -> list[str]:
    warnings: list[str] = []
    universe_size = int(coverage.get("universe_size", 0) or 0)
    min_kr_fields = int(universe_size * KR_MIN_FIELD_COVERAGE_RATIO)
    if market == "kr" and int(coverage.get("fundamentals", 0) or 0) < min_kr_fields:
        warnings.append("fundamental coverage is below KR minimum")
    if market == "kr" and int(coverage.get("market_cap", 0) or 0) < min_kr_fields:
        warnings.append("market_cap coverage is below KR minimum")
    if market == "kr" and int(coverage.get("issued_shares", 0) or 0) < min_kr_fields:
        warnings.append("issued_shares coverage is below KR minimum")
    if int(coverage.get("benchmark_bars", 0) or 0) == 0:
        warnings.append("benchmark coverage is zero")
    return warnings


def _write_snapshot_manifest(
    snapshot_dir: Path,
    *,
    market: str,
    month: str,
    snapshot: Any,
    source_profile: str,
    field_coverage: dict[str, Any],
) -> None:
    manifest = {
        "bundle_version": 1,
        "market": market,
        "month": month,
        "decision_date": _value(snapshot, "decision_date").isoformat(),
        "execution_date": _value(snapshot, "execution_date").isoformat(),
        "source_profile": source_profile,
        "field_coverage": field_coverage,
        "warnings": _manifest_warnings(market, field_coverage),
        "files": {
            "snapshot": "snapshot.json",
            "snapshot_gzip": "snapshot.json.gz",
            "metadata": "metadata.json",
            "manifest": "manifest.json",
            "summary": "summary.json",
        },
    }
    (snapshot_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _raise_for_incomplete_kr_snapshot(field_coverage: dict[str, Any]) -> None:
    universe_size = int(field_coverage.get("universe_size", 0) or 0)
    minimum = int(universe_size * KR_MIN_FIELD_COVERAGE_RATIO)
    for key in ("fundamentals", "market_cap", "issued_shares"):
        value = int(field_coverage.get(key, 0) or 0)
        if value < minimum:
            raise RuntimeError(
                f"KR snapshot {key} coverage below minimum: {value}/{universe_size} "
                f"(min {minimum})"
            )


# ---------------------------------------------------------------------------
# S&P 500 universe management
# ---------------------------------------------------------------------------

_FALLBACK_SP500 = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "BRK-B", "LLY", "AVGO",
    "JPM", "TSLA", "UNH", "V", "XOM", "MA", "COST", "PG", "JNJ", "HD",
    "ABBV", "WMT", "NFLX", "MRK", "BAC", "CRM", "ORCL", "CVX", "AMD",
    "KO", "PEP", "LIN", "TMO", "ADBE", "ACN", "MCD", "CSCO", "ABT",
    "WFC", "GE", "DHR", "INTC", "IBM", "DIS", "PM", "INTU", "CAT",
    "NOW", "TXN", "QCOM", "VZ", "CMCSA", "AXP", "GS", "ISRG", "AMGN",
    "PFE", "MS", "BLK", "RTX", "SPGI", "NEE", "BKNG", "HON", "T",
    "LOW", "UNP", "BA", "SBUX", "TJX", "AMAT", "ELV", "DE", "PLD",
    "SYK", "BMY", "MDT", "LMT", "GILD", "CB", "SCHW", "ADI", "VRTX",
    "MMC", "GD", "CI", "UPS", "BSX", "ADP", "LRCX", "PGR", "ETN",
    "SO", "ZTS", "CME", "REGN", "SLB", "TGT", "BDX", "DUK", "KLAC",
    "PANW", "COP", "NOC", "SNPS", "CDNS", "ITW", "USB", "FDX", "PH",
    "ICE", "CMI", "EMR", "WM", "MAR", "MPC", "PSX", "CVS", "CL",
    "SRE", "AEP", "ORLY", "NKE", "MCK", "HCA", "PNC", "ECL", "APD",
    "TFC", "GIS", "HUM", "TMUS", "FTNT", "CRWD", "D", "AIG", "AFL",
    "MET", "PRU", "TRV", "ALL", "CINF", "AON", "AJG", "ROP", "ROST",
    "OXY", "VLO", "EOG", "FANG", "DVN", "HAL", "BKR", "PSA", "AMT",
    "CCI", "EQIX", "SPG", "O", "WELL", "DLR", "EXR", "VICI", "NEM",
    "FCX", "DOW", "DD", "NUE", "VMC", "MLM", "SHW", "IFF", "APH",
    "MSCI", "MCHP", "ON", "NXPI", "KEYS", "TER", "ZBRA", "SWKS",
]


def _ensure_sp500_csv(path: Path) -> Path:
    """Ensure sp500.csv exists. Download from Wikipedia if missing."""
    if path.exists():
        n = sum(1 for _ in open(path)) - 1  # noqa: SIM115
        if n >= 400:
            logger.info("S&P 500 universe: %d tickers from %s", n, path)
            return path
        logger.warning("sp500.csv has only %d tickers — consider --refresh-sp500", n)
        return path
    logger.info("sp500.csv not found — downloading from Wikipedia")
    _download_sp500(path)
    return path


def _download_sp500(path: Path) -> None:
    """Download S&P 500 list from Wikipedia and save as CSV."""
    import urllib.request

    import pandas as pd

    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    logger.info("Downloading S&P 500 list from %s", url)
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as resp:
        html = resp.read().decode("utf-8")
    from io import StringIO

    tables = pd.read_html(StringIO(html))
    df = tables[0][["Symbol", "GICS Sector", "Security"]].copy()
    df.columns = ["ticker", "sector", "name"]
    df["ticker"] = df["ticker"].str.strip().str.replace(".", "-", regex=False)
    df = df.sort_values("ticker").reset_index(drop=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8")
    logger.info("Saved %d S&P 500 tickers to %s", len(df), path)


def _create_fallback_sp500(path: Path) -> None:
    """Write hardcoded fallback tickers when Wikipedia download fails."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        f.write("ticker,sector,name\n")
        for t in _FALLBACK_SP500:
            f.write(f"{t},,\n")
    logger.info("Created fallback sp500.csv with %d tickers", len(_FALLBACK_SP500))


# ---------------------------------------------------------------------------
# Phase 1: KR Raw pykrx (전종목)
# ---------------------------------------------------------------------------


def phase1_kr_pykrx(start_month: str, end_month: str, out: Path) -> None:
    """Fetch all pykrx raw data with resume/skip support."""
    try:
        from pykrx import stock
    except ImportError:
        logger.error("pykrx not installed. Run: pip install -e '.[kr]'")
        return

    from eit_market_data.kr.krx_auth import (
        KrxAuthRequired,
        install_pykrx_krx_session_hooks,
    )
    from eit_market_data.kr.market_helpers import (
        INDEX_CODE_NAMES,
        fetch_index_ohlcv_frame,
        fetch_market_ticker_list,
        fetch_stock_ohlcv_frame,
    )

    install_pykrx_krx_session_hooks()
    # KRX auth optional — ticker list and OHLCV fall back to FDR if KRX cookies are unavailable
    _krx_available = False
    try:
        from eit_market_data.kr.krx_auth import ensure_krx_authenticated_session
        ensure_krx_authenticated_session(interactive=False)
        logger.info("[Phase 1] KRX authenticated — pykrx official data available")
        _krx_available = True
    except KrxAuthRequired:
        logger.warning("[Phase 1] KRX auth unavailable — using FDR for OHLCV; monthly KRX data (cap/fundamental/shorting/foreign) will be skipped")

    months = _month_range(start_month, end_month)
    start_date = date(int(start_month[:4]), int(start_month[5:7]), 1)
    end_lbd = _today_capped(
        _last_business_day(int(end_month[:4]), int(end_month[5:7]))
    )

    out.mkdir(parents=True, exist_ok=True)
    logger.info("[Phase 1] KR pykrx raw data: %s to %s → %s", start_month, end_month, out)
    start_str = _yyyymmdd(start_date)
    end_str = _yyyymmdd(end_lbd)
    phase1_summary = _Phase1Summary()

    # --- 1a. OHLCV per ticker (full range, skip existing) ---
    # Use market_helpers.fetch_market_ticker_list (FDR first, pykrx fallback)
    # Use market_helpers.fetch_stock_ohlcv_frame (FDR) for per-ticker OHLCV
    logger.info("[Phase 1] === OHLCV (full range) ===")

    import FinanceDataReader as fdr

    for market in MARKETS:
        collector = _Phase1MarketCollector(market, phase1_summary)
        try:
            df_list = fdr.StockListing(f"{market}-DESC")
            tickers = (
                df_list["Code"].dropna().astype(str).str.zfill(6).tolist()
                if df_list is not None and not df_list.empty
                else []
            )
        except Exception as exc:
            logger.warning("[Phase 1] FDR listing %s failed: %s", market, exc)
            tickers = []

        todo = [t for t in tickers if not (out / f"market/ohlcv/{market}/{t}.parquet").exists()]
        skip_n = len(tickers) - len(todo)
        tqdm.write(f"[Phase 1] OHLCV {market}: {len(tickers)} tickers ({skip_n} already done, {len(todo)} remaining)")
        with tqdm(todo, desc=f"  OHLCV {market}", unit="ticker", ncols=100, dynamic_ncols=True) as pbar:
            for ticker in pbar:
                pbar.set_postfix_str(ticker, refresh=False)
                (df, _source), noisy_lines = _capture_output_lines(
                    fetch_stock_ohlcv_frame,
                    ticker,
                    start_date,
                    end_lbd,
                    logger_=collector,
                )
                for line in noisy_lines:
                    phase1_summary.record_ohlcv_message(market, line)
                time.sleep(PYKRX_DELAY)
                _save_parquet(df, out / f"market/ohlcv/{market}/{ticker}.parquet")
        phase1_summary.emit_ohlcv_summary(market)

    # --- 1b. Monthly snapshot data (requires KRX auth) ---
    if not _krx_available:
        tqdm.write("\n[Phase 1] Skipping monthly KRX data — KRX login required (cap/fundamental/shorting/foreign)")
    else:
        tqdm.write("\n[Phase 1] === Monthly data (cap, fundamental, investor, shorting, foreign) ===")
        with tqdm(months, desc="  Monthly data", unit="month", ncols=100, dynamic_ncols=True) as pbar:
            for month_str in pbar:
                y, m = int(month_str[:4]), int(month_str[5:7])
                lbd = _today_capped(_last_business_day(y, m))
                lbd_str = _yyyymmdd(lbd)
                month_start = date(y, m, 1)
                month_start_str = _yyyymmdd(month_start)
                pbar.set_postfix_str(month_str, refresh=False)

                for market in MARKETS:
                    # Cap
                    cap_path = out / f"market/cap/{market}_{lbd_str}.parquet"
                    if not cap_path.exists():
                        df = _pykrx_call(
                            stock.get_market_cap,
                            lbd_str,
                            market=market,
                            on_error=phase1_summary.record_pykrx_failure,
                            error_context=f"{month_str} {market}",
                        )
                        _save_parquet(df, cap_path)

                    # Fundamental (PER/PBR/EPS/DIV)
                    fund_path = out / f"market/fundamental/{market}_{lbd_str}.parquet"
                    if not fund_path.exists():
                        df = _pykrx_call(
                            stock.get_market_fundamental,
                            lbd_str,
                            market=market,
                            on_error=phase1_summary.record_pykrx_failure,
                            error_context=f"{month_str} {market}",
                        )
                        _save_parquet(df, fund_path)

                    # Investor trading
                    inv_val_path = out / f"market/investor/{market}_value_by_date_{lbd_str}.parquet"
                    if not inv_val_path.exists():
                        df = _pykrx_call(
                            stock.get_market_trading_value_by_date,
                            month_start_str, lbd_str, market,
                            on_error=phase1_summary.record_pykrx_failure,
                            error_context=f"{month_str} {market}",
                        )
                        _save_parquet(df, inv_val_path)

                    inv_vol_path = out / f"market/investor/{market}_volume_by_date_{lbd_str}.parquet"
                    if not inv_vol_path.exists():
                        df = _pykrx_call(
                            stock.get_market_trading_volume_by_date,
                            month_start_str, lbd_str, market,
                            on_error=phase1_summary.record_pykrx_failure,
                            error_context=f"{month_str} {market}",
                        )
                        _save_parquet(df, inv_vol_path)

                    # Shorting
                    short_status_path = out / f"market/shorting/{market}_status_{lbd_str}.parquet"
                    if not short_status_path.exists():
                        df = _pykrx_call(
                            stock.get_shorting_status_by_date,
                            month_start_str, lbd_str, market,
                            on_error=phase1_summary.record_pykrx_failure,
                            error_context=f"{month_str} {market}",
                        )
                        _save_parquet(df, short_status_path)

                    short_bal_path = out / f"market/shorting/{market}_balance_{lbd_str}.parquet"
                    if not short_bal_path.exists():
                        df = _pykrx_call(
                            stock.get_shorting_balance_by_date,
                            month_start_str, lbd_str, market,
                            on_error=phase1_summary.record_pykrx_failure,
                            error_context=f"{month_str} {market}",
                        )
                        _save_parquet(df, short_bal_path)

                    short_top_vol_path = out / f"market/shorting/{market}_top50_volume_{lbd_str}.parquet"
                    if not short_top_vol_path.exists():
                        df = _pykrx_call(
                            stock.get_shorting_volume_top50,
                            lbd_str,
                            market=market,
                            on_error=phase1_summary.record_pykrx_failure,
                            error_context=f"{month_str} {market}",
                        )
                        _save_parquet(df, short_top_vol_path)

                    short_top_bal_path = out / f"market/shorting/{market}_top50_balance_{lbd_str}.parquet"
                    if not short_top_bal_path.exists():
                        df = _pykrx_call(
                            stock.get_shorting_balance_top50,
                            lbd_str,
                            market=market,
                            on_error=phase1_summary.record_pykrx_failure,
                            error_context=f"{month_str} {market}",
                        )
                        _save_parquet(df, short_top_bal_path)

                    # Foreign exhaustion
                    foreign_path = out / f"market/foreign/{market}_{lbd_str}.parquet"
                    if not foreign_path.exists():
                        df = _pykrx_call(
                            stock.get_exhaustion_rates_of_foreign_investment,
                            lbd_str, market=market,
                            on_error=phase1_summary.record_pykrx_failure,
                            error_context=f"{month_str} {market}",
                        )
                        _save_parquet(df, foreign_path)
    # --- 1c. Index data (full range) ---
    logger.info("[Phase 1] === Index data ===")
    for code, name in INDEX_CODE_NAMES.items():
        idx_path = out / f"index/ohlcv/{name}.parquet"
        if not idx_path.exists():
            df_ohlcv, source = fetch_index_ohlcv_frame(
                code, start_date, end_lbd, logger_=logger, official_only=True,
            )
            if source:
                logger.debug("index OHLCV [%s] source=%s", name, source)
            _save_parquet(df_ohlcv, idx_path)

        fund_path = out / f"index/fundamental/{name}.parquet"
        if not fund_path.exists():
            df = _pykrx_call(
                stock.get_index_fundamental,
                start_str,
                end_str,
                code,
                on_error=phase1_summary.record_pykrx_failure,
                error_context=f"index {name}",
            )
            _save_parquet(df, fund_path)

    # --- 1d. ETF data (full range, skip existing) ---
    tqdm.write("\n[Phase 1] === ETF data ===")
    etf_tickers = _pykrx_call(
        stock.get_etf_ticker_list,
        end_str,
        on_error=phase1_summary.record_pykrx_failure,
        error_context="etf ticker list",
    ) or []
    etf_todo = [t for t in etf_tickers if not (out / f"etf/ohlcv/{t}.parquet").exists()]
    tqdm.write(f"[Phase 1] ETF: {len(etf_tickers)} tickers ({len(etf_tickers)-len(etf_todo)} done, {len(etf_todo)} remaining)")
    with tqdm(etf_todo, desc="  ETF OHLCV", unit="ticker", ncols=100, dynamic_ncols=True) as pbar:
        for ticker in pbar:
            pbar.set_postfix_str(ticker, refresh=False)
            df = _pykrx_call(
                stock.get_etf_ohlcv_by_ticker,
                start_str,
                end_str,
                ticker,
                on_error=phase1_summary.record_pykrx_failure,
                error_context=f"etf {ticker}",
            )
            _save_parquet(df, out / f"etf/ohlcv/{ticker}.parquet")

    # --- 1e. Ticker meta (latest date) ---
    logger.info("[Phase 1] === Ticker meta ===")
    import pandas as pd

    meta_path = out / f"meta/tickers_{end_str}.csv"
    if not meta_path.exists():
        rows = []
        for market in MARKETS:
            tickers = _pykrx_call(
                stock.get_market_ticker_list,
                end_str,
                market=market,
                on_error=phase1_summary.record_pykrx_failure,
                error_context=f"meta {market}",
            ) or []
            for ticker in tickers:
                name = _pykrx_call(
                    stock.get_market_ticker_name,
                    ticker,
                    on_error=phase1_summary.record_pykrx_failure,
                    error_context=f"meta ticker {ticker}",
                ) or ""
                rows.append({"ticker": ticker, "name": name, "market": market})
        df = pd.DataFrame(rows)
        _save_csv_df(df, meta_path)

    phase1_summary.emit_pykrx_summary()

    logger.info("[Phase 1] Done.")


# ---------------------------------------------------------------------------
# Phase 2: KR DART 재무 (전종목)
# ---------------------------------------------------------------------------


def _load_dart_progress(progress_path: Path) -> dict[str, Any]:
    if progress_path.exists():
        return json.loads(progress_path.read_text(encoding="utf-8"))
    return {"completed": [], "failed": [], "last_ticker": None}


def _save_dart_progress(progress_path: Path, progress: dict[str, Any]) -> None:
    progress_path.parent.mkdir(parents=True, exist_ok=True)
    progress_path.write_text(
        json.dumps(progress, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def phase2_kr_dart(
    end_month: str,
    out: Path,
    dart_delay: float = DART_DELAY_DEFAULT,
    dart_quarters: int = DART_QUARTERS_DEFAULT,
) -> None:
    """Fetch DART financials for all KR tickers with resume support."""
    try:
        from pykrx import stock
    except ImportError:
        raise RuntimeError("pykrx not installed (needed for ticker list)")

    from eit_market_data.kr.dart_provider import DartProvider

    try:
        dart = DartProvider()
    except (ImportError, ValueError) as exc:
        raise RuntimeError(f"Cannot initialize DartProvider: {exc}") from exc

    # Get all tickers via FDR (no KRX auth needed)
    ey, em = int(end_month[:4]), int(end_month[5:7])
    end_lbd = _today_capped(_last_business_day(ey, em))

    all_tickers = _fdr_all_kr_tickers()
    all_tickers = sorted(set(all_tickers))
    if not all_tickers:
        raise RuntimeError("No KR tickers available for DART backfill")
    logger.info(
        "[Phase 2] DART financials: %d tickers, delay=%.1fs, quarters=%d",
        len(all_tickers),
        dart_delay,
        dart_quarters,
    )

    # Load progress
    out.mkdir(parents=True, exist_ok=True)
    progress_path = out / "_progress.json"
    progress = _load_dart_progress(progress_path)
    if int(progress.get("n_quarters", 0) or 0) != dart_quarters:
        logger.info(
            "[Phase 2] DART quarter depth changed (%s -> %d); refreshing cached JSON files",
            progress.get("n_quarters"),
            dart_quarters,
        )
        progress = {"completed": [], "failed": [], "last_ticker": None}
    progress["n_quarters"] = dart_quarters
    completed_set = set(progress["completed"])

    total = len(all_tickers)
    done_count = len(completed_set)
    todo_tickers = [t for t in all_tickers if t not in completed_set]
    tqdm.write(
        f"[Phase 2] DART: {total} tickers, {done_count} already done, "
        f"{len(todo_tickers)} remaining (delay={dart_delay}s, quarters={dart_quarters})"
    )

    with tqdm(
        todo_tickers,
        desc="  DART",
        unit="ticker",
        ncols=100,
        dynamic_ncols=True,
        initial=done_count,
        total=total,
    ) as pbar:
        for ticker in pbar:
            pbar.set_postfix_str(ticker, refresh=False)
            ticker_path = out / f"{ticker}.json"
            try:
                fund_data = asyncio.run(
                    dart.fetch_fundamentals(ticker, end_lbd, n_quarters=dart_quarters)
                )
                time.sleep(dart_delay)

                filing_data = asyncio.run(dart.fetch_filing(ticker, end_lbd))
                time.sleep(dart_delay)

                result = {
                    "ticker": ticker,
                    "as_of": end_lbd.isoformat(),
                    "n_quarters": dart_quarters,
                    "fundamentals": json.loads(fund_data.model_dump_json()),
                    "filing": json.loads(filing_data.model_dump_json()),
                }
                ticker_path.write_text(
                    json.dumps(result, indent=2, ensure_ascii=False),
                    encoding="utf-8",
                )

                progress["completed"].append(ticker)
                progress["last_ticker"] = ticker
                completed_set.add(ticker)
                done_count += 1

            except (ConnectionError, OSError) as exc:
                tqdm.write(f"[Phase 2] ❌ Connection error at {ticker} — stopping. {exc}")
                progress["failed"].append({"ticker": ticker, "error": str(exc)})
                _save_dart_progress(progress_path, progress)
                tqdm.write("[Phase 2] Progress saved. Resume with same command.")
                raise RuntimeError(f"DART phase stopped at {ticker}; resume with same command") from exc

            except Exception as exc:
                tqdm.write(f"[Phase 2] ⚠ Failed {ticker}: {exc}")
                progress["failed"].append({"ticker": ticker, "error": str(exc)})

            if done_count % 50 == 0:
                _save_dart_progress(progress_path, progress)

    _save_dart_progress(progress_path, progress)
    logger.info(
        "[Phase 2] Done. completed=%d, failed=%d",
        len(progress["completed"]),
        len(progress["failed"]),
    )


# ---------------------------------------------------------------------------
# Phase 3: KR Monthly Snapshots
# ---------------------------------------------------------------------------


async def phase3_kr_snapshots(
    start_month: str,
    end_month: str,
    artifacts_root: Path,
    profile: str = "ci_safe",
) -> None:
    """Build KR monthly snapshots for each month in range."""
    from eit_market_data.snapshot import SnapshotBuilder, SnapshotConfig, create_kr_providers

    months = _month_range(start_month, end_month)

    # Exclude the current (incomplete) month for snapshots
    # Snapshots need a full month of data → cap at previous month
    today = date.today()
    current_month = today.strftime("%Y-%m")
    snapshot_months = [m for m in months if m < current_month]

    logger.info(
        "[Phase 3] KR snapshots: %d months (%s to %s), profile=%s",
        len(snapshot_months), snapshot_months[0] if snapshot_months else "?",
        snapshot_months[-1] if snapshot_months else "?", profile,
    )

    # Full KR market tickers via FDR (KOSPI + KOSDAQ).
    # SnapshotBuilder.build() throttles concurrent provider calls.
    full_tickers = _fdr_all_kr_tickers()

    todo_months = [
        m
        for m in snapshot_months
        if not _snapshot_dir_complete(artifacts_root / "kr" / "snapshots" / m, "kr")
    ]
    skip_n = len(snapshot_months) - len(todo_months)
    logger.info(
        "[Phase 3] KR snapshots: %d months (%d done, %d remaining), %d tickers/month",
        len(snapshot_months), skip_n, len(todo_months), len(full_tickers),
    )

    backfill_dart = BackfillDartProvider(BACKFILL_ROOT / "dart")
    phase_start = time.time()

    for idx, month_str in enumerate(todo_months, 1):
        month_t0 = time.time()
        logger.info(
            "[Phase 3] ▶ START %s (%d/%d, overall %d/%d)",
            month_str, idx, len(todo_months), skip_n + idx, len(snapshot_months),
        )

        try:
            builder = SnapshotBuilder(
                **create_kr_providers(
                    profile=profile,
                    universe_csv=KR_UNIVERSE_CSV,
                    dart_override=backfill_dart,
                )
            )
            config = SnapshotConfig(artifacts_dir=str(artifacts_root / "kr"))

            # Suppress FDR "invalid symbol" print spam
            with _suppress_stdout():
                snapshot = await builder.build_and_persist(month_str, full_tickers, config)

            snapshot_dir = Path(config.artifacts_dir) / "snapshots" / month_str
            field_coverage = _snapshot_field_coverage(snapshot)
            _raise_for_incomplete_kr_snapshot(field_coverage)
            _write_snapshot_manifest(
                snapshot_dir,
                market="kr",
                month=month_str,
                snapshot=snapshot,
                source_profile=profile,
                field_coverage=field_coverage,
            )
            summary = {
                "status": "ok",
                "month": month_str,
                "market": "kr",
                "universe_size": len(snapshot.universe),
                "price_tickers": len(snapshot.prices),
                "fundamental_tickers": len(snapshot.fundamentals),
                "field_coverage": field_coverage,
                "profile": profile,
            }
            (snapshot_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
            elapsed_m = time.time() - month_t0
            logger.info(
                "[Phase 3] ✅ DONE  %s — %dp/%df in %.0fs (%.1f min)",
                month_str, len(snapshot.prices), len(snapshot.fundamentals),
                elapsed_m, elapsed_m / 60,
            )

        except Exception as exc:
            elapsed_m = time.time() - month_t0
            logger.error("[Phase 3] ❌ FAIL  %s after %.0fs: %s", month_str, elapsed_m, exc)
            snap_dir = artifacts_root / "kr" / "snapshots" / month_str
            snap_dir.mkdir(parents=True, exist_ok=True)
            (snap_dir / "summary.json").write_text(
                json.dumps({"status": "failed", "month": month_str, "error": str(exc)}, indent=2),
                encoding="utf-8",
            )

    phase_elapsed = time.time() - phase_start
    logger.info("[Phase 3] All done — %d months in %.1f min.", len(todo_months), phase_elapsed / 60)


# ---------------------------------------------------------------------------
# Phase 4: US Monthly Snapshots (S&P 500)
# ---------------------------------------------------------------------------


async def phase4_us_snapshots(
    start_month: str,
    end_month: str,
    artifacts_root: Path,
) -> None:
    """Build US monthly snapshots for S&P 500."""
    from eit_market_data.snapshot import SnapshotBuilder, SnapshotConfig, create_real_providers

    # Ensure S&P 500 universe
    sp500_path = _ensure_sp500_csv(SP500_CSV)
    tickers = _load_csv_tickers(sp500_path)
    logger.info("[Phase 4] US universe: %d tickers from %s", len(tickers), sp500_path)

    months = _month_range(start_month, end_month)
    today = date.today()
    current_month = today.strftime("%Y-%m")
    snapshot_months = [m for m in months if m < current_month]

    logger.info(
        "[Phase 4] US snapshots: %d months (%s to %s)",
        len(snapshot_months),
        snapshot_months[0] if snapshot_months else "?",
        snapshot_months[-1] if snapshot_months else "?",
    )

    todo_months = [
        m
        for m in snapshot_months
        if not _snapshot_dir_complete(artifacts_root / "us" / "snapshots" / m, "us")
    ]
    skip_n = len(snapshot_months) - len(todo_months)
    logger.info(
        "[Phase 4] US snapshots: %d months (%d done, %d remaining), %d tickers/month",
        len(snapshot_months), skip_n, len(todo_months), len(tickers),
    )
    phase_start = time.time()

    for idx, month_str in enumerate(todo_months, 1):
        month_t0 = time.time()
        logger.info(
            "[Phase 4] ▶ START %s (%d/%d, overall %d/%d)",
            month_str, idx, len(todo_months), skip_n + idx, len(snapshot_months),
        )

        try:
            providers = create_real_providers()
            builder = SnapshotBuilder(**providers)
            config = SnapshotConfig(artifacts_dir=str(artifacts_root / "us"))

            with _suppress_stdout():
                snapshot = await builder.build_and_persist(month_str, tickers, config)

            out_dir = Path(config.artifacts_dir) / "snapshots" / month_str
            field_coverage = _snapshot_field_coverage(snapshot)
            _write_snapshot_manifest(
                out_dir,
                market="us",
                month=month_str,
                snapshot=snapshot,
                source_profile="real",
                field_coverage=field_coverage,
            )
            (out_dir / "metadata.json").write_text(
                json.dumps({
                    "version": "1.0", "market": "us",
                    "decision_date": str(snapshot.decision_date),
                    "execution_date": str(snapshot.execution_date),
                    "universe": snapshot.universe,
                    "providers": ["YFinanceProvider", "FredMacroProvider", "EdgarFilingProvider"],
                }, indent=2, sort_keys=True), encoding="utf-8",
            )
            (out_dir / "summary.json").write_text(
                json.dumps({
                    "status": "ok", "month": month_str, "market": "us",
                    "universe_size": len(snapshot.universe),
                    "price_tickers": len(snapshot.prices),
                    "fundamental_tickers": len(snapshot.fundamentals),
                    "field_coverage": field_coverage,
                }, indent=2), encoding="utf-8",
            )
            elapsed_m = time.time() - month_t0
            logger.info(
                "[Phase 4] ✅ DONE  %s — %dp/%df in %.0fs (%.1f min)",
                month_str, len(snapshot.prices), len(snapshot.fundamentals),
                elapsed_m, elapsed_m / 60,
            )

        except Exception as exc:
            elapsed_m = time.time() - month_t0
            logger.error("[Phase 4] ❌ FAIL  %s after %.0fs: %s", month_str, elapsed_m, exc)
            snap_dir = artifacts_root / "us" / "snapshots" / month_str
            snap_dir.mkdir(parents=True, exist_ok=True)
            (snap_dir / "summary.json").write_text(
                json.dumps({"status": "failed", "month": month_str, "error": str(exc)}, indent=2),
                encoding="utf-8",
            )

    phase_elapsed = time.time() - phase_start
    logger.info("[Phase 4] All done — %d months in %.1f min.", len(todo_months), phase_elapsed / 60)


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def run_all(
    start: str,
    end: str,
    phases: list[int],
    dart_delay: float,
    dart_quarters: int,
    kr_profile: str,
) -> None:
    """Run selected phases sequentially."""
    with logging_redirect_tqdm():
        _run_all_inner(start, end, phases, dart_delay, dart_quarters, kr_profile)


def _run_all_inner(
    start: str,
    end: str,
    phases: list[int],
    dart_delay: float,
    dart_quarters: int,
    kr_profile: str,
) -> None:
    logger.info("=" * 60)
    logger.info("Backfill: %s to %s — phases %s", start, end, phases)
    logger.info("=" * 60)

    pykrx_out = BACKFILL_ROOT / "pykrx"
    dart_out = BACKFILL_ROOT / "dart"

    try:
        if 1 in phases:
            logger.info("Phase 1: KR Raw pykrx")
            phase1_kr_pykrx(start, end, pykrx_out)

        if 2 in phases:
            logger.info("Phase 2: KR DART financials")
            phase2_kr_dart(end, dart_out, dart_delay=dart_delay, dart_quarters=dart_quarters)

        if 3 in phases:
            logger.info("Phase 3: KR Monthly Snapshots")
            asyncio.run(phase3_kr_snapshots(start, end, ARTIFACTS_ROOT, profile=kr_profile))

        if 4 in phases:
            logger.info("Phase 4: US Monthly Snapshots")
            asyncio.run(phase4_us_snapshots(start, end, ARTIFACTS_ROOT))

        logger.info("=" * 60)
        logger.info("All requested phases complete.")
        logger.info("=" * 60)

    except KeyboardInterrupt:
        logger.warning("Interrupted by user (Ctrl+C).")
        raise
    except Exception:
        logger.exception("FATAL: backfill crashed unexpectedly")
        raise


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Full historical market data backfill (KR + US).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full backfill (all 4 phases)
  python scripts/backfill_all.py --start 2022-01 --end 2026-03

  # Only DART financials
  python scripts/backfill_all.py --start 2022-01 --end 2026-03 --phase 2

  # Only US snapshots
  python scripts/backfill_all.py --start 2022-01 --end 2026-03 --phase 4

  # Refresh S&P 500 universe from Wikipedia
  python scripts/backfill_all.py --refresh-sp500
""",
    )
    parser.add_argument("--start", help="Start month (YYYY-MM), e.g. 2022-01")
    parser.add_argument("--end", help="End month (YYYY-MM), e.g. 2026-03")
    parser.add_argument(
        "--phase",
        type=int,
        nargs="+",
        default=[1, 2, 3, 4],
        help="Phases to run (default: all). 1=pykrx, 2=DART, 3=KR snapshots, 4=US snapshots",
    )
    parser.add_argument(
        "--dart-delay",
        type=float,
        default=DART_DELAY_DEFAULT,
        help=f"Seconds between DART API requests (default: {DART_DELAY_DEFAULT})",
    )
    parser.add_argument(
        "--dart-quarters",
        type=int,
        default=DART_QUARTERS_DEFAULT,
        help=(
            "Number of DART quarters to cache per ticker for historical replay "
            f"(default: {DART_QUARTERS_DEFAULT})"
        ),
    )
    parser.add_argument(
        "--kr-profile",
        default="ci_safe",
        choices=["official", "official_enriched", "ci_safe"],
        help="KR snapshot provider profile (default: ci_safe)",
    )
    parser.add_argument(
        "--refresh-sp500",
        action="store_true",
        help="Download/refresh S&P 500 universe from Wikipedia and exit",
    )
    parser.add_argument("--verbose", action="store_true")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[logging.StreamHandler(sys.stderr)],
        force=True,
    )
    # Ensure log lines are flushed immediately (important for tee/pipe)
    for handler in logging.root.handlers:
        handler.flush = handler.stream.flush  # type: ignore[union-attr]

    if args.refresh_sp500:
        _download_sp500(SP500_CSV)
        return

    if not args.start or not args.end:
        parser.error("--start and --end are required (unless --refresh-sp500)")

    run_all(
        start=args.start,
        end=args.end,
        phases=args.phase,
        dart_delay=args.dart_delay,
        dart_quarters=args.dart_quarters,
        kr_profile=args.kr_profile,
    )


if __name__ == "__main__":
    main()
