"""Shared helpers for Korean market data sources."""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
SECTOR_SNAPSHOT_DIR = _PROJECT_ROOT / "data/market/sector"

INDEX_CODE_NAMES: dict[str, str] = {
    "1001": "KOSPI",
    "2001": "KOSDAQ",
    "1028": "KOSPI200",
}

INDEX_CODE_SYMBOLS: dict[str, str] = {
    "1001": "YAHOO:^KS11",
    "2001": "YAHOO:^KQ11",
    "1028": "YAHOO:^KS200",
}


def date_to_yyyymmdd(value: date) -> str:
    """Convert a date to the YYYYMMDD format used by KRX APIs."""
    return value.strftime("%Y%m%d")


def normalize_ticker(ticker: str) -> str:
    """Normalize a ticker to a 6-digit KRX stock code."""
    digits = "".join(ch for ch in str(ticker) if ch.isdigit())
    return digits.zfill(6) if digits else str(ticker)


def fetch_index_ohlcv_frame(
    index_code: str,
    start: date,
    end: date,
    logger_: logging.Logger | None = None,
) -> tuple[Any | None, str]:
    """Fetch index OHLCV, falling back to Yahoo data when pykrx is broken."""
    active_logger = logger_ or logger
    pykrx_error: Exception | None = None

    try:
        from pykrx import stock

        try:
            df = stock.get_index_ohlcv_by_date(
                date_to_yyyymmdd(start),
                date_to_yyyymmdd(end),
                index_code,
            )
        except Exception as exc:
            pykrx_error = exc
            df = None

        if df is not None and not df.empty:
            return df, "pykrx"
    except ImportError:
        pass

    symbol = INDEX_CODE_SYMBOLS.get(index_code)
    if not symbol:
        if pykrx_error is not None:
            active_logger.warning("Index %s fetch failed in pykrx: %s", index_code, pykrx_error)
        return None, ""

    try:
        import FinanceDataReader as fdr

        df = fdr.DataReader(symbol, start.isoformat(), end.isoformat())
    except Exception as exc:
        if pykrx_error is not None:
            active_logger.warning(
                "Index %s fetch failed in pykrx (%s) and Yahoo fallback (%s)",
                index_code,
                pykrx_error,
                exc,
            )
        else:
            active_logger.warning("Index %s Yahoo fallback failed: %s", index_code, exc)
        return None, ""

    if df is None or df.empty:
        if pykrx_error is not None:
            active_logger.warning(
                "Index %s pykrx fetch failed (%s) and Yahoo fallback returned empty",
                index_code,
                pykrx_error,
            )
        return None, ""

    if pykrx_error is not None:
        active_logger.warning(
            "Index %s pykrx fetch failed (%s); using Yahoo fallback",
            index_code,
            pykrx_error,
        )
    return df, f"yahoo:{symbol}"


def fetch_live_sector_classification_map(
    market: str,
    as_of: date,
    logger_: logging.Logger | None = None,
    lookback_days: int = 8,
) -> tuple[dict[str, str], date | None]:
    """Try live pykrx sector classifications for recent trading days."""
    active_logger = logger_ or logger
    last_error: Exception | None = None

    try:
        from pykrx import stock
    except ImportError:
        return {}, None

    for offset in range(lookback_days):
        query_day = as_of - timedelta(days=offset)
        query_str = date_to_yyyymmdd(query_day)
        try:
            df = stock.get_market_sector_classifications(query_str, market=market)
        except Exception as exc:
            last_error = exc
            continue

        if df is None or df.empty:
            continue

        frame = df.reset_index() if "종목코드" not in df.columns else df
        if "종목코드" not in frame.columns or "업종명" not in frame.columns:
            continue

        sector_map: dict[str, str] = {}
        for _, row in frame.iterrows():
            code = normalize_ticker(str(row.get("종목코드", "")).strip())
            if not code:
                continue
            sector_name = str(row.get("업종명", "")).strip() or "General"
            sector_map[code] = sector_name

        if sector_map:
            return sector_map, query_day

    if last_error is not None:
        active_logger.warning(
            "Live sector classification unavailable for %s as of %s: %s",
            market,
            as_of,
            last_error,
        )
    return {}, None


def load_sector_snapshot_map(
    market: str,
    as_of: date,
    logger_: logging.Logger | None = None,
    snapshot_dir: Path = SECTOR_SNAPSHOT_DIR,
) -> tuple[dict[str, str], Path | None]:
    """Load the latest cached sector snapshot on or before the requested date."""
    active_logger = logger_ or logger

    try:
        import pandas as pd
    except ImportError:
        return {}, None

    candidates = sorted(snapshot_dir.glob(f"{market}_*.parquet"), reverse=True)
    for path in candidates:
        snapshot_token = path.stem.rsplit("_", 1)[-1]
        try:
            snapshot_date = datetime.strptime(snapshot_token, "%Y%m%d").date()
        except ValueError:
            continue
        if snapshot_date > as_of:
            continue

        try:
            df = pd.read_parquet(path)
        except Exception as exc:
            active_logger.warning("Failed to read sector snapshot %s: %s", path.name, exc)
            continue

        if df is None or df.empty:
            continue

        frame = df.reset_index(drop=True)
        if "종목코드" not in frame.columns or "업종명" not in frame.columns:
            continue

        sector_map: dict[str, str] = {}
        for _, row in frame.iterrows():
            code = normalize_ticker(str(row.get("종목코드", "")).strip())
            if not code:
                continue
            sector_name = str(row.get("업종명", "")).strip() or "General"
            sector_map[code] = sector_name

        if sector_map:
            return sector_map, path

    return {}, None
