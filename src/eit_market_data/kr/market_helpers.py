"""Shared helpers for Korean market data sources."""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from eit_market_data.kr.krx_auth import (
    KrxAuthRequired,
    ensure_krx_authenticated_session,
    install_pykrx_krx_session_hooks,
)

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

_NON_AUTHORITATIVE_SECTOR_COLUMNS = {"Industry", "ListingDate"}


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
    official_only: bool = True,
) -> tuple[Any | None, str]:
    """Fetch index OHLCV from the official pykrx KRX path."""
    active_logger = logger_ or logger

    try:
        from pykrx import stock

        install_pykrx_krx_session_hooks()
        ensure_krx_authenticated_session(interactive=False)

        try:
            df = stock.get_index_ohlcv_by_date(
                date_to_yyyymmdd(start),
                date_to_yyyymmdd(end),
                index_code,
                name_display=False,
            )
        except Exception as exc:
            active_logger.warning("Index %s fetch failed in pykrx: %s", index_code, exc)
            df = None

        if df is not None and not df.empty:
            df.columns.name = INDEX_CODE_NAMES.get(index_code, index_code)
            return df, "pykrx"
    except ImportError:
        return None, ""

    if not official_only:
        active_logger.warning(
            "Index %s official fetch returned empty; Yahoo fallback is disabled",
            index_code,
        )

    return None, ""


def fetch_market_ticker_list(as_of: date, market: str) -> list[str]:
    """Fetch market ticker list through the authenticated KRX path."""
    from pykrx import stock

    install_pykrx_krx_session_hooks()
    ensure_krx_authenticated_session(interactive=False)
    return stock.get_market_ticker_list(date_to_yyyymmdd(as_of), market=market)


def fetch_market_cap_frame(as_of: date, market: str) -> Any | None:
    """Fetch market-cap snapshot through the authenticated KRX path."""
    from pykrx import stock

    install_pykrx_krx_session_hooks()
    ensure_krx_authenticated_session(interactive=False)
    df = stock.get_market_cap(date_to_yyyymmdd(as_of), market=market)
    if df is None or df.empty:
        return None
    expected = {"종가", "시가총액", "거래량", "거래대금"}
    if not expected.issubset(set(df.columns)):
        raise KrxAuthRequired(
            f"KRX market cap returned unexpected columns for {market}: {list(df.columns)}"
        )
    return df


def fetch_market_fundamental_frame(as_of: date, market: str) -> Any | None:
    """Fetch market fundamental snapshot through the authenticated KRX path."""
    from pykrx import stock

    install_pykrx_krx_session_hooks()
    ensure_krx_authenticated_session(interactive=False)
    df = stock.get_market_fundamental(date_to_yyyymmdd(as_of), market=market)
    if df is None or df.empty:
        return None
    expected = {"BPS", "PER", "PBR", "EPS", "DIV", "DPS"}
    if not expected.issubset(set(df.columns)):
        raise KrxAuthRequired(
            f"KRX fundamental returned unexpected columns for {market}: {list(df.columns)}"
        )
    return df


def latest_krx_trading_day(
    ticker: str,
    as_of: date,
    lookback_days: int = 14,
) -> date | None:
    """Return the most recent trading day with OHLCV on or before ``as_of``."""
    try:
        from pykrx import stock

        install_pykrx_krx_session_hooks()
    except ImportError:
        return None

    start = as_of - timedelta(days=max(lookback_days, 5))
    try:
        df = stock.get_market_ohlcv_by_date(
            date_to_yyyymmdd(start),
            date_to_yyyymmdd(as_of),
            normalize_ticker(ticker),
        )
    except Exception:
        return None

    if df is None or df.empty:
        return None

    last_idx = df.index[-1]
    return last_idx.date() if hasattr(last_idx, "date") else None


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

        install_pykrx_krx_session_hooks()
        ensure_krx_authenticated_session(interactive=False)
    except ImportError:
        return {}, None

    for offset in range(lookback_days):
        query_day = as_of - timedelta(days=offset)
        query_str = date_to_yyyymmdd(query_day)
        try:
            df = stock.get_market_sector_classifications(query_str, market=market)
        except KrxAuthRequired as exc:
            last_error = exc
            break
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
    official_only: bool = True,
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
            active_logger.warning(
                "Failed to read sector snapshot %s: %s", path.name, exc
            )
            continue

        if df is None or df.empty:
            continue

        frame = df.reset_index() if "종목코드" not in df.columns else df
        if "종목코드" not in frame.columns or "업종명" not in frame.columns:
            continue

        if official_only and any(
            col in frame.columns for col in _NON_AUTHORITATIVE_SECTOR_COLUMNS
        ):
            active_logger.warning(
                "Skipping non-authoritative sector snapshot %s in official-only mode",
                path.name,
            )
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
