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
CAP_DAILY_DIR = _PROJECT_ROOT / "data/market/cap_daily"
SECTOR_SNAPSHOT_DIR = _PROJECT_ROOT / "data/market/sector"

INDEX_CODE_NAMES: dict[str, str] = {
    "1001": "KOSPI",
    "2001": "KOSDAQ",
    "1028": "KOSPI200",
}
_INDEX_FDR_SYMBOLS: dict[str, str] = {
    "1001": "^KS11",
    "2001": "^KQ11",
    "1028": "^KS200",
}
CAP_MONTHLY_DIR = _PROJECT_ROOT / "data/market/cap"
_LOCAL_CAP_MONTHLY_MAX_GAP_DAYS = 45
_PUBLIC_MARKET_CAP_MAX_AGE_DAYS = 45
_LOCAL_CAP_DAILY_MAX_GAP_DAYS = 7

_NON_AUTHORITATIVE_SECTOR_COLUMNS = {"Industry", "ListingDate"}


def date_to_yyyymmdd(value: date) -> str:
    """Convert a date to the YYYYMMDD format used by KRX APIs."""
    return value.strftime("%Y%m%d")


def normalize_ticker(ticker: str) -> str:
    """Normalize a ticker to a 6-digit KRX stock code."""
    digits = "".join(ch for ch in str(ticker) if ch.isdigit())
    return digits.zfill(6) if digits else str(ticker)


def _load_fdr():  # noqa: ANN202
    import FinanceDataReader as fdr

    return fdr


def _normalize_listing_frame(frame: Any) -> Any | None:
    if frame is None or frame.empty or "Code" not in frame.columns:
        return None
    normalized = frame.copy()
    normalized["Code"] = normalized["Code"].map(lambda value: normalize_ticker(str(value)))
    normalized = normalized[normalized["Code"] != ""]
    return normalized


def _load_local_market_cap_snapshot(
    as_of: date,
    market: str,
    snapshot_dir: Path | None = None,
) -> Any | None:
    snapshot_dir = snapshot_dir or CAP_DAILY_DIR
    if not snapshot_dir.exists():
        return None

    prefix = market.upper()
    candidates: list[tuple[date, Path]] = []
    for path in snapshot_dir.glob(f"{prefix}_*.parquet"):
        stem = path.stem
        try:
            trade_date = datetime.strptime(stem.rsplit("_", 1)[-1], "%Y%m%d").date()
        except ValueError:
            continue
        if trade_date <= as_of:
            candidates.append((trade_date, path))

    if not candidates:
        return None

    trade_date, path = max(candidates, key=lambda item: item[0])
    if (as_of - trade_date).days > _LOCAL_CAP_DAILY_MAX_GAP_DAYS:
        logger.warning(
            "Local market cap snapshot for %s is too stale for %s: %s",
            market,
            as_of,
            path.name,
        )
        return None

    import pandas as pd

    frame = pd.read_parquet(path)
    if frame is None or frame.empty:
        return None

    if "종목코드" not in frame.columns:
        if frame.index.name == "종목코드" or frame.index.dtype == object:
            frame = frame.reset_index()
        else:
            return None

    frame = frame.copy()
    frame["종목코드"] = frame["종목코드"].map(lambda value: normalize_ticker(str(value)))
    return frame.set_index("종목코드", drop=True)


def fetch_stock_ohlcv_frame(
    ticker: str,
    start: date,
    end: date,
    logger_: logging.Logger | None = None,
) -> tuple[Any | None, str]:
    """Fetch stock OHLCV through FinanceDataReader public routes."""
    active_logger = logger_ or logger
    norm_ticker = normalize_ticker(ticker)

    try:
        fdr = _load_fdr()
    except ImportError:
        return None, ""

    for symbol, source in ((norm_ticker, "fdr"), (f"NAVER:{norm_ticker}", "naver")):
        try:
            df = fdr.DataReader(symbol, start.isoformat(), end.isoformat())
        except Exception as exc:
            active_logger.warning("Ticker %s fetch failed in %s: %s", norm_ticker, source, exc)
            continue
        if df is not None and not df.empty:
            return df, source

    return None, ""


def _fetch_index_ohlcv_frame_pykrx(
    index_code: str,
    start: date,
    end: date,
    logger_: logging.Logger | None = None,
) -> tuple[Any | None, str]:
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

    return None, ""


def fetch_index_ohlcv_frame(
    index_code: str,
    start: date,
    end: date,
    logger_: logging.Logger | None = None,
    official_only: bool = True,
) -> tuple[Any | None, str]:
    """Fetch index OHLCV from public FinanceDataReader index symbols."""
    active_logger = logger_ or logger
    symbol = _INDEX_FDR_SYMBOLS.get(index_code)

    if symbol is not None:
        try:
            fdr = _load_fdr()
            df = fdr.DataReader(symbol, start.isoformat(), end.isoformat())
            if df is not None and not df.empty:
                df.columns.name = INDEX_CODE_NAMES.get(index_code, index_code)
                return df, "fdr"
        except ImportError:
            pass
        except Exception as exc:
            active_logger.warning("Index %s fetch failed in fdr: %s", index_code, exc)

    frame, source = _fetch_index_ohlcv_frame_pykrx(index_code, start, end, logger_=active_logger)
    if frame is not None and not frame.empty:
        return frame, source

    if not official_only:
        active_logger.warning(
            "Index %s official fetch returned empty; Yahoo fallback is disabled",
            index_code,
        )

    return None, ""


def fetch_market_ticker_list(as_of: date, market: str) -> list[str]:
    """Fetch market ticker list through FinanceDataReader public listings."""
    _ = as_of
    try:
        frame = _normalize_listing_frame(_load_fdr().StockListing(market.upper()))
        if frame is not None and not frame.empty:
            return frame["Code"].tolist()
    except ImportError:
        pass
    except Exception as exc:
        logger.warning("Market ticker list fetch failed in fdr for %s: %s", market, exc)

    from pykrx import stock

    install_pykrx_krx_session_hooks()
    ensure_krx_authenticated_session(interactive=False)
    return stock.get_market_ticker_list(date_to_yyyymmdd(as_of), market=market)


def _load_local_monthly_cap_snapshot(
    as_of: date,
    market: str,
    snapshot_dir: Path | None = None,
) -> Any | None:
    """Load the latest monthly cap parquet on or before as_of from data/market/cap/."""
    snapshot_dir = snapshot_dir or CAP_MONTHLY_DIR
    if not snapshot_dir.exists():
        return None

    prefix = market.upper()
    candidates: list[tuple[date, Path]] = []
    for path in snapshot_dir.glob(f"{prefix}_*.parquet"):
        stem = path.stem
        token = stem.rsplit("_", 1)[-1]
        for fmt in ("%Y%m%d", "%Y%m"):
            try:
                parsed = datetime.strptime(token, fmt).date()
                break
            except ValueError:
                continue
        else:
            continue
        if parsed <= as_of:
            candidates.append((parsed, path))

    if not candidates:
        return None

    snapshot_date, path = max(candidates, key=lambda item: item[0])
    gap_days = (as_of - snapshot_date).days
    if gap_days > _LOCAL_CAP_MONTHLY_MAX_GAP_DAYS:
        logger.warning(
            "Local monthly cap snapshot for %s may be stale (%d days) for %s: %s — using anyway",
            market,
            gap_days,
            as_of,
            path.name,
        )

    import pandas as pd

    frame = pd.read_parquet(path)
    if frame is None or frame.empty:
        return None

    # Normalise to the standard format: index=종목코드, columns include 시가총액 and 종가
    frame = frame.reset_index(drop=True)
    if "ticker" in frame.columns:
        frame = frame.rename(columns={"ticker": "종목코드"})
    if "종목코드" not in frame.columns:
        return None
    frame["종목코드"] = frame["종목코드"].map(lambda v: normalize_ticker(str(v)))
    # Keep only the latest row per ticker (in case of duplicates)
    frame = frame.sort_values("source_trade_date", ascending=True).drop_duplicates(
        subset=["종목코드"], keep="last"
    ) if "source_trade_date" in frame.columns else frame.drop_duplicates(subset=["종목코드"])
    return frame.set_index("종목코드", drop=True)


def fetch_market_cap_frame(as_of: date, market: str) -> Any | None:
    """Fetch market-cap snapshot.

    Fallback order:
    1. Local daily cache (data/market/cap_daily/)
    2. FDR public StockListing (within 45 days)
    3. pykrx KRX authenticated path
    """
    local_frame = _load_local_market_cap_snapshot(as_of, market)
    if local_frame is not None and not local_frame.empty:
        return local_frame

    age_days = (date.today() - as_of).days
    if age_days > _PUBLIC_MARKET_CAP_MAX_AGE_DAYS:
        logger.warning(
            "Market cap snapshot for %s as of %s is outside the public FDR window",
            market,
            as_of,
        )
        return None

    try:
        frame = _normalize_listing_frame(_load_fdr().StockListing(market.upper()))
        if frame is not None and not frame.empty:
            renamed = frame.rename(
                columns={
                    "Code": "종목코드",
                    "Name": "종목명",
                    "Market": "시장",
                    "Dept": "소속부",
                    "Close": "종가",
                    "Volume": "거래량",
                    "Amount": "거래대금",
                    "Marcap": "시가총액",
                    "Stocks": "상장주식수",
                }
            )
            renamed = renamed.set_index("종목코드", drop=True)
            expected = {"종가", "시가총액", "거래량", "거래대금"}
            if not expected.issubset(set(renamed.columns)):
                raise RuntimeError(
                    f"public market cap returned unexpected columns for {market}: {list(renamed.columns)}"
                )
            return renamed
    except ImportError:
        pass
    except RuntimeError:
        raise
    except Exception as exc:
        logger.warning("Market cap fetch failed in fdr for %s: %s", market, exc)

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
    start = as_of - timedelta(days=max(lookback_days, 5))
    df, _source = fetch_stock_ohlcv_frame(ticker, start, as_of)

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
    """Build a live sector map from FinanceDataReader KRX-DESC listings."""
    active_logger = logger_ or logger
    _ = lookback_days

    try:
        frame = _normalize_listing_frame(_load_fdr().StockListing("KRX-DESC"))
    except ImportError:
        return {}, None
    except Exception as exc:
        active_logger.warning(
            "Live sector classification unavailable for %s as of %s: %s",
            market,
            as_of,
            exc,
        )
        return {}, None

    if frame is None or frame.empty:
        return {}, None

    if market:
        market_prefix = market.upper()
        if "Market" in frame.columns:
            frame = frame[
                frame["Market"].astype(str).str.upper().str.startswith(market_prefix)
            ]

    sector_map: dict[str, str] = {}
    for _, row in frame.iterrows():
        code = normalize_ticker(str(row.get("Code", "")).strip())
        if not code:
            continue
        industry = str(row.get("Industry", "")).strip()
        sector = str(row.get("Sector", "")).strip()
        sector_name = industry or sector or "General"
        sector_map[code] = sector_name

    return sector_map, as_of if sector_map else None


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
