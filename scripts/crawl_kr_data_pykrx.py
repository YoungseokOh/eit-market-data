#!/usr/bin/env python3
# ruff: noqa: E402
from __future__ import annotations

import argparse
import logging
import sys
import time
from contextlib import redirect_stderr, redirect_stdout
from datetime import date
from io import StringIO
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))
load_dotenv(PROJECT_ROOT / ".env")

from eit_market_data.kr.market_helpers import INDEX_CODE_NAMES, normalize_ticker
from eit_market_data.kr.pykrx_loader import load_pykrx_stock

logger = logging.getLogger(__name__)

MARKETS = ("KOSPI", "KOSDAQ")
DEFAULT_START = "2024-01-01"
DEFAULT_END = date.today().isoformat()
DEFAULT_DELAY_SECONDS = 0.5
BENCHMARK_INDEX_CODE = "1001"


class _DropMalformedPykrxFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            message = record.getMessage()
        except TypeError:
            message = ""
        if "Expecting value: line 1 column 1" in message:
            return False
        return not (
            isinstance(record.msg, tuple)
            and isinstance(record.args, tuple)
            and len(record.args) == 1
            and isinstance(record.args[0], dict)
        )


def _parse_iso_date(raw: str) -> pd.Timestamp:
    return pd.Timestamp(raw).normalize()


def _call(fn: Any, *args: Any, delay_seconds: float = DEFAULT_DELAY_SECONDS, **kwargs: Any) -> Any:
    try:
        with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
            return fn(*args, **kwargs)
    finally:
        if delay_seconds > 0:
            time.sleep(delay_seconds)


def _safe_call(
    fn: Any,
    *args: Any,
    fallback: Any = None,
    delay_seconds: float = DEFAULT_DELAY_SECONDS,
    **kwargs: Any,
) -> Any:
    try:
        return _call(fn, *args, delay_seconds=delay_seconds, **kwargs)
    except Exception as exc:
        logger.warning("pykrx call failed in collection: %s", exc)
        return fallback


def _month_end_trading_days(
    stock: Any,
    start: pd.Timestamp,
    end: pd.Timestamp,
    delay_seconds: float = DEFAULT_DELAY_SECONDS,
) -> list[pd.Timestamp]:
    start_str = start.strftime("%Y%m%d")
    end_str = end.strftime("%Y%m%d")

    candidates = (
        lambda: stock.get_index_ohlcv_by_date(
            start_str,
            end_str,
            BENCHMARK_INDEX_CODE,
            name_display=False,
        ),
        lambda: stock.get_market_ohlcv_by_date(start_str, end_str, "005930"),
    )
    for fetch in candidates:
        try:
            frame = _call(fetch, delay_seconds=delay_seconds)
        except Exception as exc:
            logger.warning("month-end trading-day probe failed: %s", exc)
            continue
        if frame is not None and not frame.empty:
            idx = pd.DatetimeIndex(frame.sort_index().index)
            month_ends = pd.Series(idx, index=idx).groupby(idx.to_period("M")).max()
            return [pd.Timestamp(value).normalize() for value in month_ends]

    logger.warning("pykrx trading-day probes returned empty; falling back to pandas business month ends")
    return [pd.Timestamp(value).normalize() for value in pd.bdate_range(start, end, freq="BM")]


def _normalize_indexed_frame(frame: pd.DataFrame | None, ticker_column: str = "종목코드") -> pd.DataFrame | None:
    if frame is None or frame.empty:
        return None
    working = frame.copy()
    if ticker_column not in working.columns:
        working = working.reset_index()
        first = working.columns[0]
        if first != ticker_column:
            working = working.rename(columns={first: ticker_column})
    if ticker_column not in working.columns:
        return None
    working[ticker_column] = working[ticker_column].map(normalize_ticker)
    working = working[working[ticker_column] != ""].copy()
    return working


def _normalize_market_cap_frame(
    frame: pd.DataFrame | None,
    market: str,
    trade_date: pd.Timestamp,
    name_map: dict[str, str] | None = None,
) -> pd.DataFrame | None:
    working = _normalize_indexed_frame(frame)
    if working is None or working.empty:
        return None

    working["종목명"] = working["종목코드"].map(name_map or {}).fillna("")
    working["시장"] = market
    working["source_trade_date"] = trade_date.normalize()

    preferred = [
        "종목코드",
        "종목명",
        "시장",
        "종가",
        "시가총액",
        "상장주식수",
        "거래량",
        "거래대금",
        "source_trade_date",
    ]
    present = [column for column in preferred if column in working.columns]
    if not {"종목코드", "종가", "시가총액", "상장주식수"}.issubset(set(present)):
        return None
    return working[present].sort_values("종목코드").reset_index(drop=True)


def _normalize_market_fundamental_frame(
    frame: pd.DataFrame | None,
    market: str,
    trade_date: pd.Timestamp,
) -> pd.DataFrame | None:
    working = _normalize_indexed_frame(frame)
    if working is None or working.empty:
        return None

    working["시장"] = market
    working["source_trade_date"] = trade_date.normalize()
    preferred = ["종목코드", "시장", "BPS", "PER", "PBR", "EPS", "DIV", "DPS", "source_trade_date"]
    present = [column for column in preferred if column in working.columns]
    if not {"종목코드", "BPS", "PER", "PBR", "EPS", "DIV", "DPS"}.issubset(set(present)):
        return None
    return working[present].sort_values("종목코드").reset_index(drop=True)


def _normalize_sector_frame(
    frame: pd.DataFrame | None,
    market: str,
    as_of: pd.Timestamp,
) -> pd.DataFrame | None:
    working = _normalize_indexed_frame(frame)
    if working is None or working.empty:
        return None
    if "업종명" not in working.columns:
        if "지수명" in working.columns:
            working["업종명"] = working["지수명"]
        elif "업종코드" in working.columns:
            working["업종명"] = working["업종코드"].astype(str)
        else:
            return None
    if "종목명" not in working.columns:
        working["종목명"] = ""
    working["시장"] = market
    working["as_of_date"] = as_of.normalize()
    preferred = ["종목코드", "종목명", "시장", "업종명", "지수명", "업종코드", "as_of_date"]
    present = [column for column in preferred if column in working.columns]
    return working[present].sort_values("종목코드").reset_index(drop=True)


def _save_parquet(frame: pd.DataFrame | None, path: Path, overwrite: bool = False) -> Path | None:
    if frame is None or frame.empty:
        logger.warning("skip empty %s", path)
        return None
    if path.exists() and not overwrite:
        logger.info("skip existing %s", path)
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)
    logger.info("saved %s rows=%d", path, len(frame))
    return path


def _load_name_map(stock: Any, as_of: pd.Timestamp, markets: list[str], delay_seconds: float) -> dict[str, str]:
    names: dict[str, str] = {}
    date_str = as_of.strftime("%Y%m%d")
    for market in markets:
        try:
            tickers = _call(
                stock.get_market_ticker_list,
                date_str,
                market=market,
                delay_seconds=delay_seconds,
            )
        except Exception as exc:
            logger.warning("ticker list unavailable for %s: %s", market, exc)
            continue
        for ticker in tickers or []:
            norm = normalize_ticker(ticker)
            try:
                names[norm] = str(_call(stock.get_market_ticker_name, norm, delay_seconds=delay_seconds) or "")
            except Exception:
                names[norm] = ""
    return names


def collect_pykrx_range(
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
    out_root: Path,
    markets: list[str],
    delay_seconds: float = DEFAULT_DELAY_SECONDS,
    overwrite: bool = False,
    skip_meta: bool = False,
    skip_ohlcv: bool = False,
    skip_cap: bool = False,
    skip_fundamental: bool = False,
    skip_index: bool = False,
    skip_sector: bool = False,
) -> list[Path]:
    stock = load_pykrx_stock()
    month_ends = _month_end_trading_days(stock, start, end, delay_seconds=delay_seconds)
    logger.info("month-end trading days: %s", [value.strftime("%Y-%m-%d") for value in month_ends])

    written: list[Path] = []
    out_root.mkdir(parents=True, exist_ok=True)
    name_map = {} if skip_meta else _load_name_map(stock, end, markets, delay_seconds)

    for trade_date in month_ends:
        trade_date_str = trade_date.strftime("%Y%m%d")
        for market in markets:
            if not skip_cap:
                path = out_root / "market/cap_daily" / f"{market}_{trade_date_str}.parquet"
                if not path.exists() or overwrite:
                    frame = _safe_call(
                        stock.get_market_cap,
                        trade_date_str,
                        market=market,
                        delay_seconds=delay_seconds,
                        fallback=None,
                    )
                    saved = _save_parquet(
                        _normalize_market_cap_frame(frame, market, trade_date, name_map),
                        path,
                        overwrite=overwrite,
                    )
                    if saved is not None:
                        written.append(saved)

            if not skip_fundamental:
                path = out_root / "market/fundamental" / f"{market}_{trade_date_str}.parquet"
                if not path.exists() or overwrite:
                    frame = _safe_call(
                        stock.get_market_fundamental,
                        trade_date_str,
                        market=market,
                        delay_seconds=delay_seconds,
                        fallback=None,
                    )
                    saved = _save_parquet(
                        _normalize_market_fundamental_frame(frame, market, trade_date),
                        path,
                        overwrite=overwrite,
                    )
                    if saved is not None:
                        written.append(saved)

            if not skip_sector:
                path = out_root / "market/sector" / f"{market}_{trade_date_str}.parquet"
                if not path.exists() or overwrite:
                    frame = _safe_call(
                        stock.get_market_sector_classifications,
                        trade_date_str,
                        market,
                        delay_seconds=delay_seconds,
                        fallback=None,
                    )
                    saved = _save_parquet(
                        _normalize_sector_frame(frame, market, trade_date),
                        path,
                        overwrite=overwrite,
                    )
                    if saved is not None:
                        written.append(saved)

    if not skip_index:
        start_str = start.strftime("%Y%m%d")
        end_str = end.strftime("%Y%m%d")
        for code, name in INDEX_CODE_NAMES.items():
            frame = _safe_call(
                stock.get_index_ohlcv_by_date,
                start_str,
                end_str,
                code,
                name_display=False,
                delay_seconds=delay_seconds,
                fallback=None,
            )
            if frame is not None and not frame.empty:
                frame = frame.copy()
                frame.index.name = "날짜"
            saved = _save_parquet(
                frame.reset_index() if frame is not None and not frame.empty else None,
                out_root / "index/ohlcv" / f"{name}_{end_str}.parquet",
                overwrite=overwrite,
            )
            if saved is not None:
                written.append(saved)

    if not skip_ohlcv:
        start_str = start.strftime("%Y%m%d")
        end_str = end.strftime("%Y%m%d")
        for market in markets:
            tickers = _safe_call(
                stock.get_market_ticker_list,
                end_str,
                market=market,
                delay_seconds=delay_seconds,
                fallback=[],
            ) or []
            for ticker in tickers:
                norm = normalize_ticker(ticker)
                path = out_root / "market/ohlcv" / market / f"{norm}.parquet"
                if path.exists() and not overwrite:
                    continue
                frame = _safe_call(
                    stock.get_market_ohlcv_by_date,
                    start_str,
                    end_str,
                    norm,
                    adjusted=True,
                    name_display=False,
                    delay_seconds=delay_seconds,
                    fallback=None,
                )
                if frame is not None and not frame.empty:
                    frame = frame.copy()
                    frame.index.name = "날짜"
                saved = _save_parquet(
                    frame.reset_index() if frame is not None and not frame.empty else None,
                    path,
                    overwrite=overwrite,
                )
                if saved is not None:
                    written.append(saved)

    return written


def missing_monthly_market_files(
    out_root: Path,
    month_ends: list[pd.Timestamp],
    markets: list[str],
    subdir: str = "market/cap_daily",
) -> list[Path]:
    base = out_root / subdir
    missing: list[Path] = []
    for month_end in month_ends:
        for market in markets:
            path = base / f"{market}_{month_end:%Y%m%d}.parquet"
            if not path.exists():
                missing.append(path)
    return missing


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect official KR market data through pykrx.")
    parser.add_argument("--start", default=DEFAULT_START, help="Start date (YYYY-MM-DD).")
    parser.add_argument("--end", default=DEFAULT_END, help="End date (YYYY-MM-DD).")
    parser.add_argument("--output-root", default=str(PROJECT_ROOT / "data"), help="Base output directory.")
    parser.add_argument("--markets", nargs="+", default=list(MARKETS), help="Markets to collect.")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY_SECONDS, help="Delay between pykrx calls in seconds.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing parquet files.")
    parser.add_argument("--skip-meta", action="store_true", help="Skip ticker-name enrichment.")
    parser.add_argument("--skip-ohlcv", action="store_true", help="Skip per-ticker OHLCV collection.")
    parser.add_argument("--skip-cap", action="store_true", help="Skip market cap snapshots.")
    parser.add_argument("--skip-fundamental", action="store_true", help="Skip market fundamental snapshots.")
    parser.add_argument("--skip-index", action="store_true", help="Skip benchmark index OHLCV.")
    parser.add_argument("--skip-sector", action="store_true", help="Skip sector snapshots.")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    pykrx_filter = _DropMalformedPykrxFilter()
    root_logger = logging.getLogger()
    root_logger.addFilter(pykrx_filter)
    for handler in root_logger.handlers:
        handler.addFilter(pykrx_filter)

    start = _parse_iso_date(args.start)
    end = _parse_iso_date(args.end)
    if end < start:
        raise SystemExit("--end must be on or after --start")

    markets = [market.upper() for market in args.markets]
    out_root = Path(args.output_root)
    written = collect_pykrx_range(
        start=start,
        end=end,
        out_root=out_root,
        markets=markets,
        delay_seconds=max(0.0, args.delay),
        overwrite=args.overwrite,
        skip_meta=args.skip_meta,
        skip_ohlcv=args.skip_ohlcv,
        skip_cap=args.skip_cap,
        skip_fundamental=args.skip_fundamental,
        skip_index=args.skip_index,
        skip_sector=args.skip_sector,
    )

    missing_cap: list[Path] = []
    if not args.skip_cap:
        stock = load_pykrx_stock()
        month_ends = _month_end_trading_days(stock, start, end, delay_seconds=max(0.0, args.delay))
        missing_cap = missing_monthly_market_files(out_root, month_ends, markets)
    logger.info("written files: %d", len(written))
    if missing_cap:
        logger.error("cap_daily coverage incomplete: missing %d files", len(missing_cap))
        for path in missing_cap[:10]:
            logger.error("missing %s", path)
        raise SystemExit(2)


if __name__ == "__main__":
    main()
