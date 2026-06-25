#!/usr/bin/env python3
# ruff: noqa: E402
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

import FinanceDataReader as fdr
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import bootstrap

PROJECT_ROOT = bootstrap(load_env=False)

from eit_market_data.kr.krx_auth import (
    KrxAuthRequired,
    ensure_krx_authenticated_session,
    install_pykrx_krx_session_hooks,
)
from eit_market_data.kr.pykrx_loader import load_pykrx_stock

logger = logging.getLogger(__name__)

MARKETS = ("KOSPI", "KOSDAQ")
INDEX_SYMBOL = "YAHOO:^KS11"
DEFAULT_START = "2022-01-01"
DEFAULT_END = "2023-04-30"
DEFAULT_DELAY_SECONDS = 0.3


def _parse_iso_date(raw: str) -> pd.Timestamp:
    return pd.Timestamp(raw).normalize()


def _month_end_business_days(start: pd.Timestamp, end: pd.Timestamp) -> list[pd.Timestamp]:
    idx = fdr.DataReader(INDEX_SYMBOL, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    idx = idx.sort_index()
    return idx.groupby(idx.index.to_period("M")).tail(1).index.to_list()


def _normalize_ticker(value: object) -> str:
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    return digits.zfill(6) if digits else ""


def _load_name_map() -> dict[str, dict[str, str]]:
    mapping: dict[str, dict[str, str]] = {}
    for market in MARKETS:
        frame = fdr.StockListing(f"{market}-DESC")
        if frame is None or frame.empty:
            mapping[market] = {}
            continue
        working = frame.rename(columns={"Code": "종목코드", "Name": "종목명"}).copy()
        working["종목코드"] = working["종목코드"].map(_normalize_ticker)
        working = working[working["종목코드"] != ""]
        mapping[market] = dict(zip(working["종목코드"], working["종목명"], strict=False))
    return mapping


def _normalize_market_cap_frame(
    frame: pd.DataFrame | None,
    market: str,
    trade_date: pd.Timestamp,
    name_map: dict[str, str],
) -> pd.DataFrame | None:
    if frame is None or frame.empty:
        return None

    working = frame.copy()
    if "종목코드" not in working.columns:
        working = working.reset_index()
        first = working.columns[0]
        if first in {"티커", "ticker", "index"}:
            working = working.rename(columns={first: "종목코드"})
    if "종목코드" not in working.columns:
        return None

    working["종목코드"] = working["종목코드"].map(_normalize_ticker)
    working = working[working["종목코드"] != ""].copy()
    working["종목명"] = working["종목코드"].map(name_map).fillna("")
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

    ordered = working[present].sort_values("종목코드").reset_index(drop=True)
    return ordered


def _save_market_cap_daily(frame: pd.DataFrame, out_dir: Path, market: str, trade_date: pd.Timestamp) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{market}_{trade_date:%Y%m%d}.parquet"
    frame.to_parquet(path, index=False)
    return path


def missing_cap_daily_files(out_root: Path, month_ends: list[pd.Timestamp]) -> list[Path]:
    out_dir = out_root / "market/cap_daily"
    missing: list[Path] = []
    for month_end in month_ends:
        for market in MARKETS:
            path = out_dir / f"{market}_{month_end:%Y%m%d}.parquet"
            if not path.exists():
                missing.append(path)
    return missing


def collect_gap_range(
    start: pd.Timestamp,
    end: pd.Timestamp,
    out_root: Path,
    delay_seconds: float = DEFAULT_DELAY_SECONDS,
    overwrite: bool = False,
) -> list[Path]:
    stock = load_pykrx_stock()

    install_pykrx_krx_session_hooks()
    ensure_krx_authenticated_session(interactive=False)

    month_ends = _month_end_business_days(start, end)
    name_map = _load_name_map()
    out_dir = out_root / "market/cap_daily"
    written: list[Path] = []

    logger.info("Gap fill month-ends: %s", [value.strftime("%Y-%m-%d") for value in month_ends])

    for trade_date in month_ends:
        trade_date_str = trade_date.strftime("%Y%m%d")
        for market in MARKETS:
            path = out_dir / f"{market}_{trade_date_str}.parquet"
            if path.exists() and not overwrite:
                logger.info("skip existing %s", path)
                continue

            logger.info("fetch market cap %s %s", market, trade_date_str)
            frame = stock.get_market_cap(trade_date_str, market=market)
            normalized = _normalize_market_cap_frame(frame, market, trade_date, name_map.get(market, {}))
            if normalized is None or normalized.empty:
                logger.warning("empty market cap %s %s", market, trade_date_str)
                continue
            saved = _save_market_cap_daily(normalized, out_dir, market, trade_date)
            written.append(saved)
            logger.info("saved %s rows=%d", saved, len(normalized))
            time.sleep(max(delay_seconds, 0.0))

    return written


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fill the pre-3Y KR market-cap gap into data/market/cap_daily/ using authenticated KRX pykrx access."
    )
    parser.add_argument("--start", default=DEFAULT_START, help="Start date (YYYY-MM-DD).")
    parser.add_argument("--end", default=DEFAULT_END, help="End date (YYYY-MM-DD).")
    parser.add_argument("--output-root", default=str(PROJECT_ROOT / "data"), help="Base output directory.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing cap_daily parquet files.")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY_SECONDS, help="Delay between market calls in seconds.")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    start = _parse_iso_date(args.start)
    end = _parse_iso_date(args.end)
    out_root = Path(args.output_root)

    if end < start:
        raise SystemExit("--end must be on or after --start")

    try:
        written = collect_gap_range(
            start,
            end,
            out_root,
            delay_seconds=args.delay,
            overwrite=args.overwrite,
        )
    except KrxAuthRequired as exc:
        raise SystemExit(str(exc)) from exc

    month_ends = _month_end_business_days(start, end)
    missing = missing_cap_daily_files(out_root, month_ends)
    logger.info("written files: %d", len(written))
    if missing:
        logger.error("cap_daily gap-fill incomplete: missing %d files", len(missing))
        for path in missing[:10]:
            logger.error("missing %s", path)
        raise SystemExit(2)

    logger.info("cap_daily gap-fill complete for %s to %s", start.date(), end.date())


if __name__ == "__main__":
    main()
