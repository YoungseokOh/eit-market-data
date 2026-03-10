# ruff: noqa: E402
from __future__ import annotations

import argparse
import time
import sys
import warnings
from datetime import date, datetime
from pathlib import Path

import pandas as pd
from pykrx import stock

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from eit_market_data.kr.market_helpers import (
    INDEX_CODE_NAMES,
    fetch_index_ohlcv_frame,
)
from eit_market_data.kr.krx_auth import (
    ensure_krx_authenticated_session,
    install_pykrx_krx_session_hooks,
)

DELAY = 0.3
START = "20240101"
END = "20241231"
SECTOR_DATE = "20241227"

UNIVERSE_CSV = PROJECT_ROOT / "universes/kr_universe.csv"
OUTPUT_ROOT = PROJECT_ROOT / "data"

INDEX_CODES = INDEX_CODE_NAMES


def _parse_yyyymmdd(raw: str) -> datetime:
    return datetime.strptime(raw, "%Y%m%d")


def _resolve_runtime_dates(
    as_of_raw: str | None,
    start_raw: str | None,
    end_raw: str | None,
    sector_date_raw: str | None,
) -> tuple[str, str, str]:
    as_of = date.fromisoformat(as_of_raw) if as_of_raw else date.today()
    start = start_raw or f"{as_of.year}0101"
    end = end_raw or as_of.strftime("%Y%m%d")
    sector_date = sector_date_raw or end
    return start, end, sector_date


def safe_call(fn, *args, **kwargs):
    try:
        result = fn(*args, **kwargs)
        time.sleep(DELAY)
        return result
    except Exception as exc:
        warnings.warn(f"{fn.__name__}{args} failed: {exc}")
        time.sleep(DELAY)
        return None


def save_parquet(df: pd.DataFrame | None, path: Path) -> bool:
    if df is None or df.empty:
        print(f"[SKIP] empty: {path}")
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=True)
    print(f"[SAVE] {path} rows={len(df)}")
    return True


def load_universe_tickers(path: Path) -> list[str]:
    df = pd.read_csv(path, dtype={"ticker": str})
    tickers = df["ticker"].dropna().astype(str).str.zfill(6).tolist()
    print(f"[INFO] universe loaded: {len(tickers)} tickers")
    return tickers


def _month_ends_from_df(df: pd.DataFrame) -> list[tuple[str, str]]:
    tmp = df.copy()
    tmp.index = pd.to_datetime(tmp.index)
    last_days = tmp.groupby(tmp.index.to_period("M")).tail(1).index
    out: list[tuple[str, str]] = []
    for d in last_days:
        yyyymm = d.strftime("%Y%m")
        yyyymmdd = d.strftime("%Y%m%d")
        out.append((yyyymm, yyyymmdd))
    return out


def get_month_end_business_days(fallback_ticker: str) -> list[tuple[str, str]]:
    print("[STEP] Calculating month-end business days from KOSPI index OHLCV")
    idx_df, source = fetch_index_ohlcv_frame(
        "1001",
        _parse_yyyymmdd(START).date(),
        _parse_yyyymmdd(END).date(),
    )
    out: list[tuple[str, str]] = []
    source_label = source or "index"
    if idx_df is not None and not idx_df.empty:
        out = _month_ends_from_df(idx_df)
    else:
        warnings.warn(
            "Failed to compute month-end business days from official KOSPI index data."
        )

    for yyyymm, yyyymmdd in out:
        print(f"[INFO] month-end business day ({source_label}): {yyyymm} -> {yyyymmdd}")
    return out


def fetch_ohlcv_per_ticker(tickers: list[str]) -> None:
    print("[STEP] 1) Fetching OHLCV per ticker")
    for i, ticker in enumerate(tickers, start=1):
        print(f"[OHLCV] ({i}/{len(tickers)}) ticker={ticker}")
        df = safe_call(stock.get_market_ohlcv, START, END, ticker)
        save_parquet(df, OUTPUT_ROOT / f"market/ohlcv/{ticker}.parquet")


def fetch_market_cap(month_days: list[tuple[str, str]]) -> None:
    print("[STEP] 2) Fetching monthly market cap for KOSPI/KOSDAQ")
    for yyyymm, yyyymmdd in month_days:
        for market in ("KOSPI", "KOSDAQ"):
            print(f"[CAP] market={market}, date={yyyymmdd}")
            df = safe_call(stock.get_market_cap, yyyymmdd, market=market)
            save_parquet(df, OUTPUT_ROOT / f"market/cap/{market}_{yyyymm}.parquet")


def fetch_fundamental(month_days: list[tuple[str, str]]) -> None:
    print("[STEP] 3) Fetching monthly fundamentals (PER/PBR/EPS)")
    for yyyymm, yyyymmdd in month_days:
        market = "KOSPI"
        print(f"[FUND] market={market}, date={yyyymmdd}")
        df = safe_call(stock.get_market_fundamental, yyyymmdd, market=market)
        save_parquet(df, OUTPUT_ROOT / f"market/fundamental/{market}_{yyyymm}.parquet")


def fetch_index_ohlcv() -> None:
    print("[STEP] 4) Fetching index OHLCV")
    filename_suffix = END[:4] if START[:4] == END[:4] else f"{START}_{END}"
    for code, name in INDEX_CODES.items():
        print(f"[INDEX] {name} ({code})")
        df, source = fetch_index_ohlcv_frame(
            code,
            _parse_yyyymmdd(START).date(),
            _parse_yyyymmdd(END).date(),
        )
        if source:
            print(f"[INFO] index source={source}")
        save_parquet(df, OUTPUT_ROOT / f"index/ohlcv/{name}_{filename_suffix}.parquet")


def fetch_sector_classification() -> None:
    print("[STEP] 5) Fetching sector classifications")
    market = "KOSPI"
    print(f"[SECTOR] market={market}, date={SECTOR_DATE}")
    df = safe_call(stock.get_market_sector_classifications, SECTOR_DATE, market=market)
    snapshot_path = OUTPUT_ROOT / f"market/sector/{market}_{SECTOR_DATE}.parquet"
    if df is None or df.empty:
        if snapshot_path.exists():
            print(f"[SKIP] keeping existing sector snapshot: {snapshot_path}")
        else:
            print(f"[SKIP] no sector snapshot available: {snapshot_path}")
        return
    save_parquet(df, snapshot_path)


def main() -> None:
    global START, END, SECTOR_DATE, UNIVERSE_CSV, OUTPUT_ROOT

    parser = argparse.ArgumentParser(
        description="Fetch KR market data into parquet files."
    )
    parser.add_argument(
        "--as-of",
        help="Reference date in YYYY-MM-DD. Used to derive default start/end/sector dates.",
    )
    parser.add_argument("--start", help="Start date in YYYYMMDD format.")
    parser.add_argument("--end", help="End date in YYYYMMDD format.")
    parser.add_argument(
        "--sector-date",
        help="Sector snapshot date in YYYYMMDD format. Defaults to end date.",
    )
    parser.add_argument(
        "--universe-csv",
        default=str(UNIVERSE_CSV),
        help="Universe CSV path.",
    )
    parser.add_argument(
        "--output-root",
        default=str(OUTPUT_ROOT),
        help="Output directory for parquet artifacts.",
    )
    args = parser.parse_args()

    START, END, SECTOR_DATE = _resolve_runtime_dates(
        args.as_of,
        args.start,
        args.end,
        args.sector_date,
    )
    UNIVERSE_CSV = Path(args.universe_csv)
    OUTPUT_ROOT = Path(args.output_root)
    install_pykrx_krx_session_hooks()
    ensure_krx_authenticated_session(interactive=False)

    tickers = load_universe_tickers(UNIVERSE_CSV)
    month_days = get_month_end_business_days(tickers[0])

    fetch_ohlcv_per_ticker(tickers)
    if month_days:
        fetch_market_cap(month_days)
        fetch_fundamental(month_days)
    else:
        print("[WARN] Skipping monthly cap/fundamental due to missing month-end dates.")
    fetch_index_ohlcv()
    fetch_sector_classification()
    print("[DONE] Data crawling finished.")


if __name__ == "__main__":
    main()
