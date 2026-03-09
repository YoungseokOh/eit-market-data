"""Fetch all available pykrx data for a given date range and save locally.

Usage:
    python scripts/fetch_pykrx_all.py --date 2024-01-31
    python scripts/fetch_pykrx_all.py --start 2024-01-01 --end 2024-01-31
    python scripts/fetch_pykrx_all.py --date 2024-01-31 --output data/ --markets KOSPI KOSDAQ

Data saved to:
    data/market/ohlcv/       OHLCV per ticker
    data/market/cap/         Market cap
    data/market/fundamental/ PER, PBR, EPS, DIV
    data/market/investor/    Investor trading flow
    data/market/shorting/    Short selling data
    data/index/ohlcv/        Index OHLCV (KOSPI, KOSDAQ, KOSPI200)
    data/index/fundamental/  Index valuation
    data/etf/ohlcv/          ETF OHLCV
    data/meta/               Ticker name mappings (CSV)
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from eit_market_data.kr.market_helpers import (
    INDEX_CODE_NAMES,
    fetch_index_ohlcv_frame,
)

logger = logging.getLogger(__name__)

MARKETS = ["KOSPI", "KOSDAQ"]
INDEX_CODES = INDEX_CODE_NAMES
DELAY = 0.3  # seconds between pykrx calls to avoid rate limiting


def _yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def _save_parquet(df, path: Path) -> None:
    import pandas as pd  # noqa: F401

    if df is None or df.empty:
        logger.debug("skip empty: %s", path)
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    df["collected_at"] = datetime.utcnow().isoformat()
    df.to_parquet(path, index=True)
    logger.info("saved %s (%d rows)", path, len(df))


def _save_csv(df, path: Path) -> None:
    if df is None or df.empty:
        logger.debug("skip empty: %s", path)
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    df["collected_at"] = datetime.utcnow().isoformat()
    df.to_csv(path, index=True, encoding="utf-8-sig")
    logger.info("saved %s (%d rows)", path, len(df))


def _call(fn, *args, **kwargs):
    """Call a pykrx function with delay and error handling."""
    try:
        result = fn(*args, **kwargs)
        time.sleep(DELAY)
        return result
    except Exception as e:
        logger.warning("pykrx call failed %s(%s): %s", fn.__name__, args, e)
        time.sleep(DELAY)
        return None


# ---------------------------------------------------------------------------
# Fetchers
# ---------------------------------------------------------------------------

def fetch_market_ohlcv(stock, start: date, end: date, out: Path, market: str) -> None:
    """Fetch OHLCV for all tickers in a market."""
    date_str = _yyyymmdd(end)
    tickers = _call(stock.get_market_ticker_list, date_str, market=market) or []
    logger.info("[%s] %d tickers", market, len(tickers))

    start_str = _yyyymmdd(start)
    end_str = _yyyymmdd(end)

    for ticker in tickers:
        df = _call(stock.get_market_ohlcv, start_str, end_str, ticker)
        _save_parquet(df, out / f"market/ohlcv/{market}/{ticker}.parquet")


def fetch_market_cap(stock, as_of: date, out: Path, market: str) -> None:
    df = _call(stock.get_market_cap, _yyyymmdd(as_of), market=market)
    _save_parquet(df, out / f"market/cap/{market}_{_yyyymmdd(as_of)}.parquet")


def fetch_market_fundamental(stock, as_of: date, out: Path, market: str) -> None:
    df = _call(stock.get_market_fundamental, _yyyymmdd(as_of), market=market)
    _save_parquet(df, out / f"market/fundamental/{market}_{_yyyymmdd(as_of)}.parquet")


def fetch_investor_trading(stock, start: date, end: date, out: Path, market: str) -> None:
    start_str, end_str = _yyyymmdd(start), _yyyymmdd(end)

    # By date (aggregate for all tickers)
    df_val = _call(stock.get_market_trading_value_by_date, start_str, end_str, market)
    _save_parquet(df_val, out / f"market/investor/{market}_value_by_date_{end_str}.parquet")

    df_vol = _call(stock.get_market_trading_volume_by_date, start_str, end_str, market)
    _save_parquet(df_vol, out / f"market/investor/{market}_volume_by_date_{end_str}.parquet")


def fetch_shorting(stock, start: date, end: date, out: Path, market: str) -> None:
    start_str, end_str = _yyyymmdd(start), _yyyymmdd(end)
    date_str = end_str

    # Status by date (aggregate)
    df_status = _call(stock.get_shorting_status_by_date, start_str, end_str, market)
    _save_parquet(df_status, out / f"market/shorting/{market}_status_{end_str}.parquet")

    # Balance by date
    df_bal = _call(stock.get_shorting_balance_by_date, start_str, end_str, market)
    _save_parquet(df_bal, out / f"market/shorting/{market}_balance_{end_str}.parquet")

    # Top 50 by volume
    df_top = _call(stock.get_shorting_volume_top50, date_str, market=market)
    _save_parquet(df_top, out / f"market/shorting/{market}_top50_volume_{date_str}.parquet")

    # Top 50 by balance
    df_top_bal = _call(stock.get_shorting_balance_top50, date_str, market=market)
    _save_parquet(df_top_bal, out / f"market/shorting/{market}_top50_balance_{date_str}.parquet")


def fetch_index_data(stock, start: date, end: date, out: Path) -> None:
    start_str, end_str = _yyyymmdd(start), _yyyymmdd(end)

    for code, name in INDEX_CODES.items():
        df_ohlcv, source = fetch_index_ohlcv_frame(code, start, end, logger_=logger)
        if source:
            logger.info("index OHLCV [%s] source=%s", name, source)
        _save_parquet(df_ohlcv, out / f"index/ohlcv/{name}_{end_str}.parquet")

        df_fund = _call(stock.get_index_fundamental, start_str, end_str, code)
        if df_fund is None or df_fund.empty:
            logger.warning("index fundamental unavailable for %s (%s)", name, code)
        _save_parquet(df_fund, out / f"index/fundamental/{name}_{end_str}.parquet")


def fetch_etf_data(stock, start: date, end: date, out: Path) -> None:
    end_str = _yyyymmdd(end)
    tickers = _call(stock.get_etf_ticker_list, end_str) or []
    logger.info("[ETF] %d tickers", len(tickers))

    start_str = _yyyymmdd(start)
    for ticker in tickers:
        df = _call(stock.get_etf_ohlcv_by_ticker, start_str, end_str, ticker)
        _save_parquet(df, out / f"etf/ohlcv/{ticker}.parquet")


def fetch_ticker_meta(stock, as_of: date, out: Path) -> None:
    """Save ticker → name mapping as CSV for each market."""
    import pandas as pd

    date_str = _yyyymmdd(as_of)
    rows = []
    for market in MARKETS:
        tickers = _call(stock.get_market_ticker_list, date_str, market=market) or []
        for ticker in tickers:
            name = _call(stock.get_market_ticker_name, ticker) or ""
            rows.append({"ticker": ticker, "name": name, "market": market})

    df = pd.DataFrame(rows)
    _save_csv(df, out / f"meta/tickers_{date_str}.csv")


def fetch_foreign_exhaustion(stock, as_of: date, out: Path, market: str) -> None:
    df = _call(stock.get_exhaustion_rates_of_foreign_investment, _yyyymmdd(as_of), market=market)
    _save_parquet(df, out / f"market/foreign/{market}_{_yyyymmdd(as_of)}.parquet")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(start: date, end: date, out: Path, markets: list[str], skip: list[str]) -> None:
    try:
        from pykrx import stock
    except ImportError:
        print("pykrx not installed. Run: pip install pykrx")
        sys.exit(1)

    out.mkdir(parents=True, exist_ok=True)
    logger.info("Fetching pykrx data: %s to %s → %s", start, end, out)

    # Meta first (fast, needed for downstream)
    if "meta" not in skip:
        logger.info("=== Ticker meta ===")
        fetch_ticker_meta(stock, end, out)

    for market in markets:
        if "ohlcv" not in skip:
            logger.info("=== OHLCV [%s] ===", market)
            fetch_market_ohlcv(stock, start, end, out, market)

        if "cap" not in skip:
            logger.info("=== Market cap [%s] ===", market)
            fetch_market_cap(stock, end, out, market)

        if "fundamental" not in skip:
            logger.info("=== Fundamental [%s] ===", market)
            fetch_market_fundamental(stock, end, out, market)

        if "investor" not in skip:
            logger.info("=== Investor trading [%s] ===", market)
            fetch_investor_trading(stock, start, end, out, market)

        if "shorting" not in skip:
            logger.info("=== Short selling [%s] ===", market)
            fetch_shorting(stock, start, end, out, market)

        if "foreign" not in skip:
            logger.info("=== Foreign exhaustion [%s] ===", market)
            fetch_foreign_exhaustion(stock, end, out, market)

    if "index" not in skip:
        logger.info("=== Index data ===")
        fetch_index_data(stock, start, end, out)

    if "etf" not in skip:
        logger.info("=== ETF data ===")
        fetch_etf_data(stock, start, end, out)

    logger.info("Done.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch all pykrx data and save locally.")
    parser.add_argument("--date", help="Single date (YYYY-MM-DD). Sets both start and end.")
    parser.add_argument("--start", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD)")
    parser.add_argument("--output", default="data", help="Output root directory (default: data/)")
    parser.add_argument("--markets", nargs="+", default=MARKETS, help="Markets to fetch")
    parser.add_argument(
        "--skip",
        nargs="*",
        default=[],
        help="Data categories to skip: meta ohlcv cap fundamental investor shorting foreign index etf",
    )
    parser.add_argument("--lookback", type=int, default=300, help="Lookback days when --date used (default: 300 = 12mo momentum + buffer)")
    parser.add_argument("--verbose", action="store_true")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if args.date:
        end = date.fromisoformat(args.date)
        start = end - timedelta(days=args.lookback)
    elif args.start and args.end:
        start = date.fromisoformat(args.start)
        end = date.fromisoformat(args.end)
    else:
        parser.error("Provide --date or both --start and --end")

    run(start, end, Path(args.output), args.markets, args.skip or [])


if __name__ == "__main__":
    main()
