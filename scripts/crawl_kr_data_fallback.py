from __future__ import annotations

from pathlib import Path as _Path
from dotenv import load_dotenv as _load_dotenv
_load_dotenv(_Path(__file__).resolve().parents[1] / ".env")

import argparse
import concurrent.futures
import json
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import FinanceDataReader as fdr
import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_ROOT = PROJECT_ROOT / "data"
DEFAULT_UNIVERSE_CSV: Path | None = None
DEFAULT_START = "2024-01-01"
DEFAULT_END = date.today().isoformat()

FG_BASE = "https://cdn.fnguide.com/SVO2/json/chart"
FG_TIMEOUT = 10
FG_MAX_WORKERS = 24
FG_RETRIES = 2

INDEX_SYMBOLS = {
    "KOSPI": "YAHOO:^KS11",
    "KOSDAQ": "YAHOO:^KQ11",
    "KOSPI200": "YAHOO:^KS200",
}


def _to_num(value: object) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if text in {"", "-", "N/A", "nan", "None"}:
        return None
    text = text.replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def _parse_json_response(resp: requests.Response) -> dict | None:
    if resp.status_code != 200:
        return None
    text = resp.content.decode("utf-8-sig", errors="ignore").strip()
    if not text.startswith("{"):
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def _fnguide_get(path: str) -> dict | None:
    url = f"{FG_BASE}/{path}"
    headers = {"User-Agent": "Mozilla/5.0"}
    for _ in range(FG_RETRIES + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=FG_TIMEOUT)
        except requests.RequestException:
            continue
        parsed = _parse_json_response(resp)
        if parsed is not None:
            return parsed
    return None


def _parse_iso_date(raw: str) -> pd.Timestamp:
    return pd.Timestamp(raw).normalize()


def _month_end_business_days(start: pd.Timestamp, end: pd.Timestamp) -> list[pd.Timestamp]:
    idx = fdr.DataReader("YAHOO:^KS11", start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    idx = idx.sort_index()
    return idx.groupby(idx.index.to_period("M")).tail(1).index.to_list()


@dataclass
class TickerMeta:
    ticker: str
    market: str
    name: str = ""


def _load_tickers(universe_csv: Path | None = None) -> list[TickerMeta]:
    if universe_csv is not None and universe_csv.exists():
        frame = pd.read_csv(universe_csv, dtype={"ticker": str})
        tickers = [
            TickerMeta(
                ticker=str(row.get("ticker", "")).strip().zfill(6),
                market=str(row.get("market", "")).strip().upper(),
                name=str(row.get("name", "")).strip(),
            )
            for _, row in frame.iterrows()
            if str(row.get("ticker", "")).strip()
        ]
        return [meta for meta in tickers if meta.market in {"KOSPI", "KOSDAQ"}]

    frames = []
    for market in ("KOSPI", "KOSDAQ"):
        frame = fdr.StockListing(f"{market}-DESC")
        if frame is None or frame.empty:
            continue
        frame = frame.rename(columns={"Code": "ticker", "Name": "name", "Market": "market"})
        frame["ticker"] = frame["ticker"].astype(str).str.zfill(6)
        frames.append(frame[["ticker", "market", "name"]])

    if not frames:
        return []

    merged = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["ticker"])
    return [
        TickerMeta(
            ticker=str(row["ticker"]).strip(),
            market=str(row["market"]).strip().upper(),
            name=str(row["name"]).strip(),
        )
        for _, row in merged.sort_values(["market", "ticker"]).iterrows()
    ]


def _extract_daily_cap(
    meta: TickerMeta,
    start: pd.Timestamp,
    end: pd.Timestamp,
    month_end_map: dict[pd.Period, pd.Timestamp],
) -> list[dict]:
    obj = _fnguide_get(f"01_01/chart_A{meta.ticker}_3Y.json")
    if not obj:
        return []

    chart = obj.get("CHART", [])
    if not isinstance(chart, list) or not chart:
        return []

    frame = pd.DataFrame(chart)
    if "TRD_DT" not in frame.columns or "MKT_CAP" not in frame.columns or "J_PRC" not in frame.columns:
        return []

    frame["TRD_DT"] = pd.to_datetime(frame["TRD_DT"], errors="coerce")
    frame = frame.dropna(subset=["TRD_DT"]).sort_values("TRD_DT")
    frame["TRD_DT"] = frame["TRD_DT"].dt.normalize()
    frame["MKT_CAP"] = frame["MKT_CAP"].map(_to_num)
    frame["J_PRC"] = frame["J_PRC"].map(_to_num)
    frame = frame[(frame["TRD_DT"] >= start) & (frame["TRD_DT"] <= end)]

    rows: list[dict] = []
    for _, row in frame.iterrows():
        trade_date = pd.Timestamp(row["TRD_DT"]).normalize()
        target_trade_date = month_end_map.get(trade_date.to_period("M"))
        if target_trade_date is None:
            continue
        close = row.get("J_PRC")
        cap_eok = row.get("MKT_CAP")
        if close is None or close <= 0 or cap_eok is None or cap_eok <= 0:
            continue
        market_cap = int(round(cap_eok * 100_000_000))
        issued_shares = int(round(market_cap / close))
        rows.append(
            {
                "종목코드": meta.ticker,
                "종목명": meta.name,
                "시장": meta.market,
                "종가": int(round(close)),
                "시가총액": market_cap,
                "상장주식수": issued_shares,
                "source_trade_date": target_trade_date,
            }
        )
    return rows


def _extract_x_multiplier(name: str) -> float | None:
    match = re.search(r"([0-9]+(?:\\.[0-9]+)?)X", str(name))
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def _extract_monthly_fundamental(meta: TickerMeta, month_ends: list[pd.Timestamp]) -> list[dict]:
    obj = _fnguide_get(f"01_06/chart_A{meta.ticker}_D.json")
    if not obj:
        return []

    chart_e = obj.get("CHART_E", [])
    chart_b = obj.get("CHART_B", [])
    chart = obj.get("CHART", [])
    if not isinstance(chart, list) or not chart:
        return []

    e3_mult = None
    b3_mult = None
    for item in chart_e:
        if item.get("ID") == "E3":
            e3_mult = _extract_x_multiplier(item.get("NAME", ""))
            break
    for item in chart_b:
        if item.get("ID") == "B3":
            b3_mult = _extract_x_multiplier(item.get("NAME", ""))
            break
    if not e3_mult or not b3_mult:
        return []

    frame = pd.DataFrame(chart)
    if "GS_YM" not in frame.columns or "PRICE" not in frame.columns:
        return []

    frame["GS_YM"] = pd.to_datetime(frame["GS_YM"], errors="coerce")
    frame = frame.dropna(subset=["GS_YM"]).sort_values("GS_YM")
    frame["PRICE"] = frame["PRICE"].map(_to_num)
    frame["E3"] = frame.get("E3", pd.Series(dtype=float)).map(_to_num)
    frame["B3"] = frame.get("B3", pd.Series(dtype=float)).map(_to_num)

    rows: list[dict] = []
    for month_end in month_ends:
        sub = frame[frame["GS_YM"].dt.to_period("M") == month_end.to_period("M")]
        if sub.empty:
            continue
        row = sub.iloc[-1]
        price = row.get("PRICE")
        e3 = row.get("E3")
        b3 = row.get("B3")
        if price is None or e3 is None or b3 is None:
            continue

        eps = e3 / e3_mult if e3_mult else None
        bps = b3 / b3_mult if b3_mult else None
        if eps in {None, 0} or bps in {None, 0}:
            continue

        rows.append(
            {
                "ticker": meta.ticker,
                "market": meta.market,
                "target_month_end": month_end.normalize(),
                "source_month": pd.Timestamp(row["GS_YM"]).normalize(),
                "PER": float(price / eps),
                "PBR": float(price / bps),
                "EPS": float(eps),
            }
        )
    return rows


def _save_market_daily(rows: list[dict], out_dir: Path) -> None:
    if not rows:
        return
    frame = pd.DataFrame(rows)
    out_dir.mkdir(parents=True, exist_ok=True)
    grouped = frame.groupby(
        [frame["시장"], pd.to_datetime(frame["source_trade_date"]).dt.strftime("%Y%m%d")],
        sort=True,
    )
    for (market, trade_date), group in grouped:
        path = out_dir / f"{market}_{trade_date}.parquet"
        ordered = group.sort_values("종목코드").reset_index(drop=True)
        ordered.to_parquet(path, index=False)
        print(f"[SAVE] cap-daily {path} rows={len(ordered)}")


def _save_market_monthly(rows: list[dict], out_dir: Path, prefix: str) -> None:
    if not rows:
        return
    frame = pd.DataFrame(rows)
    out_dir.mkdir(parents=True, exist_ok=True)
    grouped = frame.groupby(
        [frame["market"], pd.to_datetime(frame["target_month_end"]).dt.strftime("%Y%m")],
        sort=True,
    )
    for (market, month), group in grouped:
        path = out_dir / f"{market}_{month}.parquet"
        ordered = group.sort_values("ticker").reset_index(drop=True)
        ordered.to_parquet(path, index=False)
        print(f"[SAVE] {prefix} {path} rows={len(ordered)}")


def collect_cap_daily(
    tickers: list[TickerMeta],
    start: pd.Timestamp,
    end: pd.Timestamp,
    month_ends: list[pd.Timestamp],
    out_root: Path,
) -> None:
    month_end_map = {month_end.to_period("M"): month_end.normalize() for month_end in month_ends}
    rows: list[dict] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=FG_MAX_WORKERS) as executor:
        futures = {
            executor.submit(_extract_daily_cap, meta, start, end, month_end_map): meta.ticker
            for meta in tickers
        }
        for i, future in enumerate(concurrent.futures.as_completed(futures), start=1):
            rows.extend(future.result())
            if i % 250 == 0:
                print(f"[CAP-DAILY] processed {i}/{len(futures)} tickers")

    _save_market_daily(rows, out_root / "market/cap_daily")


def collect_monthly_fundamental(
    tickers: list[TickerMeta],
    month_ends: list[pd.Timestamp],
    out_root: Path,
) -> None:
    rows: list[dict] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=FG_MAX_WORKERS) as executor:
        futures = {
            executor.submit(_extract_monthly_fundamental, meta, month_ends): meta.ticker
            for meta in tickers
        }
        for i, future in enumerate(concurrent.futures.as_completed(futures), start=1):
            rows.extend(future.result())
            if i % 250 == 0:
                print(f"[FUND] processed {i}/{len(futures)} tickers")

    _save_market_monthly(rows, out_root / "market/fundamental", "fund")


def collect_index_ohlcv(start: pd.Timestamp, end: pd.Timestamp, out_root: Path) -> None:
    out_dir = out_root / "index/ohlcv"
    out_dir.mkdir(parents=True, exist_ok=True)

    for name, symbol in INDEX_SYMBOLS.items():
        frame = fdr.DataReader(symbol, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
        if frame.empty:
            print(f"[SKIP] index empty: {name}")
            continue
        out = frame.rename(
            columns={
                "Open": "시가",
                "High": "고가",
                "Low": "저가",
                "Close": "종가",
                "Volume": "거래량",
                "Adj Close": "수정종가",
            }
        )
        out.index.name = "날짜"
        suffix = end.strftime("%Y") if start.year == end.year else f"{start:%Y%m%d}_{end:%Y%m%d}"
        path = out_dir / f"{name}_{suffix}.parquet"
        out.to_parquet(path, index=True)
        print(f"[SAVE] index {path} rows={len(out)}")


def collect_sector(as_of: pd.Timestamp, out_root: Path) -> None:
    out_dir = out_root / "market/sector"
    out_dir.mkdir(parents=True, exist_ok=True)

    for market in ("KOSPI-DESC", "KOSDAQ-DESC"):
        frame = fdr.StockListing(market)
        if frame.empty:
            print(f"[SKIP] sector empty: {market}")
            continue
        frame = frame.rename(
            columns={
                "Code": "종목코드",
                "Name": "종목명",
                "Industry": "업종명",
                "Market": "시장",
            }
        )
        frame["as_of_date"] = as_of.normalize()
        out = frame[["종목코드", "종목명", "시장", "업종명", "Sector", "ListingDate", "as_of_date"]].copy()
        out = out.sort_values("종목코드").reset_index(drop=True)
        market_name = market.replace("-DESC", "")
        path = out_dir / f"{market_name}_{as_of:%Y%m%d}.parquet"
        out.to_parquet(path, index=False)
        print(f"[SAVE] sector {path} rows={len(out)}")


def summarize_outputs(out_root: Path) -> None:
    targets = {
        "cap_daily": out_root / "market/cap_daily",
        "fundamental": out_root / "market/fundamental",
        "index": out_root / "index/ohlcv",
        "sector": out_root / "market/sector",
    }
    print("\n[SUMMARY]")
    for key, path in targets.items():
        files = sorted(path.glob("*.parquet"))
        total_rows = 0
        for file in files:
            total_rows += len(pd.read_parquet(file))
        print(f"- {key}: files={len(files)} rows={total_rows}")
        for file in files[:5]:
            print(f"  * {file} rows={len(pd.read_parquet(file))}")


def missing_cap_daily_files(out_root: Path, month_ends: list[pd.Timestamp]) -> list[Path]:
    out_dir = out_root / "market/cap_daily"
    missing: list[Path] = []
    for month_end in month_ends:
        trade_date = month_end.strftime("%Y%m%d")
        for market in ("KOSPI", "KOSDAQ"):
            path = out_dir / f"{market}_{trade_date}.parquet"
            if not path.exists():
                missing.append(path)
    return missing


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill KR market-cap snapshots and related fallback datasets."
    )
    parser.add_argument("--start", default=DEFAULT_START, help="Start date (YYYY-MM-DD).")
    parser.add_argument("--end", default=DEFAULT_END, help="End date (YYYY-MM-DD).")
    parser.add_argument(
        "--universe-csv",
        default="",
        help=(
            "Optional universe CSV path. Defaults to public full KOSPI/KOSDAQ listings; "
            "pass universes/kr_universe.csv only for pilot runs."
        ),
    )
    parser.add_argument(
        "--output-root",
        default=str(OUTPUT_ROOT),
        help="Base output directory.",
    )
    parser.add_argument(
        "--skip-fundamental",
        action="store_true",
        help="Skip monthly PER/PBR/EPS fallback extraction.",
    )
    parser.add_argument(
        "--skip-index",
        action="store_true",
        help="Skip index OHLCV fallback extraction.",
    )
    parser.add_argument(
        "--skip-sector",
        action="store_true",
        help="Skip sector snapshot extraction.",
    )
    args = parser.parse_args()

    start = _parse_iso_date(args.start)
    end = _parse_iso_date(args.end)
    out_root = Path(args.output_root)
    universe_csv = Path(args.universe_csv) if args.universe_csv else None

    month_ends = _month_end_business_days(start, end)
    print("[INFO] month-end business days:", [d.strftime("%Y-%m-%d") for d in month_ends])

    tickers = _load_tickers(universe_csv)
    print(f"[INFO] tickers loaded: {len(tickers)}")

    collect_cap_daily(tickers, start, end, month_ends, out_root)
    if not args.skip_fundamental:
        collect_monthly_fundamental(tickers, month_ends, out_root)
    if not args.skip_index:
        collect_index_ohlcv(start, end, out_root)
    if not args.skip_sector:
        collect_sector(end, out_root)
    summarize_outputs(out_root)
    missing_cap = missing_cap_daily_files(out_root, month_ends)
    if missing_cap:
        print(
            f"[ERROR] cap_daily coverage incomplete: missing {len(missing_cap)} "
            f"of {len(month_ends) * 2} expected market/month files"
        )
        for path in missing_cap[:10]:
            print(f"  * missing {path}")
        raise SystemExit(2)


if __name__ == "__main__":
    main()
