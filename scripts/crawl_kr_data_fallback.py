from __future__ import annotations

import concurrent.futures
import datetime as dt
import json
import re
from dataclasses import dataclass
from pathlib import Path

import FinanceDataReader as fdr
import pandas as pd
import requests
from pykrx import stock

START = "20240101"
END = "20241231"
PROJECT_ROOT = Path("/home/seok436/projects/eit-market-data")
OUTPUT_ROOT = PROJECT_ROOT / "data"

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


def _month_end_business_days() -> list[pd.Timestamp]:
    idx = fdr.DataReader("YAHOO:^KS11", "2024-01-01", "2024-12-31")
    idx = idx.sort_index()
    return idx.groupby(idx.index.to_period("M")).tail(1).index.to_list()


@dataclass
class TickerMeta:
    ticker: str
    market: str


def _load_tickers() -> list[TickerMeta]:
    kospi = stock.get_market_ticker_list(END, market="KOSPI")
    kosdaq = stock.get_market_ticker_list(END, market="KOSDAQ")
    out = [TickerMeta(t, "KOSPI") for t in kospi] + [TickerMeta(t, "KOSDAQ") for t in kosdaq]
    out.sort(key=lambda x: x.ticker)
    return out


def _extract_monthly_cap(meta: TickerMeta, month_ends: list[pd.Timestamp]) -> list[dict]:
    obj = _fnguide_get(f"01_01/chart_A{meta.ticker}_3Y.json")
    if not obj:
        return []
    chart = obj.get("CHART", [])
    if not isinstance(chart, list) or not chart:
        return []

    frame = pd.DataFrame(chart)
    if "TRD_DT" not in frame.columns or "MKT_CAP" not in frame.columns:
        return []

    frame["TRD_DT"] = pd.to_datetime(frame["TRD_DT"], errors="coerce")
    frame = frame.dropna(subset=["TRD_DT"]).sort_values("TRD_DT")
    frame["MKT_CAP"] = frame["MKT_CAP"].map(_to_num)
    if "J_PRC" in frame.columns:
        frame["J_PRC"] = frame["J_PRC"].map(_to_num)

    rows: list[dict] = []
    for month_end in month_ends:
        sub = frame[frame["TRD_DT"] <= month_end]
        if sub.empty:
            continue
        row = sub.iloc[-1]
        cap_eok = row.get("MKT_CAP")
        if cap_eok is None:
            continue
        close = row.get("J_PRC")
        rows.append(
            {
                "ticker": meta.ticker,
                "market": meta.market,
                "target_month_end": month_end.normalize(),
                "source_trade_date": pd.Timestamp(row["TRD_DT"]).normalize(),
                "종가": None if close is None else int(round(close)),
                "시가총액": int(round(cap_eok * 100_000_000)),
            }
        )
    return rows


def _extract_x_multiplier(name: str) -> float | None:
    m = re.search(r"([0-9]+(?:\\.[0-9]+)?)X", str(name))
    if not m:
        return None
    try:
        return float(m.group(1))
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

        per = price / eps
        pbr = price / bps

        rows.append(
            {
                "ticker": meta.ticker,
                "market": meta.market,
                "target_month_end": month_end.normalize(),
                "source_month": pd.Timestamp(row["GS_YM"]).normalize(),
                "PER": float(per),
                "PBR": float(pbr),
                "EPS": float(eps),
            }
        )
    return rows


def _save_market_monthly(rows: list[dict], out_dir: Path, prefix: str) -> None:
    if not rows:
        return
    df = pd.DataFrame(rows)
    out_dir.mkdir(parents=True, exist_ok=True)
    for (market, month), g in df.groupby(
        [df["market"], pd.to_datetime(df["target_month_end"]).dt.strftime("%Y%m")],
        sort=True,
    ):
        path = out_dir / f"{market}_{month}.parquet"
        g = g.sort_values("ticker").reset_index(drop=True)
        g.to_parquet(path, index=False)
        print(f"[SAVE] {prefix} {path} rows={len(g)}")


def collect_cap_and_fundamental(tickers: list[TickerMeta], month_ends: list[pd.Timestamp]) -> None:
    cap_rows: list[dict] = []
    fund_rows: list[dict] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=FG_MAX_WORKERS) as executor:
        cap_futs = {executor.submit(_extract_monthly_cap, t, month_ends): t.ticker for t in tickers}
        for i, fut in enumerate(concurrent.futures.as_completed(cap_futs), start=1):
            cap_rows.extend(fut.result())
            if i % 250 == 0:
                print(f"[CAP] processed {i}/{len(cap_futs)} tickers")

    with concurrent.futures.ThreadPoolExecutor(max_workers=FG_MAX_WORKERS) as executor:
        fund_futs = {
            executor.submit(_extract_monthly_fundamental, t, month_ends): t.ticker for t in tickers
        }
        for i, fut in enumerate(concurrent.futures.as_completed(fund_futs), start=1):
            fund_rows.extend(fut.result())
            if i % 250 == 0:
                print(f"[FUND] processed {i}/{len(fund_futs)} tickers")

    _save_market_monthly(cap_rows, OUTPUT_ROOT / "market/cap", "cap")
    _save_market_monthly(fund_rows, OUTPUT_ROOT / "market/fundamental", "fund")


def collect_index_ohlcv() -> None:
    out_dir = OUTPUT_ROOT / "index/ohlcv"
    out_dir.mkdir(parents=True, exist_ok=True)

    for name, symbol in INDEX_SYMBOLS.items():
        df = fdr.DataReader(symbol, "2024-01-01", "2024-12-31")
        if df.empty:
            print(f"[SKIP] index empty: {name}")
            continue
        out = df.rename(
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
        path = out_dir / f"{name}_2024.parquet"
        out.to_parquet(path, index=True)
        print(f"[SAVE] index {path} rows={len(out)}")


def collect_sector() -> None:
    out_dir = OUTPUT_ROOT / "market/sector"
    out_dir.mkdir(parents=True, exist_ok=True)

    as_of = pd.Timestamp("2024-12-31")
    for market in ("KOSPI-DESC", "KOSDAQ-DESC"):
        df = fdr.StockListing(market)
        if df.empty:
            print(f"[SKIP] sector empty: {market}")
            continue
        df = df.rename(columns={"Code": "종목코드", "Name": "종목명", "Sector": "업종명", "Market": "시장"})
        df["as_of_date"] = as_of
        out = df[["종목코드", "종목명", "시장", "업종명", "Industry", "ListingDate", "as_of_date"]].copy()
        out = out.sort_values("종목코드").reset_index(drop=True)
        market_name = market.replace("-DESC", "")
        path = out_dir / f"{market_name}_20241231.parquet"
        out.to_parquet(path, index=False)
        print(f"[SAVE] sector {path} rows={len(out)}")


def summarize_outputs() -> None:
    targets = {
        "cap": OUTPUT_ROOT / "market/cap",
        "fundamental": OUTPUT_ROOT / "market/fundamental",
        "index": OUTPUT_ROOT / "index/ohlcv",
        "sector": OUTPUT_ROOT / "market/sector",
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


def main() -> None:
    month_ends = _month_end_business_days()
    print("[INFO] month-end business days:", [d.strftime("%Y-%m-%d") for d in month_ends])

    tickers = _load_tickers()
    print(f"[INFO] tickers loaded: {len(tickers)} (KOSPI+KOSDAQ)")

    collect_cap_and_fundamental(tickers, month_ends)
    collect_index_ohlcv()
    collect_sector()
    summarize_outputs()


if __name__ == "__main__":
    main()
