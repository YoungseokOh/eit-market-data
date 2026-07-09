"""KR universe manifests: listing metadata, market caps, and KOSPI200 membership."""

from __future__ import annotations

import json
import logging
import re
import shutil
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from eit_market_data.kr.market_helpers import fetch_market_cap_frame, normalize_ticker
from eit_market_data.local_collection.constants import (
    CURRENT_KR_UNIVERSE_CSV,
    KOSPI200_INDEX_CODE,
    KOSPI200_OFFCYCLE_CHURN_THRESHOLD,
    KOSPI200_REVIEW_MONTHS,
    KOSPI200_SIZE_TOLERANCE,
    PROJECT_ROOT,
)

logger = logging.getLogger(__name__)


def _listing_metadata_frame() -> Any:
    import pandas as pd

    try:
        import FinanceDataReader as fdr
    except ImportError as exc:
        raise RuntimeError("FinanceDataReader is required to build local universe manifests.") from exc

    def _text_column(frame: Any, name: str) -> Any:
        if name in frame.columns:
            return frame[name].fillna("").astype(str).str.strip()
        return pd.Series([""] * len(frame), index=frame.index, dtype="string")

    frame = fdr.StockListing("KRX-DESC")
    if frame is not None and not frame.empty and "Code" in frame.columns:
        normalized = frame.copy()
        normalized["ticker"] = normalized["Code"].astype(str).map(normalize_ticker)
        normalized["name"] = _text_column(normalized, "Name")
        normalized["market"] = _text_column(normalized, "Market").str.upper()
        industry = _text_column(normalized, "Industry")
        sector = _text_column(normalized, "Sector")
        normalized["sector"] = industry.where(industry != "", sector).fillna("").astype(str)
        normalized = normalized.loc[normalized["market"].isin({"KOSPI", "KOSDAQ"})]
        if not normalized.empty:
            return _merge_seed_listing_metadata(
                normalized[["ticker", "name", "market", "sector"]]
            )

    frames = []
    for market in ("KOSPI", "KOSDAQ"):
        partial = fdr.StockListing(market)
        if partial is None or partial.empty or "Code" not in partial.columns:
            continue
        cloned = partial.copy()
        cloned["ticker"] = cloned["Code"].astype(str).map(normalize_ticker)
        cloned["name"] = _text_column(cloned, "Name")
        cloned["market"] = market
        cloned["sector"] = _text_column(cloned, "Industry")
        frames.append(cloned[["ticker", "name", "market", "sector"]])

    if not frames:
        raise RuntimeError("No KR listing metadata available from FinanceDataReader.")
    return _merge_seed_listing_metadata(pd.concat(frames, ignore_index=True))


def _merge_seed_listing_metadata(frame: Any) -> Any:
    import pandas as pd

    frames = [frame]
    if CURRENT_KR_UNIVERSE_CSV.exists():
        try:
            seed = pd.read_csv(CURRENT_KR_UNIVERSE_CSV, dtype={"ticker": str})
        except Exception:
            seed = None
        if seed is not None and not seed.empty and "ticker" in seed.columns:
            normalized = seed.copy()
            normalized["ticker"] = normalized["ticker"].astype(str).map(normalize_ticker)
            for column in ("name", "market", "sector"):
                if column not in normalized.columns:
                    normalized[column] = ""
            frames.append(normalized[["ticker", "name", "market", "sector"]])

    return pd.concat(frames, ignore_index=True).drop_duplicates("ticker", keep="first")


def _snapshot_market_cap_frame(as_of: date) -> Any | None:
    """Load market-cap candidates from an existing same-month KR snapshot."""
    import pandas as pd

    month = as_of.strftime("%Y-%m")
    candidates = (
        PROJECT_ROOT / "artifacts" / "kr" / "snapshots" / month / "snapshot.json",
        PROJECT_ROOT / "artifacts" / "snapshots" / month / "snapshot.json",
    )
    snapshot_path = next((path for path in candidates if path.exists()), None)
    if snapshot_path is None:
        return None

    try:
        snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    except Exception:
        return None

    rows: list[dict[str, Any]] = []
    fundamentals = snapshot.get("fundamentals", {})
    for ticker, payload in fundamentals.items():
        market_cap = payload.get("market_cap") if isinstance(payload, dict) else None
        if market_cap is None:
            continue
        rows.append(
            {
                "ticker": normalize_ticker(str(ticker)),
                "market_cap": market_cap,
            }
        )

    if not rows:
        return None
    return pd.DataFrame(rows)


def _market_cap_candidates_for_market(as_of: date, market: str) -> Any | None:
    import pandas as pd

    frame = fetch_market_cap_frame(as_of, market)
    if frame is None or frame.empty:
        return None
    working = frame.reset_index() if "종목코드" not in frame.columns else frame.reset_index(drop=True)
    if "종목코드" not in working.columns or "시가총액" not in working.columns:
        return None
    result = pd.DataFrame(
        {
            "ticker": working["종목코드"].astype(str).map(normalize_ticker),
            "market_cap": pd.to_numeric(working["시가총액"], errors="coerce"),
        }
    )
    if "종목명" in working.columns:
        result["cap_name"] = working["종목명"].fillna("").astype(str).str.strip()
    return result


def _fetch_kospi200_tickers_from_pykrx(as_of: date) -> list[str]:
    try:
        from eit_market_data.kr.pykrx_loader import load_pykrx_stock
    except Exception as exc:
        logger.warning("KOSPI200 pykrx source unavailable: %s", exc)
        return []

    try:
        stock = load_pykrx_stock()
        tickers = stock.get_index_portfolio_deposit_file(
            KOSPI200_INDEX_CODE,
            as_of.strftime("%Y%m%d"),
            alternative=True,
        )
    except Exception as exc:
        logger.warning("KOSPI200 pykrx fetch failed for %s: %s", as_of, exc)
        return []

    if tickers is None:
        return []
    return [normalize_ticker(ticker) for ticker in tickers if str(ticker).strip()]


def _fetch_kospi200_rows_from_naver_current() -> list[dict[str, str]]:
    import requests

    rows: list[dict[str, str]] = []
    headers = {"User-Agent": "Mozilla/5.0"}
    for page in range(1, 21):
        url = f"https://finance.naver.com/sise/entryJongmok.naver?&page={page}"
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        response.encoding = "euc-kr"
        matches = re.findall(
            r'<td class="ctg"><a href="/item/main\.naver\?code=([0-9A-Za-z]{6})"[^>]*>(.*?)</a></td>',
            response.text,
        )
        for ticker, name in matches:
            rows.append(
                {
                    "ticker": normalize_ticker(ticker),
                    "name": re.sub(r"<[^>]+>", "", name).strip(),
                }
            )
    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for row in rows:
        ticker = row["ticker"]
        if ticker in seen:
            continue
        seen.add(ticker)
        deduped.append(row)
    return deduped


def _is_current_or_future_month(as_of: date, *, today: date | None = None) -> bool:
    """True when ``as_of`` falls in the current calendar month (or later).

    Only a current-month ``as_of`` may use a ``date.today()``-stamped live source
    (Naver current membership). For a historical month that snapshot would stamp
    present-day membership onto the past — a look-ahead defect — so it is banned.
    """
    today = today or date.today()
    return (as_of.year, as_of.month) >= (today.year, today.month)


def _kospi200_membership_churn(
    previous: set[str], current: set[str]
) -> tuple[int, set[str], set[str]]:
    """Return ``(churn, entrants, leavers)`` for two membership sets.

    ``churn`` is ``max(len(entrants), len(leavers))`` — the count of names that
    changed in the larger direction. A clean swap of N names yields churn N.
    """
    entrants = current - previous
    leavers = previous - current
    return max(len(entrants), len(leavers)), entrants, leavers


def _carry_forward_rows(previous_members: list[dict[str, str]]) -> list[dict[str, str]]:
    return [
        {"ticker": normalize_ticker(row["ticker"]), "source_name": row.get("name", "")}
        for row in previous_members
        if str(row.get("ticker", "")).strip()
    ]


def _build_kospi200_records(
    as_of: date,
    meta: Any,
    *,
    previous_members: list[dict[str, str]] | None = None,
) -> Any:
    """Build the KOSPI200 membership records for ``as_of``.

    PIT-safe membership resolution:

    * The raw pykrx deposit-file list is the primary source.
    * Off-cycle guard: KOSPI200 only reconstitutes in June/December. In any other
      month, if the pykrx list churns more than ``KOSPI200_OFFCYCLE_CHURN_THRESHOLD``
      names versus the previous month's persisted membership, the list is treated
      as suspect (transient pykrx response or a stale current-membership snapshot)
      and the previous month's membership is carried forward.
    * Fallbacks when pykrx does not return a clean 200-name list: carry the
      previous month forward when available; otherwise use the Naver *current*
      membership ONLY for a current/future ``as_of`` (never for a historical
      month, which would stamp today's membership onto the past).
    """
    import pandas as pd

    previous_rows = _carry_forward_rows(previous_members or [])
    previous_set = {row["ticker"] for row in previous_rows}

    source = "krx_pykrx"
    source_as_of = as_of.isoformat()
    pykrx_tickers = _fetch_kospi200_tickers_from_pykrx(as_of)
    rows = [{"ticker": ticker, "source_name": ""} for ticker in pykrx_tickers]

    pykrx_ok = abs(len(rows) - 200) <= KOSPI200_SIZE_TOLERANCE

    # Off-cycle churn guard: only applies when we have a clean pykrx list AND a
    # previous month to compare against. June/December reviews are never blocked.
    if (
        pykrx_ok
        and previous_set
        and as_of.month not in KOSPI200_REVIEW_MONTHS
    ):
        current_set = {row["ticker"] for row in rows}
        churn, entrants, leavers = _kospi200_membership_churn(previous_set, current_set)
        if churn > KOSPI200_OFFCYCLE_CHURN_THRESHOLD:
            logger.warning(
                "KOSPI200 off-cycle churn for %s is %d names (>%d) in a non-review "
                "month (entrants=%d, leavers=%d); carrying forward previous membership.",
                as_of,
                churn,
                KOSPI200_OFFCYCLE_CHURN_THRESHOLD,
                len(entrants),
                len(leavers),
            )
            rows = list(previous_rows)
            source = "carry_forward_offcycle"
            source_as_of = as_of.isoformat()
            pykrx_ok = True

    if not pykrx_ok:
        if previous_rows:
            logger.warning(
                "KOSPI200 pykrx source returned %d rows for %s; carrying forward "
                "previous month's membership (%d names).",
                len(rows),
                as_of,
                len(previous_rows),
            )
            rows = list(previous_rows)
            source = "carry_forward_prev_month"
            source_as_of = as_of.isoformat()
        elif _is_current_or_future_month(as_of):
            logger.warning(
                "KOSPI200 official pykrx source returned %d rows for %s; using Naver "
                "current fallback (current month).",
                len(rows),
                as_of,
            )
            source = "naver_current_fallback"
            source_as_of = date.today().isoformat()
            rows = [
                {"ticker": row["ticker"], "source_name": row.get("name", "")}
                for row in _fetch_kospi200_rows_from_naver_current()
            ]
        else:
            raise RuntimeError(
                f"KOSPI200 universe source returned {len(rows)} rows for historical "
                f"{as_of}; refusing Naver current fallback (look-ahead) and no previous "
                "membership available to carry forward."
            )

    if abs(len(rows) - 200) > KOSPI200_SIZE_TOLERANCE:
        raise RuntimeError(
            f"KOSPI200 universe source returned {len(rows)} rows; expected "
            f"200 +/- {KOSPI200_SIZE_TOLERANCE}."
        )

    records = pd.DataFrame(rows)
    cap_frame = _market_cap_candidates_for_market(as_of, "KOSPI")
    if cap_frame is not None and not cap_frame.empty:
        records = records.merge(cap_frame, on="ticker", how="left")
    else:
        records["market_cap"] = None

    records = records.merge(meta, on="ticker", how="left")
    records["market"] = records["market"].fillna("KOSPI")
    records["sector"] = records["sector"].fillna("")
    records["name"] = records["name"].fillna("")
    if "source_name" in records.columns:
        records["name"] = records["name"].where(records["name"] != "", records["source_name"])
    if "cap_name" in records.columns:
        records["name"] = records["name"].where(records["name"] != "", records["cap_name"].fillna(""))
    records["rank"] = range(1, len(records) + 1)
    records["source"] = source
    records["source_as_of"] = source_as_of
    return records


def _load_kospi200_members_from_csv(path: Path) -> list[dict[str, str]]:
    """Load persisted KOSPI200 membership rows (ticker + name) from a CSV."""
    import pandas as pd

    try:
        frame = pd.read_csv(path, dtype={"ticker": str})
    except Exception as exc:
        logger.warning("Failed to read previous KOSPI200 universe %s: %s", path, exc)
        return []
    if frame is None or frame.empty or "ticker" not in frame.columns:
        return []
    name_col = "name" if "name" in frame.columns else None
    rows: list[dict[str, str]] = []
    for _, row in frame.iterrows():
        ticker = str(row["ticker"]).strip()
        if not ticker:
            continue
        rows.append(
            {
                "ticker": normalize_ticker(ticker),
                "name": str(row[name_col]).strip() if name_col else "",
            }
        )
    return rows


def find_previous_kospi200_members(
    *,
    storage_root: Path,
    as_of: date,
    market: str,
    phase: str,
    kind: str,
) -> list[dict[str, str]]:
    """Locate the most recent prior-month persisted KOSPI200 membership.

    Scans ``storage_root/runs/<run-date>/<label>/universes/kr/<kind>/<prev-month>.csv``
    for the calendar month immediately preceding ``as_of``. PIT-safe: only run
    directories dated strictly before ``as_of`` are considered, so no future
    membership can leak in. Returns ``[]`` when no prior month is available.
    """
    if kind.lower() != "kospi200":
        return []
    prev_anchor = as_of.replace(day=1) - timedelta(days=1)
    prev_month = prev_anchor.strftime("%Y-%m")
    label = f"{market}_{phase}_{kind}"
    runs_root = storage_root / "runs"
    if not runs_root.exists():
        return []

    candidates: list[tuple[str, Path]] = []
    for run_dir in runs_root.iterdir():
        if not run_dir.is_dir() or run_dir.name >= as_of.strftime("%Y-%m-%d"):
            continue  # never look ahead: skip runs dated on/after as_of
        csv_path = run_dir / label / "universes" / "kr" / kind / f"{prev_month}.csv"
        if csv_path.exists():
            candidates.append((run_dir.name, csv_path))
    if not candidates:
        return []
    # Most recent run-date wins (latest correction of the prior month's universe).
    _, best = max(candidates, key=lambda item: item[0])
    members = _load_kospi200_members_from_csv(best)
    if members:
        logger.info(
            "KOSPI200 previous-month membership for %s loaded from %s (%d names).",
            as_of,
            best,
            len(members),
        )
    return members


def build_local_universe_manifest(
    *,
    as_of: date,
    kind: str,
    output_path: Path,
    previous_members: list[dict[str, str]] | None = None,
) -> Path:
    import pandas as pd

    kind = kind.lower()
    meta = _listing_metadata_frame()

    if kind == "kospi200":
        records = _build_kospi200_records(as_of, meta, previous_members=previous_members)
    elif kind != "full":
        if not kind.startswith("top"):
            raise ValueError(f"Unsupported universe kind: {kind}")
        top_n = int(kind.removeprefix("top"))
        cap_frames = []
        for market in ("KOSPI", "KOSDAQ"):
            frame = _market_cap_candidates_for_market(as_of, market)
            if frame is not None and not frame.empty:
                cap_frames.append(frame[["ticker", "market_cap"]])
        if not cap_frames:
            raise RuntimeError(f"Market-cap data unavailable for {kind} universe generation.")
        snapshot_cap_frame = _snapshot_market_cap_frame(as_of)
        if snapshot_cap_frame is not None and not snapshot_cap_frame.empty:
            cap_frames.append(snapshot_cap_frame)
        cap_frame = (
            pd.concat(cap_frames, ignore_index=True)
            .dropna(subset=["market_cap"])
            .sort_values("market_cap", ascending=False)
            .drop_duplicates("ticker")
            .head(top_n)
        )
        records = cap_frame.merge(meta, on="ticker", how="left")
        records["rank"] = range(1, len(records) + 1)
    else:
        records = meta.sort_values(["market", "ticker"]).reset_index(drop=True)
        records["market_cap"] = None
        records["rank"] = None

    output_path.parent.mkdir(parents=True, exist_ok=True)
    records["as_of"] = as_of.isoformat()
    columns = ["ticker", "market", "sector", "name", "market_cap", "rank", "as_of"]
    if kind == "kospi200":
        columns.extend(["source", "source_as_of"])
    ordered = records[columns]
    ordered.to_csv(output_path, index=False, encoding="utf-8")
    return output_path


def copy_pilot_universe(output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(CURRENT_KR_UNIVERSE_CSV, output_path)
    return output_path
