"""Local-only collection orchestration with checkpoint validation."""

from __future__ import annotations

import asyncio
import csv
import gzip
import json
import logging
import re
import shutil
import subprocess
import sys
import time
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from typing import Any

from eit_market_data.core.hashing import _content_hash
from eit_market_data.core.pit import is_visible
from eit_market_data.core.sector_math import compute_sector_averages
from eit_market_data.kr.ci_safe_provider import NullMacroProvider
from eit_market_data.kr.dart_provider import (
    DartProvider,
    _DART_CACHE_DIR,
    _DART_CACHE_SIZE_LIMIT,
)
from eit_market_data.kr.ecos_provider import EcosMacroProvider
from eit_market_data.kr.fundamental_provider import CompositeKrFundamentalProvider
from eit_market_data.kr.market_helpers import fetch_market_cap_frame, normalize_ticker
from eit_market_data.kr.news_catalog import (
    KrNewsWindowCoverage,
)
from eit_market_data.kr.naver_news_provider import (
    NaverArchiveNewsRecord,
)
from eit_market_data.kr.pykrx_provider import PykrxProvider
from eit_market_data.schemas.snapshot import (
    FilingData,
    FundamentalData,
    MacroData,
    MonthlySnapshot,
    NewsItem,
    PriceBar,
    SectorAverages,
    SnapshotMetadata,
)
from eit_market_data.snapshot import (
    SnapshotConfig,
    config_hash,
    serialize_snapshot,
    serialize_snapshot_metadata,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_US_UNIVERSE = "AAPL,MSFT,GOOGL,AMZN,NVDA"
CURRENT_KR_UNIVERSE_CSV = PROJECT_ROOT / "universes" / "kr_universe.csv"
NEWS_LOOKBACK_DAYS = 30
KOSPI200_INDEX_CODE = "1028"

logger = logging.getLogger(__name__)


def _next_weekday(value: date) -> date:
    """Return the next weekday after ``value``."""
    candidate = value + timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate += timedelta(days=1)
    return candidate


def _krx_business_days_between(start: date, end: date) -> list[date]:
    """Return KRX business days for a bounded range when pykrx can provide them."""
    try:
        from eit_market_data.kr.krx_auth import (
            ensure_krx_authenticated_session,
            install_pykrx_krx_session_hooks,
        )
        from eit_market_data.kr.pykrx_loader import load_pykrx_stock

        stock = load_pykrx_stock()
        install_pykrx_krx_session_hooks()
        ensure_krx_authenticated_session(interactive=False)
        with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
            days = stock.get_previous_business_days(
                fromdate=start.strftime("%Y%m%d"),
                todate=end.strftime("%Y%m%d"),
            )
    except Exception as exc:
        logger.warning("KRX business-day lookup failed for %s..%s: %s", start, end, exc)
        return []

    parsed: list[date] = []
    for item in days:
        day = item.date() if hasattr(item, "date") else item
        if isinstance(day, date):
            parsed.append(day)
    return sorted(day for day in parsed if start <= day <= end)


def _next_kr_execution_date(as_of: date) -> date:
    """Return the next known KRX trading day, falling back to next weekday.

    pykrx exposes historical/known business days, but it may not return future
    exchange holidays. For current-day partial bundles, use KRX if available and
    otherwise avoid the old month-end placeholder by falling back to next weekday.
    """
    start = as_of + timedelta(days=1)
    end = as_of + timedelta(days=14)
    days = _krx_business_days_between(start, end)
    return days[0] if days else _next_weekday(as_of)


class ValidationError(RuntimeError):
    """Raised when a checkpoint validation fails."""


@dataclass
class CheckpointPolicy:
    every_tickers: int
    every_seconds: int


@dataclass
class ValidationCheck:
    name: str
    status: str
    detail: str
    metrics: dict[str, Any] = field(default_factory=dict)


@dataclass
class BatchPayload:
    tickers: list[str]
    prices: dict[str, list[PriceBar]]
    fundamentals: dict[str, FundamentalData]
    filings: dict[str, FilingData]
    news: dict[str, list[NewsItem]]
    news_audit: dict[str, list[NaverArchiveNewsRecord]]
    news_coverage: dict[str, KrNewsWindowCoverage]

    def merge(self, payload: "BatchPayload") -> None:
        self.tickers.extend(payload.tickers)
        self.prices.update(payload.prices)
        self.fundamentals.update(payload.fundamentals)
        self.filings.update(payload.filings)
        self.news.update(payload.news)
        self.news_audit.update(payload.news_audit)
        self.news_coverage.update(payload.news_coverage)


@dataclass
class KrCollectionState:
    prices: dict[str, list[PriceBar]] = field(default_factory=dict)
    fundamentals: dict[str, FundamentalData] = field(default_factory=dict)
    filings: dict[str, FilingData] = field(default_factory=dict)
    news: dict[str, list[NewsItem]] = field(default_factory=dict)
    news_audit: dict[str, list[NaverArchiveNewsRecord]] = field(default_factory=dict)
    news_coverage: dict[str, KrNewsWindowCoverage] = field(default_factory=dict)

    def merge(self, payload: BatchPayload) -> None:
        self.prices.update(payload.prices)
        self.fundamentals.update(payload.fundamentals)
        self.filings.update(payload.filings)
        self.news.update(payload.news)
        self.news_audit.update(payload.news_audit)
        self.news_coverage.update(payload.news_coverage)


class CacheOnlyDartProvider:
    """DART provider backed only by the local disk cache.

    Use after a live OpenDART timeout/rate-limit signal so collection can
    continue without making more OpenDART network requests.
    """

    def __init__(self, cache_dir: Path = _DART_CACHE_DIR) -> None:
        try:
            import diskcache

            self._cache: Any = diskcache.Cache(
                str(cache_dir),
                size_limit=_DART_CACHE_SIZE_LIMIT,
            )
        except ImportError:
            self._cache = None

    def _lookup(self, prefix: str, ticker: str, as_of: date) -> Any:
        """Coarse bucket-month pre-filter.

        Returns the record stored under the collection bucket whose month is
        ``<= as_of``. This is only a pre-filter on the *collection* bucket; the
        record's own real date (``filing_date`` / ``report_date``) must still be
        validated against ``as_of`` by the caller before it is trusted, because
        a bucket can legitimately hold a record whose real date is in the future
        relative to ``as_of`` (e.g. an annual report physically filed months
        after the collection month it was first seen in).
        """
        if self._cache is None:
            return None
        target_ym = as_of.strftime("%Y%m")
        exact = self._cache.get(f"{prefix}:{ticker}:{target_ym}")
        if exact is not None:
            return exact

        latest_key = None
        for key in self._cache.iterkeys():
            text = str(key)
            key_prefix = f"{prefix}:{ticker}:"
            if not text.startswith(key_prefix):
                continue
            ym = text.rsplit(":", 1)[-1]
            if ym <= target_ym and (latest_key is None or ym > latest_key.rsplit(":", 1)[-1]):
                latest_key = text
        return self._cache.get(latest_key) if latest_key is not None else None

    def _iter_cached(self, prefix: str, ticker: str) -> Any:
        """Yield every cached record for ``prefix:ticker:*`` (any bucket month)."""
        if self._cache is None:
            return
        key_prefix = f"{prefix}:{ticker}:"
        for key in self._cache.iterkeys():
            text = str(key)
            if not text.startswith(key_prefix):
                continue
            value = self._cache.get(text)
            if value is not None:
                yield value

    async def fetch_fundamentals(
        self,
        ticker: str,
        as_of: date,
        n_quarters: int = 8,
    ) -> FundamentalData:
        _ = n_quarters
        norm_ticker = normalize_ticker(ticker)
        cached = self._lookup("fundamental", norm_ticker, as_of)
        if isinstance(cached, FundamentalData):
            # Point-in-time guard: a cached bucket may carry quarters whose
            # report_date is after as_of (future annual/interim reports). Drop
            # them so the snapshot never leaks look-ahead information.
            valid_quarters = [q for q in cached.quarters if q.report_date <= as_of]
            if len(valid_quarters) != len(cached.quarters):
                return cached.model_copy(update={"quarters": valid_quarters})
            return cached
        return FundamentalData(ticker=norm_ticker)

    async def fetch_filing(self, ticker: str, as_of: date) -> FilingData:
        norm_ticker = normalize_ticker(ticker)
        # Point-in-time guard: never return a filing whose filing_date is unknown
        # or after as_of. Scan all cached buckets and pick the most recent filing
        # that was actually filed on or before as_of.
        best: FilingData | None = None
        for cached in self._iter_cached("filing", norm_ticker):
            if not isinstance(cached, FilingData):
                continue
            filing_date = cached.filing_date
            if not is_visible(filing_date, as_of):
                continue
            if best is None or best.filing_date is None or filing_date > best.filing_date:
                best = cached
        if best is not None:
            return best
        return FilingData(ticker=norm_ticker)

    async def fetch_issued_shares(self, ticker: str, as_of: date) -> float | None:
        cached = self._lookup("issued_shares", normalize_ticker(ticker), as_of)
        return float(cached) if isinstance(cached, (int, float)) and cached > 0 else None


def _is_dart_transient_error(exc: Exception) -> bool:
    text = f"{exc.__class__.__name__}: {exc}"
    markers = (
        "ConnectTimeout",
        "ReadTimeout",
        "Connection reset",
        "RemoteDisconnected",
        "Max retries exceeded",
        "HTTP 000",
        "temporarily unavailable",
    )
    return any(marker in text for marker in markers)


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _news_window_start(as_of: date, lookback_days: int = NEWS_LOOKBACK_DAYS) -> date:
    return as_of - timedelta(days=max(lookback_days - 1, 0))


def default_raw_start(as_of: date) -> date:
    return date(as_of.year, 1, 1)


def load_ticker_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return [
            {key: str(value or "").strip() for key, value in row.items()}
            for row in csv.DictReader(handle)
            if str(row.get("ticker", "")).strip()
        ]


def load_tickers(path: Path) -> list[str]:
    return [normalize_ticker(row["ticker"]) for row in load_ticker_rows(path)]


def build_run_root(
    storage_root: Path,
    as_of: date,
    market: str,
    phase: str,
    full_universe_kind: str,
) -> Path:
    label = f"{market}_{phase}_{full_universe_kind}"
    return storage_root / "runs" / as_of.strftime("%Y-%m-%d") / label


def load_progress(progress_path: Path, seed: dict[str, Any]) -> dict[str, Any]:
    if progress_path.exists():
        return _read_json(progress_path)
    return seed


def save_progress(progress_path: Path, payload: dict[str, Any]) -> None:
    payload["updated_at"] = _now_utc()
    _write_json(progress_path, payload)


def summarize_checks(checks: list[ValidationCheck]) -> dict[str, Any]:
    failed = [check for check in checks if check.status == "failed"]
    degraded = [check for check in checks if check.status == "degraded"]
    return {
        "failed": len(failed),
        "degraded": len(degraded),
        "checks": [asdict(check) for check in checks],
    }


def raise_for_failed_checks(stage: str, checks: list[ValidationCheck], report_path: Path) -> None:
    failed = [check for check in checks if check.status == "failed"]
    if failed:
        raise ValidationError(
            f"{stage} validation failed; see {report_path}: "
            + ", ".join(f"{check.name}={check.detail}" for check in failed)
        )


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


def _build_kospi200_records(as_of: date, meta: Any) -> Any:
    import pandas as pd

    source = "krx_pykrx"
    source_as_of = as_of.isoformat()
    rows = [{"ticker": ticker, "source_name": ""} for ticker in _fetch_kospi200_tickers_from_pykrx(as_of)]

    if len(rows) != 200:
        logger.warning(
            "KOSPI200 official pykrx source returned %d rows for %s; using Naver current fallback.",
            len(rows),
            as_of,
        )
        source = "naver_current_fallback"
        source_as_of = date.today().isoformat()
        rows = [
            {"ticker": row["ticker"], "source_name": row.get("name", "")}
            for row in _fetch_kospi200_rows_from_naver_current()
        ]

    if len(rows) != 200:
        raise RuntimeError(f"KOSPI200 universe source returned {len(rows)} rows; expected 200.")

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


def build_local_universe_manifest(
    *,
    as_of: date,
    kind: str,
    output_path: Path,
) -> Path:
    import pandas as pd

    kind = kind.lower()
    meta = _listing_metadata_frame()

    if kind == "kospi200":
        records = _build_kospi200_records(as_of, meta)
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


def validate_kr_raw_outputs(raw_root: Path) -> list[ValidationCheck]:
    import pandas as pd

    checks: list[ValidationCheck] = []
    expected = {
        "cap_daily": "market/cap_daily/*.parquet",
        "fundamental": "market/fundamental/*.parquet",
        "index": "index/ohlcv/*.parquet",
        "sector": "market/sector/*.parquet",
    }
    for label, pattern in expected.items():
        files = sorted(raw_root.glob(pattern))
        if not files:
            checks.append(ValidationCheck(label, "failed", "no parquet files"))
            continue
        sample = files[-1]
        try:
            frame = pd.read_parquet(sample)
        except Exception as exc:
            checks.append(ValidationCheck(label, "failed", f"read_failed={exc}"))
            continue
        if frame is None or frame.empty:
            checks.append(ValidationCheck(label, "failed", f"empty_sample={sample.name}"))
            continue
        checks.append(
            ValidationCheck(
                label,
                "ok",
                f"files={len(files)} sample={sample.name}",
                metrics={"files": len(files), "sample_rows": len(frame)},
            )
        )
    return checks


def validate_us_outputs(bundle_root: Path, as_of: date) -> list[ValidationCheck]:
    checks: list[ValidationCheck] = []
    month_dir = bundle_root / "snapshots" / as_of.strftime("%Y-%m")
    expected = {
        "snapshot": month_dir / "snapshot.json",
        "metadata": month_dir / "metadata.json",
        "manifest": month_dir / "manifest.json",
        "summary": month_dir / "summary.json",
    }
    for label, path in expected.items():
        if not path.exists():
            checks.append(ValidationCheck(f"us:{label}", "failed", "missing"))
        elif path.stat().st_size == 0:
            checks.append(ValidationCheck(f"us:{label}", "failed", "empty"))
        else:
            checks.append(ValidationCheck(f"us:{label}", "ok", path.name))
    summary_path = expected["summary"]
    if summary_path.exists():
        try:
            summary = _read_json(summary_path)
        except Exception as exc:
            checks.append(ValidationCheck("us:summary_parse", "failed", str(exc)))
        else:
            status = str(summary.get("status", ""))
            if status != "ok":
                checks.append(ValidationCheck("us:summary_status", "failed", status or "missing"))
            else:
                checks.append(
                    ValidationCheck(
                        "us:summary_status",
                        "ok",
                        "ok",
                        metrics={"tickers": len(summary.get("universe", []))},
                    )
                )
    return checks


def _is_sorted_dates(items: list[date]) -> bool:
    if len(items) < 2:
        return True
    descending = all(items[idx] >= items[idx + 1] for idx in range(len(items) - 1))
    ascending = all(items[idx] <= items[idx + 1] for idx in range(len(items) - 1))
    return ascending or descending


def compute_sector_averages_from_state(
    sector_map: dict[str, str],
    fundamentals: dict[str, FundamentalData],
) -> dict[str, SectorAverages]:
    grouped: dict[str, list[FundamentalData]] = {}
    for ticker, sector in sector_map.items():
        grouped.setdefault(sector or "General", []).append(
            fundamentals.get(ticker, FundamentalData(ticker=ticker))
        )

    return {
        sector: compute_sector_averages(sector, funds)
        for sector, funds in grouped.items()
    }


def _serialize_batch(payload: BatchPayload) -> dict[str, Any]:
    serialized = {
        "tickers": payload.tickers,
        "prices": {
            ticker: [item.model_dump(mode="json") for item in items]
            for ticker, items in payload.prices.items()
        },
        "fundamentals": {
            ticker: item.model_dump(mode="json")
            for ticker, item in payload.fundamentals.items()
        },
        "filings": {
            ticker: item.model_dump(mode="json")
            for ticker, item in payload.filings.items()
        },
    }
    if payload.news:
        serialized["news"] = {
            ticker: [item.model_dump(mode="json") for item in items]
            for ticker, items in payload.news.items()
        }
    if payload.news_audit:
        serialized["news_audit"] = {
            ticker: [asdict(item) for item in items]
            for ticker, items in payload.news_audit.items()
        }
    if payload.news_coverage:
        serialized["news_coverage"] = {
            ticker: asdict(item)
            for ticker, item in payload.news_coverage.items()
        }
    return serialized


def _load_batch_payload(path: Path) -> BatchPayload:
    payload = _read_json(path)
    return BatchPayload(
        tickers=list(payload.get("tickers", [])),
        prices={
            ticker: [PriceBar.model_validate(item) for item in items]
            for ticker, items in payload.get("prices", {}).items()
        },
        fundamentals={
            ticker: FundamentalData.model_validate(item)
            for ticker, item in payload.get("fundamentals", {}).items()
        },
        filings={
            ticker: FilingData.model_validate(item)
            for ticker, item in payload.get("filings", {}).items()
        },
        news={
            ticker: [NewsItem.model_validate(item) for item in items]
            for ticker, items in payload.get("news", {}).items()
        },
        news_audit={
            ticker: [
                NaverArchiveNewsRecord(
                    date=date.fromisoformat(item["date"]),
                    published_at=(
                        datetime.fromisoformat(item["published_at"])
                        if item.get("published_at")
                        else None
                    ),
                    headline=str(item["headline"]),
                    url=str(item["url"]),
                    source=str(item.get("source", "Naver")),
                )
                for item in items
            ]
            for ticker, items in payload.get("news_audit", {}).items()
        },
        news_coverage={
            ticker: KrNewsWindowCoverage(
                ticker=str(item["ticker"]),
                window_start=date.fromisoformat(item["window_start"]),
                window_end=date.fromisoformat(item["window_end"]),
                raw_count=int(item.get("raw_count", 0)),
                captured_days=int(item.get("captured_days", 0)),
                missing_capture_days=[
                    str(value) for value in item.get("missing_capture_days", [])
                ],
                page_cap_hit_days=[str(value) for value in item.get("page_cap_hit_days", [])],
                status=str(item.get("status", "degraded")),
            )
            for ticker, item in payload.get("news_coverage", {}).items()
        },
    )


async def validate_kr_checkpoint(
    *,
    state: KrCollectionState,
    as_of: date,
) -> list[ValidationCheck]:
    checks: list[ValidationCheck] = []
    window_start = _news_window_start(as_of)
    for ticker, bars in state.prices.items():
        dates = [bar.date for bar in bars]
        if not dates:
            checks.append(ValidationCheck(f"kr:prices:{ticker}", "failed", "empty"))
            continue
        if any(day > as_of for day in dates):
            checks.append(ValidationCheck(f"kr:prices:{ticker}", "failed", "future_date"))
            continue
        if not _is_sorted_dates(dates):
            checks.append(ValidationCheck(f"kr:prices:{ticker}", "failed", "unsorted"))

    for ticker, fund in state.fundamentals.items():
        if not fund.quarters:
            status = "degraded" if fund.market_cap is not None or fund.last_close_price is not None else "failed"
            checks.append(ValidationCheck(f"kr:fundamentals:{ticker}", status, "missing_quarters"))
            continue
        if any(quarter.report_date > as_of for quarter in fund.quarters):
            checks.append(ValidationCheck(f"kr:fundamentals:{ticker}", "failed", "future_report_date"))

    for ticker, filing in state.filings.items():
        if not filing.business_overview:
            checks.append(ValidationCheck(f"kr:filing:{ticker}", "degraded", "missing_business_overview"))
            continue
        if filing.filing_date is not None and filing.filing_date > as_of:
            checks.append(ValidationCheck(f"kr:filing:{ticker}", "failed", "future_filing_date"))

    for ticker, items in state.news.items():
        audit = state.news_audit.get(ticker, [])
        coverage = state.news_coverage.get(ticker)
        seen_urls: set[str] = set()
        if len(items) != len(audit):
            checks.append(ValidationCheck(f"kr:news:{ticker}", "failed", "audit_count_mismatch"))
            continue
        for item, record in zip(items, audit, strict=True):
            if (
                item.date != record.date
                or item.headline != record.headline
                or item.url != record.url
                or item.published_at != record.published_at
            ):
                checks.append(ValidationCheck(f"kr:news:{ticker}", "failed", "record_mismatch"))
                break
            if item.date < window_start or item.date > as_of:
                checks.append(ValidationCheck(f"kr:news:{ticker}", "failed", "date_out_of_window"))
                break
            if record.url in seen_urls:
                checks.append(ValidationCheck(f"kr:news:{ticker}", "failed", "duplicate_url"))
                break
            seen_urls.add(record.url)
        if coverage is None:
            checks.append(ValidationCheck(f"kr:news_coverage:{ticker}", "failed", "missing"))
            continue
        if coverage.window_start != window_start or coverage.window_end != as_of:
            checks.append(ValidationCheck(f"kr:news_coverage:{ticker}", "failed", "window_mismatch"))
            continue
        if coverage.status != "ok":
            checks.append(
                ValidationCheck(
                    f"kr:news_coverage:{ticker}",
                    "degraded",
                    coverage.status,
                    metrics={
                        "raw_count": coverage.raw_count,
                        "missing_capture_days": len(coverage.missing_capture_days),
                        "page_cap_hit_days": len(coverage.page_cap_hit_days),
                    },
                )
            )
    return checks


def validate_kr_final_snapshot(
    *,
    snapshot: MonthlySnapshot,
    news_audit: dict[str, list[NaverArchiveNewsRecord]],
    news_coverage: dict[str, KrNewsWindowCoverage],
    as_of: date,
) -> list[ValidationCheck]:
    checks: list[ValidationCheck] = []
    window_start = _news_window_start(as_of)
    if snapshot.decision_date != as_of:
        checks.append(ValidationCheck("kr:decision_date", "failed", "decision_date_mismatch"))
    if len(snapshot.universe) != len(snapshot.prices):
        checks.append(ValidationCheck("kr:universe_prices", "failed", "size_mismatch"))
    if not snapshot.benchmark_prices:
        checks.append(ValidationCheck("kr:benchmark", "degraded", "empty"))
    macro_keys = (
        len(snapshot.macro.rates_policy)
        + len(snapshot.macro.inflation_commodities)
        + len(snapshot.macro.growth_economy)
        + len(snapshot.macro.market_risk)
    )
    if macro_keys == 0:
        checks.append(ValidationCheck("kr:macro", "degraded", "empty"))
    for ticker, items in snapshot.news.items():
        audit = news_audit.get(ticker, [])
        coverage = news_coverage.get(ticker)
        if len(items) != len(audit):
            checks.append(ValidationCheck(f"kr:final_news:{ticker}", "failed", "audit_count_mismatch"))
            continue
        if any(item.date < window_start or item.date > as_of for item in items):
            checks.append(ValidationCheck(f"kr:final_news:{ticker}", "failed", "date_out_of_window"))
        if coverage is None:
            checks.append(ValidationCheck(f"kr:final_news_coverage:{ticker}", "failed", "missing"))
            continue
        if coverage.status != "ok":
            checks.append(
                ValidationCheck(
                    f"kr:final_news_coverage:{ticker}",
                    "degraded",
                    coverage.status,
                    metrics={
                        "raw_count": coverage.raw_count,
                        "missing_capture_days": len(coverage.missing_capture_days),
                        "page_cap_hit_days": len(coverage.page_cap_hit_days),
                    },
                )
            )
    return checks


class LocalKrCollector:
    """Collect KR snapshot data with checkpoint validation."""

    def __init__(
        self,
        *,
        as_of: date,
        storage_root: Path,
        bundle_root: Path,
        partial_root: Path,
        checkpoint_root: Path,
        policy: CheckpointPolicy,
        progress_path: Path,
        dart_mode: str = "live",
    ) -> None:
        self.as_of = as_of
        self.month = as_of.strftime("%Y-%m")
        self.storage_root = storage_root
        self.bundle_root = bundle_root
        self.partial_root = partial_root
        self.checkpoint_root = checkpoint_root
        self.policy = policy
        self.progress_path = progress_path
        self.inter_ticker_delay_seconds = 0.0 if dart_mode == "cache_only" else 4.0
        if dart_mode == "cache_only":
            self.dart = CacheOnlyDartProvider()
        else:
            self.dart = DartProvider(allow_stale_fallback=False, raise_on_error=True)
        self.pykrx = PykrxProvider(official_only=True)
        self.fundamentals = CompositeKrFundamentalProvider(
            dart_provider=self.dart,
            price_provider=self.pykrx,
            raise_on_error=True,
        )
        self.pykrx._fundamental_provider = self.fundamentals
        try:
            self.macro_provider: Any = EcosMacroProvider()
        except Exception:
            self.macro_provider = NullMacroProvider()

    def _switch_dart_cache_only(self, reason: Exception) -> None:
        if isinstance(self.dart, CacheOnlyDartProvider):
            return
        logger.warning("Switching DART provider to cache-only mode: %s", reason)
        self.dart = CacheOnlyDartProvider()
        self.fundamentals = CompositeKrFundamentalProvider(
            dart_provider=self.dart,
            price_provider=self.pykrx,
            raise_on_error=True,
        )
        self.pykrx._fundamental_provider = self.fundamentals

    async def _fetch_fundamentals_with_fallback(self, ticker: str) -> FundamentalData:
        try:
            return await self.fundamentals.fetch_fundamentals(
                ticker,
                self.as_of,
                n_quarters=8,
            )
        except Exception as exc:
            logger.warning(
                "DART fundamentals unavailable for %s as of %s: %s",
                ticker,
                self.as_of,
                exc,
            )
            if _is_dart_transient_error(exc):
                self._switch_dart_cache_only(exc)
                return await self.fundamentals.fetch_fundamentals(
                    ticker,
                    self.as_of,
                    n_quarters=8,
                )

            cache_only_fundamentals = CompositeKrFundamentalProvider(
                dart_provider=CacheOnlyDartProvider(),
                price_provider=self.pykrx,
                raise_on_error=True,
            )
            return await cache_only_fundamentals.fetch_fundamentals(
                ticker,
                self.as_of,
                n_quarters=8,
            )

    async def collect(
        self,
        *,
        universe_csv: Path,
        resume: bool,
    ) -> dict[str, Any]:
        tickers = load_tickers(universe_csv)
        self.bundle_root.mkdir(parents=True, exist_ok=True)
        self.partial_root.mkdir(parents=True, exist_ok=True)
        self.checkpoint_root.mkdir(parents=True, exist_ok=True)

        progress = load_progress(
            self.progress_path,
            {
                "as_of": self.as_of.isoformat(),
                "month": self.month,
                "next_index": 0,
                "completed_batches": 0,
                "completed_tickers": 0,
            },
        )
        state = KrCollectionState()
        next_index = 0

        if resume:
            batch_files = sorted(self.partial_root.glob("batch_*.json"))
            for batch_file in batch_files:
                state.merge(_load_batch_payload(batch_file))
            # Older partial batches may still contain news payloads.
            state.news.clear()
            state.news_audit.clear()
            state.news_coverage.clear()
            next_index = int(progress.get("next_index", 0) or 0)

        sector_map = await self.pykrx.fetch_sector_map(tickers, as_of=self.as_of)
        last_checkpoint_at = time.monotonic()
        batch_no = int(progress.get("completed_batches", 0) or 0)
        pending = BatchPayload(
            tickers=[],
            prices={},
            fundamentals={},
            filings={},
            news={},
            news_audit={},
            news_coverage={},
        )

        while next_index + len(pending.tickers) < len(tickers):
            ticker = tickers[next_index + len(pending.tickers)]
            payload = await self._collect_ticker(ticker)
            pending.merge(payload)
            preview = KrCollectionState(
                prices=dict(state.prices),
                fundamentals=dict(state.fundamentals),
                filings=dict(state.filings),
                news=dict(state.news),
                news_audit=dict(state.news_audit),
                news_coverage=dict(state.news_coverage),
            )
            preview.merge(pending)
            should_checkpoint = (
                len(pending.tickers) >= self.policy.every_tickers
                or time.monotonic() - last_checkpoint_at >= self.policy.every_seconds
                or next_index + len(pending.tickers) >= len(tickers)
            )
            if not should_checkpoint:
                continue

            checks = await validate_kr_checkpoint(
                state=preview,
                as_of=self.as_of,
            )
            batch_no += 1
            report_path = self.checkpoint_root / f"batch_{batch_no:04d}.json"
            _write_json(
                report_path,
                {
                    "stage": "kr_bundle_checkpoint",
                    "batch": batch_no,
                    "processed_tickers": next_index + len(pending.tickers),
                    "tickers": pending.tickers,
                    "summary": summarize_checks(checks),
                },
            )
            raise_for_failed_checks("kr_bundle_checkpoint", checks, report_path)

            batch_path = self.partial_root / f"batch_{batch_no:04d}.json"
            _write_json(batch_path, _serialize_batch(pending))
            state.merge(pending)
            next_index += len(pending.tickers)
            progress.update(
                {
                    "next_index": next_index,
                    "completed_batches": batch_no,
                    "completed_tickers": next_index,
                    "last_checkpoint_report": str(report_path),
                }
            )
            save_progress(self.progress_path, progress)
            last_checkpoint_at = time.monotonic()
            pending = BatchPayload(
                tickers=[],
                prices={},
                fundamentals={},
                filings={},
                news={},
                news_audit={},
                news_coverage={},
            )

        macro = await self.macro_provider.fetch_macro(self.as_of)
        benchmark_prices = await self.pykrx.fetch_benchmark(self.as_of)
        sector_averages = compute_sector_averages_from_state(sector_map, state.fundamentals)

        snapshot = self._build_snapshot(
            tickers=tickers,
            state=state,
            sector_map=sector_map,
            sector_averages=sector_averages,
            macro=macro,
            benchmark_prices=benchmark_prices,
        )
        summary = self._persist_snapshot(snapshot, state)
        final_checks = validate_kr_final_snapshot(
            snapshot=snapshot,
            news_audit=state.news_audit,
            news_coverage=state.news_coverage,
            as_of=self.as_of,
        )
        final_report_path = self.bundle_root / "snapshots" / self.month / "validation_report.json"
        _write_json(
            final_report_path,
            {
                "stage": "kr_bundle_final",
                "summary": summarize_checks(final_checks),
            },
        )
        raise_for_failed_checks("kr_bundle_final", final_checks, final_report_path)
        progress["status"] = "completed"
        progress["final_summary_path"] = summary["summary_path"]
        save_progress(self.progress_path, progress)
        return summary

    async def _collect_ticker(self, ticker: str) -> BatchPayload:
        payload = BatchPayload(
            tickers=[ticker],
            prices={},
            fundamentals={},
            filings={},
            news={},
            news_audit={},
            news_coverage={},
        )
        prices = await self.pykrx.fetch_prices(ticker, self.as_of, lookback_days=300)
        fundamentals = await self._fetch_fundamentals_with_fallback(ticker)
        try:
            filing = await self.dart.fetch_filing(ticker, self.as_of)
        except Exception as exc:
            logger.warning("DART filing unavailable for %s as of %s: %s", ticker, self.as_of, exc)
            filing = FilingData(ticker=ticker)
        payload.prices[ticker] = prices
        payload.fundamentals[ticker] = fundamentals
        payload.filings[ticker] = filing
        if self.inter_ticker_delay_seconds > 0:
            await asyncio.sleep(self.inter_ticker_delay_seconds)
        return payload

    def _build_snapshot(
        self,
        *,
        tickers: list[str],
        state: KrCollectionState,
        sector_map: dict[str, str],
        sector_averages: dict[str, SectorAverages],
        macro: MacroData,
        benchmark_prices: list[PriceBar],
    ) -> MonthlySnapshot:
        execution_date = _next_kr_execution_date(self.as_of)

        metadata = SnapshotMetadata(
            created_at=datetime.utcnow().isoformat(),
            config_hash=config_hash(SnapshotConfig(artifacts_dir=str(self.bundle_root))),
            price_hash=_content_hash({ticker: len(items) for ticker, items in state.prices.items()}),
            fundamental_hash=_content_hash(
                {ticker: len(item.quarters) for ticker, item in state.fundamentals.items()}
            ),
            filing_hash=_content_hash(
                {ticker: bool(item.business_overview) for ticker, item in state.filings.items()}
            ),
            news_hash="",
            macro_hash=_content_hash(macro.model_dump(mode="json")),
        )

        return MonthlySnapshot(
            decision_date=self.as_of,
            execution_date=execution_date,
            universe=tickers,
            prices=state.prices,
            fundamentals=state.fundamentals,
            filings=state.filings,
            macro=macro,
            sector_map=sector_map,
            sector_averages=sector_averages,
            benchmark_prices=benchmark_prices,
            input_hash=_content_hash({"decision_date": self.as_of.isoformat(), "universe": tickers}),
            metadata=metadata,
        )

    def _persist_snapshot(self, snapshot: MonthlySnapshot, state: KrCollectionState) -> dict[str, Any]:
        month_dir = self.bundle_root / "snapshots" / self.month
        month_dir.mkdir(parents=True, exist_ok=True)
        snapshot_path = month_dir / "snapshot.json"
        metadata_path = month_dir / "metadata.json"
        manifest_path = month_dir / "manifest.json"
        summary_path = month_dir / "summary.json"
        gzip_path = month_dir / "snapshot.json.gz"

        snapshot_text = json.dumps(serialize_snapshot(snapshot), indent=2, sort_keys=True)
        snapshot_path.write_text(snapshot_text, encoding="utf-8")
        gzip_path.write_bytes(gzip.compress(snapshot_text.encode("utf-8")))
        metadata_path.write_text(
            json.dumps(serialize_snapshot_metadata(snapshot.metadata), indent=2, sort_keys=True),
            encoding="utf-8",
        )

        manifest = {
            "market": "kr",
            "month": self.month,
            "decision_date": snapshot.decision_date.isoformat(),
            "execution_date": snapshot.execution_date.isoformat(),
            "files": {
                "snapshot": "snapshot.json",
                "snapshot_gzip": "snapshot.json.gz",
                "metadata": "metadata.json",
                "summary": "summary.json",
            },
        }
        _write_json(manifest_path, manifest)

        summary = {
            "status": "ok",
            "market": "kr",
            "month": self.month,
            "decision_date": snapshot.decision_date.isoformat(),
            "execution_date": snapshot.execution_date.isoformat(),
            "universe_size": len(snapshot.universe),
            "price_tickers": len(snapshot.prices),
            "fundamental_tickers": len(snapshot.fundamentals),
            "filing_tickers": len(snapshot.filings),
            "benchmark_bars": len(snapshot.benchmark_prices),
            "snapshot_path": str(snapshot_path),
            "summary_path": str(summary_path),
        }
        _write_json(summary_path, summary)
        return summary


def run_subprocess_stage(
    *,
    name: str,
    command: list[str],
    log_path: Path,
) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    completed = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
    )
    output = completed.stdout
    if completed.stderr:
        output = f"{output}\n{completed.stderr}" if output else completed.stderr
    log_path.write_text(output, encoding="utf-8")
    if completed.returncode != 0:
        raise RuntimeError(f"{name} failed; see {log_path}")


def run_local_collection(
    *,
    storage_root: Path,
    as_of: date,
    market: str,
    phase: str,
    full_universe_kind: str,
    start: date | None = None,
    resume: bool = False,
    us_universe: str = DEFAULT_US_UNIVERSE,
    dart_mode: str = "live",
) -> Path:
    storage_root = storage_root.expanduser().resolve()
    storage_root.mkdir(parents=True, exist_ok=True)
    run_root = build_run_root(storage_root, as_of, market, phase, full_universe_kind)
    if run_root.exists() and not resume:
        raise RuntimeError(f"Run root already exists: {run_root}")
    run_root.mkdir(parents=True, exist_ok=True)
    progress_path = run_root / "progress.json"
    progress = load_progress(
        progress_path,
        {
            "storage_root": str(storage_root),
            "run_root": str(run_root),
            "as_of": as_of.isoformat(),
            "market": market,
            "phase": phase,
            "full_universe_kind": full_universe_kind,
            "dart_mode": dart_mode,
            "stages": {},
        },
    )
    save_progress(progress_path, progress)

    raw_start = start or default_raw_start(as_of)
    month = as_of.strftime("%Y-%m")
    pilot_universe = run_root / "universes" / "kr" / "pilot" / f"{month}.csv"
    full_universe = run_root / "universes" / "kr" / full_universe_kind / f"{month}.csv"

    if market in {"kr", "both"}:
        if phase in {"pilot", "all"}:
            copy_pilot_universe(pilot_universe)
        if phase in {"full", "all"}:
            build_local_universe_manifest(as_of=as_of, kind=full_universe_kind, output_path=full_universe)

    async def _run_async() -> None:
        if market in {"kr", "both"} and phase in {"pilot", "all"}:
            await _run_kr_phase(
                stage_name="pilot",
                as_of=as_of,
                raw_start=raw_start,
                run_root=run_root,
                universe_csv=pilot_universe,
                    progress_path=progress_path,
                    resume=resume,
                    policy=CheckpointPolicy(every_tickers=5, every_seconds=180),
                    dart_mode=dart_mode,
                )

        if market in {"us", "both"} and phase in {"pilot", "all"}:
            _run_us_phase(
                stage_name="pilot",
                as_of=as_of,
                run_root=run_root,
                progress_path=progress_path,
                us_universe=us_universe,
            )

        if phase in {"full", "all"}:
            if market in {"kr", "both"}:
                await _run_kr_phase(
                    stage_name="full",
                    as_of=as_of,
                    raw_start=raw_start,
                    run_root=run_root,
                    universe_csv=full_universe,
                    progress_path=progress_path,
                    resume=resume,
                    policy=CheckpointPolicy(every_tickers=25, every_seconds=600),
                    dart_mode=dart_mode,
                )
            if market in {"us", "both"}:
                _run_us_phase(
                    stage_name="full",
                    as_of=as_of,
                    run_root=run_root,
                    progress_path=progress_path,
                    us_universe=us_universe,
                )

    asyncio.run(_run_async())
    progress = load_progress(progress_path, progress)
    progress["status"] = "completed"
    save_progress(progress_path, progress)
    return run_root


async def _run_kr_phase(
    *,
    stage_name: str,
    as_of: date,
    raw_start: date,
    run_root: Path,
    universe_csv: Path,
    progress_path: Path,
    resume: bool,
    policy: CheckpointPolicy,
    dart_mode: str,
) -> None:
    progress = load_progress(progress_path, {})
    stages = progress.setdefault("stages", {})
    raw_stage_key = f"{stage_name}_kr_raw"
    bundle_stage_key = f"{stage_name}_kr_bundle"

    raw_root = run_root / "raw" / "kr" / stage_name
    raw_report = run_root / "reports" / f"{raw_stage_key}.json"
    if stages.get(raw_stage_key, {}).get("status") != "completed":
        run_subprocess_stage(
            name=raw_stage_key,
            command=[
                sys.executable,
                "scripts/crawl_kr_data_pykrx.py",
                "--start",
                raw_start.isoformat(),
                "--end",
                as_of.isoformat(),
                "--output-root",
                str(raw_root),
                "--skip-meta",
                "--skip-ohlcv",
            ],
            log_path=run_root / "logs" / f"{raw_stage_key}.log",
        )
        raw_checks = validate_kr_raw_outputs(raw_root)
        _write_json(raw_report, summarize_checks(raw_checks))
        raise_for_failed_checks(raw_stage_key, raw_checks, raw_report)
        stages[raw_stage_key] = {"status": "completed", "report": str(raw_report)}
        save_progress(progress_path, progress)

    collector = LocalKrCollector(
        as_of=as_of,
        storage_root=run_root.parents[2],
        bundle_root=run_root / "bundles" / "kr" / stage_name,
        partial_root=run_root / "partials" / "kr" / stage_name,
        checkpoint_root=run_root / "reports" / "kr" / stage_name,
        policy=policy,
        progress_path=run_root / "reports" / "kr" / f"{stage_name}_progress.json",
        dart_mode=dart_mode,
    )
    summary = await collector.collect(universe_csv=universe_csv, resume=resume)
    stages[bundle_stage_key] = {"status": "completed", "summary": summary}
    save_progress(progress_path, progress)


def _run_us_phase(
    *,
    stage_name: str,
    as_of: date,
    run_root: Path,
    progress_path: Path,
    us_universe: str,
) -> None:
    progress = load_progress(progress_path, {})
    stages = progress.setdefault("stages", {})
    stage_key = f"{stage_name}_us_bundle"
    if stages.get(stage_key, {}).get("status") == "completed":
        return

    bundle_root = run_root / "bundles" / "us" / stage_name
    run_subprocess_stage(
        name=stage_key,
        command=[
            sys.executable,
            "scripts/build_us_snapshot.py",
            "--as-of",
            as_of.isoformat(),
            "--universe",
            us_universe,
            "--artifacts-root",
            str(bundle_root),
        ],
        log_path=run_root / "logs" / f"{stage_key}.log",
    )
    checks = validate_us_outputs(bundle_root, as_of)
    report_path = run_root / "reports" / f"{stage_key}.json"
    _write_json(report_path, summarize_checks(checks))
    raise_for_failed_checks(stage_key, checks, report_path)
    stages[stage_key] = {"status": "completed", "report": str(report_path)}
    save_progress(progress_path, progress)
