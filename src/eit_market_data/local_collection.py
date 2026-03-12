"""Local-only collection orchestration with checkpoint validation."""

from __future__ import annotations

import asyncio
import csv
import gzip
import hashlib
import json
import shutil
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from eit_market_data.kr.ci_safe_provider import NullMacroProvider
from eit_market_data.kr.dart_provider import DartProvider
from eit_market_data.kr.ecos_provider import EcosMacroProvider
from eit_market_data.kr.fundamental_provider import CompositeKrFundamentalProvider
from eit_market_data.kr.market_helpers import fetch_market_cap_frame, normalize_ticker
from eit_market_data.kr.naver_news_provider import (
    NaverArchiveNewsProvider,
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
from eit_market_data.snapshot import SnapshotConfig, config_hash

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_US_UNIVERSE = "AAPL,MSFT,GOOGL,AMZN,NVDA"
CURRENT_KR_UNIVERSE_CSV = PROJECT_ROOT / "universes" / "kr_universe.csv"


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

    def merge(self, payload: "BatchPayload") -> None:
        self.tickers.extend(payload.tickers)
        self.prices.update(payload.prices)
        self.fundamentals.update(payload.fundamentals)
        self.filings.update(payload.filings)
        self.news.update(payload.news)
        self.news_audit.update(payload.news_audit)


@dataclass
class KrCollectionState:
    prices: dict[str, list[PriceBar]] = field(default_factory=dict)
    fundamentals: dict[str, FundamentalData] = field(default_factory=dict)
    filings: dict[str, FilingData] = field(default_factory=dict)
    news: dict[str, list[NewsItem]] = field(default_factory=dict)
    news_audit: dict[str, list[NaverArchiveNewsRecord]] = field(default_factory=dict)

    def merge(self, payload: BatchPayload) -> None:
        self.prices.update(payload.prices)
        self.fundamentals.update(payload.fundamentals)
        self.filings.update(payload.filings)
        self.news.update(payload.news)
        self.news_audit.update(payload.news_audit)


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _date_token(value: date) -> str:
    return value.isoformat()


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _hash_blob(obj: object) -> str:
    blob = json.dumps(obj, sort_keys=True, default=str).encode()
    return hashlib.sha256(blob).hexdigest()[:16]


def _month_start(value: date) -> date:
    return date(value.year, value.month, 1)


def _next_month(value: date) -> date:
    if value.month == 12:
        return date(value.year + 1, 1, 1)
    return date(value.year, value.month + 1, 1)


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
            return normalized[["ticker", "name", "market", "sector"]].drop_duplicates("ticker")

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
    return pd.concat(frames, ignore_index=True).drop_duplicates("ticker")


def build_local_universe_manifest(
    *,
    as_of: date,
    kind: str,
    output_path: Path,
) -> Path:
    import pandas as pd

    kind = kind.lower()
    meta = _listing_metadata_frame()

    if kind != "full":
        if not kind.startswith("top"):
            raise ValueError(f"Unsupported universe kind: {kind}")
        top_n = int(kind.removeprefix("top"))
        cap_frames = []
        for market in ("KOSPI", "KOSDAQ"):
            frame = fetch_market_cap_frame(as_of, market)
            if frame is None or frame.empty:
                continue
            working = frame.reset_index() if "종목코드" not in frame.columns else frame.reset_index(drop=True)
            if "종목코드" not in working.columns or "시가총액" not in working.columns:
                continue
            cap_frames.append(
                pd.DataFrame(
                    {
                        "ticker": working["종목코드"].astype(str).map(normalize_ticker),
                        "market_cap": pd.to_numeric(working["시가총액"], errors="coerce"),
                    }
                )
            )
        if not cap_frames:
            raise RuntimeError(f"Market-cap data unavailable for {kind} universe generation.")
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
    ordered = records[["ticker", "market", "sector", "name", "market_cap", "rank", "as_of"]]
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
    import numpy as np

    grouped: dict[str, list[FundamentalData]] = {}
    for ticker, sector in sector_map.items():
        grouped.setdefault(sector or "General", []).append(
            fundamentals.get(ticker, FundamentalData(ticker=ticker))
        )

    result: dict[str, SectorAverages] = {}
    for sector, funds in grouped.items():
        metrics: dict[str, list[float]] = {}
        for fund in funds:
            if not fund.quarters:
                continue
            q = fund.quarters[0]
            revenue = q.revenue
            total_assets = q.total_assets
            if not revenue or not total_assets or total_assets == 0:
                continue

            def _add(key: str, value: float | None) -> None:
                if value is not None:
                    metrics.setdefault(key, []).append(value)

            _add("roa", (q.net_income or 0) / total_assets if total_assets else None)
            _add("roe", (q.net_income or 0) / q.total_equity if q.total_equity else None)
            _add("gross_margin", (q.gross_profit or 0) / revenue)
            _add("operating_margin", (q.operating_income or 0) / revenue)
            _add("net_margin", (q.net_income or 0) / revenue)
            if q.current_liabilities and q.current_liabilities > 0:
                _add("current_ratio", (q.current_assets or 0) / q.current_liabilities)
            if q.total_equity and q.total_equity > 0:
                _add("debt_to_equity", (q.total_debt or 0) / q.total_equity)
            _add("asset_turnover", revenue / total_assets)
            if fund.last_close_price and q.eps and q.eps > 0:
                _add("pe_ttm", fund.last_close_price / (q.eps * 4))

        avg_metrics = {
            key: round(float(np.mean(values)), 4)
            for key, values in metrics.items()
            if values
        }
        result[sector] = SectorAverages(sector=sector, avg_metrics=avg_metrics)
    return result


def _serialize_batch(payload: BatchPayload) -> dict[str, Any]:
    return {
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
        "news": {
            ticker: [item.model_dump(mode="json") for item in items]
            for ticker, items in payload.news.items()
        },
        "news_audit": {
            ticker: [asdict(item) for item in items]
            for ticker, items in payload.news_audit.items()
        },
    }


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
                    headline=str(item["headline"]),
                    url=str(item["url"]),
                    source=str(item.get("source", "Naver")),
                )
                for item in items
            ]
            for ticker, items in payload.get("news_audit", {}).items()
        },
    )


async def validate_kr_checkpoint(
    *,
    state: KrCollectionState,
    as_of: date,
    news_provider: NaverArchiveNewsProvider,
) -> list[ValidationCheck]:
    checks: list[ValidationCheck] = []
    month_start = _month_start(as_of)
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
            checks.append(ValidationCheck(f"kr:fundamentals:{ticker}", "failed", "empty"))
            continue
        if any(quarter.report_date > as_of for quarter in fund.quarters):
            checks.append(ValidationCheck(f"kr:fundamentals:{ticker}", "failed", "future_report_date"))

    for ticker, filing in state.filings.items():
        if not filing.business_overview:
            checks.append(ValidationCheck(f"kr:filing:{ticker}", "failed", "missing_business_overview"))
            continue
        if filing.filing_date is not None and filing.filing_date > as_of:
            checks.append(ValidationCheck(f"kr:filing:{ticker}", "failed", "future_filing_date"))

    for ticker, items in state.news.items():
        audit = state.news_audit.get(ticker, [])
        seen_urls: set[str] = set()
        if len(items) != len(audit):
            checks.append(ValidationCheck(f"kr:news:{ticker}", "failed", "audit_count_mismatch"))
            continue
        for item, record in zip(items, audit, strict=True):
            if item.date != record.date or item.headline != record.headline:
                checks.append(ValidationCheck(f"kr:news:{ticker}", "failed", "record_mismatch"))
                break
            if item.date < month_start or item.date > as_of:
                checks.append(ValidationCheck(f"kr:news:{ticker}", "failed", "date_out_of_month"))
                break
            if record.url in seen_urls:
                checks.append(ValidationCheck(f"kr:news:{ticker}", "failed", "duplicate_url"))
                break
            seen_urls.add(record.url)

    sample_ticker = next((ticker for ticker, items in state.news_audit.items() if items), None)
    if sample_ticker is not None:
        refetched = await news_provider.fetch_archive_records(
            sample_ticker,
            as_of=as_of,
            lookback_days=(as_of - month_start).days + 1,
        )
        stored = state.news_audit[sample_ticker]
        compare_count = min(5, len(stored), len(refetched))
        if compare_count == 0 and stored:
            checks.append(ValidationCheck("kr:news:sample_refetch", "failed", "sample_refetch_empty"))
        else:
            left = [
                (stored[idx].date.isoformat(), stored[idx].headline, stored[idx].url)
                for idx in range(compare_count)
            ]
            right = [
                (refetched[idx].date.isoformat(), refetched[idx].headline, refetched[idx].url)
                for idx in range(compare_count)
            ]
            if left != right:
                checks.append(ValidationCheck("kr:news:sample_refetch", "failed", "sample_refetch_mismatch"))
            else:
                checks.append(ValidationCheck("kr:news:sample_refetch", "ok", sample_ticker))
    return checks


def validate_kr_final_snapshot(
    *,
    snapshot: MonthlySnapshot,
    news_audit: dict[str, list[NaverArchiveNewsRecord]],
    as_of: date,
) -> list[ValidationCheck]:
    checks: list[ValidationCheck] = []
    month_start = _month_start(as_of)
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
        if len(items) != len(audit):
            checks.append(ValidationCheck(f"kr:final_news:{ticker}", "failed", "audit_count_mismatch"))
            continue
        if any(item.date < month_start or item.date > as_of for item in items):
            checks.append(ValidationCheck(f"kr:final_news:{ticker}", "failed", "date_out_of_month"))
    return checks


class LocalKrCollector:
    """Collect KR snapshot data with checkpoint validation."""

    def __init__(
        self,
        *,
        as_of: date,
        bundle_root: Path,
        partial_root: Path,
        checkpoint_root: Path,
        policy: CheckpointPolicy,
        progress_path: Path,
    ) -> None:
        self.as_of = as_of
        self.month = as_of.strftime("%Y-%m")
        self.bundle_root = bundle_root
        self.partial_root = partial_root
        self.checkpoint_root = checkpoint_root
        self.policy = policy
        self.progress_path = progress_path
        self.inter_ticker_delay_seconds = 4.0
        self.dart = DartProvider(allow_stale_fallback=False, raise_on_error=True)
        self.pykrx = PykrxProvider(official_only=True)
        self.fundamentals = CompositeKrFundamentalProvider(
            dart_provider=self.dart,
            price_provider=self.pykrx,
            raise_on_error=True,
        )
        self.pykrx._fundamental_provider = self.fundamentals
        self.news_provider = NaverArchiveNewsProvider(
            max_pages=200,
            page_delay_seconds=0.1,
            require_full_coverage=True,
            raise_on_error=True,
        )
        try:
            self.macro_provider: Any = EcosMacroProvider()
        except Exception:
            self.macro_provider = NullMacroProvider()

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
                news_provider=self.news_provider,
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
        summary = self._persist_snapshot(snapshot)
        final_checks = validate_kr_final_snapshot(
            snapshot=snapshot,
            news_audit=state.news_audit,
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
        )
        lookback_days = (self.as_of - _month_start(self.as_of)).days + 1
        prices = await self.pykrx.fetch_prices(ticker, self.as_of, lookback_days=300)
        fundamentals = await self.fundamentals.fetch_fundamentals(ticker, self.as_of, n_quarters=8)
        filing = await self.dart.fetch_filing(ticker, self.as_of)
        records = await self.news_provider.fetch_archive_records(
            ticker,
            as_of=self.as_of,
            lookback_days=lookback_days,
        )
        payload.prices[ticker] = prices
        payload.fundamentals[ticker] = fundamentals
        payload.filings[ticker] = filing
        payload.news[ticker] = [
            NewsItem(
                date=record.date,
                source=record.source,
                headline=record.headline,
                summary="",
            )
            for record in records
        ]
        payload.news_audit[ticker] = records
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
        execution_date = _next_month(self.as_of)
        while execution_date.weekday() >= 5:
            execution_date += timedelta(days=1)

        metadata = SnapshotMetadata(
            created_at=datetime.utcnow().isoformat(),
            config_hash=config_hash(SnapshotConfig(artifacts_dir=str(self.bundle_root))),
            price_hash=_hash_blob({ticker: len(items) for ticker, items in state.prices.items()}),
            fundamental_hash=_hash_blob(
                {ticker: len(item.quarters) for ticker, item in state.fundamentals.items()}
            ),
            filing_hash=_hash_blob(
                {ticker: bool(item.business_overview) for ticker, item in state.filings.items()}
            ),
            news_hash=_hash_blob({ticker: len(items) for ticker, items in state.news.items()}),
            macro_hash=_hash_blob(macro.model_dump(mode="json")),
        )

        return MonthlySnapshot(
            decision_date=self.as_of,
            execution_date=execution_date,
            universe=tickers,
            prices=state.prices,
            fundamentals=state.fundamentals,
            filings=state.filings,
            news=state.news,
            macro=macro,
            sector_map=sector_map,
            sector_averages=sector_averages,
            benchmark_prices=benchmark_prices,
            input_hash=_hash_blob({"decision_date": self.as_of.isoformat(), "universe": tickers}),
            metadata=metadata,
        )

    def _persist_snapshot(self, snapshot: MonthlySnapshot) -> dict[str, Any]:
        month_dir = self.bundle_root / "snapshots" / self.month
        month_dir.mkdir(parents=True, exist_ok=True)
        snapshot_path = month_dir / "snapshot.json"
        metadata_path = month_dir / "metadata.json"
        manifest_path = month_dir / "manifest.json"
        summary_path = month_dir / "summary.json"
        gzip_path = month_dir / "snapshot.json.gz"

        snapshot_text = snapshot.model_dump_json(indent=2)
        snapshot_path.write_text(snapshot_text, encoding="utf-8")
        gzip_path.write_bytes(gzip.compress(snapshot_text.encode("utf-8")))
        metadata_path.write_text(snapshot.metadata.model_dump_json(indent=2), encoding="utf-8")

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
            "news_tickers": len(snapshot.news),
            "news_items": sum(len(items) for items in snapshot.news.values()),
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
                "scripts/crawl_kr_data_fallback.py",
                "--start",
                raw_start.isoformat(),
                "--end",
                as_of.isoformat(),
                "--universe-csv",
                str(universe_csv),
                "--output-root",
                str(raw_root),
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
        bundle_root=run_root / "bundles" / "kr" / stage_name,
        partial_root=run_root / "partials" / "kr" / stage_name,
        checkpoint_root=run_root / "reports" / "kr" / stage_name,
        policy=policy,
        progress_path=run_root / "reports" / "kr" / f"{stage_name}_progress.json",
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
