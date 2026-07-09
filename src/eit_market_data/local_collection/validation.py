"""Checkpoint and final-snapshot validation checks."""

from __future__ import annotations

from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any

from eit_market_data.kr.naver_news_provider import NaverArchiveNewsRecord
from eit_market_data.kr.news_catalog import KrNewsWindowCoverage
from eit_market_data.local_collection.progress import _news_window_start, _read_json
from eit_market_data.local_collection.state import (
    KrCollectionState,
    ValidationCheck,
    ValidationError,
)
from eit_market_data.schemas.snapshot import MonthlySnapshot


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
