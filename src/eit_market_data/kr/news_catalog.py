"""Persistent local storage for KR news crawl results."""

from __future__ import annotations

import gzip
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from eit_market_data.kr.market_helpers import normalize_ticker
from eit_market_data.kr.naver_news_provider import (
    NaverArchiveFetchResult,
    NaverArchiveNewsProvider,
    NaverArchiveNewsRecord,
)
from eit_market_data.schemas.snapshot import NewsItem


@dataclass(frozen=True)
class KrNewsCatalogEntry:
    ticker: str
    date: date
    published_at: datetime | None
    source: str
    headline: str
    url: str
    summary: str = ""


@dataclass(frozen=True)
class KrNewsCatalogDayMeta:
    ticker: str
    date: date
    status: str
    page_cap_hit: bool
    raw_count: int
    source_crawl_dates: list[str]
    updated_at: str


@dataclass(frozen=True)
class KrNewsWindowCoverage:
    ticker: str
    window_start: date
    window_end: date
    raw_count: int
    captured_days: int
    missing_capture_days: list[str]
    page_cap_hit_days: list[str]
    status: str


@dataclass(frozen=True)
class KrNewsWindow:
    items: list[NewsItem]
    audit: list[NaverArchiveNewsRecord]
    coverage: KrNewsWindowCoverage


def _daterange(start: date, end: date) -> list[date]:
    days: list[date] = []
    current = start
    while current <= end:
        days.append(current)
        current += timedelta(days=1)
    return days


def _json_default(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"Unsupported value: {type(value)!r}")


def _record_sort_key(record: NaverArchiveNewsRecord) -> tuple[datetime | None, date, str]:
    return (record.published_at, record.date, record.headline)


def _entry_key(entry: KrNewsCatalogEntry) -> tuple[str, str, str]:
    if entry.url:
        return ("url", entry.url, entry.ticker)
    published = entry.published_at.isoformat() if entry.published_at is not None else entry.date.isoformat()
    return ("headline", entry.headline, published)


class KrNewsCatalogStore:
    """Store day-level news captures for later rolling-window reconstruction."""

    def __init__(self, storage_root: Path) -> None:
        self.storage_root = storage_root.expanduser().resolve()
        self.catalog_root = self.storage_root / "catalogs" / "kr" / "news"

    def day_dir(self, capture_date: date) -> Path:
        return self.catalog_root / capture_date.isoformat()

    def entries_path(self, ticker: str, capture_date: date) -> Path:
        return self.day_dir(capture_date) / f"{normalize_ticker(ticker)}.jsonl.gz"

    def meta_path(self, ticker: str, capture_date: date) -> Path:
        return self.day_dir(capture_date) / f"{normalize_ticker(ticker)}.meta.json"

    async def capture_archive_window(
        self,
        *,
        provider: NaverArchiveNewsProvider,
        ticker: str,
        as_of: date,
        lookback_days: int = 30,
    ) -> NaverArchiveFetchResult:
        result = await provider.fetch_archive_result(ticker, as_of, lookback_days=lookback_days)
        self.ingest_fetch_result(ticker=ticker, crawl_date=as_of, result=result)
        return result

    async def capture_and_build_window(
        self,
        *,
        provider: NaverArchiveNewsProvider,
        ticker: str,
        as_of: date,
        lookback_days: int = 30,
    ) -> KrNewsWindow:
        await self.capture_archive_window(
            provider=provider,
            ticker=ticker,
            as_of=as_of,
            lookback_days=lookback_days,
        )
        return self.build_window(ticker=ticker, as_of=as_of, lookback_days=lookback_days)

    def ingest_fetch_result(
        self,
        *,
        ticker: str,
        crawl_date: date,
        result: NaverArchiveFetchResult,
    ) -> None:
        grouped: dict[date, list[NaverArchiveNewsRecord]] = {}
        for record in result.records:
            grouped.setdefault(record.date, []).append(record)

        partial_day = result.last_in_window_date if result.reached_page_cap else None
        fully_covered_start = result.required_start
        if partial_day is not None:
            fully_covered_start = min(crawl_date, partial_day + timedelta(days=1))

        for capture_day, records in grouped.items():
            self._merge_day_capture(
                ticker=ticker,
                capture_day=capture_day,
                records=records,
                crawl_date=crawl_date,
                status="partial" if partial_day == capture_day else "ok",
            )

        for capture_day in _daterange(fully_covered_start, crawl_date):
            if capture_day in grouped:
                continue
            self._merge_day_capture(
                ticker=ticker,
                capture_day=capture_day,
                records=[],
                crawl_date=crawl_date,
                status="ok",
            )

    def build_window(
        self,
        *,
        ticker: str,
        as_of: date,
        lookback_days: int = 30,
    ) -> KrNewsWindow:
        norm_ticker = normalize_ticker(ticker)
        window_start = as_of - timedelta(days=max(lookback_days - 1, 0))
        missing_capture_days: list[str] = []
        page_cap_hit_days: list[str] = []
        seen_keys: set[tuple[str, str, str]] = set()
        audit_records: list[NaverArchiveNewsRecord] = []
        captured_days = 0

        for capture_day in _daterange(window_start, as_of):
            meta = self._read_meta(norm_ticker, capture_day)
            if meta is None:
                missing_capture_days.append(capture_day.isoformat())
                continue
            captured_days += 1
            if meta.page_cap_hit:
                page_cap_hit_days.append(capture_day.isoformat())
            for entry in self._read_entries(norm_ticker, capture_day):
                key = _entry_key(entry)
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                audit_records.append(
                    NaverArchiveNewsRecord(
                        date=entry.date,
                        published_at=entry.published_at,
                        headline=entry.headline,
                        url=entry.url,
                        source=entry.source,
                    )
                )

        audit_records.sort(key=_record_sort_key, reverse=True)
        items = [
            NewsItem(
                date=record.date,
                published_at=record.published_at,
                source=record.source,
                headline=record.headline,
                summary="",
                url=record.url,
            )
            for record in audit_records
        ]
        coverage = KrNewsWindowCoverage(
            ticker=norm_ticker,
            window_start=window_start,
            window_end=as_of,
            raw_count=len(audit_records),
            captured_days=captured_days,
            missing_capture_days=missing_capture_days,
            page_cap_hit_days=page_cap_hit_days,
            status="ok" if not missing_capture_days and not page_cap_hit_days else "degraded",
        )
        return KrNewsWindow(items=items, audit=audit_records, coverage=coverage)

    def _merge_day_capture(
        self,
        *,
        ticker: str,
        capture_day: date,
        records: list[NaverArchiveNewsRecord],
        crawl_date: date,
        status: str,
    ) -> None:
        norm_ticker = normalize_ticker(ticker)
        existing_meta = self._read_meta(norm_ticker, capture_day)
        existing_entries = self._read_entries(norm_ticker, capture_day)

        merged: dict[tuple[str, str, str], KrNewsCatalogEntry] = {
            _entry_key(entry): entry for entry in existing_entries
        }
        for record in records:
            entry = KrNewsCatalogEntry(
                ticker=norm_ticker,
                date=record.date,
                published_at=record.published_at,
                source=record.source,
                headline=record.headline,
                url=record.url,
            )
            merged.setdefault(_entry_key(entry), entry)

        merged_entries = sorted(
            merged.values(),
            key=lambda entry: (entry.published_at, entry.date, entry.headline),
            reverse=True,
        )

        source_crawl_dates = [crawl_date.isoformat()]
        if existing_meta is not None:
            source_crawl_dates.extend(existing_meta.source_crawl_dates)
        merged_status = "ok" if status == "ok" or (existing_meta and existing_meta.status == "ok") else "partial"

        meta = KrNewsCatalogDayMeta(
            ticker=norm_ticker,
            date=capture_day,
            status=merged_status,
            page_cap_hit=merged_status != "ok",
            raw_count=len(merged_entries),
            source_crawl_dates=sorted(set(source_crawl_dates)),
            updated_at=datetime.now(UTC).isoformat(),
        )
        self._write_entries(norm_ticker, capture_day, merged_entries)
        self._write_meta(norm_ticker, capture_day, meta)

    def _read_entries(self, ticker: str, capture_day: date) -> list[KrNewsCatalogEntry]:
        path = self.entries_path(ticker, capture_day)
        if not path.exists():
            return []
        entries: list[KrNewsCatalogEntry] = []
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                payload = json.loads(line)
                entries.append(
                    KrNewsCatalogEntry(
                        ticker=str(payload["ticker"]),
                        date=date.fromisoformat(payload["date"]),
                        published_at=(
                            datetime.fromisoformat(payload["published_at"])
                            if payload.get("published_at")
                            else None
                        ),
                        source=str(payload.get("source", "")),
                        headline=str(payload["headline"]),
                        url=str(payload.get("url", "")),
                        summary=str(payload.get("summary", "")),
                    )
                )
        return entries

    def _write_entries(
        self,
        ticker: str,
        capture_day: date,
        entries: list[KrNewsCatalogEntry],
    ) -> None:
        path = self.entries_path(ticker, capture_day)
        path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path, "wt", encoding="utf-8") as handle:
            for entry in entries:
                handle.write(json.dumps(entry.__dict__, default=_json_default, sort_keys=True))
                handle.write("\n")

    def _read_meta(self, ticker: str, capture_day: date) -> KrNewsCatalogDayMeta | None:
        path = self.meta_path(ticker, capture_day)
        if not path.exists():
            return None
        payload = json.loads(path.read_text(encoding="utf-8"))
        return KrNewsCatalogDayMeta(
            ticker=str(payload["ticker"]),
            date=date.fromisoformat(payload["date"]),
            status=str(payload["status"]),
            page_cap_hit=bool(payload.get("page_cap_hit", False)),
            raw_count=int(payload.get("raw_count", 0)),
            source_crawl_dates=[str(item) for item in payload.get("source_crawl_dates", [])],
            updated_at=str(payload.get("updated_at", "")),
        )

    def _write_meta(self, ticker: str, capture_day: date, meta: KrNewsCatalogDayMeta) -> None:
        path = self.meta_path(ticker, capture_day)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(meta.__dict__, default=_json_default, sort_keys=True, indent=2), encoding="utf-8")
