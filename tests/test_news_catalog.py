from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from eit_market_data.kr.news_catalog import KrNewsCatalogStore
from eit_market_data.kr.naver_news_provider import (
    NaverArchiveFetchResult,
    NaverArchiveNewsRecord,
)


class _StubArchiveProvider:
    def __init__(self, result: NaverArchiveFetchResult) -> None:
        self.result = result

    async def fetch_archive_result(self, ticker: str, as_of: date, lookback_days: int = 30) -> NaverArchiveFetchResult:
        _ = (ticker, as_of, lookback_days)
        return self.result


def test_catalog_store_materializes_partial_trailing_day(tmp_path: Path) -> None:
    store = KrNewsCatalogStore(tmp_path)
    tz = timezone(timedelta(hours=9))
    result = NaverArchiveFetchResult(
        records=[
            NaverArchiveNewsRecord(
                date=date(2026, 3, 12),
                published_at=datetime(2026, 3, 12, 9, 0, tzinfo=tz),
                headline="당일 기사",
                url="https://example.com/1",
                source="연합",
            ),
            NaverArchiveNewsRecord(
                date=date(2026, 3, 10),
                published_at=datetime(2026, 3, 10, 8, 0, tzinfo=tz),
                headline="경계일 기사",
                url="https://example.com/2",
                source="매체B",
            ),
        ],
        required_start=date(2026, 3, 10),
        oldest_kept=date(2026, 3, 10),
        fetched_pages=200,
        reached_page_cap=True,
        last_in_window_date=date(2026, 3, 10),
    )

    import asyncio

    asyncio.run(
        store.capture_archive_window(
            provider=_StubArchiveProvider(result),
            ticker="005930",
            as_of=date(2026, 3, 12),
            lookback_days=3,
        )
    )
    window = store.build_window(ticker="005930", as_of=date(2026, 3, 12), lookback_days=3)

    assert [item.headline for item in window.items] == ["당일 기사", "경계일 기사"]
    assert window.coverage.missing_capture_days == []
    assert window.coverage.page_cap_hit_days == ["2026-03-10"]
    assert (tmp_path / "catalogs" / "kr" / "news" / "2026-03-11" / "005930.meta.json").exists()


def test_catalog_store_marks_missing_days_when_not_materialized(tmp_path: Path) -> None:
    store = KrNewsCatalogStore(tmp_path)
    window = store.build_window(ticker="005930", as_of=date(2026, 3, 12), lookback_days=2)

    assert window.coverage.status == "degraded"
    assert window.coverage.missing_capture_days == ["2026-03-11", "2026-03-12"]
