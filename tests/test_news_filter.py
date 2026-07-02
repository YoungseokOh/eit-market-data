from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from eit_market_data.kr.naver_news_provider import NaverArchiveNewsRecord
from eit_market_data.kr.news_filter import (
    NewsFilterConfig,
    filter_news_window,
)

_TZ = timezone(timedelta(hours=9))


def _record(
    *,
    day: date,
    hour: int,
    headline: str,
    source: str = "매체X",
    url: str | None = None,
) -> NaverArchiveNewsRecord:
    return NaverArchiveNewsRecord(
        date=day,
        published_at=datetime(day.year, day.month, day.day, hour, 0, tzinfo=_TZ),
        headline=headline,
        url=url or f"https://example.com/{headline}-{hour}",
        source=source,
    )


def test_dedup_collapses_same_normalized_title() -> None:
    as_of = date(2026, 3, 12)
    records = [
        _record(day=as_of, hour=9, headline="[특징주] 삼성전자, 신제품 공개", source="연합뉴스"),
        _record(day=as_of, hour=11, headline="삼성전자 신제품 공개!!", source="한국경제"),
        _record(day=as_of, hour=13, headline="[속보] 삼성전자   신제품 공개", source="매일경제"),
    ]

    result = filter_news_window(records, as_of=as_of, company_name="삼성전자")

    assert len(result) == 1
    rep = result[0]
    assert rep.cluster_size == 3
    # Representative is the EARLIEST published_at (PIT-safe, drops republished copies).
    assert rep.record.published_at.hour == 9
    assert rep.record.source == "연합뉴스"


def test_pit_drops_future_dated_articles() -> None:
    as_of = date(2026, 3, 12)
    records = [
        _record(day=as_of, hour=9, headline="오늘 기사"),
        _record(day=date(2026, 3, 13), hour=9, headline="미래 기사"),
        _record(day=date(2026, 3, 20), hour=9, headline="더 먼 미래 기사"),
    ]

    result = filter_news_window(records, as_of=as_of)

    headlines = {item.record.headline for item in result}
    assert headlines == {"오늘 기사"}


def test_per_day_cap_limits_articles() -> None:
    as_of = date(2026, 3, 12)
    records = [
        _record(day=as_of, hour=h, headline=f"서로 다른 기사 {h}")
        for h in range(8)
    ]

    config = NewsFilterConfig(per_day_cap=3, top_k=100)
    result = filter_news_window(records, as_of=as_of, config=config)

    assert len(result) == 3


def test_ranking_orders_by_score() -> None:
    as_of = date(2026, 3, 12)
    records = [
        _record(day=as_of, hour=9, headline="평범한 소식", source="무명매체"),
        _record(day=as_of, hour=10, headline="삼성전자 대규모 수주 공시", source="연합뉴스"),
        _record(day=as_of, hour=11, headline="삼성전자 신제품 발표", source="무명매체"),
    ]

    result = filter_news_window(records, as_of=as_of, company_name="삼성전자")

    scores = [item.relevance for item in result]
    assert scores == sorted(scores, reverse=True)
    # Whitelisted source + company + two event keywords (수주, 공시) ranks first.
    assert result[0].record.headline == "삼성전자 대규모 수주 공시"
    assert result[0].relevance > result[-1].relevance


def test_top_k_truncates_window() -> None:
    as_of = date(2026, 3, 12)
    records = []
    for offset in range(10):
        day = as_of - timedelta(days=offset)
        records.append(_record(day=day, hour=9, headline=f"고유 기사 {offset}"))

    config = NewsFilterConfig(per_day_cap=10, top_k=4)
    result = filter_news_window(records, as_of=as_of, config=config)

    assert len(result) == 4


def test_deterministic_output() -> None:
    as_of = date(2026, 3, 12)
    records = [
        _record(day=as_of, hour=h, headline=f"결정적 기사 {h}", source="연합뉴스")
        for h in range(5)
    ]

    first = filter_news_window(records, as_of=as_of)
    second = filter_news_window(list(reversed(records)), as_of=as_of)

    assert [i.record.headline for i in first] == [i.record.headline for i in second]
