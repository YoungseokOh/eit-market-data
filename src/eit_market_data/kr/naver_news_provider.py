"""Naver Finance Korean news provider."""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from typing import Any
from urllib.parse import urljoin

from eit_market_data.kr.market_helpers import normalize_ticker
from eit_market_data.schemas.snapshot import NewsItem

logger = logging.getLogger(__name__)

_NAVER_MAIN_NEWS_URL = "https://finance.naver.com/item/main.nhn?code={ticker}"
_NAVER_NEWS_PAGE_URL = "https://finance.naver.com/item/news.naver?code={ticker}"
_NAVER_ARCHIVE_URL = "https://finance.naver.com/item/news_news.naver?code={ticker}&page={page}"
_NAVER_BASE_URL = "https://finance.naver.com"
_SEOUL = timezone(timedelta(hours=9))
_DATE_PATTERN = re.compile(r"(\d{4})\.(\d{2})\.(\d{2})(?:\s+(\d{2}):(\d{2}))?")
_MONTH_DAY_PATTERN = re.compile(r"(\d{2})/(\d{2})")


def _combine_with_default_time(value: date, *, hour: int = 0, minute: int = 0) -> datetime:
    return datetime.combine(value, datetime_time(hour, minute), tzinfo=_SEOUL)


def _parse_naver_date(date_str: str, as_of: date) -> date | None:
    """Parse date strings from Naver Finance relative to the requested as_of."""
    if not date_str:
        return None

    date_str = date_str.strip()

    # Try YYYY.MM.DD format
    match = _DATE_PATTERN.match(date_str)
    if match:
        try:
            return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        except ValueError:
            return None

    # Naver main page uses MM/DD for recent headlines.
    match = _MONTH_DAY_PATTERN.match(date_str)
    if match:
        month = int(match.group(1))
        day = int(match.group(2))
        try:
            parsed = date(as_of.year, month, day)
        except ValueError:
            return None
        if parsed > as_of:
            try:
                return date(as_of.year - 1, month, day)
            except ValueError:
                return None
        return parsed

    # Try "N분전", "N시간전", "N일전" formats
    if "분전" in date_str:
        match = re.search(r"(\d+)분전", date_str)
        if match:
            return as_of
    elif "시간전" in date_str:
        match = re.search(r"(\d+)시간전", date_str)
        if match:
            return as_of
    elif "일전" in date_str:
        match = re.search(r"(\d+)일전", date_str)
        if match:
            days = int(match.group(1))
            return as_of - timedelta(days=days)

    return None


def _parse_naver_timestamp(date_str: str, as_of: date) -> datetime | None:
    """Parse Naver timestamps into timezone-aware datetimes when possible."""
    if not date_str:
        return None

    date_str = date_str.strip()
    match = _DATE_PATTERN.match(date_str)
    if match:
        parsed_date = _parse_naver_date(date_str, as_of)
        if parsed_date is None:
            return None
        hour = int(match.group(4) or 0)
        minute = int(match.group(5) or 0)
        return _combine_with_default_time(parsed_date, hour=hour, minute=minute)

    parsed_date = _parse_naver_date(date_str, as_of)
    if parsed_date is None:
        return None
    return _combine_with_default_time(parsed_date)


def _apply_response_encoding(response: Any) -> None:
    content_type = str(response.headers.get("content-type", "")).lower()
    if "charset=" in content_type:
        charset = content_type.split("charset=", 1)[1].split(";", 1)[0].strip()
        if charset:
            response.encoding = charset
            return

    apparent = getattr(response, "apparent_encoding", "")
    if apparent:
        response.encoding = apparent
        return

    if not getattr(response, "encoding", ""):
        response.encoding = "euc-kr"


@dataclass(frozen=True)
class NaverArchiveNewsRecord:
    date: date
    published_at: datetime | None
    headline: str
    url: str
    source: str = "Naver"


@dataclass(frozen=True)
class NaverArchiveFetchResult:
    records: list[NaverArchiveNewsRecord]
    required_start: date
    oldest_kept: date | None
    fetched_pages: int
    reached_page_cap: bool
    last_in_window_date: date | None


class NaverNewsProvider:
    """Fetch Korean stock news from Naver Finance via web scraping."""

    def __init__(self) -> None:
        self._semaphore = asyncio.Semaphore(4)

    async def fetch_news(
        self, ticker: str, as_of: date, lookback_days: int = 30
    ) -> list[NewsItem]:
        """Fetch news for a Korean stock ticker from Naver Finance."""
        norm_ticker = normalize_ticker(ticker)
        async with self._semaphore:
            try:
                return await asyncio.to_thread(
                    self._fetch_news_sync, norm_ticker, as_of, lookback_days
                )
            except Exception as e:
                logger.warning("Naver news fetch failed for %s: %s", norm_ticker, e)
                return []

    def _fetch_news_sync(
        self, ticker: str, as_of: date, lookback_days: int
    ) -> list[NewsItem]:
        """Synchronously fetch news from the Naver Finance main-page news block."""
        try:
            import requests
            from bs4 import BeautifulSoup
        except ImportError as e:
            logger.warning("requests/BeautifulSoup required for Naver news: %s", e)
            return []

        cutoff_date = as_of - timedelta(days=lookback_days)
        news_items: list[NewsItem] = []
        seen_links: set[str] = set()

        try:
            url = _NAVER_MAIN_NEWS_URL.format(ticker=ticker)
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            _apply_response_encoding(response)
            soup = BeautifulSoup(response.text, "html.parser")

            container = soup.select_one("div.sub_section.news_section")
            if container is None:
                logger.debug("No news section found for %s", ticker)
                return []

            for row in container.find_all("li"):
                headline_elem = row.select_one('span.txt > a[href*="/item/news_read.naver"]')
                if headline_elem is None:
                    continue

                href = str(headline_elem.get("href", "")).strip()
                if not href or href in seen_links:
                    continue
                seen_links.add(href)

                headline = headline_elem.get_text(" ", strip=True)
                if not headline:
                    continue

                date_elem = row.find("em")
                date_text = date_elem.get_text(" ", strip=True) if date_elem else ""
                if not date_text:
                    continue

                news_date = _parse_naver_date(date_text, as_of)
                if news_date is None:
                    continue
                published_at = _parse_naver_timestamp(date_text, as_of)

                if news_date > as_of:
                    continue
                if news_date < cutoff_date:
                    continue

                news_items.append(
                    NewsItem(
                        headline=headline,
                        date=news_date,
                        published_at=published_at,
                        source="Naver",
                        url=urljoin(_NAVER_BASE_URL, href),
                    )
                )

                if len(news_items) >= 15:
                    break

        except requests.RequestException as e:
            logger.warning("Request failed for Naver news %s: %s", ticker, e)
        except Exception as e:
            logger.warning("Error parsing Naver news for %s: %s", ticker, e)

        return news_items


class NaverArchiveNewsProvider:
    """Fetch windowed Korean stock news from Naver Finance archive pages."""

    def __init__(
        self,
        *,
        max_pages: int = 200,
        page_delay_seconds: float = 0.3,
        timeout_seconds: int = 10,
        require_full_coverage: bool = False,
        raise_on_error: bool = False,
    ) -> None:
        self._semaphore = asyncio.Semaphore(2)
        self._max_pages = max_pages
        self._page_delay_seconds = page_delay_seconds
        self._timeout_seconds = timeout_seconds
        self._require_full_coverage = require_full_coverage
        self._raise_on_error = raise_on_error

    async def fetch_news(
        self,
        ticker: str,
        as_of: date,
        lookback_days: int = 30,
    ) -> list[NewsItem]:
        records = await self.fetch_archive_records(
            ticker=ticker,
            as_of=as_of,
            lookback_days=lookback_days,
        )
        return [
            NewsItem(
                headline=record.headline,
                date=record.date,
                published_at=record.published_at,
                source=record.source,
                url=record.url,
            )
            for record in records
        ]

    async def fetch_archive_records(
        self,
        ticker: str,
        as_of: date,
        lookback_days: int = 30,
    ) -> list[NaverArchiveNewsRecord]:
        result = await self.fetch_archive_result(
            ticker=ticker,
            as_of=as_of,
            lookback_days=lookback_days,
        )
        return result.records

    async def fetch_archive_result(
        self,
        ticker: str,
        as_of: date,
        lookback_days: int = 30,
    ) -> NaverArchiveFetchResult:
        norm_ticker = normalize_ticker(ticker)
        async with self._semaphore:
            try:
                return await asyncio.to_thread(
                    self._fetch_archive_result_sync,
                    norm_ticker,
                    as_of,
                    lookback_days,
                )
            except Exception as e:
                logger.warning("Naver archive fetch failed for %s: %s", norm_ticker, e)
                if self._raise_on_error:
                    raise
                return NaverArchiveFetchResult(
                    records=[],
                    required_start=as_of - timedelta(days=max(lookback_days - 1, 0)),
                    oldest_kept=None,
                    fetched_pages=0,
                    reached_page_cap=False,
                    last_in_window_date=None,
                )

    def _fetch_archive_records_sync(
        self,
        ticker: str,
        as_of: date,
        lookback_days: int,
    ) -> list[NaverArchiveNewsRecord]:
        result = self._fetch_archive_result_sync(ticker, as_of, lookback_days)
        return result.records

    def _fetch_archive_result_sync(
        self,
        ticker: str,
        as_of: date,
        lookback_days: int,
    ) -> NaverArchiveFetchResult:
        try:
            import requests
            from bs4 import BeautifulSoup
            import time
        except ImportError as e:
            logger.warning("requests/BeautifulSoup required for Naver archive news: %s", e)
            required_start = as_of - timedelta(days=max(lookback_days - 1, 0))
            return NaverArchiveFetchResult(
                records=[],
                required_start=required_start,
                oldest_kept=None,
                fetched_pages=0,
                reached_page_cap=False,
                last_in_window_date=None,
            )

        required_start = as_of - timedelta(days=max(lookback_days - 1, 0))
        session = requests.Session()
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
            ),
            "Referer": _NAVER_NEWS_PAGE_URL.format(ticker=ticker),
        }

        seen_urls: set[str] = set()
        records: list[NaverArchiveNewsRecord] = []
        last_page_signature: tuple[str, ...] | None = None
        oldest_kept: date | None = None
        fetched_pages = 0
        reached_page_cap = False
        last_in_window_date: date | None = None
        terminated_early = False

        try:
            for page in range(1, self._max_pages + 1):
                fetched_pages = page
                response = session.get(
                    _NAVER_ARCHIVE_URL.format(ticker=ticker, page=page),
                    headers=headers,
                    timeout=self._timeout_seconds,
                )
                response.raise_for_status()
                _apply_response_encoding(response)
                soup = BeautifulSoup(response.text, "html.parser")

                page_records = self._extract_archive_page(soup, as_of)
                page_signature = tuple(record.url for record in page_records[:10])
                if not page_records:
                    terminated_early = True
                    break
                if last_page_signature is not None and page_signature == last_page_signature:
                    terminated_early = True
                    break
                last_page_signature = page_signature

                page_has_target_month = False
                page_has_older_rows = False
                page_window_dates: list[date] = []
                for record in page_records:
                    if record.date > as_of:
                        continue
                    if record.date < required_start:
                        page_has_older_rows = True
                        continue
                    page_window_dates.append(record.date)
                    if record.url in seen_urls:
                        continue
                    seen_urls.add(record.url)
                    records.append(record)
                    oldest_kept = record.date if oldest_kept is None else min(oldest_kept, record.date)
                    page_has_target_month = True
                if page_window_dates:
                    last_in_window_date = min(page_window_dates)

                if not page_has_target_month and page_has_older_rows:
                    terminated_early = True
                    break
                if self._page_delay_seconds > 0:
                    time.sleep(self._page_delay_seconds)
            reached_page_cap = fetched_pages == self._max_pages and not terminated_early
        except requests.RequestException as e:
            logger.warning("Request failed for Naver archive news %s: %s", ticker, e)
        except Exception as e:
            logger.warning("Error parsing Naver archive news for %s: %s", ticker, e)
        finally:
            session.close()

        records.sort(
            key=lambda item: (
                item.published_at or _combine_with_default_time(item.date),
                item.headline,
            ),
            reverse=True,
        )
        if self._require_full_coverage and records and (oldest_kept is None or oldest_kept > required_start):
            raise RuntimeError(
                f"Naver archive did not reach required start date for {ticker}: "
                f"oldest_kept={oldest_kept} required_start={required_start}"
            )
        return NaverArchiveFetchResult(
            records=records,
            required_start=required_start,
            oldest_kept=oldest_kept,
            fetched_pages=fetched_pages,
            reached_page_cap=reached_page_cap,
            last_in_window_date=last_in_window_date,
        )

    def _extract_archive_page(
        self,
        soup: Any,
        as_of: date,
    ) -> list[NaverArchiveNewsRecord]:
        rows = soup.select("table.type5 tr")
        records: list[NaverArchiveNewsRecord] = []

        for row in rows:
            headline_elem = row.select_one('td.title a[href*="/item/news_read.naver"]')
            date_elem = row.select_one("td.date")
            source_elem = row.select_one("td.info")
            if headline_elem is None or date_elem is None:
                continue

            href = str(headline_elem.get("href", "")).strip()
            if not href:
                continue

            headline = headline_elem.get_text(" ", strip=True)
            if not headline:
                continue

            news_date = _parse_naver_date(date_elem.get_text(" ", strip=True), as_of)
            if news_date is None:
                continue
            published_at = _parse_naver_timestamp(date_elem.get_text(" ", strip=True), as_of)

            source = source_elem.get_text(" ", strip=True) if source_elem else "Naver"
            records.append(
                NaverArchiveNewsRecord(
                    date=news_date,
                    published_at=published_at,
                    headline=headline,
                    source=source or "Naver",
                    url=urljoin(_NAVER_BASE_URL, href),
                )
            )

        return records
