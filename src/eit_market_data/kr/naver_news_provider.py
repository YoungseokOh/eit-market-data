"""Naver Finance Korean news provider."""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import date, timedelta
from typing import Any

from eit_market_data.kr.market_helpers import normalize_ticker
from eit_market_data.schemas.snapshot import NewsItem

logger = logging.getLogger(__name__)

_NAVER_MAIN_NEWS_URL = "https://finance.naver.com/item/main.nhn?code={ticker}"
_DATE_PATTERN = re.compile(r"(\d{4})\.(\d{2})\.(\d{2})")
_MONTH_DAY_PATTERN = re.compile(r"(\d{2})/(\d{2})")


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

                if news_date > as_of:
                    continue
                if news_date < cutoff_date:
                    continue

                news_items.append(
                    NewsItem(
                        headline=headline,
                        date=news_date,
                        source="Naver",
                    )
                )

                if len(news_items) >= 15:
                    break

        except requests.RequestException as e:
            logger.warning("Request failed for Naver news %s: %s", ticker, e)
        except Exception as e:
            logger.warning("Error parsing Naver news for %s: %s", ticker, e)

        return news_items
