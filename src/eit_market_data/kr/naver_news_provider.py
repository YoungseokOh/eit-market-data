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


def _parse_naver_date(date_str: str) -> date | None:
    """Parse date strings from Naver Finance (YYYY.MM.DD or relative dates)."""
    if not date_str:
        return None

    date_str = date_str.strip()

    # Try YYYY.MM.DD format
    match = re.match(r"(\d{4})\.(\d{2})\.(\d{2})", date_str)
    if match:
        try:
            return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        except ValueError:
            return None

    # Try "N분전", "N시간전", "N일전" formats
    if "분전" in date_str:
        match = re.search(r"(\d+)분전", date_str)
        if match:
            return date.today()
    elif "시간전" in date_str:
        match = re.search(r"(\d+)시간전", date_str)
        if match:
            return date.today()
    elif "일전" in date_str:
        match = re.search(r"(\d+)일전", date_str)
        if match:
            days = int(match.group(1))
            return date.today() - timedelta(days=days)

    return None


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
        """Synchronously fetch news from Naver Finance."""
        try:
            import requests
            from bs4 import BeautifulSoup
        except ImportError as e:
            logger.warning("requests/BeautifulSoup required for Naver news: %s", e)
            return []

        cutoff_date = as_of - timedelta(days=lookback_days)
        news_items: list[NewsItem] = []

        try:
            url = f"https://finance.naver.com/item/news_news.nhn?code={ticker}&page=1"
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
            response = requests.get(url, headers=headers, timeout=10)
            response.encoding = "utf-8"
            soup = BeautifulSoup(response.text, "html.parser")

            # Find news items in the table
            # Naver Finance typically uses <table> with <tr> rows for news
            table = soup.find("table", {"class": "type5"})
            if table is None:
                # Try alternative class
                table = soup.find("table")

            if table is None:
                logger.debug("No news table found for %s", ticker)
                return []

            rows = table.find_all("tr")
            for row in rows:
                # Skip header rows
                if row.find("th"):
                    continue

                cells = row.find_all("td")
                if len(cells) < 2:
                    continue

                # Extract headline from first cell
                headline_elem = cells[0].find("a")
                if not headline_elem:
                    continue
                headline = headline_elem.get_text(strip=True)

                # Extract date - usually in second or third column
                date_text = None
                for cell in cells[1:]:
                    text = cell.get_text(strip=True)
                    if re.match(r"\d{4}\.\d{2}\.\d{2}", text) or any(
                        suffix in text for suffix in ["분전", "시간전", "일전"]
                    ):
                        date_text = text
                        break

                if not date_text:
                    continue

                news_date = _parse_naver_date(date_text)
                if news_date is None:
                    continue

                # Filter by as_of (no future articles)
                if news_date > as_of:
                    continue

                # Filter by lookback window
                if news_date < cutoff_date:
                    continue

                # Try to extract source
                source = "Naver"
                for cell in cells:
                    source_elem = cell.find("span", {"class": "press"})
                    if source_elem:
                        source = source_elem.get_text(strip=True)
                        break

                if headline and news_date:
                    news_items.append(
                        NewsItem(
                            headline=headline,
                            date=news_date,
                            source=source,
                        )
                    )

                # Limit to 15 items max
                if len(news_items) >= 15:
                    break

        except requests.RequestException as e:
            logger.warning("Request failed for Naver news %s: %s", ticker, e)
        except Exception as e:
            logger.warning("Error parsing Naver news for %s: %s", ticker, e)

        return news_items
