"""SEC EDGAR filing provider.

Implements FilingProvider using the free SEC EDGAR API to fetch
10-K annual report text sections (Item 1, 1A, 7, 10/14).

Requires ``SEC_EDGAR_USER_AGENT`` environment variable in the format:
    "YourName your@email.com"

SEC EDGAR rate limit: 10 requests/second. We use a semaphore + delay.
"""

from __future__ import annotations

import asyncio
import html as html_module
import logging
import os
import re
from datetime import date

from eit_market_data.schemas.snapshot import FilingData

logger = logging.getLogger(__name__)

# Rate limiter for SEC EDGAR (max ~5 concurrent to stay well under 10/s)
_EDGAR_SEMAPHORE = asyncio.Semaphore(5)
_EDGAR_DELAY = 0.2  # seconds between requests


def _get_user_agent() -> str:
    """Get SEC EDGAR User-Agent from environment."""
    ua = os.environ.get("SEC_EDGAR_USER_AGENT", "")
    if not ua:
        raise ValueError(
            "SEC_EDGAR_USER_AGENT environment variable is required. "
            "Set it to 'YourName your@email.com' in .env"
        )
    return ua


def _get_httpx_client():  # noqa: ANN202
    """Create an httpx AsyncClient with SEC EDGAR headers."""
    import httpx

    return httpx.AsyncClient(
        headers={
            "User-Agent": _get_user_agent(),
            "Accept-Encoding": "gzip, deflate",
        },
        timeout=30.0,
        follow_redirects=True,
    )


async def _rate_limited_get(client, url: str) -> str | None:
    """GET with rate limiting for SEC EDGAR."""
    async with _EDGAR_SEMAPHORE:
        try:
            resp = await client.get(url)
            resp.raise_for_status()
            await asyncio.sleep(_EDGAR_DELAY)
            return resp.text
        except Exception as e:
            logger.warning("EDGAR fetch failed for %s: %s", url, e)
            await asyncio.sleep(_EDGAR_DELAY)
            return None


# ---------------------------------------------------------------------------
# CIK lookup
# ---------------------------------------------------------------------------

_CIK_CACHE: dict[str, str] = {}


async def _ticker_to_cik(client, ticker: str) -> str | None:
    """Convert ticker to CIK (Central Index Key)."""
    if ticker in _CIK_CACHE:
        return _CIK_CACHE[ticker]

    url = "https://www.sec.gov/files/company_tickers.json"
    text = await _rate_limited_get(client, url)
    if not text:
        return None

    import json

    try:
        data = json.loads(text)
        for _key, entry in data.items():
            t = entry.get("ticker", "").upper()
            cik = str(entry.get("cik_str", ""))
            _CIK_CACHE[t] = cik.zfill(10)

        return _CIK_CACHE.get(ticker.upper())
    except Exception as e:
        logger.warning("Failed to parse CIK data: %s", e)
        return None


# ---------------------------------------------------------------------------
# Filing lookup
# ---------------------------------------------------------------------------


async def _find_10k_url(
    client, cik: str, as_of: date
) -> tuple[str | None, date | None]:
    """Find the most recent 10-K filing URL filed on or before as_of."""
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    text = await _rate_limited_get(client, url)
    if not text:
        return None, None

    import json

    try:
        data = json.loads(text)
        recent = data.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        accessions = recent.get("accessionNumber", [])
        primary_docs = recent.get("primaryDocument", [])

        for i, form in enumerate(forms):
            if form not in ("10-K", "10-K/A"):
                continue
            filing_date = date.fromisoformat(dates[i])
            if filing_date > as_of:
                continue

            # Build document URL
            acc = accessions[i].replace("-", "")
            doc = primary_docs[i]
            doc_url = (
                f"https://www.sec.gov/Archives/edgar/data/"
                f"{cik.lstrip('0')}/{acc}/{doc}"
            )
            return doc_url, filing_date

    except Exception as e:
        logger.warning("Failed to parse filing index for CIK %s: %s", cik, e)

    return None, None


# ---------------------------------------------------------------------------
# HTML → text extraction
# ---------------------------------------------------------------------------

_SECTION_PATTERNS: dict[str, list[re.Pattern]] = {
    "business_overview": [
        re.compile(
            r"item\s*1[.\s]*(?:business|description)",
            re.IGNORECASE,
        ),
    ],
    "risks": [
        re.compile(r"item\s*1a[.\s]*risk\s*factors?", re.IGNORECASE),
    ],
    "mda": [
        re.compile(
            r"item\s*7[.\s]*management.{0,30}discussion",
            re.IGNORECASE,
        ),
    ],
    "governance": [
        re.compile(
            r"item\s*(?:10|14)[.\s]*(?:directors|corporate\s*governance)",
            re.IGNORECASE,
        ),
    ],
}


def _strip_html(raw: str) -> str:
    """Remove HTML tags and decode entities."""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(raw, "html.parser")
    text = soup.get_text(separator="\n")
    text = html_module.unescape(text)
    # Collapse whitespace
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()


def _extract_sections(html_text: str, max_chars: int = 8000) -> dict[str, str]:
    """Extract 10-K sections using regex pattern matching.

    A best-effort approach: find section headers and extract text
    between them.
    """
    plain = _strip_html(html_text)
    sections: dict[str, str] = {}

    for section_name, patterns in _SECTION_PATTERNS.items():
        for pattern in patterns:
            matches = list(pattern.finditer(plain))
            if not matches:
                continue

            start = matches[0].end()

            # Find the next "Item N" header to determine section end
            next_item = re.search(
                r"\n\s*item\s*\d+[a-z]?[.\s]",
                plain[start:],
                re.IGNORECASE,
            )
            end = start + next_item.start() if next_item else min(start + max_chars, len(plain))

            section_text = plain[start:end].strip()
            if len(section_text) > max_chars:
                section_text = section_text[:max_chars]

            if len(section_text) > 50:  # Skip trivially short extractions
                sections[section_name] = section_text
                break

    return sections


# ---------------------------------------------------------------------------
# Provider class
# ---------------------------------------------------------------------------


class EdgarFilingProvider:
    """FilingProvider implementation using SEC EDGAR.

    Fetches 10-K filings and extracts text sections for
    business overview, risk factors, MD&A, and governance.

    Requires ``SEC_EDGAR_USER_AGENT`` environment variable.
    Install with: ``pip install httpx beautifulsoup4``
    """

    async def fetch_filing(self, ticker: str, as_of: date) -> FilingData:
        """Fetch the most recent 10-K filing for the ticker."""
        try:
            return await self._fetch_filing_impl(ticker, as_of)
        except Exception as e:
            logger.warning(
                "EDGAR filing fetch failed for %s: %s — returning empty",
                ticker,
                e,
            )
            return FilingData(ticker=ticker)

    async def _fetch_filing_impl(self, ticker: str, as_of: date) -> FilingData:
        async with _get_httpx_client() as client:
            # Step 1: ticker → CIK
            cik = await _ticker_to_cik(client, ticker)
            if not cik:
                logger.info("No CIK found for %s", ticker)
                return FilingData(ticker=ticker)

            # Step 2: find 10-K URL
            doc_url, filing_date = await _find_10k_url(client, cik, as_of)
            if not doc_url:
                logger.info("No 10-K found for %s before %s", ticker, as_of)
                return FilingData(ticker=ticker)

            # Step 3: download filing document
            html_text = await _rate_limited_get(client, doc_url)
            if not html_text:
                return FilingData(ticker=ticker, filing_date=filing_date)

            # Step 4: extract sections
            sections = _extract_sections(html_text)

            return FilingData(
                ticker=ticker,
                filing_date=filing_date,
                filing_type="10-K",
                business_overview=sections.get("business_overview"),
                risks=sections.get("risks"),
                mda=sections.get("mda"),
                governance=sections.get("governance"),
            )
