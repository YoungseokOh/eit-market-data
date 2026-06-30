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


async def _rate_limited_get(client, url: str, attempts: int = 3) -> str | None:
    """GET with rate limiting for SEC EDGAR.

    Retries a few times on transient failures (e.g. HTTP 429/503 or connection
    resets) with a short backoff. SEC sporadically throttles bursts; without a
    retry the first document fetched after the metadata calls can fail and leave
    an otherwise-extractable filing empty.
    """
    async with _EDGAR_SEMAPHORE:
        last_exc: Exception | None = None
        for attempt in range(attempts):
            try:
                resp = await client.get(url)
                resp.raise_for_status()
                await asyncio.sleep(_EDGAR_DELAY)
                return resp.text
            except Exception as e:  # noqa: BLE001
                last_exc = e
                await asyncio.sleep(_EDGAR_DELAY * (attempt + 2))
        logger.warning("EDGAR fetch failed for %s: %s", url, last_exc)
        return None


# ---------------------------------------------------------------------------
# CIK lookup
# ---------------------------------------------------------------------------

_CIK_CACHE: dict[str, str] = {}

# A few active filers are absent from SEC's ``company_tickers.json`` /
# ``company_tickers_exchange.json`` maps (their submission ``tickers`` list is
# empty). These verified ticker→CIK mappings let us still resolve them.
# CTRA = Coterra Energy Inc. (legacy Cabot Oil & Gas; files 10-K as "cog-*.htm").
_CIK_OVERRIDES: dict[str, str] = {
    "CTRA": "0000858470",
}


async def _ticker_to_cik(client, ticker: str) -> str | None:
    """Convert ticker to CIK (Central Index Key)."""
    key = ticker.upper()
    if ticker in _CIK_CACHE:
        return _CIK_CACHE[ticker]
    if key in _CIK_OVERRIDES:
        return _CIK_OVERRIDES[key]

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

        return _CIK_CACHE.get(key) or _CIK_OVERRIDES.get(key)
    except Exception as e:
        logger.warning("Failed to parse CIK data: %s", e)
        return _CIK_OVERRIDES.get(key)


# ---------------------------------------------------------------------------
# Filing lookup
# ---------------------------------------------------------------------------


async def _find_annual_filings(
    client, cik: str, as_of: date, limit: int = 3
) -> list[tuple[str, date, str, int | None, str]]:
    """Find up to ``limit`` most-recent distinct annual reports on/before as_of.

    Domestic registrants file ``10-K``; foreign private issuers (ASML, ARM,
    PDD, …) file ``20-F`` instead. We collect the 10-K series if present,
    otherwise fall back to the 20-F series.

    Returns a newest-first list of
    ``(doc_url, filing_date, accession, fiscal_year, form_type)``.
    PIT: never includes a filing with ``filingDate`` after ``as_of``.
    For each fiscal year the *original* report is preferred over a ``/A``
    amendment — amendments are often partial (e.g. Part III only) and omit the
    risk-factor / business text, so selecting them leaves sections empty.
    """
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    text = await _rate_limited_get(client, url)
    if not text:
        return []

    import json

    def _collect(
        recent: dict, primary_form: str
    ) -> list[tuple[str, date, str, int | None, str]]:
        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        accessions = recent.get("accessionNumber", [])
        primary_docs = recent.get("primaryDocument", [])
        report_dates = recent.get("reportDate", [])
        accepted = {primary_form, f"{primary_form}/A"}

        # One entry per fiscal year, preferring the original over an amendment.
        by_year: dict[int, tuple[bool, tuple[str, date, str, int | None, str]]] = {}
        for i, form in enumerate(forms):
            if form not in accepted:
                continue
            filing_date = date.fromisoformat(dates[i])
            if filing_date > as_of:
                continue

            fiscal_year: int | None = None
            if i < len(report_dates) and report_dates[i]:
                try:
                    fiscal_year = date.fromisoformat(report_dates[i]).year
                except ValueError:
                    fiscal_year = None
            if fiscal_year is None:
                fiscal_year = filing_date.year - 1

            doc = primary_docs[i]
            if not doc:
                continue
            acc = accessions[i].replace("-", "")
            doc_url = (
                f"https://www.sec.gov/Archives/edgar/data/"
                f"{cik.lstrip('0')}/{acc}/{doc}"
            )
            is_amendment = form.endswith("/A")
            cand = (doc_url, filing_date, accessions[i], fiscal_year, primary_form)

            existing = by_year.get(fiscal_year)
            if existing is None:
                by_year[fiscal_year] = (is_amendment, cand)
            elif existing[0] and not is_amendment:
                # Replace a previously-stored amendment with the original report.
                by_year[fiscal_year] = (is_amendment, cand)

        ordered = sorted(by_year.items(), key=lambda kv: kv[0], reverse=True)
        return [cand for _year, (_amend, cand) in ordered][:limit]

    _COLUMNS = ("form", "filingDate", "accessionNumber", "primaryDocument", "reportDate")

    async def _collect_with_overflow(
        recent: dict, filings: dict, form: str
    ) -> list[tuple[str, date, str, int | None, str]]:
        results = _collect(recent, form)
        accepted = {form, f"{form}/A"}
        recent_has = any(f in accepted for f in recent.get("form", []))
        # Prolific filers (e.g. Citigroup, Morgan Stanley) push older annual
        # reports out of the ~1000-entry ``recent`` window into paginated
        # ``filings.files`` overflow pages. Only merge those when this filer
        # actually uses the form but ``recent`` has fewer than ``limit`` of it.
        if len(results) >= limit or not recent_has:
            return results
        merged = {k: list(recent.get(k, [])) for k in _COLUMNS}
        for entry in filings.get("files", []):
            name = entry.get("name")
            if not name:
                continue
            otext = await _rate_limited_get(
                client, f"https://data.sec.gov/submissions/{name}"
            )
            if not otext:
                continue
            try:
                odata = json.loads(otext)
            except Exception:  # noqa: BLE001
                continue
            for k in _COLUMNS:
                merged[k].extend(odata.get(k, []))
        return _collect(merged, form)

    try:
        data = json.loads(text)
        filings = data.get("filings", {})
        recent = filings.get("recent", {})
        # Domestic 10-K → foreign-private-issuer 20-F → Canadian MJDS 40-F.
        for form in ("10-K", "20-F", "40-F"):
            results = await _collect_with_overflow(recent, filings, form)
            if results:
                return results
        return []
    except Exception as e:
        logger.warning("Failed to parse filing index for CIK %s: %s", cik, e)
        return []


# ---------------------------------------------------------------------------
# HTML → text extraction
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Section header specifications
#
# Modern 10-K / 20-F filings are inline-XBRL HTML.  Two facts break naive
# phrase matching:
#   1. A Table-of-Contents (and per-page running headers) repeat the section
#      title many times.  A regex match on the ToC yields a tiny stub or the
#      wrong boundary.
#   2. The body header is frequently split across <span>/style tags, e.g.
#      Microsoft renders "ITEM 1A. RISK FACTORS" as "ITEM 1A. RIS\nK FACTORS"
#      (the word "RISK" is broken mid-word), and some issuers use em/en-dash
#      separators ("Item 1A—Risk Factors").
#
# To be robust we match prefixed headers ("Item 1A. Risk Factors") against a
# *whitespace-collapsed* copy of the text (so tag-splitting and odd separators
# no longer matter), and fall back to a *line-anchored* bare-title match
# ("Risk Factors" alone on a line) for issuers that drop the Item prefix in the
# body (e.g. Intel, Citigroup) or that file a 20-F ("D. Risk Factors").
#
# Every candidate header position is then scored by the prose that follows it,
# discarding ToC / cross-reference / running-header hits.
# ---------------------------------------------------------------------------

# Prefixed patterns run against the lowercase, whitespace-stripped "compact"
# text.  ``\s`` is never used here because all whitespace has been removed.
_PREFIXED_PATTERNS: dict[str, list[str]] = {
    "business_overview": [
        r"item1\.?(?:businessoverview|business|descriptionof(?:our)?business)",
        # 20-F equivalent
        r"item4\.?(?:informationonthecompany|businessoverview)",
    ],
    "risks": [
        r"item1a\.?riskfactors?",
        # 20-F equivalent ("Item 3.D Risk Factors")
        r"item3\.?d\.?riskfactors?",
    ],
    "mda": [
        r"item7\.?management[a-z'’]{0,5}discussion",
        # 20-F equivalent
        r"item5\.?operating(?:and)?financialreview",
        r"item5\.?operatingresults",
    ],
    "governance": [
        r"item(?:10|14)\.?(?:directors|corporategovernance)",
        # 20-F equivalent
        r"item6\.?directors,?seniormanagement",
    ],
}

# Bare-title fallbacks run against the (line-structured) plain text.  Each must
# occupy its own line so that inline cross-references ("see Risk Factors below")
# are not matched.
_BARE_PATTERNS: dict[str, list[re.Pattern]] = {
    "business_overview": [
        re.compile(r"(?im)^[ \t>]*(?:[a-d]\.[ \t]*)?information on the company[ \t]*$"),
        re.compile(r"(?im)^[ \t>]*business overview[ \t]*$"),
    ],
    "risks": [
        re.compile(
            r"(?im)^[ \t>]*(?:[a-d]\.[ \t]*)?risk factors"
            r"[ \t]*[.:]?[ \t]*(?:\(continued\))?[ \t]*$"
        ),
        # Some issuers (e.g. SYF) head the body with "Risk Factors Summary" or
        # "Risk Factors Relating to …" rather than a bare "Risk Factors".
        re.compile(r"(?im)^[ \t>]*risk factors\s+(?:summary|relating\b).*$"),
    ],
    "mda": [
        re.compile(r"(?im)^[ \t>]*management['’]?s discussion and analysis.*$"),
        re.compile(r"(?im)^[ \t>]*operating and financial review.*$"),
    ],
    "governance": [
        re.compile(r"(?im)^[ \t>]*corporate governance[ \t]*$"),
        re.compile(r"(?im)^[ \t>]*directors,? senior management.*$"),
    ],
}

# When a section is shorter than ``max_chars`` we trim it at the start of the
# *next* section.  The stop pattern must reference a later item number so that
# a running page header for the current section does not truncate it early.
_STOP_PATTERNS: dict[str, re.Pattern] = {
    "business_overview": re.compile(r"\bitem\s*1a\b", re.IGNORECASE),
    "risks": re.compile(r"\bitem\s*1b\b|\bitem\s*1c\b|\bitem\s*2\b", re.IGNORECASE),
    "mda": re.compile(r"\bitem\s*7a\b|\bitem\s*8\b", re.IGNORECASE),
    "governance": re.compile(r"\bitem\s*11\b|\bitem\s*15\b", re.IGNORECASE),
}

_FUNC_WORDS = re.compile(
    r"\b(?:the|and|to|of|that|our|we|in|or|for|as|be|is|are|may|with|on|by|"
    r"an|its|these|such|which|have|has|could|would|will|not|from)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Risk-content validator
#
# The boundary scorer (``_body_score``) only verifies that a candidate header
# is followed by substantive *prose* (not a ToC / running header). It cannot
# tell whether that prose is genuinely *risk-factor* text. Several 10-K layouts
# put non-risk prose adjacent to a header that the compact matcher latches onto
# — e.g. the "Information About Our Executive Officers" bio table (Item 1 tail),
# the cautionary "forward-looking statements" preamble, or a credit-rating
# legend. Emitting one of those as ``risks`` creates a false filing-diff signal.
# This validator decides whether an extracted ``risks`` body is real risk prose;
# the caller drops it (→ ``None``) when it is not. "Better empty than wrong".
# ---------------------------------------------------------------------------

_RISK_WORDS_RE = re.compile(
    r"\b(?:risk|risks|adversely|adverse|could|may|materially|material|"
    r"uncertaint\w+|harm|harmed|negatively|decline|failure|losses)\b",
    re.IGNORECASE,
)
# Executive-officer bio block (Item 1 tail mis-grabbed as 1A).
_EXEC_BIO_RE = re.compile(
    r"information about our executive officers"
    r"|executive officers of the (?:registrant|company)"
    r"|provides information regarding our executive officers",
    re.IGNORECASE,
)
# Credit-rating scale tokens (a rating legend, not risk prose).
_RATING_TOK_RE = re.compile(r"\b(?:AAA|AA\+|BBB|CCC|BB\+)\b")


def _looks_like_risk_text(text: str | None) -> bool:
    """True if ``text`` reads as genuine 10-K/20-F risk-factor prose.

    Rejects executive-officer bios, the forward-looking-statements preamble, a
    credit-rating legend, and bodies too short or too sparse in risk language to
    be a real Item 1A. Tuned so known-good Item 1A intros (AVGO/MSFT/HD/PEP/V/
    QCOM/TSLA/AMD/C/COST/INTC) pass while the known mis-grabs fail.
    """
    if not text:
        return False
    joined = re.sub(r"\s+", " ", text).strip()
    if len(joined) < 400:
        return False
    head = joined[:600]
    low = joined.lower()
    # Executive-officer bio table (Item 1 tail).
    if _EXEC_BIO_RE.search(head):
        return False
    if re.search(r"\bage\b[\s\S]{0,30}\bposition\b", head, re.IGNORECASE):
        return False
    if low[:120].startswith("available information") or "available information" in low[:120]:
        return False
    # Credit-rating legend (rating tokens clustered with rating vocabulary).
    if len(_RATING_TOK_RE.findall(joined[:1500])) >= 3 and (
        "obligor" in low[:2500]
        or "capacity to" in low[:2500]
        or "creditworthiness" in low[:2500]
    ):
        return False
    risk_hits = len(_RISK_WORDS_RE.findall(joined[:2500]))
    # Forward-looking-statements preamble: opens with the cautionary boilerplate
    # and lacks enumerated-risk density.
    if "forward-looking statements" in low[:250] and risk_hits < 15:
        return False
    if risk_hits < 6:
        return False
    return True


def _strip_html(raw: str) -> str:
    """Remove HTML tags and decode entities."""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(raw, "html.parser")
    text = soup.get_text(separator="\n")
    text = html_module.unescape(text)
    # Normalize non-breaking spaces (\xa0) to regular spaces so that
    # section header patterns (e.g. "Item 1.\xa0\xa0\xa0\xa0Business") match correctly.
    text = text.replace("\xa0", " ")
    # Collapse whitespace
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()


def _compact_with_map(plain: str) -> tuple[str, list[int]]:
    """Return a lowercase, whitespace-free copy of ``plain`` plus an index map.

    ``index_map[j]`` is the offset in ``plain`` of compact character ``j``.
    This lets prefixed header patterns match across inline-XBRL tag splitting
    (e.g. "RIS\\nK FACTORS") while still recovering real text positions.
    """
    chars: list[str] = []
    index_map: list[int] = []
    for i, ch in enumerate(plain):
        # Keep only alphanumerics: dropping whitespace AND punctuation lets the
        # prefixed patterns match across tag-splitting ("RIS\\nK FACTORS") *and*
        # odd separators ("Item 1A—Risk Factors", "Item 1A.Risk Factors").
        if not ch.isalnum():
            continue
        chars.append(ch.lower())
        index_map.append(i)
    return "".join(chars), index_map


def _is_header_like(plain: str, header_start: int) -> bool:
    """True if the text preceding a match looks like a header, not a sentence.

    Inline cross-references ("see Part I, Item 1A, Risk Factors of the …") are
    embedded mid-sentence, so the preceding non-space character is a lowercase
    letter or a comma/quote. A real section header is preceded by a page number,
    newline, period, or "PART I".
    """
    before = plain[max(0, header_start - 40) : header_start].rstrip()
    if not before:
        return True
    last = before[-1]
    # Mid-sentence list cues ("Part I, Item 1A,", a connective, or an open quote).
    if last in ",;:\"'“”’‘":
        return False
    if re.search(r"\b(?:see|refer|under|in|to|and|of|the)$", before, re.IGNORECASE):
        return False
    return True


def _body_score(plain: str, start: int) -> int:
    """Score the text following a candidate header.

    Returns a positive score for substantive section prose and ``-1`` for
    Table-of-Contents entries, cross-reference indexes, running page headers
    pointing at a numeric page, and trivially short stubs.
    """
    seg = plain[start : start + 1500]
    # A Table-of-Contents / index lists short title lines (+ page numbers); real
    # section prose contains at least one long line (a sentence/paragraph).
    line_lengths = [len(ln.strip()) for ln in seg.split("\n") if ln.strip()]
    if not line_lengths or max(line_lengths) < 45:
        return -1
    joined = re.sub(r"\s+", " ", seg).strip()
    if len(joined) < 120:
        return -1
    alpha = sum(c.isalpha() for c in joined)
    if alpha < 200:
        return -1
    # A section divider / running nav banner opens with a run of ALL-CAPS
    # headings (e.g. "STRATEGIC REPORT CORPORATE GOVERNANCE SUSTAINABILITY …").
    caps_run = 0
    for w in re.findall(r"[A-Za-z]{2,}", joined[:140]):
        if w.isupper() and len(w) >= 3:
            caps_run += 1
            if caps_run >= 4:
                return -1
        else:
            caps_run = 0
    # Real prose flows as sentences/clauses: lowercase words followed by
    # ./,/; + space. Table-of-Contents and cross-reference indexes are lists of
    # Title-Case headings + page numbers and have almost none of this.
    if len(re.findall(r"[a-z]{3,}[.,;]\s", joined)) < 3:
        return -1
    # A ToC / cross-reference index lists several "Item N" tokens up front,
    # or bare item-number tokens ("1B.", "1C.", "2.") and page ranges.
    if len(re.findall(r"item\s*\d", joined, re.IGNORECASE)) >= 3:
        return -1
    if len(re.findall(r"(?<!\w)\d{1,2}[A-Da-d]?\.\s", joined)) >= 3:
        return -1
    if len(re.findall(r"\b\d{1,3}\s*[–—-]\s*\d{2,}", joined)) >= 2:
        return -1
    digits = sum(c.isdigit() for c in joined)
    if digits > alpha:  # numeric table / financial-data index
        return -1
    func_hits = len(_FUNC_WORDS.findall(joined))
    if func_hits < 8:  # ToC entries are noun-phrase titles with few function words
        return -1
    return alpha + func_hits * 5


def _section_end(plain: str, start: int, section_name: str, max_chars: int) -> int:
    """Determine the end offset for a section body starting at ``start``."""
    limit = min(start + max_chars, len(plain))
    stop = _STOP_PATTERNS.get(section_name)
    if stop is not None:
        # Skip a little past the header itself before looking for the next section.
        m = stop.search(plain, start + 80, limit)
        if m is not None:
            return m.start()
    return limit


def _extract_sections(html_text: str, max_chars: int = 8000) -> dict[str, str]:
    """Extract 10-K / 20-F sections, robust to inline-XBRL formatting.

    For each section the real body header is located by collecting every
    candidate position (prefixed matches against a whitespace-collapsed copy,
    plus line-anchored bare-title fallbacks), scoring the prose that follows,
    and choosing the earliest substantive hit — preferring prefixed matches.
    """
    plain = _strip_html(html_text)
    compact, index_map = _compact_with_map(plain)
    sections: dict[str, str] = {}

    for section_name in _PREFIXED_PATTERNS:
        # candidate = (tier, start_offset_in_plain); tier 0 = prefixed (preferred)
        candidates: list[tuple[int, int]] = []

        for pat in _PREFIXED_PATTERNS[section_name]:
            for m in re.finditer(pat, compact):
                if m.end() == 0:
                    continue
                header_start = index_map[m.start()]
                if not _is_header_like(plain, header_start):
                    continue
                # Body begins just after the matched header in the plain text.
                start = index_map[m.end() - 1] + 1
                candidates.append((0, start))

        # Bare-title matches are already line-anchored (the title occupies its
        # own line), so they need no preceding-context check — unlike prefixed
        # matches, which can land on inline cross-references.
        for pattern in _BARE_PATTERNS.get(section_name, []):
            for m in pattern.finditer(plain):
                candidates.append((1, m.end()))

        if not candidates:
            continue

        # Rank qualifying candidates. A Table-of-Contents entry or a running
        # page header begins with a page number ("49 SUSTAINABILITY", "37 We
        # have…") and is discarded — the *true* section start begins directly
        # with prose. Among the rest, prefer prefixed over bare, then the
        # earliest occurrence (the section's beginning, not a per-page repeat).
        ranked: list[tuple[int, int, str]] = []
        for tier, start in sorted(set(candidates)):
            if _body_score(plain, start) <= 0:
                continue
            end = _section_end(plain, start, section_name, max_chars)
            text = plain[start:end].strip()
            if len(text) <= 300:
                continue
            # A real header is not quoted; a body that opens with a closing
            # quote/paren came from an inline reference to a quoted title
            # (e.g. 'Item 1A, "Risk Factors" included elsewhere …').
            if text[:1] in "”\"’')]":
                continue
            # Number-leading bodies are ToC rows / running page headers, never a
            # real section start.
            if re.match(r"\d", text):
                continue
            body = text[:max_chars]
            # Content validation for the risk section: skip a candidate whose
            # body is an exec-officer bio, the forward-looking preamble, or a
            # rating legend (all of which can sit next to a "risk factors"
            # token). If no candidate passes, ``risks`` is left unset (None) —
            # an empty section is safer than a wrong one for filing diffs.
            if section_name == "risks" and not _looks_like_risk_text(body):
                continue
            ranked.append((tier, start, body))

        if ranked:
            ranked.sort(key=lambda r: (r[0], r[1]))
            sections[section_name] = ranked[0][2]

    return sections


# ---------------------------------------------------------------------------
# Per-accession section cache
# ---------------------------------------------------------------------------

# 10-K text/sections never change after filing, so cache the extracted sections
# keyed by accession to avoid re-downloading the same document across months and
# tickers during the trailing-history backfill.
_ACCESSION_SECTION_CACHE: dict[str, dict[str, str]] = {}


async def _largest_alt_document(client, doc_url: str) -> str | None:
    """Return the URL of the largest *body-like* HTML document in the accession.

    Used as a fallback: some registrants (notably large banks — WFC, USB, BNY,
    SYF) point ``primaryDocument`` at a thin cover page that incorporates the
    real 10-K body by reference. The full filing lives in another HTML document
    of the same accession (typically the largest one). XBRL viewer renders
    (``R\\d+.htm``) and the original primary document are excluded.
    """
    base, primary = doc_url.rsplit("/", 1)
    text = await _rate_limited_get(client, f"{base}/index.json")
    if not text:
        return None

    import json

    try:
        items = json.loads(text).get("directory", {}).get("item", [])
    except Exception:
        return None

    best: str | None = None
    best_size = 0
    for it in items:
        name = str(it.get("name", ""))
        low = name.lower()
        if not low.endswith((".htm", ".html")):
            continue
        if name == primary:
            continue
        # Skip XBRL viewer renders and linkbase HTML.
        if re.match(r"r\d+\.htm", low) or low.endswith(
            ("_def.htm", "_lab.htm", "_pre.htm", "_cal.htm")
        ):
            continue
        try:
            size = int(it.get("size", 0) or 0)
        except (TypeError, ValueError):
            size = 0
        if size > best_size:
            best_size = size
            best = name
    return f"{base}/{best}" if best else None


async def _sections_for_accession(client, accession: str, doc_url: str) -> dict[str, str]:
    """Download (once) and extract sections for a given accession."""
    if accession in _ACCESSION_SECTION_CACHE:
        return _ACCESSION_SECTION_CACHE[accession]
    sections: dict[str, str] = {}
    html_text = await _rate_limited_get(client, doc_url)
    if html_text:
        sections = _extract_sections(html_text)
        # Fallback for "incorporated by reference" thin primary documents
        # (large banks: WFC/USB/BNY/SYF): the primary fetched fine but carries
        # no risk text, so recover the body from the largest HTML document in
        # the same accession. Only when the primary *did* fetch — a failed
        # fetch must stay empty rather than be masked by a wrong document.
        if not sections.get("risks"):
            alt_url = await _largest_alt_document(client, doc_url)
            if alt_url:
                alt_html = await _rate_limited_get(client, alt_url)
                if alt_html:
                    alt_sections = _extract_sections(alt_html)
                    for key, value in alt_sections.items():
                        if value and not sections.get(key):
                            sections[key] = value

    _ACCESSION_SECTION_CACHE[accession] = sections
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

            # Step 2: find up to 3 most-recent distinct annual reports
            # (10-K, or 20-F for foreign private issuers; newest-first, PIT)
            filings = await _find_annual_filings(client, cik, as_of, limit=3)
            if not filings:
                logger.info("No 10-K/20-F found for %s before %s", ticker, as_of)
                return FilingData(ticker=ticker)

            # Step 3: build a history entry per filing (sections cached by accession)
            history: list[FilingData] = []
            for doc_url, filing_date, accession, fiscal_year, form_type in filings:
                sections = await _sections_for_accession(client, accession, doc_url)
                history.append(
                    FilingData(
                        ticker=ticker,
                        filing_date=filing_date,
                        filing_type=form_type,
                        business_overview=sections.get("business_overview"),
                        risks=sections.get("risks"),
                        mda=sections.get("mda"),
                        governance=sections.get("governance"),
                        fiscal_year=fiscal_year,
                        accession=accession,
                    )
                )

            # Step 4: top-level fields mirror history[0] (most recent filing)
            top = history[0]
            return FilingData(
                ticker=ticker,
                filing_date=top.filing_date,
                filing_type=top.filing_type,
                business_overview=top.business_overview,
                risks=top.risks,
                mda=top.mda,
                governance=top.governance,
                fiscal_year=top.fiscal_year,
                accession=top.accession,
                history=history,
            )
