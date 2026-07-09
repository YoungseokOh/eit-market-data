"""Disk-cache-only DART provider and DART transient-error detection."""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path
from typing import Any

from eit_market_data.core.pit import is_visible
from eit_market_data.kr.dart_provider import (
    _DART_CACHE_DIR,
    _DART_CACHE_SIZE_LIMIT,
    _extract_sections,
    _looks_like_risk_text,
)
from eit_market_data.kr.market_helpers import normalize_ticker
from eit_market_data.schemas.snapshot import FilingData, FundamentalData


def _filing_richness(filing: FilingData) -> int:
    """Rank how complete a cached filing copy is (more populated sections win)."""
    return sum(
        1
        for value in (
            filing.business_overview,
            filing.risks,
            filing.mda,
            filing.governance,
        )
        if value
    )


class CacheOnlyDartProvider:
    """DART provider backed only by the local disk cache.

    Use after a live OpenDART timeout/rate-limit signal so collection can
    continue without making more OpenDART network requests.
    """

    def __init__(self, cache_dir: Path = _DART_CACHE_DIR) -> None:
        try:
            import diskcache

            self._cache: Any = diskcache.Cache(
                str(cache_dir),
                size_limit=_DART_CACHE_SIZE_LIMIT,
            )
        except ImportError:
            self._cache = None

    def _lookup(self, prefix: str, ticker: str, as_of: date) -> Any:
        """Coarse bucket-month pre-filter.

        Returns the record stored under the collection bucket whose month is
        ``<= as_of``. This is only a pre-filter on the *collection* bucket; the
        record's own real date (``filing_date`` / ``report_date``) must still be
        validated against ``as_of`` by the caller before it is trusted, because
        a bucket can legitimately hold a record whose real date is in the future
        relative to ``as_of`` (e.g. an annual report physically filed months
        after the collection month it was first seen in).
        """
        if self._cache is None:
            return None
        target_ym = as_of.strftime("%Y%m")
        exact = self._cache.get(f"{prefix}:{ticker}:{target_ym}")
        if exact is not None:
            return exact

        latest_key = None
        for key in self._cache.iterkeys():
            text = str(key)
            key_prefix = f"{prefix}:{ticker}:"
            if not text.startswith(key_prefix):
                continue
            ym = text.rsplit(":", 1)[-1]
            if ym <= target_ym and (latest_key is None or ym > latest_key.rsplit(":", 1)[-1]):
                latest_key = text
        return self._cache.get(latest_key) if latest_key is not None else None

    def _iter_cached(self, prefix: str, ticker: str) -> Any:
        """Yield every cached record for ``prefix:ticker:*`` (any bucket month)."""
        if self._cache is None:
            return
        key_prefix = f"{prefix}:{ticker}:"
        for key in self._cache.iterkeys():
            text = str(key)
            if not text.startswith(key_prefix):
                continue
            value = self._cache.get(text)
            if value is not None:
                yield value

    async def fetch_fundamentals(
        self,
        ticker: str,
        as_of: date,
        n_quarters: int = 8,
    ) -> FundamentalData:
        _ = n_quarters
        norm_ticker = normalize_ticker(ticker)
        cached = self._lookup("fundamental", norm_ticker, as_of)
        if isinstance(cached, FundamentalData):
            # Point-in-time guard: a cached bucket may carry quarters whose
            # report_date is after as_of (future annual/interim reports). Drop
            # them so the snapshot never leaks look-ahead information.
            valid_quarters = [q for q in cached.quarters if q.report_date <= as_of]
            if len(valid_quarters) != len(cached.quarters):
                return cached.model_copy(update={"quarters": valid_quarters})
            return cached
        return FundamentalData(ticker=norm_ticker)

    async def fetch_filing(self, ticker: str, as_of: date) -> FilingData:
        norm_ticker = normalize_ticker(ticker)
        # Point-in-time guard: never return a filing whose filing_date is unknown
        # or after as_of. Gather every distinct annual filing visible at as_of,
        # newest-first, and build the trailing history (current + up to 2 prior).
        by_date: dict[date, FilingData] = {}
        for cached in self._iter_cached("filing", norm_ticker):
            if not isinstance(cached, FilingData):
                continue
            filing_date = cached.filing_date
            if filing_date is None or not is_visible(filing_date, as_of):
                continue
            # A given filing_date can appear in several collection-month buckets;
            # keep the richest copy (prefer one with business_overview, then risks).
            existing = by_date.get(filing_date)
            if existing is None or _filing_richness(cached) > _filing_richness(existing):
                by_date[filing_date] = cached

        if not by_date:
            return FilingData(ticker=norm_ticker)

        # Group by *fiscal year* (parsed from the report period), not filing_date.
        # A late-filed amendment of an OLD report otherwise slips into the
        # trailing-3-by-date window and is mislabeled (e.g. Doosan's 2024
        # [기재정정]사업보고서 (2020.12) → fy=2020), breaking the strictly-descending
        # fiscal_year invariant. Per year keep the latest-filed (richest) copy.
        by_year: dict[int, tuple[date, FilingData]] = {}
        for filing_date, entry in by_date.items():
            fy = self._fiscal_year_for(norm_ticker, filing_date)
            if fy is None:
                continue
            existing = by_year.get(fy)
            if existing is None or filing_date > existing[0]:
                by_year[fy] = (filing_date, entry)

        if not by_year:
            return FilingData(ticker=norm_ticker)

        ordered_years = sorted(by_year, reverse=True)[:3]

        history: list[FilingData] = []
        for fy in ordered_years:
            filing_date, entry = by_year[fy]
            # Re-validate stored risks: cached copies from older runs frequently
            # hold the WRONG text (product marketing, a rating legend). If the
            # stored value fails the content validator, re-extract from the cached
            # raw document; the re-extraction is itself validator-gated and yields
            # valid risk prose or None. "Better empty than wrong".
            risks = entry.risks
            if not (risks and _looks_like_risk_text(risks)):
                risks = self._reextract_risks(norm_ticker, filing_date)
            history.append(
                FilingData(
                    ticker=norm_ticker,
                    filing_date=filing_date,
                    filing_type=entry.filing_type,
                    business_overview=entry.business_overview,
                    risks=risks,
                    mda=entry.mda,
                    governance=entry.governance,
                    fiscal_year=fy,
                    accession=self._accession_for(norm_ticker, filing_date),
                )
            )

        top = history[0]
        return FilingData(
            ticker=norm_ticker,
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

    def _report_index(self, ticker: str) -> dict[str, tuple[str, str]]:
        """Map ``rcept_dt (YYYYMMDD) -> (rcept_no, report_nm)`` for this ticker's
        annual reports, from cached ``reports:<corp>:<ym>`` frames.

        Built lazily once per process and cached per ticker. Enables offline
        mapping from a cached filing's ``filing_date`` back to its raw
        ``doc:<rcept_no>`` text for risk re-extraction.
        """
        cache = getattr(self, "_report_index_cache", None)
        if cache is None:
            cache = {}
            self._report_index_cache = cache
        if ticker in cache:
            return cache[ticker]

        index: dict[str, tuple[str, str]] = {}
        if self._cache is not None:
            for key in self._cache.iterkeys():
                text = str(key)
                if not text.startswith("reports:"):
                    continue
                frame = self._cache.get(text)
                if frame is None or not hasattr(frame, "columns"):
                    continue
                cols = set(frame.columns)
                if not {"stock_code", "rcept_dt", "rcept_no", "report_nm"} <= cols:
                    continue
                try:
                    for _, row in frame.iterrows():
                        if normalize_ticker(str(row.get("stock_code", ""))) != ticker:
                            continue
                        report_nm = str(row.get("report_nm", ""))
                        if "사업보고서" not in report_nm:
                            continue
                        rcept_dt = str(row.get("rcept_dt", "")).strip()
                        rcept_no = str(row.get("rcept_no", "")).strip()
                        if rcept_dt and rcept_no:
                            index[rcept_dt] = (rcept_no, report_nm)
                except Exception:  # pragma: no cover - defensive over cached frames
                    continue
        cache[ticker] = index
        return index

    def _reextract_risks(self, ticker: str, filing_date: date) -> str | None:
        if self._cache is None:
            return None
        entry = self._report_index(ticker).get(filing_date.strftime("%Y%m%d"))
        if entry is None:
            return None
        rcept_no = entry[0]
        doc = self._cache.get(f"doc:{rcept_no}")
        if doc is None:
            return None
        text = doc.decode("utf-8", errors="ignore") if isinstance(doc, bytes) else str(doc)
        try:
            sections = _extract_sections(text)
        except Exception:
            return None
        return sections.get("risks")

    def _fiscal_year_for(self, ticker: str, filing_date: date) -> int | None:
        entry = self._report_index(ticker).get(filing_date.strftime("%Y%m%d"))
        if entry is not None:
            m = re.search(r"\((\d{4})", entry[1])
            if m:
                return int(m.group(1))
        # Annual report is filed in the year after the fiscal year it reports on.
        return filing_date.year - 1

    def _accession_for(self, ticker: str, filing_date: date) -> str | None:
        """DART rcept_no for this filing (audit/dedup identity), if known."""
        entry = self._report_index(ticker).get(filing_date.strftime("%Y%m%d"))
        return entry[0] if entry is not None else None

    async def fetch_issued_shares(self, ticker: str, as_of: date) -> float | None:
        cached = self._lookup("issued_shares", normalize_ticker(ticker), as_of)
        return float(cached) if isinstance(cached, (int, float)) and cached > 0 else None


def _is_dart_transient_error(exc: Exception) -> bool:
    text = f"{exc.__class__.__name__}: {exc}"
    markers = (
        "ConnectTimeout",
        "ReadTimeout",
        "Connection reset",
        "RemoteDisconnected",
        "Max retries exceeded",
        "HTTP 000",
        "temporarily unavailable",
    )
    return any(marker in text for marker in markers)
