"""OpenDartReader-based Korean fundamentals and filing provider."""

from __future__ import annotations

import asyncio
import logging
import os
import re
from datetime import date
from pathlib import Path
from typing import Any

from eit_market_data.core.pit import is_visible
from eit_market_data.kr.market_helpers import normalize_ticker
from eit_market_data.schemas.snapshot import FilingData, FundamentalData, QuarterlyFinancials

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_DART_CACHE_DIR = Path(
    os.environ.get(
        "EIT_DART_CACHE_DIR",
        str(_PROJECT_ROOT / "data" / "dart_cache"),
    )
).expanduser()
_DART_CACHE_SIZE_LIMIT = int(
    os.environ.get("EIT_DART_CACHE_SIZE_LIMIT_BYTES", str(50 * 1024 * 1024 * 1024))
)
_FINSTATE_TTL = 120 * 86_400   # quarterly statements are final once filed
_REPORT_LIST_TTL = 30 * 86_400  # new filings may appear; refresh monthly
_DOC_TTL = 365 * 86_400        # documents never change after filing

_ACCOUNT_MAP: dict[str, list[str]] = {
    "revenue": ["매출액", "영업수익"],
    "operating_income": ["영업이익", "영업이익(손실)"],
    "net_income": ["당기순이익", "당기순이익(손실)", "분기순이익", "반기순이익"],
    "total_assets": ["자산총계"],
    "total_liabilities": ["부채총계"],
    "total_equity": ["자본총계"],
    "current_assets": ["유동자산"],
    "current_liabilities": ["유동부채"],
    "gross_profit": ["매출총이익", "매출총손익"],
    "total_debt": ["차입금합계", "총차입금", "단기차입금", "차입금", "금융부채", "사채 및 차입금"],
    "eps": ["주당순이익", "주당이익", "기본주당이익"],
    "interest_expense": ["이자비용"],
    "operating_cash_flow": ["영업활동현금흐름", "영업활동으로 인한 현금흐름"],
    "capital_expenditure": ["유형자산의취득", "유형자산취득"],
    "cost_of_goods_sold": ["매출원가"],
    "cash_and_equivalents": ["현금및현금성자산", "현금 및 현금성자산"],
    "inventory": ["재고자산"],
    "accounts_receivable": ["매출채권", "매출채권 및 기타채권"],
}

_SECTION_PATTERNS: dict[str, list[str]] = {
    "business_overview": [
        r"사업의\s*내용",
        r"회사의\s*개요",
    ],
    "risks": [
        r"위험\s*요소",
        r"리스크\s*요인",
    ],
    "mda": [
        r"재무상태\s*및\s*영업실적",
        r"경영진의\s*논의",
        r"MD&A",
    ],
}

_REPORT_CODE_TO_QUARTER: dict[str, str] = {
    "11013": "Q1",
    "11012": "Q2",
    "11014": "Q3",
    "11011": "Q4",
}

_FLOW_FIELDS = {
    "revenue",
    "operating_income",
    "net_income",
    "gross_profit",
    "eps",
    "interest_expense",
    "operating_cash_flow",
    "capital_expenditure",
    "cost_of_goods_sold",
    "ebitda",
    "free_cash_flow",
}

_EPS_FIELDS = {"eps"}


def _normalize_ticker(ticker: str) -> str:
    return normalize_ticker(ticker)


def _date_to_yyyymmdd(value: date) -> str:
    return value.strftime("%Y%m%d")


def _parse_date_yyyymmdd(raw: Any) -> date | None:
    try:
        text = str(raw).strip()
        if len(text) != 8 or not text.isdigit():
            return None
        return date(int(text[:4]), int(text[4:6]), int(text[6:8]))
    except Exception:
        return None


def _parse_amount_to_krw(raw: Any) -> float | None:
    """Parse a DART financial-statement amount to raw KRW (won).

    DART ``thstrm_amount`` / ``thstrm_add_amount`` are reported in full KRW
    (won). We preserve that native magnitude so aggregate fundamentals share the
    same unit as ``market_cap`` (raw won, via ``market_helpers`` 시가총액) and as
    the US bundle (raw USD via ``edgar_xbrl_provider``). This keeps the
    consumer's cross-market ratios unit-correct without per-market scaling:
    ``net_income * 4 / market_cap`` (earnings yield), ROA, ROE, and
    ``market_cap / total_equity`` (P/B) all become unitless and realistic.

    NOTE: a prior version divided by 1000 here (despite the ``_to_million``
    name), silently storing KRW *thousands* — 1000x smaller than ``market_cap``
    (raw won). That uniform scale error preserved ranking but made absolute
    yields 1000x off.
    """
    if raw is None:
        return None
    text = str(raw).strip()
    if not text or text in {"-", "N/A", "nan", "None"}:
        return None
    text = text.replace(",", "").replace(" ", "")
    negative = False
    if text.startswith("(") and text.endswith(")"):
        negative = True
        text = text[1:-1]
    try:
        value = float(text)
    except ValueError:
        return None
    if negative:
        value = -value
    return round(value, 1)


def _parse_eps(raw: Any) -> float | None:
    """Parse DART EPS value in native KRW per share (no unit conversion)."""
    if raw is None:
        return None
    text = str(raw).strip()
    if not text or text in {"-", "N/A", "nan", "None"}:
        return None
    text = text.replace(",", "").replace(" ", "")
    negative = False
    if text.startswith("(") and text.endswith(")"):
        negative = True
        text = text[1:-1]
    try:
        value = float(text)
    except ValueError:
        return None
    if negative:
        value = -value
    return round(value, 2)


def _clean_document_text(raw: str) -> str:
    text = re.sub(r"<[^>]+>", "\n", raw)
    text = text.replace("&nbsp;", " ").replace("&#160;", " ").replace("&amp;", "&")
    text = text.replace("\r", "\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()


def _extract_sections(doc_text: str, max_chars: int = 8000) -> dict[str, str]:
    plain = _clean_document_text(doc_text)
    matches: list[tuple[int, int, str]] = []

    for section_name, patterns in _SECTION_PATTERNS.items():
        best_match: re.Match[str] | None = None
        for pattern in patterns:
            m = re.search(pattern, plain, flags=re.IGNORECASE)
            if m and (best_match is None or m.start() < best_match.start()):
                best_match = m
        if best_match is not None:
            matches.append((best_match.start(), best_match.end(), section_name))

    if not matches:
        return {}

    matches.sort(key=lambda item: item[0])
    extracted: dict[str, str] = {}
    for i, (_start, section_start, section_name) in enumerate(matches):
        next_start = matches[i + 1][0] if i + 1 < len(matches) else min(
            len(plain), section_start + max_chars
        )
        chunk = plain[section_start:next_start].strip()
        if len(chunk) > max_chars:
            chunk = chunk[:max_chars]
        if len(chunk) > 60:
            extracted[section_name] = chunk

    return extracted


def _parse_share_count(raw: str) -> float | None:
    text = raw.strip().replace(",", "")
    if not text or not text.isdigit():
        return None
    value = float(text)
    return value if value > 0 else None


def _extract_issued_shares_from_document(doc_text: str) -> float | None:
    """Extract common issued shares from DART's share-count table."""
    plain = _clean_document_text(doc_text)
    starts: list[int] = []
    for marker in ("주식의 총수 현황", "4. 주식의 총수 등", "주식의 총수 등"):
        offset = 0
        while True:
            found = plain.find(marker, offset)
            if found < 0:
                break
            starts.append(found)
            offset = found + len(marker)

    if not starts:
        starts = [0]

    patterns = (
        r"발행주식의\s*총수\s*\([^)]*\)\s*\n+\s*([0-9][0-9,]+)",
        r"발행주식총수\s*\n+\s*([0-9][0-9,]+)",
    )
    for start in sorted(set(starts)):
        chunk = plain[start : start + 8000]
        for pattern in patterns:
            match = re.search(pattern, chunk)
            if match is None:
                continue
            parsed = _parse_share_count(match.group(1))
            if parsed is not None:
                return parsed
    return None


def _quarter_sort_key(fiscal_quarter: str) -> tuple[int, int]:
    year = int(fiscal_quarter[:4])
    quarter_num = int(fiscal_quarter[-1])
    return year, quarter_num


def _previous_cumulative_quarter(fiscal_quarter: str) -> str | None:
    year = int(fiscal_quarter[:4])
    quarter = fiscal_quarter[-2:]
    if quarter == "Q1":
        return None
    if quarter == "Q2":
        return f"{year}Q1"
    if quarter == "Q3":
        return f"{year}Q2"
    if quarter == "Q4":
        return f"{year}Q3"
    return None


def _round_quarter_value(field: str, value: float) -> float:
    return round(value, 2 if field in _EPS_FIELDS else 1)


def _normalize_quarter_values(
    fiscal_quarter: str,
    raw_values: dict[str, float | None],
    raw_quarter_map: dict[str, dict[str, float | None]],
) -> dict[str, float | None]:
    normalized = dict(raw_values)
    previous_quarter = _previous_cumulative_quarter(fiscal_quarter)
    for field in _FLOW_FIELDS:
        current = raw_values.get(field)
        if fiscal_quarter.endswith("Q1") or current is None:
            normalized[field] = current
            continue

        if previous_quarter is None:
            normalized[field] = current
            continue

        previous = raw_quarter_map.get(previous_quarter, {}).get(field)
        if previous is None:
            normalized[field] = None
            continue

        normalized[field] = _round_quarter_value(field, current - previous)
    return normalized


def _parse_report_nm(report_nm: str) -> tuple[str, str] | None:
    """Parse DART report name to extract year and report code.

    Examples:
        '분기보고서 (2025.09)' → ('2025', '11014')  # Q3
        '반기보고서 (2025.06)' → ('2025', '11012')  # Q2
        '분기보고서 (2025.03)' → ('2025', '11013')  # Q1
        '사업보고서 (2024.12)' → ('2024', '11011')  # Q4
    """
    if not report_nm:
        return None

    # Extract year and month from report_nm: "보고서명 (YYYY.MM)"
    match = re.search(r"\((\d{4})\.(\d{2})\)", report_nm)
    if not match:
        return None

    year = match.group(1)
    month = match.group(2)

    # Map month to report code
    month_to_code = {
        "03": "11013",  # Q1
        "06": "11012",  # Q2 or H1
        "09": "11014",  # Q3
        "12": "11011",  # Q4 or annual
    }

    reprt_code = month_to_code.get(month)
    if reprt_code is None:
        return None

    return year, reprt_code


def _report_entries_from_list(report_list: Any, as_of: date) -> list[dict[str, Any]]:
    """Extract report entries from OpenDartReader list() response.

    Handles both old-style (with reprt_code, bsns_year columns) and new-style
    (with report_nm field) API responses.
    """
    if report_list is None or report_list.empty:
        return []

    reports = report_list.copy()
    if "rcept_dt" in reports.columns:
        reports = reports.loc[
            reports["rcept_dt"].fillna("").astype(str) <= _date_to_yyyymmdd(as_of)
        ]
    if reports.empty:
        return []

    entries_by_key: dict[tuple[str, str], dict[str, Any]] = {}

    # Check if old-style columns exist (reprt_code, bsns_year)
    has_old_style = "reprt_code" in reports.columns and "bsns_year" in reports.columns

    for _, row in reports.iterrows():
        report_date = _parse_date_yyyymmdd(row.get("rcept_dt"))
        rcept_no = str(row.get("rcept_no", "")).strip()

        if report_date is None or not rcept_no:
            continue

        # Try old-style parsing first
        if has_old_style:
            reprt_code = str(row.get("reprt_code", "")).strip()
            bsns_year = str(row.get("bsns_year", "")).strip()
            if not reprt_code or not bsns_year.isdigit():
                continue
        else:
            # New-style parsing from report_nm
            report_nm = str(row.get("report_nm", "")).strip()
            parsed = _parse_report_nm(report_nm)
            if parsed is None:
                continue
            bsns_year, reprt_code = parsed

        quarter_label = _REPORT_CODE_TO_QUARTER.get(reprt_code)
        if quarter_label is None:
            continue

        key = (bsns_year, reprt_code)
        current = entries_by_key.get(key)
        entry = {
            "fiscal_quarter": f"{bsns_year}{quarter_label}",
            "report_date": report_date,
            "bsns_year": bsns_year,
            "reprt_code": reprt_code,
            "rcept_no": rcept_no,
        }
        if current is None or report_date > current["report_date"]:
            entries_by_key[key] = entry

    entries = list(entries_by_key.values())
    entries.sort(key=lambda item: item["report_date"], reverse=True)
    return entries


class DartProvider:
    """Korean fundamentals/filings provider backed by OpenDartReader."""

    def __init__(
        self,
        api_key: str | None = None,
        *,
        allow_stale_fallback: bool = True,
        raise_on_error: bool = False,
    ) -> None:
        try:
            from OpenDartReader import OpenDartReader as _OpenDartReader
        except ImportError:
            try:
                # Some OpenDartReader builds expose the class directly on import.
                import OpenDartReader as _OpenDartReader  # type: ignore[no-redef]
            except ImportError as e:
                raise ImportError(
                    "OpenDartReader is required for Korean filings/fundamentals. "
                    "Install with: pip install -e '.[kr]'"
                ) from e

        key = api_key or os.environ.get("DART_API_KEY", "")
        if not key:
            raise ValueError(
                "DART_API_KEY environment variable is required for DartProvider."
            )

        self._dart = _OpenDartReader(key)
        self._corp_cache: dict[str, str | None] = {}
        self._corp_list: Any = None
        self._semaphore = asyncio.Semaphore(2)
        self._allow_stale_fallback = allow_stale_fallback
        self._raise_on_error = raise_on_error

        try:
            import diskcache
            _DART_CACHE_DIR.mkdir(parents=True, exist_ok=True)
            self._cache: Any = diskcache.Cache(
                str(_DART_CACHE_DIR),
                size_limit=_DART_CACHE_SIZE_LIMIT,
            )
        except ImportError:
            self._cache = None

    async def fetch_fundamentals(
        self, ticker: str, as_of: date, n_quarters: int = 8
    ) -> FundamentalData:
        norm_ticker = _normalize_ticker(ticker)
        cache_key = f"fundamental:{norm_ticker}:{as_of.strftime('%Y%m')}"
        cached = self._cache_get(cache_key)
        if cached is not None and isinstance(cached, FundamentalData) and cached.quarters:
            # Point-in-time guard: drop any cached quarter reported after as_of.
            valid = [q for q in cached.quarters if q.report_date <= as_of]
            if valid:
                return cached.model_copy(update={"quarters": valid}) if len(valid) != len(cached.quarters) else cached

        async with self._semaphore:
            try:
                result = await asyncio.to_thread(
                    self._fetch_fundamentals_sync, norm_ticker, as_of, n_quarters
                )
            except Exception as e:
                logger.warning("DART fundamentals fetch failed for %s: %s", norm_ticker, e)
                if self._raise_on_error:
                    raise
                result = FundamentalData(ticker=norm_ticker)

        if result.quarters:
            self._cache_set(cache_key, result, _FINSTATE_TTL)
        elif not result.quarters:
            # API returned empty — try any stale entry for this ticker, but never
            # leak quarters reported after as_of.
            stale = self._cache_stale(f"fundamental:{norm_ticker}:")
            if (
                self._allow_stale_fallback
                and stale is not None
                and isinstance(stale, FundamentalData)
                and stale.quarters
            ):
                valid = [q for q in stale.quarters if q.report_date <= as_of]
                if valid:
                    logger.warning("DART API returned empty; using stale fundamentals cache for %s", norm_ticker)
                    return stale.model_copy(update={"quarters": valid})
            if self._raise_on_error:
                raise RuntimeError(f"DART fundamentals returned empty for {norm_ticker}")
        return result

    async def fetch_filing(self, ticker: str, as_of: date) -> FilingData:
        norm_ticker = _normalize_ticker(ticker)
        cache_key = f"filing:{norm_ticker}:{as_of.strftime('%Y%m')}"
        cached = self._cache_get(cache_key)
        if (
            cached is not None
            and isinstance(cached, FilingData)
            and cached.business_overview
            and is_visible(cached.filing_date, as_of)
        ):
            # Point-in-time guard: only trust a cached filing actually filed by as_of.
            return cached

        async with self._semaphore:
            try:
                result = await asyncio.to_thread(self._fetch_filing_sync, norm_ticker, as_of)
            except Exception as e:
                logger.warning("DART filing fetch failed for %s: %s", norm_ticker, e)
                if self._raise_on_error:
                    raise
                result = FilingData(ticker=norm_ticker)

        if result.business_overview:
            self._cache_set(cache_key, result, _DOC_TTL)
        elif not result.business_overview:
            stale = self._cache_stale(f"filing:{norm_ticker}:")
            if (
                self._allow_stale_fallback
                and stale is not None
                and isinstance(stale, FilingData)
                and stale.business_overview
                and is_visible(stale.filing_date, as_of)
            ):
                # Point-in-time guard: never fall back to a filing filed after as_of.
                logger.warning("DART API returned empty; using stale filing cache for %s", norm_ticker)
                return stale
            if self._raise_on_error:
                raise RuntimeError(f"DART filing returned empty for {norm_ticker}")
        return result

    async def fetch_issued_shares(self, ticker: str, as_of: date) -> float | None:
        norm_ticker = _normalize_ticker(ticker)
        cache_key = f"issued_shares:{norm_ticker}:{as_of.strftime('%Y%m')}"
        cached = self._cache_get(cache_key)
        if isinstance(cached, (int, float)) and cached > 0:
            return float(cached)

        async with self._semaphore:
            try:
                result = await asyncio.to_thread(
                    self._fetch_issued_shares_sync,
                    norm_ticker,
                    as_of,
                )
            except Exception as e:
                logger.warning("DART issued-shares fetch failed for %s: %s", norm_ticker, e)
                if self._raise_on_error:
                    raise
                result = None

        if result is not None:
            self._cache_set(cache_key, result, _DOC_TTL)
        elif self._allow_stale_fallback:
            stale = self._cache_stale(f"issued_shares:{norm_ticker}:")
            if isinstance(stale, (int, float)) and stale > 0:
                return float(stale)
        return result

    # ------------------------------------------------------------------
    # Fundamentals
    # ------------------------------------------------------------------

    def _get_corp_list(self):  # noqa: ANN202
        if self._corp_list is None:
            corp_list = getattr(self._dart, "corp_codes", None)
            if corp_list is None:
                corp_list = getattr(self._dart, "corp_code", None)
            if callable(corp_list):
                corp_list = corp_list()
            self._corp_list = corp_list
        return self._corp_list

    def _ticker_to_corp_code(self, ticker: str) -> str | None:
        if ticker in self._corp_cache:
            return self._corp_cache[ticker]

        try:
            finder = getattr(self._dart, "find_corp_code", None)
            if callable(finder):
                corp_code = str(finder(ticker) or "").strip()
                if corp_code:
                    self._corp_cache[ticker] = corp_code
                    return corp_code

            corp_list = self._get_corp_list()
            if corp_list is None or corp_list.empty:
                self._corp_cache[ticker] = None
                return None
            if "stock_code" not in corp_list.columns or "corp_code" not in corp_list.columns:
                self._corp_cache[ticker] = None
                return None

            stock_codes = corp_list["stock_code"].fillna("").astype(str).str.zfill(6)
            matched = corp_list.loc[stock_codes == ticker, "corp_code"]
            if matched.empty:
                self._corp_cache[ticker] = None
                return None

            corp_code = str(matched.iloc[0]).strip()
            self._corp_cache[ticker] = corp_code
            return corp_code
        except Exception as e:
            logger.warning("DART corp_code lookup failed for %s: %s", ticker, e)
            self._corp_cache[ticker] = None
            return None

    def _cache_get(self, key: str) -> Any:
        if self._cache is None:
            return None
        try:
            return self._cache.get(key)
        except Exception:
            return None

    def _cache_set(self, key: str, value: Any, ttl: int) -> None:
        if self._cache is None:
            return
        try:
            self._cache.set(key, value, expire=ttl)
        except Exception:
            pass

    def _cache_stale(self, prefix: str) -> Any:
        """Return the first expired/evicted entry whose key starts with prefix."""
        if self._cache is None:
            return None
        try:
            for key in self._cache:
                if isinstance(key, str) and key.startswith(prefix):
                    val = self._cache.get(key)
                    if val is not None:
                        return val
        except Exception:
            pass
        return None

    def _fetch_finstate(self, corp_code: str, year: str, reprt_code: str):  # noqa: ANN202
        for fs_div in ("CFS", "OFS"):
            cache_key = f"finstate:{corp_code}:{year}:{reprt_code}:{fs_div}"
            cached = self._cache_get(cache_key)
            if cached is not None:
                return cached

            try:
                df = self._dart.finstate(corp_code, year, reprt_code=reprt_code, fs_div=fs_div)
            except TypeError:
                # Older OpenDartReader may not support fs_div kwarg.
                try:
                    df = self._dart.finstate(corp_code, year, reprt_code=reprt_code)
                except Exception:
                    df = None
            except Exception:
                df = None

            if df is not None and not df.empty:
                self._cache_set(cache_key, df, _FINSTATE_TTL)
                return df

        # All API calls failed — try any stale cache entry for this report
        stale = self._cache_stale(f"finstate:{corp_code}:{year}:{reprt_code}:")
        if self._allow_stale_fallback and stale is not None:
            logger.warning("DART API unavailable; using stale finstate cache for %s %s %s", corp_code, year, reprt_code)
            return stale
        if self._raise_on_error:
            raise RuntimeError(
                f"DART finstate unavailable for corp={corp_code} year={year} reprt_code={reprt_code}"
            )
        return None

    def _pick_account_value(self, df: Any, candidates: list[str]) -> float | None:
        if df is None or df.empty or "account_nm" not in df.columns:
            return None

        names = df["account_nm"].fillna("").astype(str).str.strip()
        for candidate in candidates:
            exact = df.loc[names == candidate]
            if not exact.empty:
                val = _parse_amount_to_krw(exact.iloc[0].get("thstrm_amount"))
                if val is not None:
                    return val

        for candidate in candidates:
            partial = df.loc[names.str.contains(candidate, regex=False)]
            if not partial.empty:
                val = _parse_amount_to_krw(partial.iloc[0].get("thstrm_amount"))
                if val is not None:
                    return val
        return None

    def _pick_cumulative_value(self, df: Any, candidates: list[str]) -> float | None:
        """Pick the cumulative (YTD) value for a flow account.

        DART interim ``finstate`` reports expose the single-quarter (standalone
        3-month) value in ``thstrm_amount`` and the cumulative year-to-date value
        in ``thstrm_add_amount``. Flow fields must use the cumulative column so the
        downstream cumulative-subtraction in ``_normalize_quarter_values`` yields a
        correct per-quarter figure. ``thstrm_add_amount`` is absent for the annual
        report (reprt_code 11011) and equal to ``thstrm_amount`` for Q1, so fall
        back to ``thstrm_amount`` when the cumulative column is missing/empty.
        """
        if df is None or df.empty or "account_nm" not in df.columns:
            return None

        has_add = "thstrm_add_amount" in df.columns
        names = df["account_nm"].fillna("").astype(str).str.strip()

        def _read(row: Any) -> float | None:
            if has_add:
                cumulative = _parse_amount_to_krw(row.get("thstrm_add_amount"))
                if cumulative is not None:
                    return cumulative
            return _parse_amount_to_krw(row.get("thstrm_amount"))

        for candidate in candidates:
            exact = df.loc[names == candidate]
            if not exact.empty:
                val = _read(exact.iloc[0])
                if val is not None:
                    return val

        for candidate in candidates:
            partial = df.loc[names.str.contains(candidate, regex=False)]
            if not partial.empty:
                val = _read(partial.iloc[0])
                if val is not None:
                    return val
        return None

    def _pick_eps_value(self, df: Any) -> float | None:
        """Pick EPS value using native KRW unit (no /1000 conversion)."""
        if df is None or df.empty or "account_nm" not in df.columns:
            return None

        names = df["account_nm"].fillna("").astype(str).str.strip()
        candidates = _ACCOUNT_MAP["eps"]
        for candidate in candidates:
            exact = df.loc[names == candidate]
            if not exact.empty:
                val = _parse_eps(exact.iloc[0].get("thstrm_amount"))
                if val is not None:
                    return val

        for candidate in candidates:
            partial = df.loc[names.str.contains(candidate, regex=False)]
            if not partial.empty:
                val = _parse_eps(partial.iloc[0].get("thstrm_amount"))
                if val is not None:
                    return val
        return None

    def _fetch_report_list(self, corp_code: str, as_of: date):  # noqa: ANN202
        cache_key = f"reports:{corp_code}:{as_of.strftime('%Y%m')}"
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached

        try:
            result = self._dart.list(
                corp_code,
                start=f"{max(as_of.year - 10, 2000)}0101",
                end=_date_to_yyyymmdd(as_of),
                kind="A",
            )
        except Exception as exc:
            # API unavailable — try most recent stale entry for this corp
            stale = self._cache_stale(f"reports:{corp_code}:")
            if self._allow_stale_fallback and stale is not None:
                logger.warning("DART API unavailable; using stale report list for %s: %s", corp_code, exc)
                return stale
            raise

        if result is not None and not result.empty:
            self._cache_set(cache_key, result, _REPORT_LIST_TTL)
        return result

    def _build_raw_quarter_data(self, df_fin: Any) -> dict[str, float | None]:
        raw = {
            # Flow (income/cash-flow) fields: read the cumulative YTD column so the
            # downstream per-quarter decomposition is correct.
            "revenue": self._pick_cumulative_value(df_fin, _ACCOUNT_MAP["revenue"]),
            "operating_income": self._pick_cumulative_value(
                df_fin, _ACCOUNT_MAP["operating_income"]
            ),
            "net_income": self._pick_cumulative_value(df_fin, _ACCOUNT_MAP["net_income"]),
            # Stock (balance-sheet) fields: point-in-time balances, read as-is.
            "total_assets": self._pick_account_value(df_fin, _ACCOUNT_MAP["total_assets"]),
            "total_liabilities": self._pick_account_value(
                df_fin, _ACCOUNT_MAP["total_liabilities"]
            ),
            "total_equity": self._pick_account_value(df_fin, _ACCOUNT_MAP["total_equity"]),
            "current_assets": self._pick_account_value(df_fin, _ACCOUNT_MAP["current_assets"]),
            "current_liabilities": self._pick_account_value(
                df_fin, _ACCOUNT_MAP["current_liabilities"]
            ),
            "gross_profit": self._pick_cumulative_value(df_fin, _ACCOUNT_MAP["gross_profit"]),
            "total_debt": self._pick_account_value(df_fin, _ACCOUNT_MAP["total_debt"]),
            "eps": self._pick_eps_value(df_fin),
            "interest_expense": self._pick_cumulative_value(
                df_fin, _ACCOUNT_MAP["interest_expense"]
            ),
            "operating_cash_flow": self._pick_cumulative_value(
                df_fin, _ACCOUNT_MAP["operating_cash_flow"]
            ),
            "capital_expenditure": self._pick_cumulative_value(
                df_fin, _ACCOUNT_MAP["capital_expenditure"]
            ),
            "cost_of_goods_sold": self._pick_cumulative_value(
                df_fin, _ACCOUNT_MAP["cost_of_goods_sold"]
            ),
            "cash_and_equivalents": self._pick_account_value(
                df_fin, _ACCOUNT_MAP["cash_and_equivalents"]
            ),
            "inventory": self._pick_account_value(df_fin, _ACCOUNT_MAP["inventory"]),
            "accounts_receivable": self._pick_account_value(
                df_fin, _ACCOUNT_MAP["accounts_receivable"]
            ),
        }

        # Calculate derived fields
        # gross_profit = revenue - cost_of_goods_sold
        if not raw.get("gross_profit") and raw.get("revenue") and raw.get("cost_of_goods_sold"):
            raw["gross_profit"] = raw["revenue"] - raw["cost_of_goods_sold"]

        # ebitda ≈ operating_income (lower-bound approximation)
        if not raw.get("ebitda") and raw.get("operating_income"):
            raw["ebitda"] = raw["operating_income"]

        # free_cash_flow = operating_cash_flow - capital_expenditure
        if raw.get("operating_cash_flow") and raw.get("capital_expenditure"):
            raw["free_cash_flow"] = raw["operating_cash_flow"] - abs(raw["capital_expenditure"])

        return raw

    def _fetch_fundamentals_sync(
        self, ticker: str, as_of: date, n_quarters: int
    ) -> FundamentalData:
        corp_code = self._ticker_to_corp_code(ticker)
        if not corp_code:
            return FundamentalData(ticker=ticker)

        try:
            report_list = self._fetch_report_list(corp_code, as_of)
        except Exception as e:
            logger.warning("DART report list fetch failed for %s: %s", ticker, e)
            if self._raise_on_error:
                raise
            return FundamentalData(ticker=ticker)

        entries = _report_entries_from_list(report_list, as_of)
        raw_quarter_map: dict[str, dict[str, float | None]] = {}
        report_dates: dict[str, date] = {}

        for entry in sorted(entries, key=lambda item: _quarter_sort_key(item["fiscal_quarter"])):
            fiscal_quarter = entry["fiscal_quarter"]
            try:
                df_fin = self._fetch_finstate(
                    corp_code,
                    entry["bsns_year"],
                    entry["reprt_code"],
                )
            except Exception:
                df_fin = None
            if df_fin is None or df_fin.empty:
                continue

            raw_values = self._build_raw_quarter_data(df_fin)
            if all(value is None for value in raw_values.values()):
                continue

            raw_quarter_map[fiscal_quarter] = raw_values
            report_dates[fiscal_quarter] = entry["report_date"]

        quarters: list[QuarterlyFinancials] = []
        for fiscal_quarter, raw_values in raw_quarter_map.items():
            normalized = _normalize_quarter_values(
                fiscal_quarter,
                raw_values,
                raw_quarter_map,
            )
            if all(value is None for value in normalized.values()):
                continue
            quarters.append(
                QuarterlyFinancials(
                    fiscal_quarter=fiscal_quarter,
                    report_date=report_dates[fiscal_quarter],
                    **normalized,
                )
            )

        quarters.sort(key=lambda quarter: quarter.report_date, reverse=True)

        return FundamentalData(
            ticker=ticker,
            quarters=quarters[:n_quarters],
        )

    def _fetch_issued_shares_sync(self, ticker: str, as_of: date) -> float | None:
        corp_code = self._ticker_to_corp_code(ticker)
        if not corp_code:
            return None

        try:
            report_list = self._fetch_report_list(corp_code, as_of)
        except Exception as e:
            logger.warning("DART report list fetch failed for issued shares %s: %s", ticker, e)
            if self._raise_on_error:
                raise
            return None

        entries = _report_entries_from_list(report_list, as_of)
        for entry in entries:
            rcept_no = entry.get("rcept_no")
            if not rcept_no:
                continue
            doc_text = self._fetch_document(str(rcept_no))
            if not doc_text:
                continue
            issued_shares = _extract_issued_shares_from_document(doc_text)
            if issued_shares is not None:
                return issued_shares
        return None

    # ------------------------------------------------------------------
    # Filing
    # ------------------------------------------------------------------

    def _fetch_document(self, rcept_no: str) -> str:
        doc_cache_key = f"doc:{rcept_no}"
        doc = self._cache_get(doc_cache_key)
        if doc is None:
            doc = self._dart.document(rcept_no)
            if doc:
                self._cache_set(doc_cache_key, doc, _DOC_TTL)
        if isinstance(doc, bytes):
            return doc.decode("utf-8", errors="ignore")
        return str(doc) if doc else ""

    def _filing_report_candidates(self, report_list: Any, as_of: date) -> Any:
        reports = report_list.copy()
        if "rcept_dt" in reports.columns:
            reports = reports.loc[
                reports["rcept_dt"].fillna("").astype(str) <= _date_to_yyyymmdd(as_of)
            ]
        if reports.empty:
            return reports

        annual_mask = None
        if "reprt_code" in reports.columns:
            annual_mask = reports["reprt_code"].fillna("").astype(str) == "11011"
        if "report_nm" in reports.columns:
            name_mask = reports["report_nm"].fillna("").astype(str).str.contains(
                "사업보고서",
                regex=False,
            )
            annual_mask = name_mask if annual_mask is None else annual_mask | name_mask
        if annual_mask is not None and annual_mask.any():
            reports = reports.loc[annual_mask]

        if "rcept_dt" in reports.columns:
            reports = reports.sort_values("rcept_dt", ascending=False)
        return reports

    def _fetch_filing_sync(self, ticker: str, as_of: date) -> FilingData:
        corp_code = self._ticker_to_corp_code(ticker)
        if not corp_code:
            return FilingData(ticker=ticker)

        try:
            report_list = self._fetch_report_list(corp_code, as_of)
        except Exception as e:
            logger.warning("DART report list fetch failed for %s: %s", ticker, e)
            if self._raise_on_error:
                raise
            return FilingData(ticker=ticker)

        if report_list is None or report_list.empty:
            return FilingData(ticker=ticker)

        reports = self._filing_report_candidates(report_list, as_of)
        if reports.empty:
            return FilingData(ticker=ticker)

        fallback_date: date | None = None
        for _, report in reports.iterrows():
            rcept_no = str(report.get("rcept_no", "")).strip()
            filing_date = _parse_date_yyyymmdd(report.get("rcept_dt"))
            if fallback_date is None:
                fallback_date = filing_date
            if not rcept_no:
                continue

            try:
                doc_text = self._fetch_document(rcept_no)
                sections = _extract_sections(doc_text) if doc_text else {}
            except Exception as e:
                logger.warning(
                    "DART document fetch/parse failed for %s %s: %s",
                    ticker,
                    rcept_no,
                    e,
                )
                continue
            if not sections.get("business_overview"):
                continue
            return FilingData(
                ticker=ticker,
                filing_date=filing_date,
                filing_type="사업보고서",
                business_overview=sections.get("business_overview"),
                risks=sections.get("risks"),
                mda=sections.get("mda"),
            )

        return FilingData(
            ticker=ticker,
            filing_date=fallback_date,
            filing_type="사업보고서",
        )
