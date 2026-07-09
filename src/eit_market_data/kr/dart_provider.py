"""OpenDartReader-based Korean fundamentals and filing provider.

Parsing/normalization helpers live in :mod:`eit_market_data.kr.dart_parsing`
and document/section extraction in :mod:`eit_market_data.kr.dart_document`;
both are re-exported here for backward compatibility.
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import date
from pathlib import Path
from typing import Any

from eit_market_data.core.pit import is_visible
from eit_market_data.kr.dart_document import (  # noqa: F401 - re-exported for backward compat
    _SECTION_PATTERNS,
    _clean_document_text,
    _extract_issued_shares_from_document,
    _extract_sections,
    _is_toc_chunk,
    _looks_like_risk_text,
    _parse_share_count,
)
from eit_market_data.kr.dart_parsing import (  # noqa: F401 - re-exported for backward compat
    _ACCOUNT_MAP,
    _EPS_FIELDS,
    _FLOW_FIELDS,
    _REPORT_CODE_TO_QUARTER,
    _date_to_yyyymmdd,
    _fiscal_year_from_report_nm,
    _normalize_quarter_values,
    _parse_amount_to_krw,
    _parse_date_yyyymmdd,
    _parse_eps,
    _parse_report_nm,
    _previous_cumulative_quarter,
    _quarter_sort_key,
    _report_entries_from_list,
    _round_quarter_value,
)
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


def _normalize_ticker(ticker: str) -> str:
    return normalize_ticker(ticker)


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

    def _fetch_finstate_all(self, corp_code: str, year: str, reprt_code: str):  # noqa: ANN202
        """Cache-only full-statements (finstate_all) lookup.

        The major-accounts ``finstate`` endpoint omits COGS, gross profit and the
        cash-flow statement (OCF); those live in ``finstate_all``. This reads only
        what the controlled ``scripts/backfill_finstate_all.py`` driver has cached
        under ``finstate_all:corp:year:reprt:fs_div`` and NEVER issues a live call,
        so a normal fundamentals build cannot trigger unthrottled DART traffic for
        these fields. Returns None when the period is not cached.
        """
        for fs_div in ("CFS", "OFS"):
            cached = self._cache_get(f"finstate_all:{corp_code}:{year}:{reprt_code}:{fs_div}")
            if cached is not None:
                return cached
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

    def _build_raw_quarter_data(
        self, df_fin: Any, df_all: Any = None
    ) -> dict[str, float | None]:
        # COGS / gross profit / OCF are absent from the major-accounts finstate;
        # read them from the full-statements finstate_all when available, falling
        # back to df_fin (which yields None) so existing fields are untouched.
        df_flow = df_all if df_all is not None else df_fin
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
            "gross_profit": self._pick_cumulative_value(df_flow, _ACCOUNT_MAP["gross_profit"]),
            "total_debt": self._pick_account_value(df_fin, _ACCOUNT_MAP["total_debt"]),
            "eps": self._pick_eps_value(df_fin),
            "interest_expense": self._pick_cumulative_value(
                df_fin, _ACCOUNT_MAP["interest_expense"]
            ),
            "operating_cash_flow": self._pick_cumulative_value(
                df_flow, _ACCOUNT_MAP["operating_cash_flow"]
            ),
            "capital_expenditure": self._pick_cumulative_value(
                df_fin, _ACCOUNT_MAP["capital_expenditure"]
            ),
            "cost_of_goods_sold": self._pick_cumulative_value(
                df_flow, _ACCOUNT_MAP["cost_of_goods_sold"]
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

            try:
                df_all = self._fetch_finstate_all(
                    corp_code, entry["bsns_year"], entry["reprt_code"]
                )
            except Exception:
                df_all = None

            raw_values = self._build_raw_quarter_data(df_fin, df_all)
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
            # Alphanumeric KRX codes (e.g. ``0126Z0`` = 삼성에피스홀딩스, a newly
            # spun-off holding company) are special/temporary trading codes that
            # have no DART corp_code mapping and therefore no standalone
            # 사업보고서. Record the reason rather than silently returning empty.
            if any(ch.isalpha() for ch in str(ticker)):
                logger.warning(
                    "No DART corp_code for special KRX code %s "
                    "(preferred/holding/when-issued code with no standalone "
                    "사업보고서); leaving filing empty",
                    ticker,
                )
            else:
                logger.info("No DART corp_code for %s; leaving filing empty", ticker)
            return FilingData(ticker=ticker)

        try:
            report_list = self._fetch_report_list(corp_code, as_of)
        except Exception as e:
            logger.warning("DART report list fetch failed for %s: %s", ticker, e)
            if self._raise_on_error:
                raise
            return FilingData(ticker=ticker)

        if report_list is None or report_list.empty:
            logger.info(
                "No DART 사업보고서 found for %s (corp_code=%s) on/before %s",
                ticker,
                corp_code,
                as_of,
            )
            return FilingData(ticker=ticker)

        reports = self._filing_report_candidates(report_list, as_of)
        if reports.empty:
            logger.info(
                "No annual 사업보고서 candidate for %s on/before %s", ticker, as_of
            )
            return FilingData(ticker=ticker)

        # Group candidate 사업보고서 by *fiscal year* (parsed from the report
        # period), not by filing_date. A late-filed amendment of an OLD report
        # (e.g. Doosan's 2024 [기재정정]사업보고서 (2020.12)) otherwise slips into the
        # trailing-3-by-date window and is mislabeled fy=2020, breaking the
        # strictly-descending fiscal_year invariant. Within a fiscal year we try
        # the latest-filed report first (an amendment supersedes the original).
        fallback_date: date | None = None
        by_year: dict[int, list[tuple[date, str]]] = {}
        for _, report in reports.iterrows():
            rcept_no = str(report.get("rcept_no", "")).strip()
            filing_date = _parse_date_yyyymmdd(report.get("rcept_dt"))
            fiscal_year = _fiscal_year_from_report_nm(
                str(report.get("report_nm", "")), filing_date
            )
            if fallback_date is None:
                fallback_date = filing_date
            if not rcept_no or fiscal_year is None or filing_date is None:
                continue
            by_year.setdefault(fiscal_year, []).append((filing_date, rcept_no))

        history: list[FilingData] = []
        # Newest distinct fiscal years, descending → strictly-descending labels;
        # collect up to 3 that actually extract (reach past a year that fails).
        for fiscal_year in sorted(by_year, reverse=True):
            if len(history) >= 3:
                break
            chosen: FilingData | None = None
            # Latest-filed report for this year first (amendment over original).
            for filing_date, rcept_no in sorted(
                by_year[fiscal_year], key=lambda fr: fr[0], reverse=True
            ):
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
                chosen = FilingData(
                    ticker=ticker,
                    filing_date=filing_date,
                    filing_type="사업보고서",
                    business_overview=sections.get("business_overview"),
                    risks=sections.get("risks"),
                    mda=sections.get("mda"),
                    fiscal_year=fiscal_year,
                    accession=rcept_no,
                )
                break
            if chosen is not None:
                history.append(chosen)

        if history:
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

        return FilingData(
            ticker=ticker,
            filing_date=fallback_date,
            filing_type="사업보고서",
        )
