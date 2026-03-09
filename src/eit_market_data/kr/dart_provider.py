"""OpenDartReader-based Korean fundamentals and filing provider."""

from __future__ import annotations

import asyncio
import logging
import os
import re
from datetime import date, timedelta
from typing import Any

from eit_market_data.schemas.snapshot import FilingData, FundamentalData, QuarterlyFinancials

logger = logging.getLogger(__name__)

_DART_SEMAPHORE = asyncio.Semaphore(2)

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
    "total_debt": ["단기차입금", "차입금합계", "총차입금"],
    "eps": ["주당순이익", "주당이익", "기본주당이익"],
    "interest_expense": ["이자비용"],
    "operating_cash_flow": ["영업활동현금흐름", "영업활동으로 인한 현금흐름"],
    "capital_expenditure": ["유형자산의취득", "유형자산취득"],
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


def _normalize_ticker(ticker: str) -> str:
    digits = "".join(ch for ch in str(ticker) if ch.isdigit())
    return digits.zfill(6) if digits else str(ticker)


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


def _parse_amount_to_million(raw: Any) -> float | None:
    """Parse DART amount text to KRW millions.

    DART financial statement values are typically in KRW thousands.
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
    return round(value / 1000.0, 1)


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


class DartProvider:
    """Korean fundamentals/filings provider backed by OpenDartReader."""

    def __init__(self, api_key: str | None = None) -> None:
        try:
            import OpenDartReader
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

        self._dart = OpenDartReader.OpenDartReader(key)
        self._corp_cache: dict[str, str | None] = {}
        self._corp_list: Any = None

    async def fetch_fundamentals(
        self, ticker: str, as_of: date, n_quarters: int = 8
    ) -> FundamentalData:
        norm_ticker = _normalize_ticker(ticker)
        async with _DART_SEMAPHORE:
            try:
                return await asyncio.to_thread(
                    self._fetch_fundamentals_sync, norm_ticker, as_of, n_quarters
                )
            except Exception as e:
                logger.warning("DART fundamentals fetch failed for %s: %s", norm_ticker, e)
                return FundamentalData(ticker=norm_ticker)

    async def fetch_filing(self, ticker: str, as_of: date) -> FilingData:
        norm_ticker = _normalize_ticker(ticker)
        async with _DART_SEMAPHORE:
            try:
                return await asyncio.to_thread(self._fetch_filing_sync, norm_ticker, as_of)
            except Exception as e:
                logger.warning("DART filing fetch failed for %s: %s", norm_ticker, e)
                return FilingData(ticker=norm_ticker)

    # ------------------------------------------------------------------
    # Fundamentals
    # ------------------------------------------------------------------

    def _get_corp_list(self):  # noqa: ANN202
        if self._corp_list is None:
            self._corp_list = self._dart.corp_code
        return self._corp_list

    def _ticker_to_corp_code(self, ticker: str) -> str | None:
        if ticker in self._corp_cache:
            return self._corp_cache[ticker]

        try:
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

    def _fetch_finstate(self, corp_code: str, year: str, reprt_code: str):  # noqa: ANN202
        for fs_div in ("CFS", "OFS"):
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
                return df
        return None

    def _pick_account_value(self, df: Any, candidates: list[str]) -> float | None:
        if df is None or df.empty or "account_nm" not in df.columns:
            return None

        names = df["account_nm"].fillna("").astype(str).str.strip()
        for candidate in candidates:
            exact = df.loc[names == candidate]
            if not exact.empty:
                val = _parse_amount_to_million(exact.iloc[0].get("thstrm_amount"))
                if val is not None:
                    return val

        for candidate in candidates:
            partial = df.loc[names.str.contains(candidate, regex=False)]
            if not partial.empty:
                val = _parse_amount_to_million(partial.iloc[0].get("thstrm_amount"))
                if val is not None:
                    return val
        return None

    def _quarter_candidates(self, as_of: date) -> list[tuple[str, date, str, str]]:
        candidates: list[tuple[str, date, str, str]] = []
        quarter_defs = [
            ("Q4", 12, 31, "11011"),
            ("Q3", 9, 30, "11014"),
            ("Q2", 6, 30, "11012"),
            ("Q1", 3, 31, "11013"),
        ]
        for year in range(as_of.year, as_of.year - 10, -1):
            for q_label, month, day, reprt_code in quarter_defs:
                period_end = date(year, month, day)
                report_date = period_end + timedelta(days=60)
                if report_date <= as_of:
                    candidates.append((f"{year}{q_label}", report_date, str(year), reprt_code))
        candidates.sort(key=lambda x: x[1], reverse=True)
        return candidates

    def _fetch_fundamentals_sync(
        self, ticker: str, as_of: date, n_quarters: int
    ) -> FundamentalData:
        corp_code = self._ticker_to_corp_code(ticker)
        if not corp_code:
            return FundamentalData(ticker=ticker)

        quarters: list[QuarterlyFinancials] = []
        for fiscal_q, report_date, bsns_year, reprt_code in self._quarter_candidates(as_of):
            if len(quarters) >= n_quarters:
                break

            try:
                df_fin = self._fetch_finstate(corp_code, bsns_year, reprt_code)
            except Exception:
                df_fin = None
            if df_fin is None or df_fin.empty:
                continue

            q_data = {
                "revenue": self._pick_account_value(df_fin, _ACCOUNT_MAP["revenue"]),
                "operating_income": self._pick_account_value(
                    df_fin, _ACCOUNT_MAP["operating_income"]
                ),
                "net_income": self._pick_account_value(df_fin, _ACCOUNT_MAP["net_income"]),
                "total_assets": self._pick_account_value(df_fin, _ACCOUNT_MAP["total_assets"]),
                "total_liabilities": self._pick_account_value(
                    df_fin, _ACCOUNT_MAP["total_liabilities"]
                ),
                "total_equity": self._pick_account_value(df_fin, _ACCOUNT_MAP["total_equity"]),
                "current_assets": self._pick_account_value(
                    df_fin, _ACCOUNT_MAP["current_assets"]
                ),
                "current_liabilities": self._pick_account_value(
                    df_fin, _ACCOUNT_MAP["current_liabilities"]
                ),
                "gross_profit": self._pick_account_value(df_fin, _ACCOUNT_MAP["gross_profit"]),
                "total_debt": self._pick_account_value(df_fin, _ACCOUNT_MAP["total_debt"]),
                "eps": self._pick_account_value(df_fin, _ACCOUNT_MAP["eps"]),
                "interest_expense": self._pick_account_value(
                    df_fin, _ACCOUNT_MAP["interest_expense"]
                ),
                "operating_cash_flow": self._pick_account_value(
                    df_fin, _ACCOUNT_MAP["operating_cash_flow"]
                ),
                "capital_expenditure": self._pick_account_value(
                    df_fin, _ACCOUNT_MAP["capital_expenditure"]
                ),
            }

            if all(v is None for v in q_data.values()):
                continue

            quarters.append(
                QuarterlyFinancials(
                    fiscal_quarter=fiscal_q,
                    report_date=report_date,
                    **q_data,
                )
            )

        return FundamentalData(
            ticker=ticker,
            quarters=quarters,
        )

    # ------------------------------------------------------------------
    # Filing
    # ------------------------------------------------------------------

    def _fetch_filing_sync(self, ticker: str, as_of: date) -> FilingData:
        corp_code = self._ticker_to_corp_code(ticker)
        if not corp_code:
            return FilingData(ticker=ticker)

        try:
            report_list = self._dart.list(
                corp_code,
                start=f"{max(as_of.year - 10, 2000)}0101",
                end=_date_to_yyyymmdd(as_of),
                kind="A",
            )
        except Exception as e:
            logger.warning("DART report list fetch failed for %s: %s", ticker, e)
            return FilingData(ticker=ticker)

        if report_list is None or report_list.empty:
            return FilingData(ticker=ticker)

        reports = report_list.copy()
        if "rcept_dt" in reports.columns:
            reports = reports.loc[
                reports["rcept_dt"].fillna("").astype(str) <= _date_to_yyyymmdd(as_of)
            ]
        if "reprt_code" in reports.columns:
            annual = reports.loc[reports["reprt_code"].astype(str) == "11011"]
            if not annual.empty:
                reports = annual
        if reports.empty:
            return FilingData(ticker=ticker)

        if "rcept_dt" in reports.columns:
            reports = reports.sort_values("rcept_dt", ascending=False)
        latest = reports.iloc[0]

        rcept_no = str(latest.get("rcept_no", "")).strip()
        filing_date = _parse_date_yyyymmdd(latest.get("rcept_dt"))
        if not rcept_no:
            return FilingData(ticker=ticker, filing_date=filing_date, filing_type="사업보고서")

        try:
            doc = self._dart.document(rcept_no)
            if isinstance(doc, bytes):
                doc_text = doc.decode("utf-8", errors="ignore")
            else:
                doc_text = str(doc)
            sections = _extract_sections(doc_text)
        except Exception as e:
            logger.warning("DART document fetch/parse failed for %s: %s", ticker, e)
            sections = {}

        return FilingData(
            ticker=ticker,
            filing_date=filing_date,
            filing_type="사업보고서",
            business_overview=sections.get("business_overview"),
            risks=sections.get("risks"),
            mda=sections.get("mda"),
        )
