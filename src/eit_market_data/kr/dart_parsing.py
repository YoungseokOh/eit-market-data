"""Pure parsing/normalization helpers for DART financial statements.

Everything here is side-effect free: amount/EPS/date parsers, the account-name
map, quarter cumulative-to-standalone decomposition, and report-list entry
extraction. The live provider lives in :mod:`eit_market_data.kr.dart_provider`.
"""

from __future__ import annotations

import re
from datetime import date
from typing import Any

from eit_market_data.kr.market_helpers import date_to_yyyymmdd

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


def _date_to_yyyymmdd(value: date) -> str:
    return date_to_yyyymmdd(value)


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
    value = _parse_signed_number(raw)
    return round(value, 1) if value is not None else None


def _parse_eps(raw: Any) -> float | None:
    """Parse DART EPS value in native KRW per share (no unit conversion)."""
    value = _parse_signed_number(raw)
    return round(value, 2) if value is not None else None


def _parse_signed_number(raw: Any) -> float | None:
    """Parse a DART numeric string: comma-separated, ``(...)`` marks negative."""
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
    return -value if negative else value


def _fiscal_year_from_report_nm(report_nm: str, filing_date: date | None) -> int | None:
    """Derive the reported fiscal year from a report name like ``사업보고서 (2023.12)``.

    Falls back to ``filing_date.year - 1`` (annual reports are filed the year
    after the fiscal year they cover).
    """
    m = re.search(r"\((\d{4})", report_nm or "")
    if m:
        return int(m.group(1))
    if filing_date is not None:
        return filing_date.year - 1
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
