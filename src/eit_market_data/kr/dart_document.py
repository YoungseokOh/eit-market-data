"""DART 사업보고서 document-text helpers: section extraction and validation.

Pure text processing over raw DART document bodies — no network access. The
live provider lives in :mod:`eit_market_data.kr.dart_provider`.
"""

from __future__ import annotations

import re

_SECTION_PATTERNS: dict[str, list[str]] = {
    "business_overview": [
        r"사업의\s*내용",
        r"회사의\s*개요",
    ],
    "risks": [
        r"위험\s*요소",
        r"리스크\s*요인",
        r"투자\s*위험\s*요소",
        r"위험관리\s*및\s*파생거래",
        r"재무위험관리정책",
    ],
    "mda": [
        r"재무상태\s*및\s*영업실적",
        r"경영진의\s*논의",
        r"MD&A",
    ],
}


def _clean_document_text(raw: str) -> str:
    text = re.sub(r"<[^>]+>", "\n", raw)
    text = text.replace("&nbsp;", " ").replace("&#160;", " ").replace("&amp;", "&")
    text = text.replace("\r", "\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()


# ---------------------------------------------------------------------------
# Risk-content validator (KR)
#
# 사업보고서 has no single guaranteed "투자위험요소" section, so the risk patterns
# can latch onto non-risk prose that happens to sit near a 위험-keyword: product
# marketing copy (양산/제품 스펙), a credit-rating legend (등급정의/원리금 채무불이행
# table), or a cross-reference stub ("...참고하시기 바랍니다"). Emitting any of
# those as ``risks`` creates a false filing-diff signal. This validator decides
# whether an extracted body is genuine risk-factor / 재무위험관리 prose; the caller
# drops it (→ None) otherwise. "Better empty than wrong".
# ---------------------------------------------------------------------------

_KR_RISK_RE = re.compile(
    r"위험|악영향|부정적|손실|노출|불확실|하락|감소할|미칠\s*수\s*있|리스크"
)
# Credit-rating legend signals (a rating-scale table, not risk prose).
_KR_RATING_RE = re.compile(
    r"등급정의|등급의\s*정의|원리금의?\s*(?:상환|채무불이행)|신용평가사|기업어음의?\s*등급"
)
# Product-marketing / spec-sheet signals.
_KR_MARKETING_RE = re.compile(
    r"양산|출시|나노|소비\s*전력|읽기\s*속도|데이터\s*전송|인치|픽셀|화소|Gbps|GB|TB"
)


def _looks_like_risk_text(text: str | None) -> bool:
    """True if ``text`` reads as genuine KR 위험요소 / 재무위험관리 prose.

    Rejects credit-rating legends, product-marketing copy, cross-reference
    stubs, and bodies too short or too sparse in risk language. 재무위험관리정책 /
    시장위험 financial-risk-management notes are accepted (they carry dense 위험
    vocabulary); marketing spec copy and rating tables are not.
    """
    if not text:
        return False
    joined = re.sub(r"\s+", " ", text).strip()
    if len(joined) < 200:
        return False
    head = joined[:1500]
    # Credit-rating legend (rating-definition vocabulary clustered up front).
    if len(_KR_RATING_RE.findall(head)) >= 2:
        return False
    risk_hits = len(_KR_RISK_RE.findall(head))
    marketing_hits = len(_KR_MARKETING_RE.findall(head))
    # Marketing / spec copy: many product terms, little risk language.
    if marketing_hits >= 3 and risk_hits < 8:
        return False
    if risk_hits < 5:
        return False
    return True


def _is_toc_chunk(chunk: str) -> bool:
    """Heuristic: a table-of-contents entry, not a real section body.

    DART 사업보고서 ToC lines use dotted/dashed page leaders (``------ 39``)
    near the start of the chunk, whereas a real section body opens with prose.
    """
    head = chunk[:400]
    return bool(re.search(r"-{5,}", head)) or bool(re.search(r"\.{5,}", head))


def _extract_sections(
    doc_text: str, max_chars: int = 8000, min_body: int = 60
) -> dict[str, str]:
    """Extract 사업보고서 sections, skipping table-of-contents header hits.

    Every section header (across all section patterns) is collected as a
    boundary. For each section we iterate its header matches newest-found-first
    and pick the first whose following body is substantial (``>= min_body``) and
    is not a ToC stub. The body runs from the chosen header to the next header of
    *any other* section (capped at ``max_chars``). This lets the risk section be
    recovered from older reports where the only risk discussion lives under a
    deep ``5. 위험관리 및 파생거래`` heading that follows a ToC entry of the same name.
    """
    plain = _clean_document_text(doc_text)

    # Collect header start positions per section.
    section_matches: dict[str, list[re.Match[str]]] = {}
    for section_name, patterns in _SECTION_PATTERNS.items():
        found: list[re.Match[str]] = []
        for pattern in patterns:
            found.extend(re.finditer(pattern, plain, flags=re.IGNORECASE))
        found.sort(key=lambda m: m.start())
        section_matches[section_name] = found

    extracted: dict[str, str] = {}
    for section_name, matches in section_matches.items():
        # Boundaries are headers of *other* sections only, so dense repeats of a
        # section's own keyword (e.g. multiple 재무위험관리정책 hits) don't truncate it.
        other_starts = sorted(
            m.start()
            for name, ms in section_matches.items()
            if name != section_name
            for m in ms
        )
        # For the risk section, collect *all* substantive non-ToC candidate
        # bodies that pass the content validator and pick the one with the
        # highest risk-language density (a dedicated 위험요소 / 시장위험 section beats
        # a thin financial-risk note). If none pass, leave ``risks`` unset (None)
        # rather than emit marketing copy or a rating legend.
        risk_candidates: list[tuple[int, int, str]] = []
        for m in matches:
            body_start = m.end()
            end = len(plain)
            for s in other_starts:
                if s >= body_start:
                    end = s
                    break
            if end <= body_start:
                end = min(body_start + max_chars, len(plain))
            chunk = plain[body_start:end].strip()
            if len(chunk) > max_chars:
                chunk = chunk[:max_chars]
            if len(chunk) < min_body or _is_toc_chunk(chunk):
                continue
            if section_name == "risks":
                if not _looks_like_risk_text(chunk):
                    continue
                density = len(_KR_RISK_RE.findall(chunk[:2000]))
                risk_candidates.append((density, -body_start, chunk))
                continue
            extracted[section_name] = chunk
            break

        if section_name == "risks" and risk_candidates:
            # Highest density wins; tie-break on earliest occurrence.
            risk_candidates.sort(key=lambda c: (c[0], c[1]), reverse=True)
            extracted["risks"] = risk_candidates[0][2]

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
