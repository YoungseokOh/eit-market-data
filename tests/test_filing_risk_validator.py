"""Unit tests for the filing risk-content validators (US + KR).

These guard the "better empty than wrong" contract: genuine Item 1A / 위험요소
prose is accepted, while exec-officer bios, forward-looking preambles, credit-
rating legends, and product-marketing copy are rejected.
"""

from __future__ import annotations

from eit_market_data.edgar_provider import _looks_like_risk_text as us_valid
from eit_market_data.kr.dart_provider import _looks_like_risk_text as kr_valid


def test_us_accepts_genuine_item_1a() -> None:
    good = (
        "Our business, operations and financial results are subject to various "
        "risks and uncertainties, including those described below, that could "
        "adversely affect our business, results of operations and financial "
        "condition and could cause our actual results to differ materially from "
        "those projected. The risks described below are not the only ones we "
        "face; additional risks not presently known to us or that we currently "
        "deem immaterial may also impair our business. If any of these risks "
        "actually occurs, our business could be materially and adversely harmed."
    )
    assert us_valid(good) is True


def test_us_rejects_executive_officer_bios() -> None:
    bad = (
        "Information About Our Executive Officers The following table provides "
        "information regarding our executive officers as of December 18, 2025: "
        "Name and Title Age Position. John Smith 57 Chief Executive Officer has "
        "served as our CEO since 2010. Jane Doe 49 Chief Financial Officer has "
        "served in that role since 2015 and previously held senior finance roles."
    )
    assert us_valid(bad) is False


def test_us_rejects_forward_looking_preamble() -> None:
    bad = (
        "This Annual Report on Form 10-K contains forward-looking statements "
        "within the meaning of the Private Securities Litigation Reform Act of "
        "1995. You should read the following discussion in conjunction with the "
        "consolidated financial statements and the notes thereto included "
        "elsewhere in this report. Actual results could differ materially from "
        "those projected. Words such as expects, anticipates and intends are "
        "intended to identify forward-looking statements."
    )
    assert us_valid(bad) is False


def test_us_rejects_rating_legend() -> None:
    bad = (
        "The following sets forth the rating definitions. AAA: highest credit "
        "quality. AA+ AA AA- BBB BB+ B CCC CC C D. An obligor rated AAA has "
        "extremely strong capacity to meet its financial commitments. "
        "Creditworthiness is assessed by the rating agency accordingly."
    )
    assert us_valid(bad) is False


def test_kr_accepts_financial_risk_management() -> None:
    good = (
        "가. 재무위험관리정책 당사는 영업활동에서 파생되는 시장위험, 신용위험, 유동성위험 "
        "등을 최소화하는데 중점을 두고 재무위험을 관리하고 있으며, 이를 위해 각각의 위험요인에 "
        "대해 면밀하게 모니터링 및 대응하고 있습니다. 외환위험은 환율 변동에 따라 손익에 악영향을 "
        "미칠 수 있으며 회사는 이러한 위험에 노출되어 있습니다. 시장위험은 환율, 이자율, 주가 등 "
        "시장가격의 변동으로 인하여 금융상품의 공정가치나 미래현금흐름이 변동할 위험을 의미하며, "
        "이러한 위험은 당사의 재무상태 및 경영성과에 부정적인 영향을 미칠 수 있습니다. 신용위험은 "
        "거래상대방이 계약상 의무를 이행하지 못하여 당사에 재무적 손실을 초래할 위험을 말합니다."
    )
    assert kr_valid(good) is True


def test_kr_rejects_rating_legend() -> None:
    bad = (
        "가 내포되어 있음 CC 원리금의 채무불이행이 발생할 가능성이 높음 C 원리금의 채무불이행이 "
        "발생할 가능성이 지극히 높음 D 현재 채무불이행 상태에 있음 ※ AA ~ B등급까지는 상대적 "
        "우열에 따라 + 또는 - 기호를 부여함 [참고] 국내 신용평가사 기업어음의 등급정의 신용등급 "
        "등급의 정의"
    )
    assert kr_valid(bad) is False


def test_kr_rejects_product_marketing() -> None:
    bad = (
        "로부터 데이터 보호하는 내구성 지원 Automotive 초저전력 차량용 UFS 3.1 양산 업계 "
        "최저 소비 전력을 가진 차량용 인포테인먼트 UFS 3.1 양산으로 전기차, 자율주행차 등에 "
        "최적화 전 세대 제품 대비 33% 낮은 소비 전력 256GB 기준 최대 읽기 속도 2,000 출시"
    )
    assert kr_valid(bad) is False


def test_validators_reject_empty_and_short() -> None:
    assert us_valid(None) is False
    assert us_valid("") is False
    assert kr_valid("위험 위험 위험") is False
