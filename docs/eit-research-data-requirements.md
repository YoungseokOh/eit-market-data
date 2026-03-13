# eit-research 데이터 요구사항 분석

출처: `YoungseokOh/eit-research` 코드베이스 분석 (commit 9d432da 기준)

---

## 1. 가격 데이터 (OHLCV)

| 항목 | 값 | 근거 |
|------|-----|------|
| 기본 lookback | **300 거래일** | `providers.py:29`, `yfinance_provider.py:142` |
| 벤치마크 lookback | 300 거래일 | `yfinance_provider.py:439` |
| 최소 동작 lookback | 253 거래일 | 12개월 모멘텀(252일) + 1 |
| 실사용 컬럼 | `close, high, low, date` | `technical.py:267` |
| 미사용 컬럼 | `open, volume` | 현재 지표 계산에 직접 미사용 |

### 기술지표별 최소 거래일

| 지표 | 파라미터 | 최소 거래일 |
|------|----------|-----------|
| 모멘텀 (RoC) | 12m = 252 | **253** ← 최대값 |
| Bollinger Band | 20일 | 20 |
| MACD | (12, 26, 9) | 26 |
| RSI | 14일 | 15 |
| Stochastic | (14, 3, 3) | 14 |
| 단기 모멘텀 | 5/10/20/30일 | 30 |
| 중기 모멘텀 | 1m(21)/3m(63)/6m(126) | 126 |

**결론: pykrx fetch 시 `lookback_days=300` 사용 (현재 기본값과 일치)**

---

## 2. 재무 데이터 (Fundamentals)

| 항목 | 값 | 근거 |
|------|-----|------|
| 수집 분기 수 | **8분기** | `providers.py:40`, `yfinance_provider.py:189` |
| 정렬 순서 | 최신 분기 우선 (내림차순) | `snapshot.py:78` |

### 실사용 분기 필드 (`QuarterlyFinancials`)

**Income Statement**
- `revenue` — 매출액
- `gross_profit` — 매출총이익
- `operating_income` — 영업이익
- `net_income` — 당기순이익
- `ebitda` — EBITDA
- `eps` — 주당순이익
- `interest_expense` — 이자비용

**Balance Sheet**
- `total_assets` — 자산총계
- `total_liabilities` — 부채총계
- `total_equity` — 자본총계
- `current_assets` — 유동자산
- `current_liabilities` — 유동부채
- `total_debt` — 총차입금
- `cash_and_equivalents` — 현금및현금성자산
- `inventory` — 재고자산
- `accounts_receivable` — 매출채권
- `issued_shares` — 발행주식수

**Cash Flow**
- `operating_cash_flow` — 영업활동현금흐름
- `capital_expenditure` — 유형자산취득
- `free_cash_flow` — 잉여현금흐름
- `dividends_paid` — 배당금지급

**종목 단위 (FundamentalData)**
- `market_cap` — 시가총액 (decision_date 기준)
- `last_close_price` — 결정일 종가

### Quantitative Agent 활용 필드

`quantitative_agent.py` + `quantitative.py:150` 기준으로 계산되는 지표:

| 지표 | 필요 필드 |
|------|----------|
| ROA | `net_income`, `total_assets` |
| ROE | `net_income`, `total_equity` |
| Gross Margin | `gross_profit`, `revenue` |
| Operating Margin | `operating_income`, `revenue` |
| Net Margin | `net_income`, `revenue` |
| Current Ratio | `current_assets`, `current_liabilities` |
| Debt/Equity | `total_debt`, `total_equity` |
| Asset Turnover | `revenue`, `total_assets` |
| PE (TTM) | `last_close_price`, `eps` |

---

## 3. 매크로 데이터

`MacroData` 4개 카테고리 전체 사용 (`macro_agent.py:40`):

### rates_policy (금리/통화정책)
- `fed_funds_rate` / 한국: `base_rate` — 기준금리
- `treasury_10y` / `yield_10y` — 국고채 10년
- `treasury_2y` / `yield_3y` — 국고채 3년(한국)
- `yield_curve_spread_10y_2y` / `yield_curve_spread_10y_3y` — 장단기 스프레드
- `policy_stance` — hawkish/neutral/dovish 분류

### inflation_commodities (물가/원자재)
- `cpi_yoy`, `cpi_mom` — CPI 전년비/전월비
- `ppi_yoy` — PPI 전년비
- `oil_wti` — WTI 원유가 (한국: 없음)
- `gold`, `copper` — 금, 구리 (한국: 없음)

### growth_economy (성장/경제)
- `gdp_growth_yoy` — GDP 성장률
- `unemployment_rate` — 실업률
- `trade_balance` — 무역수지 (한국 특화)
- `consumer_confidence` — 소비자신뢰지수 (US)
- `nonfarm_payrolls_k` — 비농업 고용 (US)
- `ism_manufacturing` — ISM 제조업 (US)

### market_risk (시장/리스크)
- `vix` — VIX (US)
- `usd_krw` — 원달러 환율 (한국 특화)
- `ig_credit_spread`, `hy_credit_spread` — 신용스프레드 (US)
- `sp500_level`, `sp500_monthly_return` — S&P500 (US)

---

## 4. 공시/텍스트 데이터 (Filings)

`qualitative_agent.py` + `qualitative.py:64`:

| 필드 | 한국 소스 |
|------|----------|
| `filing_type` | "사업보고서" |
| `filing_date` | 공시접수일 (`rcept_dt`) |
| `business_overview` | 사업의 내용 |
| `risks` | 위험요소 |
| `mda` | 재무상태 및 영업실적 |
| `governance` | (현재 미수집) |

---

## 5. 뉴스 데이터

현재 MVP 기본 계약에서는 뉴스 데이터를 요구하지 않습니다.

- 새 KR bundle은 `news` 필드를 생략할 수 있습니다.
- `eit-research`는 old bundle의 `news`는 읽지만, 기본 DAG에서는 사용하지 않습니다.
- 뉴스 수집 코드는 legacy/diagnostic 경로로만 유지됩니다.

---

## 6. 섹터 데이터

`sector_agent.py:54`:
- `sector_map`: `{ticker: sector_name}`
- `sector_averages`: `{sector: SectorAverages(avg_metrics=...)}`
- `avg_metrics` 계산에 위 Quantitative 지표들 활용

---

## 7. 수집하지만 현재 미사용

| 데이터 | 이유 |
|--------|------|
| `benchmark_prices` | 스냅샷에 저장되나 백테스트 수익률 계산에 미사용 |
| 가격 `open`, `volume` | 기술지표 계산에 직접 사용 없음 |
| `input_hash`, `metadata.*` | 재현성/로깅용, 모델 입력 미사용 |
| 수급/공매도/대차/옵션OI | 코드 없음 (미구현) |

---

## 8. pykrx 수집 우선순위 요약

| 데이터 | 필수 | lookback |
|--------|------|----------|
| OHLCV (close, high, low) | 필수 | 300 거래일 |
| 시가총액 (`market_cap`) | 필수 | 단일 날짜 |
| PER/PBR/EPS (`get_market_fundamental`) | 중요 | 단일 날짜 |
| 섹터 분류 | 필수 | 단일 날짜 |
| 지수 OHLCV (KOSPI 1001) | 중요 | 300 거래일 |
| 투자자 수급 | 선택 | 30일 |
| 공매도 | 선택 | 30일 |
| ETF | 선택 | 300 거래일 |
