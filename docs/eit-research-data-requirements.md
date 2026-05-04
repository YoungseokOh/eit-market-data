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

## 8. KR 공식 수집 우선순위 요약

| 데이터 | 필수 | lookback |
|--------|------|----------|
| OHLCV (close, high, low) | 필수 | 300 거래일 |
| 시가총액 (`market_cap`) | 필수 | 단일 날짜 |
| PER/PBR/EPS (`get_market_fundamental`) | 과거 기준/legacy(현재 KR 공식 경로 미사용) | 단일 날짜 |
| 섹터 분류 | 필수 | 단일 날짜 |
| 지수 OHLCV (KOSPI 1001) | 중요 | 300 거래일 |
| 투자자 수급 | 선택 | 30일 |
| 공매도 | 선택 | 30일 |
| ETF | 선택 | 300 거래일 |

실제 KR 공식 경로에서는 가격/마켓/섹터/벤치마크 조회를 `pykrx` 기반으로 처리하고,
replay에서 필요한 `market_cap`/`last_close_price`는 DART + 로컬 보조 스냅샷(`cap_daily`) 계열과 결합해 사용합니다.

KOSPI200 local run에서 OpenDART timeout 이후 `--dart-mode cache_only`로 재개한 경우,
`fundamental_tickers`/`filing_tickers`는 객체 수일 뿐 DART 완전 coverage가 아닙니다.
OpenDART live 호출은 전체/KOSPI200 ticker를 넓게 반복 실행하지 않고, explicit universe 기반의
저속 cache backfill(`5s+` delay, cached ticker skip, progress/resume, transient 즉시 중단)에만 사용합니다.
timeout/rate-limit 신호 이후에는 KRX/pykrx market data 수집을 멈추지 말고 `--dart-mode cache_only`로 이어갑니다.
controlled backfill의 `progress.json`은 재개 상태 파일입니다. cache size limit 때문에 key가
evict될 수 있으므로 리서치 투입 전에는 실제 `data/dart_cache/`에서 대상 universe/month의
fundamental key가 모두 있는지와 filing optional miss를 별도로 검증합니다.
OpenDART document API가 최신 `[첨부정정]사업보고서` 본문을 `014`로 반환하면,
provider는 같은 `as_of` 이전의 다음 retrievable 사업보고서를 사용합니다. 이 경우 coverage는
채워질 수 있지만 filing freshness는 실제 `filing_date`로 별도 설명합니다.
운영 실수 방지: `014`를 filing 결측으로 바로 기록하지 않습니다. 먼저 같은 `as_of` 이전의
사업보고서 후보 목록, 실제 `doc:<rcept_no>` cache, fallback 후 `filing_date`를 확인합니다.
KRX ticker는 숫자 6자리만 있다고 가정하지 않습니다. 알파뉴메릭 코드가 universe에 있으면
그 값을 그대로 key로 사용해야 하며, 숫자만 추출한 대체 코드는 다른 종목 데이터를 섞을 수 있습니다.
리서치 투입 전에는 별도로 아래를 확인합니다:

- `quarters_nonempty`: DART 재무 분기 데이터가 실제 있는 종목 수
- `filing_text_nonempty`: 공시 텍스트 섹션이 실제 있는 종목 수
- `missing_fundamental`: target month cache에서 빠진 DART fundamental key 수
- `missing_filing`: target month cache에서 빠진 filing key 수. 기본 filing optional run에서는
  `filing_empty`와 함께 설명 가능
- `latest_quarter_distribution`: fundamental key가 있어도 최신 분기가 낡았는지 확인
- `market_cap_nonnull`: KRX/pykrx 시가총액 coverage
- `last_close_nonnull`: KRX/pykrx 종가 coverage

리서치 투입 가능 여부는 특정 날짜의 과거 run을 기준으로 삼지 말고,
현재 대상 universe와 `--as-of`에 대해 생성된 run root에서 판단합니다.
운영자가 확인해야 하는 최소 기준은 아래와 같습니다:

- run root: `out/<label>/runs/<AS_OF>/kr_full_<universe>`
- final validation: `failed=0`, `degraded=0` 또는 degradation 사유가 명시됨
- market data: price/market cap/last close/sector map이 대상 universe 전체를 채움
- price last date: `decision_date`보다 미래가 아니며, 휴장일이면 직전 거래일로 설명 가능
- execution date: `decision_date` 다음 KRX 영업일, 불가 시 next weekday fallback 사유가 명시됨
- DART actual coverage: `fundamental_tickers`/`filing_tickers` 객체 수가 아니라
  `quarters_nonempty`/`filing_text_nonempty`로 보고함
- DART latest quarter distribution: 아직 발표되지 않은 분기는 결측으로 인정하고,
  사용 가능한 최신 분기 분포를 별도로 적음
