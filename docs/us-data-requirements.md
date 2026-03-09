# US 데이터 요구사항 분석

출처: `YoungseokOh/eit-research` 코드베이스 분석 기준

---

## 1. yfinance Provider

**API 키**: 불필요 (라이브러리 설치만 필요)
**동시성**: `asyncio.Semaphore(3)`, 명시적 retry 없음

### 가격 데이터 (`fetch_prices`)

| 항목 | 값 |
|------|-----|
| lookback | **300 거래일** |
| 반환 | `PriceBar(date, open, high, low, close, volume)` |
| adjusted | `auto_adjust=True` → **수정주가 기준** |
| 조회 기간 | `as_of - lookback*1.6` 캘린더일 ~ `as_of` |
| 필터 | `bar_date > as_of` 제거 (look-ahead 방지) |

### 재무 데이터 (`fetch_fundamentals`)

| 항목 | 값 |
|------|-----|
| 분기 수 | **8분기** |
| 소스 | `quarterly_income_stmt`, `quarterly_balance_sheet`, `quarterly_cashflow` |
| 공시 지연 | **60일** 가정 (`col_date + 60 <= as_of`) |
| 분기 라벨 | `YYYYQn` (e.g. `2024Q1`) |

**필드 매핑 (손익계산서)**
| 내부 필드 | yfinance 컬럼 |
|-----------|-------------|
| `revenue` | `Total Revenue` |
| `cost_of_goods_sold` | `Cost Of Revenue` |
| `gross_profit` | `Gross Profit` |
| `operating_income` | `Operating Income` |
| `net_income` | `Net Income` |
| `ebitda` | `EBITDA` |
| `eps` | `Basic EPS` |
| `interest_expense` | `Interest Expense` |

**필드 매핑 (재무상태표)**
| 내부 필드 | yfinance 컬럼 |
|-----------|-------------|
| `total_assets` | `Total Assets` |
| `total_liabilities` | `Total Liabilities Net Minority Interest` |
| `total_equity` | `Stockholders Equity` |
| `current_assets` | `Current Assets` |
| `current_liabilities` | `Current Liabilities` |
| `total_debt` | `Total Debt` |
| `cash_and_equivalents` | `Cash And Cash Equivalents` |
| `inventory` | `Inventory` |
| `accounts_receivable` | `Accounts Receivable` |
| `issued_shares` | `Ordinary Shares Number` |

**필드 매핑 (현금흐름)**
| 내부 필드 | yfinance 컬럼 |
|-----------|-------------|
| `operating_cash_flow` | `Operating Cash Flow` |
| `capital_expenditure` | `Capital Expenditure` |
| `free_cash_flow` | `Free Cash Flow` |
| `dividends_paid` | `Cash Dividends Paid` |

### 뉴스 데이터 (`fetch_news`)

| 항목 | 값 |
|------|-----|
| lookback | **30일** |
| 최대 건수 | **15건** |
| 필드 | `date, source, headline, summary` |
| 날짜 소스 | `providerPublishTime` → `content.pubDate` fallback |

### 섹터 (`fetch_sector_map`)

- `ticker.info["sector"]` 조회, 없으면 `"General"`

### 벤치마크 (`fetch_benchmark`)

- **`^GSPC`** (S&P 500), lookback 300 거래일

---

## 2. FRED Provider

**API 키**: `FRED_API_KEY` 필수 (없으면 `ValueError`)
**Rate limit**: 없음 (fail-soft 패턴)

### 매크로 지표 전체 목록

#### rates_policy (금리/통화정책)

| 지표 key | FRED Series ID | 주기 | lookback |
|----------|---------------|------|----------|
| `fed_funds_rate` | `DFF` | daily | 90일 |
| `treasury_10y` | `DGS10` | daily | 90일 |
| `treasury_2y` | `DGS2` | daily | 90일 |
| `yield_curve_spread_10y_2y` | (파생) | — | — |
| `policy_stance` | (파생, fed_funds_rate 기반) | — | — |

#### inflation_commodities (물가/원자재)

| 지표 key | FRED Series ID | 주기 | lookback |
|----------|---------------|------|----------|
| `cpi_yoy` | `CPIAUCSL` | monthly | ~400일 (전년비 계산) |
| `cpi_mom` | `CPIAUCSL` | monthly | 90일 |
| `ppi_yoy` | `PPIACO` | monthly | ~400일 |
| `oil_wti` | `DCOILWTICO` | daily | 90일 |
| `gold` | `GOLDAMGBD228NLBM` | daily | 90일 |
| `copper` | `PCOPPUSDM` | monthly | 90일 |

#### growth_economy (성장/경제)

| 지표 key | FRED Series ID | 주기 | lookback |
|----------|---------------|------|----------|
| `gdp_growth_yoy` | `A191RL1Q225SBEA` | quarterly | 180일 |
| `unemployment_rate` | `UNRATE` | monthly | 90일 |
| `consumer_confidence` | `UMCSENT` | monthly | 90일 |
| `nonfarm_payrolls_k` | `PAYEMS` | monthly | 90일 (전월 대비 증감) |
| `ism_manufacturing` | `NAPM` (fallback: `MANEMP`) | monthly | 90일 |

#### market_risk (시장/리스크)

| 지표 key | FRED Series ID | 주기 | lookback |
|----------|---------------|------|----------|
| `vix` | `VIXCLS` | daily | 90일 |
| `ig_credit_spread` | `BAMLC0A0CM` | daily | 90일 |
| `hy_credit_spread` | `BAMLH0A0HYM2` | daily | 90일 |
| `sp500_level` | yfinance `^GSPC` (보강) | daily | — |
| `sp500_monthly_return` | yfinance `^GSPC` (파생) | monthly | — |

---

## 3. EDGAR Provider

**API 키**: `SEC_EDGAR_USER_AGENT` 필수 (`"Name email@example.com"` 형식)
**동시성**: `asyncio.Semaphore(5)` + 요청마다 `0.2s` sleep
**Retry**: 없음 (실패 시 warning 후 빈 값 반환)

### 수집 대상

| 항목 | 값 |
|------|-----|
| 보고서 유형 | `10-K`, `10-K/A` 만 |
| `10-Q` | 현재 미수집 |

### 파싱 섹션

| 내부 필드 | 10-K 섹션 |
|-----------|----------|
| `business_overview` | Item 1 — Business |
| `risks` | Item 1A — Risk Factors |
| `mda` | Management Discussion & Analysis (Item 7) |
| `governance` | Item 10/14 — Directors / Corporate Governance |

### 처리 흐름

1. ticker → CIK (`company_tickers.json` SEC 공개 데이터)
2. CIK → `submissions/CIKxxxx.json` → 최근 10-K URL 탐색
3. 문서 다운로드 → BeautifulSoup HTML 파싱 → 태그 제거
4. 섹션 헤더 정규식 매칭 → 다음 Item 헤더까지 슬라이스
5. 섹션당 최대 **8,000자**, 50자 이하 버림

---

## 4. 환경변수 요약

| 변수 | Provider | 필수 여부 |
|------|----------|---------|
| `FRED_API_KEY` | `FredMacroProvider` | 필수 |
| `SEC_EDGAR_USER_AGENT` | `EdgarFilingProvider` | 필수 |
| yfinance | `YFinanceProvider` | 불필요 |

---

## 5. eit-market-data 구현 현황

| 데이터 | eit-market-data 구현 | 비고 |
|--------|---------------------|------|
| US 가격 | `YFinanceProvider` | 완료 |
| US 재무 | `YFinanceProvider` | 완료 |
| US 뉴스 | `YFinanceProvider` | 완료 |
| US 매크로 | `FredMacroProvider` | 완료 |
| US 공시 | `EdgarFilingProvider` | 완료 |
| KR 가격 | `PykrxProvider` | 완료 |
| KR 재무 | `DartProvider` | 완료 |
| KR 공시 | `DartProvider` | 완료 |
| KR 매크로 | `EcosMacroProvider` | 완료 |
| KR 뉴스 | (stub) | pykrx 미지원 |
