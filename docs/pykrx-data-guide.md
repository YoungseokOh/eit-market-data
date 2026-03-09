# pykrx 데이터 가이드

pykrx로 수집 가능한 데이터 종류와 로컬 저장 전략 정리.

---

## 데이터 카테고리

### 1. 주식 OHLCV / 가격

| 함수 | 반환 컬럼 | 조회 축 |
|------|-----------|---------|
| `get_market_ohlcv` | 시가, 고가, 저가, 종가, 거래량, 거래대금, 등락률 | 날짜별 or 종목별 |
| `get_market_price_change` | 종목명, 시가, 종가, 변동폭, 등락률, 거래량, 거래대금 | 기간 요약 |

### 2. 시가총액 / 기본지표

| 함수 | 반환 컬럼 | 비고 |
|------|-----------|------|
| `get_market_cap` | 시가총액, 거래량, 거래대금, 상장주식수, 외국인보유주식수 | 전종목 or 단일종목 |
| `get_market_fundamental` | BPS, PER, PBR, EPS, DIV, DPS | 밸류에이션 지표 |

### 3. 투자자별 수급

| 함수 | 반환 컬럼 |
|------|-----------|
| `get_market_trading_value_by_date` | 기관합계, 기타법인, 개인, 외국인합계, 전체 |
| `get_market_trading_volume_by_date` | 기관합계, 기타법인, 개인, 외국인합계, 전체 |
| `get_market_trading_value_by_investor` | 매도, 매수, 순매수 (index: 투자자구분) |
| `get_market_trading_volume_by_investor` | 매도, 매수, 순매수 (index: 투자자구분) |
| `get_market_net_purchases_of_equities` | 종목명, 매도/매수/순매수 거래량·거래대금 |

### 4. 외국인 한도소진율

| 함수 | 반환 컬럼 |
|------|-----------|
| `get_exhaustion_rates_of_foreign_investment` | 상장주식수, 보유수량, 지분율, 한도수량, 한도소진율 |

### 5. 지수 (인덱스)

| 함수 | 반환 컬럼 |
|------|-----------|
| `get_index_ohlcv` | 시가, 고가, 저가, 종가, 거래량 |
| `get_index_fundamental` | 종가, 등락률, PER, 선행PER, PBR, 배당수익률 |
| `get_index_price_change` | 시가, 종가, 등락률, 거래량, 거래대금 |
| `get_index_listing_date` | 기준시점, 발표시점, 기준지수, 종목수 |
| `get_index_portfolio_deposit_file` | 종목 구성 비중 |

주요 지수 코드:
- `1001` — KOSPI
- `2001` — KOSDAQ
- `1028` — KOSPI200

### 6. 공매도

| 함수 | 반환 컬럼 |
|------|-----------|
| `get_shorting_status_by_date` | 공매도, 잔고, 공매도금액, 잔고금액 |
| `get_shorting_volume_by_ticker` | 공매도, 매수, 비중 |
| `get_shorting_value_by_ticker` | 공매도, 매수, 비중 (금액 기준) |
| `get_shorting_volume_by_date` | 공매도, 매수, 비중 |
| `get_shorting_investor_volume_by_date` | 기관, 개인, 외국인, 기타, 합계 |
| `get_shorting_investor_value_by_date` | 기관, 개인, 외국인, 기타, 합계 |
| `get_shorting_balance_by_date` | 공매도잔고, 상장주식수, 공매도금액, 시가총액, 비중 |
| `get_shorting_volume_top50` | 순위, 공매도거래대금, 공매도비중, 직전40일평균, 증가율 등 |
| `get_shorting_balance_top50` | 순위, 공매도잔고, 상장주식수, 비중 |

### 7. ETF / ETN / ELW

| 함수 | 반환 컬럼 |
|------|-----------|
| `get_etf_ohlcv_by_date` | NAV, 시가, 고가, 저가, 종가, 거래량, 거래대금, 기초지수 |
| `get_etf_ohlcv_by_ticker` | NAV, 시가, 고가, 저가, 종가, 거래량, 거래대금, 기초지수 |
| `get_etf_price_change_by_ticker` | 시가, 종가, 변동폭, 등락률, 거래량, 거래대금 |
| `get_etf_portfolio_deposit_file` | 계약수, 금액, 비중 |
| `get_etf_price_deviation` | 종가, NAV, 괴리율 |
| `get_etf_tracking_error` | NAV, 지수, 추적오차율 |
| `get_etf_trading_volume_and_value` | 투자자별 매도/매수/순매수 거래량·거래대금 |

---

## 로컬 저장 전략

### 디렉토리 구조

```
data/
├── market/
│   ├── ohlcv/           # 주가 OHLCV (parquet, 일별 append)
│   ├── cap/             # 시가총액 (parquet)
│   ├── fundamental/     # PER/PBR/EPS (parquet)
│   ├── investor/        # 투자자 수급 (parquet)
│   ├── foreign/         # 외국인 한도소진율 (parquet)
│   └── shorting/        # 공매도 (parquet)
├── index/
│   ├── ohlcv/           # 지수 OHLCV (parquet)
│   └── fundamental/     # 지수 밸류에이션 (parquet)
├── etf/
│   ├── ohlcv/           # ETF OHLCV (parquet)
│   ├── tracking/        # 추적오차/괴리율 (parquet)
│   └── portfolio/       # 구성종목 (parquet)
└── meta/
    ├── tickers.csv      # 종목코드-종목명 매핑 (일별 스냅샷)
    └── index_list.csv   # 지수 목록
```

### 파일 포맷

| 데이터 종류 | 포맷 | 이유 |
|------------|------|------|
| 시계열 대용량 (OHLCV, 수급, 시총) | Parquet | 컬럼형 압축, dtype 보존, 빠른 필터 |
| 메타/매핑 (티커명, 지수목록) | CSV | 사람이 바로 확인 가능 |

### 파티션 전략

```
ohlcv/market=KOSPI/year=2024/month=01/data.parquet
```

또는 단순하게:

```
ohlcv/KOSPI_2024-01.parquet
```

### 스키마 컨벤션

- 원본 한글 컬럼명 유지 (데이터 신뢰성)
- 메타 컬럼 추가: `collected_at`, `source_fn`, `pykrx_version`
- 정정주가(`adjusted=True`) 여부를 파일명 또는 컬럼으로 명시

---

## 수집 스크립트

```bash
python scripts/fetch_pykrx_all.py --date 2024-01-31 --output data/
```

자세한 사용법은 `scripts/fetch_pykrx_all.py` 참고.
