# pykrx 데이터 가이드

pykrx로 수집 가능한 데이터 종류와 로컬 저장 전략 정리.

⚠️ **참고:** 이 가이드는 pykrx 자체의 API/컬렉션 범위를 정리한 문서입니다.
`eit-market-data`에서는 공식 KR 경로 기준을 pykrx 기준으로 정리했으며, FDR(`FinanceDataReader`)은
주로 진단/비상 폴백으로만 사용합니다.

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

## KOSPI200 유니버스

`top200`은 KOSPI/KOSDAQ 전체 시가총액 상위 200개이고, `kospi200`은 KRX KOSPI200 지수 구성종목이다.
두 기준은 서로 다르므로 리서치용 KOSPI200 번들은 반드시 별도 유니버스 파일을 사용한다.

```bash
python scripts/build_local_universe.py \
  --as-of 2025-12-31 \
  --kind kospi200 \
  --output universes/kr_kospi200_2025-12-31.csv
```

생성 경로는 KRX/pykrx `get_index_portfolio_deposit_file("1028", as_of)`를 우선 사용한다.
KRX 세션이 `LOGOUT` 등으로 구성종목을 반환하지 못하면 Naver KOSPI200 현재 구성으로 fallback 하며,
이 경우 CSV의 `source`와 `source_as_of` 컬럼에 `naver_current_fallback` 및 수집일이 기록된다.
과거 시점 백테스트에서는 이 fallback 파일을 point-in-time 공식 원천으로 간주하지 않는다.

## Local KOSPI200 수집 runbook

KRX/pykrx가 정상이고 KOSPI200 기준으로 현재 연도 데이터를 만들 때는
`run_local_collection.py`를 사용한다. 이 경로는 KOSPI200 universe를 만들고,
official pykrx raw 월말 자료를 수집한 뒤, KOSPI200 bundle을 생성한다.

```bash
AS_OF=YYYY-MM-DD
YEAR_START=YYYY-01-01
RUN_LABEL=kr_<period>_official

python scripts/preflight_kr_data.py --as-of "$AS_OF" --ticker 005930 --skip-news

python scripts/run_local_collection.py \
  --storage-root "out/$RUN_LABEL" \
  --as-of "$AS_OF" \
  --market kr \
  --phase full \
  --full-universe-kind kospi200 \
  --start "$YEAR_START"
```

raw 단계는 공식 pykrx collector를 사용한다:

```bash
scripts/crawl_kr_data_pykrx.py --start "$YEAR_START" --end "$AS_OF" --skip-meta --skip-ohlcv
```

이 collector는 project `.env`를 로드해 `KRX_ID`/`KRX_PW`를 pykrx에 전달해야 한다.
값이 빠지면 KRX 인증 실패가 `종가`, `BPS`, `PER`, `지수명` 컬럼 오류처럼 보일 수 있다.
그 경우 schema를 먼저 바꾸지 말고 `preflight_kr_data.py --skip-news`로 공식 pykrx 경로를 확인한다.

`--skip-meta`는 KOSPI200 universe가 이미 이름/섹터를 갖고 있을 때 불필요한 종목명 호출을 줄이기 위한 옵션이고,
`--skip-ohlcv`는 전체 KOSPI/KOSDAQ 개별 OHLCV raw 덤프를 피하기 위한 옵션이다.
KOSPI200 개별 가격은 bundle 단계에서 `PykrxProvider`가 300 거래일 lookback으로 수집한다.

부분 현재월 bundle을 만들 때는 `decision_date`를 실제 `--as-of`로 유지한다. `execution_date`는
`run_local_collection.py`가 KRX/pykrx의 알려진 business day를 우선 사용해 `decision_date` 이후
첫 거래일로 잡고, KRX가 미래 거래일을 아직 반환하지 못하면 다음 weekday로 fallback한다.
완료 월이 아닌 partial bundle에서 월말/다음달 placeholder를 쓰면 point-in-time 계약 위반이다.

DART는 KRX/pykrx market data를 막는 gate가 아니라 재무/공시 enrichment다. KOSPI200/전체 종목을
live DART로 넓게 반복 실행하지 않는다. live DART가 필요한 경우에는 explicit universe 또는
실패 ticker 목록을 대상으로, cached ticker/month skip, ticker당 `5s+` delay, progress/resume,
transient error 즉시 중단 조건을 갖춘 cache backfill만 사용한다.

OpenDART가 broad run 중 timeout을 내면 live DART를 계속 재시도하지 말고 cache-only로 재개한다:

```bash
python scripts/run_local_collection.py \
  --storage-root "out/$RUN_LABEL" \
  --as-of "$AS_OF" \
  --market kr \
  --phase full \
  --full-universe-kind kospi200 \
  --start "$YEAR_START" \
  --resume \
  --dart-mode cache_only
```

성공 후 확인할 파일:

- `out/<label>/runs/YYYY-MM-DD/kr_full_kospi200/progress.json`
- `out/<label>/runs/YYYY-MM-DD/kr_full_kospi200/reports/full_kr_raw.json`
- `out/<label>/runs/YYYY-MM-DD/kr_full_kospi200/bundles/kr/full/snapshots/YYYY-MM/summary.json`
- `out/<label>/runs/YYYY-MM-DD/kr_full_kospi200/bundles/kr/full/snapshots/YYYY-MM/validation_report.json`

coverage를 보고할 때는 `fundamental_tickers`/`filing_tickers`만 보지 말고
`quarters_nonempty`, `filing_text_nonempty`, `market_cap_nonnull`, `last_close_nonnull`을 별도로 계산한다.

### 검증 결과 보고 기준

검증 결과는 현재 run root의 파일을 근거로 보고한다. 날짜별 예전 run을 기본값처럼 문서에
박아두지 않는다.

필수 확인 항목:

- raw pykrx: `cap_daily`, `fundamental`, `index`, `sector` 파일 수
- raw validation: `failed=0`, `degraded` 사유
- bundle validation: `failed=0`, `degraded` 사유
- KOSPI200 universe: `200`
- price coverage: `price_tickers`, last date `<= decision_date`
- market cap / last close: `market_cap_nonnull`, `last_close_nonnull`
- benchmark: `benchmark_bars`, last date `<= decision_date`
- sector map coverage
- DART actual coverage: `quarters_nonempty`, `filing_text_nonempty`
- latest DART quarters among non-empty tickers
