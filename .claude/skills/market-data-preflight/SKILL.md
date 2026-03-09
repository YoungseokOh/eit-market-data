---
name: market-data-preflight
description: |
  Run before any Korean or US market data collection, SnapshotBuilder.build() execution,
  fetch_pykrx_all.py run, or monthly snapshot pipeline start. Use this skill whenever
  the user is about to collect market data, run a data pipeline, test API connections,
  or set up a new environment for eit-market-data. Triggers on phrases like "데이터 수집",
  "snapshot 빌드", "pykrx 돌려봐", "API 연결 확인", "환경 설정", or any mention of
  running the data pipeline.
---

# Market Data Preflight

데이터 수집 전 환경과 연결 상태를 검증하는 체크리스트.
**반드시 데이터 수집 명령 실행 전에 이 체크를 완료할 것.**

## 체크 순서

### 1. 환경변수 확인

```bash
# 필수 API 키 존재 여부 확인
python -c "
import os
keys = {
    'DART_API_KEY': os.environ.get('DART_API_KEY', ''),
    'ECOS_API_KEY': os.environ.get('ECOS_API_KEY', ''),
}
for name, val in keys.items():
    status = 'OK' if val else 'MISSING'
    masked = val[:4] + '****' + val[-4:] if len(val) > 8 else '(empty)'
    print(f'{status}: {name} = {masked}')
"
```

- `DART_API_KEY` 없으면: 재무/공시 데이터 수집 불가 → `DartProvider` 초기화 실패
- `ECOS_API_KEY` 없으면: 매크로 데이터 수집 불가 → `EcosMacroProvider` 초기화 실패
- pykrx는 API 키 불필요 (단, 네트워크 필요)

키 발급 방법: `docs/api-keys.md` 참고

### 2. 패키지 설치 확인

```bash
python -c "
packages = ['pykrx', 'OpenDartReader', 'requests', 'pandas', 'pyarrow']
for pkg in packages:
    try:
        __import__(pkg.lower().replace('-', '_').replace('opendartreader', 'OpenDartReader'))
        print(f'OK: {pkg}')
    except ImportError:
        print(f'MISSING: {pkg} → pip install {pkg}')
"
```

kr 의존성 일괄 설치: `pip install -e '.[kr]'`

### 3. API 연결 테스트 (샘플 쿼리)

```python
import sys; sys.path.insert(0, 'src')
from datetime import date

# pykrx 연결 테스트 (API 키 불필요)
from pykrx import stock
tickers = stock.get_market_ticker_list('20240131', market='KOSPI')
print(f'pykrx OK: {len(tickers)} KOSPI 종목')

# DART 연결 테스트
from eit_market_data.kr.dart_provider import DartProvider
dart = DartProvider()
# corp_list 로딩만 확인 (실제 조회 X)
corp_list = dart._get_corp_list()
print(f'DART OK: corp_list {len(corp_list)} 건')

# ECOS 연결 테스트 (기준금리 최신값 1건)
from eit_market_data.kr.ecos_provider import EcosMacroProvider
import asyncio
ecos = EcosMacroProvider()
macro = asyncio.run(ecos.fetch_macro(date(2024, 1, 31)))
print(f'ECOS OK: base_rate={macro.rates_policy.get("base_rate")}')
```

### 4. 대상 날짜 거래일 확인

수집 대상 날짜가 한국 거래일인지 확인:

```python
from pykrx import stock
from datetime import date, timedelta

target = date(2024, 1, 31)  # 수집 대상 날짜로 변경
date_str = target.strftime('%Y%m%d')

# 해당 날짜 데이터가 있는지 확인 (삼성전자로 테스트)
df = stock.get_market_ohlcv(date_str, date_str, '005930')
if df is None or df.empty:
    # 최근 거래일 찾기
    for offset in range(1, 8):
        alt = target - timedelta(days=offset)
        df = stock.get_market_ohlcv(alt.strftime('%Y%m%d'), alt.strftime('%Y%m%d'), '005930')
        if df is not None and not df.empty:
            print(f'주의: {target}은 비거래일. 가장 가까운 거래일: {alt}')
            break
else:
    print(f'OK: {target}은 거래일')
```

비거래일(공휴일/주말)이면 pykrx가 빈 DataFrame 반환 → `SnapshotBuilder`가 빈 가격 배열 저장.
`decision_date`는 `_last_business_day()` 자동 계산이지만, 수동 날짜 지정 시 반드시 확인.

### 5. 출력 디렉토리 및 디스크 용량

```bash
# 디스크 용량 확인
df -h . | tail -1

# 출력 디렉토리 생성
mkdir -p data/market/ohlcv data/market/cap data/market/fundamental
mkdir -p data/market/investor data/market/shorting data/market/foreign
mkdir -p data/index/ohlcv data/index/fundamental
mkdir -p data/etf/ohlcv data/meta artifacts/snapshots
```

예상 용량:
- KOSPI+KOSDAQ 전종목 OHLCV 300일: ~500MB (parquet)
- 시가총액, 수급, 공매도: ~200MB
- ETF OHLCV: ~100MB
- 스냅샷 메타: ~수MB

### 6. 최종 실행 요약 출력

체크 완료 후 실행 파라미터 확인:

```
=== Preflight 완료 ===
대상 날짜: YYYY-MM-DD (거래일 확인: OK)
pykrx: OK
DART: OK (or MISSING)
ECOS: OK (or MISSING)
출력 경로: data/
예상 수집 종목: KOSPI N개 + KOSDAQ M개
예상 소요 시간: ~X분 (0.3초 딜레이 × 종목수 기준)
```

## 실행 명령 (preflight 통과 후)

```bash
# 전체 수집
python scripts/fetch_pykrx_all.py --date 2024-01-31 --output data/ --verbose

# 빠른 검증 (OHLCV만)
python scripts/fetch_pykrx_all.py --date 2024-01-31 --skip cap fundamental investor shorting foreign index etf

# 스냅샷 빌드
python -c "
import sys, asyncio; sys.path.insert(0, 'src')
from eit_market_data import SnapshotBuilder, SnapshotConfig
from eit_market_data.snapshot import create_kr_providers
builder = SnapshotBuilder(**create_kr_providers())
snap = asyncio.run(builder.build('2024-01', ['005930', '000660']))
print(snap.decision_date, len(snap.prices['005930']), 'bars')
"
```

## 자주 발생하는 문제

| 증상 | 원인 | 해결 |
|------|------|------|
| `DART_API_KEY` 오류 | 환경변수 미설정 | `export DART_API_KEY=키값` |
| pykrx 빈 DataFrame | 비거래일 조회 | 거래일로 날짜 조정 |
| `ModuleNotFoundError: pykrx` | 패키지 미설치 | `pip install -e '.[kr]'` |
| ECOS 429 에러 | Rate limit | 자동 재시도 1회 내장, 그래도 실패 시 잠시 후 재시도 |
| 디스크 부족 | 전체 수집 시 ~1GB | `--skip` 옵션으로 필요한 것만 수집 |
