---
name: point-in-time-guardrails
description: |
  Use whenever modifying SnapshotBuilder, provider fetch methods, data joining logic,
  backfill scripts, or any code that combines data from multiple time points.
  Triggers on: "스냅샷 수정", "provider 변경", "백필", "데이터 조인", "look-ahead",
  "point-in-time", adding new data fields to snapshots, changing decision_date logic,
  or any code that fetches/merges data with date parameters.
  CRITICAL for maintaining backtesting integrity — use proactively whenever touching
  data pipeline code in eit-market-data or eit-research.
---

# Point-in-Time Guardrails

look-ahead bias(미래 데이터 누출)를 방지하는 체크리스트.
**스냅샷이나 데이터 파이프라인 코드를 수정할 때마다 이 체크를 실행할 것.**

## 핵심 원칙

`MonthlySnapshot`은 **decision_date 시점에 실제로 알 수 있었던 데이터만** 포함해야 한다.
미래에 발표된 재무/공시 데이터가 섞이면 백테스트 수익률이 과장된다.

## 체크 1: as_of 날짜 전파 확인

새 provider 메서드나 데이터 fetch 로직에 `as_of` 파라미터가 올바르게 전달되는지 확인:

```python
# 올바른 패턴 - as_of가 전체 체인에 전달됨
async def fetch_fundamentals(self, ticker: str, as_of: date, n_quarters: int = 8):
    candidates = self._quarter_candidates(as_of)  # as_of 기준으로 후보 생성
    for fiscal_q, report_date, bsns_year, reprt_code in candidates:
        if report_date <= as_of:  # 반드시 이 조건 확인
            ...

# 위험한 패턴 - 현재 날짜 또는 고정 날짜 사용
df = stock.get_market_sector_classifications(date.today().strftime('%Y%m%d'))  # BAD
df = stock.get_market_ohlcv('20240101', '20241231', ticker)  # BAD (미래 포함 가능)
```

**확인 포인트:**
- `date.today()` 사용 여부 (as_of 대신 사용하면 look-ahead)
- 고정 종료 날짜 사용 여부
- 데이터 필터에서 `report_date <= as_of` 또는 `rcept_dt <= as_of` 조건 존재 여부

## 체크 2: 재무 데이터 발표일 지연 반영

재무제표는 기간 종료 후 **최소 45~60일 후에 공시**된다:

```python
# DartProvider._quarter_candidates() 패턴 확인
quarter_defs = [
    ("Q4", 12, 31, "11011"),  # 연간: 12/31 종료 → 약 3월 공시
    ("Q3", 9, 30, "11014"),   # 3분기: 9/30 종료 → 약 11월 공시
    ("Q2", 6, 30, "11012"),   # 반기: 6/30 종료 → 약 8월 공시
    ("Q1", 3, 31, "11013"),   # 1분기: 3/31 종료 → 약 5월 공시
]
# report_date = period_end + timedelta(days=60)  ← 이 60일 버퍼가 핵심
```

**확인 포인트:**
- 새 재무 데이터 소스 추가 시: `보고서_공시일 <= decision_date` 필터 존재 여부
- 60일 버퍼가 너무 짧거나 없으면 look-ahead 발생 가능

## 체크 3: 가격 데이터 범위

```python
# 올바른 패턴 - as_of 이후 데이터 제거
for idx, row in df.iterrows():
    bar_date = idx.date() if hasattr(idx, 'date') else idx
    if bar_date > as_of:  # 이 필터가 반드시 있어야 함
        continue
    bars.append(PriceBar(...))
```

**확인 포인트:**
- OHLCV fetch 후 `bar_date > as_of` 필터링 여부
- lookback 시작 날짜가 `as_of - timedelta(days=lookback)` 으로 계산되는지
- pykrx 기본 lookback: **300 거래일** (12개월 모멘텀=252 + 여유)

## 체크 4: 매크로 데이터 시점

```python
# EcosMacroProvider._latest_value() 패턴 확인
for row in rows:
    obs_date = _parse_ecos_time(str(row.get('TIME', '')), period)
    if obs_date is None or obs_date > as_of:  # 이 필터 필수
        continue
```

**확인 포인트:**
- 새 ECOS 지표 추가 시: `obs_date <= as_of` 필터 존재 여부
- 연간(A) 지표는 발표 지연이 더 길 수 있음 (GDP 등)

## 체크 5: 스냅샷 조립 순서

`SnapshotBuilder.build()`에서 데이터 조합 시:

```python
# decision_date가 모든 fetch 호출에 전달되는지 확인
decision_date = _last_business_day(year, mon)

price_tasks = {t: self.price.fetch_prices(t, decision_date) for t in universe}
fund_tasks = {t: self.fundamental.fetch_fundamentals(t, decision_date) for t in universe}
# ↑ decision_date가 일관되게 전달되어야 함
```

**확인 포인트:**
- 새 provider 연결 시 `decision_date` 파라미터 누락 여부
- `sector_averages` 계산에 `as_of=decision_date` 전달 여부

## 체크 6: 빠른 look-ahead 탐지 테스트

```python
import sys, asyncio; sys.path.insert(0, 'src')
from datetime import date
from eit_market_data import SnapshotBuilder, SnapshotConfig
from eit_market_data.snapshot import create_kr_providers

# 과거 날짜로 스냅샷 빌드
builder = SnapshotBuilder(**create_kr_providers())
snap = asyncio.run(builder.build('2023-06', ['005930']))

# 가격 데이터가 decision_date 이내인지 확인
decision = snap.decision_date
for bar in snap.prices.get('005930', []):
    assert bar.date <= decision, f"LOOK-AHEAD: {bar.date} > {decision}"

# 재무 데이터 report_date 확인
fund = snap.fundamentals.get('005930')
if fund and fund.quarters:
    for q in fund.quarters:
        assert q.report_date <= decision, f"LOOK-AHEAD: {q.report_date} > {decision}"

print("Point-in-time 검증 통과")
```

## 수정 후 체크리스트

- [ ] 새 fetch 메서드에 `as_of: date` 파라미터 존재
- [ ] 반환 데이터에 `> as_of` 필터링 적용
- [ ] 재무 데이터 공시 지연(60일) 버퍼 확인
- [ ] `date.today()` 직접 사용 없음 (모두 `as_of` 또는 `decision_date` 사용)
- [ ] look-ahead 탐지 테스트 통과
- [ ] 기존 백테스트 결과와 대비하여 수익률 급변 없음

## 자주 발생하는 실수

| 코드 패턴 | 위험도 | 수정 방법 |
|-----------|--------|----------|
| `date.today()` in provider | 높음 | `as_of or date.today()` |
| 재무 데이터 필터 없이 최신값 사용 | 높음 | `report_date <= as_of` 필터 추가 |
| 가격 df에 종료일 고정 | 중간 | `as_of` 동적 사용 |
| 섹터 분류 `date.today()` | 중간 | `as_of` 파라미터 전달 (H3 수정됨) |
| ECOS `obs_date > as_of` 필터 누락 | 높음 | `_latest_value()` 패턴 따르기 |
