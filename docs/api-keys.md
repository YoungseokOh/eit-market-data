# API 키 발급 가이드

한국 시장 데이터 수집에 필요한 외부 API 키 발급 절차입니다.

---

## DART API 키 (`DART_API_KEY`)

금융감독원 전자공시시스템(OpenDART) — 재무제표, 사업보고서, 공시 데이터

**URL:** https://opendart.fss.or.kr

### 발급 절차

1. 사이트 접속 → 상단 메뉴 **"인증키 신청/관리"** 클릭
2. 오픈API 이용약관 및 개인정보 수집·이용 동의
3. 신청구분 선택 후 신청서 작성
   - **개인용**: 이메일, 비밀번호, 사용 용도
   - **기업용**: 회사명, 사업자번호, 담당자 연락처, 사업자등록증 추가
4. 제출 → 승인 즉시 인증키 발급 (로그인 후 "인증키 관리"에서 확인)

### 직접 신청 링크

https://opendart.fss.or.kr/uss/umt/EgovMberInsertView.do

### 주의사항

- 일일 요청 한도: **약 20,000건** (초과 시 에러코드 `020`)
- 1인 1키 정책 (중복 발급 제한)
- 시스템 점검 시간에 API 일시 중단 (점검 공지 확인 필요)

---

## ECOS API 키 (`ECOS_API_KEY`)

한국은행 경제통계시스템 — 기준금리, 국고채, CPI, GDP, 환율 등 매크로 지표

**URL:** https://ecos.bok.or.kr/api

### 발급 절차

1. 사이트 접속 → **"인증키 신청"** 클릭
2. 한국은행 회원가입 (이메일 인증 필요)
3. 로그인 → 마이페이지 → **"인증키 신청"** 메뉴
4. 사용 용도 입력 후 신청 → 이메일로 키 수신

### 주의사항

- 활성화 대기: 최대 **1일** 소요 가능
- 일일 한도: 로그인 후 마이페이지에서 확인
- 인증키 없이도 `sample` 키로 일부 통계 샘플 조회 가능

---

## KRX 로그인 세션 (`KRX Data Marketplace`)

KRX 지수/시장 전체 데이터는 더 이상 API 키가 아니라 **브라우저 로그인 세션**이 필요합니다.

이 프로젝트는 로컬/WSL 기준으로 아래 스크립트로 KRX 세션 쿠키를 생성합니다:

```bash
python scripts/krx_login.py
```

기본 쿠키 파일:

```text
~/.cache/eit-market-data/krx-profile/cookies.json
```

동작 방식:

- Chromium 브라우저가 열리면 KRX Data Marketplace에 직접 로그인
- 로그인 완료 후 `JSESSIONID` 등 세션 쿠키를 JSON으로 저장
- 이후 `preflight`, `crawl`, `fetch_pykrx_all`은 이 쿠키를 재사용

주의사항:

- 이 세션은 KRX 로그인 만료 정책에 따라 다시 갱신이 필요할 수 있음
- 쿠키 파일은 비밀정보이므로 외부 공유 금지
- 현재 1차 구현은 로컬/WSL 우선이며, GitHub Actions 무인 로그인은 범위 밖

---

## 환경변수 설정

프로젝트 루트에 `.env` 파일 생성:

```bash
DART_API_KEY=발급받은키
ECOS_API_KEY=발급받은키
```

실행 시 로드:

```bash
export $(cat .env | xargs)
```

또는 `python-dotenv` 사용:

```python
from dotenv import load_dotenv
load_dotenv()
```

`.env` 파일은 반드시 `.gitignore`에 포함시킬 것.

## GitHub Actions Secrets

GitHub Actions로 자동 실행할 때는 `.env` 파일을 업로드하지 말고 repository secrets에 아래 이름으로 저장:

```text
DART_API_KEY
ECOS_API_KEY
FRED_API_KEY
SEC_EDGAR_USER_AGENT
```

현재 일일 배치(`scripts/run_daily_batch.py`)는 KR preflight, KR crawl, KR snapshot 기준이라서 실제 필수 secret은 `DART_API_KEY`, `ECOS_API_KEY`다. `FRED_API_KEY`, `SEC_EDGAR_USER_AGENT`는 향후 US provider 자동화에 대비한 이름으로 유지한다.

---

## 연결 테스트

```python
import sys, os, asyncio
sys.path.insert(0, "src")
from datetime import date

# DART
from eit_market_data.kr.dart_provider import DartProvider
dart = DartProvider()  # DART_API_KEY 환경변수 필요
fund = asyncio.run(dart.fetch_fundamentals("005930", date(2024, 1, 31), n_quarters=2))
print(fund.ticker, len(fund.quarters))

# ECOS
from eit_market_data.kr.ecos_provider import EcosMacroProvider
ecos = EcosMacroProvider()  # ECOS_API_KEY 환경변수 필요
macro = asyncio.run(ecos.fetch_macro(date(2024, 1, 31)))
print(macro.rates_policy)
```
