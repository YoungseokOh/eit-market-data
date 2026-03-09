# CLAUDE.md — eit-market-data

Agent instructions for working in this repository.

---

## Section 1: Commit Rules

규칙 전문: `.claude/commit-rules.md`

### 한줄 요약

```
feat(scope): 한 일  |  fix(scope): 한 일
```

`feat`와 `fix` **두 가지 type만** 사용한다.

### 금지사항

- `git commit --no-verify` 사용 금지
- 빈 커밋 금지
- `python -c "import eit_market_data"` 통과 전 커밋 금지

---

## Section 2: Code Generation Rules

### Python 버전 및 기본 설정

- **Python 3.11+** (pyproject.toml 기준). 새 코드는 3.12+ 기능을 사용해도 무방.
- 모든 파일 첫 줄에 `from __future__ import annotations` 필수.
- 모든 public 함수/메서드에 type hint 필수. `Any`는 진짜 불명확한 경우에만 허용.

```python
from __future__ import annotations

from datetime import date

def fetch_prices(self, ticker: str, as_of: date) -> list[PriceBar]:
    ...
```

### Async 패턴

- **Blocking I/O는 반드시 `asyncio.to_thread()`로 감싼다.** 동기 라이브러리(pykrx, opendartreader 등)를 event loop에서 직접 호출하지 않는다.
- **Rate limiting은 `asyncio.Semaphore`로 제어한다.** 모듈 레벨에 선언하고 `_run_limited()` 패턴으로 캡슐화한다.

```python
_PROVIDER_SEMAPHORE = asyncio.Semaphore(2)
_DELAY_SECONDS = 0.5

async def _run_limited(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    async with _PROVIDER_SEMAPHORE:
        try:
            return await asyncio.to_thread(fn, *args, **kwargs)
        finally:
            await asyncio.sleep(_DELAY_SECONDS)
```

### Provider 패턴

- 모든 Provider는 `providers.py`에 정의된 Protocol 인터페이스를 구현한다.
- **에러 시 빈 값을 반환하고 `logger.warning()`으로 기록한다. 예외를 caller로 전파하지 않는다.**
- import는 lazy (메서드 내부 또는 `__init__` 진입 시점)로 처리해 선택적 의존성을 보호한다.

```python
async def fetch_prices(self, ticker: str, as_of: date) -> list[PriceBar]:
    try:
        return await self._run_limited(self._fetch_prices_sync, ticker, as_of)
    except Exception as e:
        logger.warning("price fetch failed for %s: %s", ticker, e)
        return []
```

- Provider `__init__`에서 선택적 라이브러리 import에 실패하면 `ImportError`를 re-raise하되, 설치 방법을 메시지에 포함한다.

```python
def __init__(self) -> None:
    try:
        import pykrx  # noqa: F401
    except ImportError as e:
        raise ImportError("Install with: pip install -e '.[kr]'") from e
```

### Point-in-Time 규칙

- **모든 데이터 fetch 함수에 `as_of: date` 파라미터가 필수**다. `date.today()`를 기본값으로 쓰지 않는다.
- fetch 결과에서 `as_of` 이후 날짜 데이터는 반드시 필터링한다 (look-ahead 방지).

```python
# pykrx 가격 필터링 예시
if bar_date > as_of:
    continue
```

### Korean Market 특이사항

- **KR ticker는 6자리 zero-padded string**이다. `_normalize_ticker()` 함수를 사용한다.

```python
def _normalize_ticker(ticker: str) -> str:
    digits = "".join(ch for ch in str(ticker) if ch.isdigit())
    return digits.zfill(6) if digits else str(ticker)
```

- **pykrx 날짜 포맷은 `YYYYMMDD` string**이다. `date_to_yyyymmdd()` 헬퍼를 사용한다.
- **재무 금액 단위는 KRW millions (백만원)**이다. DART/opendartreader 원본이 원(KRW) 단위일 경우 `/ 1_000_000`으로 변환 후 저장한다.
- KOSPI 벤치마크 종목코드: `"1001"`, KOSDAQ: `"2001"`.

### Logging

- 모듈 레벨에서 `logger = logging.getLogger(__name__)` 선언.
- 데이터 수집 실패: `logger.warning()` 사용, exception은 `str(e)`로 메시지에 포함.
- `logger.error()` 또는 `raise`는 시스템 전체에 영향을 주는 치명적 오류에만 사용.
- `print()` 사용 금지 (스크립트 진입점 제외).

### 테스트

- **새 코드를 추가할 때 최소한 컴파일 검증을 함께 커밋한다.**
- 최소 기준: `python -c "from eit_market_data.xxx import Yyy"` 통과.
- 단위 테스트는 `tests/` 디렉토리, pytest 사용. 외부 API 호출은 mock 또는 `SyntheticProvider`로 대체.
- Async 테스트는 `pytest-asyncio` 사용 (`@pytest.mark.asyncio`).

### 기타

- `SnapshotBuilder.build()`에 신규 provider를 추가할 때는 `create_real_providers()`와 `create_kr_providers()` 팩토리 함수도 함께 업데이트한다.
- Pydantic 스키마 변경 시 기존 JSON artifacts와의 하위 호환성을 고려한다.
- `asyncio.gather()`에서 `return_exceptions=True`를 쓸 때는 반드시 결과를 `isinstance(result, Exception)`으로 검사한다.

---

## Section 3: Project Structure

```
eit-market-data/
├── src/eit_market_data/          # 핵심 패키지
│   ├── __init__.py
│   ├── providers.py              # Protocol 인터페이스 (PriceProvider 등)
│   ├── snapshot.py               # SnapshotBuilder, SnapshotConfig, 팩토리 함수
│   ├── synthetic.py              # SyntheticProvider (API 키 불필요, 테스트용 기본값)
│   ├── cache.py                  # ResponseCache (diskcache 기반)
│   ├── yfinance_provider.py      # US 가격/펀더멘털/뉴스/섹터/벤치마크
│   ├── fred_provider.py          # US 매크로 (FRED API)
│   ├── edgar_provider.py         # US 공시 (SEC EDGAR)
│   └── schemas/
│       ├── __init__.py
│       └── snapshot.py           # Pydantic 스키마 (MonthlySnapshot 등)
│
├── src/eit_market_data/kr/       # 한국 시장 프로바이더
│   ├── pykrx_provider.py         # 가격/섹터/벤치마크 (pykrx)
│   ├── dart_provider.py          # 재무제표/공시 (opendartreader, DART_API_KEY)
│   └── ecos_provider.py          # 매크로 (한국은행 ECOS API, ECOS_API_KEY)
│
├── scripts/                      # 데이터 수집 실행 스크립트
├── docs/                         # 문서
├── universes/                    # 유니버스 CSV 파일 (버전 관리)
│   └── kr_universe.csv           # KR 유니버스 35종목
├── data/                         # 로컬 원본 데이터 (gitignored)
├── artifacts/                    # 스냅샷 출력물 (gitignored)
├── tests/                        # pytest 테스트
├── pyproject.toml                # 패키지 설정 및 의존성
└── CLAUDE.md                     # 이 파일
```

### 의존성 설치

```bash
# 기본 (SyntheticProvider만 사용 가능)
pip install -e .

# US 실데이터 (yfinance, FRED, EDGAR)
pip install -e '.[real-data]'

# KR 실데이터 (pykrx, opendartreader)
pip install -e '.[kr]'

# 전체
pip install -e '.[all]'

# 개발 도구 (pytest, ruff)
pip install -e '.[dev]'
```

### 환경 변수

| 변수 | 설명 | 필요 프로바이더 |
|------|------|----------------|
| `FRED_API_KEY` | FRED API 키 | `FredMacroProvider` |
| `SEC_EDGAR_USER_AGENT` | SEC EDGAR 식별자 (`"Name email"` 형식) | `EdgarFilingProvider` |
| `DART_API_KEY` | DART Open API 키 | `DartProvider` |
| `ECOS_API_KEY` | 한국은행 ECOS API 키 | `EcosMacroProvider` |

### Claude Code 스킬

이 프로젝트의 Claude Code 스킬은 `.claude/skills/`에 버전관리된다 (프로젝트 전용).

| 스킬 | 위치 | 용도 |
|------|------|------|
| `market-data-preflight` | `.claude/skills/market-data-preflight/SKILL.md` | 데이터 수집 전 환경/API/거래일 5단계 점검 |
| `point-in-time-guardrails` | `.claude/skills/point-in-time-guardrails/SKILL.md` | look-ahead bias 방지 체크 |
