# DART API Rate Limit Rules

## Rule

**DART API(`opendart.fss.or.kr`)에 연속 반복 요청을 절대 하지 않는다.**

단순 연결 확인, 키 유효성 테스트, 엔드포인트 진단을 목적으로 같은 IP에서 수십 회 이상
요청을 반복하면 해당 IP가 당일 임시 차단되어 실제 데이터 수집이 불가능해진다.

**Why:** 2026-03-12 디버깅 세션에서 DART 접속 문제를 진단하는 과정에 수십 회 연속
요청을 보냈고, 그 결과 KT 망 외부 IP(125.129.15.123)가 당일 전체 차단되었다.
WSL2뿐 아니라 같은 IP를 쓰는 Windows Python도 동시에 차단됐다.
자정 이후 차단이 풀릴 때까지 DART 기반 `fundamentals`/`filings` 수집이 전혀 불가능했다.

**How to apply:**

1. **연결 테스트는 1회만 한다.** 실패해도 헤더나 파라미터를 바꿔가며 재시도하지 않는다.
2. **진단 루프 금지.** curl/requests로 엔드포인트를 반복 테스트하는 코드나 명령을 작성하지 않는다.
3. **코드 변경 후 빌드 검증은 1회**로 제한한다. 같은 빌드를 연속으로 여러 번 실행하지 않는다.
4. **넓은 live DART 수집 금지.** KOSPI200/전체 종목을 live OpenDART로 한 번에 밀지 않는다.
   live DART는 단일 probe 또는 resume 가능한 저속 cache backfill에만 쓴다.
5. 접속이 안 될 때는 먼저 **`docs/wsl2-runbook.md`의 DART 섹션**을 확인하고,
   stale 캐시(`data/dart_cache/`) 또는 `scripts/seed_dart_cache.py`로 오프라인 빌드를 수행한다.
6. 차단이 의심될 때는 **자정(00:00 KST) 이후 재시도**한다. 강제로 뚫으려 하지 않는다.

## 허용되는 live DART 수집 형태

live OpenDART는 아래 조건을 모두 만족할 때만 사용한다.

- Universe가 명시되어 있어야 한다. 예: KOSPI200 CSV, 실패 ticker 목록, 단일 ticker.
- 이미 `data/dart_cache/`에 있는 ticker/month는 건너뛴다.
- ticker 사이에 최소 `5s` 이상 delay를 둔다.
- 진행 상태를 파일에 남기고 `--resume`으로 재개 가능해야 한다.
- `ConnectTimeout`, `ReadTimeout`, `RemoteDisconnected`, `Connection reset`, HTTP 000,
  `Max retries exceeded`, repeated empty `013` 중 하나라도 나오면 즉시 live 호출을 멈춘다.
- `DART fundamentals returned empty`가 나오면 더 넓게 밀지 말고 live backfill을 중단한다.
  내부적으로 반복 `013`이 쌓이는 패턴일 수 있다.
- 기본 `--filing-mode optional`에서 `DART filing returned empty`는 `filing_empty`로 기록하고
  계속한다. filing text가 hard gate인 검증에서만 strict mode로 중단한다.
- 최신 `[첨부정정]사업보고서` document가 `014`로 비어 있으면 같은 `as_of` 이전의 다음
  retrievable 사업보고서로 fallback할 수 있다. 이 경우 실제 `filing_date`를 보고한다.
- `014`를 filing 결측으로 바로 기록하지 않는다. report 후보와 `doc:<rcept_no>` cache를
  확인한 뒤 fallback 후보가 없을 때만 결측으로 본다.
- 중단 후 broad KR collection은 `--dart-mode cache_only --resume`으로 이어간다.
- `progress.json`은 재개 상태일 뿐 완료 증거가 아니다. 실제 `data/dart_cache/`에서
  target month의 missing fundamental/filing key, latest quarter distribution, cache size/volume을
  확인한 뒤 완료로 본다.
- DART diskcache 기본 size limit은 50GB이며 `EIT_DART_CACHE_SIZE_LIMIT_BYTES`로 조정할 수 있다.
- Universe ticker는 숫자 6자리만 있다고 가정하지 않는다. 알파뉴메릭 KRX 코드는 그대로 보존한다.

반대로, 전체 universe를 live provider로 무작정 반복 실행하거나 실패 원인을 찾기 위해 같은 endpoint를
여러 번 때리는 방식은 금지한다.

## 허용되는 단일 테스트

```bash
# 연결 상태 확인 — 딱 1번만
curl -s --max-time 10 \
  "https://opendart.fss.or.kr/api/company.json?crtfc_key=${DART_API_KEY}&corp_code=00126380" \
  | python3 -m json.tool | head -5
```

응답이 없으면 중단. 재시도하지 않는다.

## 차단 판단 기준

| 증상 | 판단 |
|------|------|
| `Connection reset by peer` / `RemoteDisconnected` | IP 임시 차단 가능성 |
| `curl: (56)` / `WinError 10054` | 동일. WSL2·Windows 공통 |
| `corpCode.xml`만 됐던 이력 있음 | 새벽에 한 번 성공한 것, 이후 차단 |
| HTTP 000 (연결 자체 실패) | 차단 또는 서버 점검 |

## 오프라인 대안

1. `python scripts/seed_dart_cache.py` — 기존 스냅샷 JSON에서 캐시 시딩
2. `python scripts/build_kr_snapshot.py --profile ci_safe` — DART 없이 빌드
3. GitHub Actions 결과물 다운로드 후 로컬에서 사용
4. `python scripts/run_local_collection.py ... --resume --dart-mode cache_only` — KRX/pykrx 수집은 계속하고 DART는 캐시만 사용
