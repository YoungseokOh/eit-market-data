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
4. 접속이 안 될 때는 먼저 **`docs/wsl2-runbook.md`의 DART 섹션**을 확인하고,
   stale 캐시(`data/dart_cache/`) 또는 `scripts/seed_dart_cache.py`로 오프라인 빌드를 수행한다.
5. 차단이 의심될 때는 **자정(00:00 KST) 이후 재시도**한다. 강제로 뚫으려 하지 않는다.

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
