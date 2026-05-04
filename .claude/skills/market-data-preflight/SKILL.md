---
name: market-data-preflight
description: |
  Use before running Korean market-data collection, bundle export, or daily batch commands in
  eit-market-data. Trigger when the user wants to run or debug preflight, crawl_kr_data_pykrx.py
  (official raw collection), `crawl_kr_data_fallback.py` (legacy FnGuide repair path),
  `crawl_kr_data.py` (legacy authenticated recovery),
  build_kr_snapshot.py, run_daily_batch.py, GitHub Actions data jobs, or local environment setup.
  Also use when validating API keys, package installation, output directories, or batch readiness.
---

# Market Data Preflight

Use this skill to gate any operational run of the KR data pipeline.

## Use It For

- Pre-run checks before `scripts/preflight_kr_data.py`
- KR crawl and snapshot export runs
- GitHub Actions or local batch readiness checks
- Environment validation after dependency or secret changes

## Workflow

1. Inspect the exact entrypoint the user wants to run.
2. Run the preflight path first unless the user is only asking about static code.
3. Validate secrets, dependencies, and expected artifacts.
4. If official KRX endpoints fail, switch to `krx-auth-recovery`.
5. If snapshot field timing or joins change, also use `point-in-time-guardrails`.

## pykrx / KRX Credential Guard

- Official raw collection through `scripts/crawl_kr_data_pykrx.py` must load project `.env`
  before importing or calling pykrx.
- If `KRX_ID`/`KRX_PW` are missing in a non-interactive shell, pykrx failures can look like
  schema bugs: `None of [Index(...)] are in the [columns]`, missing `종가`, or `KeyError('지수명')`.
- Treat those as KRX/auth/session symptoms first. Re-run `scripts/preflight_kr_data.py --skip-news`
  and use `krx-auth-recovery` before changing data schemas.

## DART API — Rate Limit Guard

> **DART API 접속 실패 시 절대 반복 시도하지 않는다.**

- `Connection reset` / `RemoteDisconnected` / HTTP 000 → **1회 확인 후 중단**
- `ConnectTimeout` / repeated OpenDART `013` empty responses during broad runs → **live DART 중단**
- `DART fundamentals returned empty` during cache backfill → **live DART 중단**
- `DART filing returned empty` during default controlled backfill → `filing_empty`로 기록하고 계속
- 진단 목적으로 curl/requests를 반복 실행하면 IP가 당일 차단됨 (WSL2·Windows 공통)
- 차단 해제는 자정(00:00 KST) 이후
- KOSPI200/전체 종목을 live DART로 무식하게 넓게 돌리지 않는다.
- live DART는 단일 probe 또는 `universe 제한 + 5s+ delay + resume + cached ticker skip + transient 즉시 중단`
  조건을 갖춘 cache backfill에만 사용한다.
- completed-month bundle 보강 시 `--as-of`는 반드시 해당 snapshot decision date로 맞춘다.
- 오프라인/계속 진행 대안:
  - `python scripts/seed_dart_cache.py` → `--profile ci_safe` 빌드
  - local KOSPI200 수집은 `scripts/run_local_collection.py ... --dart-mode cache_only --resume`
- 보고할 때는 `fundamental_tickers`/`filing_tickers`만 보지 말고 실제 coverage인
  `quarters_nonempty`와 `filing_text_nonempty`를 함께 보고한다.
- 최신 `[첨부정정]사업보고서` document가 OpenDART `014`로 비어 있으면 provider는 같은
  `as_of` 이전의 다음 retrievable 사업보고서로 내려간다. coverage와 별개로 `filing_date`
  freshness를 확인한다.
- `014`를 filing 결측으로 바로 판단하지 않는다. report 후보와 `doc:<rcept_no>` cache를
  확인하고 retrievable fallback이 없을 때만 결측으로 분류한다.
- controlled backfill의 `progress.json`은 재개용 상태일 뿐 완료 증거가 아니다. 완료 전에는
  실제 `data/dart_cache/`에서 target month의 `missing_fundamental`, `missing_filing`,
  `latest_quarter_distribution`, `cache.size_limit`, `cache.volume()`을 확인한다.
- DART diskcache 기본 size limit은 50GB이고 `EIT_DART_CACHE_SIZE_LIMIT_BYTES`로 키울 수 있다.
  여러 universe/month를 채울 때는 예상 volume의 3~5배 이상 headroom을 둔다.
- KRX ticker를 숫자 6자리로만 가정하지 않는다. `0126Z0` 같은 알파뉴메릭 코드는 그대로
  보존해야 하며, 숫자만 추출하면 다른 종목 DART cache로 오염될 수 있다.

규칙 전문: `@rules/dart-api-limits.md`

## Read Next

- For commands, outputs, and failure triage: `references/operations.md`
- For KRX login/session failures: `../krx-auth-recovery/SKILL.md`
- For snapshot timing safety: `../point-in-time-guardrails/SKILL.md`
- For DART rate limit rules: `@rules/dart-api-limits.md`

## Commit Format

If you make a commit while using this skill, follow `@rules/commit-messages.md`:
`<type>: <message>` then a blank line then `<body>`.
