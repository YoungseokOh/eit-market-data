---
name: market-data-preflight
description: |
  Use before running Korean market-data collection, bundle export, or daily batch commands in
  eit-market-data. Trigger when the user wants to run or debug preflight, crawl_kr_data.py,
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

## DART API — Rate Limit Guard

> **DART API 접속 실패 시 절대 반복 시도하지 않는다.**

- `Connection reset` / `RemoteDisconnected` / HTTP 000 → **1회 확인 후 중단**
- 진단 목적으로 curl/requests를 반복 실행하면 IP가 당일 차단됨 (WSL2·Windows 공통)
- 차단 해제는 자정(00:00 KST) 이후
- 오프라인 대안: `python scripts/seed_dart_cache.py` → `--profile ci_safe` 빌드

규칙 전문: `@rules/dart-api-limits.md`

## Read Next

- For commands, outputs, and failure triage: `references/operations.md`
- For KRX login/session failures: `../krx-auth-recovery/SKILL.md`
- For snapshot timing safety: `../point-in-time-guardrails/SKILL.md`
- For DART rate limit rules: `@rules/dart-api-limits.md`

## Commit Format

If you make a commit while using this skill, follow `@rules/commit-messages.md`:
`<type>: <message>` then a blank line then `<body>`.
