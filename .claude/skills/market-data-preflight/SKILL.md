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

## Read Next

- For commands, outputs, and failure triage: `references/operations.md`
- For KRX login/session failures: `../krx-auth-recovery/SKILL.md`
- For snapshot timing safety: `../point-in-time-guardrails/SKILL.md`

