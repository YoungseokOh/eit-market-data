---
name: krx-auth-recovery
description: |
  Use when official KRX-backed pykrx paths fail in eit-market-data. Trigger on KRX login changes,
  `KrxAuthRequired`, `LOGOUT`, `400`, `KeyError('지수명')`, empty KRX index or listing frames,
  browser cookie/profile issues, or when scripts/krx_login.py and src/eit_market_data/kr/krx_auth.py
  need to be modified or debugged.
---

# KRX Auth Recovery

Use this skill for official KRX session problems, not for general CI-safe bundle work.

## Workflow

1. Confirm whether the failing path is truly KRX-authenticated or a CI-safe path.
2. Inspect `krx_auth.py`, `market_helpers.py`, `pykrx_provider.py`, and `scripts/krx_login.py`.
3. Reproduce with `scripts/preflight_kr_data.py` before changing code.
4. Preserve the rule that GitHub-hosted CI must not depend on KRX browser login.
5. If the user only needs KR research data in CI, redirect to `kr-bundle-pipeline`.

## Read Next

- Known symptoms and recovery steps: `references/runbook.md`
- CI-safe alternative path: `../kr-bundle-pipeline/SKILL.md`

