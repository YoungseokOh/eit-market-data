---
name: krx-auth-recovery
description: |
  Use when official KRX-backed pykrx paths fail in eit-market-data. Trigger on KRX login changes,
  `KrxAuthRequired`, `LOGOUT`, `400`, `KeyError('지수명')`, empty KRX index or listing frames,
  pykrx dataframe column errors like missing `종가`/`BPS`/`PER`,
  browser cookie/profile issues, or when scripts/krx_login.py and src/eit_market_data/kr/krx_auth.py
  need to be modified or debugged.
---

# KRX Auth Recovery

Use this skill for official KRX session problems, not for general CI-safe bundle work.

## Workflow

1. Confirm whether the failing path is truly KRX-authenticated or a CI-safe path.
2. Inspect `krx_auth.py`, `market_helpers.py`, `pykrx_provider.py`, and `scripts/krx_login.py`.
3. Check whether the installed `pykrx` has `pykrx.website.comm.auth.KRXSession`.
4. Reproduce with `scripts/preflight_kr_data.py` before changing code.
5. Preserve the rule that GitHub-hosted CI must not depend on KRX browser login.
6. If the user only needs KR research data in CI, redirect to `kr-bundle-pipeline`.

## Modern pykrx Rule

Recent `pykrx` versions can authenticate directly from `KRX_ID` and `KRX_PW`.
When that auth module exists, do not install legacy `webio.Get/Post` monkey-patches.
Overriding `webio` can bypass pykrx's authenticated session and make KRX POSTs return
`LOGOUT` even when credentials are valid.

Use `ensure_krx_authenticated_session(interactive=False)` as the local authenticated
probe. A successful non-interactive path should make `scripts/krx_login.py --timeout 30`
print `[OK] probe rows=...` without needing the browser.

All official pykrx scripts should load project `.env` before pykrx calls. If this is missing,
`crawl_kr_data_pykrx.py` can fail with misleading column errors (`종가`, `BPS`, `PER`,
`지수명`) even though the real issue is missing `KRX_ID`/`KRX_PW`.

## Read Next

- Known symptoms and recovery steps: `references/runbook.md`
- CI-safe alternative path: `../kr-bundle-pipeline/SKILL.md`

## Commit Format

If you make a commit while using this skill, follow `@rules/commit-messages.md`:
`<type>: <message>` then a blank line then `<body>`.
