---
name: kr-bundle-pipeline
description: |
  Use when building, validating, or debugging KR snapshot bundles and the daily batch flow.
  Trigger on build_kr_snapshot.py, run_daily_batch.py, snapshot.json, manifest.json, summary.json,
  GitHub Actions daily-market-data.yml, ci_safe profile behavior, or eit-research bundle loading.
---

# KR Bundle Pipeline

Use this skill for the KR snapshot export path that feeds `eit-research`.

## Workflow

1. Choose the correct profile:
   - `ci_safe` for GitHub-hosted or headless environments
   - official profiles only for local or self-hosted flows
2. Validate the exported bundle files and coverage fields.
3. Check consumer compatibility against `/home/seok436/projects/eit-research`.
4. Keep benchmark and market-cap behavior aligned with the current `ci_safe` contract.
5. If the change affects timing semantics, also use `point-in-time-guardrails`.

## Read Next

- Contract and commands: `references/contract.md`
- Batch artifact expectations: `references/batch.md`

