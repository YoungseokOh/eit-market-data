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
   - `run_local_collection.py --dart-mode cache_only` only after live OpenDART times out
   - do not run broad KOSPI200/all-ticker live DART without bounded cache-backfill controls
2. Validate the exported bundle files and coverage fields.
3. Check consumer compatibility against `/home/seok436/projects/eit-research`.
4. Keep benchmark and market-cap behavior aligned with the current `ci_safe` contract.
5. If the change affects timing semantics, also use `point-in-time-guardrails`.

## Timing Contract

For partial current-month local bundles, keep `decision_date` equal to the actual
`as_of` date. Do not expand it to the calendar month-end. `execution_date` should
be the next KRX trading day when pykrx can identify one from known business days;
otherwise fall back to the next weekday. Never emit the next-month first business
day as a placeholder for a partial-month bundle.

## Coverage Reporting

Do not treat `fundamental_tickers` and `filing_tickers` as full DART coverage.
They count objects present in `snapshot.json`. For local cache-only DART runs, also
report:

- `quarters_nonempty`
- `filing_text_nonempty`
- `market_cap_nonnull`
- `last_close_nonnull`
- actual DART cache coverage for the target month: `missing_fundamental`,
  `missing_filing`, latest quarter distribution, cache size/volume

## DART Collection Safety

For KOSPI200 bundles, live OpenDART is enrichment, not the gate for KRX/pykrx market data.
If live DART times out, stop live calls and resume with `--dart-mode cache_only`.
Only use live DART backfill when it is universe-limited, delayed (`5s+` per ticker), resumable,
skips cached ticker/month keys, and stops on transient network/rate-limit symptoms.
If strict DART returns empty fundamentals/filing during controlled backfill, stop live calls;
do not push through repeated `013` responses.
Do not use controlled backfill `progress.json` as completion evidence by itself. It is
only a resume checkpoint; diskcache eviction can make completed tickers disappear from
the actual cache when the size limit is too small.

## Evidence Standard

When reporting a KR bundle run, cite the run root and validation reports from that run.
Always separate market coverage (`price_tickers`, `market_cap_nonnull`,
`last_close_nonnull`, `sector_map`, `benchmark_bars`) from DART enrichment coverage
(`quarters_nonempty`, `filing_text_nonempty`). Do not encode one date-specific run as
the durable baseline for future collections.

Expected shape for a healthy KOSPI200 local bundle:

- raw and bundle validation have `failed=0`
- universe size is 200 for `kospi200`
- price, market cap, last close, and sector map are complete or explicitly explained
- last price date is at or before `decision_date`
- DART coverage is reported as actual non-empty quarters and filing text, not object counts

## Read Next

- Contract and commands: `references/contract.md`
- Batch artifact expectations: `references/batch.md`

## Commit Format

If you make a commit while using this skill, follow `@rules/commit-messages.md`:
`<type>: <message>` then a blank line then `<body>`.
