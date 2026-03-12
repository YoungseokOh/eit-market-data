# Agent Notes

Repository-local guidance for coding agents working in this repo.

## KR Runtime

- Default KR runtime uses public `FinanceDataReader 0.9.110` routes.
- Do not assume KRX login is required for normal preflight or snapshot builds.
- KRX login scripts (`scripts/krx_login.py`, `scripts/probe_fdr_krx_session.py`) are manual diagnostics only.

## Historical Market Cap

- Historical KR `market_cap` backfill lives under `data/market/cap_daily/`.
- Generate it with `python scripts/crawl_kr_data_fallback.py --start YYYY-MM-DD --end YYYY-MM-DD`.
- The runtime lookup in `src/eit_market_data/kr/market_helpers.py` consumes `cap_daily` first, then falls back to recent public FDR listings.
- FnGuide `TRD_DT` in this flow is a month label, not an exact trading day. Store snapshots at each month's last business day.

## Batch / Actions

- `scripts/run_daily_batch.py` should use `scripts/crawl_kr_data_fallback.py`, not the legacy authenticated crawler.
- GitHub Actions should treat DART and ECOS keys as optional enrichment for KR, not as hard requirements for the base KR build.
- Release/upload paths must match the actual batch output under `out/<run>/artifacts/snapshots/`.

## Docs

- Keep README, `docs/api-keys.md`, and `docs/wsl2-runbook.md` aligned with the public-FDR-first design.
- If workflow behavior changes, update `.github/workflows/daily-market-data.yml` and the related docs in the same change.
