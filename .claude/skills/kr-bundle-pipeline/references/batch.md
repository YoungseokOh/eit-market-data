# Batch

## Entrypoints

- `.github/workflows/daily-market-data.yml`
- `scripts/run_daily_batch.py`

## Expected steps

1. `preflight`
2. `crawl_kr_data`
3. `build_kr_snapshot`
4. write `summary.json`

## Failure model

- Exit `1`: hard failure
- Exit `2`: degraded success
- Missing crawl categories should be reflected as degraded detail in batch summary

## Important artifact paths

- `out/<as_of>_<timestamp>/summary.json`
- `out/<as_of>_<timestamp>/logs/*.log`
- `out/<as_of>_<timestamp>/artifacts/snapshots/YYYY-MM/*.json`

