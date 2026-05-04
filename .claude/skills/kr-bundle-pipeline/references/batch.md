# Batch

## Entrypoints

- `.github/workflows/daily-market-data.yml`
- `scripts/run_daily_batch.py`
- `scripts/run_local_collection.py`

## Expected steps

1. `preflight`
2. optional: `crawl_kr_data_pykrx`
   - historical backfill normally uses official collector; legacy FnGuide fallback `crawl_kr_data_fallback`
     is used only when needed for historical repair.
3. `build_kr_snapshot`
4. write `summary.json`

## Local KOSPI200 collection steps

1. build `kospi200` universe from KRX/pykrx index code `1028`
2. raw official pykrx crawl:
   `scripts/crawl_kr_data_pykrx.py --start ... --end ... --skip-meta --skip-ohlcv`
   - ensure `.env` is loaded so `KRX_ID`/`KRX_PW` are available; otherwise auth failures
     can look like dataframe column errors.
3. bundle KOSPI200 ticker data with official pykrx + DART + ECOS
4. do not broad-retry live DART; if OpenDART times out, stop live DART and resume with `--dart-mode cache_only`
5. validate `validation_report.json` and report both object counts and real DART coverage

## Failure model

- Exit `1`: hard failure
- Exit `2`: degraded success
- Missing crawl categories should be reflected as degraded detail in batch summary
- DART cache-only is acceptable for KRX-complete runs only if DART coverage is reported explicitly
- Any live DART cache backfill must be universe-limited, delayed by `5s+` per ticker,
  resumable, skip cached ticker/month keys, and stop on transient OpenDART errors.
- Empty strict DART results during controlled backfill should also stop live calls because they
  can hide repeated `013` responses.

## Evidence Checklist

For each KR bundle run, report the concrete run root and the validation files you
inspected. Keep these counts separate:

- raw pykrx outputs: `cap_daily`, `fundamental`, `index`, `sector`
- validation status: `failed`, `degraded`
- market coverage: `price_tickers`, `market_cap_nonnull`, `last_close_nonnull`, `sector_map`
- benchmark coverage: `benchmark_bars` and last benchmark date
- DART enrichment: `quarters_nonempty`, `filing_text_nonempty`, latest available fiscal quarters

Do not make a single historical run path or date the default baseline for future
collections.

## Important artifact paths

- `out/<as_of>_<timestamp>/summary.json`
- `out/<as_of>_<timestamp>/logs/*.log`
- `out/<as_of>_<timestamp>/artifacts/snapshots/YYYY-MM/*.json`
