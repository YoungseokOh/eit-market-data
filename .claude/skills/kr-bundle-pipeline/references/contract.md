# Contract

## Producer files

- `scripts/build_kr_snapshot.py`
- `src/eit_market_data/snapshot.py`
- `src/eit_market_data/kr/ci_safe_provider.py`
- `src/eit_market_data/kr/fundamental_provider.py`

## Consumer files

- `/home/seok436/projects/eit-research/src/eit/data/snapshot.py`
- `/home/seok436/projects/eit-research/src/eit/cli.py`
- `/home/seok436/projects/eit-research/docs/integrations/eit-market-data.md`

## Bundle layout

- `artifacts/snapshots/YYYY-MM/snapshot.json`
- `artifacts/snapshots/YYYY-MM/metadata.json`
- `artifacts/snapshots/YYYY-MM/manifest.json`
- `artifacts/snapshots/YYYY-MM/summary.json`
- local collection layout:
  `out/<label>/runs/YYYY-MM-DD/kr_full_kospi200/bundles/kr/full/snapshots/YYYY-MM/*.json`

## Timing semantics

- `decision_date` is the point-in-time cutoff. For partial current-month bundles,
  it must remain the actual `as_of` date.
- `execution_date` is the assumed next trade date after `decision_date`.
- `run_local_collection.py` should resolve `execution_date` from known KRX business
  days when pykrx can provide them, and otherwise fall back to the next weekday.
- Do not use next-month first business day as the `execution_date` placeholder for
  partial current-month bundles.

## Current ci_safe expectations

- Required: prices, DART fundamentals with `report_date`, filing text, macro, seed sector map
- Optional: benchmark prices, `market_cap`, `issued_shares`
- `manifest.json` should record `source_profile`, `field_coverage`, and warnings

## Local official KOSPI200 expectations

- Required: KOSPI200 universe, prices, `market_cap`, `last_close_price`, sector map,
  benchmark, ECOS macro
- DART live mode: DART fundamentals and filing text are attempted per ticker
- DART cache-only mode: use local `data/dart_cache/` only; no OpenDART network calls
- In cache-only mode, `fundamental_tickers=200` can still mean many tickers only have
  market fields. Always inspect `quarters_nonempty` and `filing_text_nonempty`.
- For cache repairs, do not trust the controlled backfill progress file alone. Verify target-month
  diskcache keys, optional filing misses, latest quarter distribution, and cache size/volume.

## Primary commands

- Producer:
  - `python scripts/build_kr_snapshot.py --as-of YYYY-MM-DD --profile ci_safe --force`
  - `python scripts/run_local_collection.py --storage-root out/<label> --as-of YYYY-MM-DD --market kr --phase full --full-universe-kind kospi200 --start YYYY-01-01`
  - `python scripts/run_local_collection.py --storage-root out/<label> --as-of YYYY-MM-DD --market kr --phase full --full-universe-kind kospi200 --start YYYY-01-01 --resume --dart-mode cache_only`
- Consumer:
  - `eit build-snapshot YYYY-MM --market kr --bundle-dir ../eit-market-data/artifacts/snapshots`
