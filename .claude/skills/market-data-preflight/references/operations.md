# Operations

## Primary entrypoints

- `python scripts/preflight_kr_data.py --as-of YYYY-MM-DD --ticker 005930`
- `python scripts/crawl_kr_data.py --as-of YYYY-MM-DD --universe-csv universes/kr_universe.csv --output-root out/data`
- `python scripts/build_kr_snapshot.py --as-of YYYY-MM-DD --profile ci_safe --force`
- `python scripts/run_daily_batch.py --as-of YYYY-MM-DD --snapshot-profile ci_safe --force-snapshot`

## Minimum readiness checks

- `DART_API_KEY` and `ECOS_API_KEY` exist for KR fundamentals and macro.
- KR optional deps are installed from `.[kr]`.
- `universes/kr_universe.csv` exists and has `ticker`, `market`, `sector`, `name`.
- Output roots are writable: `artifacts/`, `out/`, or caller-provided paths.

## Interpret preflight outcomes

- `failed`: stop the pipeline.
- `degraded`: continue only if the requested profile allows it.
- `ok`: continue to crawl or bundle export.

## Common escalation paths

- `krx:auth`, `LOGOUT`, `KeyError('지수명')`, empty KRX index/listing/cap data:
  use `krx-auth-recovery`.
- Missing `report_date`, future-dated values, `date.today()` leaks:
  use `point-in-time-guardrails`.
- Bundle export or consumer mismatch:
  use `kr-bundle-pipeline`.

## Artifacts to inspect

- Daily batch: `out/<as_of>_<timestamp>/summary.json`
- Snapshot build: `artifacts/snapshots/YYYY-MM/summary.json`
- Bundle files:
  - `snapshot.json`
  - `metadata.json`
  - `manifest.json`
  - `summary.json`

