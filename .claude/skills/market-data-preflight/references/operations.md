# Operations

## Primary entrypoints

- `python scripts/preflight_kr_data.py --as-of YYYY-MM-DD --ticker 005930`
- `python scripts/crawl_kr_data_pykrx.py --start YYYY-MM-DD --end YYYY-MM-DD --output-root data`
- `python scripts/crawl_kr_data_fallback.py --start YYYY-MM-DD --end YYYY-MM-DD --output-root data` (legacy)
- `python scripts/build_kr_snapshot.py --as-of YYYY-MM-DD --profile ci_safe --force`
- `python scripts/run_local_collection.py --storage-root out/<label> --as-of YYYY-MM-DD --market kr --phase full --full-universe-kind kospi200 --start YYYY-01-01`
- `python scripts/run_daily_batch.py --as-of YYYY-MM-DD --snapshot-profile ci_safe --force-snapshot`

## Minimum readiness checks

- `KRX_ID` and `KRX_PW` are loaded for official local pykrx collection.
- `DART_API_KEY` and `ECOS_API_KEY` exist for KR fundamentals and macro.
- KR optional deps are installed from `.[kr]`.
- `universes/kr_universe.csv` exists and has `ticker`, `market`, `sector`, `name`.
- Output roots are writable: `artifacts/`, `out/`, or caller-provided paths.

## Interpret preflight outcomes

- `failed`: stop the pipeline.
- `degraded`: continue only if the requested profile allows it.
- `ok`: continue to crawl or bundle export.

## pykrx auth-shaped schema errors

When `scripts/crawl_kr_data_pykrx.py` runs without `.env`/`KRX_ID`/`KRX_PW` in a
non-interactive shell, pykrx may emit misleading schema errors instead of a clean auth
failure:

- `None of [Index(['종가', '시가총액', ...])] are in the [columns]`
- `None of [Index(['BPS', 'PER', ...])] are in the [columns]`
- missing `종가`
- `KeyError('지수명')`

Do not patch schemas first. Load `.env`, verify `KRX_ID`/`KRX_PW` presence without printing
secret values, run `scripts/preflight_kr_data.py --as-of YYYY-MM-DD --ticker 005930 --skip-news`,
then retry the official pykrx collector.

## DART timeout fallback

If a broad KR run reaches OpenDART `ConnectTimeout`, `RemoteDisconnected`, HTTP 000,
or repeated empty `013` responses, do not keep probing DART. Continue the same local
run without OpenDART network calls:

```bash
python scripts/run_local_collection.py \
  --storage-root out/<label> \
  --as-of YYYY-MM-DD \
  --market kr \
  --phase full \
  --full-universe-kind kospi200 \
  --start YYYY-01-01 \
  --resume \
  --dart-mode cache_only
```

This preserves KRX/pykrx prices, market cap, sectors, benchmark, and ECOS macro.
DART fundamentals/filing text become cache coverage, so report `quarters_nonempty`
and `filing_text_nonempty` separately from `fundamental_tickers`/`filing_tickers`.

## Controlled DART cache backfill

Do not run broad live DART collection directly against KOSPI200/all tickers. If live
DART must be used to improve cache coverage, use a bounded cache backfill pattern:

- Input must be an explicit universe or failed-ticker list.
- Skip ticker/month keys already present in `data/dart_cache/`.
- Sleep at least `5s` between tickers.
- Persist progress and support resume.
- Stop immediately on `ConnectTimeout`, `ReadTimeout`, `RemoteDisconnected`,
  `Connection reset`, HTTP 000, `Max retries exceeded`, or repeated empty `013`.
- Stop on `DART fundamentals returned empty`; it can hide repeated `013` empty responses
  for a ticker.
- Treat `DART filing returned empty` as a filing-text extraction miss in the default
  `--filing-mode optional`: record `filing_empty` and continue so financial statement
  coverage can still complete. Use `--filing-mode strict` only when filing text is the
  hard gate.
- Set `--as-of` to the bundle decision date being repaired. A cache filled at a later
  month is intentionally not used by older point-in-time snapshots.
- After stopping, resume the KR run with `--dart-mode cache_only`.
- Treat `progress.json` as a resume checkpoint, not completion proof. Before reporting
  the backfill complete, read the actual diskcache keys for the target universe/month and
  report `missing_fundamental`, `missing_filing`, latest quarter distribution,
  `cache.size_limit`, and `cache.volume()`.
- The DART diskcache default size limit is 50GB. Increase it with
  `EIT_DART_CACHE_SIZE_LIMIT_BYTES` when covering broader universes or many months; keep
  at least 3-5x expected cache volume as headroom to avoid eviction.

Use `scripts/backfill_dart_cache_controlled.py` for this pattern; do not use
`scripts/backfill_all.py --phase 2` for KOSPI200-only cache repair because it targets
the broad KR universe.

## Run Evidence

For each local KOSPI200 refresh, inspect and report evidence from the current run root:

- raw pykrx report: counts for `cap_daily`, `fundamental`, `index`, and `sector`
- validation reports: `failed=0` is the hard gate; explain any `degraded` items
- market coverage: `price_tickers`, `market_cap_nonnull`, `last_close_nonnull`, sector map coverage
- last price date: must be at or before the bundle `decision_date`
- DART actual coverage: `quarters_nonempty` and `filing_text_nonempty`

Never treat one dated run root as the permanent “known good” baseline. Use it only as
historical evidence if the user explicitly asks about that run.

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
- Local KOSPI200 run:
  - `out/<label>/runs/YYYY-MM-DD/kr_full_kospi200/progress.json`
  - `out/<label>/runs/YYYY-MM-DD/kr_full_kospi200/reports/full_kr_raw.json`
  - `out/<label>/runs/YYYY-MM-DD/kr_full_kospi200/bundles/kr/full/snapshots/YYYY-MM/summary.json`
  - `out/<label>/runs/YYYY-MM-DD/kr_full_kospi200/bundles/kr/full/snapshots/YYYY-MM/validation_report.json`
