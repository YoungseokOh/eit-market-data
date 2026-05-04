# Provider Map

## Factory and protocol roots

- `src/eit_market_data/providers.py`
- `src/eit_market_data/snapshot.py`
- `src/eit_market_data/schemas/snapshot.py`

## KR providers

- `src/eit_market_data/kr/dart_provider.py`
- `src/eit_market_data/kr/ecos_provider.py`
- `src/eit_market_data/kr/ci_safe_provider.py`
- `src/eit_market_data/kr/fundamental_provider.py`
- `src/eit_market_data/kr/pykrx_provider.py`
- `src/eit_market_data/kr/krx_auth.py`

## Operational scripts

- `scripts/preflight_kr_data.py`
- `scripts/crawl_kr_data_pykrx.py`
- `scripts/crawl_kr_data_fallback.py`
- `scripts/crawl_kr_data.py` (legacy authenticated recovery path, not default KR runtime)
- `scripts/build_kr_snapshot.py`
- `scripts/run_daily_batch.py`
- `scripts/run_local_collection.py`
- `scripts/backfill_dart_cache_controlled.py`

## pykrx script requirements

- Official pykrx scripts must load project `.env` so `KRX_ID`/`KRX_PW` are available in
  non-interactive shells.
- Missing KRX credentials can look like dataframe schema drift (`종가`, `BPS`, `PER`, `지수명`)
  rather than a clean login exception.
- Pair any pykrx entrypoint change with a bounded preflight or one-date schema probe.

## DART modes

- `DartProvider`: live OpenDART + diskcache; use for small probes or controlled live runs.
- `CacheOnlyDartProvider`: diskcache only; use after OpenDART timeout/rate-limit signals so
  KRX/pykrx collection can finish without more DART network requests.
- Controlled live runs must be universe-limited, skip cached ticker/month keys, sleep at least
  `5s` between tickers, persist progress for resume, and stop on transient OpenDART errors.
- Also stop on empty fundamental results (`DART fundamentals returned empty`) because these can
  follow repeated `013` responses.
- Default filing backfill is optional: `DART filing returned empty` is recorded as
  `filing_empty` so financial statements can continue. Use strict filing mode only when
  filing text is the gate.
- Use the target bundle decision date as `--as-of`; later cache months must not repair older
  point-in-time snapshots.
- Never make broad KOSPI200/all-ticker collection depend on repeated live DART retries.
- The controlled script is `scripts/backfill_dart_cache_controlled.py`.
- DART diskcache readers/writers should share the repo size limit default (50GB) and respect
  `EIT_DART_CACHE_SIZE_LIMIT_BYTES`.
- Treat progress files as resume checkpoints only. Validate actual cache keys and latest quarter
  distribution before calling a DART cache repair complete.

## Test clusters

- `tests/test_kr_fundamental_provider.py`
- `tests/test_ci_safe_provider.py`
- `tests/test_pykrx_provider.py`
- `tests/test_krx_auth.py`
- `tests/test_market_helpers.py`
- `tests/test_daily_batch.py`
- `tests/test_scripts.py`

## Typical paired edits

- Provider behavior change -> factory wiring + tests
- New snapshot field -> schema + provider + bundle exporter + consumer contract
- New dependency -> `pyproject.toml` + docs + environment checks
