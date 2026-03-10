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
- `scripts/crawl_kr_data.py`
- `scripts/build_kr_snapshot.py`
- `scripts/run_daily_batch.py`

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

