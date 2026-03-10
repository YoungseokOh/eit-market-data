# Runbook

## Relevant files

- `src/eit_market_data/kr/krx_auth.py`
- `src/eit_market_data/kr/market_helpers.py`
- `src/eit_market_data/kr/pykrx_provider.py`
- `scripts/krx_login.py`
- `scripts/preflight_kr_data.py`

## Canonical symptoms

- `KrxAuthRequired`
- `400 LOGOUT`
- `KeyError('지수명')`
- Empty result from KRX index, ticker-list, market-cap, or market-fundamental paths

## First commands

- `python scripts/krx_login.py`
- `python scripts/preflight_kr_data.py --as-of YYYY-MM-DD --ticker 005930`

## Constraints

- Official KRX login is local/WSL or self-hosted only.
- Do not route GitHub-hosted CI back to browser-authenticated pykrx.
- Keep Yahoo fallback as non-default and avoid reintroducing it as the primary path.

