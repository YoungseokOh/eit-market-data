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
- `None of [Index(['종가', '시가총액', ...])] are in the [columns]`
- `None of [Index(['BPS', 'PER', ...])] are in the [columns]`
- missing `종가` during official pykrx collection

## First commands

- `python scripts/krx_login.py`
- `python scripts/preflight_kr_data.py --as-of YYYY-MM-DD --ticker 005930`

## Modern pykrx credential path

If `.env` has `KRX_ID` and `KRX_PW`, verify the direct pykrx path before opening a
browser:

```bash
python scripts/krx_login.py --timeout 30
```

Expected healthy output:

```text
KRX 로그인 시도...
KRX 로그인 완료.
[OK] probe rows=50
```

Then verify official market paths:

```bash
python - <<'PY'
from dotenv import load_dotenv
load_dotenv(".env")
from datetime import date, timedelta
from eit_market_data.kr.krx_auth import ensure_krx_authenticated_session
from eit_market_data.kr.market_helpers import (
    fetch_index_ohlcv_frame,
    fetch_market_cap_frame,
    fetch_market_ticker_list,
)

AS_OF = "YYYY-MM-DD"  # target trading/as-of date for the probe
as_of = date.fromisoformat(AS_OF)
session = ensure_krx_authenticated_session(interactive=False)
print("session", type(session).__name__, len(session.cookies))
print("tickers", len(fetch_market_ticker_list(as_of, "KOSPI")))
idx, source = fetch_index_ohlcv_frame(
    "1001", as_of - timedelta(days=10), as_of, official_only=True
)
print("index_source", source, "rows", 0 if idx is None else len(idx))
cap = fetch_market_cap_frame(as_of, "KOSPI", use_local=False)
print("cap_rows", 0 if cap is None else len(cap))
PY
```

Expected healthy shape:

```text
session Session ...
tickers 900+
index_source pykrx rows ...
cap_rows 900+
```

## Misleading pykrx schema errors

When a non-interactive shell does not load `.env`, official pykrx calls may not authenticate
and can return empty/malformed frames. `crawl_kr_data_pykrx.py` may then report missing
columns such as `종가`, `시가총액`, `BPS`, `PER`, or `KeyError('지수명')`.

Recovery order:

1. Confirm `.env` exists and `KRX_ID`/`KRX_PW` are present without printing values.
2. Confirm the entrypoint loads `.env` before pykrx calls.
3. Run `python scripts/preflight_kr_data.py --as-of YYYY-MM-DD --ticker 005930 --skip-news`.
4. Only patch dataframe normalizers after auth/session has been ruled out.

## LOGOUT after successful pykrx login

If pure `pykrx` calls succeed but repo helpers still fail with `400 LOGOUT`, inspect
`src/eit_market_data/kr/krx_auth.py` for legacy `webio.Get/Post` overrides. In modern
`pykrx`, those hooks must be skipped when `pykrx.website.comm.auth` exposes both
`KRXSession` and `get_auth_session`.

The local fix pattern is:

- Detect modern `pykrx` auth before loading legacy `webio`.
- Prefer `pykrx_auth.get_auth_session().session` when `KRX_ID/KRX_PW` exist.
- Keep cookie/browser login as a fallback, not the first path.
- Add regression tests that fail if legacy `webio` hooks are installed in modern pykrx.

Use these checks after code changes:

```bash
python -m pytest tests/test_krx_auth.py tests/test_market_helpers.py -q
python -m ruff check src/eit_market_data/kr/krx_auth.py tests/test_krx_auth.py
python scripts/preflight_kr_data.py --as-of YYYY-MM-DD --ticker 005930 --skip-news
```

`preflight_kr_data.py` may exit nonzero if DART enrichment is degraded. For KRX auth,
check that `public:ticker-list`, `public:market-cap`, and `public:benchmark` are `[OK]`
and that the summary has `failed=0`.

## Constraints

- Official KRX login is local/WSL or self-hosted only.
- Do not route GitHub-hosted CI back to browser-authenticated pykrx.
- Keep fallback/retry paths scoped to recovery only and avoid promoting them to the default CI path.
