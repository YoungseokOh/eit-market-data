# FinanceDataReader KRX Replacement Assessment

Date: 2026-03-12

## Summary

Scope is limited to the KRX-authenticated parts of the KR bundle:

- prices
- ticker-list
- market-cap
- market-fundamental
- benchmark
- sector

Decision bar is strict parity at the bundle behavior level. A source counts as replaceable only if it exposes a stable public API that can reproduce the required data shape and runtime behavior.

Conclusion: `FinanceDataReader` can replace only part of the current KRX-authenticated stack. It is not a full replacement for the KR bundle in its current state.

## Environment

- Repo default cookie path: `/root/.cache/eit-market-data/krx-profile/cookies.json`
- Synced source cookie path: `/mnt/c/Users/seok436/.cache/eit-market-data/krx-profile/cookies.json`
- Default-path verification passed:
  - `python scripts/probe_fdr_krx_session.py`
  - auth, KRX price, KRX index, KRX index constituents, and KRX marcap all returned `OK`
- Installed library version: `FinanceDataReader 0.9.110`
- Upstream references:
  - GitHub: <https://github.com/FinanceData/FinanceDataReader>
  - PyPI: <https://pypi.org/project/finance-datareader/>

## Findings Matrix

| Area | Current requirement | FDR public API candidate | Result | Notes |
|---|---|---|---|---|
| prices | Daily KRX OHLCV for a ticker | `fdr.DataReader("KRX:005930", start, end)` | Replaceable | Live call returned OHLCV plus `MarCap` and `Shares`. |
| ticker-list | Market ticker universe for KOSPI/KOSDAQ | `fdr.StockListing("KOSPI")`, `fdr.StockListing("KOSDAQ")` | Replaceable | Live call returned rows with `Code`, `Name`, `Market`. |
| market-cap | Market snapshot with close, cap, volume, amount | `fdr.StockListing("KRX")` or `fdr.StockListing("KRX-MARCAP")` | Replaceable | Live call returned `Close`, `Marcap`, `Volume`, `Amount`, `Stocks`. A thin column adapter is enough. |
| market-fundamental | Market-wide `BPS`, `PER`, `PBR`, `EPS`, `DIV`, `DPS` snapshot | None found | Not replaceable | Public FDR KRX listing API does not expose these fields. |
| benchmark | Official KOSPI index OHLCV | `fdr.DataReader("KRX-INDEX:1001", start, end)` | Replaceable | Live call returned index OHLCV and market cap fields. |
| sector | Point-in-time sector map for KOSPI/KOSDAQ | None stable | Not replaceable | No public FDR sector API. Internal `sector_stock_list()` returned empty in this environment. |

## Code-Level Mapping

| Bundle area | Current code path | Current live source | FDR candidate | Replacement status | Gap |
|---|---|---|---|---|---|
| prices | `PykrxProvider.fetch_prices()` in `src/eit_market_data/kr/pykrx_provider.py` | `pykrx.stock.get_market_ohlcv_by_date()` | `fdr.DataReader("KRX:<ticker>", start, end)` | Replaceable | Only a column adapter is needed. |
| ticker-list | `fetch_market_ticker_list()` in `src/eit_market_data/kr/market_helpers.py` | `pykrx.stock.get_market_ticker_list()` | `fdr.StockListing("KOSPI")`, `fdr.StockListing("KOSDAQ")` | Replaceable | Need to derive ticker list from `Code` column. |
| market-cap | `fetch_market_cap_frame()` in `src/eit_market_data/kr/market_helpers.py` and `_market_cap_frame()` in `src/eit_market_data/kr/fundamental_provider.py` | `pykrx.stock.get_market_cap()` | `fdr.StockListing("KRX")` or `fdr.StockListing("KRX-MARCAP")` | Replaceable | Need column rename: `Marcap -> 시가총액`, `Stocks -> 상장주식수`, etc. |
| market-fundamental | `fetch_market_fundamental_frame()` in `src/eit_market_data/kr/market_helpers.py` | `pykrx.stock.get_market_fundamental()` | None found | Not replaceable | No public FDR API returning `BPS`, `PER`, `PBR`, `EPS`, `DIV`, `DPS` market-wide. |
| benchmark | `PykrxProvider.fetch_benchmark()` via `fetch_index_ohlcv_frame()` | `pykrx.stock.get_index_ohlcv_by_date()` | `fdr.DataReader("KRX-INDEX:1001", start, end)` | Replaceable | Same benchmark semantics are available. |
| sector | `PykrxProvider.fetch_sector_map()` via `load_sector_snapshot_map()` then `fetch_live_sector_classification_map()` | cached parquet snapshots, then `pykrx.stock.get_market_sector_classifications()` | None stable | Not replaceable | No public FDR point-in-time sector API. Internal Naver helper is not reliable. |

## Existing Behavior Details

### market-fundamental

There are two related paths in the current repo.

1. Preflight health check
   - `scripts/preflight_kr_data.py`
   - calls `fetch_market_fundamental_frame(as_of, "KOSPI")`
   - this goes to `pykrx.stock.get_market_fundamental()`
   - expected columns are exactly:
     - `BPS`
     - `PER`
     - `PBR`
     - `EPS`
     - `DIV`
     - `DPS`

2. Actual bundle fundamentals
   - `CompositeKrFundamentalProvider` in `src/eit_market_data/kr/fundamental_provider.py`
   - does **not** use the market-fundamental frame directly
   - instead it merges:
     - DART quarterly statements
     - pykrx market-cap snapshot
     - pykrx last-close snapshot
   - fields injected from the market snapshot are:
     - `market_cap`
     - `issued_shares`
     - `last_close_price`
   - if `eps` is missing, it is derived from `net_income / issued_shares`

Implication:
- `market-fundamental` is currently a real pykrx dependency for preflight and for parity with the existing market helper layer.
- The bundle's per-ticker fundamental object is mainly `DART + pykrx market snapshot`, not `pykrx market fundamental` wholesale.

### sector

Current sector resolution order is:

1. `PykrxProvider.fetch_sector_map()` in `src/eit_market_data/kr/pykrx_provider.py`
2. load cached sector parquet from `load_sector_snapshot_map()`
3. if unresolved, call live `fetch_live_sector_classification_map()`
4. live source is `pykrx.stock.get_market_sector_classifications()`
5. unresolved tickers fall back to `"General"`

This path is used directly by the KR official bundle setup in `create_kr_providers()`:

- `sector_provider=pykrx`

Implication:
- sector is a direct bundle input, not just a diagnostics check
- replacing it requires a stable point-in-time sector map source, not just a best-effort label lookup

## Evidence

### 1. KRX-authenticated probes succeeded after cookie sync

Default-path run:

```text
[OK] krx:auth: probe rows=50
[OK] fdr:price: 005930 rows=8 latest=2026-03-12 ...
[OK] fdr:index: 1001 rows=8 latest=2026-03-12 ...
[OK] fdr:index-stock: 1001 constituents=837
[OK] fdr:marcap: KRX rows=2881 ...
```

### 2. Public FDR APIs that passed live checks

- `fdr.DataReader("KRX:005930", "2026-03-01", "2026-03-12")`
  - returned `Open`, `High`, `Low`, `Close`, `Volume`, `MarCap`, `Shares`
- `fdr.DataReader("KRX-INDEX:1001", "2026-03-01", "2026-03-12")`
  - returned index OHLCV
- `fdr.StockListing("KOSPI")`
  - returned 951 rows in this environment
- `fdr.StockListing("KRX")`
  - returned 2881 rows in this environment
- `fdr.SnapDataReader("KRX/INDEX/STOCK/1001")`
  - returned 837 constituents

### 3. Gaps that block full replacement

- `market-fundamental`
  - `fdr.StockListing("KRX-MARCAP")` did not expose `BPS`, `PER`, `PBR`, `EPS`, `DIV`, `DPS`
  - no alternative public API was found in the installed package
- `sector`
  - no public `SnapDataReader` or `StockListing` endpoint was found for point-in-time sector classification
  - internal `FinanceDataReader.naver.snap.sector_stock_list()` returned an empty frame

## Notes on Upstream State

- The installed package reports `__version__ = "0.9.110"`.
- PyPI shows `0.9.110` released on 2026-03-11.
- GitHub upstream was updated on 2026-03-11 and recent commit messages mention new KRX listing and caching work.
- Even with these fresh KRX additions, the current public surface still does not cover bundle-parity `market-fundamental` and `sector`.

## Final Verdict

`FinanceDataReader` is viable as a partial replacement for the KRX-authenticated stack:

- keep: `prices`, `ticker-list`, `market-cap`, `benchmark`
- keep `pykrx` or another source for: `market-fundamental`, `sector`

Therefore, it cannot replace the full KRX-authenticated part of the KR bundle by itself as of 2026-03-12.
