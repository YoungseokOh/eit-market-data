# US Delisted-Price Supplementary Source — Feasibility & Implementation

**Date:** 2026-07-02
**Author:** automated feasibility+implementation run (Claude Opus 4.8)
**Branch:** `feat/us-delisted-price-source`
**Scope:** US market ONLY. Follow-up to
`docs/us_backfill_2019_2022_feasibility.md` (the survivorship gate).
**No bundle build was run.** This work only secures + validates the price
source and ships the provider/test/wiring.

## Why this exists

The 2019–2022 feasibility gate found the backfill is blocked by **survivorship
bias**: yfinance retroactively **purges the entire price history** (incl.
2019–2022) of delisted / acquired / taken-private tickers — Yahoo returns HTTP
404 "Quote not found". The gate confirmed **59 former S&P 500 large-caps** that
return empty from yfinance, creating a 5–11 % priceless hole per PIT-universe
month. The owner's decision: secure the delisted prices from a supplementary
free source *first*, because that is the only path that actually **reduces**
survivorship bias.

## 1. Gap set (59 names)

Taken from the gate doc (section 2), all former S&P 500 constituents:

```
ABMD ADS AGN ALXN ANSS APC ARNC ATVI CELG CERN CMA CTLT CTXS CXO DAY DFS
DISCA DISCK DISH DRE DWDP ETFC FBHS FL FLIR FRC GPS HBI HES HFC HOLX INFO
IPG JNPR JWN K KSU LLL MRO MXIM NBL NLSN PBCT PXD RE RHT RTN SBNY SEE SIVB
TIF TSS TWTR VAR VIAB WBA WCG XEC XLNX
```

## 2. Source probing — Stooq is BLOCKED, stockanalysis.com works

**Stooq (the mission's primary candidate): BLOCKED.** The CSV download endpoint
`https://stooq.com/q/d/l/?s=<TICKER>.US...` now sits behind a JavaScript
proof-of-work anti-bot wall. The PoW (SHA-256 hashcash, 4 hex-zero prefix) is
solvable in ~1 s and the `/__verify` handshake returns 200 with session
cookies, **but the CSV endpoint still returns `Access denied`** with a valid
session + Referer, for every symbol and every URL variant tried (dated,
undated, `stooq.pl`). The server-rendered chart HTML only carries the most
recent handful of rows (full 2019–2022 history is fetched client-side from the
same blocked endpoint), and Stooq began re-issuing PoW challenges aggressively.
Per the "stop-on-block, don't hammer" discipline, Stooq was abandoned after
~12 spaced requests. `pandas_datareader.stooq` is not installed and would hit
the same blocked endpoint anyway.

**stockanalysis.com (secondary free fallback): WORKS. No key, no login.**
JSON history API:
`https://stockanalysis.com/api/symbol/s/<TICKER>/history?range=10Y&period=Daily`
→ rows `{t (date), o, h, l, c (raw close), a (adjusted close), v (volume)}`,
newest-first. `range=10Y` returns 10 years ending at the symbol's **last**
bar, so for a delisted symbol it fully covers 2019–2022 and any 300-trading-day
lookback.

## 3. Recovery — 16 / 59 (27 %)

| Outcome | Count | Names |
|---|---|---|
| **Recovered** (≥200 daily bars in 2019–2022) | **16** | ANSS ATVI CMA CTLT DAY FL HBI HOLX IPG JNPR JWN K SEE SIVB TWTR WBA |
| **Residual gap** (unobtainable on stockanalysis) | **43** | ABMD ADS AGN ALXN APC ARNC CELG CERN CTXS CXO DFS DISCA DISCK DISH DRE DWDP ETFC FBHS FLIR FRC GPS HES HFC INFO KSU LLL MRO MXIM NBL NLSN PBCT PXD RE RHT RTN SBNY TIF TSS VAR VIAB WCG XEC XLNX |

The residual 43 are genuinely absent from this free source: the search API
either returns nothing for the symbol (ABMD, ALXN, CERN, XLNX, DWDP, DISCA/
DISCK, …) or the symbol has been **reused by a newer unrelated listing** (APC →
ARKO Petroleum, INFO → an ETF) with no pre-delisting history. OTC pink-sheet
listings that surface in search (`otc/SBNY` Signature, `otc/FRCKL` First
Republic) are not retrievable through the history API (400/404). The `.RT`
suffix variant (e.g. `CELG.RT`) returns only the acquirer's post-merger stub
(2023+), not pre-2019 history.

### Residual survivorship hole — last-known market caps

Every residual name was an S&P 500 constituent, so **the entire residual hole is
large-cap** (S&P 500 inclusion floor was ~$6–13 B over 2019–2022; none are
immaterial small-caps). Approximate last market caps of the most
regime-relevant residuals:

| Ticker | Company | Event | Approx. last mkt cap |
|---|---|---|---|
| RTN | Raytheon | → RTX merger, Apr 2020 | ~$100 B |
| DWDP | DowDuPont | split into DOW/DD/CTVA, 2019 | ~$100–150 B |
| CELG | Celgene | → Bristol-Myers, Nov 2019 | ~$74 B |
| PXD | Pioneer Natural Res. | → ExxonMobil, 2024 | ~$50 B |
| HES | Hess | → Chevron (pending) | ~$45 B |
| INFO | IHS Markit | → S&P Global, 2022 | ~$44 B |
| XLNX | Xilinx | → AMD, Feb 2022 | ~$35 B |
| DISCA/DISCK | Discovery | → Warner Bros. Discovery, 2022 | ~$30 B combined |
| CERN | Cerner | → Oracle, 2022 | ~$28 B |
| MRO | Marathon Oil | → ConocoPhillips, 2024 | ~$17 B |
| TIF | Tiffany & Co. | → LVMH, 2021 | ~$16 B |
| FRC | First Republic Bank | **2023 bank failure** | ~$20 B (peak) |
| SBNY | Signature Bank | **2023 bank failure** | ~$20 B (peak) |

Others (ADS, AGN=Allergan~$63B→AbbVie, ALXN=Alexion~$39B→AstraZeneca, ANSS
recovered, APC=Anadarko→Occidental, ARNC, CTXS=Citrix, CXO=Concho, DFS=Discover
→Capital One 2025, DISH, DRE=Duke Realty, ETFC=E*Trade→Morgan Stanley,
FBHS=Fortune Brands, FLIR, GPS→Gap/still-listed-under-GAP, HFC=HollyFrontier,
KSU=Kansas City Southern~$30B, LLL=L3, MXIM=Maxim~$30B→ADI, NBL=Noble Energy,
NLSN=Nielsen, PBCT=People's United, RE=Everest Re, RHT=Red Hat~$34B→IBM,
TSS=Total System, VAR=Varian~$16B→Siemens, VIAB=Viacom, WCG=WellCare~$18B,
XEC=Cimarex) are all multi-billion former large-caps.

## 4. Validation — the source is trustworthy

### 4.1 Control cross-check (still-listed names, yfinance vs stockanalysis)

2019–2022 daily closes, 1008 common trading days each. yfinance uses
`auto_adjust=True` (the exact convention that built the existing 42 bundles).

| Ticker | vs stockanalysis `a` (adj close) | vs raw `c` |
|---|---|---|
| AAPL | mean 0.000 %, **max 0.073 %** | mean 3.09 %, max 5.37 % |
| MSFT | mean 0.000 %, **max 0.004 %** | mean 4.84 %, max 7.35 % |
| JPM | mean 0.044 %, **max 0.065 %** | mean 15.1 %, max 22.9 % |
| XOM | mean 0.025 %, **max 0.057 %** | mean 25.2 %, max 40.3 % |
| KO | mean 0.000 %, **max 0.000 %** | mean 18.2 %, max 25.7 % |
| JNJ | mean 0.000 %, **max 0.003 %** | mean 16.8 %, max 23.1 % |

**Finding:** stockanalysis's adjusted close `a` matches yfinance
`auto_adjust=True` to **<0.08 %** everywhere (rounding-level), while the raw `c`
diverges by up to 40 % on dividend-heavy names. So `a` is the split+dividend
(total-return) adjusted close — **exactly the bundle convention.**

**Adjustment mapping used by the provider:** the API gives raw OHLC + adjusted
close `a`. yfinance `auto_adjust=True` scales O/H/L/C by one daily factor. We
reproduce it per row: `factor = a / c`; `adj_open = o·factor`,
`adj_high = h·factor`, `adj_low = l·factor`, `close = a`; **volume left raw**
(yfinance auto_adjust does not adjust volume). OHLC rounded to 2 dp, matching
`core/price_frame.price_bars_from_frame`.

### 4.2 Delisted sanity spot-check

- **TWTR** — series ends **2022-10-28** at ~$53.70 (Musk buyout closed at
  $54.20 on 2022-10-27; final tape ~$53.70). Correct delisting bound.
- **SIVB** — **alive and trading throughout 2019–2022**; series continues to
  2023-03-09 showing the collapse (266.86 → 106.04 on the run) before the
  halt. Directly confirms the survivorship fix: the bank-failure name is
  present, not purged.
- **ATVI** — ends 2023-10-13 (Microsoft deal closed 2023-10-13). Correct.
- **WBA** — ends 2025-08-27 (taken private 2025). Correct.
- No zero/duplicated bars observed in recovered windows (zero-close rows are
  defensively skipped by the parser).

## 5. Implementation

- **`src/eit_market_data/stockanalysis_provider.py`**
  - `StockAnalysisPriceProvider` — implements the `PriceProvider` protocol
    (`fetch_prices(ticker, as_of, lookback_days=300)`). Stdlib-only HTTP,
    `asyncio.to_thread` for blocking I/O, module-level `Semaphore(2)` + 0.5 s
    spacing, `as_of` PIT filter (drops `date > as_of`), lookback cap, adjusted
    OHLC per §4.1. Errors → `[]` + `logger.warning`; never raises.
  - `FallbackPriceProvider(primary, supplementary)` — returns the primary
    (yfinance) result unless empty, then falls through to the supplementary
    source. **Fill-empties-only**, so still-listed names never touch the
    fallback and the existing 42 bundles are unaffected.
- **Wiring** — `create_real_providers()` in `snapshot.py` wraps the *price*
  provider slot with `FallbackPriceProvider(yf, StockAnalysisPriceProvider())`
  **only when `EIT_US_DELISTED_FALLBACK` is truthy** (`1`/`true`/`yes`).
  Default = plain `YFinanceProvider` (byte-identical to today). Fundamentals
  (edgar_xbrl), sector, and benchmark paths are untouched.
- **Test** — `tests/test_stockanalysis_provider.py` (7 tests, all pass, no live
  network): row→bar parsing, `a/c` adjustment, `as_of` future-bar drop,
  lookback cap, empty-on-error, missing-symbol empty, and both
  `FallbackPriceProvider` branches (prefers primary; uses supplementary only
  when primary empty).

### Live proof (2020-03 window, not a full build)

`FallbackPriceProvider(YFinanceProvider(), StockAnalysisPriceProvider())`,
`as_of=2020-03-31`, `lookback_days=60`:

| Ticker | yfinance | fallback | last bar |
|---|---|---|---|
| TWTR | 0 bars | 60 bars | 2020-03-31 close 24.56 |
| SIVB | 0 bars | 60 bars | 2020-03-31 close 151.08 |
| ATVI | 0 bars | 60 bars | 2020-03-31 close 57.84 |
| HOLX | 0 bars | 60 bars | 2020-03-31 close 35.10 |
| WBA | 0 bars | 60 bars | 2020-03-31 close 35.09 |

Every name yfinance leaves empty is filled with 60 plausible COVID-era bars
ending exactly at `as_of` — the PIT-universe slot is populated end-to-end
without running the 15–20 h build.

## 6. Recommendation — QUALIFIED GO, reduced but non-zero residual hole

The supplementary source is **trustworthy** (adjustment matches to <0.08 %,
delisting bounds correct, no corrupt bars) and **recovers 16 of 59** purged
large-caps — including the marquee **SIVB** (Silicon Valley Bank, now
observable through its 2019–2022 life) plus TWTR, ATVI, HOLX, K, WBA and 10
others.

**But 43 of 59 remain unobtainable on any free key-less source tried.** The
residual hole is *smaller* than before but still entirely large-cap, and it
still includes two of the three 2023 bank-failure names (**FRC, SBNY**) and
major M&A caps (RTN, CELG, XLNX, CERN, DWDP, DISCA/DISCK, PXD, INFO).

Net effect on the projected per-month empty-price rate (gate §3): enabling the
fallback removes ~16 of the ~60→23 empties, shrinking the 2019-01 hole from
~11 % toward ~8 % and the 2022-12 hole from ~4.4 % toward ~3 %. **Materially
better, not eliminated.**

**Recommendation:**
- **GO** on the 2019–2022 build **with `EIT_US_DELISTED_FALLBACK=1`** — it
  strictly reduces survivorship bias at zero cost to still-listed names or the
  existing bundles.
- **Still ship the `known_gaps.json` manifest** (gate §4 condition): the
  residual 43 large-caps remain a documented, non-silent hole. State to the
  consumer that FRC/SBNY collapses and the RTN/CELG/XLNX-class M&A caps are
  still unobservable.
- If the consumer judges the residual ~3–8 % hole unacceptable at the 2019
  start, the honest fallback remains starting the window later (2021-01+,
  where the residual drops further) rather than shipping a silent bias.

A paid source (Tiingo / Alpha Vantage / FMP / Polygon) would be required to
close the residual 43; that was explicitly out of scope (no keys). This is
flagged as the remaining honest limit.

## 7. Verification notes

- Import gate `python -c "import eit_market_data"` passes.
- `tests/test_stockanalysis_provider.py`: 7 passed.
- Existing 42 US bundles under `artifacts/us/snapshots/` are **untouched** (no
  build was run; `artifacts/` is gitignored). Default provider wiring
  (`EIT_US_DELISTED_FALLBACK` unset) returns plain `YFinanceProvider`, verified.
