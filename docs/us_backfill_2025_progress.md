# US Backfill — Progress & Findings

> Status doc for the eit-research side. Updated live by the producer while the
> US monthly-bundle backfill is in progress.
> **Last updated:** 2026-06-24 (XBRL backfill phase)

## UPDATE 2026-06-24 — switched fundamentals to SEC EDGAR XBRL, extended to 2023-01

The yfinance fundamentals wall (rolling ~5 quarters) is now bypassed by a new
**SEC EDGAR XBRL** fundamentals provider — true point-in-time (per-fact `filed`
dates), history back to ~2009. Decision: **backfill 2023-01 .. 2026-06** with
XBRL fundamentals (≈37 months built; **2026-01..05 stay frozen/untouched**).

- New provider: `src/eit_market_data/edgar_xbrl_provider.py`
  (`EdgarXbrlFundamentalProvider`). Cached `companyfacts` per ticker → cheap
  multi-month backfill. PIT rule: a fact is visible only when `filed <= as_of`;
  latest restatement known by `as_of` wins; `report_date` = real SEC filing date.
- `market_cap` reconstructed as-of and **split-consistent**: as-of shares
  (dei cover-page, `filed<=as_of`) × as-*traded* close (yfinance split-adjusted
  close × split factor undone for splits after `as_of`). Validated:
  AAPL 2020-06 = 1.58T, NVDA 2024-01 (pre 10:1) = 1.52T, NVDA 2025-06 = 3.85T.
- Selectable via `build_us_batch --fundamentals-source edgar_xbrl`
  (`create_real_providers(fundamentals_source=...)`). Default stays `yfinance`
  so the daily job is unchanged.
- ⚠️ **Survivorship bias caveat:** the backfill reuses *today's* S&P500+NDX
  membership (~517) for all historical months. Names that had not yet IPO'd/been
  added drop out of early months; de-listed/removed names are absent. Bias grows
  the further back you go — material to interpret 2023–2024 results.

## TL;DR for consumers

- **Goal:** ≥12 evaluable US decision-months.
- **Chosen plan (decided):** build the viable window **2025-06 .. 2026-06**
  (13 complete bundles → **12 evaluable** decision-months, 2025-06..2026-05).
  Meets ≥12 using existing sources only. (2025-01..05 are not buildable — see
  fundamentals wall.)
- **Status:** 🟢 **Building** (started 2026-06-24). The already-complete bundles
  **2026-01..2026-05 are untouched and safe to keep using.** 2026-06 is being
  rebuilt from a 5-ticker partial to the full ~517 universe (decision 2026-06-23).
- **Producer bug FIXED:** US `market_cap`/`last_close_price` were pulled from
  yfinance **current** `.info` (`marketCap`/`previousClose`) — look-ahead. Now
  reconstructed as-of: `last_close = unadjusted close@decision_date`,
  `market_cap = last_close × issued_shares(most-recent PIT quarter)`. **Applies to
  newly built months only** (2025-06..2025-12, 2026-06); the frozen 2026-01..05
  retain their prior basis by instruction (do-not-touch).

### As-of proof (look-ahead fixed) — sample

| Ticker | 2025-06-30 close / market_cap | current (2026-06-18) close / market_cap |
|---|---|---|
| AAPL | 205.17 / 3.065T | 298.01 / 4.371T |
| MSFT | 497.41 / 3.698T | 379.40 / 2.819T |
| JPM  | 289.91 / 0.806T | 325.22 / 0.871T |

Values differ across dates → confirms as-of reconstruction, not a current pull.

## Audit results (point-in-time correctness of the US provider path)

| Source | Field | PIT-correct for historical months? | Notes |
|---|---|---|---|
| yfinance | daily prices / benchmark | ✅ yes | `auto_adjust=True`, filtered `date<=as_of` |
| yfinance | quarterly fundamentals | ⚠️ **only ~5 trailing quarters retained** | older quarters have **aged out** — see wall below |
| yfinance | `market_cap` / `last_close` | ❌ **look-ahead (current `.info`)** | fix in progress |
| SEC EDGAR | 10-K filing text + `filing_date` | ✅ yes | picks most recent 10-K with `filing_date<=as_of` |
| FRED | macro | ⚠️ mostly | filtered by `observation_end=as_of`; does **not** model release-lag/revision vintage (residual, cannot trip the consumer PIT guard since macro values aren't date-stamped) |

### The fundamentals wall (the blocker)

yfinance serves only a **rolling ~5-quarter window** of quarterly financials.
Probe (today, 2026-06-24):

```
AAPL/MSFT raw retained period-ends: 2026-03-31, 2025-12-31, 2025-09-30, 2025-06-30, 2025-03-31
JPM (deeper): … back to 2024-09-30 (7 qtrs), but issued_shares = None
fetch_fundamentals(AAPL, as_of=2025-01-31) -> quarters: []   # data gone
```

Consequence: for **early-2025 decision months** the PIT-available quarters
(period-end ≲ 2024-12) no longer exist in yfinance, so fundamentals coverage
collapses. Prices/benchmark/filings/macro for 2025 **are** available; only
**fundamentals** (and therefore `market_cap` and the value-yield screen) are not.

**Quantified feasibility (12-ticker sample, probed 2026-06-24):**

| Decision month | ≥1 PIT quarter | has `issued_shares` (→ as-of market_cap → value screen) |
|---|---|---|
| 2025-01 | 6/12 | **0/12** ❌ |
| 2025-06 | 12/12 | **12/12** ✅ |
| 2025-09 | 12/12 | 12/12 ✅ |
| 2025-12 | 12/12 | 12/12 ✅ |
| 2026-06 | 12/12 | 12/12 ✅ |

→ **Viable window from existing sources = 2025-06 .. 2026-06 (13 bundles → 12
evaluable decision-months).** This **meets the ≥12 goal.** Months
**2025-01..2025-05** cannot get PIT fundamentals/`market_cap` from yfinance and
would fail the ≥200-fundamentals / value-screen criteria.

This is an external-source limitation, not a code bug, and cannot be solved
without either (a) a true PIT fundamentals source (e.g. SEC XBRL
`companyfacts`, same EDGAR origin, but a **new provider**) or (b) accepting
degraded/empty 2025 fundamentals.

## Plan (pending user decision on the fundamentals wall)

1. ✅ Audit US provider path for look-ahead (done — findings above).
2. 🔧 Fix `market_cap`/`last_close` look-ahead in `YFinanceProvider`
   (as-of reconstruction). Benefits all bundles.
3. ⏳ Rebuild **2026-06** to full universe (feasible today, PIT-correct).
4. ⏳ 2025-01..2025-12 — **depends on chosen path** for the fundamentals wall.
5. ⏳ PIT-guard scan + as-of proof + value-screen smoke + schema parity.

## Per-month status

| Month | Bundle | Notes |
|---|---|---|
| 2023-01..2025-12 | 🟢 building (XBRL) | new PIT bundles, full ~517 universe; ~37 months total |
| 2026-01..2026-05 | ✅ complete | **untouched / in use** (yfinance-era fundamentals) |
| 2026-06 | 🟢 rebuilding (XBRL) | 5-ticker partial → full universe, decision 2026-06-23 |

ETA ≈ 40 min/month → roughly a day for the full 2023-01..2026-06 range. Build is
a detached background job (survives session resets).

_Per-month universe counts and PIT-scan results are filled in below as each
month completes._

### Operational note — daily job paused during backfill

The launchd daily job (`com.eit.market-data.daily`) overwrites the *current*
month US bundle with a 5-ticker universe each morning, which collides with the
full-universe backfill of 2026-06. It has been **unloaded (paused)** for the
duration of the backfill; the 5-ticker 2026-06 was removed so the backfill emits
a full-universe 2026-06 at the end (decision 2026-06-23). **Must re-enable after
backfill:** `launchctl load ~/Library/LaunchAgents/com.eit.market-data.daily.plist`.

### ⚠️ Bug found in verification (2026-06-24) — FIXED, first 9 months rebuilt

A quality audit of the first 9 completed months found that **~31% of tickers**
(153/493 in 2023-01) had a *stale* `quarters[0]`: quarters were ordered by
`report_date`, but old periods reappear as **comparative columns** in recent
10-K/10-Q filings, giving them a recent `filed` date that floated them to the
top (e.g. AAPL `quarters[0]` showed `2019Q3`). The value screen would then read
ancient fundamentals. PIT itself was never violated (all `report_date<=decision`),
but the most-recent-quarter selection was wrong.

**Fix:** order quarters by economic period **end** date, not filing date
(`edgar_xbrl_provider.py`). Validated: AAPL→2022Q2, MSFT→2022Q4 (filed 2023-01-24),
JPM→2022Q3 for a 2023-01-31 decision. Regression test added
(`test_quarters_ordered_by_period_end_not_comparative_filing`). The 9
buggy months were **discarded and are being rebuilt** with the fix.

### Verified-good signals (pre-fix audit, still valid)

- PIT look-ahead scan: **0 violations** across all bundles.
- Coverage (2023, per month): universe 517 · prices ~510 · fundamentals ~493 ·
  as-of market_cap ~480 · filings ~440 · benchmark 300 — all far above ≥200.
- As-of proof: `market_cap`/`last_close` are real historical values that move
  month-to-month (AAPL 2023-01 $144/2.30T → 2023-08 $188/2.94T; NVDA pre/post
  prices correct) — no current-data leak.
- Schema parity vs 2026-03: identical top-level + bar + quarter keys.
- Value screen: 15 qualifying names every month.

### Independent agent review (2026-06-24) — PASS, + one more fix

A single read-only review agent re-checked PIT correctness and wiring:
**(A) PIT correctness PASS** (verified `report_date>decision = 0` across all 493
tickers in a real 2023-01 bundle), **(B) wiring PASS** (source threads through;
daily job unchanged). It found a residual *quality* (not look-ahead) issue:
~9/493 tickers (AEP, BKNG, TFC, …) lacked recent quarterly net_income/revenue
under the standard us-gaap tags, so end-date ordering surfaced a years-old
quarter as "most recent".

**Fixes applied (then rebuilt):**
- **Staleness guard** — drop a ticker's fundamentals if its newest available
  quarter ends >270 days before `as_of` (coverage gap → excluded from the value
  screen rather than feeding decade-old data). AEP, BKNG now → 0 quarters.
- **Broadened bank/insurer revenue tags** (`RevenuesNetOfInterestExpense`,
  `InterestAndDividendIncomeOperating`) — recovered e.g. TFC to a recent quarter.
- Verified: AAPL→2022Q2, MSFT→2022Q4, JPM→2022Q3, TFC→2022Q3; AEP/BKNG dropped.

### Three-agent audit (2026-06-24) — PIT PASS, wiring PASS, data-quality fixed

Three independent read-only coding agents audited PIT, wiring, and data quality.
- **PIT/look-ahead: PASS** — 0 violations across 36 XBRL probe cases + the frozen
  bundles; `report_date<=as_of`, `_asof_shares` dual-gated, split direction correct.
- **Wiring/pipeline: PASS** — source threads through; chunk merge can't drop/dupe
  tickers; frozen 2026-01..05 will be skipped; 2026-06 will build at the end;
  schema parity holds; daily job unchanged. (Note: daily `build_us_snapshot`
  writes a *wrapped* metadata.json shape vs the batch's flat shape — cross-path
  reader caveat, not a backfill defect.)
- **Data quality: CONCERNS → fixed.** Two HIGH issues found and corrected:
  1. **Tag-priority bug** — `_FLOW_TAGS` was first-*present*-wins, so issuers that
     migrated XBRL tags (keeping the dead tag key, populated only to ~2019-20)
     locked onto the stale tag → 27% of `quarters[0]` lacked revenue and BKNG/AEP
     dropped to 0 quarters. **Fix:** merge facts across all candidate tags then
     PIT-pick per period, so the current tag's recent periods surface. Verified:
     NVDA/GOOGL/JPM/WFC/XOM/TSLA revenue recovered; BKNG, AEP now 4 quarters.
  2. **`_asof_shares` shares bug** — used a decade-old / wrong-class cover-page
     count (BRK-B market_cap ~2000× off). **Fix:** staleness guard + tag priority
     (outstanding before issued, so treasury stock no longer inflates the count).
     Verified market caps at 2023-12-29: AAPL 2994B, MSFT 2795B, NVDA 1223B,
     JPM 491.8B, XOM 396B, GOOGL 1752B, KO 255B, JNJ 377B, BAC 266B — all within
     tolerance of known values. (Dual-class BRK-B still drops — documented edge.)
  Remaining accepted limitations: XBRL field coverage (revenue ~) is lower than
  yfinance (no `ebitda`/`total_debt`); `fiscal_quarter` is calendar-derived (label
  only, values placed by real period end).

Unit tests now 10 (XBRL 7 + yfinance as-of 3), incl. regressions for tag-merge
recency and shares priority/staleness.

### FINAL rebuild (2026-06-24) — full finance-expert remediation

A finance-expert audit returned **NOT FIT as-is** and flagged construction biases
beyond the code mechanics. All producer-side items are now fixed and a single
definitive rebuild is running into a **staging tree** (`artifacts_staging/`) so the
live bundles the consumer reads stay up until the replacement is verified.

Fixes in this build:
- **Survivorship-free universe** (`us_universe.py`, `--universe-mode pit`):
  S&P 500 membership reconstructed per month by reverse-applying Wikipedia's
  change log (+ current NDX). Verified: 2023-01 excludes GEHC/KVUE/PLTR/DELL;
  2024-06 excludes PLTR/DELL; correct counts (2023-01 = 527 names).
- **Mega-cap / dual-class market_cap**: weighted-average diluted/basic shares
  fallback recovers META/FOX/NWS (META 2023-01 = $405B); outstanding-before-issued
  priority + staleness guard fixed JPM/BRK-class errors. (BRK-B still drops — A/B
  1500:1 not reconstructable from companyfacts; documented.)
- **EBITDA + FCF via YTD-difference decomposition** (`_standalone_flows`): cash-flow
  / D&A YTD cumulants are differenced into standalone quarters, which also yields
  the **accounting Q4 = annual − 9-month**. Coverage on a 15-name sample rose to
  FCF 13/15, EBITDA 9/15 (was 1 and 0), Q4 present 15/15, 0 PIT violations.
- **Fiscal-quarter labeling** uses the period-END year + XBRL fp (FY→Q4), fixing
  the comparative-refiling fy contamination that produced duplicate labels.
- **Tag-merge** (revenue/NI across migrated tags) and **as-of split-consistent
  market_cap** retained from earlier fixes.

Unit tests: **15** (XBRL 10 incl. YTD-diff/Q4/label + fiscal regression; yfinance
as-of 3; PIT universe 2). All pass.

Accepted residual limitations (documented, not blocking): BRK-B dual-class drop;
EBITDA coverage < 100% (some filers don't tag quarterly D&A — book/earnings yields
unaffected); NDX historical membership approximated by current list; FRED macro is
observation-dated, not ALFRED-vintage (flag if macro drives the trading rule).

### Re-audit (2026-06-24) — inline verification of the remediation

Re-ran the finance checks (dedicated agent was blocked by API 529s; verified
inline, same probes):
- **C1 coverage** (25-ticker sample): net_income 100%, total_equity 100%, FCF
  84-88%, EBITDA 48% (was 0%). Screen is now genuinely multi-factor
  (earnings/book/fcf + partial ebitda), no longer a degenerate P/B.
- **C2 TTM**: provider now returns 8 standalone quarters → TTM (sum of 4) is
  computable from the data (the value_screen_smoke ×4 is the *consumer's* replica;
  producer data supports TTM).
- **I1 mega-cap**: META recovered ($935B), FOXA/NWSA recovered; **BRK-B still
  drops** (A/B 1500:1, documented); **GOOG + GOOGL both present (~$1.75T each) =
  Alphabet double-counted** — left in deliberately (both classes are real index
  members / investable); documented as a known universe choice.
- **I2 Q4/labels/PIT**: accounting Q4 present (AAPL/NKE/COST/WMT), 0 PIT
  violations. Duplicate labels eliminated for the common case (MSFT/WMT) by
  calendar-quarter labeling. **Caveat:** for off-calendar fiscal filers whose two
  quarter-ends fall in one calendar quarter (e.g. AAPL Q3 ~Jul-1 and Q4 ~Sep-30
  both in calendar Q3), `fiscal_quarter` can repeat — it is descriptive metadata
  only; **consumers must order/select quarters by end date (quarters[0..3]), not
  by the label.** Values, ordering, and PIT are unaffected.
- **I3 splice**: all 42 months on one XBRL+PIT methodology (no splice).
- **I4 survivorship**: PIT membership differs by ~82 names between 2023-01 and
  2025-09; PLTR/DELL/KVUE/SMCI/VST correctly absent in 2023-01, present later.
  Residual: NDX uses today's list (small); S&P deletions re-added via the
  'Removed' column.
- **I5 FRED**: still observation-dated (not ALFRED vintage) — flagged; only
  matters if macro drives the trade rule.

Unit tests: 15 pass (label test updated to calendar-quarter semantics).

### Definitive remediation (2026-06-25) — resolve, don't just document

Rather than accept the residuals, the cleanly-resolvable ones were fixed:
- **A. Dual-class double-count → RESOLVED.** Universe now de-dupes by SEC CIK
  (`us_universe._dedup_by_cik`): GOOG/GOOGL, FOX/FOXA, NWS/NWSA share a CIK and
  collapse to one primary listing (shortest symbol). Verified: each appears once;
  2023-01 universe 527→524.
- **B. FRED macro look-ahead → RESOLVED.** `fred_provider._vintage_series` now
  pulls ALFRED real-time vintages (realtime_start=realtime_end=as_of), so only
  data actually *released* by the decision date is used. Verified: on 2023-02-01
  the latest CPI is Dec-2022 (Jan CPI not yet published); on 2023-02-20 it becomes
  Jan-2023. Release-lag + revision look-ahead eliminated.
- **C. Fiscal labels → RESOLVED (true fiscal, unique).** `_standalone_flows` now
  carries the NATIVE (earliest-filed/original) fy/fp for each period, immune to
  comparative-refiling contamination; `_fiscal_label` maps Q1/Q2/Q3 directly and
  FY→Q4. Verified: AAPL = 2024Q4/Q3/Q2/Q1/2023Q4/Q3 (Sept-end correctly Q4), no
  duplicates on MSFT/WMT/NKE, 0 PIT violations. Values still use latest-filed
  (restatements honoured).
- **E. EBITDA coverage → IMPROVED.** Added D&A tag variants
  (DepreciationAmortizationAndOther, etc.); EBITDA ~52-60% (some filers genuinely
  don't tag quarterly D&A — book/earnings/fcf carry the screen).

Remaining (genuinely uneconomic to fully resolve, documented & low-impact):
- **BRK-B market_cap**: Berkshire reports shares only via class-dimensioned XBRL
  members that the flattened companyfacts strips; recovering it needs the
  dimensioned API for one ticker. Accepted (BRK-B drops from the screen).
- **NDX historical membership**: uses today's NDX list (its change history is not
  cleanly tabulated). Impact small — NDX∩S&P is already PIT via the S&P
  reconstruction; NDX-only names are ~14 stable large-caps.

Unit tests now **17** (added CIK-dedup and native-label regressions). All pass.

Build: `--fundamentals-source edgar_xbrl --universe-mode pit`, 2023-01..2026-06
(42 months incl. a fresh full-universe 2026-06 at decision 2026-06-23) into
`artifacts_staging/`. ~40-65 min/month. On completion: PIT scan = 0, value-screen
smoke, coverage report, then swap staging→live and re-enable the daily job.

_This file is regenerated/appended as the backfill proceeds._
