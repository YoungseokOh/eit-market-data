# KR 2019-2022 KOSPI200 backfill — coverage report

Extends the KR monthly snapshot bundles back from 2023-01 to cover
**2019-01 .. 2022-12 (48 months)**, using the same proven offline method as the
2023-2025 backfill. Built with `scripts/archive/build_kr_backfill_2019.py` (holiday-aware
XKRX decision date, PIT KOSPI200 membership, DART served `cache_only`).

## Method (no broad live DART — see `.claude/rules/dart-api-limits.md`)

- **Fundamentals**: re-derived OFFLINE from the raw `finstate:` disk cache
  (years 2015-2022) and re-bucketed to an early anchor
  `fundamental:<ticker>:201901` via `scripts/archive/rebucket_dart_fundamentals_2019.py`.
  `CacheOnlyDartProvider._lookup` serves the latest bucket `<= as_of` month, so a
  2019-2022 `as_of` now resolves the 201901 anchor; the per-record PIT guard
  (`report_date <= as_of`) does the real filtering. 208 tickers anchored, span
  report_date 2016-03 .. 2022-12. **Zero live OpenDART DATA calls.** Values are
  raw KRW (원), consistent with `market_cap` and the US bundle.
- **Membership**: point-in-time KOSPI200 from pykrx
  `get_index_portfolio_deposit_file("1028", as_of)`, persisted per month under
  `universes/kr/kospi200/<YYYY-MM>.csv`. Includes names later removed
  (survivorship-correct). Jun/Dec semiannual rebalances show real churn
  (2020-06 +11/-11, 2020-12 +10/-10, 2021-06 +6/-8, 2021-12 +6/-7, 2022-06 +7/-7).
- **Decision date**: holiday-aware last KRX trading day
  (`core.calendar._last_business_day(y, m, "XKRX")`), never a calendar month-end;
  `market_cap` is non-zero in all 48 months (e.g. 2022-12-29, not 12-31).
- **Prices / benchmark / sector / market_cap**: pykrx (same provider as the
  existing bundles → identical split/dividend adjustment basis).

### KOSPI200 size tolerance

The pykrx deposit-file endpoint returns 201-202 distinct common-stock
constituents on nine historical dates (2019-01..03, 2021-01..05, 2021-11) — a
known quirk (no duplicates, no preferred shares). `_build_kospi200_records` now
accepts `200 ± 3` as a valid PIT membership instead of hard-requiring exactly
200, so those months carry their true membership rather than a fabricated trim.
Genuinely broken responses still fail and fall back to carry-forward.

## Coverage

- 48/48 months built, 0 PIT look-ahead violations (`scan_kr_lookahead.py`), 0 on
  the 90-bundle total.
- Universe/prices/market_cap: 200-202 per month, market_cap non-zero everywhere
  (one month, 2021-02, has market_cap for 194/201 names).
- Fundamentals coverage grows 135 (2019) → 168 (2022) of ~200; union of the
  2019-2022 KOSPI200 is 253 tickers, of which **172 are ever funded from cache
  and 81 are never funded** (names delisted/removed before 2023 with no cached
  `finstate`, plus 32 cached tickers with no decomposable history). This is a
  known offline gap; closing it would require broad live DART, which is out of
  scope per the rate-limit rule. Forensics does not read fundamentals.
- Filings are sparse before 2022 (1 in 2019, ~5 in 2020, ~28 in 2021, 176-188
  from 2022-03), because the `filing:` cache mostly holds FY2021+ 사업보고서.
  Filling FY2018-FY2020 needs controlled live-DART filing-only passes (~600
  document fetches); not done here (optional-mode; forensics does not read
  filings).

## daily_prices store

Extended from its 2021-11-15 floor back to **2019-01-02** via offline bundle
seeding (`build_daily_price_store.py --market kr --start 2019-01-01
--source bundle`). Existing rows are preserved verbatim (incremental merge);
only earlier dates are prepended. The new 2019-2022 bars come from the same
pykrx bundles → identical adjustment basis (verified: 0/280 overlapping-date
disagreements between the new 2022-12 bundle and the frozen 2023-01 bundle).

## Forensics gate (`backfill_forensics.py --market kr`)

| check | status |
|-------|--------|
| exit_rate | PASS (6.87%) |
| covid_2020_spike | PASS (2020 exits 22 > baseline 11.5) |
| ipo_spac_cohort | PASS (38 >= 9.4) |
| kr_short_ban | PASS |
| adjustment_continuity | FAIL (107/199) — **check artifact, not a data defect** |

**adjustment_continuity is a false positive.** The check compares the *last* bar
of the 2022-12 bundle (2022-12-29) to the *first* bar of the 2023-01 bundle,
which is 2021-11-15 for 199/200 names (bundles carry ~300 trailing bars). It
therefore measures a ~14-month bear-market price move, not a split-adjustment
break. Proof of true continuity: the 2022-12 and 2023-01 bundles agree on **all
280 overlapping dates (0 disagreements)**, and none of the 107 flagged tickers
disagrees on any overlapping date. The correct fix is consumer-side (compare
closes on a shared/adjacent date rather than window endpoints); it is not
resolvable producer-side without fabricating 2022-12-29 prices or rewriting the
frozen 2023-01 bundle.
