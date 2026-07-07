# KR top-500 covid_2020_spike FAIL — producer analysis for consumer review

**Status:** the KR top-500 (KOSPI+KOSDAQ ADV-ranked, hysteresis-banded 450/1000)
build is complete (90 months, 2019-01..2026-06), short-sale-ban stamped, and
survivorship-verified (0 violations — see `docs/kr_top500_delisted_manifest.md`).

The consumer forensics gate (`scripts/backfill_forensics.py --market kr
--universe-type broad`) reports:

| check | status | value |
|---|---|---|
| exit_rate | WARN | 47.84%/yr (accepted per prior agreement — [[kr_top500_universe_banding]]) |
| **covid_2020_spike** | **FAIL** | 2020 exits 212 vs surrounding baseline 247.5 |
| ipo_spac_cohort | PASS | 416 vs 131.4 |
| kr_short_ban | PASS | flags present for both windows |
| adjustment_continuity | PASS | 478 names, 1 seam checked |

This note documents why the FAIL is a byproduct of the same root cause as the
already-accepted `exit_rate` WARN — ADV-rank liquidity churn, not a
survivorship gap — so the consumer can decide how to treat it. **The producer
did NOT modify `backfill_forensics.py`.**

## Root cause: 93% of counted "exits" are liquidity churn, not delistings

The manifest (`docs/kr_top500_delisted_manifest.json`, 1,252 total universe
exits across 90 months) breaks down as:

| category | count | % |
|---|---|---|
| dropped out of top-500/1000 band, still listed elsewhere | 1,162 | 92.8% |
| delisted (real exit from the exchange) | 90 | 7.2% |
| — of which failure-to-zero (collapse captured) | 16 | 1.3% |

The `covid_2020_spike` check counts **all** universe exits (any name leaving
the membership list that month), not just real delistings. In a large-cap
index (S&P 500 / KOSPI200) membership exits are almost entirely genuine
removals, so the check is a reasonable delisting-spike proxy. In an
**ADV-ranked broad universe with hysteresis banding**, the overwhelming
majority of exits are names whose 20-day dollar volume rank drifted outside
the band — normal liquidity noise — which swamps whatever signal a real
delisting spike would produce.

## The 2020 exit counts, decomposed

**All universe exits (churn + real), by year:**

| year | exits |
|---|---|
| 2019 | 188 |
| **2020** | **212** |
| 2021 | 307 |
| 2022 | 243 |
| 2023 | 279 |

2020 is not below 2019, but it is well below several later years — because
liquidity churn (not COVID) dominates the trend, and churn grew over the
window as more small/mid names cycled through the ADV band.

**Real delistings only (`classification == "delisted"`), by year:**

| year | real delistings |
|---|---|
| 2019 | 18 |
| **2020** | **17** |
| 2021 | 11 |
| 2022 | 14 |
| 2023 | 21 |

Real KR delistings in 2020 are flat versus 2019 (17 vs 18), not elevated. This
is consistent with the earlier US large-cap finding
(`docs/us_exit_rate_calibration.md`): the crash hit prices immediately, but
KRX delisting review is a multi-quarter administrative process (audit
opinion, capital impairment review, improvement period) — forced delistings
for COVID-distressed issuers show up with a lag, visible in 2023's elevated
count (21), not concentrated in 2020.

**2020 monthly exit detail (all exits) — no crash-month spike, if anything a dip:**

| month | exits |
|---|---|
| 2020-01 | 22 |
| 2020-02 | 18 |
| 2020-03 | 14 |
| 2020-04 | 11 |
| 2020-05 | 9 |
| 2020-06 | 16 |
| 2020-07 | 21 |
| 2020-08 | 29 |
| 2020-09 | 20 |

The acute crash months (Mar-May 2020) show the **lowest** exit counts in the
year. A plausible mechanism: the crash drove a market-wide volume surge (panic
trading), which temporarily lifted smaller names' ADV rank and reduced churn
at the band boundary — the opposite of what a "COVID delisting spike" would
predict, and consistent with churn-driven noise rather than survivorship
cleaning (a truly cleaned dataset would show near-zero exits everywhere, not
a dip specifically in the crash months).

## Why this is not a survivorship defect

- 90 real delistings are captured and exit at their genuine month (verified
  0 violations against the corrected manifest).
- 16 failure-to-zero collapse paths are captured with real pykrx-retained
  post-halt OHLCV (constant last-print series), not silently dropped.
- The FAIL is explained entirely by a churn-dominated denominator, which is
  the same mechanism already documented and accepted for `exit_rate`.

## Options for the consumer

1. **Recalibrate `covid_2020_spike` for ADV-ranked/broad universes** — e.g.
   compute the spike check on real-delistings-only counts (available in the
   delisting manifest) rather than raw universe-exit counts, analogous to how
   `exit_rate`'s floor was made universe-type-aware.
2. **Treat as advisory for broad/ADV-ranked books** — keep the check but do
   not gate KR top-500 on it, since the survivorship signal it proxies is
   disproven above (same disposition already taken for KR top-500's
   `exit_rate` WARN).

The producer is ready to implement (1) in the consumer's forensics with
tests, on the consumer's decision, exactly as was done for `adjustment_continuity`
and the US `exit_rate` recalibration.
