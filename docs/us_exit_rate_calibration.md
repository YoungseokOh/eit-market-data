# US exit_rate / covid_2020_spike — producer analysis for consumer review

**Status:** the 2019–2022 US backfill is complete (90 months, 2019-01..2026-06).
The consumer forensics gate (`scripts/backfill_forensics.py --market us`) reports
2 FAILs. This note documents evidence that they are **not** a survivorship defect
in the producer data, so the consumer can decide how to treat them (recalibrate
the check for a large-cap universe, or accept them as advisory). **The producer
did NOT change these two checks** — only `adjustment_continuity` was fixed
earlier (a genuine endpoint-vs-shared-date bug, separately documented).

## Forensics result (US, 90 months)

| check | status | value |
|---|---|---|
| exit_rate | FAIL | 3.58%/yr (floor 4%) |
| covid_2020_spike | FAIL | 2020 exits 17 vs surrounding baseline 19 |
| ipo_spac_cohort | PASS | 30 vs 12.6 |
| adjustment_continuity | WARN | 2 / 500 names (below FAIL threshold) |
| kr_short_ban | PASS | N/A for US |

## Evidence the data is not survivorship-cleaned

**1. Real removals are captured, ~17–19/yr, flat across the window.**

| year | universe exits (this data) |
|---|---|
| 2019 | 19 |
| 2020 | 17 |
| 2021 | 19 |
| 2022 | 17 |

**2. The 2020 exits are all genuine S&P 500 / NDX removals** (acquisition,
merger, or index removal), not silent drops:
WCG (Centene acq.), RTN (UTC merger), AGN (AbbVie acq.), ETFC (Morgan Stanley
acq.), NBL (Chevron acq.), M / JWN / KSS (Macy's, Nordstrom, Kohl's removed),
XEC, ARNC, CPRI, HP, ADS, HOG, COTY, HRB, AIV.

**3. Removed names are retained in history.** The PIT S&P 500 ∪ NDX union over
2019–2022 is **600 names vs 514 today** — 132 names that later left the index are
still present in the months they were members, exiting at their real dates. A
survivorship-cleaned history would show today's ~514 names retro-applied and a
near-zero exit rate; this data does not.

## Why the two checks nonetheless FAIL

- **exit_rate (3.58% < 4% floor):** the universe is **large-cap** (S&P 500 ∪
  NDX, ~500–600 names). Large-cap index turnover is genuinely ~17–19 removals/yr
  ≈ 3.4–3.8%/yr — structurally below the 4–6% base-rate band, which is calibrated
  for broader / smaller-cap universes with more delistings. For comparison the KR
  KOSPI200 book passes at 6.87% because it has semiannual rebalances.
- **covid_2020_spike (17 vs 19):** COVID crashed *prices*, but the S&P 500 / NDX
  committees did **not** mass-remove large caps in 2020 — big companies did not
  delist. The delisting/bankruptcy cohort of 2020 was concentrated in small-caps,
  energy, and SPAC liquidations, which are outside a large-cap index. So the
  absence of a 2020 *large-cap* exit spike is a true market fact, not a data gap.
  (KR's 2020 book *does* spike, 22 vs 11.5, and passes.)

## Options for the consumer

1. **Recalibrate for universe type** — apply a large-cap band (e.g. floor ~3%)
   or make `covid_2020_spike` large-cap-aware; the evidence above justifies it.
2. **Treat both as advisory for large-cap books** — keep the checks but do not
   gate US on them, since the survivorship signal they proxy is disproven here.

The producer is ready to implement (1) in the consumer's forensics with tests,
on the consumer's decision, exactly as was done for `adjustment_continuity`.
