# KR top-500 universe — hysteresis banding (turnover control)

Records the point-in-time membership rule for `universes/kr/top500/<YYYY-MM>.csv`
and the turnover-vs-liquidity trade-off behind the chosen band. For the consumer's
review (turnover-sensitive multifactor pipeline).

## Problem

A plain "top-500 by trailing ADV" rule over KOSPI+KOSDAQ churns violently:
**~17%/month leavers (≈206%/yr)**. The churn is *proven-real ADV rotation*, not a
ranking bug — the leavers are genuinely ranked past the top-500 the next month
(27–54 of 500 members/month cross the boundary), because KOSPI+KOSDAQ small/mid-cap
**trailing-ADV rank is intrinsically volatile**. It is not fixable without either
(a) accepting the churn, or (b) retaining names far outside the liquid top-500.

Note: exits are almost never *rank-absent* — pykrx retains delisted/thin OHLCV
(the KR data advantage), so a delisting shows up as ADV rank *drift*, and its
terminal collapse path is captured in `docs/kr_top500_delisted_manifest.json`.

## Rule: sequential carry-forward hysteresis banding

Implemented in `scripts/build_kr_top500_universe.py` (`_compute_phase`):

- Each month, rank **all** then-listed KOSPI+KOSDAQ ADV-rankable eligibles by
  trailing 60-business-day ADV (unchanged signal).
- **Seed month (2019-01):** plain top-`target` (500) by ADV.
- **Thereafter (carry-forward):**
  - **RETAIN** a current member while its ADV rank ≤ `band_upper`.
  - **ADD** a non-member only if its ADV rank ≤ `band_lower`.
  - Target exactly `target`: keep retained (rank ≤ upper); if that exceeds
    `target`, keep the best-ranked `target`; else fill remaining slots from
    best-ranked non-members with rank ≤ `band_lower`.
- **Survivorship-free:** a delisted name is absent from the live listed set at
  build time, so it cannot be ranked or selected — it exits naturally at its real
  month. Real exits are preserved.
- Parameters: `--band-lower` (default 450), `--band-upper` (default 1000),
  `--top-n` (default 500).

## Band sweep (measured on the actual 2019-01..2026-06 data, 90 months)

| lower / upper | turnover | member rank p50 | p90 | % members outside top-500 | outside top-1000 |
|---|---|---|---|---|---|
| plain top-500 (no band) | **~206%/yr** | — | — | 0% | 0% |
| 450 / 600 | 135%/yr | 250 | 479 | 7.8% | 0% |
| **450 / 1000 (CHOSEN)** | **~48%/yr** | 282 | 760 | 26.8% | **3.2%** |
| 450 / 1500 | 16%/yr | 373 | 1046 | 38.9% | 11.6% |
| 450 / 2000 | 6.3%/yr | 469 | 1393 | 47.7% | 21.8% |

The two naive requirements — a tight 450/600 band **and** a 3-6%/yr turnover
target — are **mutually exclusive for this universe**: hitting 3-6%/yr needs
`upper≈2000`, at which point ~48% of the universe is outside the liquid top-500
(untradeable names the strategy can't actually hold), which harms the backtest
more than turnover does.

## Chosen band: `lower=450, upper=1000`

Rationale (owner-approved): the balance point. It cuts turnover **~5x**
(206% → ~48%/yr — the reduction that matters for the consumer's turnover-sensitive
pipeline) while keeping members **within ~top-1000 by ADV** (only ~3% beyond
rank-1000, p90 rank 760) — i.e. still tradeable. `upper=2000` would meet the
turnover target numerically but at an unacceptable liquidity cost; `upper=600`
barely improves on the naive rule.

## Acceptance note (for the consumer)

The residual **`exit_rate` WARN is accepted by design.** The remaining ~48%/yr
turnover is *real small-cap ADV rotation*, and both the owner and the consumer's
own request treat high small-cap exit as a genuine feature of the KR broad
universe. The acceptance bar for the broad universe is **exit-0 / no-FAIL, NOT
no-WARN**. Delistings remain fully captured (survivorship-free) in
`docs/kr_top500_delisted_manifest.json`, including failure-to-zero collapse paths.
