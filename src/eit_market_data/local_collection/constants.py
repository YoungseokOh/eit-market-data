"""Shared constants for the local-collection package."""

from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_US_UNIVERSE = "AAPL,MSFT,GOOGL,AMZN,NVDA"
CURRENT_KR_UNIVERSE_CSV = PROJECT_ROOT / "universes" / "kr_universe.csv"
NEWS_LOOKBACK_DAYS = 30
KOSPI200_INDEX_CODE = "1028"
# KOSPI200 reconstitutes on a fixed semi-annual cycle (effective the trading day
# after the second Thursday of June and December). Any month outside this set is
# "off-cycle" and should see at most a couple of names change (delistings/M&A).
KOSPI200_REVIEW_MONTHS = frozenset({6, 12})
# Maximum month-over-month membership churn (max of entrants vs leavers) we trust
# from the raw pykrx deposit-file list in an off-cycle month. A spurious transient
# list (or a today()-stamped current-membership fallback applied to a historical
# month) shows up as a large round-trip; legitimate off-cycle changes do not.
KOSPI200_OFFCYCLE_CHURN_THRESHOLD = 10
# The pykrx KOSPI200 deposit-file endpoint occasionally returns 201-202 distinct
# common-stock constituents on certain historical dates (a known quirk around
# constituent events; no duplicates or preferred shares involved). Treat any list
# within this tolerance of the nominal 200 as a valid point-in-time membership
# rather than fabricating a trim to exactly 200. Genuinely broken responses (far
# from 200) still fail the size gate and fall back to carry-forward.
KOSPI200_SIZE_TOLERANCE = 3
