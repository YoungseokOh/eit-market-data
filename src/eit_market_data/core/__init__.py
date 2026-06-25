"""Shared point-in-time helpers reused across providers.

These modules consolidate logic that was previously duplicated byte-for-byte
across the US and KR provider implementations (sector-average math, OHLCV
frame -> PriceBar conversion, point-in-time visibility checks, weekday-only
business-day helpers, and content hashing). Extraction is behaviour-preserving:
every helper reproduces the exact rounding, look-ahead filtering, and formula
semantics of the call sites it replaces.
"""

from __future__ import annotations
