"""KR market-wide short-selling ban regimes.

Korea imposed full short-selling bans in two windows during 2019-2026. A
long/short backtest must not assume executable shorts in these months, so each
KR monthly snapshot carries a ``metadata.short_sale_ban`` flag. Month ranges are
inclusive and compared as ``YYYY-MM`` strings (the granularity the consumer's
forensics gate reads).
"""

from __future__ import annotations

# Inclusive month windows (YYYY-MM) of a KR market-wide short-selling ban.
#   2020-03-16 .. 2021-05-02  — COVID emergency ban
#   2023-11-06 .. 2025-03-31  — full ban
KR_SHORT_BAN_WINDOWS: tuple[tuple[str, str], ...] = (
    ("2020-03", "2021-05"),
    ("2023-11", "2025-03"),
)


def is_kr_short_ban_month(month: str) -> bool:
    """Return True if ``month`` ("YYYY-MM") falls in a KR short-ban window."""
    return any(lo <= month <= hi for lo, hi in KR_SHORT_BAN_WINDOWS)
