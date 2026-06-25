"""Point-in-time visibility helper.

A date is *visible* at ``as_of`` only when it exists and is on or before
``as_of``. This is the bare ``d is not None and d <= as_of`` guard duplicated
across the filing/quarter point-in-time checks.
"""

from __future__ import annotations

from datetime import date


def is_visible(d: date | None, as_of: date) -> bool:
    """Return True iff ``d`` is known and not after ``as_of``."""
    return d is not None and d <= as_of
