"""Shared plumbing for ``scripts/`` entrypoints.

This module centralizes the boilerplate that was copy-pasted across the data
collection / snapshot scripts:

* :func:`bootstrap` — resolve ``PROJECT_ROOT``, put ``src`` on ``sys.path`` and
  load ``.env``. Returns ``PROJECT_ROOT`` so callers keep their module-level
  ``PROJECT_ROOT`` global (referenced throughout the scripts and tests).
* :func:`load_tickers` — read a universe CSV into a normalized ticker list,
  wrapping the canonical KR normalizer and the US ``upper``/``.``→``-`` rule.
* :func:`parse_month` / :func:`month_range` / :func:`next_month` — YYYY-MM
  helpers.
* :func:`write_json` — ``json.dumps(payload, indent=2, sort_keys=True)`` to a
  path (the byte-exact serialization every bundle file uses).

All helpers are intentionally behavior-preserving: they reproduce the exact
normalization / serialization the scripts performed inline before.
"""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path


def bootstrap(*, load_env: bool = True) -> Path:
    """Resolve project root, add ``src`` to ``sys.path`` and load ``.env``.

    Mirrors the boilerplate that every script ran at import time:

    * ``PROJECT_ROOT = <repo root>``
    * ``sys.path.insert(0, str(PROJECT_ROOT / "src"))`` (idempotent)
    * ``load_dotenv(PROJECT_ROOT / ".env")``

    Returns ``PROJECT_ROOT`` so callers can keep their module-level global.

    NOTE: callers that need a pre-import side effect (e.g. ``build_us_batch``'s
    ``socket.setdefaulttimeout(90)`` which must run before importing
    ``eit_market_data``) must perform it themselves *before* importing the
    package — this helper only handles path + dotenv setup.
    """
    # parents[1] == repo root (this file lives in <root>/scripts/).
    project_root = Path(__file__).resolve().parents[1]
    src_root = project_root / "src"
    if str(src_root) not in sys.path:
        sys.path.insert(0, str(src_root))
    if load_env:
        from dotenv import load_dotenv

        load_dotenv(project_root / ".env")
    return project_root


def _normalize_kr(ticker: str) -> str:
    from eit_market_data.kr.market_helpers import normalize_ticker

    return normalize_ticker(ticker)


def _normalize_us(ticker: str) -> str:
    return ticker.strip().upper().replace(".", "-")


def load_tickers(path: Path, market: str) -> list[str]:
    """Load a universe CSV ``ticker`` column into a normalized ticker list.

    * ``market="kr"`` uses the canonical
      :func:`eit_market_data.kr.market_helpers.normalize_ticker` (6-digit zfill
      while preserving alphanumeric KRX codes).
    * ``market="us"`` upper-cases and maps ``.`` to ``-``.

    Rows with a blank ``ticker`` are skipped. Order is preserved (no dedupe).
    """
    market_key = market.strip().lower()
    if market_key == "kr":
        normalize = _normalize_kr
    elif market_key == "us":
        normalize = _normalize_us
    else:
        raise ValueError(f"unknown market: {market!r}")

    tickers: list[str] = []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            raw = str(row.get("ticker", "")).strip()
            if not raw:
                continue
            tickers.append(normalize(raw))
    return tickers


def parse_month(value: str) -> tuple[int, int]:
    """Parse a ``YYYY-MM`` string into a ``(year, month)`` tuple."""
    year, month = value.split("-")
    return int(year), int(month)


def month_range(start: str, end: str) -> list[str]:
    """Generate ``YYYY-MM`` strings from *start* to *end* inclusive.

    Returns an empty list when *end* precedes *start*.
    """
    months: list[str] = []
    y, m = parse_month(start)
    ey, em = parse_month(end)
    while (y, m) <= (ey, em):
        months.append(f"{y:04d}-{m:02d}")
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1
    return months


def next_month(month: str) -> str:
    """Return the ``YYYY-MM`` month immediately following *month*."""
    y, m = parse_month(month)
    return f"{y + 1:04d}-01" if m == 12 else f"{y:04d}-{m + 1:02d}"


def write_json(path: Path, payload: object) -> None:
    """Write *payload* as ``json.dumps(indent=2, sort_keys=True)`` UTF-8 text."""
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
