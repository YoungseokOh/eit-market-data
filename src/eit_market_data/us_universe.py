"""Point-in-time US index membership (survivorship-bias-free universe).

Reusing *today's* S&P 500 / Nasdaq-100 membership for historical months injects
survivorship bias (deleted names vanish; not-yet-added names appear). This module
reconstructs the S&P 500 constituents AS OF a past date by taking Wikipedia's
current list and reverse-applying its published change log:

    for every change with effective_date > as_of:
        undo it  ->  re-add the 'Removed' ticker, drop the 'Added' ticker

The result is the membership that was actually in effect on ``as_of``. Nasdaq-100
historical membership is not as cleanly tabulated; today's NDX list is unioned in
as a documented approximation (its survivorship effect is small — NDX names are
large-caps that rarely leave). Results are cached per process.
"""

from __future__ import annotations

import logging
import ssl
import urllib.request
from datetime import date
from io import StringIO
from typing import Any

logger = logging.getLogger(__name__)

_SP500_WIKI = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
_SP400_WIKI = "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies"
_NDX_WIKI = "https://en.wikipedia.org/wiki/Nasdaq-100"

_CACHE: dict[str, Any] = {}


def _norm(ticker: str) -> str:
    return str(ticker).strip().upper().replace(".", "-")


def _wiki_tables(url: str) -> list[Any]:
    import pandas as pd

    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    ctx = ssl.create_default_context()
    html = urllib.request.urlopen(req, timeout=30, context=ctx).read().decode("utf-8", "ignore")
    return pd.read_html(StringIO(html))


def _load_sp500() -> tuple[set[str], list[dict]]:
    """Return (current S&P 500 tickers, change-log rows newest-first)."""
    if "sp500" in _CACHE:
        return _CACHE["sp500"]
    import pandas as pd

    tables = _wiki_tables(_SP500_WIKI)
    current = {
        _norm(t)
        for t in tables[0]["Symbol"].tolist()
        if str(t).strip()
    }
    changes: list[dict] = []
    chg = tables[1]
    # Flatten the MultiIndex columns ("Effective Date","Added"/"Removed","Reason").
    for _, row in chg.iterrows():
        vals = list(row.values)
        # columns: [eff_date, added_ticker, added_name, removed_ticker, removed_name, reason]
        eff_raw, added, _an, removed, _rn = vals[0], vals[1], vals[2], vals[3], vals[4]
        eff = pd.to_datetime(eff_raw, errors="coerce")
        if pd.isna(eff):
            continue
        changes.append(
            {
                "date": eff.date(),
                "added": _norm(added) if str(added).strip() and str(added) != "nan" else None,
                "removed": _norm(removed) if str(removed).strip() and str(removed) != "nan" else None,
            }
        )
    changes.sort(key=lambda c: c["date"], reverse=True)
    _CACHE["sp500"] = (current, changes)
    return current, changes


def pit_sp500(as_of: date) -> set[str]:
    """S&P 500 membership as of ``as_of`` (reverse-applied change log)."""
    current, changes = _load_sp500()
    members = set(current)
    for c in changes:
        if c["date"] <= as_of:
            break  # changes are newest-first; the rest are already <= as_of
        # Undo a change effective AFTER as_of.
        if c["added"]:
            members.discard(c["added"])  # it wasn't a member yet
        if c["removed"]:
            members.add(c["removed"])  # it was still a member
    return members


def _load_sp400() -> tuple[set[str], list[dict]]:
    """Return (current S&P MidCap 400 tickers, change-log rows newest-first).

    The S&P 400 Wikipedia page mirrors the S&P 500 page's structure: table[0] is
    the current constituents (``Symbol`` column) and table[1] is the dated change
    log. The change log uses a two-row (MultiIndex) header
    ``[Date, (Added, Ticker), (Added, Security), (Removed, Ticker),
    (Removed, Security), Reason]``, but the positional column order matches the
    S&P 500 log, so the same positional parse applies. Dense back to 2012, so
    reverse-application to 2019-01 is well covered.
    """
    if "sp400" in _CACHE:
        return _CACHE["sp400"]
    import pandas as pd

    tables = _wiki_tables(_SP400_WIKI)
    current = {
        _norm(t)
        for t in tables[0]["Symbol"].tolist()
        if str(t).strip()
    }
    changes: list[dict] = []
    chg = tables[1]
    for _, row in chg.iterrows():
        vals = list(row.values)
        # positional: [eff_date, added_ticker, added_name, removed_ticker, removed_name, reason]
        eff_raw, added, _an, removed, _rn = vals[0], vals[1], vals[2], vals[3], vals[4]
        eff = pd.to_datetime(eff_raw, errors="coerce")
        if pd.isna(eff):
            continue
        changes.append(
            {
                "date": eff.date(),
                "added": _norm(added) if str(added).strip() and str(added) != "nan" else None,
                "removed": _norm(removed) if str(removed).strip() and str(removed) != "nan" else None,
            }
        )
    changes.sort(key=lambda c: c["date"], reverse=True)
    _CACHE["sp400"] = (current, changes)
    return current, changes


def pit_sp400(as_of: date) -> set[str]:
    """S&P MidCap 400 membership as of ``as_of`` (reverse-applied change log)."""
    current, changes = _load_sp400()
    members = set(current)
    for c in changes:
        if c["date"] <= as_of:
            break  # changes are newest-first; the rest are already <= as_of
        if c["added"]:
            members.discard(c["added"])  # it wasn't a member yet
        if c["removed"]:
            members.add(c["removed"])  # it was still a member
    return members


def _load_ndx_current() -> set[str]:
    if "ndx" in _CACHE:
        return _CACHE["ndx"]
    import pandas as pd

    out: set[str] = set()
    try:
        for df in _wiki_tables(_NDX_WIKI):
            cols = [str(c).lower() for c in df.columns]
            if "ticker" in cols and len(df) >= 90:
                col = next(c for c in df.columns if str(c).lower() == "ticker")
                out = {_norm(t) for t in df[col].tolist() if str(t).strip()}
                break
    except Exception as exc:  # noqa: BLE001
        logger.warning("NDX membership fetch failed: %s", exc)
    _CACHE["ndx"] = out
    return out


def _ticker_cik_map() -> dict[str, str]:
    """Map ticker -> CIK from SEC's company_tickers.json (cached)."""
    if "cikmap" in _CACHE:
        return _CACHE["cikmap"]
    import json as _json
    import os

    ua = os.environ.get("SEC_EDGAR_USER_AGENT", "eit-market-data contact@example.com")
    req = urllib.request.Request(
        "https://www.sec.gov/files/company_tickers.json",
        headers={"User-Agent": ua, "Accept-Encoding": "gzip, deflate"},
    )
    out: dict[str, str] = {}
    try:
        import gzip

        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
            if resp.headers.get("Content-Encoding") == "gzip":
                raw = gzip.decompress(raw)
        data = _json.loads(raw)
        for entry in data.values():
            t = _norm(entry.get("ticker", ""))
            cik = str(entry.get("cik_str", ""))
            if t and cik:
                out[t] = cik.zfill(10)
    except Exception as exc:  # noqa: BLE001
        logger.warning("CIK map fetch failed (dedup disabled this run): %s", exc)
    _CACHE["cikmap"] = out
    return out


def _dedup_by_cik(tickers: set[str]) -> list[str]:
    """Collapse share-class duplicates that share a CIK to one primary ticker.

    GOOG/GOOGL, FOX/FOXA, NWS/NWSA, etc. are the same issuer (same CIK) and would
    otherwise double-count a company in the value screen. Among tickers sharing a
    CIK we keep one deterministically (shortest symbol, then alphabetical). A
    ticker with no CIK mapping is always kept.
    """
    cik_map = _ticker_cik_map()
    by_cik: dict[str, list[str]] = {}
    keep: set[str] = set()
    for t in tickers:
        cik = cik_map.get(t)
        if cik is None:
            keep.add(t)  # unknown mapping: never drop
        else:
            by_cik.setdefault(cik, []).append(t)
    for _cik, group in by_cik.items():
        keep.add(min(group, key=lambda s: (len(s), s)))
    return sorted(keep)


def _has_pit_listing(ticker: str, as_of: date, listed_asof: Any) -> bool:
    """True if ``ticker`` had price history on/before ``as_of``.

    ``listed_asof`` is a callable ``(ticker, as_of) -> bool`` (or truthy/None) the
    caller injects — typically wrapping the price provider's as-of close lookup.
    Any error is treated as "listing unknown" and the name is KEPT (we never drop
    on a transient data hiccup). Only an explicit, successful "no data" drops it.
    """
    try:
        result = listed_asof(ticker, as_of)
    except Exception as exc:  # noqa: BLE001
        logger.debug("listing check failed for %s, keeping: %s", ticker, exc)
        return True
    return bool(result)


def pit_universe(
    as_of: date,
    *,
    include_ndx: bool = True,
    include_sp400: bool = False,
    dedup: bool = True,
    drop_unlisted: Any = None,
) -> list[str]:
    """Survivorship-aware US universe as of ``as_of``.

    S&P 500 is reconstructed point-in-time. The Nasdaq-100 *current* list is
    unioned in as a documented approximation — NDX historical membership isn't
    cleanly tabulated. That union injects **phantom** names into early months:
    issuers added to NDX only later (ARM, GEHC, ...) that had not yet listed as of
    an early-2023 decision date. They carry no price/fundamentals so they're inert
    in the value screen, but they inflate the raw universe count.

    ``drop_unlisted`` removes those phantoms point-in-time. Pass a callable
    ``(ticker, as_of) -> bool`` (truthy = the ticker had price history on/before
    ``as_of``); any NDX-only name (i.e. not an S&P 500 PIT member) that the
    callable reports as having no as-of listing is dropped. S&P 500 PIT members
    are never dropped (their membership is already point-in-time). When
    ``drop_unlisted`` is None the NDX union is kept verbatim (prior behavior).

    When ``include_sp400`` is set, the S&P MidCap 400 membership as of ``as_of``
    (reconstructed point-in-time from its own Wikipedia change log) is unioned in
    as well, expanding the large-cap universe with mid-caps. Unlike the NDX union
    it is fully point-in-time, so it is added verbatim (no ``drop_unlisted``
    phantom filtering needed).

    Same-CIK share-class duplicates are collapsed so a company is not
    double-counted. Returns sorted tickers.

    RESIDUAL: with ``drop_unlisted=None`` the early-month NDX phantom names remain
    (inert but counted). Even with the hook, a name that *had* listed before ``as_of``
    but joined NDX only later is still unioned in — full NDX point-in-time
    membership would require a tabulated NDX change log, which isn't available here.
    """
    sp500 = pit_sp500(as_of)
    members = set(sp500)
    if include_ndx:
        ndx_only = _load_ndx_current() - sp500
        if drop_unlisted is not None:
            ndx_only = {
                t for t in ndx_only if _has_pit_listing(t, as_of, drop_unlisted)
            }
        members |= ndx_only
    if include_sp400:
        # S&P MidCap 400 is reconstructed point-in-time (dense Wikipedia change
        # log), so unlike the NDX union it needs no ``drop_unlisted`` phantom
        # guard — its members were actual constituents on ``as_of``.
        members |= pit_sp400(as_of)
    if dedup:
        return _dedup_by_cik(members)
    return sorted(members)
