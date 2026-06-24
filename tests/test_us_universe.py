from __future__ import annotations

from datetime import date

import eit_market_data.us_universe as uu


def test_pit_sp500_reverse_applies_change_log(monkeypatch) -> None:
    """Membership as of a past date undoes changes effective after it."""
    current = {"AAPL", "MSFT", "NEWCO"}  # NEWCO added recently; OLDCO removed recently
    changes = [
        # newest first
        {"date": date(2024, 9, 20), "added": "NEWCO", "removed": "OLDCO"},
        {"date": date(2023, 3, 15), "added": "MSFT", "removed": "GONE"},
    ]
    monkeypatch.setitem(uu._CACHE, "sp500", (current, changes))

    # Before both changes: NEWCO absent, OLDCO present; MSFT absent, GONE present.
    m_2023_01 = uu.pit_sp500(date(2023, 1, 31))
    assert "NEWCO" not in m_2023_01 and "OLDCO" in m_2023_01
    assert "MSFT" not in m_2023_01 and "GONE" in m_2023_01
    assert "AAPL" in m_2023_01

    # Between the two changes: MSFT now in, GONE out; NEWCO still out, OLDCO still in.
    m_2024_06 = uu.pit_sp500(date(2024, 6, 28))
    assert "MSFT" in m_2024_06 and "GONE" not in m_2024_06
    assert "NEWCO" not in m_2024_06 and "OLDCO" in m_2024_06

    # After both: equals current.
    m_2025 = uu.pit_sp500(date(2025, 1, 31))
    assert m_2025 == current


def test_pit_universe_unions_ndx(monkeypatch) -> None:
    monkeypatch.setitem(uu._CACHE, "sp500", ({"AAPL"}, []))
    monkeypatch.setitem(uu._CACHE, "ndx", {"ARM"})
    monkeypatch.setitem(uu._CACHE, "cikmap", {"AAPL": "1", "ARM": "2"})
    u = uu.pit_universe(date(2024, 1, 31))
    assert u == ["AAPL", "ARM"]


def test_dedup_by_cik_collapses_share_classes(monkeypatch) -> None:
    # GOOG/GOOGL share a CIK -> collapse to one (shortest symbol kept); unknown kept.
    monkeypatch.setitem(
        uu._CACHE,
        "cikmap",
        {"GOOG": "1652044", "GOOGL": "1652044", "FOX": "1754301", "FOXA": "1754301", "AAPL": "320193"},
    )
    out = uu._dedup_by_cik({"GOOG", "GOOGL", "FOX", "FOXA", "AAPL", "UNKNOWNX"})
    assert "GOOGL" not in out and "GOOG" in out  # shortest kept
    assert "FOXA" not in out and "FOX" in out
    assert "AAPL" in out
    assert "UNKNOWNX" in out  # no CIK mapping -> never dropped
