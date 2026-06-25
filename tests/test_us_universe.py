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


def test_pit_universe_drops_unlisted_ndx_phantoms(monkeypatch) -> None:
    """drop_unlisted removes NDX-only names with no as-of listing; SP500 PIT kept."""
    # AAPL is an S&P 500 PIT member; ARM is NDX-only and not yet listed in early 2023.
    monkeypatch.setitem(uu._CACHE, "sp500", ({"AAPL"}, []))
    monkeypatch.setitem(uu._CACHE, "ndx", {"AAPL", "ARM", "MSFT"})
    monkeypatch.setitem(uu._CACHE, "cikmap", {"AAPL": "1", "ARM": "2", "MSFT": "3"})

    # ARM has no price history as-of; MSFT does.
    listed = {"MSFT": True, "ARM": False}

    def has_listing(ticker, as_of):  # noqa: ANN001, ANN202
        return listed.get(ticker, True)

    u = uu.pit_universe(date(2023, 2, 28), drop_unlisted=has_listing)
    assert "ARM" not in u  # NDX-only phantom dropped
    assert "MSFT" in u  # NDX-only but listed -> kept
    assert "AAPL" in u  # S&P 500 PIT member -> always kept


def test_pit_universe_keeps_sp500_member_even_if_listing_check_false(monkeypatch) -> None:
    """An S&P 500 PIT member is never dropped, even if the listing probe says no."""
    monkeypatch.setitem(uu._CACHE, "sp500", ({"AAPL"}, []))
    monkeypatch.setitem(uu._CACHE, "ndx", {"AAPL"})
    monkeypatch.setitem(uu._CACHE, "cikmap", {"AAPL": "1"})
    u = uu.pit_universe(date(2023, 2, 28), drop_unlisted=lambda t, a: False)
    assert u == ["AAPL"]


def test_pit_universe_keeps_phantom_on_listing_check_error(monkeypatch) -> None:
    """A transient error in the listing probe keeps the name (no false drops)."""
    monkeypatch.setitem(uu._CACHE, "sp500", ({"AAPL"}, []))
    monkeypatch.setitem(uu._CACHE, "ndx", {"AAPL", "ARM"})
    monkeypatch.setitem(uu._CACHE, "cikmap", {"AAPL": "1", "ARM": "2"})

    def boom(ticker, as_of):  # noqa: ANN001, ANN202
        raise RuntimeError("yfinance down")

    u = uu.pit_universe(date(2023, 2, 28), drop_unlisted=boom)
    assert "ARM" in u and "AAPL" in u


def test_pit_universe_none_hook_keeps_ndx_union(monkeypatch) -> None:
    """drop_unlisted=None preserves prior behavior (full NDX union)."""
    monkeypatch.setitem(uu._CACHE, "sp500", ({"AAPL"}, []))
    monkeypatch.setitem(uu._CACHE, "ndx", {"ARM"})
    monkeypatch.setitem(uu._CACHE, "cikmap", {"AAPL": "1", "ARM": "2"})
    assert uu.pit_universe(date(2023, 2, 28)) == ["AAPL", "ARM"]


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
