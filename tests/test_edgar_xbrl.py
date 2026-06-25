from __future__ import annotations

import asyncio
from datetime import date

import eit_market_data.edgar_xbrl_provider as xbrl
from eit_market_data.edgar_xbrl_provider import (
    EdgarXbrlFundamentalProvider,
    _fiscal_label,
    _pick_pit,
    _standalone_flows,
)
from datetime import date as _date

from helpers import flow_fact as _flow
from helpers import instant_fact as _inst


def test_standalone_flows_decomposes_ytd_and_q4() -> None:
    """3M direct; Q2=6M-3M; Q3=9M-6M; Q4=annual-9M (same fiscal-year start)."""
    s = "2024-01-01"
    entries = [
        {"start": s, "end": "2024-03-31", "val": 100, "filed": "2024-05-01", "fp": "Q1", "fy": 2024},
        {"start": s, "end": "2024-06-30", "val": 250, "filed": "2024-08-01", "fp": "Q2", "fy": 2024},
        {"start": s, "end": "2024-09-30", "val": 450, "filed": "2024-11-01", "fp": "Q3", "fy": 2024},
        {"start": s, "end": "2024-12-31", "val": 700, "filed": "2025-02-01", "fp": "FY", "fy": 2024},
    ]
    out = _standalone_flows(entries, _date(2025, 6, 30))
    assert out["2024-03-31"]["val"] == 100  # direct 3M
    assert out["2024-06-30"]["val"] == 150  # 250 - 100
    assert out["2024-09-30"]["val"] == 200  # 450 - 250
    assert out["2024-12-31"]["val"] == 250  # 700 - 450  (accounting Q4)
    assert out["2024-12-31"]["fp"] == "FY"


def test_standalone_flows_respects_pit_filed() -> None:
    s = "2024-01-01"
    entries = [
        {"start": s, "end": "2024-03-31", "val": 100, "filed": "2024-05-01", "fp": "Q1", "fy": 2024},
        {"start": s, "end": "2024-06-30", "val": 250, "filed": "2024-08-01", "fp": "Q2", "fy": 2024},
    ]
    # Before Q2 is filed, only Q1 is visible.
    out = _standalone_flows(entries, _date(2024, 7, 1))
    assert "2024-03-31" in out and "2024-06-30" not in out


def test_fiscal_label_uses_native_fy_fp() -> None:
    # With NATIVE (earliest-filed) fy/fp the label is the true fiscal quarter.
    assert _fiscal_label(_date(2024, 3, 30), 2024, "Q2") == "2024Q2"
    assert _fiscal_label(_date(2024, 9, 28), 2024, "FY") == "2024Q4"  # annual -> Q4
    assert _fiscal_label(_date(2023, 6, 30), None, None) == "2023Q2"  # calendar fallback


def test_standalone_flows_label_from_original_not_comparative() -> None:
    """fy/fp come from the earliest-filed (original) fact, not a comparative refiling."""
    s = "2024-01-01"
    entries = [
        # original Q1 report
        {"start": s, "end": "2024-03-31", "val": 100, "filed": "2024-05-01", "fp": "Q1", "fy": 2024},
        # same period re-reported as a comparative in a later FY2025 filing (contaminated fy/fp)
        {"start": s, "end": "2024-03-31", "val": 101, "filed": "2025-05-01", "fp": "Q2", "fy": 2025},
    ]
    out = _standalone_flows(entries, _date(2025, 12, 31))
    # value = latest-filed restatement (101); label = original (fy 2024, fp Q1)
    assert out["2024-03-31"]["val"] == 101
    assert out["2024-03-31"]["fy"] == 2024
    assert out["2024-03-31"]["fp"] == "Q1"


def test_pick_pit_filed_filter_and_latest_restatement() -> None:
    entries = [
        _flow("2024-01-01", "2024-03-31", 1000, "2024-05-01"),
        _flow("2024-01-01", "2024-03-31", 1050, "2024-09-01"),  # restatement
        _flow("2024-01-01", "2024-06-30", 2100, "2024-08-01"),  # YTD (181d) - excluded
    ]
    # Before the restatement is filed: original value, YTD excluded by duration.
    early = _pick_pit(entries, date(2024, 6, 30), instant=False)
    assert list(early.keys()) == ["2024-01-01:2024-03-31"]
    assert early["2024-01-01:2024-03-31"]["val"] == 1000
    # After restatement is visible: latest filed wins.
    late = _pick_pit(entries, date(2024, 12, 31), instant=False)
    assert late["2024-01-01:2024-03-31"]["val"] == 1050


def test_pick_pit_excludes_future_filings() -> None:
    entries = [_inst("2024-03-31", 5000, "2024-05-01"), _inst("2024-06-30", 5200, "2024-08-01")]
    pit = _pick_pit(entries, date(2024, 6, 30), instant=True)
    assert "2024-03-31" in pit and "2024-06-30" not in pit  # 06-30 filed after as_of


def _provider_with_facts(facts: dict, *, close: float | None, split: float = 1.0):
    prov = EdgarXbrlFundamentalProvider.__new__(EdgarXbrlFundamentalProvider)
    prov._price_provider = None
    prov._cache = None
    prov._semaphore = asyncio.Semaphore(1)
    prov._companyfacts = lambda cik: _async(facts)  # type: ignore[assignment]
    prov._asof_close = lambda ticker, as_of: close  # type: ignore[assignment]
    prov._split_factor_after = lambda ticker, as_of: split  # type: ignore[assignment]
    return prov


async def _async(value):  # noqa: ANN001, ANN202
    return value


def test_fetch_fundamentals_pit_and_market_cap(monkeypatch) -> None:
    facts = {
        "facts": {
            "us-gaap": {
                "Revenues": {
                    "units": {
                        "USD": [
                            _flow("2024-01-01", "2024-03-31", 1000, "2024-05-01"),
                            _flow("2024-04-01", "2024-06-30", 1100, "2024-08-01"),  # filed > as_of
                        ]
                    }
                },
                "NetIncomeLoss": {
                    "units": {"USD": [_flow("2024-01-01", "2024-03-31", 200, "2024-05-01")]}
                },
                "Assets": {
                    "units": {
                        "USD": [
                            _inst("2024-03-31", 5000, "2024-05-01"),
                            _inst("2024-06-30", 5200, "2024-08-01"),  # filed > as_of
                        ]
                    }
                },
                "StockholdersEquity": {
                    "units": {"USD": [_inst("2024-03-31", 3000, "2024-05-01")]}
                },
            },
            "dei": {
                "EntityCommonStockSharesOutstanding": {
                    "units": {"shares": [_inst("2024-04-15", 100, "2024-05-01")]}
                }
            },
        }
    }

    monkeypatch.setenv("SEC_EDGAR_USER_AGENT", "Test test@example.com")

    async def _fake_cik(client, ticker):  # noqa: ANN001, ANN202
        return "0000000001"

    monkeypatch.setattr(xbrl, "_ticker_to_cik", _fake_cik)
    prov = _provider_with_facts(facts, close=50.0, split=1.0)

    fd = asyncio.run(prov.fetch_fundamentals("TEST", date(2024, 6, 30), n_quarters=8))

    # Only the 2024-03-31 quarter is point-in-time visible at 2024-06-30.
    assert len(fd.quarters) == 1
    q = fd.quarters[0]
    assert q.fiscal_quarter == "2024Q1"
    assert q.report_date == date(2024, 5, 1) and q.report_date <= date(2024, 6, 30)
    assert q.revenue == 1000 and q.net_income == 200
    assert q.total_assets == 5000 and q.total_equity == 3000
    # market_cap = as-of close x as-of shares.
    assert fd.last_close_price == 50.0
    assert fd.market_cap == 50.0 * 100


def test_quarters_ordered_by_period_end_not_comparative_filing(monkeypatch) -> None:
    """A stale period re-filed as a comparative must not become quarters[0]."""
    facts = {
        "facts": {
            "us-gaap": {
                "Revenues": {
                    "units": {
                        "USD": [
                            # True most recent quarter, originally filed 2022-07-29.
                            _flow("2022-04-01", "2022-06-30", 1000, "2022-07-29"),
                            # Ancient 2019 quarter re-reported as a comparative column
                            # in the recent (2022-10-28) annual filing -> recent `filed`.
                            _flow("2019-07-01", "2019-09-30", 50, "2022-10-28"),
                        ]
                    }
                },
                "NetIncomeLoss": {
                    "units": {
                        "USD": [
                            _flow("2022-04-01", "2022-06-30", 200, "2022-07-29"),
                            _flow("2019-07-01", "2019-09-30", 10, "2022-10-28"),
                        ]
                    }
                },
            }
        }
    }

    monkeypatch.setenv("SEC_EDGAR_USER_AGENT", "Test test@example.com")

    async def _fake_cik(client, ticker):  # noqa: ANN001, ANN202
        return "0000000001"

    monkeypatch.setattr(xbrl, "_ticker_to_cik", _fake_cik)
    prov = _provider_with_facts(facts, close=None, split=1.0)
    fd = asyncio.run(prov.fetch_fundamentals("TEST", date(2023, 1, 31), n_quarters=8))

    assert len(fd.quarters) == 2
    # Most recent by economic period end, not by comparative filing date.
    assert fd.quarters[0].fiscal_quarter == "2022Q2"
    assert fd.quarters[0].net_income == 200
    assert fd.quarters[-1].fiscal_quarter == "2019Q3"


def test_merged_tags_recent_period_beats_deprecated_tag(monkeypatch) -> None:
    """A deprecated tag (old periods) must not hide a current tag's recent period."""
    facts = {
        "facts": {
            "us-gaap": {
                # Deprecated revenue tag, only ancient periods.
                "Revenues": {"units": {"USD": [_flow("2020-01-01", "2020-03-31", 500, "2020-05-01")]}},
                # Current revenue tag, recent period.
                "RevenueFromContractWithCustomerExcludingAssessedTax": {
                    "units": {"USD": [_flow("2024-01-01", "2024-03-31", 1500, "2024-05-01")]}
                },
                "NetIncomeLoss": {
                    "units": {"USD": [_flow("2024-01-01", "2024-03-31", 300, "2024-05-01")]}
                },
            }
        }
    }
    monkeypatch.setenv("SEC_EDGAR_USER_AGENT", "Test test@example.com")

    async def _fake_cik(client, ticker):  # noqa: ANN001, ANN202
        return "0000000001"

    monkeypatch.setattr(xbrl, "_ticker_to_cik", _fake_cik)
    prov = _provider_with_facts(facts, close=None, split=1.0)
    fd = asyncio.run(prov.fetch_fundamentals("TEST", date(2024, 6, 30), n_quarters=8))
    assert fd.quarters[0].fiscal_quarter == "2024Q1"
    assert fd.quarters[0].revenue == 1500  # recent period from the current tag


def test_asof_shares_prefers_outstanding_and_guards_staleness() -> None:
    prov = EdgarXbrlFundamentalProvider.__new__(EdgarXbrlFundamentalProvider)
    as_of = date(2024, 6, 30)
    # Outstanding (correct) is smaller than issued (includes treasury) at same end.
    dei = {
        "EntityCommonStockSharesOutstanding": {
            "units": {"shares": [_inst("2024-04-15", 100, "2024-05-01")]}
        }
    }
    gaap = {
        "CommonStockSharesIssued": {"units": {"shares": [_inst("2024-03-31", 150, "2024-05-01")]}}
    }
    assert prov._asof_shares(gaap, dei, as_of) == 100  # outstanding preferred over issued

    # Staleness guard: a decade-old count is rejected (no recent entry -> None).
    stale_only = {
        "EntityCommonStockSharesOutstanding": {
            "units": {"shares": [_inst("2011-04-15", 941481, "2011-05-01")]}
        }
    }
    assert prov._asof_shares({}, stale_only, as_of) is None


def test_issued_shares_backfilled_from_asof_count(monkeypatch) -> None:
    """When no us-gaap issued-shares tag exists, issued_shares is filled from the
    as-of share count used for market_cap (and market_cap is unchanged)."""
    facts = {
        "facts": {
            "us-gaap": {
                "Revenues": {"units": {"USD": [_flow("2024-01-01", "2024-03-31", 1000, "2024-05-01")]}},
                "NetIncomeLoss": {"units": {"USD": [_flow("2024-01-01", "2024-03-31", 200, "2024-05-01")]}},
                # No CommonStockSharesOutstanding/Issued tag -> per-quarter
                # issued_shares would be None. Shares come only from the
                # weighted-average fallback (mimics ACN/F/CMCSA/BRK-B).
                "WeightedAverageNumberOfDilutedSharesOutstanding": {
                    "units": {"shares": [_inst("2024-03-31", 640, "2024-05-01")]}
                },
            }
        }
    }
    monkeypatch.setenv("SEC_EDGAR_USER_AGENT", "Test test@example.com")

    async def _fake_cik(client, ticker):  # noqa: ANN001, ANN202
        return "0000000001"

    monkeypatch.setattr(xbrl, "_ticker_to_cik", _fake_cik)
    prov = _provider_with_facts(facts, close=10.0, split=1.0)
    fd = asyncio.run(prov.fetch_fundamentals("TEST", date(2024, 6, 30), n_quarters=8))

    assert len(fd.quarters) == 1
    # issued_shares backfilled from the as-of share count (640).
    assert fd.quarters[0].issued_shares == 640
    # market_cap = close x as-of shares = 10 * 640, unchanged by the backfill.
    assert fd.market_cap == 10.0 * 640


def test_issued_shares_not_overwritten_when_tag_present(monkeypatch) -> None:
    """A real CommonStockSharesOutstanding value is NOT replaced by the fallback."""
    facts = {
        "facts": {
            "us-gaap": {
                "Revenues": {"units": {"USD": [_flow("2024-01-01", "2024-03-31", 1000, "2024-05-01")]}},
                "NetIncomeLoss": {"units": {"USD": [_flow("2024-01-01", "2024-03-31", 200, "2024-05-01")]}},
                "CommonStockSharesOutstanding": {
                    "units": {"shares": [_inst("2024-03-31", 500, "2024-05-01")]}
                },
            },
            "dei": {
                "EntityCommonStockSharesOutstanding": {
                    "units": {"shares": [_inst("2024-04-15", 510, "2024-05-01")]}
                }
            },
        }
    }
    monkeypatch.setenv("SEC_EDGAR_USER_AGENT", "Test test@example.com")

    async def _fake_cik(client, ticker):  # noqa: ANN001, ANN202
        return "0000000001"

    monkeypatch.setattr(xbrl, "_ticker_to_cik", _fake_cik)
    prov = _provider_with_facts(facts, close=10.0, split=1.0)
    fd = asyncio.run(prov.fetch_fundamentals("TEST", date(2024, 6, 30), n_quarters=8))
    # Per-quarter issued_shares keeps its reported value (500), not the dei 510.
    assert fd.quarters[0].issued_shares == 500


def test_market_cap_applies_split_factor(monkeypatch) -> None:
    facts = {
        "facts": {
            "us-gaap": {
                "Revenues": {"units": {"USD": [_flow("2024-01-01", "2024-03-31", 1000, "2024-05-01")]}},
                "NetIncomeLoss": {"units": {"USD": [_flow("2024-01-01", "2024-03-31", 200, "2024-05-01")]}},
            },
            "dei": {
                "EntityCommonStockSharesOutstanding": {
                    "units": {"shares": [_inst("2024-04-15", 100, "2024-05-01")]}
                }
            },
        }
    }

    monkeypatch.setenv("SEC_EDGAR_USER_AGENT", "Test test@example.com")

    async def _fake_cik(client, ticker):  # noqa: ANN001, ANN202
        return "0000000001"

    monkeypatch.setattr(xbrl, "_ticker_to_cik", _fake_cik)
    # _asof_close already incorporates the split factor in production; here we
    # mock it returning the split-corrected (as-traded) close directly.
    prov = _provider_with_facts(facts, close=200.0, split=1.0)
    fd = asyncio.run(prov.fetch_fundamentals("TEST", date(2024, 6, 30), n_quarters=8))
    assert fd.market_cap == 200.0 * 100
