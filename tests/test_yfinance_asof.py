from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from eit_market_data.yfinance_provider import YFinanceProvider


def _frame(index: dict[str, list[float]], cols: list[str]) -> pd.DataFrame:
    columns = [pd.Timestamp(c) for c in cols]
    return pd.DataFrame(index.values(), index=list(index.keys()), columns=columns)


def _provider_with_statements(income, balance, cashflow, asof_close):
    prov = YFinanceProvider()
    stmts = {
        "quarterly_income_stmt": income,
        "quarterly_balance_sheet": balance,
        "quarterly_cashflow": cashflow,
    }
    prov._get_statement = lambda symbol, attr: stmts.get(attr)  # type: ignore[assignment]
    prov._asof_close = lambda symbol, as_of: asof_close  # type: ignore[assignment]
    return prov


def test_market_cap_reconstructed_as_of_not_current() -> None:
    """market_cap = as-of close x issued shares; future quarter dropped."""
    cols = ["2025-03-31", "2025-06-30"]  # 2nd is after the 60d lag for a 2025-06 as_of
    income = _frame({"Total Revenue": [500.0, 600.0], "Net Income": [100.0, 120.0]}, cols)
    balance = _frame(
        {
            "Total Assets": [2000.0, 2100.0],
            "Stockholders Equity": [800.0, 850.0],
            "Ordinary Shares Number": [1000.0, 1010.0],
        },
        cols,
    )
    cashflow = _frame({"Free Cash Flow": [90.0, 95.0]}, cols)
    prov = _provider_with_statements(income, balance, cashflow, asof_close=150.0)

    fd = prov._fetch_fundamentals_sync("TEST", date(2025, 6, 30), n_quarters=8)

    # Only the 2025-03-31 quarter is PIT-available for a 2025-06-30 decision.
    assert len(fd.quarters) == 1
    assert fd.quarters[0].report_date == date(2025, 3, 31)
    assert fd.quarters[0].issued_shares == 1000.0
    # last_close is the as-of close, market_cap = close x issued shares.
    assert fd.last_close_price == 150.0
    assert fd.market_cap == pytest.approx(150.0 * 1000.0)


def test_market_cap_none_without_issued_shares() -> None:
    cols = ["2025-03-31"]
    income = _frame({"Total Revenue": [500.0], "Net Income": [100.0]}, cols)
    balance = _frame({"Total Assets": [2000.0], "Stockholders Equity": [800.0]}, cols)
    cashflow = _frame({"Free Cash Flow": [90.0]}, cols)
    prov = _provider_with_statements(income, balance, cashflow, asof_close=150.0)

    fd = prov._fetch_fundamentals_sync("TEST", date(2025, 6, 30), n_quarters=8)
    assert fd.last_close_price == 150.0
    assert fd.market_cap is None  # no issued_shares -> cannot reconstruct


def test_no_close_yields_no_market_cap() -> None:
    cols = ["2025-03-31"]
    income = _frame({"Total Revenue": [500.0], "Net Income": [100.0]}, cols)
    balance = _frame(
        {"Total Assets": [2000.0], "Ordinary Shares Number": [1000.0]}, cols
    )
    cashflow = _frame({"Free Cash Flow": [90.0]}, cols)
    prov = _provider_with_statements(income, balance, cashflow, asof_close=None)

    fd = prov._fetch_fundamentals_sync("TEST", date(2025, 6, 30), n_quarters=8)
    assert fd.last_close_price is None
    assert fd.market_cap is None
