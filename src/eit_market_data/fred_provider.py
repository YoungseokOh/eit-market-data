"""FRED-based macro data provider.

Implements MacroProvider using the free FRED API
(Federal Reserve Economic Data, https://fred.stlouisfed.org).

Requires a free API key from https://fred.stlouisfed.org/docs/api/api_key.html
Set via ``FRED_API_KEY`` environment variable or ``.env`` file.
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import date, timedelta

from eit_market_data.schemas.snapshot import MacroData

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# FRED Series IDs → MacroData key mapping
# ---------------------------------------------------------------------------

_RATES_SERIES: dict[str, str] = {
    "DFF": "fed_funds_rate",           # Effective Federal Funds Rate
    "DGS10": "treasury_10y",           # 10-Year Treasury
    "DGS2": "treasury_2y",             # 2-Year Treasury
}

_INFLATION_SERIES: dict[str, str] = {
    "CPIAUCSL": "cpi_index",           # CPI All Urban Consumers (index, we compute YoY)
    "PPIACO": "ppi_index",             # PPI All Commodities (index, we compute YoY)
    "DCOILWTICO": "oil_wti",           # WTI Crude Oil
    # GOLDAMGBD228NLBM (LBMA gold fix) was retired from FRED; gold fetched via yfinance GC=F
    "PCOPPUSDM": "copper",             # Copper Price
}

_GROWTH_SERIES: dict[str, str] = {
    "A191RL1Q225SBEA": "gdp_growth_yoy",   # Real GDP % change (quarterly, annualized)
    "UNRATE": "unemployment_rate",          # Unemployment Rate
    "MANEMP": "ism_proxy",                  # Manufacturing Employment (ISM proxy)
    "UMCSENT": "consumer_confidence",       # U of Michigan Consumer Sentiment
    "PAYEMS": "nonfarm_payrolls",           # Total Nonfarm Payrolls (thousands)
    # NAPM (ISM Manufacturing PMI) was discontinued; MANEMP used as proxy above
}

_MARKET_SERIES: dict[str, str] = {
    "VIXCLS": "vix",                        # VIX Index
    "BAMLC0A0CM": "ig_credit_spread",       # ICE BofA IG Corporate Spread
    "BAMLH0A0HYM2": "hy_credit_spread",     # ICE BofA HY Corporate Spread
}


def _get_fred_client():  # noqa: ANN202
    """Create a Fred client with API key from environment."""
    from fredapi import Fred

    api_key = os.environ.get("FRED_API_KEY", "")
    if not api_key:
        raise ValueError(
            "FRED_API_KEY environment variable is required. "
            "Get a free key at https://fred.stlouisfed.org/docs/api/api_key.html"
        )
    return Fred(api_key=api_key)


def _latest_value(
    fred, series_id: str, as_of: date, lookback_days: int = 90
) -> float | None:
    """Get the most recent observation on or before ``as_of``."""
    try:
        start = as_of - timedelta(days=lookback_days)
        data = fred.get_series(
            series_id,
            observation_start=start,
            observation_end=as_of,
        )
        if data is not None and not data.empty:
            # Drop NaN and get last valid
            data = data.dropna()
            if not data.empty:
                return round(float(data.iloc[-1]), 4)
    except Exception as e:
        logger.warning("Failed to fetch FRED series %s: %s", series_id, e)
    return None


def _yoy_change(
    fred, series_id: str, as_of: date
) -> float | None:
    """Compute year-over-year % change for a monthly/quarterly index series."""
    try:
        start = as_of - timedelta(days=400)
        data = fred.get_series(
            series_id,
            observation_start=start,
            observation_end=as_of,
        )
        if data is None or data.empty:
            return None
        data = data.dropna()
        if len(data) < 2:
            return None

        current = float(data.iloc[-1])
        # Find observation approximately 12 months ago
        target = as_of - timedelta(days=365)
        idx = data.index.get_indexer([target], method="nearest")[0]
        if idx < 0:
            return None
        past = float(data.iloc[idx])
        if past == 0:
            return None
        return round((current - past) / past * 100, 1)
    except Exception as e:
        logger.warning("Failed to compute YoY for %s: %s", series_id, e)
        return None


def _mom_change(
    fred, series_id: str, as_of: date
) -> float | None:
    """Compute month-over-month % change."""
    try:
        start = as_of - timedelta(days=90)
        data = fred.get_series(
            series_id,
            observation_start=start,
            observation_end=as_of,
        )
        if data is None or data.empty:
            return None
        data = data.dropna()
        if len(data) < 2:
            return None
        current = float(data.iloc[-1])
        prev = float(data.iloc[-2])
        if prev == 0:
            return None
        return round((current - prev) / prev * 100, 1)
    except Exception as e:
        logger.warning("Failed to compute MoM for %s: %s", series_id, e)
        return None


def _nonfarm_change(
    fred, as_of: date
) -> float | None:
    """Compute month-over-month change in nonfarm payrolls (thousands)."""
    try:
        start = as_of - timedelta(days=90)
        data = fred.get_series(
            "PAYEMS",
            observation_start=start,
            observation_end=as_of,
        )
        if data is None or data.empty:
            return None
        data = data.dropna()
        if len(data) < 2:
            return None
        return round(float(data.iloc[-1] - data.iloc[-2]), 0)
    except Exception as e:
        logger.warning("Failed to compute nonfarm change: %s", e)
        return None


class FredMacroProvider:
    """MacroProvider implementation using FRED API.

    Requires ``FRED_API_KEY`` environment variable (free).
    Install with: ``pip install fredapi``
    """

    async def fetch_macro(self, as_of: date) -> MacroData:
        """Fetch macro indicators from FRED, filtered to as_of."""
        return await asyncio.to_thread(self._fetch_macro_sync, as_of)

    def _fetch_macro_sync(self, as_of: date) -> MacroData:
        fred = _get_fred_client()

        # --- Rates & Policy ---
        rates: dict[str, object] = {}
        for series_id, key in _RATES_SERIES.items():
            val = _latest_value(fred, series_id, as_of)
            if val is not None:
                rates[key] = val

        # Yield curve spread
        if "treasury_10y" in rates and "treasury_2y" in rates:
            rates["yield_curve_spread_10y_2y"] = round(
                float(rates["treasury_10y"]) - float(rates["treasury_2y"]), 2
            )

        # Policy stance heuristic
        ff = rates.get("fed_funds_rate")
        if ff is not None:
            ff_val = float(ff)
            if ff_val >= 5.0:
                rates["policy_stance"] = "hawkish"
            elif ff_val <= 2.0:
                rates["policy_stance"] = "dovish"
            else:
                rates["policy_stance"] = "neutral"

        # --- Inflation & Commodities ---
        inflation: dict[str, object] = {}
        cpi_yoy = _yoy_change(fred, "CPIAUCSL", as_of)
        if cpi_yoy is not None:
            inflation["cpi_yoy"] = cpi_yoy
        cpi_mom = _mom_change(fred, "CPIAUCSL", as_of)
        if cpi_mom is not None:
            inflation["cpi_mom"] = cpi_mom
        ppi_yoy = _yoy_change(fred, "PPIACO", as_of)
        if ppi_yoy is not None:
            inflation["ppi_yoy"] = ppi_yoy

        for series_id in ("DCOILWTICO", "PCOPPUSDM"):
            key = _INFLATION_SERIES[series_id]
            val = _latest_value(fred, series_id, as_of)
            if val is not None:
                inflation[key] = val

        # Gold: fetch via yfinance GC=F (LBMA series GOLDAMGBD228NLBM was retired from FRED)
        try:
            from eit_market_data.yfinance_provider import YFinanceProvider

            yf_prov = YFinanceProvider()
            gold_bars = yf_prov._fetch_prices_sync("GC=F", as_of, lookback_days=30)
            if gold_bars:
                inflation["gold"] = round(gold_bars[-1].close, 2)
        except Exception as e:
            logger.debug("Could not fetch gold price via yfinance: %s", e)

        # --- Growth & Economy ---
        growth: dict[str, object] = {}
        gdp = _latest_value(fred, "A191RL1Q225SBEA", as_of, lookback_days=180)
        if gdp is not None:
            growth["gdp_growth_yoy"] = gdp
        for series_id in ("UNRATE", "UMCSENT"):
            key = _GROWTH_SERIES[series_id]
            val = _latest_value(fred, series_id, as_of)
            if val is not None:
                growth[key] = val

        nfp = _nonfarm_change(fred, as_of)
        if nfp is not None:
            growth["nonfarm_payrolls_k"] = nfp

        # ISM Manufacturing proxy via MANEMP (NAPM was discontinued on FRED)
        ism = _latest_value(fred, "MANEMP", as_of)
        if ism is not None:
            growth["ism_manufacturing"] = ism

        # --- Market Risk ---
        market: dict[str, object] = {}
        for series_id, key in _MARKET_SERIES.items():
            val = _latest_value(fred, series_id, as_of)
            if val is not None:
                market[key] = val

        # S&P 500 level from yfinance if available
        try:
            from eit_market_data.yfinance_provider import YFinanceProvider

            yf_prov = YFinanceProvider()
            sp_bars = yf_prov._fetch_prices_sync("^GSPC", as_of, lookback_days=30)
            if sp_bars:
                market["sp500_level"] = round(sp_bars[-1].close, 0)
                if len(sp_bars) >= 22:
                    prev = sp_bars[-22].close
                    curr = sp_bars[-1].close
                    market["sp500_monthly_return"] = round(
                        (curr - prev) / prev * 100, 1
                    )
        except Exception as e:
            logger.debug("Could not fetch S&P 500 for macro: %s", e)

        logger.info(
            "FRED macro fetched: rates=%d, inflation=%d, growth=%d, market=%d keys",
            len(rates),
            len(inflation),
            len(growth),
            len(market),
        )

        return MacroData(
            rates_policy=rates,
            inflation_commodities=inflation,
            growth_economy=growth,
            market_risk=market,
        )
