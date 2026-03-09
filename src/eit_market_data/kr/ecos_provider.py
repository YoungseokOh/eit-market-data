"""Bank of Korea ECOS macro provider for Korean market data."""

from __future__ import annotations

import asyncio
import logging
import os
import time
from datetime import date, timedelta
from typing import Any

from eit_market_data.schemas.snapshot import MacroData

logger = logging.getLogger(__name__)

_ECOS_BASE_URL = "https://ecos.bok.or.kr/api/StatisticSearch"

KR_RATES: dict[tuple[str, str, str], str] = {
    ("722Y001", "0101000", "D"): "base_rate",     # 기준금리
    ("817Y002", "010190000", "MM"): "yield_3y",   # 국고채 3년
    ("817Y002", "010210000", "MM"): "yield_10y",  # 국고채 10년
}

KR_INFLATION: dict[tuple[str, str, str], str] = {
    ("901Y009", "0", "MM"): "cpi_index",          # 소비자물가지수
    ("404Y014", "BBB", "MM"): "ppi_index",        # 생산자물가지수
}

KR_GROWTH: dict[tuple[str, str, str], str] = {
    ("200Y001", "10101", "A"): "gdp_growth_yoy",  # 실질 GDP 성장률
    ("101Y003", "LF", "MM"): "unemployment_rate",  # 실업률
    ("521Y001", "A", "MM"): "trade_balance",      # 무역수지
}

KR_MARKET: dict[tuple[str, str, str], str] = {
    ("731Y001", "0000001", "D"): "usd_krw",       # 원달러 환율
}


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text in {"-", "N/A", "nan", "None"}:
        return None
    try:
        return float(text.replace(",", ""))
    except ValueError:
        return None


def _month_start(year: int, month: int) -> date:
    return date(year, month, 1)


def _parse_ecos_time(raw_time: str, period: str) -> date | None:
    try:
        time_str = str(raw_time).strip()
        if period == "D":
            return date(int(time_str[:4]), int(time_str[4:6]), int(time_str[6:8]))
        if period == "MM":
            return _month_start(int(time_str[:4]), int(time_str[4:6]))
        if period == "A":
            return date(int(time_str[:4]), 1, 1)
    except Exception:
        return None
    return None


def _period_range(as_of: date, period: str) -> tuple[str, str]:
    if period == "D":
        start = (as_of - timedelta(days=730)).strftime("%Y%m%d")
        end = as_of.strftime("%Y%m%d")
        return start, end
    if period == "MM":
        start = f"{as_of.year - 3:04d}01"
        end = as_of.strftime("%Y%m")
        return start, end
    if period == "A":
        start = str(as_of.year - 10)
        end = str(as_of.year)
        return start, end
    raise ValueError(f"Unsupported period: {period}")


class EcosMacroProvider:
    """MacroProvider implementation using ECOS API."""

    def __init__(self, api_key: str | None = None) -> None:
        try:
            import requests as requests_module
        except ImportError as e:
            raise ImportError(
                "requests is required for EcosMacroProvider. "
                "Install with: pip install -e '.[kr]'"
            ) from e

        key = api_key or os.environ.get("ECOS_API_KEY", "")
        if not key:
            raise ValueError(
                "ECOS_API_KEY environment variable is required for EcosMacroProvider."
            )
        self._requests = requests_module
        self._api_key = key

    async def fetch_macro(self, as_of: date) -> MacroData:
        """Fetch Korean macro indicators from ECOS."""
        try:
            return await asyncio.to_thread(self._fetch_macro_sync, as_of)
        except Exception as e:
            logger.warning("ECOS macro fetch failed: %s", e)
            return MacroData()

    def _fetch_rows(
        self,
        stat_code: str,
        item_code: str,
        period: str,
        start_date: str,
        end_date: str,
    ) -> list[dict[str, Any]]:
        url = (
            f"{_ECOS_BASE_URL}/{self._api_key}/json/kr/1/100/"
            f"{stat_code}/{period}/{start_date}/{end_date}/{item_code}"
        )
        try:
            resp = None
            for attempt, backoff in enumerate([0, 1, 2, 4]):
                if backoff:
                    time.sleep(backoff)
                resp = self._requests.get(url, timeout=15)
                if resp.status_code != 429:
                    break
                logger.warning(
                    "ECOS 429 rate limit (%s/%s), attempt %d/3",
                    stat_code, item_code, attempt + 1,
                )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.warning("ECOS request failed (%s/%s): %s", stat_code, item_code, e)
            return []

        rows = data.get("StatisticSearch", {}).get("row", [])
        return rows if isinstance(rows, list) else []

    def _latest_value(
        self, stat_code: str, item_code: str, period: str, as_of: date
    ) -> tuple[float | None, date | None]:
        start, end = _period_range(as_of, period)
        rows = self._fetch_rows(stat_code, item_code, period, start, end)

        values: list[tuple[date, float]] = []
        for row in rows:
            obs_date = _parse_ecos_time(str(row.get("TIME", "")), period)
            if obs_date is None or obs_date > as_of:
                continue
            obs_val = _safe_float(row.get("DATA_VALUE"))
            if obs_val is None:
                continue
            values.append((obs_date, obs_val))

        if not values:
            return None, None
        values.sort(key=lambda x: x[0])
        latest_date, latest_val = values[-1]
        return round(latest_val, 4), latest_date

    def _cpi_yoy(self, as_of: date) -> float | None:
        stat_code, item_code, period = ("901Y009", "0", "MM")
        start = f"{as_of.year - 4:04d}01"
        end = as_of.strftime("%Y%m")
        rows = self._fetch_rows(stat_code, item_code, period, start, end)

        values: list[tuple[date, float]] = []
        for row in rows:
            obs_date = _parse_ecos_time(str(row.get("TIME", "")), period)
            obs_val = _safe_float(row.get("DATA_VALUE"))
            if obs_date is None or obs_val is None or obs_date > as_of:
                continue
            values.append((obs_date, obs_val))

        if len(values) < 13:
            return None
        values.sort(key=lambda x: x[0])
        current_date, current_val = values[-1]

        past_val: float | None = None
        target_year = current_date.year - 1
        target_month = current_date.month
        for obs_date, obs_val in values:
            if obs_date.year == target_year and obs_date.month == target_month:
                past_val = obs_val
                break

        if past_val is None:
            target_date = current_date - timedelta(days=365)
            nearest = min(values, key=lambda x: abs((x[0] - target_date).days))
            if abs((nearest[0] - target_date).days) <= 40:
                past_val = nearest[1]

        if past_val is None or past_val == 0:
            return None
        return round((current_val - past_val) / past_val * 100, 1)

    def _collect_category(
        self,
        mapping: dict[tuple[str, str, str], str],
        as_of: date,
    ) -> dict[str, float]:
        category: dict[str, float] = {}
        for (stat_code, item_code, period), key in mapping.items():
            value, _obs_date = self._latest_value(stat_code, item_code, period, as_of)
            if value is not None:
                category[key] = value
        return category

    def _fetch_macro_sync(self, as_of: date) -> MacroData:
        rates = self._collect_category(KR_RATES, as_of)
        inflation = self._collect_category(KR_INFLATION, as_of)
        growth = self._collect_category(KR_GROWTH, as_of)
        market = self._collect_category(KR_MARKET, as_of)

        cpi_yoy = self._cpi_yoy(as_of)
        if cpi_yoy is not None:
            inflation["cpi_yoy"] = cpi_yoy

        base_rate = rates.get("base_rate")
        if base_rate is not None:
            if base_rate >= 3.5:
                rates["policy_stance"] = "hawkish"
            elif base_rate <= 1.5:
                rates["policy_stance"] = "dovish"
            else:
                rates["policy_stance"] = "neutral"

        if "yield_10y" in rates and "yield_3y" in rates:
            rates["yield_curve_spread_10y_3y"] = round(
                rates["yield_10y"] - rates["yield_3y"], 4
            )

        return MacroData(
            rates_policy=rates,
            inflation_commodities=inflation,
            growth_economy=growth,
            market_risk=market,
        )
