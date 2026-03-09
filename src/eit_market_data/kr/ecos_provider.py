"""Bank of Korea ECOS macro provider for Korean market data."""

from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from eit_market_data.schemas.snapshot import MacroData

logger = logging.getLogger(__name__)

_ECOS_SEARCH_BASE_URL = "https://ecos.bok.or.kr/api/StatisticSearch"
_ECOS_ITEM_LIST_BASE_URL = "https://ecos.bok.or.kr/api/StatisticItemList"
_ECOS_PAGE_SIZE = 1000
_ECOS_BACKOFF_SECONDS = (0, 1, 2, 4)


@dataclass(frozen=True)
class EcosSeriesSpec:
    """Definition for one ECOS time series."""

    stat_code: str
    item_code: str
    period: str
    category: str
    key: str


BASE_RATE_SPEC = EcosSeriesSpec("722Y001", "0101000", "D", "rates_policy", "base_rate")
YIELD_3Y_SPEC = EcosSeriesSpec("817Y002", "010200000", "D", "rates_policy", "yield_3y")
YIELD_10Y_SPEC = EcosSeriesSpec("817Y002", "010210000", "D", "rates_policy", "yield_10y")
CPI_INDEX_SPEC = EcosSeriesSpec("901Y009", "0", "M", "inflation_commodities", "cpi_index")
PPI_INDEX_SPEC = EcosSeriesSpec("404Y014", "*AA", "M", "inflation_commodities", "ppi_index")
GDP_GROWTH_SPEC = EcosSeriesSpec("902Y015", "KOR", "Q", "growth_economy", "gdp_growth_yoy")
UNEMPLOYMENT_SPEC = EcosSeriesSpec(
    "902Y021",
    "KOR",
    "M",
    "growth_economy",
    "unemployment_rate",
)
USD_KRW_SPEC = EcosSeriesSpec("731Y001", "0000001", "D", "market_risk", "usd_krw")
EXPORT_VALUE_SPEC = EcosSeriesSpec("901Y118", "T002", "M", "growth_economy", "exports")
IMPORT_VALUE_SPEC = EcosSeriesSpec("901Y118", "T004", "M", "growth_economy", "imports")

KR_SERIES: tuple[EcosSeriesSpec, ...] = (
    BASE_RATE_SPEC,
    YIELD_3Y_SPEC,
    YIELD_10Y_SPEC,
    CPI_INDEX_SPEC,
    PPI_INDEX_SPEC,
    GDP_GROWTH_SPEC,
    UNEMPLOYMENT_SPEC,
    USD_KRW_SPEC,
)


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


def _quarter_start(year: int, quarter: int) -> date:
    return date(year, (quarter - 1) * 3 + 1, 1)


def _parse_ecos_time(raw_time: str, period: str) -> date | None:
    try:
        time_str = str(raw_time).strip()
        if period == "D":
            return date(int(time_str[:4]), int(time_str[4:6]), int(time_str[6:8]))
        if period == "M":
            return _month_start(int(time_str[:4]), int(time_str[4:6]))
        if period == "Q":
            return _quarter_start(int(time_str[:4]), int(time_str[-1]))
        if period == "A":
            return date(int(time_str[:4]), 1, 1)
    except Exception:
        return None
    return None


def _period_token_key(raw_time: str, period: str) -> tuple[int, ...]:
    token = str(raw_time).strip()
    if period == "D":
        return int(token[:4]), int(token[4:6]), int(token[6:8])
    if period == "M":
        return int(token[:4]), int(token[4:6])
    if period == "Q":
        return int(token[:4]), int(token[-1])
    if period == "A":
        return (int(token[:4]),)
    raise ValueError(f"Unsupported period: {period}")


def _period_range(as_of: date, period: str) -> tuple[str, str]:
    if period == "D":
        return (as_of - timedelta(days=730)).strftime("%Y%m%d"), as_of.strftime("%Y%m%d")
    if period == "M":
        return f"{as_of.year - 4:04d}01", as_of.strftime("%Y%m")
    if period == "Q":
        return f"{as_of.year - 6:04d}Q1", f"{as_of.year:04d}Q{((as_of.month - 1) // 3) + 1}"
    if period == "A":
        return str(as_of.year - 10), str(as_of.year)
    raise ValueError(f"Unsupported period: {period}")


def _same_period_previous_year(obs_date: date, period: str) -> tuple[int, int | None]:
    if period == "M":
        return obs_date.year - 1, obs_date.month
    if period == "Q":
        return obs_date.year - 1, ((obs_date.month - 1) // 3) + 1
    if period == "A":
        return obs_date.year - 1, None
    raise ValueError(f"Unsupported period for YoY: {period}")


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
        self._bounds_cache: dict[tuple[str, str, str], tuple[str | None, str | None]] = {}

    async def fetch_macro(self, as_of: date) -> MacroData:
        """Fetch Korean macro indicators from ECOS."""
        try:
            return await asyncio.to_thread(self._fetch_macro_sync, as_of)
        except Exception as e:
            logger.warning("ECOS macro fetch failed: %s", e)
            return MacroData()

    def _request_json(self, url: str, context: str) -> dict[str, Any] | None:
        for attempt, backoff in enumerate(_ECOS_BACKOFF_SECONDS, start=1):
            if backoff:
                time.sleep(backoff)
            try:
                resp = self._requests.get(url, timeout=15)
            except Exception as e:
                if attempt == len(_ECOS_BACKOFF_SECONDS):
                    logger.warning("ECOS request failed (%s): %s", context, e)
                    return None
                continue

            if resp.status_code == 429:
                logger.warning("ECOS 429 rate limit (%s), attempt %d/4", context, attempt)
                continue

            try:
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                logger.warning("ECOS response parse failed (%s): %s", context, e)
                return None

        logger.warning("ECOS request exhausted retries (%s)", context)
        return None

    def _series_bounds(self, spec: EcosSeriesSpec) -> tuple[str | None, str | None]:
        cache_key = (spec.stat_code, spec.item_code, spec.period)
        if cache_key in self._bounds_cache:
            return self._bounds_cache[cache_key]

        url = (
            f"{_ECOS_ITEM_LIST_BASE_URL}/{self._api_key}/json/kr/1/1000/{spec.stat_code}"
        )
        data = self._request_json(url, f"item-list:{spec.stat_code}")
        if not data:
            self._bounds_cache[cache_key] = (None, None)
            return None, None

        rows = data.get("StatisticItemList", {}).get("row", [])
        if not isinstance(rows, list):
            self._bounds_cache[cache_key] = (None, None)
            return None, None

        matched = [
            row
            for row in rows
            if str(row.get("ITEM_CODE", "")).strip() == spec.item_code
            and str(row.get("CYCLE", "")).strip() == spec.period
        ]
        if not matched:
            logger.warning(
                "ECOS item metadata missing for %s/%s/%s",
                spec.stat_code,
                spec.item_code,
                spec.period,
            )
            self._bounds_cache[cache_key] = (None, None)
            return None, None

        start_token = min(
            (str(row.get("START_TIME", "")).strip() for row in matched if row.get("START_TIME")),
            key=lambda raw: _period_token_key(raw, spec.period),
            default=None,
        )
        end_token = max(
            (str(row.get("END_TIME", "")).strip() for row in matched if row.get("END_TIME")),
            key=lambda raw: _period_token_key(raw, spec.period),
            default=None,
        )
        self._bounds_cache[cache_key] = (start_token, end_token)
        return start_token, end_token

    def _clamp_range(self, spec: EcosSeriesSpec, start_token: str, end_token: str) -> tuple[str, str] | None:
        bound_start, bound_end = self._series_bounds(spec)
        start = start_token
        end = end_token

        if bound_start and _period_token_key(start, spec.period) < _period_token_key(bound_start, spec.period):
            start = bound_start
        if bound_end and _period_token_key(end, spec.period) > _period_token_key(bound_end, spec.period):
            end = bound_end

        if _period_token_key(start, spec.period) > _period_token_key(end, spec.period):
            return None
        return start, end

    def _fetch_rows_for_spec(self, spec: EcosSeriesSpec, as_of: date) -> list[dict[str, Any]]:
        raw_range = _period_range(as_of, spec.period)
        clamped = self._clamp_range(spec, *raw_range)
        if clamped is None:
            return []
        start_token, end_token = clamped

        rows: list[dict[str, Any]] = []
        start_row = 1
        while True:
            end_row = start_row + _ECOS_PAGE_SIZE - 1
            url = (
                f"{_ECOS_SEARCH_BASE_URL}/{self._api_key}/json/kr/{start_row}/{end_row}/"
                f"{spec.stat_code}/{spec.period}/{start_token}/{end_token}/{spec.item_code}"
            )
            data = self._request_json(url, f"{spec.stat_code}/{spec.item_code}")
            if not data:
                return rows

            result = data.get("RESULT") or data.get("StatisticSearch", {}).get("RESULT")
            if result:
                if result.get("CODE") != "INFO-200":
                    logger.warning(
                        "ECOS search returned %s for %s/%s: %s",
                        result.get("CODE"),
                        spec.stat_code,
                        spec.item_code,
                        result.get("MESSAGE", ""),
                    )
                return rows

            chunk = data.get("StatisticSearch", {}).get("row", [])
            if not isinstance(chunk, list) or not chunk:
                return rows

            rows.extend(chunk)
            if len(chunk) < _ECOS_PAGE_SIZE:
                return rows
            start_row += _ECOS_PAGE_SIZE

    def _series_values(self, spec: EcosSeriesSpec, as_of: date) -> list[tuple[date, float]]:
        values: list[tuple[date, float]] = []
        for row in self._fetch_rows_for_spec(spec, as_of):
            obs_date = _parse_ecos_time(str(row.get("TIME", "")), spec.period)
            if obs_date is None or obs_date > as_of:
                continue
            obs_val = _safe_float(row.get("DATA_VALUE"))
            if obs_val is None:
                continue
            values.append((obs_date, obs_val))
        values.sort(key=lambda item: item[0])
        return values

    def _latest_value(self, spec: EcosSeriesSpec, as_of: date) -> tuple[float | None, date | None]:
        values = self._series_values(spec, as_of)
        if not values:
            return None, None
        latest_date, latest_value = values[-1]
        return round(latest_value, 4), latest_date

    def _yoy_change(self, spec: EcosSeriesSpec, as_of: date) -> float | None:
        if spec.period not in {"M", "Q", "A"}:
            return None

        values = self._series_values(spec, as_of)
        if len(values) < 2:
            return None

        current_date, current_val = values[-1]
        target_year, target_slot = _same_period_previous_year(current_date, spec.period)

        prior_val: float | None = None
        for obs_date, obs_val in values:
            if spec.period == "M" and obs_date.year == target_year and obs_date.month == target_slot:
                prior_val = obs_val
                break
            if spec.period == "Q":
                quarter = ((obs_date.month - 1) // 3) + 1
                if obs_date.year == target_year and quarter == target_slot:
                    prior_val = obs_val
                    break
            if spec.period == "A" and obs_date.year == target_year:
                prior_val = obs_val
                break

        if prior_val is None or prior_val == 0:
            return None
        return round((current_val - prior_val) / prior_val * 100, 1)

    def _latest_trade_balance(self, as_of: date) -> float | None:
        export_value, _ = self._latest_value(EXPORT_VALUE_SPEC, as_of)
        import_value, _ = self._latest_value(IMPORT_VALUE_SPEC, as_of)
        if export_value is None or import_value is None:
            return None
        return round(export_value - import_value, 4)

    def _fetch_macro_sync(self, as_of: date) -> MacroData:
        categories: dict[str, dict[str, Any]] = {
            "rates_policy": {},
            "inflation_commodities": {},
            "growth_economy": {},
            "market_risk": {},
        }

        for spec in KR_SERIES:
            value, _obs_date = self._latest_value(spec, as_of)
            if value is None:
                continue
            categories[spec.category][spec.key] = value

        cpi_yoy = self._yoy_change(CPI_INDEX_SPEC, as_of)
        if cpi_yoy is not None:
            categories["inflation_commodities"]["cpi_yoy"] = cpi_yoy

        ppi_yoy = self._yoy_change(PPI_INDEX_SPEC, as_of)
        if ppi_yoy is not None:
            categories["inflation_commodities"]["ppi_yoy"] = ppi_yoy

        trade_balance = self._latest_trade_balance(as_of)
        if trade_balance is not None:
            categories["growth_economy"]["trade_balance"] = trade_balance

        rates = categories["rates_policy"]
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
            rates_policy=categories["rates_policy"],
            inflation_commodities=categories["inflation_commodities"],
            growth_economy=categories["growth_economy"],
            market_risk=categories["market_risk"],
        )
