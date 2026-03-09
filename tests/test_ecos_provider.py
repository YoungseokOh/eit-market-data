from __future__ import annotations

from datetime import date

from eit_market_data.kr.ecos_provider import (
    BASE_RATE_SPEC,
    CPI_INDEX_SPEC,
    EXPORT_VALUE_SPEC,
    EcosMacroProvider,
    PPI_INDEX_SPEC,
    USD_KRW_SPEC,
    YIELD_10Y_SPEC,
    YIELD_3Y_SPEC,
)


def test_latest_value_paginates_to_newest_row(monkeypatch) -> None:
    provider = EcosMacroProvider(api_key="test")
    monkeypatch.setattr("eit_market_data.kr.ecos_provider._ECOS_PAGE_SIZE", 2)

    def fake_request_json(url: str, context: str):  # noqa: ANN001
        if "StatisticItemList" in url:
            return {
                "StatisticItemList": {
                    "row": [
                        {
                            "ITEM_CODE": BASE_RATE_SPEC.item_code,
                            "CYCLE": BASE_RATE_SPEC.period,
                            "START_TIME": "20240101",
                            "END_TIME": "20240131",
                        }
                    ]
                }
            }
        if "/json/kr/1/2/" in url:
            return {
                "StatisticSearch": {
                    "row": [
                        {"TIME": "20240101", "DATA_VALUE": "3.0"},
                        {"TIME": "20240102", "DATA_VALUE": "3.1"},
                    ]
                }
            }
        if "/json/kr/3/4/" in url:
            return {
                "StatisticSearch": {
                    "row": [{"TIME": "20240103", "DATA_VALUE": "3.2"}]
                }
            }
        raise AssertionError(url)

    monkeypatch.setattr(provider, "_request_json", fake_request_json)

    value, obs_date = provider._latest_value(BASE_RATE_SPEC, date(2024, 1, 4))

    assert value == 3.2
    assert obs_date == date(2024, 1, 3)


def test_latest_value_clamps_end_to_series_metadata(monkeypatch) -> None:
    provider = EcosMacroProvider(api_key="test")
    seen_urls: list[str] = []

    def fake_request_json(url: str, context: str):  # noqa: ANN001
        seen_urls.append(url)
        if "StatisticItemList" in url:
            return {
                "StatisticItemList": {
                    "row": [
                        {
                            "ITEM_CODE": EXPORT_VALUE_SPEC.item_code,
                            "CYCLE": EXPORT_VALUE_SPEC.period,
                            "START_TIME": "200001",
                            "END_TIME": "202512",
                        }
                    ]
                }
            }
        return {
            "StatisticSearch": {
                "row": [{"TIME": "202512", "DATA_VALUE": "52054195"}]
            }
        }

    monkeypatch.setattr(provider, "_request_json", fake_request_json)

    value, obs_date = provider._latest_value(EXPORT_VALUE_SPEC, date(2026, 2, 27))

    assert value == 52054195.0
    assert obs_date == date(2025, 12, 1)
    assert any("/901Y118/M/202201/202512/T002" in url for url in seen_urls)


def test_fetch_macro_sync_populates_expected_keys(monkeypatch) -> None:
    provider = EcosMacroProvider(api_key="test")
    latest_values = {
        BASE_RATE_SPEC.key: 3.5,
        YIELD_3Y_SPEC.key: 2.9,
        YIELD_10Y_SPEC.key: 3.1,
        CPI_INDEX_SPEC.key: 114.2,
        PPI_INDEX_SPEC.key: 120.5,
        "gdp_growth_yoy": 1.3,
        "unemployment_rate": 2.8,
        USD_KRW_SPEC.key: 1378.2,
    }

    def fake_latest_value(spec, as_of):  # noqa: ANN001
        return latest_values.get(spec.key), as_of

    def fake_yoy_change(spec, as_of):  # noqa: ANN001
        if spec.key == CPI_INDEX_SPEC.key:
            return 2.1
        if spec.key == PPI_INDEX_SPEC.key:
            return 1.4
        return None

    monkeypatch.setattr(provider, "_latest_value", fake_latest_value)
    monkeypatch.setattr(provider, "_yoy_change", fake_yoy_change)
    monkeypatch.setattr(provider, "_latest_trade_balance", lambda as_of: 7.5)

    macro = provider._fetch_macro_sync(date(2026, 2, 27))

    assert macro.rates_policy["base_rate"] == 3.5
    assert macro.rates_policy["policy_stance"] == "hawkish"
    assert macro.rates_policy["yield_curve_spread_10y_3y"] == 0.2
    assert macro.inflation_commodities["cpi_yoy"] == 2.1
    assert macro.inflation_commodities["ppi_yoy"] == 1.4
    assert macro.growth_economy["trade_balance"] == 7.5
    assert macro.market_risk["usd_krw"] == 1378.2
