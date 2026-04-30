from __future__ import annotations

import asyncio
from datetime import date
from pathlib import Path

import pandas as pd

from eit_market_data.kr.ci_safe_provider import FdrNaverPriceProvider, SeedSectorProvider
from eit_market_data.snapshot import create_kr_providers
from eit_market_data.schemas.snapshot import FundamentalData, QuarterlyFinancials


def test_create_kr_providers_ci_safe_uses_seed_sector(tmp_path: Path, monkeypatch) -> None:
    universe_csv = tmp_path / "kr_universe.csv"
    universe_csv.write_text(
        "ticker,market,sector,name\n005930,KOSPI,Technology,삼성전자\n",
        encoding="utf-8",
    )

    class DummyDartProvider:
        pass

    class DummyMacroProvider:
        pass

    monkeypatch.setattr("eit_market_data.kr.dart_provider.DartProvider", DummyDartProvider)
    monkeypatch.setattr("eit_market_data.kr.ecos_provider.EcosMacroProvider", DummyMacroProvider)
    providers = create_kr_providers(profile="ci_safe", universe_csv=universe_csv)

    assert type(providers["price_provider"]).__name__ == "FdrNaverPriceProvider"
    assert type(providers["sector_provider"]).__name__ == "SeedSectorProvider"
    assert type(providers["benchmark_provider"]).__name__ == "FdrBenchmarkProvider"


def test_create_kr_providers_official_falls_back_without_dart_and_ecos(monkeypatch) -> None:
    class DummyPykrxProvider:
        def __init__(self, official_only=True) -> None:
            self.official_only = official_only
            self._fundamental_provider = None

    monkeypatch.setattr(
        "eit_market_data.kr.dart_provider.DartProvider",
        lambda: (_ for _ in ()).throw(ValueError("missing dart key")),
    )
    monkeypatch.setattr(
        "eit_market_data.kr.ecos_provider.EcosMacroProvider",
        lambda: (_ for _ in ()).throw(ValueError("missing ecos key")),
    )
    monkeypatch.setattr("eit_market_data.kr.pykrx_provider.PykrxProvider", DummyPykrxProvider)

    providers = create_kr_providers(profile="official")

    assert type(providers["filing_provider"]).__name__ == "NullDartProvider"
    assert type(providers["macro_provider"]).__name__ == "NullMacroProvider"
    assert type(providers["price_provider"]).__name__ == "DummyPykrxProvider"


def test_seed_sector_provider_uses_universe_csv(tmp_path: Path) -> None:
    universe_csv = tmp_path / "kr_universe.csv"
    universe_csv.write_text(
        "ticker,market,sector,name\n005930,KOSPI,Technology,삼성전자\n",
        encoding="utf-8",
    )
    provider = SeedSectorProvider(universe_csv=universe_csv)

    sector_map = asyncio.run(provider.fetch_sector_map(["005930", "000660"]))

    assert sector_map["005930"] == "Technology"
    assert sector_map["000660"] == "General"


def test_seed_sector_provider_computes_sector_averages() -> None:
    class DummyFundamentalProvider:
        async def fetch_fundamentals(self, ticker, as_of, n_quarters=4):  # noqa: ANN001
            _ = (ticker, as_of, n_quarters)
            return FundamentalData(
                ticker="005930",
                quarters=[
                    QuarterlyFinancials(
                        fiscal_quarter="2024Q4",
                        report_date=date(2025, 3, 10),
                        revenue=100.0,
                        gross_profit=40.0,
                        operating_income=15.0,
                        net_income=10.0,
                        total_assets=200.0,
                        total_equity=100.0,
                        current_assets=80.0,
                        current_liabilities=40.0,
                        total_debt=50.0,
                        eps=2.0,
                    )
                ],
                last_close_price=80.0,
            )

    provider = SeedSectorProvider(fundamental_provider=DummyFundamentalProvider())

    averages = asyncio.run(
        provider.fetch_sector_averages("Technology", ["005930", "000660"], date(2025, 3, 31))
    )

    assert averages.sector == "Technology"
    assert averages.avg_metrics["pe_ttm"] == 10.0


def test_fdr_price_provider_normalizes_nav_data(monkeypatch) -> None:
    provider = FdrNaverPriceProvider()
    frame = pd.DataFrame(
        {
            "Open": [70000.0, 70500.0],
            "High": [71000.0, 71500.0],
            "Low": [69000.0, 70000.0],
            "Close": [70500.0, 71200.0],
            "Volume": [100.0, 200.0],
        },
        index=pd.to_datetime(["2025-03-28", "2025-03-31"]),
    )

    class DummyFdrModule:
        @staticmethod
        def DataReader(symbol, start, end):  # noqa: N802
            assert symbol == "NAVER:005930"
            _ = (start, end)
            return frame

    monkeypatch.setitem(__import__("sys").modules, "FinanceDataReader", DummyFdrModule())

    bars = asyncio.run(provider.fetch_prices("5930", date(2025, 3, 31), lookback_days=2))

    assert [bar.close for bar in bars] == [70500.0, 71200.0]
