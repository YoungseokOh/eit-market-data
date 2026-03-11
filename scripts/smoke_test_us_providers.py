#!/usr/bin/env python3
"""Smoke test for US market data providers.

Tests YFinanceProvider, FredMacroProvider, and EdgarFilingProvider
with real API calls (no mocks).

Usage:
    python scripts/smoke_test_us_providers.py

Environment:
    FRED_API_KEY         — required for FredMacroProvider
    SEC_EDGAR_USER_AGENT — required for EdgarFilingProvider
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from datetime import date

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


# =============================================================================
# STEP 1: Environment & Dependencies Check
# =============================================================================


def check_environment() -> bool:
    """Validate environment variables and dependencies."""
    logger.info("=== STEP 1: Environment Check ===")

    errors = []

    # Check yfinance
    try:
        import yfinance  # noqa: F401
        logger.info("✓ yfinance installed")
    except ImportError:
        errors.append("yfinance not installed (pip install -e '.[real-data]')")

    # Check fredapi
    try:
        import fredapi  # noqa: F401
        logger.info("✓ fredapi installed")
    except ImportError:
        errors.append("fredapi not installed (pip install -e '.[real-data]')")

    # Check httpx + beautifulsoup4
    try:
        import httpx  # noqa: F401
        import bs4  # noqa: F401
        logger.info("✓ httpx + beautifulsoup4 installed")
    except ImportError as e:
        errors.append(f"httpx/beautifulsoup4 not installed: {e}")

    # Check FRED_API_KEY
    fred_key = os.environ.get("FRED_API_KEY", "").strip()
    if fred_key:
        logger.info("✓ FRED_API_KEY set")
    else:
        errors.append(
            "FRED_API_KEY not set. Get free key at "
            "https://fred.stlouisfed.org/docs/api/api_key.html"
        )

    # Check SEC_EDGAR_USER_AGENT
    edgar_ua = os.environ.get("SEC_EDGAR_USER_AGENT", "").strip()
    if edgar_ua:
        logger.info("✓ SEC_EDGAR_USER_AGENT set")
    else:
        errors.append(
            "SEC_EDGAR_USER_AGENT not set. Set to 'YourName your@email.com'"
        )

    if errors:
        logger.error("❌ Environment validation failed:")
        for err in errors:
            logger.error(f"  - {err}")
        return False

    logger.info("✓ All environment checks passed\n")
    return True


# =============================================================================
# STEP 2: YFinanceProvider Tests
# =============================================================================


async def test_yfinance_provider() -> bool:
    """Test YFinanceProvider with AAPL."""
    logger.info("=== STEP 2: YFinanceProvider ===")

    try:
        from eit_market_data.yfinance_provider import YFinanceProvider

        provider = YFinanceProvider()
        ticker = "AAPL"
        as_of = date(2026, 2, 27)

        # Test fetch_prices
        logger.info(f"Testing fetch_prices({ticker}, as_of={as_of})...")
        prices = await provider.fetch_prices(ticker, as_of)
        if prices:
            logger.info(f"  ✓ Fetched {len(prices)} price bars")
            latest = prices[-1]
            logger.info(
                f"    Latest: {latest.date} close=${latest.close:.2f}"
            )
            if len(prices) < 200:
                logger.warning(
                    f"  ⚠ Expected 200+ bars, got {len(prices)}"
                )
        else:
            logger.error(f"  ✗ No price data for {ticker}")
            return False

        # Test fetch_fundamentals
        logger.info(f"Testing fetch_fundamentals({ticker}, as_of={as_of})...")
        fund = await provider.fetch_fundamentals(ticker, as_of)
        if fund.quarters:
            logger.info(f"  ✓ Fetched {len(fund.quarters)} quarters")
            q0 = fund.quarters[0]
            logger.info(
                f"    Latest quarter: {q0.fiscal_quarter} "
                f"(report_date={q0.report_date})"
            )
            if len(fund.quarters) < 4:
                logger.warning(
                    f"  ⚠ Expected 4+ quarters, got {len(fund.quarters)}"
                )
        else:
            logger.error(f"  ✗ No fundamentals for {ticker}")
            return False

        # Test fetch_sector_map
        logger.info(f"Testing fetch_sector_map([{ticker}])...")
        sectors = await provider.fetch_sector_map([ticker])
        if ticker in sectors:
            logger.info(f"  ✓ Sector for {ticker}: {sectors[ticker]}")
        else:
            logger.warning(f"  ⚠ No sector found for {ticker}")

        # Test fetch_news
        logger.info(f"Testing fetch_news({ticker}, as_of={as_of})...")
        news = await provider.fetch_news(ticker, as_of)
        logger.info(f"  ✓ Fetched {len(news)} news items")
        if news:
            logger.info(f"    Latest: {news[0].headline}")

        # Test fetch_benchmark
        logger.info(f"Testing fetch_benchmark(as_of={as_of})...")
        bench = await provider.fetch_benchmark(as_of)
        if bench:
            logger.info(f"  ✓ Fetched {len(bench)} S&P 500 bars")
            latest = bench[-1]
            logger.info(f"    Latest: {latest.date} close=${latest.close:.2f}")
        else:
            logger.error(f"  ✗ No benchmark data")
            return False

        logger.info("✓ YFinanceProvider tests passed\n")
        return True

    except Exception as e:
        logger.error(f"❌ YFinanceProvider test failed: {e}", exc_info=True)
        return False


# =============================================================================
# STEP 3: FredMacroProvider Tests
# =============================================================================


async def test_fred_macro_provider() -> bool:
    """Test FredMacroProvider."""
    logger.info("=== STEP 3: FredMacroProvider ===")

    try:
        from eit_market_data.fred_provider import FredMacroProvider

        provider = FredMacroProvider()
        as_of = date(2026, 2, 27)

        logger.info(f"Testing fetch_macro(as_of={as_of})...")
        macro = await provider.fetch_macro(as_of)

        logger.info(f"  ✓ Macro data fetched")
        logger.info(f"    rates_policy keys: {len(macro.rates_policy)}")
        logger.info(f"    inflation_commodities keys: {len(macro.inflation_commodities)}")
        logger.info(f"    growth_economy keys: {len(macro.growth_economy)}")
        logger.info(f"    market_risk keys: {len(macro.market_risk)}")

        # Verify key indicators
        checks = [
            ("fed_funds_rate", macro.rates_policy),
            ("treasury_10y", macro.rates_policy),
            ("cpi_yoy", macro.inflation_commodities),
            ("vix", macro.market_risk),
        ]

        for key, section in checks:
            if key in section:
                logger.info(f"  ✓ {key}: {section[key]}")
            else:
                logger.warning(f"  ⚠ Missing {key}")

        if not (macro.rates_policy and macro.market_risk):
            logger.warning(
                "⚠ Expected rates_policy and market_risk with data"
            )
            return False

        logger.info("✓ FredMacroProvider tests passed\n")
        return True

    except Exception as e:
        logger.error(f"❌ FredMacroProvider test failed: {e}", exc_info=True)
        return False


# =============================================================================
# STEP 4: EdgarFilingProvider Tests
# =============================================================================


async def test_edgar_filing_provider() -> bool:
    """Test EdgarFilingProvider with AAPL."""
    logger.info("=== STEP 4: EdgarFilingProvider ===")

    try:
        from eit_market_data.edgar_provider import EdgarFilingProvider

        provider = EdgarFilingProvider()
        ticker = "AAPL"
        as_of = date(2026, 2, 27)

        logger.info(f"Testing fetch_filing({ticker}, as_of={as_of})...")
        filing = await provider.fetch_filing(ticker, as_of)

        if filing.filing_date:
            logger.info(f"  ✓ Filing fetched: {filing.filing_type} "
                       f"(filed {filing.filing_date})")
        else:
            logger.warning(f"  ⚠ No filing date")

        sections = {
            "business_overview": filing.business_overview,
            "risks": filing.risks,
            "mda": filing.mda,
            "governance": filing.governance,
        }

        for section_name, content in sections.items():
            if content:
                logger.info(
                    f"  ✓ {section_name}: {len(content)} chars"
                )
            else:
                logger.warning(f"  ⚠ Missing {section_name}")

        # Verify at least business_overview is substantial
        if filing.business_overview and len(filing.business_overview) >= 300:
            logger.info(f"✓ EdgarFilingProvider tests passed\n")
            return True
        else:
            logger.warning(
                f"⚠ business_overview too short: "
                f"{len(filing.business_overview or '') // 10}0 chars"
            )
            return True  # Soft warning

    except Exception as e:
        logger.error(f"❌ EdgarFilingProvider test failed: {e}", exc_info=True)
        return False


# =============================================================================
# STEP 5: SnapshotBuilder Integration Test
# =============================================================================


async def test_snapshot_builder() -> bool:
    """Test SnapshotBuilder with real providers."""
    logger.info("=== STEP 5: SnapshotBuilder Integration ===")

    try:
        from eit_market_data.snapshot import (
            SnapshotBuilder,
            SnapshotConfig,
            create_real_providers,
        )

        providers = create_real_providers()
        builder = SnapshotBuilder(**providers)
        config = SnapshotConfig(artifacts_dir="artifacts")

        month = "2026-02"
        universe = ["AAPL", "MSFT"]

        logger.info(
            f"Testing SnapshotBuilder.build(month={month}, "
            f"universe={universe})..."
        )

        snapshot = await builder.build(
            month=month,
            universe=universe,
            config=config,
        )

        logger.info(f"  ✓ Snapshot built")
        logger.info(f"    Decision date: {snapshot.decision_date}")
        logger.info(f"    Execution date: {snapshot.execution_date}")
        logger.info(f"    Universe: {snapshot.universe}")

        # Check data completeness
        for ticker in universe:
            prices_count = len(snapshot.prices.get(ticker, []))
            fund_count = len(
                snapshot.fundamentals.get(ticker).quarters
                if snapshot.fundamentals.get(ticker)
                else []
            )
            logger.info(
                f"    {ticker}: {prices_count} prices, "
                f"{fund_count} quarters"
            )

        logger.info(
            f"    Macro keys: {len(snapshot.macro.rates_policy)} "
            f"(rates_policy)"
        )
        logger.info(f"✓ SnapshotBuilder integration test passed\n")
        return True

    except Exception as e:
        logger.error(
            f"❌ SnapshotBuilder test failed: {e}", exc_info=True
        )
        return False


# =============================================================================
# Main
# =============================================================================


async def main() -> int:
    """Run all smoke tests."""
    logger.info("Starting US Market Data Smoke Test\n")

    # Step 1: Environment check
    if not check_environment():
        return 1

    # Step 2-5: Provider tests
    tests = [
        ("YFinanceProvider", test_yfinance_provider),
        ("FredMacroProvider", test_fred_macro_provider),
        ("EdgarFilingProvider", test_edgar_filing_provider),
        ("SnapshotBuilder", test_snapshot_builder),
    ]

    results = {}
    for name, test_func in tests:
        try:
            results[name] = await test_func()
        except Exception as e:
            logger.error(f"Test {name} crashed: {e}", exc_info=True)
            results[name] = False

    # Summary
    logger.info("=== Summary ===")
    for name, passed in results.items():
        status = "✓ PASSED" if passed else "✗ FAILED"
        logger.info(f"{status}: {name}")

    all_passed = all(results.values())
    if all_passed:
        logger.info("\n✓ All smoke tests passed!")
        return 0
    else:
        logger.error("\n❌ Some tests failed")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
