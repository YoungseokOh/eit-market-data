# US Market Data Developer Guide

This guide explains how to use the US market data providers (`YFinanceProvider`, `FredMacroProvider`, `EdgarFilingProvider`) in `eit-market-data`.

## Overview

The US data stack is designed to be **point-in-time safe** (no look-ahead bias) and **modular** (use only what you need).

```
YFinanceProvider  → Prices, Fundamentals, Sectors, News, Benchmarks
     ↓
FredMacroProvider → Macro Indicators (21 series)
     ↓
EdgarFilingProvider → 10-K Filing Text (4 sections)
     ↓
SnapshotBuilder   → Unified Monthly Snapshot
```

---

## Installation

### Basic (Prices only, no API keys needed)

```bash
pip install yfinance
```

### Full (All US providers)

```bash
pip install -e '.[real-data]'
```

Or manually:

```bash
pip install yfinance fredapi httpx beautifulsoup4
```

---

## API Key Setup

### FRED_API_KEY (FredMacroProvider)

**Free registration required:**

1. Go to https://fred.stlouisfed.org/docs/api/api_key.html
2. Register with email
3. Copy your API key
4. Add to `.env`:

```bash
FRED_API_KEY=your_key_here
```

**Data available**: 21 macro indicators (interest rates, inflation, growth, market risk)

### SEC_EDGAR_USER_AGENT (EdgarFilingProvider)

**No registration required**, but SEC requires identification:

1. Choose any identifier: `"YourName your@email.com"`
2. Add to `.env`:

```bash
SEC_EDGAR_USER_AGENT="John Doe john@example.com"
```

**Data available**: 10-K annual filings with 4 text sections

---

## Usage

### 1. YFinanceProvider

```python
from eit_market_data.yfinance_provider import YFinanceProvider
from datetime import date

yf = YFinanceProvider()

# Prices (300 daily bars)
prices = await yf.fetch_prices("AAPL", as_of=date(2026, 2, 27))
# → [PriceBar(date, open, high, low, close, volume), ...]

# Fundamentals (4 quarters)
fund = await yf.fetch_fundamentals("AAPL", as_of=date(2026, 2, 27), n_quarters=8)
# → FundamentalData(
#     ticker="AAPL",
#     quarters=[
#       QuarterlyFinancials(
#         fiscal_quarter="2025Q3",
#         report_date=date(2025, 9, 30),
#         revenue=94.7,  # billions USD
#         net_income=21.2,
#         ...
#       ),
#       ...
#     ],
#     market_cap=2700.5,
#     last_close_price=264.18
#   )

# Sector mapping
sectors = await yf.fetch_sector_map(["AAPL", "MSFT"])
# → {"AAPL": "Technology", "MSFT": "Technology"}

# News (up to 15 items, 30-day lookback)
news = await yf.fetch_news("AAPL", as_of=date(2026, 2, 27))
# → [NewsItem(date, source, headline, summary), ...]

# S&P 500 Benchmark
benchmark = await yf.fetch_benchmark(as_of=date(2026, 2, 27))
# → [PriceBar(...), ...]  # ^GSPC prices
```

**Limitations**:
- `fetch_filing()` returns only `longBusinessSummary` from `.info` (stub)
- `fetch_macro()` returns empty `MacroData` (stub)
- News may be empty for quiet periods

### 2. FredMacroProvider

```python
from eit_market_data.fred_provider import FredMacroProvider
from datetime import date

fred = FredMacroProvider()

macro = await fred.fetch_macro(as_of=date(2026, 2, 27))
# → MacroData(
#     rates_policy={
#       "fed_funds_rate": 3.64,
#       "treasury_10y": 3.97,
#       "treasury_2y": 3.85,
#       "yield_curve_spread_10y_2y": 0.12,
#       "policy_stance": "neutral"  # "dovish" | "neutral" | "hawkish"
#     },
#     inflation_commodities={
#       "cpi_yoy": 2.1,           # year-over-year %
#       "ppi_yoy": 1.8,
#       "oil_wti": 82.45,          # USD/barrel
#       "copper": 4.25,            # USD/lb
#       "gold": 2142.50            # USD/oz (via yfinance GC=F)
#     },
#     growth_economy={
#       "gdp_growth_yoy": 2.3,     # annualized %
#       "unemployment_rate": 3.9,
#       "consumer_confidence": 71.4,
#       "nonfarm_payrolls_k": 156.0  # MoM change, thousands
#     },
#     market_risk={
#       "vix": 19.86,
#       "ig_credit_spread": 85,      # basis points
#       "hy_credit_spread": 312,
#       "sp500_level": 6878.88,
#       "sp500_monthly_return": 2.15  # %
#     }
#   )
```

**Data Series** (21 total):

| Category | Series | Notes |
|----------|--------|-------|
| **Rates** | Fed Funds Rate, 10Y, 2Y, Yield Curve | Latest value + spread |
| **Inflation** | CPI, PPI, WTI Oil, Copper, Gold | YoY % change + level |
| **Growth** | GDP, Unemployment, ISM Proxy, Payrolls, Sentiment | Mixed frequency |
| **Market** | VIX, IG/HY Spreads, S&P 500 Level | Daily data |

**Limitations**:
- ISM Manufacturing is proxied by `MANEMP` (employment) since `NAPM` was discontinued
- All values rounded to 1-4 decimal places
- Monthly/quarterly data reported on last available observation

### 3. EdgarFilingProvider

```python
from eit_market_data.edgar_provider import EdgarFilingProvider
from datetime import date

edgar = EdgarFilingProvider()

filing = await edgar.fetch_filing("AAPL", as_of=date(2026, 2, 27))
# → FilingData(
#     ticker="AAPL",
#     filing_date=date(2025, 10, 31),
#     filing_type="10-K",
#     business_overview="Apple Inc. designs, manufactures, and markets...",  # 8000 chars
#     risks="We face intense competition...",  # 8000 chars
#     mda="The following discussion...",  # 8000 chars
#     governance="The Board is divided into three classes..."  # variable
#   )
```

**Extraction**:
- **Item 1 (Business Overview)**: Company description
- **Item 1A (Risk Factors)**: Key risks
- **Item 7 (MD&A)**: Management discussion and analysis
- **Item 10/14 (Governance)**: Directors and corporate governance

**Limitations**:
- 10-K only (no 10-Q quarterly filings)
- Text is extracted from HTML; some formatting lost
- Max 8000 chars per section (truncated if longer)
- SEC EDGAR rate limit: 10 requests/sec (internally managed)

---

## Point-in-Time Safety

All `fetch_*` methods accept an `as_of: date` parameter to prevent look-ahead bias:

```python
# Decision made on 2026-02-27
as_of = date(2026, 2, 27)

# All data is filtered to ≤ as_of
prices = await yf.fetch_prices("AAPL", as_of=as_of)
# → Only bars with date ≤ 2026-02-27 are returned

fund = await yf.fetch_fundamentals("AAPL", as_of=as_of)
# → Quarters with report_date + 60-day filing lag ≤ 2026-02-27
# → Q3 (2025-09-30) available only if 60 days have passed

macro = await fred.fetch_macro(as_of=as_of)
# → FRED series observations ≤ 2026-02-27

filing = await edgar.fetch_filing("AAPL", as_of=as_of)
# → 10-K with filing_date ≤ 2026-02-27
```

**Filing Lag** (FundamentalProvider):
- Quarterly earnings typically announced 25-45 days after period end
- Provider uses **60-day buffer** to be conservative
- Example: Q3 (ends 2025-09-30) available as of 2025-11-29+

---

## Integration with SnapshotBuilder

Use `create_real_providers()` to wire up all three:

```python
from eit_market_data.snapshot import SnapshotBuilder, create_real_providers
from datetime import date

# Create all three providers at once
providers = create_real_providers()
# Returns {
#   "price_provider": YFinanceProvider(),
#   "fundamental_provider": YFinanceProvider(),
#   "filing_provider": EdgarFilingProvider(),
#   "news_provider": YFinanceProvider(),
#   "macro_provider": FredMacroProvider(),
#   "sector_provider": YFinanceProvider(),
#   "benchmark_provider": YFinanceProvider(),
# }

# Build snapshot
builder = SnapshotBuilder(**providers)
snapshot = await builder.build(
    month="2026-02",
    universe=["AAPL", "MSFT", "GOOGL"],
)

# Access data
for ticker in snapshot.universe:
    prices = snapshot.prices[ticker]  # 300 PriceBar
    fund = snapshot.fundamentals[ticker]  # FundamentalData
    filing = snapshot.filings[ticker]  # FilingData
    news = snapshot.news[ticker]  # list[NewsItem]

macro = snapshot.macro  # MacroData with 21 indicators
sectors = snapshot.sector_map  # {"AAPL": "Technology", ...}
```

---

## Error Handling

Providers follow **fail-safe** strategy: return empty/default values instead of raising:

```python
# API error → empty data, logged warning
prices = await yf.fetch_prices("INVALID", as_of=date(2026, 2, 27))
# → [] (empty list), WARNING logged

# Missing fundamental data → partial quarters
fund = await yf.fetch_fundamentals("PENNY", as_of=date(2026, 2, 27))
# → FundamentalData(ticker="PENNY", quarters=[], market_cap=None)

# Missing EDGAR filing → empty sections
filing = await edgar.fetch_filing("UNKNOWN", as_of=date(2026, 2, 27))
# → FilingData(ticker="UNKNOWN", filing_date=None, business_overview=None, ...)
```

**Always check output for None/empty** before using in analysis.

---

## Testing

### Smoke Test (recommended before use)

```bash
python scripts/smoke_test_us_providers.py
```

This verifies:
- Environment setup (API keys, dependencies)
- Each provider works with sample data (AAPL, MSFT)
- Data meets verification criteria
- SnapshotBuilder integration works

### Unit Testing

```python
import pytest
from datetime import date
from eit_market_data.yfinance_provider import YFinanceProvider

@pytest.mark.asyncio
async def test_yfinance_prices():
    yf = YFinanceProvider()
    prices = await yf.fetch_prices("AAPL", as_of=date(2026, 2, 27))
    assert len(prices) > 200  # At least 200 trading days
    assert prices[-1].date == date(2026, 2, 27)
```

---

## Performance Tips

1. **Caching**: Results are cached in `.cache/` by default (diskcache)
   - Clear with `rm -rf .cache/eit_market_data`

2. **Concurrency**: Providers use `asyncio.Semaphore` to avoid rate limits
   - YFinance: max 3 concurrent
   - EDGAR: max 5 concurrent (well under SEC limit of 10/sec)

3. **Batch Mode**: Use `asyncio.gather()` to fetch multiple tickers:
   ```python
   tickers = ["AAPL", "MSFT", "GOOGL"]
   prices = await asyncio.gather(
       *[yf.fetch_prices(t, as_of) for t in tickers]
   )
   ```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `ImportError: No module named 'yfinance'` | `pip install -e '.[real-data]'` |
| `FRED_API_KEY not set` | Get free key from fred.stlouisfed.org, add to `.env` |
| `SEC_EDGAR_USER_AGENT not set` | Set env var to `"Name email"` format |
| `No price data for TICKER` | Check ticker spelling (case-insensitive) |
| `EdgarFilingProvider returns empty FilingData` | No 10-K filed before `as_of` date |
| `FredMacroProvider returns 0 keys` | Check FRED_API_KEY is valid |
| API rate limit hit | Wait 1 minute; providers have built-in backoff |

---

## Next Steps for eit-research

1. Install: `pip install -e '../eit-market-data[all]'`
2. Test: `python ../eit-market-data/scripts/smoke_test_us_providers.py`
3. Integrate: Use `create_real_providers()` in your pipeline
4. Build snapshots: `eit build-snapshot 2026-02 --market us`
5. Backtest with US data alongside KR data

See [smoke-test-results.md](smoke-test-results.md) for verification details.
