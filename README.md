# eit-market-data

Standalone point-in-time market data layer for EIT.

## Setup

```bash
uv sync --extra all --extra dev
```

If you are not using `uv`, install the package in editable mode:

```bash
pip install -e '.[all,dev]'
```

## KR Preflight

KRX 공식 지수/시장 데이터는 2026년 KRX Data Marketplace 로그인 정책 변경 이후
브라우저 로그인 세션이 필요합니다. 먼저 로컬에서 한 번 로그인 세션을 생성하세요:

```bash
python scripts/krx_login.py
```

기본 쿠키 저장 위치:

```text
~/.cache/eit-market-data/krx-profile/cookies.json
```

그 다음 preflight를 실행합니다:

```bash
python scripts/preflight_kr_data.py --as-of 2026-03-06 --ticker 005930
```

This checks:
- WSL2 detection and `/etc/resolv.conf`
- DNS resolution for KRX/Naver/ECOS
- KRX authenticated session health for official index/market-wide endpoints
- `pykrx` price, benchmark, and sector lookup
- DART fundamentals
- ECOS macro coverage

## WSL2 Notes

- Apply the known-good DNS config with `scripts/apply_wsl_dns_config.sh`
- Run `wsl --shutdown` from Windows after changing `/etc/wsl.conf`
- `.bashrc` sources `scripts/auto_shell.sh`, which activates `.venv` and loads `.env`

See [docs/wsl2-runbook.md](docs/wsl2-runbook.md) for the full runbook.

## Docs

- [docs/api-keys.md](docs/api-keys.md)
- [docs/eit-research-data-requirements.md](docs/eit-research-data-requirements.md)
- [docs/wsl2-runbook.md](docs/wsl2-runbook.md)

## Automated Snapshot Generation

### For `eit-research` (Recommended)

**GitHub Actions automatically generates KR + US snapshots monthly.**

Download pre-built snapshots from GitHub releases (no local build needed):

```bash
# Download latest KR snapshot
gh release download $(gh release list | head -1 | awk '{print $1}') \
  --pattern '*kr*' --dir ../eit-market-data/artifacts/snapshots/

# Download latest US snapshot
gh release download $(gh release list | head -1 | awk '{print $1}') \
  --pattern '*us*' --dir ../eit-market-data/artifacts/snapshots/
```

Then use directly:

```bash
eit build-snapshot 2026-03 --market kr --bundle-dir ../eit-market-data/artifacts/snapshots
eit build-snapshot 2026-03 --market us --bundle-dir ../eit-market-data/artifacts/snapshots
```

### Manual Build (Local Development)

Build KR snapshot locally:

```bash
python scripts/build_kr_snapshot.py --as-of 2026-03-31 --profile ci_safe --force
```

Build US snapshot locally (requires `FRED_API_KEY`, `SEC_EDGAR_USER_AGENT`):

```bash
python scripts/build_us_snapshot.py --as-of 2026-02-27 --universe AAPL,MSFT,GOOGL
```

Or both together (KR + US):

```bash
python scripts/run_daily_batch.py --as-of 2026-02-27
```

Output files are written under `artifacts/snapshots/YYYY-MM/`:

- `snapshot.json` — Point-in-time snapshot data
- `metadata.json` — Provider metadata and verification info
- `manifest.json` — File manifest for loading
- `summary.json` — Build summary and statistics

## US Market Data

### Quick Start

**Setup** (one-time):

```bash
# Copy template and fill in your keys
cp .env.example .env
```

**Requirements**:
- `FRED_API_KEY` (free from https://fred.stlouisfed.org/docs/api/api_key.html)
- `SEC_EDGAR_USER_AGENT` (format: "Name your@email.com", no registration needed)

**Installation**:

```bash
pip install -e '.[real-data]'  # US providers only
pip install -e '.[all]'        # KR + US providers
```

**Smoke Test** (verify your setup):

```bash
python scripts/smoke_test_us_providers.py
```

**Usage**:

```python
from eit_market_data.snapshot import SnapshotBuilder, create_real_providers
import asyncio

providers = create_real_providers()  # YFinance + FRED + EDGAR
builder = SnapshotBuilder(**providers)

snapshot = await builder.build(
    month="2026-02",
    universe=["AAPL", "MSFT", "GOOGL"]
)
```

**Data Coverage** (as of 2026-02-27):
- **Prices**: 300 daily OHLCV bars per ticker
- **Fundamentals**: 4 quarters (income, balance sheet, cash flow)
- **Macro**: 21 indicators (rates, inflation, growth, market risk)
- **Filings**: 10-K text (business, risks, MD&A, governance)
- **News**: Up to 15 items (30-day window)
- **Sectors & Benchmarks**: S&P 500, NASDAQ-100

See [docs/us-developer-guide.md](docs/us-developer-guide.md) for provider details and point-in-time filtering.

## GitHub Actions Automation

- Scheduled workflow: `.github/workflows/daily-market-data.yml`
- Cron: `30 22 * * 0-4` (UTC), which is `07:30 Asia/Seoul` on weekdays
- Current daily batch entrypoint: `python scripts/run_daily_batch.py`
- Batch artifacts are written under `out/<as_of>_<timestamp>/` and uploaded as GitHub Actions artifacts
- Current scheduled batch scope is KR preflight + KR crawl + month-end KR snapshot build
