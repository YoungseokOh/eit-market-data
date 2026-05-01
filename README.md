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

KR 공식 스냅샷 빌드는 `official` 프로필 기준으로 pykrx(공식 KRX 경로)에서
가격/마켓/섹터/벤치마크를 우선 조회합니다.  
재무는 DART, 매크로는 ECOS에서 보강되며, FDR(`FinanceDataReader`)은
정상 경로가 아니라 주로 진단·비상 대체/CI-safe 경로로 사용됩니다.

preflight를 실행합니다:

```bash
python scripts/preflight_kr_data.py --as-of 2026-03-06 --ticker 005930
```

This checks:
- WSL2 detection and `/etc/resolv.conf`
- DNS resolution for KRX/Naver/ECOS
- KR 공개/공인 경로 health-check checks (가격/티커/마켓캡/지수/섹터)
- DART fundamentals
- ECOS macro coverage

KRX 로그인 관련 스크립트(`scripts/krx_login.py`, `scripts/probe_fdr_krx_session.py`)는
기본 런타임이 아니라 수동 진단/실험용으로만 남아 있습니다:

```bash
python scripts/krx_login.py
python scripts/probe_fdr_krx_session.py
```

기본 쿠키 저장 위치:

```text
~/.cache/eit-market-data/krx-profile/cookies.json
```

세션 쿠키는 인증 정보이므로 저장소에 커밋하면 안 됩니다. WSL2에서는 Windows에서 생성한
쿠키 파일을 그대로 재사용할 수 있고, `scripts/auto_shell.sh`는
`/mnt/c/Users/$USER/.cache/eit-market-data/krx-profile/cookies.json` 이 있으면
`EIT_KRX_COOKIE_PATH` 와 `EIT_KRX_PROFILE_DIR` 를 자동으로 맞춥니다.

Windows에서 이 repo를 직접 열어 로그인과 probe를 한 번에 실행하려면:

```powershell
scripts\windows_krx_setup_and_probe.cmd
```

히스토리컬 KR raw backfill이 필요하면 기본적으로 공식 경로(pykrx)를 우선 사용합니다.
필요할 때만 FnGuide 레거시 경로를 병행합니다.

```bash
# 기본 수집(권장)
python scripts/crawl_kr_data_pykrx.py --start 2025-01-01 --end 2026-03-12 --output-root data

# 보완용(필요 시)
python scripts/crawl_kr_data_fallback.py --start 2025-01-01 --end 2026-03-12
```

`crawl_kr_data_pykrx.py`는 공식 경로 재현/정규 수집 경로이고,  
`crawl_kr_data_fallback.py`는 FnGuide 기반 legacy 보완 경로입니다.
`scripts/crawl_kr_data.py`는 인증 기반 복구/디버깅 경로로만 유지됩니다.

이 스크립트는 다음을 생성합니다:

- `data/market/cap_daily/*.parquet`
- `data/market/fundamental/*.parquet`
- `data/index/ohlcv/*.parquet`
- `data/market/sector/*.parquet`

월말 snapshot 재현은 먼저 `data/market/cap_daily/`를 같은 월말 거래일 기준으로 채웁니다.
`scripts/fill_kr_cap_daily_gap.py`는 보조 보정 경로이며, 권장 경로는
`crawl_kr_data_pykrx.py`(기본) 또는 필요 시 legacy `crawl_kr_data_fallback.py` 병행입니다.

## KR News Diagnostics

뉴스 크롤링 코드는 저장소에 남아 있지만, 현재 MVP 기본 KR bundle 경로에는 포함되지 않습니다.
즉 `scripts/run_local_collection.py`와 기본 snapshot build는 가격/재무/공시/매크로/섹터/벤치마크 중심으로
bundle을 만들고, 새 bundle에는 `snapshot.news`와 `news_coverage.json`이 기본적으로 없습니다.

`scripts/capture_kr_news_catalog.py`와 `scripts/preflight_kr_data.py`의 뉴스 경로는
legacy diagnostic 용도로만 유지됩니다.

## WSL2 Notes

- Apply the known-good DNS config with `scripts/apply_wsl_dns_config.sh`
- Run `wsl --shutdown` from Windows after changing `/etc/wsl.conf`
- `.bashrc` sources `scripts/auto_shell.sh`, which activates `.venv` and loads `.env`
- WSL2에서는 Windows에서 만든 KRX 쿠키를 `/mnt/c/Users/$USER/.cache/eit-market-data/krx-profile/cookies.json` 경로로 재사용할 수 있음

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
  --pattern '*kr*' --dir ../eit-market-data/out/<run>/artifacts/snapshots/

# Download latest US snapshot
gh release download $(gh release list | head -1 | awk '{print $1}') \
  --pattern '*us*' --dir ../eit-market-data/out/<run>/artifacts/snapshots/
```

Then use directly:

```bash
eit build-snapshot 2026-03 --market kr --bundle-dir ../eit-market-data/artifacts/kr/snapshots
eit build-snapshot 2026-03 --market us --bundle-dir ../eit-market-data/artifacts/us/snapshots
```

### Manual Build (Local Development)

Build KR snapshot locally:

```bash
python scripts/build_kr_snapshot.py --as-of 2026-03-31 --profile official --force
```

For historical months, backfill `cap_daily` with the official collector first:

```bash
python scripts/crawl_kr_data_pykrx.py --start 2025-01-01 --end 2026-03-31 --output-root data

# only if official collector cannot complete required range
python scripts/crawl_kr_data_fallback.py --start 2025-01-01 --end 2026-03-31
python scripts/build_kr_snapshot.py --as-of 2025-12-31 --profile official --force
```

`run_crawling.sh` supports explicit control of delay/log settings for stable batch runs:

```bash
DRY_RUN=1 \
  START_MONTH=<YYYY-MM> END_MONTH=<YYYY-MM> \
  LOG_DIR=logs CRAWL_DELAY_SECONDS=0.8 DART_DELAY_SECONDS=2.5 \
  MAIN_PHASE_DELAY_SECONDS=1 MIN_CAP_DAILY_FILES=100 CAP_DAILY_THRESHOLD=90 \
  RUN_ID=$(date +%Y%m%d_%H%M%S) ./run_crawling.sh us
```

The command writes a per-run log at:

```text
logs/crawling_${RUN_ID}.log
```

`MIN_CAP_DAILY_FILES` controls the auto-mode cap file threshold; `CAP_DAILY_THRESHOLD` controls whether phase-1 official crawl is forced before US/KR snapshot phases.
`PHASE_TIMEOUT_SECONDS` can be set to a positive value (seconds) to hard-stop a stalled phase.
`BASE_MONTH`는 기준월(기본: 직전 달)을 덮어쓰고, `LOOKBACK_MONTHS`는 기본으로 처리할 개월 수를 정합니다(기본 `12`).
`START_MONTH`/`END_MONTH`를 모두 생략하면 `END_MONTH=BASE_MONTH`, `START_MONTH=END_MONTH - LOOKBACK_MONTHS + 1`로 자동 계산합니다.

Build US snapshot locally (requires `FRED_API_KEY`, `SEC_EDGAR_USER_AGENT`):

```bash
python scripts/build_us_snapshot.py --as-of 2026-02-27 --universe AAPL,MSFT,GOOGL
```

Or both together (KR + US):

```bash
python scripts/run_daily_batch.py --as-of 2026-02-27
```

For multi-year historical backfills, `scripts/backfill_all.py` caches 32 DART quarters per ticker by default
(`--dart-quarters`) so early replay months have point-in-time financial statements.
`./run_crawling.sh kr` and auto mode also run the pykrx crawler first when historical
`data/market/cap_daily/` coverage is incomplete.
Override `START_MONTH`/`END_MONTH` to bound long-range replay runs, e.g.
`START_MONTH=2023-05 END_MONTH=2026-03 ./run_crawling.sh auto`.

Output files are written under:

- `artifacts/kr/snapshots/YYYY-MM/`
- `artifacts/us/snapshots/YYYY-MM/`

(`run_daily_batch.py` writes to `out/<run>/artifacts/snapshots/YYYY-MM/` for CI batch artifacts.)

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
- Current scheduled batch scope is KR preflight + KR official pykrx crawl + KR/US snapshot build
