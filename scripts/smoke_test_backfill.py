"""Smoke test for backfill_all.py — run before full overnight crawl.

Tests:
  1. pykrx 전종목 ticker list 조회
  2. Phase 1 mini — 3종목 OHLCV + 1개월 cap/fundamental
  3. Phase 3 mini — KR snapshot 1개월, 5종목 (ci_safe)
  4. Phase 4 mini — US snapshot 1개월, 5종목

Usage:
    python scripts/smoke_test_backfill.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import time
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from dotenv import load_dotenv

load_dotenv(PROJECT_ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("smoke_test")

SMOKE_OUT = PROJECT_ROOT / "data" / "smoke_test"
SMOKE_ARTIFACTS = PROJECT_ROOT / "artifacts" / "smoke_test"

SAMPLE_KR = ["005930", "000660", "035420", "005380", "207940"]  # 삼전, SK하이닉스, NAVER, 현대차, 삼바
SAMPLE_US = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"]
SMOKE_MONTH = "2026-01"  # 완성된 최근 월

PASS = "✅"
FAIL = "❌"
SKIP = "⏭️"

results: dict[str, str] = {}


def _mark(name: str, ok: bool, detail: str = "") -> None:
    icon = PASS if ok else FAIL
    results[name] = icon
    level = logging.INFO if ok else logging.ERROR
    logger.log(level, "%s %s %s", icon, name, detail)


# ---------------------------------------------------------------------------
# Test 1: pykrx 전종목 ticker list
# ---------------------------------------------------------------------------

def test_pykrx_ticker_list() -> bool:
    logger.info("=" * 50)
    logger.info("Test 1: 전종목 ticker list (FDR StockListing -DESC)")
    try:
        import FinanceDataReader as fdr

        kospi_df = fdr.StockListing("KOSPI-DESC")
        kosdaq_df = fdr.StockListing("KOSDAQ-DESC")
        kospi = kospi_df["Code"].dropna().tolist() if kospi_df is not None else []
        kosdaq = kosdaq_df["Code"].dropna().tolist() if kosdaq_df is not None else []
        total = len(kospi) + len(kosdaq)
        _mark("ticker list (FDR-DESC)", total > 500, f"KOSPI={len(kospi)} KOSDAQ={len(kosdaq)} total={total}")
        return total > 500
    except Exception as e:
        _mark("ticker list (FDR-DESC)", False, str(e))
        return False


# ---------------------------------------------------------------------------
# Test 2: Phase 1 mini — OHLCV + 1개월 cap
# ---------------------------------------------------------------------------

def test_phase1_mini() -> bool:
    logger.info("=" * 50)
    logger.info("Test 2: Phase 1 mini (3종목 OHLCV via FDR)")
    try:
        from datetime import date as date_

        from eit_market_data.kr.market_helpers import fetch_stock_ohlcv_frame

        SMOKE_OUT.mkdir(parents=True, exist_ok=True)
        start = date_(2026, 1, 1)
        end = date_(2026, 1, 31)

        ok_count = 0
        for ticker in SAMPLE_KR[:3]:
            df, source = fetch_stock_ohlcv_frame(ticker, start, end)
            time.sleep(0.3)
            if df is not None and not df.empty:
                out = SMOKE_OUT / f"ohlcv_{ticker}.parquet"
                df.to_parquet(out)
                ok_count += 1
                logger.info("  OHLCV %s: %d rows (source=%s)", ticker, len(df), source)

        ok = ok_count >= 2
        _mark("phase1 mini (FDR)", ok, f"ohlcv={ok_count}/3")
        return ok
    except Exception as e:
        _mark("phase1 mini (FDR)", False, str(e))
        return False


# ---------------------------------------------------------------------------
# Test 3: DART provider 초기화 (연결 테스트 1회)
# ---------------------------------------------------------------------------

def test_dart_init() -> bool:
    logger.info("=" * 50)
    logger.info("Test 3: DART provider 초기화")
    try:
        from eit_market_data.kr.dart_provider import DartProvider
        dart = DartProvider()
        _mark("dart init", True, "DartProvider 초기화 성공")
        return True
    except ValueError as e:
        _mark("dart init", False, f"DART_API_KEY 없음: {e}")
        return False
    except ImportError as e:
        _mark("dart init", False, f"opendartreader 없음: {e}")
        return False


# ---------------------------------------------------------------------------
# Test 4: Phase 3 mini — KR snapshot 1개월 5종목
# ---------------------------------------------------------------------------

async def test_phase3_kr_mini() -> bool:
    logger.info("=" * 50)
    logger.info("Test 4: Phase 3 mini — KR snapshot %s, %d tickers", SMOKE_MONTH, len(SAMPLE_KR))
    try:
        from eit_market_data.snapshot import SnapshotBuilder, SnapshotConfig, create_kr_providers

        SMOKE_ARTIFACTS.mkdir(parents=True, exist_ok=True)

        import importlib.util
        _spec = importlib.util.spec_from_file_location("backfill_all", PROJECT_ROOT / "scripts" / "backfill_all.py")
        _mod = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_mod)
        backfill_dart = _mod.BackfillDartProvider(PROJECT_ROOT / "data" / "backfill" / "dart")

        builder = SnapshotBuilder(
            **create_kr_providers(
                profile="ci_safe",
                universe_csv=PROJECT_ROOT / "universes" / "kr_universe.csv",
                dart_override=backfill_dart,
            )
        )
        config = SnapshotConfig(artifacts_dir=str(SMOKE_ARTIFACTS / "kr"))
        snapshot = await builder.build_and_persist(SMOKE_MONTH, SAMPLE_KR, config)

        n_prices = sum(1 for bars in snapshot.prices.values() if bars)
        n_funds = sum(1 for f in snapshot.fundamentals.values() if f.quarters)
        _mark(
            "phase3 kr mini",
            n_prices >= 3,
            f"prices={n_prices}/5 fundamentals={n_funds}/5 decision_date={snapshot.decision_date}",
        )
        return n_prices >= 3
    except Exception as e:
        _mark("phase3 kr mini", False, str(e))
        logger.exception("phase3 kr mini failed")
        return False


# ---------------------------------------------------------------------------
# Test 5: Phase 4 mini — US snapshot 1개월 5종목
# ---------------------------------------------------------------------------

async def test_phase4_us_mini() -> bool:
    logger.info("=" * 50)
    logger.info("Test 5: Phase 4 mini — US snapshot %s, %d tickers", SMOKE_MONTH, len(SAMPLE_US))
    try:
        from eit_market_data.snapshot import SnapshotBuilder, SnapshotConfig, create_real_providers

        SMOKE_ARTIFACTS.mkdir(parents=True, exist_ok=True)

        providers = create_real_providers()
        builder = SnapshotBuilder(**providers)
        config = SnapshotConfig(artifacts_dir=str(SMOKE_ARTIFACTS / "us"))
        snapshot = await builder.build_and_persist(SMOKE_MONTH, SAMPLE_US, config)

        n_prices = sum(1 for bars in snapshot.prices.values() if bars)
        n_funds = sum(1 for f in snapshot.fundamentals.values() if f.quarters)
        n_macro = len(snapshot.macro.rates_policy) + len(snapshot.macro.growth_economy)
        _mark(
            "phase4 us mini",
            n_prices >= 4,
            f"prices={n_prices}/5 fundamentals={n_funds}/5 macro_keys={n_macro}",
        )
        return n_prices >= 4
    except Exception as e:
        _mark("phase4 us mini", False, str(e))
        logger.exception("phase4 us mini failed")
        return False


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def main() -> None:
    logger.info("=" * 50)
    logger.info("Backfill Smoke Test 시작")
    logger.info("=" * 50)

    t1 = test_pykrx_ticker_list()
    t2 = test_phase1_mini() if t1 else (results.update({"phase1 mini": SKIP}), False)[1]
    t3 = test_dart_init()
    t4, t5 = asyncio.run(_run_async())

    print("\n" + "=" * 50)
    print("Smoke Test 결과")
    print("=" * 50)
    all_ok = True
    for name, icon in results.items():
        print(f"  {icon}  {name}")
        if icon == FAIL:
            all_ok = False

    print("=" * 50)
    if all_ok:
        print(f"\n{PASS} 모든 테스트 통과 — 본 백필 실행 가능합니다.")
        print("\n실행 명령:")
        print("  nohup ./run_crawling.sh > /dev/null 2>&1 &")
        sys.exit(0)
    else:
        failed = [n for n, v in results.items() if v == FAIL]
        print(f"\n{FAIL} 실패 항목: {', '.join(failed)}")
        print("실패 원인 확인 후 본 백필을 실행하세요.")
        sys.exit(1)


async def _run_async() -> tuple[bool, bool]:
    t4 = await test_phase3_kr_mini()
    t5 = await test_phase4_us_mini()
    return t4, t5


if __name__ == "__main__":
    main()
