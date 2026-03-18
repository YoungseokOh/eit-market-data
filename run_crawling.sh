#!/bin/bash
# Full historical backfill — nohup ./run_crawling.sh &
#
# Usage:
#   ./run_crawling.sh          # auto: detect which phases are needed and run them
#   ./run_crawling.sh us       # US snapshots only (phase 4)
#   ./run_crawling.sh kr       # KR snapshots only (phase 3)
#   ./run_crawling.sh 3 4      # explicit phase numbers
#
# Auto mode (no argument):
#   Checks completion state of each phase and only runs incomplete ones.
#   Phase 1 (pykrx)  — skipped if data/backfill/pykrx/market/ohlcv/ has files
#   Phase 2 (DART)   — skipped if data/backfill/dart/_progress.json shows completed≥2000
#   Phase 3 (KR snap)— always included (monthly snapshot builder skips existing months)
#   Phase 4 (US snap) — always included (monthly snapshot builder skips existing months)
set -euo pipefail
cd "$(dirname "$0")"

LOG="logs/crawling_$(date +%Y%m%d_%H%M%S).log"
mkdir -p logs

# --- Determine which phases to run ---
PHASE_ARGS=()

case "${1:-auto}" in
  auto)
    PHASES=()

    # Phase 1: skip if pykrx OHLCV files already exist
    OHLCV_COUNT=$(find data/backfill/pykrx/market/ohlcv -name "*.parquet" 2>/dev/null | wc -l || true)
    if [[ "$OHLCV_COUNT" -lt 100 ]]; then
      echo "[auto] Phase 1: pykrx OHLCV not done (${OHLCV_COUNT} parquet files) — will run"
      PHASES+=(1)
    else
      echo "[auto] Phase 1: DONE (${OHLCV_COUNT} parquet files) — skipping"
    fi

    # Phase 2: skip if DART progress shows ≥2000 completed tickers
    DART_DONE=0
    if [[ -f data/backfill/dart/_progress.json ]]; then
      DART_DONE=$(python3 -c "import json; d=json.load(open('data/backfill/dart/_progress.json')); print(len(d.get('completed', [])))" 2>/dev/null || echo 0)
    fi
    if [[ "$DART_DONE" -lt 2000 ]]; then
      echo "[auto] Phase 2: DART not done (${DART_DONE} tickers) — will run"
      PHASES+=(2)
    else
      echo "[auto] Phase 2: DONE (${DART_DONE} tickers) — skipping"
    fi

    # Phase 3: KR snapshots — always run (builder skips existing months internally)
    KR_DONE=$(find artifacts/kr/snapshots -name "snapshot.json" 2>/dev/null | wc -l || true)
    echo "[auto] Phase 3: KR snapshots (${KR_DONE} months done) — will run remaining"
    PHASES+=(3)

    # Phase 4: US snapshots — always run (builder skips existing months internally)
    US_DONE=$(find artifacts/us/snapshots -name "snapshot.json" 2>/dev/null | wc -l || true)
    echo "[auto] Phase 4: US snapshots (${US_DONE} months done) — will run remaining"
    PHASES+=(4)

    if [[ ${#PHASES[@]} -gt 0 ]]; then
      PHASE_ARGS=(--phase "${PHASES[@]}")
    fi
    ;;
  us)   PHASE_ARGS=(--phase 4) ;;
  kr)   PHASE_ARGS=(--phase 3) ;;
  *)    PHASE_ARGS=(--phase "$@") ;;
esac

echo "=== Backfill started at $(date) ==="
echo "Log: $LOG"

python scripts/backfill_all.py \
  --start 2022-01 --end 2026-03 \
  "${PHASE_ARGS[@]}" \
  2>&1 | tee "$LOG"

echo "=== Backfill finished at $(date) ==="
