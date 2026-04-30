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

START_MONTH=${START_MONTH:-2022-01}
END_MONTH=${END_MONTH:-2026-03}
START_DATE=${START_DATE:-${START_MONTH}-01}
END_DATE=${END_DATE:-$(python3 -c 'from datetime import date, timedelta; import sys; y, m = map(int, sys.argv[1].split("-")); n = date(y + (m // 12), (m % 12) + 1, 1); print((n - timedelta(days=1)).isoformat())' "$END_MONTH")}

LOG="logs/crawling_$(date +%Y%m%d_%H%M%S).log"
mkdir -p logs

# --- Determine which phases to run ---
PHASE_ARGS=()
RUN_KR_FALLBACK=0

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

    # Phase 2: skip if DART progress shows ≥2000 completed tickers at the
    # historical replay quarter depth.
    DART_DONE=0
    DART_QUARTERS=0
    if [[ -f data/backfill/dart/_progress.json ]]; then
      DART_DONE=$(python3 -c "import json; d=json.load(open('data/backfill/dart/_progress.json')); print(len(d.get('completed', [])))" 2>/dev/null || echo 0)
      DART_QUARTERS=$(python3 -c "import json; d=json.load(open('data/backfill/dart/_progress.json')); print(int(d.get('n_quarters') or 0))" 2>/dev/null || echo 0)
    fi
    if [[ "$DART_DONE" -lt 2000 || "$DART_QUARTERS" -lt 32 ]]; then
      echo "[auto] Phase 2: DART not done (${DART_DONE} tickers, quarters=${DART_QUARTERS}) — will run"
      PHASES+=(2)
    else
      echo "[auto] Phase 2: DONE (${DART_DONE} tickers, quarters=${DART_QUARTERS}) — skipping"
    fi

    # Phase 3: KR snapshots — always run (builder skips existing months internally)
    KR_DONE=$(find artifacts/kr/snapshots -name "snapshot.json" 2>/dev/null | wc -l || true)
    echo "[auto] Phase 3: KR snapshots (${KR_DONE} months done) — will run remaining"
    PHASES+=(3)
    RUN_KR_FALLBACK=1

    # Phase 4: US snapshots — always run (builder skips existing months internally)
    US_DONE=$(find artifacts/us/snapshots -name "snapshot.json" 2>/dev/null | wc -l || true)
    echo "[auto] Phase 4: US snapshots (${US_DONE} months done) — will run remaining"
    PHASES+=(4)

    if [[ ${#PHASES[@]} -gt 0 ]]; then
      PHASE_ARGS=(--phase "${PHASES[@]}")
    fi
    ;;
  us)   PHASE_ARGS=(--phase 4) ;;
  kr)   PHASE_ARGS=(--phase 2 3); RUN_KR_FALLBACK=1 ;;
  *)
    HAS_PHASE2=0
    HAS_PHASE3=0
    for phase in "$@"; do
      if [[ "$phase" == "2" ]]; then
        HAS_PHASE2=1
      fi
      if [[ "$phase" == "3" ]]; then
        RUN_KR_FALLBACK=1
        HAS_PHASE3=1
      fi
    done
    if [[ "$HAS_PHASE3" == "1" && "$HAS_PHASE2" == "0" ]]; then
      PHASE_ARGS=(--phase 2 "$@")
    else
      PHASE_ARGS=(--phase "$@")
    fi
    ;;
esac

echo "=== Backfill started at $(date) ==="
echo "Log: $LOG"

# Unbuffered Python output so log lines appear in tee immediately
export PYTHONUNBUFFERED=1

if [[ "$RUN_KR_FALLBACK" == "1" ]]; then
  CAP_DAILY_COUNT=$(find data/market/cap_daily -name "*.parquet" 2>/dev/null | wc -l || true)
  if [[ "$CAP_DAILY_COUNT" -lt 90 ]]; then
    echo "[auto] KR cap_daily incomplete (${CAP_DAILY_COUNT} parquet files) — running fallback crawler"
python scripts/crawl_kr_data_fallback.py \
      --start "$START_DATE" --end "$END_DATE" \
      2>&1 | tee -a "$LOG"
    RC=${PIPESTATUS[0]}
    if [[ "$RC" -ne 0 ]]; then
      echo "=== KR fallback crawl failed at $(date) (exit code $RC) ==="
      exit "$RC"
    fi
  else
    echo "[auto] KR cap_daily present (${CAP_DAILY_COUNT} parquet files) — skipping fallback crawler"
  fi
fi

python scripts/backfill_all.py \
  --start "$START_MONTH" --end "$END_MONTH" \
  "${PHASE_ARGS[@]}" \
  2>&1 | tee "$LOG"

RC=${PIPESTATUS[0]}
echo "=== Backfill finished at $(date) (exit code $RC) ==="
exit $RC
