#!/bin/bash
# Full historical backfill — nohup ./run_crawling.sh &
#
# Usage:
#   ./run_crawling.sh          # auto: detect which phases are needed and run them
#   ./run_crawling.sh us       # US snapshots only (phase 4)
#   ./run_crawling.sh kr       # KR snapshots only (phase 3)
#   ./run_crawling.sh 3 4      # explicit phase numbers
#
# Runtime env vars (optional):
#   LOG_DIR=logs
#   RUN_ID=<timestamp>             # custom log filename suffix
#   CRAWL_DELAY_SECONDS=0.5        # delay for pykrx crawler
#   DART_DELAY_SECONDS=5.0         # delay for DART requests
#   MAIN_PHASE_DELAY_SECONDS=1      # delay between big phases
#   PHASE_TIMEOUT_SECONDS=0         # timeout for each phase (seconds), 0 means no timeout
#   BASE_MONTH=<YYYY-MM>            # 기준 월(기본: 이전 달)
#   LOOKBACK_MONTHS=12              # auto mode when START_MONTH 미지정 시 역산할 개월 수
#   MIN_CAP_DAILY_FILES=100        # auto-mode phase-1 done threshold
#   CAP_DAILY_THRESHOLD=90         # when to force official crawl before KR snapshots
#   DRY_RUN=1                      # print commands only
#
# Auto mode (no argument):
#   Checks completion state of each phase and only runs incomplete ones.
#   Phase 1 (pykrx crawler) — skipped if data/market/cap_daily/ has sufficient files
#   Phase 2 (DART)   — skipped if data/backfill/dart/_progress.json shows completed≥2000
#   Phase 3 (KR snap)— always included (monthly snapshot builder skips existing months)
#   Phase 4 (US snap) — always included (monthly snapshot builder skips existing months)
set -euo pipefail
cd "$(dirname "$0")"

# Auto month: if START_MONTH/END_MONTH are not explicitly provided, auto-generate window
LOOKBACK_MONTHS=${LOOKBACK_MONTHS:-12}

START_MONTH_PROVIDED=0
END_MONTH_PROVIDED=0
if [[ -n "${START_MONTH:-}" ]]; then
  START_MONTH_PROVIDED=1
fi
if [[ -n "${END_MONTH:-}" ]]; then
  END_MONTH_PROVIDED=1
fi

BASE_MONTH=${BASE_MONTH:-$(python3 -c "from datetime import date; today = date.today(); y, m = ((today.year - 1, 12) if today.month == 1 else (today.year, today.month - 1)); print(f'{y:04d}-{m:02d}')")}
END_MONTH=${END_MONTH:-$BASE_MONTH}
if [[ "$START_MONTH_PROVIDED" == "0" ]]; then
  START_MONTH=$(python3 -c "from sys import argv; y, m = map(int, argv[1].split('-')); lookback = max(1, int(argv[2])) - 1; start_index = (y * 12 + (m - 1)) - lookback; yy, mm = divmod(start_index, 12); print(f'{yy:04d}-{mm+1:02d}')" "$END_MONTH" "$LOOKBACK_MONTHS")
fi
START_DATE=${START_DATE:-${START_MONTH}-01}
END_DATE=${END_DATE:-$(python3 -c 'from datetime import date, timedelta; import sys; y, m = map(int, sys.argv[1].split("-")); n = date(y + (m // 12), (m % 12) + 1, 1); print((n - timedelta(days=1)).isoformat())' "$END_MONTH")}

# --- Runtime controls ---
LOG_DIR=${LOG_DIR:-logs}
RUN_ID=${RUN_ID:-$(date +%Y%m%d_%H%M%S)}
LOG_FILE="${LOG_DIR}/crawling_${RUN_ID}.log"
MAIN_PHASE_DELAY_SECONDS=${MAIN_PHASE_DELAY_SECONDS:-1}
PHASE_TIMEOUT_SECONDS=${PHASE_TIMEOUT_SECONDS:-0}
CRAWL_DELAY_SECONDS=${CRAWL_DELAY_SECONDS:-0.5}
DART_DELAY_SECONDS=${DART_DELAY_SECONDS:-5.0}
MIN_CAP_DAILY_FILES=${MIN_CAP_DAILY_FILES:-100}
CAP_DAILY_THRESHOLD=${CAP_DAILY_THRESHOLD:-90}

mkdir -p "$LOG_DIR"

log() {
  local message=$1
  echo "$(date '+%F %T') [run_crawling] ${message}" | tee -a "$LOG_FILE"
}

log_progress() {
  local message=$1
  echo "$(date '+%F %T') [run_crawling][progress] ${message}" | tee -a "$LOG_FILE"
}

run_with_log() {
  local label=$1
  shift
  local -a command=("$@")
  local phase_start

  log "=== $label START ==="
  log "command: ${command[*]}"
  phase_start=$(date +%s)
  if [[ "${DRY_RUN:-0}" == "1" ]]; then
    log "DRY_RUN=1, skipping execution"
    return 0
  fi

  if [[ "$PHASE_TIMEOUT_SECONDS" == "0" ]]; then
    "${command[@]}" 2>&1 | tee -a "$LOG_FILE"
  else
    timeout "$PHASE_TIMEOUT_SECONDS" "${command[@]}" 2>&1 | tee -a "$LOG_FILE"
  fi
  local rc=${PIPESTATUS[0]}
  local elapsed=$(( $(date +%s) - phase_start ))
  log "=== $label END (rc=$rc, elapsed=${elapsed}s) ==="
  if [[ "$rc" -ne 0 ]]; then
    return "$rc"
  fi

  if [[ "$MAIN_PHASE_DELAY_SECONDS" != "0" ]]; then
    sleep "$MAIN_PHASE_DELAY_SECONDS"
  fi
}

log "--- run_crawling.sh starting ---"
log "Run id: $RUN_ID"
log "Date window: ${START_MONTH} ~ ${END_MONTH}"
if [[ "$START_MONTH_PROVIDED" == "0" && "$END_MONTH_PROVIDED" == "0" ]]; then
  log "Auto mode window: 기준월=${END_MONTH} 기준, ${LOOKBACK_MONTHS}개월 자동 관리"
elif [[ "$START_MONTH_PROVIDED" == "0" ]]; then
  log "START_MONTH 자동 계산: ${END_MONTH} 기준 ${LOOKBACK_MONTHS}개월 전부터"
fi
if [[ "$BASE_MONTH" != "$END_MONTH" ]]; then
  log "BASE_MONTH override: ${BASE_MONTH}"
fi
log "Command window: ${START_DATE} ~ ${END_DATE}"
log "Log file: ${LOG_FILE}"
log "Settings: CRAWL_DELAY_SECONDS=${CRAWL_DELAY_SECONDS}, DART_DELAY_SECONDS=${DART_DELAY_SECONDS}, MIN_CAP_DAILY_FILES=${MIN_CAP_DAILY_FILES}, CAP_DAILY_THRESHOLD=${CAP_DAILY_THRESHOLD}, MAIN_PHASE_DELAY_SECONDS=${MAIN_PHASE_DELAY_SECONDS}"
if [[ "$PHASE_TIMEOUT_SECONDS" != "0" ]]; then
  log "Phase timeout: ${PHASE_TIMEOUT_SECONDS}s"
fi

if [[ "${VERBOSE:-0}" != "0" ]]; then
  log "Verbose mode enabled"
fi

# --- Determine which phases to run ---
PHASE_ARGS=()
RUN_KR_OFFICIAL_CRAWL=0

case "${1:-auto}" in
  auto)
    PHASES=()

    # Phase 1: skip if official crawl already produced enough cap_daily files
    OHLCV_COUNT=$(find data/market/cap_daily -name "*.parquet" 2>/dev/null | wc -l || true)
    if [[ "$OHLCV_COUNT" -lt "$MIN_CAP_DAILY_FILES" ]]; then
      log "[auto] Phase 1: KR official crawl not done (${OHLCV_COUNT} cap_daily files) — will run"
      PHASES+=(1)
    else
      log "[auto] Phase 1: DONE (${OHLCV_COUNT} cap_daily files) — skipping"
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
      log "[auto] Phase 2: DART not done (${DART_DONE} tickers, quarters=${DART_QUARTERS}) — will run"
      PHASES+=(2)
    else
      log "[auto] Phase 2: DONE (${DART_DONE} tickers, quarters=${DART_QUARTERS}) — skipping"
    fi

    # Phase 3: KR snapshots — always run (builder skips existing months internally)
    KR_DONE=$(find artifacts/kr/snapshots -name "snapshot.json" 2>/dev/null | wc -l || true)
    log "[auto] Phase 3: KR snapshots (${KR_DONE} months done) — will run remaining"
    PHASES+=(3)
    RUN_KR_OFFICIAL_CRAWL=1

    # Phase 4: US snapshots — always run (builder skips existing months internally)
    US_DONE=$(find artifacts/us/snapshots -name "snapshot.json" 2>/dev/null | wc -l || true)
    log "[auto] Phase 4: US snapshots (${US_DONE} months done) — will run remaining"
    PHASES+=(4)

    if [[ ${#PHASES[@]} -gt 0 ]]; then
      PHASE_ARGS=(--phase "${PHASES[@]}")
    fi
    ;;
  us)   PHASE_ARGS=(--phase 4) ;;
  kr)   PHASE_ARGS=(--phase 2 3); RUN_KR_OFFICIAL_CRAWL=1 ;;
  *)
    HAS_PHASE2=0
    HAS_PHASE3=0
    for phase in "$@"; do
      if [[ "$phase" == "2" ]]; then
        HAS_PHASE2=1
      fi
      if [[ "$phase" == "3" ]]; then
        RUN_KR_OFFICIAL_CRAWL=1
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

log "Selected phases: ${PHASE_ARGS[*]:-none}"
log_progress "Run phases: ${PHASE_ARGS[*]:-none}"
log_progress "Log file target: ${LOG_FILE}"

# Unbuffered Python output so log lines appear in tee immediately
export PYTHONUNBUFFERED=1

if [[ "$RUN_KR_OFFICIAL_CRAWL" == "1" ]]; then
  CAP_DAILY_COUNT=$(find data/market/cap_daily -name "*.parquet" 2>/dev/null | wc -l || true)
  if [[ "$CAP_DAILY_COUNT" -lt "$CAP_DAILY_THRESHOLD" ]]; then
    log "[auto] KR cap_daily incomplete (${CAP_DAILY_COUNT} parquet files) — running pykrx crawler"
    log_progress "[phase-1] official crawler window=${START_DATE}~${END_DATE}, delay=${CRAWL_DELAY_SECONDS}"
    run_with_log "phase-1-pykrx-official-crawl" \
      python scripts/crawl_kr_data_pykrx.py \
        --start "$START_DATE" --end "$END_DATE" \
        --output-root data \
        --delay "$CRAWL_DELAY_SECONDS" \
        --skip-meta \
        --skip-ohlcv
  else
    log "[auto] KR cap_daily present (${CAP_DAILY_COUNT} parquet files) — skipping official crawler"
  fi
fi

run_with_log "phase-2~4-backfill-all" \
  python scripts/backfill_all.py \
    --start "$START_MONTH" --end "$END_MONTH" \
    --dart-delay "$DART_DELAY_SECONDS" \
    "${PHASE_ARGS[@]}"
RC=$?

log "=== Backfill finished at $(date) (exit code $RC) ==="
log "All logs written to ${LOG_FILE}"
exit $RC
