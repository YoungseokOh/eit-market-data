#!/bin/bash
# 2026 monthly market-data collection orchestrator (KR KOSPI200 + US S&P500/Nasdaq-100).
#
# Designed to be driven by a thin, self-paced /loop: ALL phase/resume/abort logic
# lives here (cheap bash) so each loop tick only reads data/collect_2026/state.json.
#
# Phases (ordered by rate-limit risk, low -> high):
#   1. pykrx KR prices/cap          (low risk)
#   2. US snapshots S&P500+Nasdaq   (low risk, chunked)
#   3. DART controlled backfill     (HIGH risk -> stops on transient, resumable)
#   4. KR monthly snapshots         (cache replay)
#
# Resumable: completed phases are skipped via data/collect_2026/done.phaseN markers
# and per-month artifact existence. Re-running after a DART block resumes phase 3
# via --progress, then continues to phase 4.
#
# Manual run:    bash scripts/collect_2026.sh
# Resume:        bash scripts/collect_2026.sh        (same command; it skips done work)
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
export TZ="Asia/Seoul"
PYBIN="$REPO_ROOT/.venv/bin/python"

STATE_DIR="$REPO_ROOT/data/collect_2026"
STATE="$STATE_DIR/state.json"
LOG_DIR="$STATE_DIR/logs"
mkdir -p "$STATE_DIR" "$LOG_DIR" "$REPO_ROOT/data/backfill"

# --- collection parameters -------------------------------------------------
KOSPI_CSV="$REPO_ROOT/universes/kr_kospi200_2026-05-04.csv"
START_DATE="2026-01-01"
PARTIAL_AS_OF="2026-06-19"          # current partial-month decision date
COMPLETED_MONTH_ENDS=(2026-01-30 2026-02-27 2026-03-31 2026-04-30 2026-05-29)
DART_PROGRESS="$REPO_ROOT/data/backfill/dart_kospi200.json"
DART_QUARTERS=8                     # enough to PIT-replay all of 2026
DART_DELAY=6                        # >= 5s per dart-api-limits rule

# Transient DART symptoms -> stop immediately, resume after midnight KST.
DART_TRANSIENT='ConnectTimeout|ReadTimeout|RemoteDisconnected|Connection reset|Max retries exceeded|HTTP 000|returned empty|조회된 데이타가 없습니다'

# --- helpers ---------------------------------------------------------------
set_state() {  # phase status detail
  "$PYBIN" - "$1" "$2" "$3" "$STATE" <<'PY'
import json, sys, datetime
phase, status, detail, path = sys.argv[1:5]
json.dump({
    "phase": phase, "status": status, "detail": detail,
    "updated": datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
}, open(path, "w"), ensure_ascii=False, indent=2)
PY
  echo "[state] phase=$1 status=$2 ${3:+detail=$3}"
}

log_for() { echo "$LOG_DIR/$1_$(date +%Y%m%d_%H%M%S).log"; }

# --- phase 1: pykrx KR prices / market cap ---------------------------------
phase1() {
  [ -f "$STATE_DIR/done.phase1" ] && { echo "[phase1] skip (done)"; return 0; }
  set_state 1 running "pykrx crawl ${START_DATE}..${PARTIAL_AS_OF}"
  local lg; lg="$(log_for phase1)"
  "$PYBIN" scripts/crawl_kr_data_pykrx.py \
    --start "$START_DATE" --end "$PARTIAL_AS_OF" --output-root data >"$lg" 2>&1
  local rc=$?
  if [ $rc -ne 0 ]; then set_state 1 error "rc=$rc see $lg"; return 1; fi
  touch "$STATE_DIR/done.phase1"; echo "[phase1] done"
}

# --- phase 2: US snapshots, completed months 2026-01..05 (chunked batch) -----
# Current partial month (June) US is intentionally left to the daily batch to
# avoid a month-end-labeled partial here; KR June partial is still built below.
phase2() {
  [ -f "$STATE_DIR/done.phase2" ] && { echo "[phase2] skip (done)"; return 0; }
  set_state 2 running "US S&P500+Nasdaq-100 2026-01..05"
  local lg; lg="$(log_for phase2)"
  "$PYBIN" scripts/build_us_batch.py \
    --start-month 2026-01 --end-month 2026-05 \
    --artifacts-root artifacts >"$lg" 2>&1
  local rc=$?
  if [ $rc -ne 0 ]; then set_state 2 error "rc=$rc see $lg"; return 1; fi
  touch "$STATE_DIR/done.phase2"; echo "[phase2] done"
}

# --- phase 3: DART controlled backfill (HIGH risk, resumable) ---------------
phase3() {
  [ -f "$STATE_DIR/done.phase3" ] && { echo "[phase3] skip (done)"; return 0; }
  set_state 3 running "DART controlled backfill KOSPI200 as_of=${PARTIAL_AS_OF} delay=${DART_DELAY}s q=${DART_QUARTERS}"
  local lg; lg="$(log_for phase3)"
  "$PYBIN" scripts/backfill_dart_cache_controlled.py \
    --universe-csv "$KOSPI_CSV" --as-of "$PARTIAL_AS_OF" \
    --progress "$DART_PROGRESS" --quarters "$DART_QUARTERS" \
    --delay "$DART_DELAY" --filing-mode optional >"$lg" 2>&1
  local rc=$?
  if grep -Eq "$DART_TRANSIENT" "$lg"; then
    set_state 3 blocked "transient DART symptom; resume after 00:00 KST. log=$lg"
    echo "[phase3] BLOCKED on transient DART symptom"; return 10
  fi
  if [ $rc -ne 0 ]; then set_state 3 error "rc=$rc see $lg"; return 1; fi
  touch "$STATE_DIR/done.phase3"; echo "[phase3] done"
}

# --- phase 4: KR monthly snapshots from DART cache --------------------------
build_kr_month() {  # as_of force_flag
  local as_of="$1" force="$2" month="${1:0:7}"
  if [ -f "artifacts/kr/snapshots/${month}/snapshot.json" ]; then
    echo "[phase4] skip ${month} (exists)"; return 0; fi
  "$PYBIN" scripts/build_kr_snapshot.py \
    --as-of "$as_of" --universe-csv "$KOSPI_CSV" \
    --artifacts-root artifacts --market-subdir kr --profile official $force \
    >"$(log_for phase4_${month})" 2>&1
}

phase4() {
  [ -f "$STATE_DIR/done.phase4" ] && { echo "[phase4] skip (done)"; return 0; }
  set_state 4 running "KR snapshots KOSPI200 (cache replay)"
  for ae in "${COMPLETED_MONTH_ENDS[@]}"; do
    build_kr_month "$ae" "" || { set_state 4 error "kr ${ae:0:7} build failed"; return 1; }
  done
  build_kr_month "$PARTIAL_AS_OF" "--force" || { set_state 4 error "kr 2026-06 build failed"; return 1; }
  touch "$STATE_DIR/done.phase4"; echo "[phase4] done"
}

# --- main ------------------------------------------------------------------
phase1 || exit 1
phase2 || exit 1
phase3 || exit $?     # 10 = BLOCKED (expected, resume later); 1 = error
phase4 || exit 1
set_state done done "all phases complete"
echo "[collect_2026] ALL DONE"
