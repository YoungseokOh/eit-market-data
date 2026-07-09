#!/usr/bin/env bash
# Resumable wrapper for the additive S&P MidCap 400 merge into US bundles.
#
# Idempotent: names already merged into a month's universe are skipped, so this
# can be killed and re-run freely (a kill loses at most one in-flight ~25-name
# chunk). Logs to logs/sp400_merge.log and writes a status marker.
#
# Usage:
#   scripts/run_sp400_merge.sh [START_MONTH] [END_MONTH]
# Defaults: 2019-01 .. 2026-06
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

START_MONTH="${1:-2019-01}"
END_MONTH="${2:-2026-06}"

mkdir -p logs
LOG="logs/sp400_merge.log"
STATUS="logs/sp400_merge.status"
LOCK="logs/sp400_merge.lock"

# Single-instance guard: if another merge is already running (e.g. an earlier
# hourly resume that survived), exit immediately instead of racing on the same
# snapshot.json files. Uses a lock dir (atomic mkdir) + stale-pid cleanup.
if ! mkdir "$LOCK" 2>/dev/null; then
  if [ -f "$LOCK/pid" ] && kill -0 "$(cat "$LOCK/pid")" 2>/dev/null; then
    echo "already running pid=$(cat "$LOCK/pid") $(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$STATUS"
    exit 0
  fi
  # stale lock (holder died): reclaim it.
  rm -rf "$LOCK"; mkdir "$LOCK"
fi
echo "$$" > "$LOCK/pid"
cleanup() { rm -rf "$LOCK"; }
trap cleanup EXIT

# shellcheck disable=SC1091
source .venv/bin/activate
set -a
# shellcheck disable=SC1091
[ -f ./.env ] && . ./.env
set +a

export EIT_US_DELISTED_FALLBACK=1
export EIT_EDGAR_FILING_CACHE=1

echo "running $(date -u +%Y-%m-%dT%H:%M:%SZ) pid=$$ range=${START_MONTH}..${END_MONTH}" > "$STATUS"

python scripts/merge_us_sp400.py \
  --start-month "$START_MONTH" \
  --end-month "$END_MONTH" \
  --chunk-size 25 \
  --fundamentals-source edgar_xbrl \
  >> "$LOG" 2>&1

echo "finished $(date -u +%Y-%m-%dT%H:%M:%SZ) rc=$?" > "$STATUS"
