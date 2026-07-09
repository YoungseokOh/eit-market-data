#!/usr/bin/env bash
# Keep the S&P 400 merge alive: relaunch the (idempotent, lock-guarded) wrapper
# whenever it exits before completion. Completion = all 90 US months have a
# universe >= 850 names. Re-running this driver is itself safe.
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
mkdir -p logs
DLOG="logs/sp400_drive.log"

complete() {
  ./.venv/bin/python - <<'PY'
import json,glob,sys
from pathlib import Path
n=done=0
for p in glob.glob("artifacts/us/snapshots/*/snapshot.json"):
    n+=1
    if len(json.loads(Path(p).read_text())["universe"])>=850: done+=1
sys.exit(0 if (n>=90 and done>=90) else 1)
PY
}

echo "driver start $(date -u +%FT%TZ)" >> "$DLOG"
for _ in $(seq 1 500); do
  if complete; then
    echo "driver: merge complete $(date -u +%FT%TZ)" >> "$DLOG"
    break
  fi
  bash scripts/run_sp400_merge.sh 2019-01 2026-06 >> "$DLOG" 2>&1 || true
  sleep 20
done
echo "driver exit $(date -u +%FT%TZ)" >> "$DLOG"
