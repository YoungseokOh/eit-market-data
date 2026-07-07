#!/usr/bin/env bash
# Keep the KR top-500 DART fundamentals backfill moving without per-cycle agent
# babysitting. The underlying script (backfill_dart_cache_controlled.py) is
# itself the safety mechanism: it stops after --max-consecutive-empty empties
# (a real signal worth pausing on per .claude/rules/dart-api-limits.md) or on a
# raised transient exception. This driver only relaunches on a clean stop
# (any exit), so it advances past legitimate empty-name clusters while still
# respecting >=5s per-ticker delay and never retrying a hot connection loop.
#
# It does NOT loop forever blindly: it stops relaunching once every ticker in
# the universe CSV is in progress["completed"], and it caps relaunches at 200
# to avoid an unbounded loop if something is systemically broken.
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
mkdir -p logs
DLOG="logs/kr_top500_dart_drive.log"
PROGRESS="data/dart_top500_missing_progress.json"
UNIVERSE="universes/kr/top500/_dart_missing.csv"

complete() {
  ./.venv/bin/python - <<PY
import json
from pathlib import Path
import csv
total = sum(1 for _ in csv.DictReader(open("$UNIVERSE")))
p = Path("$PROGRESS")
done = len(json.loads(p.read_text()).get("completed", [])) if p.exists() else 0
raise SystemExit(0 if done >= total else 1)
PY
}

echo "driver start $(date -u +%FT%TZ)" >> "$DLOG"
for _ in $(seq 1 200); do
  if complete; then
    echo "driver: dart backfill complete $(date -u +%FT%TZ)" >> "$DLOG"
    break
  fi
  ./.venv/bin/python scripts/backfill_dart_cache_controlled.py \
      --universe-csv "$UNIVERSE" \
      --as-of 2026-06-30 \
      --progress "$PROGRESS" \
      --delay 5.0 \
      --continue-on-empty \
      --max-consecutive-empty 8 \
      --filing-mode optional >> "$DLOG" 2>&1 || true
  sleep 20
done
echo "driver exit $(date -u +%FT%TZ)" >> "$DLOG"
