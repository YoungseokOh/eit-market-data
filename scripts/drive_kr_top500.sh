#!/usr/bin/env bash
# Keep the KR top-500 bundle build alive: relaunch the (idempotent, skip-existing)
# builder whenever it exits before completion. Completion = all 90 KR top-500
# months have a snapshot.json. Re-running this driver is itself safe: the builder
# skips months that already exist, so no work is duplicated.
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
mkdir -p logs
DLOG="logs/kr_top500_drive.log"

complete() {
  n=$(ls -d artifacts_top500/kr/snapshots/*/snapshot.json 2>/dev/null | wc -l | tr -d ' ')
  [ "$n" -ge 90 ]
}

echo "driver start $(date -u +%FT%TZ)" >> "$DLOG"
for _ in $(seq 1 500); do
  if complete; then
    echo "driver: kr top500 build complete $(date -u +%FT%TZ)" >> "$DLOG"
    break
  fi
  ./.venv/bin/python scripts/build_kr_top500_bundles.py \
      --start-month 2019-01 --end-month 2026-06 --dart-mode cache_only >> "$DLOG" 2>&1 || true
  sleep 20
done
echo "driver exit $(date -u +%FT%TZ)" >> "$DLOG"
