#!/usr/bin/env bash
# Quiet watchdog for the S&P 400 merge. Emits exactly one stdout line — and
# exits — on one of three terminal conditions, so the monitor stays silent
# through routine per-month progress:
#   COMPLETE     all 90 US months have universe >= 850 and status shows finished
#   BLOCK: ...   a hard source block appeared in newly-appended log lines
#   DRIVER_DOWN  the self-relaunching driver died before completion
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
LOG="logs/sp400_merge.log"
prev=$(wc -l < "$LOG" 2>/dev/null | tr -d ' ' || echo 0)

is_complete() {
  ./.venv/bin/python - <<'PY'
import json,glob,sys
from pathlib import Path
n=done=0
for p in glob.glob("artifacts/us/snapshots/*/snapshot.json"):
    n+=1
    try:
        if len(json.loads(Path(p).read_text())["universe"])>=850: done+=1
    except Exception: pass
ok = n>=90 and done>=90 and Path("logs/sp400_merge.status").exists() \
     and "finished" in Path("logs/sp400_merge.status").read_text()
sys.exit(0 if ok else 1)
PY
}

while true; do
  if is_complete; then
    echo "COMPLETE all 90 months >=850 and status finished"; exit 0
  fi
  cur=$(wc -l < "$LOG" 2>/dev/null | tr -d ' ' || echo 0)
  if [ "$cur" -gt "$prev" ]; then
    blk=$(sed -n "$((prev+1)),${cur}p" "$LOG" 2>/dev/null \
      | grep -E "HTTP/1\.1 (403|429)|Max retries|Connection reset|RemoteDisconnected|Too Many Requests|Forbidden" \
      | head -1 || true)
    if [ -n "$blk" ]; then echo "BLOCK: $blk"; exit 0; fi
  fi
  prev=$cur
  if ! pgrep -f drive_sp400.sh >/dev/null 2>&1; then
    # driver gone; if a lone merge process is still finishing, allow it, else report
    if ! pgrep -f merge_us_sp400.py >/dev/null 2>&1; then
      echo "DRIVER_DOWN driver and merge both not running (incomplete)"; exit 0
    fi
  fi
  sleep 300
done
