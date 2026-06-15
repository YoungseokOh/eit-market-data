#!/bin/bash
# Local daily market-data batch for the Mac Mini (launchd-scheduled).
#
# Runs the same entrypoint as the GitHub Actions workflow, but entirely on
# this machine with no GitHub-hosted minutes. API keys are read from the
# repo-local .env (loaded by run_daily_batch.py via python-dotenv).
#
# Scheduled by ~/Library/LaunchAgents/com.eit.market-data.daily.plist
# Manual run:  bash scripts/run_daily_local.sh
set -euo pipefail

# launchd hands jobs a minimal PATH; make Homebrew uv/python reachable.
export PATH="/opt/homebrew/bin:/opt/homebrew/sbin:/usr/bin:/bin:/usr/sbin:/sbin:${PATH:-}"
export TZ="Asia/Seoul"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

LOG_DIR="$REPO_ROOT/logs"
mkdir -p "$LOG_DIR"
TS="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="$LOG_DIR/daily_local_${TS}.log"

echo "[run_daily_local] start $(date '+%F %T %Z')" | tee -a "$LOG_FILE"

# run_daily_batch.py self-loads .env and gates the KR snapshot to month-end
# (no broad live DART on ordinary weekdays).
set +e
uv run --python 3.11 python scripts/run_daily_batch.py "$@" 2>&1 | tee -a "$LOG_FILE"
rc=${PIPESTATUS[0]}
set -e

echo "[run_daily_local] end rc=${rc} $(date '+%F %T %Z')" | tee -a "$LOG_FILE"

# Keep only the 30 most recent local wrapper logs.
ls -1t "$LOG_DIR"/daily_local_*.log 2>/dev/null | tail -n +31 | xargs -I {} rm -f {} || true

exit "$rc"
