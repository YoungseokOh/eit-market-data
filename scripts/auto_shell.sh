#!/usr/bin/env bash

__eit_market_data_autoenv() {
    local project_root="/home/seok436/projects/eit-market-data"
    local pyproject="$project_root/pyproject.toml"
    local env_file="$project_root/.env"
    local activate_script="$project_root/.venv/bin/activate"
    local sync_stamp="$project_root/.venv/.eit_sync_stamp"
    local cache_dir="/tmp/uv-cache"
    local wsl_windows_home="/mnt/c/Users/${USER}"
    local shared_krx_profile="$wsl_windows_home/.cache/eit-market-data/krx-profile"
    local shared_krx_cookie="$shared_krx_profile/cookies.json"
    local now
    local env_sig

    case "$PWD/" in
        "$project_root/"*) ;;
        *) return 0 ;;
    esac

    if [[ ! -x "$project_root/.venv/bin/python" || ! -f "$sync_stamp" || "$pyproject" -nt "$sync_stamp" ]]; then
        now=$(date +%s)
        if [[ -n "${__EIT_MARKET_DATA_LAST_SYNC_FAIL_AT:-}" ]] && (( now - __EIT_MARKET_DATA_LAST_SYNC_FAIL_AT < 300 )); then
            return 0
        fi
        if command -v uv >/dev/null 2>&1; then
            (
                cd "$project_root" || exit 1
                UV_CACHE_DIR="$cache_dir" uv sync --extra all --extra dev
            ) || {
                __EIT_MARKET_DATA_LAST_SYNC_FAIL_AT=$now
                return 0
            }
            touch "$sync_stamp"
            unset __EIT_MARKET_DATA_LAST_SYNC_FAIL_AT
        fi
    fi

    if [[ -f "$activate_script" && "${VIRTUAL_ENV:-}" != "$project_root/.venv" ]]; then
        # shellcheck disable=SC1090
        source "$activate_script"
    fi

    if [[ -f "$env_file" ]]; then
        env_sig="$(stat -c '%Y:%s' "$env_file" 2>/dev/null || printf '')"
        if [[ "${__EIT_MARKET_DATA_ENV_SIG:-}" != "$env_sig" ]]; then
            set -a
            # shellcheck disable=SC1090
            source "$env_file"
            set +a
            __EIT_MARKET_DATA_ENV_SIG="$env_sig"
        fi
    fi

    if [[ -z "${EIT_KRX_PROFILE_DIR:-}" && -d "$shared_krx_profile" ]]; then
        export EIT_KRX_PROFILE_DIR="$shared_krx_profile"
    fi

    if [[ -z "${EIT_KRX_COOKIE_PATH:-}" && -f "$shared_krx_cookie" ]]; then
        export EIT_KRX_COOKIE_PATH="$shared_krx_cookie"
    fi
}
