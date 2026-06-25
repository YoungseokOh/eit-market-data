#!/usr/bin/env python3
# ruff: noqa: E402
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import bootstrap

PROJECT_ROOT = bootstrap()

from eit_market_data.kr.krx_auth import (
    check_krx_auth,
    ensure_krx_authenticated_session,
    has_krx_env_credentials,
    resolve_cookie_path,
    resolve_profile_dir,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create or refresh a local KRX login session."
    )
    parser.add_argument(
        "--profile-dir",
        help="Persistent Playwright Chromium profile directory.",
    )
    parser.add_argument(
        "--cookie-path",
        help="JSON file to store exported KRX cookies.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=300,
        help="Maximum seconds to wait for manual login.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Ignore any cached cookies and force a fresh browser login.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    profile_dir = resolve_profile_dir(args.profile_dir)
    cookie_path = resolve_cookie_path(args.cookie_path, profile_dir)
    print(f"[INFO] profile_dir={profile_dir}")
    print(f"[INFO] cookie_path={cookie_path}")
    if has_krx_env_credentials():
        print("[INFO] KRX_ID/KRX_PW found in .env or environment; auto-login will be attempted.")
        print("[INFO] If KRX asks for extra verification, complete it in the opened Chromium window.")
    else:
        print("[INFO] Complete the KRX login in the opened Chromium window.")
    if os.environ.get("WSL_DISTRO_NAME") or os.environ.get("WSL_INTEROP"):
        print(
            "[INFO] WSL detected. If the Chromium window does not appear, "
            "run scripts/windows_krx_setup_and_probe.cmd from Windows PowerShell or cmd."
        )

    session = ensure_krx_authenticated_session(
        interactive=True,
        force_refresh=args.force,
        profile_dir=profile_dir,
        cookie_path=cookie_path,
        timeout_seconds=args.timeout,
    )
    status = check_krx_auth(session)
    print(f"[{('OK' if status.authenticated else 'FAILED')}] {status.detail}")


if __name__ == "__main__":
    main()
