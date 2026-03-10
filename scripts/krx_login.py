#!/usr/bin/env python3
# ruff: noqa: E402
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from eit_market_data.kr.krx_auth import (
    check_krx_auth,
    ensure_krx_authenticated_session,
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
    print("[INFO] Complete the KRX login in the opened Chromium window.")

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
