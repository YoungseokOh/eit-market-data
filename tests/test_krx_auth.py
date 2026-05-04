from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import requests

from eit_market_data.kr import krx_auth

from eit_market_data.kr.krx_auth import (
    _krx_credentials_from_env,
    _is_wsl_runtime,
    _login_timeout_hint,
    _playwright_launch_options,
    has_krx_env_credentials,
)


def test_playwright_launch_options_maximize_window() -> None:
    options = _playwright_launch_options(Path("/tmp/krx-profile"))

    assert options["user_data_dir"] == "/tmp/krx-profile"
    assert options["headless"] is False
    assert options["no_viewport"] is True
    assert "--start-maximized" in options["args"]


def test_login_timeout_hint_mentions_windows_helper_on_wsl(monkeypatch) -> None:
    monkeypatch.setenv("WSL_DISTRO_NAME", "Ubuntu")

    assert _is_wsl_runtime() is True
    assert "windows_krx_setup_and_probe.cmd" in _login_timeout_hint(300)


def test_login_timeout_hint_is_plain_outside_wsl(monkeypatch) -> None:
    monkeypatch.delenv("WSL_DISTRO_NAME", raising=False)
    monkeypatch.delenv("WSL_INTEROP", raising=False)

    assert _is_wsl_runtime() is False
    assert _login_timeout_hint(300) == "KRX login did not complete within 300 seconds."


def test_krx_credentials_from_env_requires_id_and_password(monkeypatch) -> None:
    monkeypatch.setenv("KRX_ID", "user")
    monkeypatch.delenv("KRX_PW", raising=False)

    assert _krx_credentials_from_env() is None
    assert has_krx_env_credentials() is False

    monkeypatch.setenv("KRX_PW", "secret")

    assert _krx_credentials_from_env() == ("user", "secret")
    assert has_krx_env_credentials() is True


def test_install_pykrx_hooks_skips_legacy_patch_when_pykrx_has_auth(
    monkeypatch,
) -> None:
    monkeypatch.setattr(krx_auth, "_pykrx_hooks_installed", False)
    monkeypatch.setattr(
        krx_auth,
        "_modern_pykrx_auth_module",
        lambda: SimpleNamespace(KRXSession=object, get_auth_session=lambda: None),
    )

    def fail_load_webio():  # noqa: ANN202
        raise AssertionError("legacy webio hooks must not be installed")

    monkeypatch.setattr(
        "eit_market_data.kr.pykrx_loader.load_pykrx_webio",
        fail_load_webio,
    )

    krx_auth.install_pykrx_krx_session_hooks()

    assert krx_auth._pykrx_hooks_installed is True


def test_ensure_krx_authenticated_session_prefers_modern_pykrx_session(
    monkeypatch,
) -> None:
    session = requests.Session()
    pykrx_auth = SimpleNamespace(
        get_auth_session=lambda: SimpleNamespace(session=session),
    )

    monkeypatch.setenv("KRX_ID", "user")
    monkeypatch.setenv("KRX_PW", "secret")
    monkeypatch.setattr(krx_auth, "_configured_session", None)
    monkeypatch.setattr(krx_auth, "install_pykrx_krx_session_hooks", lambda: None)
    monkeypatch.setattr(krx_auth, "install_fdr_krx_session_hooks", lambda: None)
    monkeypatch.setattr(krx_auth, "_modern_pykrx_auth_module", lambda: pykrx_auth)

    assert krx_auth.ensure_krx_authenticated_session(interactive=False) is session
    assert krx_auth.get_krx_session() is session
