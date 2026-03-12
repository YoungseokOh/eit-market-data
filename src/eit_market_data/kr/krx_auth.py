"""KRX Data Marketplace authentication helpers for KRX-backed calls."""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any

import requests

logger = logging.getLogger(__name__)

KRX_JSON_URL = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
KRX_LOGIN_URL = (
    "https://data.krx.co.kr/contents/MDC/COMS/client/MDCCOMS001.cmd?locale=ko_KR"
)
KRX_REFERER = "https://data.krx.co.kr/contents/MDC/MDI/outerLoader/index.cmd"
KRX_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": KRX_REFERER,
}
KRX_AUTH_PROBE_PAYLOAD = {
    "bld": "dbms/MDC/STAT/standard/MDCSTAT00401",
    "locale": "ko_KR",
    "idxIndMidclssCd": "02",
}
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_LOGIN_TIMEOUT_SECONDS = 300
_PROFILE_ENV = "EIT_KRX_PROFILE_DIR"
_COOKIE_ENV = "EIT_KRX_COOKIE_PATH"

_session_lock = threading.Lock()
_configured_session: requests.Session | None = None
_pykrx_hooks_installed = False
_fdr_hooks_installed = False


class _RequestsProxy:
    """Proxy only the requests calls used inside FDR KRX modules."""

    def __init__(self, base: ModuleType, *, get_fn, post_fn) -> None:  # noqa: ANN001
        self._base = base
        self.get = get_fn
        self.post = post_fn

    def __getattr__(self, name: str) -> Any:
        return getattr(self._base, name)


class KrxAuthRequired(RuntimeError):
    """Raised when KRX requires an authenticated Data Marketplace session."""


class KrxOfficialDataUnavailable(RuntimeError):
    """Raised when KRX official data is unavailable despite authentication."""


@dataclass(frozen=True)
class AuthStatus:
    authenticated: bool
    detail: str
    cookie_path: Path | None = None


def resolve_profile_dir(profile_dir: str | Path | None = None) -> Path:
    if profile_dir is not None:
        return Path(profile_dir).expanduser().resolve()
    raw = os.environ.get(_PROFILE_ENV)
    if raw:
        return Path(raw).expanduser().resolve()
    return (Path.home() / ".cache" / "eit-market-data" / "krx-profile").resolve()


def resolve_cookie_path(
    cookie_path: str | Path | None = None,
    profile_dir: str | Path | None = None,
) -> Path:
    if cookie_path is not None:
        return Path(cookie_path).expanduser().resolve()
    raw = os.environ.get(_COOKIE_ENV)
    if raw:
        return Path(raw).expanduser().resolve()
    return (resolve_profile_dir(profile_dir) / "cookies.json").resolve()


def build_krx_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(KRX_HEADERS)
    return session


def configure_krx_session(session: requests.Session | None) -> None:
    global _configured_session
    with _session_lock:
        _configured_session = session


def get_krx_session() -> requests.Session:
    with _session_lock:
        if _configured_session is not None:
            return _configured_session
    return build_krx_session()


def save_cookies_to_file(cookies: list[dict[str, Any]], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cookies, ensure_ascii=True, indent=2))
    return path


def load_cookies_from_file(
    path: Path, session: requests.Session | None = None
) -> requests.Session:
    target = session or build_krx_session()
    cookies = json.loads(path.read_text())
    for cookie in cookies:
        if not isinstance(cookie, dict):
            continue
        name = str(cookie.get("name", "")).strip()
        if not name:
            continue
        target.cookies.set(
            name,
            str(cookie.get("value", "")),
            domain=cookie.get("domain"),
            path=cookie.get("path", "/"),
        )
    return target


def _looks_like_login_html(text: str) -> bool:
    lowered = text.lower()
    return (
        "krx data marketplace" in lowered
        and "로그인" in text
        or "mdccoms001.cmd" in lowered
        or "alert('로그인 또는 회원가입이 필요합니다.')" in text
        or "location.href='/contents/mdc/coms/client/mdccoms001.cmd" in lowered
    )


def auth_failure_reason(response: requests.Response) -> str | None:
    text = response.text[:4096]
    if "LOGOUT" in text:
        return f"status={response.status_code} LOGOUT"

    content_type = response.headers.get("content-type", "").lower()
    if "text/html" in content_type and _looks_like_login_html(text):
        return "redirected to KRX login page"

    return None


def raise_for_auth_failure(response: requests.Response, context: str) -> None:
    reason = auth_failure_reason(response)
    if reason:
        raise KrxAuthRequired(f"{context}: {reason}")


def check_krx_auth(session: requests.Session | None = None) -> AuthStatus:
    active_session = session or get_krx_session()
    try:
        response = active_session.post(
            KRX_JSON_URL,
            headers=KRX_HEADERS,
            data=KRX_AUTH_PROBE_PAYLOAD,
            timeout=DEFAULT_TIMEOUT_SECONDS,
        )
    except requests.RequestException as exc:
        return AuthStatus(False, f"probe request failed: {exc}")

    reason = auth_failure_reason(response)
    if reason:
        return AuthStatus(False, reason)

    try:
        payload = response.json()
    except ValueError as exc:
        return AuthStatus(False, f"probe returned non-JSON payload: {exc}")

    output = payload.get("output")
    if isinstance(output, list) and output:
        return AuthStatus(True, f"probe rows={len(output)}")
    return AuthStatus(False, "probe returned empty output")


def install_pykrx_krx_session_hooks() -> None:
    global _pykrx_hooks_installed
    with _session_lock:
        if _pykrx_hooks_installed:
            return

        from pykrx.website.comm import webio

        def _read_with_shared_get(self, **params):  # noqa: ANN001
            session = get_krx_session()
            headers = dict(KRX_HEADERS)
            headers.update(getattr(self, "headers", {}))
            response = session.get(
                self.url,
                headers=headers,
                params=params,
                timeout=DEFAULT_TIMEOUT_SECONDS,
            )
            raise_for_auth_failure(response, f"KRX GET {self.url}")
            return response

        def _read_with_shared_post(self, **params):  # noqa: ANN001
            session = get_krx_session()
            headers = dict(KRX_HEADERS)
            headers.update(getattr(self, "headers", {}))
            response = session.post(
                self.url,
                headers=headers,
                data=params,
                timeout=DEFAULT_TIMEOUT_SECONDS,
            )
            raise_for_auth_failure(response, f"KRX POST {self.url}")
            return response

        webio.Get.read = _read_with_shared_get
        webio.Post.read = _read_with_shared_post
        _pykrx_hooks_installed = True


def install_fdr_krx_session_hooks() -> None:
    global _fdr_hooks_installed
    with _session_lock:
        if _fdr_hooks_installed:
            return

        import FinanceDataReader.krx.data as fdr_krx_data
        import FinanceDataReader.krx.listing as fdr_krx_listing
        import FinanceDataReader.krx.snap as fdr_krx_snap

        def _shared_get(url: str, **kwargs):  # noqa: ANN001
            session = get_krx_session()
            headers = dict(KRX_HEADERS)
            headers.update(kwargs.pop("headers", {}) or {})
            response = session.get(
                url,
                headers=headers,
                timeout=kwargs.pop("timeout", DEFAULT_TIMEOUT_SECONDS),
                **kwargs,
            )
            if "data.krx.co.kr" in url.lower():
                raise_for_auth_failure(response, f"KRX GET {url}")
            return response

        def _shared_post(url: str, data=None, json=None, **kwargs):  # noqa: ANN001
            session = get_krx_session()
            headers = dict(KRX_HEADERS)
            headers.update(kwargs.pop("headers", {}) or {})
            response = session.post(
                url,
                data=data,
                json=json,
                headers=headers,
                timeout=kwargs.pop("timeout", DEFAULT_TIMEOUT_SECONDS),
                **kwargs,
            )
            if "data.krx.co.kr" in url.lower():
                raise_for_auth_failure(response, f"KRX POST {url}")
            return response

        for module in (fdr_krx_data, fdr_krx_listing, fdr_krx_snap):
            module.requests = _RequestsProxy(  # type: ignore[assignment]
                module.requests,
                get_fn=_shared_get,
                post_fn=_shared_post,
            )

        _fdr_hooks_installed = True


def _interactive_login_cookie_export(
    profile_dir: Path,
    cookie_path: Path,
    timeout_seconds: int,
) -> Path:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise KrxAuthRequired(
            "Playwright is required for interactive KRX login. "
            "Install with: pip install -e '.[kr]' && playwright install chromium"
        ) from exc

    profile_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Opening KRX login page in Chromium profile %s", profile_dir)

    with sync_playwright() as playwright:
        context = None
        try:
            context = playwright.chromium.launch_persistent_context(
                str(profile_dir),
                headless=False,
            )
            page = context.pages[0] if context.pages else context.new_page()
            page.goto(KRX_LOGIN_URL, wait_until="domcontentloaded")
            deadline = time.monotonic() + timeout_seconds

            while time.monotonic() < deadline:
                cookies = context.cookies()
                if cookies:
                    save_cookies_to_file(cookies, cookie_path)
                    session = load_cookies_from_file(cookie_path)
                    status = check_krx_auth(session)
                    if status.authenticated:
                        return cookie_path
                time.sleep(2)
        except Exception as exc:
            if exc.__class__.__name__ == "TargetClosedError":
                raise KrxAuthRequired(
                    "Chromium window closed before KRX login completed. "
                    "Keep the browser open until the terminal prints [OK]. "
                    "On Windows, run scripts/windows_krx_setup_and_probe.cmd "
                    "from PowerShell or cmd."
                ) from exc
            raise
        finally:
            if context is not None:
                context.close()

    raise KrxAuthRequired(
        f"KRX login did not complete within {timeout_seconds} seconds"
    )


def ensure_krx_authenticated_session(
    *,
    interactive: bool = False,
    force_refresh: bool = False,
    profile_dir: str | Path | None = None,
    cookie_path: str | Path | None = None,
    timeout_seconds: int = DEFAULT_LOGIN_TIMEOUT_SECONDS,
) -> requests.Session:
    install_pykrx_krx_session_hooks()
    install_fdr_krx_session_hooks()

    if not force_refresh:
        current = get_krx_session()
        status = check_krx_auth(current)
        if status.authenticated:
            configure_krx_session(current)
            return current

    cookie_file = resolve_cookie_path(cookie_path, profile_dir)
    if cookie_file.exists():
        session = load_cookies_from_file(cookie_file)
        status = check_krx_auth(session)
        if status.authenticated:
            configure_krx_session(session)
            return session
        logger.warning("Stored KRX cookies are invalid: %s", status.detail)

    if interactive:
        exported = _interactive_login_cookie_export(
            resolve_profile_dir(profile_dir),
            cookie_file,
            timeout_seconds,
        )
        session = load_cookies_from_file(exported)
        status = check_krx_auth(session)
        if status.authenticated:
            configure_krx_session(session)
            return session
        raise KrxAuthRequired(
            f"KRX login completed but probe still failed: {status.detail}"
        )

    raise KrxAuthRequired(
        "KRX login required. Run `python scripts/krx_login.py` first "
        f"to create {cookie_file}."
    )
