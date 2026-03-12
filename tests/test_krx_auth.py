from __future__ import annotations

from pathlib import Path

import requests

from eit_market_data.kr.krx_auth import (
    auth_failure_reason,
    build_krx_session,
    configure_krx_session,
    install_fdr_krx_session_hooks,
    load_cookies_from_file,
    save_cookies_to_file,
)


def _response(
    status_code: int, text: str, content_type: str = "text/html; charset=utf-8"
) -> requests.Response:
    response = requests.Response()
    response.status_code = status_code
    response._content = text.encode("utf-8")  # noqa: SLF001
    response.headers["content-type"] = content_type
    response.url = "https://data.krx.co.kr/test"
    return response


def test_auth_failure_reason_detects_logout() -> None:
    response = _response(400, "LOGOUT")

    reason = auth_failure_reason(response)

    assert reason == "status=400 LOGOUT"


def test_auth_failure_reason_detects_login_html() -> None:
    html = """
    <html lang="ko">
      <head><title>로그인 - KRX | KRX Data Marketplace</title></head>
      <body>alert('로그인 또는 회원가입이 필요합니다.');</body>
    </html>
    """
    response = _response(200, html)

    reason = auth_failure_reason(response)

    assert reason == "redirected to KRX login page"


def test_cookie_roundtrip(tmp_path: Path) -> None:
    cookie_path = tmp_path / "cookies.json"
    cookies = [
        {
            "name": "JSESSIONID",
            "value": "abc",
            "domain": "data.krx.co.kr",
            "path": "/",
        }
    ]

    save_cookies_to_file(cookies, cookie_path)
    session = load_cookies_from_file(cookie_path, build_krx_session())

    assert session.cookies.get("JSESSIONID") == "abc"


def test_install_fdr_hooks_uses_shared_session_without_mutating_global_requests(
    monkeypatch,
) -> None:
    import FinanceDataReader.krx.data as fdr_krx_data
    import FinanceDataReader.krx.listing as fdr_krx_listing
    import FinanceDataReader.krx.snap as fdr_krx_snap
    import requests as requests_module
    from eit_market_data.kr import krx_auth

    original_get = requests_module.get
    original_post = requests_module.post

    monkeypatch.setattr(krx_auth, "_configured_session", None)
    monkeypatch.setattr(krx_auth, "_fdr_hooks_installed", False)
    monkeypatch.setattr(fdr_krx_data, "requests", requests_module)
    monkeypatch.setattr(fdr_krx_listing, "requests", requests_module)
    monkeypatch.setattr(fdr_krx_snap, "requests", requests_module)

    class DummySession:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str, dict[str, object]]] = []

        def get(self, url: str, **kwargs):  # noqa: ANN001, ANN202
            self.calls.append(("get", url, kwargs))
            response = requests.Response()
            response.status_code = 200
            response._content = b"{}"  # noqa: SLF001
            response.headers["content-type"] = "application/json"
            response.url = url
            return response

        def post(self, url: str, **kwargs):  # noqa: ANN001, ANN202
            self.calls.append(("post", url, kwargs))
            response = requests.Response()
            response.status_code = 200
            response._content = b"{}"  # noqa: SLF001
            response.headers["content-type"] = "application/json"
            response.url = url
            return response

    session = DummySession()
    configure_krx_session(session)  # type: ignore[arg-type]

    install_fdr_krx_session_hooks()

    assert requests_module.get is original_get
    assert requests_module.post is original_post
    assert fdr_krx_data.requests is not requests_module
    assert fdr_krx_listing.requests is not requests_module
    assert fdr_krx_snap.requests is not requests_module

    fdr_krx_data.requests.get("https://data.krx.co.kr/test", headers={"X-Test": "1"})
    fdr_krx_listing.requests.post(
        "https://data.krx.co.kr/test",
        data={"foo": "bar"},
        timeout=7,
    )

    assert session.calls == [
        (
            "get",
            "https://data.krx.co.kr/test",
            {
                "headers": {
                    "User-Agent": "Mozilla/5.0",
                    "Referer": "https://data.krx.co.kr/contents/MDC/MDI/outerLoader/index.cmd",
                    "X-Test": "1",
                },
                "timeout": 30,
            },
        ),
        (
            "post",
            "https://data.krx.co.kr/test",
            {
                "data": {"foo": "bar"},
                "json": None,
                "headers": {
                    "User-Agent": "Mozilla/5.0",
                    "Referer": "https://data.krx.co.kr/contents/MDC/MDI/outerLoader/index.cmd",
                },
                "timeout": 7,
            },
        ),
    ]
