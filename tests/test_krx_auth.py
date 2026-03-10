from __future__ import annotations

from pathlib import Path

import requests

from eit_market_data.kr.krx_auth import (
    auth_failure_reason,
    build_krx_session,
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
