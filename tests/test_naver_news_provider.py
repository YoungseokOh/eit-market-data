from __future__ import annotations

import asyncio
import sys
import types
from datetime import date

from eit_market_data.kr.naver_news_provider import NaverNewsProvider, _parse_naver_date


class _DummyResponse:
    def __init__(
        self,
        text: str,
        *,
        content_type: str = "text/html; charset=EUC-KR",
        apparent_encoding: str = "EUC-KR",
    ) -> None:
        self.text = text
        self.headers = {"content-type": content_type}
        self.apparent_encoding = apparent_encoding
        self.encoding = ""

    def raise_for_status(self) -> None:
        return None


def _install_dummy_requests(monkeypatch, *, text: str = "", raises: bool = False) -> None:
    class DummyRequestException(Exception):
        pass

    def get(url, headers=None, timeout=10):  # noqa: ANN001, ANN202
        assert "item/main.nhn?code=" in url
        _ = (headers, timeout)
        if raises:
            raise DummyRequestException("boom")
        return _DummyResponse(text)

    dummy_module = types.SimpleNamespace(
        get=get,
        RequestException=DummyRequestException,
    )
    monkeypatch.setitem(sys.modules, "requests", dummy_module)


def test_parse_naver_date_supports_relative_and_year_rollover() -> None:
    as_of = date(2026, 1, 2)

    assert _parse_naver_date("12/31", as_of) == date(2025, 12, 31)
    assert _parse_naver_date("2일전", as_of) == date(2025, 12, 31)
    assert _parse_naver_date("3시간전", as_of) == as_of
    assert _parse_naver_date("15분전", as_of) == as_of


def test_fetch_news_parses_main_page_news_section(monkeypatch) -> None:
    _install_dummy_requests(
        monkeypatch,
        text="""
        <div class="sub_section news_section">
          <ul>
            <li>
              <span class="txt">
                <a href="/item/news_read.naver?article_id=1&office_id=001&code=005930">첫 기사</a>
              </span>
              <em>03/12</em>
            </li>
            <li>
              <span class="txt">
                <a href="/item/news_read.naver?article_id=2&office_id=002&code=005930">둘째 기사</a>
                <a class="link_relation" href="/item/news.naver?code=005930&clusterId=2">관련 2건</a>
              </span>
              <em>1일전</em>
            </li>
          </ul>
        </div>
        """,
    )

    items = asyncio.run(NaverNewsProvider().fetch_news("005930", date(2026, 3, 12)))

    assert [item.headline for item in items] == ["첫 기사", "둘째 기사"]
    assert [item.date for item in items] == [date(2026, 3, 12), date(2026, 3, 11)]
    assert all(item.source == "Naver" for item in items)
    assert all(item.summary == "" for item in items)


def test_fetch_news_returns_empty_when_section_missing(monkeypatch) -> None:
    _install_dummy_requests(monkeypatch, text="<html><body><div>no news here</div></body></html>")

    items = asyncio.run(NaverNewsProvider().fetch_news("005930", date(2026, 3, 12)))

    assert items == []


def test_fetch_news_returns_empty_on_request_failure(monkeypatch) -> None:
    _install_dummy_requests(monkeypatch, raises=True)

    items = asyncio.run(NaverNewsProvider().fetch_news("005930", date(2026, 3, 12)))

    assert items == []
