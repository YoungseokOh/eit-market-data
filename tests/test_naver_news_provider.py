from __future__ import annotations
import sys
import types
from datetime import date
from urllib.parse import parse_qs, urlparse

from eit_market_data.kr.naver_news_provider import (
    NaverArchiveNewsProvider,
    NaverNewsProvider,
    _parse_naver_date,
)


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


def _install_dummy_archive_requests(monkeypatch, pages: dict[int, str], *, expected_referer: str | None = None) -> None:
    class DummyRequestException(Exception):
        pass

    class DummySession:
        def get(self, url, headers=None, timeout=10):  # noqa: ANN001, ANN202
            _ = (headers, timeout)
            if expected_referer is not None:
                assert headers is not None
                assert headers.get("Referer") == expected_referer
            page = int(parse_qs(urlparse(url).query).get("page", ["1"])[0])
            return _DummyResponse(pages.get(page, ""))

        def close(self) -> None:
            return None

    dummy_module = types.SimpleNamespace(
        Session=DummySession,
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

    items = NaverNewsProvider()._fetch_news_sync("005930", date(2026, 3, 12), 30)

    assert [item.headline for item in items] == ["첫 기사", "둘째 기사"]
    assert [item.date for item in items] == [date(2026, 3, 12), date(2026, 3, 11)]
    assert all(item.source == "Naver" for item in items)
    assert all(item.summary == "" for item in items)


def test_fetch_news_returns_empty_when_section_missing(monkeypatch) -> None:
    _install_dummy_requests(monkeypatch, text="<html><body><div>no news here</div></body></html>")

    items = NaverNewsProvider()._fetch_news_sync("005930", date(2026, 3, 12), 30)

    assert items == []


def test_fetch_news_returns_empty_on_request_failure(monkeypatch) -> None:
    _install_dummy_requests(monkeypatch, raises=True)

    items = NaverNewsProvider()._fetch_news_sync("005930", date(2026, 3, 12), 30)

    assert items == []


def test_fetch_archive_records_sync_dedupes_and_keeps_target_month(monkeypatch) -> None:
    _install_dummy_archive_requests(
        monkeypatch,
        pages={
            1: """
            <table class="type5">
              <tr>
                <td class="title"><a href="/item/news_read.naver?article_id=1&code=005930">첫 기사</a></td>
                <td class="info">연합</td>
                <td class="date">2026.03.12</td>
              </tr>
              <tr>
                <td class="title"><a href="/item/news_read.naver?article_id=2&code=005930">둘째 기사</a></td>
                <td class="info">매체B</td>
                <td class="date">2026.03.10</td>
              </tr>
            </table>
            """,
            2: """
            <table class="type5">
              <tr>
                <td class="title"><a href="/item/news_read.naver?article_id=2&code=005930">둘째 기사</a></td>
                <td class="info">매체B</td>
                <td class="date">2026.03.10</td>
              </tr>
              <tr>
                <td class="title"><a href="/item/news_read.naver?article_id=3&code=005930">셋째 기사</a></td>
                <td class="info">매체C</td>
                <td class="date">2026.03.01</td>
              </tr>
              <tr>
                <td class="title"><a href="/item/news_read.naver?article_id=4&code=005930">이전달 기사</a></td>
                <td class="info">매체D</td>
                <td class="date">2026.02.28</td>
              </tr>
            </table>
            """,
        },
        expected_referer="https://finance.naver.com/item/news.naver?code=005930",
    )

    records = NaverArchiveNewsProvider(
        max_pages=5,
        page_delay_seconds=0,
    )._fetch_archive_records_sync(
        "005930",
        date(2026, 3, 12),
        lookback_days=12,
    )

    assert [record.headline for record in records] == ["첫 기사", "둘째 기사", "셋째 기사"]
    assert [record.date for record in records] == [
        date(2026, 3, 12),
        date(2026, 3, 10),
        date(2026, 3, 1),
    ]
    assert len({record.url for record in records}) == 3


def test_fetch_archive_records_sync_raises_when_full_coverage_not_reached(monkeypatch) -> None:
    _install_dummy_archive_requests(
        monkeypatch,
        pages={
            1: """
            <table class="type5">
              <tr>
                <td class="title"><a href="/item/news_read.naver?article_id=1&code=005930">첫 기사</a></td>
                <td class="info">연합</td>
                <td class="date">2026.03.12</td>
              </tr>
            </table>
            """,
        },
        expected_referer="https://finance.naver.com/item/news.naver?code=005930",
    )

    provider = NaverArchiveNewsProvider(
        max_pages=1,
        page_delay_seconds=0,
        require_full_coverage=True,
        raise_on_error=True,
    )

    try:
        provider._fetch_archive_records_sync("005930", date(2026, 3, 12), 12)
    except RuntimeError as exc:
        assert "did not reach required start date" in str(exc)
    else:
        raise AssertionError("expected strict coverage failure")
