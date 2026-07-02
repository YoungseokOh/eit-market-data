from __future__ import annotations

import tempfile

import pytest

import eit_market_data.edgar_provider as ep


class _FakeResp:
    def __init__(self, text: str) -> None:
        self.text = text

    def raise_for_status(self) -> None:
        pass


class _FakeClient:
    def __init__(self) -> None:
        self.calls = 0

    async def get(self, url: str) -> _FakeResp:
        self.calls += 1
        return _FakeResp(f"BODY::{url}")


@pytest.fixture(autouse=True)
def _isolate_cache(monkeypatch):
    """Point the filing cache at a temp dir and reset the module handle."""
    monkeypatch.setenv("EIT_EDGAR_FILING_CACHE_DIR", tempfile.mkdtemp(prefix="edgcache_"))
    ep._filing_cache = None
    yield
    if ep._filing_cache is not None:
        ep._filing_cache.close()
    ep._filing_cache = None


@pytest.mark.asyncio
async def test_cache_hit_avoids_second_fetch(monkeypatch):
    monkeypatch.setenv("EIT_EDGAR_FILING_CACHE", "1")
    ep._filing_cache = None
    client = _FakeClient()
    url = "https://www.sec.gov/Archives/edgar/data/320193/x/aapl-10k.htm"

    first = await ep._rate_limited_get(client, url)
    second = await ep._rate_limited_get(client, url)

    assert first == second == f"BODY::{url}"
    assert client.calls == 1  # second call served from cache


@pytest.mark.asyncio
async def test_distinct_urls_each_fetch_once(monkeypatch):
    monkeypatch.setenv("EIT_EDGAR_FILING_CACHE", "1")
    ep._filing_cache = None
    client = _FakeClient()

    await ep._rate_limited_get(client, "https://sec.gov/a")
    await ep._rate_limited_get(client, "https://sec.gov/b")
    await ep._rate_limited_get(client, "https://sec.gov/a")

    assert client.calls == 2  # 'a' cached on repeat, 'b' fetched once


@pytest.mark.asyncio
async def test_disabled_by_default_no_caching(monkeypatch):
    monkeypatch.delenv("EIT_EDGAR_FILING_CACHE", raising=False)
    ep._filing_cache = None
    client = _FakeClient()
    url = "https://www.sec.gov/Archives/edgar/data/1/x/z-10k.htm"

    await ep._rate_limited_get(client, url)
    await ep._rate_limited_get(client, url)

    assert client.calls == 2  # no cache => every call hits the network
