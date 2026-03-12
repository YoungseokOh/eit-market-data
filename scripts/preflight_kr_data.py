#!/usr/bin/env python3
# ruff: noqa: E402
from __future__ import annotations

import argparse
import asyncio
import os
import platform
import socket
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

from eit_market_data.kr.ecos_provider import EcosMacroProvider
from eit_market_data.kr.fundamental_provider import CompositeKrFundamentalProvider
from eit_market_data.kr.market_helpers import (
    fetch_index_ohlcv_frame,
    fetch_market_cap_frame,
    fetch_market_ticker_list,
)
from eit_market_data.kr.pykrx_provider import PykrxProvider
from eit_market_data.kr.naver_news_provider import NaverNewsProvider

HOSTS = (
    "data.krx.co.kr",
    "finance.naver.com",
    "opendart.fss.or.kr",
    "ecos.bok.or.kr",
)
CRITICAL_MACRO_KEYS = (
    "base_rate",
    "yield_3y",
    "yield_10y",
    "usd_krw",
    "gdp_growth_yoy",
    "unemployment_rate",
    "trade_balance",
)


@dataclass(frozen=True)
class CheckResult:
    name: str
    status: str
    detail: str


def _read_nameservers() -> list[str]:
    path = Path("/etc/resolv.conf")
    if not path.exists():
        return []

    nameservers: list[str] = []
    for line in path.read_text().splitlines():
        stripped = line.strip()
        if stripped.startswith("nameserver "):
            nameservers.append(stripped.split(maxsplit=1)[1])
    return nameservers


def _check_wsl() -> CheckResult:
    release = platform.release().lower()
    is_wsl = "microsoft" in release or bool(os.environ.get("WSL_DISTRO_NAME"))
    status = "ok" if is_wsl else "degraded"
    return CheckResult("wsl", status, platform.release())


def _check_resolver() -> CheckResult:
    nameservers = _read_nameservers()
    if not nameservers:
        return CheckResult(
            "resolver", "failed", "no nameserver entries in /etc/resolv.conf"
        )
    return CheckResult("resolver", "ok", ", ".join(nameservers))


def _check_dns(host: str) -> CheckResult:
    try:
        infos = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    except Exception as exc:
        return CheckResult(f"dns:{host}", "failed", str(exc))

    addresses = sorted({info[4][0] for info in infos})
    return CheckResult(f"dns:{host}", "ok", ", ".join(addresses[:3]))


def _probe_naver_news_links(ticker: str) -> int:
    try:
        import requests
        from bs4 import BeautifulSoup
    except ImportError as exc:
        raise RuntimeError("requests/BeautifulSoup are required for Naver news preflight") from exc

    response = requests.get(
        f"https://finance.naver.com/item/main.nhn?code={ticker}",
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=10,
    )
    response.raise_for_status()
    content_type = str(response.headers.get("content-type", "")).lower()
    if "charset=" in content_type:
        response.encoding = content_type.split("charset=", 1)[1].split(";", 1)[0].strip()
    elif getattr(response, "apparent_encoding", ""):
        response.encoding = response.apparent_encoding

    soup = BeautifulSoup(response.text, "html.parser")
    container = soup.select_one("div.sub_section.news_section")
    if container is None:
        return 0

    links = {
        str(anchor.get("href", "")).strip()
        for anchor in container.select('span.txt > a[href*="/item/news_read.naver"]')
        if str(anchor.get("href", "")).strip()
    }
    return len(links)


async def _check_market_stack(as_of: date, ticker: str) -> list[CheckResult]:
    results: list[CheckResult] = []
    provider = PykrxProvider(official_only=False)

    prices = await provider.fetch_prices(ticker, as_of, lookback_days=20)
    if prices:
        results.append(
            CheckResult(
                "public:prices",
                "ok",
                f"{ticker} bars={len(prices)} last={prices[-1].date}",
            )
        )
    else:
        results.append(
            CheckResult("public:prices", "failed", f"{ticker} returned no bars")
        )

    try:
        tickers = fetch_market_ticker_list(as_of, "KOSPI")
    except Exception as exc:
        results.append(CheckResult("public:ticker-list", "failed", str(exc)))
    else:
        if tickers:
            results.append(
                CheckResult("public:ticker-list", "ok", f"KOSPI tickers={len(tickers)}")
            )
        else:
            results.append(
                CheckResult("public:ticker-list", "failed", "KOSPI returned no tickers")
            )

    try:
        cap_frame = fetch_market_cap_frame(as_of, "KOSPI")
    except Exception as exc:
        results.append(CheckResult("public:market-cap", "failed", str(exc)))
    else:
        if cap_frame is not None and not cap_frame.empty:
            results.append(
                CheckResult("public:market-cap", "ok", f"KOSPI rows={len(cap_frame)}")
            )
        else:
            results.append(
                CheckResult("public:market-cap", "degraded", "KOSPI market cap unavailable")
            )

    benchmark_start = as_of - timedelta(days=40)
    try:
        benchmark_frame, benchmark_source = fetch_index_ohlcv_frame(
            "1001",
            benchmark_start,
            as_of,
            official_only=False,
        )
        benchmark = await provider.fetch_benchmark(as_of, lookback_days=20)
    except Exception as exc:
        results.append(CheckResult("public:benchmark", "failed", str(exc)))
    else:
        if benchmark and benchmark_source in {"fdr", "pykrx"} and benchmark_frame is not None:
            results.append(
                CheckResult(
                    "public:benchmark",
                    "ok",
                    f"source={benchmark_source} bars={len(benchmark)} last={benchmark[-1].date}",
                )
            )
        else:
            results.append(
                CheckResult(
                    "public:benchmark",
                    "failed",
                    f"source={benchmark_source or 'missing'} benchmark returned no bars",
                )
            )

    sectors = await provider.fetch_sector_map([ticker], as_of=as_of)
    sector_name = sectors.get(ticker, "General")
    if sector_name and sector_name != "General":
        results.append(CheckResult("public:sector", "ok", sector_name))
    else:
        results.append(
            CheckResult("public:sector", "degraded", sector_name or "missing")
        )

    return results


async def _check_dart(as_of: date, ticker: str) -> CheckResult:
    if not os.environ.get("DART_API_KEY"):
        return CheckResult("dart", "failed", "DART_API_KEY missing")

    try:
        provider = CompositeKrFundamentalProvider()
        fundamentals = await provider.fetch_fundamentals(ticker, as_of, n_quarters=4)
    except Exception as exc:
        return CheckResult("dart", "failed", str(exc))

    if len(fundamentals.quarters) < 4:
        return CheckResult("dart", "degraded", f"quarters={len(fundamentals.quarters)}")

    latest = fundamentals.quarters[0]
    missing = [
        field
        for field in (
            "revenue",
            "operating_income",
            "net_income",
            "total_assets",
            "total_equity",
        )
        if getattr(latest, field) is None
    ]
    if latest.report_date > as_of:
        missing.append("report_date")
    if fundamentals.market_cap is None:
        missing.append("market_cap")
    if fundamentals.last_close_price is None:
        missing.append("last_close_price")

    if not missing:
        return CheckResult(
            "dart",
            "ok",
            (
                f"quarters={len(fundamentals.quarters)} "
                f"latest={latest.fiscal_quarter} "
                f"report_date={latest.report_date}"
            ),
        )
    return CheckResult("dart", "degraded", f"missing={','.join(missing)}")


async def _check_ecos(as_of: date) -> CheckResult:
    if not os.environ.get("ECOS_API_KEY"):
        return CheckResult("ecos", "failed", "ECOS_API_KEY missing")

    try:
        provider = EcosMacroProvider()
        macro = await provider.fetch_macro(as_of)
    except Exception as exc:
        return CheckResult("ecos", "failed", str(exc))

    macro_keys = {
        *macro.rates_policy.keys(),
        *macro.inflation_commodities.keys(),
        *macro.growth_economy.keys(),
        *macro.market_risk.keys(),
    }
    missing = [key for key in CRITICAL_MACRO_KEYS if key not in macro_keys]
    if not macro_keys:
        return CheckResult("ecos", "failed", "macro payload empty")
    if missing:
        return CheckResult("ecos", "degraded", f"missing={','.join(missing)}")
    return CheckResult("ecos", "ok", f"keys={','.join(sorted(macro_keys))}")


async def _check_news(as_of: date, ticker: str) -> CheckResult:
    provider = NaverNewsProvider()
    try:
        items = await provider.fetch_news(ticker, as_of)
    except Exception as exc:
        return CheckResult("naver:news", "failed", str(exc))

    valid_items = [
        item
        for item in items
        if item.headline and item.date is not None and item.date <= as_of
    ]
    if valid_items:
        latest = max(item.date for item in valid_items)
        return CheckResult(
            "naver:news",
            "ok",
            f"{ticker} items={len(valid_items)} latest={latest.isoformat()}",
        )

    try:
        raw_links = _probe_naver_news_links(ticker)
    except Exception as exc:
        return CheckResult("naver:news", "failed", f"raw_probe_failed={exc}")

    if raw_links > 0:
        return CheckResult(
            "naver:news",
            "failed",
            f"{ticker} provider_returned_no_items raw_links={raw_links}",
        )
    return CheckResult(
        "naver:news",
        "degraded",
        f"{ticker} provider_returned_no_items raw_links=0",
    )


def _print_result(result: CheckResult) -> None:
    print(f"[{result.status.upper()}] {result.name}: {result.detail}")


async def _run_checks(as_of: date, ticker: str) -> list[CheckResult]:
    results = [_check_wsl(), _check_resolver()]
    results.extend(_check_dns(host) for host in HOSTS)
    results.extend(await _check_market_stack(as_of, ticker))
    results.append(await _check_news(as_of, ticker))
    results.append(await _check_dart(as_of, ticker))
    results.append(await _check_ecos(as_of))
    return results


def main() -> None:
    parser = argparse.ArgumentParser(
        description="KR data preflight after reboot/WSL changes."
    )
    parser.add_argument(
        "--as-of", default=date.today().isoformat(), help="Probe date (YYYY-MM-DD)"
    )
    parser.add_argument("--ticker", default="005930", help="KR ticker to probe")
    args = parser.parse_args()

    as_of = date.fromisoformat(args.as_of)
    results = asyncio.run(_run_checks(as_of, args.ticker))
    for result in results:
        _print_result(result)

    failed = sum(1 for result in results if result.status == "failed")
    degraded = sum(1 for result in results if result.status == "degraded")
    print(
        f"[SUMMARY] ok={len(results) - failed - degraded} degraded={degraded} failed={failed}"
    )

    if failed:
        raise SystemExit(1)
    if degraded:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
