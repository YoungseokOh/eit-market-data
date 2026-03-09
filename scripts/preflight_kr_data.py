#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import os
import platform
import socket
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from eit_market_data.kr.dart_provider import DartProvider
from eit_market_data.kr.ecos_provider import EcosMacroProvider
from eit_market_data.kr.market_helpers import load_sector_snapshot_map
from eit_market_data.kr.pykrx_provider import PykrxProvider

HOSTS = (
    "data.krx.co.kr",
    "fchart.stock.naver.com",
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
        return CheckResult("resolver", "failed", "no nameserver entries in /etc/resolv.conf")
    return CheckResult("resolver", "ok", ", ".join(nameservers))


def _check_dns(host: str) -> CheckResult:
    try:
        infos = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    except Exception as exc:
        return CheckResult(f"dns:{host}", "failed", str(exc))

    addresses = sorted({info[4][0] for info in infos})
    return CheckResult(f"dns:{host}", "ok", ", ".join(addresses[:3]))


async def _check_market_stack(as_of: date, ticker: str) -> list[CheckResult]:
    results: list[CheckResult] = []
    provider = PykrxProvider()

    prices = await provider.fetch_prices(ticker, as_of, lookback_days=20)
    if prices:
        results.append(
            CheckResult(
                "pykrx:prices",
                "ok",
                f"{ticker} bars={len(prices)} last={prices[-1].date}",
            )
        )
    else:
        results.append(CheckResult("pykrx:prices", "failed", f"{ticker} returned no bars"))

    benchmark = await provider.fetch_benchmark(as_of, lookback_days=20)
    if benchmark:
        results.append(
            CheckResult(
                "pykrx:benchmark",
                "ok",
                f"bars={len(benchmark)} last={benchmark[-1].date}",
            )
        )
    else:
        results.append(CheckResult("pykrx:benchmark", "failed", "benchmark returned no bars"))

    sectors = await provider.fetch_sector_map([ticker], as_of=as_of)
    sector_name = sectors.get(ticker, "General")
    if sector_name and sector_name != "General":
        results.append(CheckResult("pykrx:sector", "ok", sector_name))
    else:
        results.append(CheckResult("pykrx:sector", "degraded", sector_name or "missing"))

    snapshot_map, snapshot_path = load_sector_snapshot_map("KOSPI", as_of)
    if snapshot_path is None or not snapshot_map:
        results.append(CheckResult("sector-snapshot", "failed", "no cached KOSPI snapshot"))
    else:
        age_days = (as_of - date.fromisoformat(snapshot_path.stem.rsplit("_", 1)[-1][:4] + "-" + snapshot_path.stem.rsplit("_", 1)[-1][4:6] + "-" + snapshot_path.stem.rsplit("_", 1)[-1][6:8])).days
        status = "ok" if age_days <= 31 else "degraded"
        results.append(
            CheckResult(
                "sector-snapshot",
                status,
                f"{snapshot_path.name} age_days={age_days}",
            )
        )

    return results


async def _check_dart(as_of: date, ticker: str) -> CheckResult:
    if not os.environ.get("DART_API_KEY"):
        return CheckResult("dart", "failed", "DART_API_KEY missing")

    try:
        provider = DartProvider()
        fundamentals = await provider.fetch_fundamentals(ticker, as_of, n_quarters=1)
    except Exception as exc:
        return CheckResult("dart", "failed", str(exc))

    if fundamentals.quarters:
        return CheckResult(
            "dart",
            "ok",
            f"quarters={len(fundamentals.quarters)} latest={fundamentals.quarters[0].fiscal_quarter}",
        )
    return CheckResult("dart", "degraded", "no fundamentals returned")


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


def _print_result(result: CheckResult) -> None:
    print(f"[{result.status.upper()}] {result.name}: {result.detail}")


async def _run_checks(as_of: date, ticker: str) -> list[CheckResult]:
    results = [_check_wsl(), _check_resolver()]
    results.extend(_check_dns(host) for host in HOSTS)
    results.extend(await _check_market_stack(as_of, ticker))
    results.append(await _check_dart(as_of, ticker))
    results.append(await _check_ecos(as_of))
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="KR data preflight after reboot/WSL changes.")
    parser.add_argument("--as-of", default=date.today().isoformat(), help="Probe date (YYYY-MM-DD)")
    parser.add_argument("--ticker", default="005930", help="KR ticker to probe")
    args = parser.parse_args()

    as_of = date.fromisoformat(args.as_of)
    results = asyncio.run(_run_checks(as_of, args.ticker))
    for result in results:
        _print_result(result)

    failed = sum(1 for result in results if result.status == "failed")
    degraded = sum(1 for result in results if result.status == "degraded")
    print(f"[SUMMARY] ok={len(results) - failed - degraded} degraded={degraded} failed={failed}")

    if failed:
        raise SystemExit(1)
    if degraded:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
