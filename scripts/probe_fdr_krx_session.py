#!/usr/bin/env python3
# ruff: noqa: E402
from __future__ import annotations

import argparse
import pickle
import sys
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from eit_market_data.kr.krx_auth import (
    AuthStatus,
    check_krx_auth,
    ensure_krx_authenticated_session,
    load_cookies_from_file,
    resolve_cookie_path,
)


@dataclass(frozen=True)
class ProbeResult:
    name: str
    ok: bool
    detail: str


def _load_session(args: argparse.Namespace):  # noqa: ANN001
    if args.pickle_path:
        return pickle.loads(Path(args.pickle_path).read_bytes())

    if args.cookie_path:
        return load_cookies_from_file(Path(args.cookie_path))

    default_cookie = resolve_cookie_path()
    if default_cookie.exists():
        return load_cookies_from_file(default_cookie)

    return ensure_krx_authenticated_session(interactive=False)


def _patch_fdr_requests(session) -> None:  # noqa: ANN001
    import FinanceDataReader.krx.data as fdr_krx_data
    import FinanceDataReader.krx.listing as fdr_krx_listing
    import FinanceDataReader.krx.snap as fdr_krx_snap

    def patched_get(url, **kwargs):  # noqa: ANN001
        kwargs.setdefault("timeout", 30)
        return session.get(url, **kwargs)

    def patched_post(url, data=None, json=None, **kwargs):  # noqa: ANN001
        kwargs.setdefault("timeout", 30)
        return session.post(url, data=data, json=json, **kwargs)

    for module in (fdr_krx_data, fdr_krx_listing, fdr_krx_snap):
        module.requests.get = patched_get
        module.requests.post = patched_post


def _probe_price(ticker: str, start: str, end: str) -> ProbeResult:
    import FinanceDataReader as fdr

    df = fdr.DataReader(f"KRX:{ticker}", start, end)
    if df is None or df.empty:
        return ProbeResult("fdr:price", False, f"{ticker} returned no rows")
    latest = df.index[-1]
    row = df.iloc[-1]
    marcap = row.get("MarCap")
    shares = row.get("Shares")
    return ProbeResult(
        "fdr:price",
        True,
        (
            f"{ticker} rows={len(df)} latest={getattr(latest, 'date', lambda: latest)()} "
            f"close={row.get('Close')} marcap={marcap} shares={shares}"
        ),
    )


def _probe_index(start: str, end: str) -> ProbeResult:
    import FinanceDataReader as fdr

    df = fdr.DataReader("KRX-INDEX:1001", start, end)
    if df is None or df.empty:
        return ProbeResult("fdr:index", False, "1001 returned no rows")
    latest = df.index[-1]
    row = df.iloc[-1]
    return ProbeResult(
        "fdr:index",
        True,
        f"1001 rows={len(df)} latest={getattr(latest, 'date', lambda: latest)()} close={row.get('Close')}",
    )


def _probe_index_stocks() -> ProbeResult:
    import FinanceDataReader as fdr

    df = fdr.SnapDataReader("KRX/INDEX/STOCK/1001")
    if df is None or df.empty:
        return ProbeResult("fdr:index-stock", False, "KRX/INDEX/STOCK/1001 returned no rows")
    return ProbeResult("fdr:index-stock", True, f"1001 constituents={len(df)}")


def _probe_live_marcap(market: str) -> ProbeResult:
    from FinanceDataReader.krx.listing import KrxMarcapListing

    df = KrxMarcapListing(market).read()
    if df is None or df.empty:
        return ProbeResult("fdr:marcap", False, f"{market} returned no rows")
    return ProbeResult(
        "fdr:marcap",
        True,
        f"{market} rows={len(df)} top={df.iloc[0].get('Code')} marcap={df.iloc[0].get('Marcap')}",
    )


def _print_auth(status: AuthStatus) -> None:
    label = "OK" if status.authenticated else "FAILED"
    print(f"[{label}] krx:auth: {status.detail}")


def _print_result(result: ProbeResult) -> None:
    label = "OK" if result.ok else "FAILED"
    print(f"[{label}] {result.name}: {result.detail}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Probe FinanceDataReader KRX routes with a shared authenticated session."
    )
    parser.add_argument("--ticker", default="005930", help="KR ticker to probe.")
    parser.add_argument("--start", default="2026-03-01", help="Start date (YYYY-MM-DD).")
    parser.add_argument("--end", default="2026-03-12", help="End date (YYYY-MM-DD).")
    parser.add_argument("--market", default="KRX", help="Market for live marcap listing.")
    parser.add_argument("--cookie-path", help="Path to exported KRX cookies.json.")
    parser.add_argument("--pickle-path", help="Path to a pickled requests.Session.")
    args = parser.parse_args()

    try:
        session = _load_session(args)
    except Exception as exc:
        print(f"[FAILED] krx:session-load: {exc}")
        raise SystemExit(1) from exc

    status = check_krx_auth(session)
    _print_auth(status)
    if not status.authenticated:
        raise SystemExit(1)

    _patch_fdr_requests(session)

    results = [
        _probe_price(args.ticker, args.start, args.end),
        _probe_index(args.start, args.end),
        _probe_index_stocks(),
        _probe_live_marcap(args.market),
    ]
    failed = 0
    for result in results:
        _print_result(result)
        failed += int(not result.ok)

    raise SystemExit(1 if failed else 0)


if __name__ == "__main__":
    main()
