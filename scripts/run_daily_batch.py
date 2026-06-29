from __future__ import annotations
# ruff: noqa: E402

from pathlib import Path as _Path
from dotenv import load_dotenv as _load_dotenv
_load_dotenv(_Path(__file__).resolve().parents[1] / ".env")

import argparse
import json
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SEOUL = ZoneInfo("Asia/Seoul")
UNIVERSE_CSV = PROJECT_ROOT / "universes/kr_universe.csv"
OUTPUT_ROOT = PROJECT_ROOT / "out"
# Stable, month-accumulating bundle tree consumed by eit-research.
# Per-run logs/data stay under out/<run>/; snapshots land here once, by market.
BUNDLE_ROOT = PROJECT_ROOT / "artifacts"

# Tiny smoke universe for explicit preflight/dev runs only. NEVER used as the
# month-end production universe — that resolves to the survivorship-free PIT list
# (see resolve_us_universe). Writing this 5-ticker list over the full month-end
# bundle is exactly the clobber bug this gate exists to prevent.
US_SMOKE_UNIVERSE = "AAPL,MSFT,GOOGL,AMZN,NVDA"


@dataclass
class StepResult:
    name: str
    status: str
    return_code: int
    command: list[str]
    log_path: str
    detail: str = ""


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path)


def us_is_month_end(as_of: date) -> bool:
    """True when ``as_of`` is the last XNYS (NYSE/Nasdaq) trading day of its month.

    Mirrors the KR ``should_build_monthly_snapshot`` gate in
    scripts/build_kr_snapshot.py, but for the US trading calendar so the US
    snapshot bundle is only (re)built at month-end and ordinary weekdays do not
    overwrite it with a partial universe.
    """
    from eit_market_data.core.calendar import _last_business_day

    return as_of == _last_business_day(as_of.year, as_of.month, "XNYS")


def resolve_us_universe(
    as_of: date,
    *,
    explicit_universe: str | None,
    universe_mode: str,
) -> list[str]:
    """Resolve the US ticker list for a snapshot build.

    Precedence:
      1. An explicit ``--us-universe`` always wins (back-compat for callers that
         pin a list), regardless of mode.
      2. ``--us-universe-mode pit`` reconstructs the survivorship-free S&P 500 +
         Nasdaq-100 universe as of ``as_of`` (network calls — only invoked at
         month-end or under --force-snapshot).
      3. Otherwise fall back to the tiny smoke list (explicit smoke/preflight).
    """
    if explicit_universe:
        return [t.strip() for t in explicit_universe.split(",") if t.strip()]
    if universe_mode == "pit":
        from eit_market_data.us_universe import pit_universe

        return pit_universe(as_of)
    return [t.strip() for t in US_SMOKE_UNIVERSE.split(",") if t.strip()]


def previous_business_day(reference_date: date) -> date:
    current = reference_date - timedelta(days=1)
    while current.weekday() >= 5:
        current -= timedelta(days=1)
    return current


def resolve_as_of(as_of_raw: str | None) -> date:
    if as_of_raw:
        return date.fromisoformat(as_of_raw)
    now_seoul = datetime.now(SEOUL).date()
    return previous_business_day(now_seoul)


def build_run_root(base_dir: Path, as_of: date, now_utc: datetime | None = None) -> Path:
    timestamp = (now_utc or datetime.now(timezone.utc)).strftime("%Y%m%dT%H%M%SZ")
    return base_dir / f"{as_of.strftime('%Y%m%d')}_{timestamp}"


def run_step(name: str, command: list[str], log_dir: Path) -> StepResult:
    log_dir.mkdir(parents=True, exist_ok=True)
    completed = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
    )
    log_path = log_dir / f"{name}.log"
    combined_output = completed.stdout
    if completed.stderr:
        combined_output = f"{combined_output}\n{completed.stderr}" if combined_output else completed.stderr
    log_path.write_text(combined_output, encoding="utf-8")

    if completed.returncode == 0:
        status = "ok"
    elif completed.returncode == 2:
        status = "degraded"
    else:
        status = "failed"
    return StepResult(
        name=name,
        status=status,
        return_code=completed.returncode,
        command=command,
        log_path=display_path(log_path),
    )


def write_summary(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def assess_crawl_outputs(data_dir: Path) -> list[str]:
    expected = {
        "market/cap_daily": "market/cap_daily/*.parquet",
        "market/fundamental": "market/fundamental/*.parquet",
        "index/ohlcv": "index/ohlcv/*.parquet",
        "market/sector": "market/sector/*.parquet",
    }
    missing: list[str] = []
    for label, pattern in expected.items():
        if not list(data_dir.glob(pattern)):
            missing.append(label)
    return missing


def inspect_snapshot_step(bundle_root: Path, as_of: date, market_subdir: str) -> tuple[str, str]:
    summary_path = (
        bundle_root / market_subdir / "snapshots" / as_of.strftime("%Y-%m") / "summary.json"
    )
    if not summary_path.exists():
        return "ok", ""
    try:
        payload = json.loads(summary_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return "ok", ""
    status = str(payload.get("status", "ok"))
    if status == "skipped":
        return "skipped", str(payload.get("reason", ""))
    return "ok", ""


def run_daily_batch(
    *,
    as_of: date,
    output_root: Path,
    universe_csv: Path,
    ticker: str,
    force_snapshot: bool,
    snapshot_profile: str,
    bundle_root: Path = BUNDLE_ROOT,
    us_universe: str | None = None,
    us_universe_mode: str = "pit",
    skip_us: bool = False,
) -> tuple[int, dict[str, object]]:
    run_root = build_run_root(output_root, as_of)
    logs_dir = run_root / "logs"
    data_dir = run_root / "data"

    started_at = datetime.now(timezone.utc).isoformat()
    step_results: list[StepResult] = []
    overall_status = "ok"

    preflight = run_step(
        "preflight",
        [
            sys.executable,
            "scripts/preflight_kr_data.py",
            "--as-of",
            as_of.isoformat(),
            "--ticker",
            ticker,
            "--skip-news",
        ],
        logs_dir,
    )
    step_results.append(preflight)
    if preflight.status == "failed":
        overall_status = "failed"
    elif preflight.status == "degraded":
        overall_status = "degraded"

    if overall_status != "failed":
        crawl = run_step(
            "crawl_kr_data_pykrx",
            [
                sys.executable,
                "scripts/crawl_kr_data_pykrx.py",
                "--start",
                f"{as_of.year}-01-01",
                "--end",
                as_of.isoformat(),
                "--output-root",
                str(data_dir),
            ],
            logs_dir,
        )
        step_results.append(crawl)
        if crawl.status == "failed":
            overall_status = "failed"
        else:
            missing_outputs = assess_crawl_outputs(data_dir)
            if missing_outputs:
                crawl.status = "degraded"
                crawl.detail = f"missing_outputs={','.join(missing_outputs)}"
                overall_status = "degraded"

    if overall_status != "failed":
        snapshot_cmd = [
            sys.executable,
            "scripts/build_kr_snapshot.py",
            "--as-of",
            as_of.isoformat(),
            "--universe-csv",
            str(universe_csv),
            "--artifacts-root",
            str(bundle_root),
            "--market-subdir",
            "kr",
            "--profile",
            snapshot_profile,
        ]
        if force_snapshot:
            snapshot_cmd.append("--force")
        snapshot = run_step("build_kr_snapshot", snapshot_cmd, logs_dir)
        step_results.append(snapshot)
        if snapshot.status == "failed":
            overall_status = "failed"
        elif snapshot.status == "degraded":
            overall_status = "degraded"
        else:
            snapshot_status, snapshot_detail = inspect_snapshot_step(bundle_root, as_of, "kr")
            snapshot.status = snapshot_status
            snapshot.detail = snapshot_detail

    # Build US snapshot — month-end gated, mirroring the KR snapshot gate. On
    # ordinary weekdays the full month-end bundle is left untouched; only the
    # last XNYS trading day of the month (or an explicit --force-snapshot)
    # rebuilds it, and then with the survivorship-free PIT universe rather than
    # the 5-ticker smoke list. This prevents the daily job from clobbering the
    # ~512-ticker bundle with a partial one.
    if not skip_us and overall_status != "failed":
        build_us = force_snapshot or us_is_month_end(as_of)
        if not build_us:
            us_snapshot = StepResult(
                name="build_us_snapshot",
                status="skipped",
                return_code=0,
                command=[],
                log_path="",
                detail="not_month_end_business_day",
            )
            step_results.append(us_snapshot)
        else:
            resolved_universe = resolve_us_universe(
                as_of,
                explicit_universe=us_universe,
                universe_mode=us_universe_mode,
            )
            us_snapshot_cmd = [
                sys.executable,
                "scripts/build_us_snapshot.py",
                "--as-of",
                as_of.isoformat(),
                "--universe",
                ",".join(resolved_universe),
                "--artifacts-root",
                str(bundle_root),
                "--market-subdir",
                "us",
            ]
            us_snapshot = run_step("build_us_snapshot", us_snapshot_cmd, logs_dir)
            step_results.append(us_snapshot)
            if us_snapshot.status == "failed":
                overall_status = "degraded"  # US failure is degraded, not failed
            else:
                us_status, us_detail = inspect_snapshot_step(bundle_root, as_of, "us")
                us_snapshot.status = us_status
                us_snapshot.detail = us_detail

    # Refresh the per-ticker daily OHLCV price store every run (NOT month-gated,
    # unlike the snapshot bundle). The store universe is derived from the
    # published bundles so it stays consistent with them; the live provider
    # appends only trading days after the latest bundle. A store failure is
    # degrading, never fatal.
    if overall_status != "failed":
        store_markets = ["kr"] if skip_us else ["kr", "us"]
        for store_market in store_markets:
            store_cmd = [
                sys.executable,
                "scripts/build_daily_price_store.py",
                "--market",
                store_market,
                "--artifacts-root",
                str(bundle_root),
                "--source",
                "auto",
                "--end",
                as_of.isoformat(),
            ]
            store_step = run_step(f"daily_price_store_{store_market}", store_cmd, logs_dir)
            step_results.append(store_step)
            if store_step.status == "failed" and overall_status == "ok":
                overall_status = "degraded"

    ended_at = datetime.now(timezone.utc).isoformat()
    payload = {
        "status": overall_status,
        "as_of": as_of.isoformat(),
        "started_at": started_at,
        "ended_at": ended_at,
        "run_root": display_path(run_root),
        "bundle_root": display_path(bundle_root),
        "steps": [asdict(step) for step in step_results],
    }
    write_summary(run_root / "summary.json", payload)

    if overall_status == "failed":
        return 1, payload
    if overall_status == "degraded":
        return 2, payload
    return 0, payload


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the daily market data batch (KR + US)."
    )
    parser.add_argument(
        "--as-of",
        help="Reference date in YYYY-MM-DD. Defaults to previous Seoul business day.",
    )
    parser.add_argument("--ticker", default="005930", help="Ticker for preflight validation.")
    parser.add_argument(
        "--universe-csv",
        default=str(UNIVERSE_CSV),
        help="Universe CSV path for KR market.",
    )
    parser.add_argument(
        "--output-root",
        default=str(OUTPUT_ROOT),
        help="Base output directory for batch artifacts.",
    )
    parser.add_argument(
        "--force-snapshot",
        action="store_true",
        help="Build the monthly snapshot even when as-of is not month-end.",
    )
    parser.add_argument(
        "--snapshot-profile",
        default="official",
        choices=["official", "official_enriched", "ci_safe"],
        help="Profile passed through to scripts/build_kr_snapshot.py.",
    )
    parser.add_argument(
        "--us-universe",
        default=None,
        help="Explicit comma-separated US tickers. Overrides --us-universe-mode "
        "(back-compat). When omitted, the universe is resolved from "
        "--us-universe-mode.",
    )
    parser.add_argument(
        "--us-universe-mode",
        default="pit",
        choices=["pit", "smoke"],
        help="US universe resolution when --us-universe is not given. "
        "pit = survivorship-free S&P500+NDX as of as-of (full month-end bundle); "
        "smoke = 5-ticker preflight list. Default pit.",
    )
    parser.add_argument(
        "--skip-us",
        action="store_true",
        help="Skip US snapshot build (KR only).",
    )
    parser.add_argument(
        "--bundle-root",
        default=str(BUNDLE_ROOT),
        help="Stable bundle tree consumed by eit-research (snapshots/<market>/...).",
    )
    args = parser.parse_args()

    exit_code, summary = run_daily_batch(
        as_of=resolve_as_of(args.as_of),
        output_root=Path(args.output_root),
        universe_csv=Path(args.universe_csv),
        ticker=args.ticker,
        force_snapshot=args.force_snapshot,
        snapshot_profile=args.snapshot_profile,
        bundle_root=Path(args.bundle_root),
        us_universe=args.us_universe,
        us_universe_mode=args.us_universe_mode,
        skip_us=args.skip_us,
    )
    print(
        f"[SUMMARY] status={summary['status']} "
        f"as_of={summary['as_of']} "
        f"run_root={summary['run_root']}"
    )
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
