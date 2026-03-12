from __future__ import annotations

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


def inspect_snapshot_step(artifacts_dir: Path, as_of: date) -> tuple[str, str]:
    summary_path = artifacts_dir / "snapshots" / as_of.strftime("%Y-%m") / "summary.json"
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
    us_universe: str = "AAPL,MSFT,GOOGL,AMZN,NVDA",
    skip_us: bool = False,
) -> tuple[int, dict[str, object]]:
    run_root = build_run_root(output_root, as_of)
    logs_dir = run_root / "logs"
    data_dir = run_root / "data"
    artifacts_dir = run_root / "artifacts"

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
            "crawl_kr_data_fallback",
            [
                sys.executable,
                "scripts/crawl_kr_data_fallback.py",
                "--start",
                f"{as_of.year}-01-01",
                "--end",
                as_of.isoformat(),
                "--universe-csv",
                str(universe_csv),
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
            str(artifacts_dir),
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
            snapshot_status, snapshot_detail = inspect_snapshot_step(artifacts_dir, as_of)
            snapshot.status = snapshot_status
            snapshot.detail = snapshot_detail

    # Build US snapshot (unless skipped)
    if not skip_us and overall_status != "failed":
        us_snapshot_cmd = [
            sys.executable,
            "scripts/build_us_snapshot.py",
            "--as-of",
            as_of.isoformat(),
            "--universe",
            us_universe,
            "--artifacts-root",
            str(artifacts_dir),
        ]
        us_snapshot = run_step("build_us_snapshot", us_snapshot_cmd, logs_dir)
        step_results.append(us_snapshot)
        if us_snapshot.status == "failed":
            overall_status = "degraded"  # US failure is degraded, not failed

    ended_at = datetime.now(timezone.utc).isoformat()
    payload = {
        "status": overall_status,
        "as_of": as_of.isoformat(),
        "started_at": started_at,
        "ended_at": ended_at,
        "run_root": display_path(run_root),
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
        default="AAPL,MSFT,GOOGL,AMZN,NVDA",
        help="Comma-separated list of US tickers (default: top 5).",
    )
    parser.add_argument(
        "--skip-us",
        action="store_true",
        help="Skip US snapshot build (KR only).",
    )
    args = parser.parse_args()

    exit_code, summary = run_daily_batch(
        as_of=resolve_as_of(args.as_of),
        output_root=Path(args.output_root),
        universe_csv=Path(args.universe_csv),
        ticker=args.ticker,
        force_snapshot=args.force_snapshot,
        snapshot_profile=args.snapshot_profile,
        us_universe=args.us_universe,
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
