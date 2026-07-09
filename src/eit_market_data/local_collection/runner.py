"""Top-level run orchestration for local KR/US collection phases."""

from __future__ import annotations

import asyncio
import subprocess
import sys
from datetime import date
from pathlib import Path

from eit_market_data.local_collection.collector import LocalKrCollector
from eit_market_data.local_collection.constants import DEFAULT_US_UNIVERSE, PROJECT_ROOT
from eit_market_data.local_collection.progress import (
    _write_json,
    build_run_root,
    default_raw_start,
    load_progress,
    save_progress,
)
from eit_market_data.local_collection.state import CheckpointPolicy
from eit_market_data.local_collection.universe import (
    build_local_universe_manifest,
    copy_pilot_universe,
    find_previous_kospi200_members,
)
from eit_market_data.local_collection.validation import (
    raise_for_failed_checks,
    summarize_checks,
    validate_kr_raw_outputs,
    validate_us_outputs,
)


def run_subprocess_stage(
    *,
    name: str,
    command: list[str],
    log_path: Path,
) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    completed = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
    )
    output = completed.stdout
    if completed.stderr:
        output = f"{output}\n{completed.stderr}" if output else completed.stderr
    log_path.write_text(output, encoding="utf-8")
    if completed.returncode != 0:
        raise RuntimeError(f"{name} failed; see {log_path}")


def run_local_collection(
    *,
    storage_root: Path,
    as_of: date,
    market: str,
    phase: str,
    full_universe_kind: str,
    start: date | None = None,
    resume: bool = False,
    us_universe: str = DEFAULT_US_UNIVERSE,
    dart_mode: str = "live",
) -> Path:
    storage_root = storage_root.expanduser().resolve()
    storage_root.mkdir(parents=True, exist_ok=True)
    run_root = build_run_root(storage_root, as_of, market, phase, full_universe_kind)
    if run_root.exists() and not resume:
        raise RuntimeError(f"Run root already exists: {run_root}")
    run_root.mkdir(parents=True, exist_ok=True)
    progress_path = run_root / "progress.json"
    progress = load_progress(
        progress_path,
        {
            "storage_root": str(storage_root),
            "run_root": str(run_root),
            "as_of": as_of.isoformat(),
            "market": market,
            "phase": phase,
            "full_universe_kind": full_universe_kind,
            "dart_mode": dart_mode,
            "stages": {},
        },
    )
    save_progress(progress_path, progress)

    raw_start = start or default_raw_start(as_of)
    month = as_of.strftime("%Y-%m")
    pilot_universe = run_root / "universes" / "kr" / "pilot" / f"{month}.csv"
    full_universe = run_root / "universes" / "kr" / full_universe_kind / f"{month}.csv"

    if market in {"kr", "both"}:
        if phase in {"pilot", "all"}:
            copy_pilot_universe(pilot_universe)
        if phase in {"full", "all"}:
            previous_members = find_previous_kospi200_members(
                storage_root=storage_root,
                as_of=as_of,
                market=market,
                phase=phase,
                kind=full_universe_kind,
            )
            build_local_universe_manifest(
                as_of=as_of,
                kind=full_universe_kind,
                output_path=full_universe,
                previous_members=previous_members,
            )

    async def _run_async() -> None:
        if market in {"kr", "both"} and phase in {"pilot", "all"}:
            await _run_kr_phase(
                stage_name="pilot",
                as_of=as_of,
                raw_start=raw_start,
                run_root=run_root,
                universe_csv=pilot_universe,
                progress_path=progress_path,
                resume=resume,
                policy=CheckpointPolicy(every_tickers=5, every_seconds=180),
                dart_mode=dart_mode,
            )

        if market in {"us", "both"} and phase in {"pilot", "all"}:
            _run_us_phase(
                stage_name="pilot",
                as_of=as_of,
                run_root=run_root,
                progress_path=progress_path,
                us_universe=us_universe,
            )

        if phase in {"full", "all"}:
            if market in {"kr", "both"}:
                await _run_kr_phase(
                    stage_name="full",
                    as_of=as_of,
                    raw_start=raw_start,
                    run_root=run_root,
                    universe_csv=full_universe,
                    progress_path=progress_path,
                    resume=resume,
                    policy=CheckpointPolicy(every_tickers=25, every_seconds=600),
                    dart_mode=dart_mode,
                )
            if market in {"us", "both"}:
                _run_us_phase(
                    stage_name="full",
                    as_of=as_of,
                    run_root=run_root,
                    progress_path=progress_path,
                    us_universe=us_universe,
                )

    asyncio.run(_run_async())
    progress = load_progress(progress_path, progress)
    progress["status"] = "completed"
    save_progress(progress_path, progress)
    return run_root


async def _run_kr_phase(
    *,
    stage_name: str,
    as_of: date,
    raw_start: date,
    run_root: Path,
    universe_csv: Path,
    progress_path: Path,
    resume: bool,
    policy: CheckpointPolicy,
    dart_mode: str,
) -> None:
    progress = load_progress(progress_path, {})
    stages = progress.setdefault("stages", {})
    raw_stage_key = f"{stage_name}_kr_raw"
    bundle_stage_key = f"{stage_name}_kr_bundle"

    raw_root = run_root / "raw" / "kr" / stage_name
    raw_report = run_root / "reports" / f"{raw_stage_key}.json"
    if stages.get(raw_stage_key, {}).get("status") != "completed":
        run_subprocess_stage(
            name=raw_stage_key,
            command=[
                sys.executable,
                "scripts/crawl_kr_data_pykrx.py",
                "--start",
                raw_start.isoformat(),
                "--end",
                as_of.isoformat(),
                "--output-root",
                str(raw_root),
                "--skip-meta",
                "--skip-ohlcv",
            ],
            log_path=run_root / "logs" / f"{raw_stage_key}.log",
        )
        raw_checks = validate_kr_raw_outputs(raw_root)
        _write_json(raw_report, summarize_checks(raw_checks))
        raise_for_failed_checks(raw_stage_key, raw_checks, raw_report)
        stages[raw_stage_key] = {"status": "completed", "report": str(raw_report)}
        save_progress(progress_path, progress)

    collector = LocalKrCollector(
        as_of=as_of,
        storage_root=run_root.parents[2],
        bundle_root=run_root / "bundles" / "kr" / stage_name,
        partial_root=run_root / "partials" / "kr" / stage_name,
        checkpoint_root=run_root / "reports" / "kr" / stage_name,
        policy=policy,
        progress_path=run_root / "reports" / "kr" / f"{stage_name}_progress.json",
        dart_mode=dart_mode,
    )
    summary = await collector.collect(universe_csv=universe_csv, resume=resume)
    stages[bundle_stage_key] = {"status": "completed", "summary": summary}
    save_progress(progress_path, progress)


def _run_us_phase(
    *,
    stage_name: str,
    as_of: date,
    run_root: Path,
    progress_path: Path,
    us_universe: str,
) -> None:
    progress = load_progress(progress_path, {})
    stages = progress.setdefault("stages", {})
    stage_key = f"{stage_name}_us_bundle"
    if stages.get(stage_key, {}).get("status") == "completed":
        return

    bundle_root = run_root / "bundles" / "us" / stage_name
    run_subprocess_stage(
        name=stage_key,
        command=[
            sys.executable,
            "scripts/build_us_snapshot.py",
            "--as-of",
            as_of.isoformat(),
            "--universe",
            us_universe,
            "--artifacts-root",
            str(bundle_root),
        ],
        log_path=run_root / "logs" / f"{stage_key}.log",
    )
    checks = validate_us_outputs(bundle_root, as_of)
    report_path = run_root / "reports" / f"{stage_key}.json"
    _write_json(report_path, summarize_checks(checks))
    raise_for_failed_checks(stage_key, checks, report_path)
    stages[stage_key] = {"status": "completed", "report": str(report_path)}
    save_progress(progress_path, progress)
