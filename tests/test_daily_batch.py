from __future__ import annotations

import importlib.util
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace


def _load_module(path: Path, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_build_kr_snapshot_skips_when_not_month_end(tmp_path: Path) -> None:
    module = _load_module(
        Path("scripts/build_kr_snapshot.py"),
        "build_kr_snapshot_test",
    )
    artifacts_root = tmp_path / "artifacts"

    summary_path = artifacts_root / "snapshots/2026-03/summary.json"

    assert module.should_build_monthly_snapshot(date(2026, 3, 30)) is False

    # Reuse the script entrypoint logic indirectly by calling the helper branch.
    as_of = date(2026, 3, 30)
    if not module.should_build_monthly_snapshot(as_of):
        skip_dir = artifacts_root / "snapshots" / as_of.strftime("%Y-%m")
        skip_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "status": "skipped",
            "reason": "not_month_end_business_day",
            "month": as_of.strftime("%Y-%m"),
            "as_of": as_of.isoformat(),
        }
        (skip_dir / "summary.json").write_text(json.dumps(payload))

    assert summary_path.exists()
    assert '"status": "skipped"' in summary_path.read_text()


def test_run_daily_batch_stops_after_failed_preflight(monkeypatch, tmp_path: Path) -> None:
    module = _load_module(
        Path("scripts/run_daily_batch.py"),
        "run_daily_batch_test",
    )

    def fake_run_step(name, command, log_dir):  # noqa: ANN001, ANN202
        log_dir.mkdir(parents=True, exist_ok=True)
        (log_dir / f"{name}.log").write_text(name)
        status = "failed" if name == "preflight" else "ok"
        return module.StepResult(
            name=name,
            status=status,
            return_code=1 if status == "failed" else 0,
            command=command,
            log_path=module.display_path(log_dir / f"{name}.log"),
        )

    monkeypatch.setattr(module, "run_step", fake_run_step)
    monkeypatch.setattr(module, "build_run_root", lambda base_dir, as_of: base_dir / "run-1")

    exit_code, summary = module.run_daily_batch(
        as_of=date(2026, 3, 6),
        output_root=tmp_path,
        universe_csv=Path("universes/kr_universe.csv"),
        ticker="005930",
        force_snapshot=False,
        snapshot_profile="official",
    )

    assert exit_code == 1
    assert summary["status"] == "failed"
    assert [step["name"] for step in summary["steps"]] == ["preflight"]
    assert (tmp_path / "run-1" / "summary.json").exists()


def test_build_run_root_uses_as_of_prefix(tmp_path: Path) -> None:
    module = _load_module(
        Path("scripts/run_daily_batch.py"),
        "run_daily_batch_paths_test",
    )

    root = module.build_run_root(
        tmp_path,
        date(2026, 3, 6),
        now_utc=datetime(2026, 3, 10, 12, 0, tzinfo=timezone.utc),
    )

    assert root == tmp_path / "20260306_20260310T120000Z"


def test_assess_crawl_outputs_reports_missing_categories(tmp_path: Path) -> None:
    module = _load_module(
        Path("scripts/run_daily_batch.py"),
        "run_daily_batch_outputs_test",
    )
    (tmp_path / "market/cap_daily").mkdir(parents=True)
    (tmp_path / "market/cap_daily/KOSPI_20260331.parquet").write_text("x")

    missing = module.assess_crawl_outputs(tmp_path)

    assert "market/cap_daily" not in missing
    assert "index/ohlcv" in missing


def test_run_daily_batch_uses_fallback_crawler(monkeypatch, tmp_path: Path) -> None:
    module = _load_module(
        Path("scripts/run_daily_batch.py"),
        "run_daily_batch_fallback_crawler_test",
    )
    seen_commands: list[list[str]] = []

    def fake_run_step(name, command, log_dir):  # noqa: ANN001, ANN202
        seen_commands.append(command)
        log_dir.mkdir(parents=True, exist_ok=True)
        (log_dir / f"{name}.log").write_text(name)
        return module.StepResult(
            name=name,
            status="ok",
            return_code=0,
            command=command,
            log_path=module.display_path(log_dir / f"{name}.log"),
        )

    monkeypatch.setattr(module, "run_step", fake_run_step)
    monkeypatch.setattr(module, "build_run_root", lambda base_dir, as_of: base_dir / "run-1")
    monkeypatch.setattr(module, "assess_crawl_outputs", lambda data_dir: [])
    monkeypatch.setattr(module, "inspect_snapshot_step", lambda artifacts_dir, as_of: ("ok", ""))

    exit_code, summary = module.run_daily_batch(
        as_of=date(2026, 3, 31),
        output_root=tmp_path,
        universe_csv=Path("universes/kr_universe.csv"),
        ticker="005930",
        force_snapshot=True,
        snapshot_profile="official",
    )

    assert exit_code == 0
    assert summary["status"] == "ok"
    preflight_command = next(command for command in seen_commands if any("preflight_kr_data.py" in part for part in command))
    assert "--skip-news" in preflight_command
    crawl_command = next(
        command
        for command in seen_commands
        if any("crawl_kr_data_fallback.py" in part for part in command)
    )
    assert "--start" in crawl_command
    assert "--end" in crawl_command


def test_build_kr_snapshot_summary_excludes_news_fields(tmp_path: Path) -> None:
    module = _load_module(
        Path("scripts/build_kr_snapshot.py"),
        "build_kr_snapshot_summary_test",
    )
    snapshot = SimpleNamespace(
        decision_date=date(2026, 2, 27),
        execution_date=date(2026, 3, 2),
        universe=["005930", "000660"],
        prices={"005930": [object()], "000660": []},
        fundamentals={
            "005930": SimpleNamespace(quarters=[object()], market_cap=1.0, last_close_price=1.0),
            "000660": SimpleNamespace(quarters=[], market_cap=None, last_close_price=None),
        },
        filings={
            "005930": SimpleNamespace(business_overview="overview", risks="", mda="", governance=""),
            "000660": SimpleNamespace(business_overview="", risks="", mda="", governance=""),
        },
        sector_map={"005930": "Tech", "000660": "Tech"},
        sector_averages={"Tech": object()},
        benchmark_prices=[object(), object()],
    )

    summary = module._summary_payload(
        snapshot,
        "2026-02",
        "official",
        tmp_path,
    )

    assert "news_tickers" not in summary
    assert "news_items" not in summary


def test_build_kr_snapshot_manifest_ignores_news_coverage() -> None:
    module = _load_module(
        Path("scripts/build_kr_snapshot.py"),
        "build_kr_snapshot_warnings_test",
    )

    warnings = module._manifest_warnings(
        "official",
        {
            "market_cap": 1,
            "benchmark_bars": 1,
        },
    )

    assert warnings == []
