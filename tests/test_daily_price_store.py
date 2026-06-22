from __future__ import annotations

import csv
import importlib.util
import json
import sys
from datetime import date
from pathlib import Path


def _load_module():
    path = Path("scripts/build_daily_price_store.py")
    spec = importlib.util.spec_from_file_location("build_daily_price_store_test", path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    sys.modules["build_daily_price_store_test"] = module
    spec.loader.exec_module(module)
    return module


def _bar(d: str, o: float, h: float, lo: float, c: float, v: float) -> dict:
    return {"date": d, "open": o, "high": h, "low": lo, "close": c, "volume": v}


def _write_bundle(root: Path, market: str, month: str, prices: dict[str, list[dict]]) -> None:
    snap_dir = root / market / "snapshots" / month
    snap_dir.mkdir(parents=True, exist_ok=True)
    payload = {"decision_date": f"{month}-28", "prices": prices}
    (snap_dir / "snapshot.json").write_text(json.dumps(payload), encoding="utf-8")


def _read_store(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _build(module, root: Path, market: str = "us", **kw):
    return module.build_daily_price_store(
        market=market,
        artifacts_root=root,
        universe_csv=None,
        start=None,
        end=None,
        source="bundle",
        rebuild=kw.pop("rebuild", False),
        min_rows=kw.pop("min_rows", 1),
        tail_lookback_days=10,
        **kw,
    )


def test_schema_and_ordering(tmp_path: Path) -> None:
    module = _load_module()
    _write_bundle(
        tmp_path,
        "us",
        "2026-01",
        {"AAPL": [_bar("2026-01-02", 1, 2, 0.5, 1.5, 100), _bar("2026-01-03", 1.5, 2.5, 1, 2, 200)]},
    )
    _build(module, tmp_path)
    store = tmp_path / "us" / "daily_prices" / "AAPL.csv"
    with store.open() as h:
        header = h.readline().strip()
    assert header == "date,open,high,low,close,volume"
    rows = _read_store(store)
    dates = [r["date"] for r in rows]
    assert dates == sorted(dates)
    assert dates == ["2026-01-02", "2026-01-03"]


def test_later_bundle_wins_on_overlap(tmp_path: Path) -> None:
    """Overlapping dates take the later bundle's value (latest adjustment basis)."""
    module = _load_module()
    _write_bundle(tmp_path, "us", "2026-01", {"AAPL": [_bar("2026-01-30", 10, 10, 10, 10, 1)]})
    # Feb bundle re-reports the same date with a re-adjusted close.
    _write_bundle(
        tmp_path,
        "us",
        "2026-02",
        {"AAPL": [_bar("2026-01-30", 5, 5, 5, 5, 1), _bar("2026-02-02", 6, 6, 6, 6, 2)]},
    )
    _build(module, tmp_path)
    rows = {r["date"]: r for r in _read_store(tmp_path / "us" / "daily_prices" / "AAPL.csv")}
    assert float(rows["2026-01-30"]["close"]) == 5.0  # later bundle won
    assert float(rows["2026-02-02"]["close"]) == 6.0


def test_consistency_with_bundle(tmp_path: Path) -> None:
    """Store equals the latest-wins bundle union for every overlapping cell."""
    module = _load_module()
    jan = {"AAPL": [_bar("2026-01-30", 1, 2, 0.5, 1.7, 10)]}
    feb = {"AAPL": [_bar("2026-01-30", 1, 2, 0.5, 1.7, 10), _bar("2026-02-27", 3, 4, 2, 3.3, 20)]}
    _write_bundle(tmp_path, "us", "2026-01", jan)
    _write_bundle(tmp_path, "us", "2026-02", feb)
    _build(module, tmp_path)
    rows = {r["date"]: r for r in _read_store(tmp_path / "us" / "daily_prices" / "AAPL.csv")}
    for bar in feb["AAPL"]:
        for col in ("open", "high", "low", "close", "volume"):
            assert abs(float(rows[bar.__getitem__("date")][col]) - float(bar[col])) < 1e-9


def test_idempotent_rerun(tmp_path: Path) -> None:
    module = _load_module()
    _write_bundle(
        tmp_path,
        "us",
        "2026-01",
        {"AAPL": [_bar("2026-01-02", 1, 2, 0.5, 1.5, 100), _bar("2026-01-03", 1.5, 2.5, 1, 2, 200)]},
    )
    _build(module, tmp_path)
    store = tmp_path / "us" / "daily_prices" / "AAPL.csv"
    first = store.read_text()
    _build(module, tmp_path)
    assert store.read_text() == first


def test_incremental_appends_only_new_days(tmp_path: Path) -> None:
    module = _load_module()
    _write_bundle(tmp_path, "us", "2026-01", {"AAPL": [_bar("2026-01-30", 1, 2, 0.5, 1.5, 100)]})
    _build(module, tmp_path)
    store = tmp_path / "us" / "daily_prices" / "AAPL.csv"
    assert len(_read_store(store)) == 1
    # A later bundle adds one new trading day; existing row must be untouched.
    _write_bundle(
        tmp_path,
        "us",
        "2026-02",
        {"AAPL": [_bar("2026-01-30", 1, 2, 0.5, 1.5, 100), _bar("2026-02-02", 2, 3, 1.5, 2.5, 150)]},
    )
    _build(module, tmp_path)
    rows = _read_store(store)
    assert [r["date"] for r in rows] == ["2026-01-30", "2026-02-02"]


def test_consumer_roundtrip_pit_filter(tmp_path: Path) -> None:
    """Replicate the consumer read: one row per day, date<=as_of slicing."""
    module = _load_module()
    _write_bundle(
        tmp_path,
        "us",
        "2026-01",
        {
            "AAPL": [
                _bar("2026-01-02", 1, 2, 0.5, 1.5, 100),
                _bar("2026-01-03", 1.5, 2.5, 1, 2, 200),
                _bar("2026-01-06", 2, 3, 1.5, 2.5, 300),
            ]
        },
    )
    _build(module, tmp_path)
    store = tmp_path / "us" / "daily_prices" / "AAPL.csv"
    rows = _read_store(store)
    as_of = date(2026, 1, 3)
    pit = [r for r in rows if date.fromisoformat(r["date"]) <= as_of]
    assert [r["date"] for r in pit] == ["2026-01-02", "2026-01-03"]
    # one row per trading day (no dupes)
    assert len({r["date"] for r in rows}) == len(rows)


def test_tail_only_appends_after_latest_seed_date() -> None:
    """Provider tail never rewrites a bundle-covered date; it only appends newer."""
    module = _load_module()
    B = module.Bar
    seed = {
        date(2026, 1, 29): B(date(2026, 1, 29), 1, 1, 1, 1, 1),
        date(2026, 1, 30): B(date(2026, 1, 30), 2, 2, 2, 2, 2),
    }
    tail = [
        B(date(2026, 1, 30), 99, 99, 99, 99, 99),  # overlaps seed -> must be ignored
        B(date(2026, 2, 2), 3, 3, 3, 3, 3),  # after seed_max -> appended
    ]
    bars, _added = module.merge_bars({}, seed, tail, rebuild=True)
    by_date = {b.date: b for b in bars}
    assert by_date[date(2026, 1, 30)].close == 2  # seed kept, tail did NOT overwrite
    assert date(2026, 2, 2) in by_date  # newer tail day appended


def test_missing_history_reported(tmp_path: Path) -> None:
    module = _load_module()
    # AAPL has bars, GHOST is present as a key but with no bars.
    _write_bundle(
        tmp_path,
        "us",
        "2026-01",
        {"AAPL": [_bar("2026-01-02", 1, 2, 0.5, 1.5, 100)], "GHOST": []},
    )
    result = _build(module, tmp_path)
    assert "GHOST" in result.missing
    assert not (tmp_path / "us" / "daily_prices" / "GHOST.csv").exists()
