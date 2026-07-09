"""Progress-file, ticker-CSV, and small JSON I/O helpers."""

from __future__ import annotations

import csv
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from eit_market_data.kr.market_helpers import normalize_ticker
from eit_market_data.local_collection.constants import NEWS_LOOKBACK_DAYS


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _news_window_start(as_of: date, lookback_days: int = NEWS_LOOKBACK_DAYS) -> date:
    return as_of - timedelta(days=max(lookback_days - 1, 0))


def default_raw_start(as_of: date) -> date:
    return date(as_of.year, 1, 1)


def load_ticker_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return [
            {key: str(value or "").strip() for key, value in row.items()}
            for row in csv.DictReader(handle)
            if str(row.get("ticker", "")).strip()
        ]


def load_tickers(path: Path) -> list[str]:
    return [normalize_ticker(row["ticker"]) for row in load_ticker_rows(path)]


def build_run_root(
    storage_root: Path,
    as_of: date,
    market: str,
    phase: str,
    full_universe_kind: str,
) -> Path:
    label = f"{market}_{phase}_{full_universe_kind}"
    return storage_root / "runs" / as_of.strftime("%Y-%m-%d") / label


def load_progress(progress_path: Path, seed: dict[str, Any]) -> dict[str, Any]:
    if progress_path.exists():
        return _read_json(progress_path)
    return seed


def save_progress(progress_path: Path, payload: dict[str, Any]) -> None:
    payload["updated_at"] = _now_utc()
    _write_json(progress_path, payload)
