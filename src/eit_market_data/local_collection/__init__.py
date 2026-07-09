"""Local-only collection orchestration with checkpoint validation.

This package was split out of a single ``local_collection.py`` module. Every
symbol that used to live at ``eit_market_data.local_collection.<name>`` is
re-exported here, so existing imports keep working unchanged. New code should
prefer importing from the specific submodule.
"""

from __future__ import annotations

from eit_market_data.local_collection import (  # noqa: F401 - expose submodules for patching
    cache_only_dart,
    collector,
    constants,
    progress,
    runner,
    state,
    universe,
    validation,
)
from eit_market_data.local_collection.cache_only_dart import (
    CacheOnlyDartProvider,
    _filing_richness,
    _is_dart_transient_error,
)
from eit_market_data.local_collection.collector import (
    LocalKrCollector,
    _next_kr_execution_date,
    compute_sector_averages_from_state,
)
from eit_market_data.local_collection.constants import (
    CURRENT_KR_UNIVERSE_CSV,
    DEFAULT_US_UNIVERSE,
    KOSPI200_INDEX_CODE,
    KOSPI200_OFFCYCLE_CHURN_THRESHOLD,
    KOSPI200_REVIEW_MONTHS,
    KOSPI200_SIZE_TOLERANCE,
    NEWS_LOOKBACK_DAYS,
    PROJECT_ROOT,
)
from eit_market_data.local_collection.progress import (
    _news_window_start,
    _now_utc,
    _read_json,
    _write_json,
    build_run_root,
    default_raw_start,
    load_progress,
    load_ticker_rows,
    load_tickers,
    save_progress,
)
from eit_market_data.local_collection.runner import (
    _run_kr_phase,
    _run_us_phase,
    run_local_collection,
    run_subprocess_stage,
)
from eit_market_data.local_collection.state import (
    BatchPayload,
    CheckpointPolicy,
    KrCollectionState,
    ValidationCheck,
    ValidationError,
    _load_batch_payload,
    _serialize_batch,
)
from eit_market_data.local_collection.universe import (
    _build_kospi200_records,
    _carry_forward_rows,
    _fetch_kospi200_rows_from_naver_current,
    _fetch_kospi200_tickers_from_pykrx,
    _is_current_or_future_month,
    _kospi200_membership_churn,
    _listing_metadata_frame,
    _load_kospi200_members_from_csv,
    _market_cap_candidates_for_market,
    _merge_seed_listing_metadata,
    _snapshot_market_cap_frame,
    build_local_universe_manifest,
    copy_pilot_universe,
    find_previous_kospi200_members,
)
from eit_market_data.local_collection.validation import (
    _is_sorted_dates,
    raise_for_failed_checks,
    summarize_checks,
    validate_kr_checkpoint,
    validate_kr_final_snapshot,
    validate_kr_raw_outputs,
    validate_us_outputs,
)

__all__ = [
    "BatchPayload",
    "CacheOnlyDartProvider",
    "CheckpointPolicy",
    "CURRENT_KR_UNIVERSE_CSV",
    "DEFAULT_US_UNIVERSE",
    "KOSPI200_INDEX_CODE",
    "KOSPI200_OFFCYCLE_CHURN_THRESHOLD",
    "KOSPI200_REVIEW_MONTHS",
    "KOSPI200_SIZE_TOLERANCE",
    "KrCollectionState",
    "LocalKrCollector",
    "NEWS_LOOKBACK_DAYS",
    "PROJECT_ROOT",
    "ValidationCheck",
    "ValidationError",
    "build_local_universe_manifest",
    "build_run_root",
    "compute_sector_averages_from_state",
    "copy_pilot_universe",
    "default_raw_start",
    "find_previous_kospi200_members",
    "load_progress",
    "load_ticker_rows",
    "load_tickers",
    "raise_for_failed_checks",
    "run_local_collection",
    "run_subprocess_stage",
    "save_progress",
    "summarize_checks",
    "validate_kr_checkpoint",
    "validate_kr_final_snapshot",
    "validate_kr_raw_outputs",
    "validate_us_outputs",
]
