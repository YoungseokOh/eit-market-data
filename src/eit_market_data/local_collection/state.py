"""Collection state containers, checkpoint policy, and batch (de)serialization."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

from eit_market_data.kr.naver_news_provider import NaverArchiveNewsRecord
from eit_market_data.kr.news_catalog import KrNewsWindowCoverage
from eit_market_data.local_collection.progress import _read_json
from eit_market_data.schemas.snapshot import (
    FilingData,
    FundamentalData,
    NewsItem,
    PriceBar,
)


class ValidationError(RuntimeError):
    """Raised when a checkpoint validation fails."""


@dataclass
class CheckpointPolicy:
    every_tickers: int
    every_seconds: int


@dataclass
class ValidationCheck:
    name: str
    status: str
    detail: str
    metrics: dict[str, Any] = field(default_factory=dict)


@dataclass
class BatchPayload:
    tickers: list[str]
    prices: dict[str, list[PriceBar]]
    fundamentals: dict[str, FundamentalData]
    filings: dict[str, FilingData]
    news: dict[str, list[NewsItem]]
    news_audit: dict[str, list[NaverArchiveNewsRecord]]
    news_coverage: dict[str, KrNewsWindowCoverage]

    def merge(self, payload: "BatchPayload") -> None:
        self.tickers.extend(payload.tickers)
        self.prices.update(payload.prices)
        self.fundamentals.update(payload.fundamentals)
        self.filings.update(payload.filings)
        self.news.update(payload.news)
        self.news_audit.update(payload.news_audit)
        self.news_coverage.update(payload.news_coverage)


@dataclass
class KrCollectionState:
    prices: dict[str, list[PriceBar]] = field(default_factory=dict)
    fundamentals: dict[str, FundamentalData] = field(default_factory=dict)
    filings: dict[str, FilingData] = field(default_factory=dict)
    news: dict[str, list[NewsItem]] = field(default_factory=dict)
    news_audit: dict[str, list[NaverArchiveNewsRecord]] = field(default_factory=dict)
    news_coverage: dict[str, KrNewsWindowCoverage] = field(default_factory=dict)

    def merge(self, payload: BatchPayload) -> None:
        self.prices.update(payload.prices)
        self.fundamentals.update(payload.fundamentals)
        self.filings.update(payload.filings)
        self.news.update(payload.news)
        self.news_audit.update(payload.news_audit)
        self.news_coverage.update(payload.news_coverage)


def _serialize_batch(payload: BatchPayload) -> dict[str, Any]:
    serialized = {
        "tickers": payload.tickers,
        "prices": {
            ticker: [item.model_dump(mode="json") for item in items]
            for ticker, items in payload.prices.items()
        },
        "fundamentals": {
            ticker: item.model_dump(mode="json")
            for ticker, item in payload.fundamentals.items()
        },
        "filings": {
            ticker: item.model_dump(mode="json")
            for ticker, item in payload.filings.items()
        },
    }
    if payload.news:
        serialized["news"] = {
            ticker: [item.model_dump(mode="json") for item in items]
            for ticker, items in payload.news.items()
        }
    if payload.news_audit:
        serialized["news_audit"] = {
            ticker: [asdict(item) for item in items]
            for ticker, items in payload.news_audit.items()
        }
    if payload.news_coverage:
        serialized["news_coverage"] = {
            ticker: asdict(item)
            for ticker, item in payload.news_coverage.items()
        }
    return serialized


def _load_batch_payload(path: Path) -> BatchPayload:
    payload = _read_json(path)
    return BatchPayload(
        tickers=list(payload.get("tickers", [])),
        prices={
            ticker: [PriceBar.model_validate(item) for item in items]
            for ticker, items in payload.get("prices", {}).items()
        },
        fundamentals={
            ticker: FundamentalData.model_validate(item)
            for ticker, item in payload.get("fundamentals", {}).items()
        },
        filings={
            ticker: FilingData.model_validate(item)
            for ticker, item in payload.get("filings", {}).items()
        },
        news={
            ticker: [NewsItem.model_validate(item) for item in items]
            for ticker, items in payload.get("news", {}).items()
        },
        news_audit={
            ticker: [
                NaverArchiveNewsRecord(
                    date=date.fromisoformat(item["date"]),
                    published_at=(
                        datetime.fromisoformat(item["published_at"])
                        if item.get("published_at")
                        else None
                    ),
                    headline=str(item["headline"]),
                    url=str(item["url"]),
                    source=str(item.get("source", "Naver")),
                )
                for item in items
            ]
            for ticker, items in payload.get("news_audit", {}).items()
        },
        news_coverage={
            ticker: KrNewsWindowCoverage(
                ticker=str(item["ticker"]),
                window_start=date.fromisoformat(item["window_start"]),
                window_end=date.fromisoformat(item["window_end"]),
                raw_count=int(item.get("raw_count", 0)),
                captured_days=int(item.get("captured_days", 0)),
                missing_capture_days=[
                    str(value) for value in item.get("missing_capture_days", [])
                ],
                page_cap_hit_days=[str(value) for value in item.get("page_cap_hit_days", [])],
                status=str(item.get("status", "degraded")),
            )
            for ticker, item in payload.get("news_coverage", {}).items()
        },
    )
