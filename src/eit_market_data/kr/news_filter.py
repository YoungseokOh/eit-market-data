"""Pure, offline, deterministic filter/ranking layer for KR news windows.

Naver Finance archive capture is high-volume and noisy: the same event is
republished by many outlets on the same day, and low-signal wire copies dominate
the raw list. This module reduces a captured window of
:class:`~eit_market_data.kr.naver_news_provider.NaverArchiveNewsRecord` into a
compact, ranked, point-in-time-safe list WITHOUT any network access or
re-collection. It is applied at window-reconstruction time
(``KrNewsCatalogStore.build_window``), so raw captures never need to be re-fetched.

Pipeline (all stages are pure functions of the input window + ``as_of``):

1. **PIT guard** — drop any record whose ``published_at``/``date`` is after
   ``as_of`` (defensive; upstream already skips these).
2. **Title-normalized dedup / clustering** — strip bracket tags
   (``[특징주]``, ``[공시]`` ...), the company name, punctuation, and whitespace,
   then cluster records that share a normalized title or whose char 3-gram
   Jaccard similarity is at least :data:`DEFAULT_JACCARD_THRESHOLD`. The cluster
   representative is the record with the EARLIEST ``published_at`` (reduces
   republished-copy noise and is PIT-safe).
3. **Relevance ranking** — score each representative by source-tier weighting
   (major-outlet whitelist), a company-name-in-title bonus, and an event-keyword
   bonus.
4. **Per-day cap** — keep the top ``per_day_cap`` representatives per calendar day.
5. **Top-K** — keep the top ``top_k`` representatives across the whole window.

There is no reliance on wall-clock time or randomness; ordering is fully
deterministic given the same input records and ``as_of``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from datetime import time as datetime_time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from eit_market_data.kr.naver_news_provider import NaverArchiveNewsRecord

# ---------------------------------------------------------------------------
# Named constants (no magic numbers inline)
# ---------------------------------------------------------------------------

_SEOUL = timezone(timedelta(hours=9))

#: Default number of cluster representatives kept per calendar day.
DEFAULT_PER_DAY_CAP = 5
#: Default number of cluster representatives kept per window.
DEFAULT_TOP_K = 40
#: Minimum char n-gram Jaccard similarity to merge two titles into one cluster.
DEFAULT_JACCARD_THRESHOLD = 0.8
#: Character n-gram size used for near-duplicate similarity.
DEFAULT_NGRAM_SIZE = 3

#: Major Korean financial/news outlets. Sources matching one of these
#: (bidirectional substring match, since parsed source labels are abbreviated)
#: receive the source-tier weight.
DEFAULT_SOURCE_WHITELIST: frozenset[str] = frozenset(
    {
        "연합뉴스",
        "연합인포맥스",
        "한국경제",
        "매일경제",
        "서울경제",
        "조선비즈",
        "이데일리",
        "머니투데이",
        "파이낸셜뉴스",
        "뉴스핌",
        "헤럴드경제",
        "아시아경제",
        "한겨레",
        "중앙일보",
        "동아일보",
    }
)

#: Event keywords that signal higher-signal, price-relevant articles.
DEFAULT_EVENT_KEYWORDS: tuple[str, ...] = (
    "실적",
    "수주",
    "유상증자",
    "무상증자",
    "자사주",
    "합병",
    "인수",
    "소송",
    "공시",
    "배당",
    "신제품",
    "감자",
    "리콜",
    "특허",
)

#: Score added when the article's source is on the whitelist.
DEFAULT_SOURCE_TIER_WEIGHT = 2.0
#: Score added when the (normalized) company name appears in the title.
DEFAULT_COMPANY_TITLE_BONUS = 1.5
#: Score added per distinct matched event keyword.
DEFAULT_EVENT_KEYWORD_WEIGHT = 1.0

# Bracket-tag patterns commonly prefixed to Korean finance headlines.
_BRACKET_PATTERN = re.compile(r"[\[\(【<][^\]\)】>]*[\]\)】>]")
# Anything that is not a Korean syllable, latin letter, or digit.
_NON_ALNUM_PATTERN = re.compile(r"[^0-9a-z가-힣]+")


@dataclass(frozen=True)
class NewsFilterConfig:
    """Configuration for :func:`filter_news_window`.

    All parameters have sensible defaults drawn from the module-level
    ``DEFAULT_*`` constants so callers can enable filtering with zero tuning.
    """

    per_day_cap: int = DEFAULT_PER_DAY_CAP
    top_k: int = DEFAULT_TOP_K
    jaccard_threshold: float = DEFAULT_JACCARD_THRESHOLD
    ngram_size: int = DEFAULT_NGRAM_SIZE
    source_whitelist: frozenset[str] = DEFAULT_SOURCE_WHITELIST
    event_keywords: tuple[str, ...] = DEFAULT_EVENT_KEYWORDS
    source_tier_weight: float = DEFAULT_SOURCE_TIER_WEIGHT
    company_title_bonus: float = DEFAULT_COMPANY_TITLE_BONUS
    event_keyword_weight: float = DEFAULT_EVENT_KEYWORD_WEIGHT


@dataclass(frozen=True)
class ScoredNewsRecord:
    """A cluster representative with its cluster size and relevance score."""

    record: NaverArchiveNewsRecord
    cluster_size: int = 1
    relevance: float = 0.0


def _published_key(record: NaverArchiveNewsRecord) -> datetime:
    """Return a comparable timezone-aware datetime for ordering.

    Records without an explicit ``published_at`` fall back to midnight (Seoul)
    of their calendar date so ordering stays deterministic.
    """
    if record.published_at is not None:
        return record.published_at
    return datetime.combine(record.date, datetime_time(0, 0), tzinfo=_SEOUL)


def _normalize_title(headline: str, company_name: str | None) -> str:
    """Normalize a headline for dedup/clustering.

    Strips bracket tags, the company name, punctuation, and whitespace, then
    lowercases. The result is a compact token stream suitable for exact-match
    and n-gram similarity comparison.
    """
    text = _BRACKET_PATTERN.sub(" ", headline)
    text = text.lower()
    if company_name:
        company_norm = _NON_ALNUM_PATTERN.sub("", company_name.lower())
        if company_norm:
            text = text.replace(company_name.lower(), " ")
            text = text.replace(company_norm, " ")
    text = _NON_ALNUM_PATTERN.sub("", text)
    return text


def _char_ngrams(text: str, size: int) -> set[str]:
    if size <= 0 or not text:
        return set()
    if len(text) <= size:
        return {text}
    return {text[i : i + size] for i in range(len(text) - size + 1)}


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    intersection = len(a & b)
    union = len(a | b)
    return intersection / union if union else 0.0


@dataclass
class _Cluster:
    normalized: str
    ngrams: set[str]
    members: list[NaverArchiveNewsRecord] = field(default_factory=list)


def _pit_drop(
    records: list[NaverArchiveNewsRecord], as_of: date
) -> list[NaverArchiveNewsRecord]:
    kept: list[NaverArchiveNewsRecord] = []
    for record in records:
        if record.date > as_of:
            continue
        if record.published_at is not None and record.published_at.date() > as_of:
            continue
        kept.append(record)
    return kept


def _cluster_records(
    records: list[NaverArchiveNewsRecord],
    company_name: str | None,
    config: NewsFilterConfig,
) -> list[_Cluster]:
    """Cluster records by normalized title + char n-gram Jaccard similarity.

    Iteration order is deterministic: records are processed sorted by
    (earliest published_at, date, headline). Each record joins the first
    existing cluster whose representative title is an exact match or whose
    n-gram Jaccard similarity meets the threshold; otherwise it seeds a new one.
    """
    ordered = sorted(
        records,
        key=lambda r: (_published_key(r), r.date, r.headline),
    )
    clusters: list[_Cluster] = []
    exact_index: dict[str, _Cluster] = {}
    for record in ordered:
        normalized = _normalize_title(record.headline, company_name)
        ngrams = _char_ngrams(normalized, config.ngram_size)

        cluster = exact_index.get(normalized) if normalized else None
        if cluster is None:
            for candidate in clusters:
                if _jaccard(ngrams, candidate.ngrams) >= config.jaccard_threshold:
                    cluster = candidate
                    break
        if cluster is None:
            cluster = _Cluster(normalized=normalized, ngrams=ngrams)
            clusters.append(cluster)
            if normalized:
                exact_index[normalized] = cluster
        cluster.members.append(record)
    return clusters


def _source_is_whitelisted(source: str, whitelist: frozenset[str]) -> bool:
    source_norm = source.strip()
    if not source_norm:
        return False
    return any(entry in source_norm or source_norm in entry for entry in whitelist)


def _score_record(
    record: NaverArchiveNewsRecord,
    company_name: str | None,
    config: NewsFilterConfig,
) -> float:
    score = 0.0
    if _source_is_whitelisted(record.source, config.source_whitelist):
        score += config.source_tier_weight
    if company_name and company_name.lower() in record.headline.lower():
        score += config.company_title_bonus
    matched = {kw for kw in config.event_keywords if kw in record.headline}
    score += config.event_keyword_weight * len(matched)
    return score


def filter_news_window(
    records: list[NaverArchiveNewsRecord],
    *,
    as_of: date,
    company_name: str | None = None,
    config: NewsFilterConfig | None = None,
) -> list[ScoredNewsRecord]:
    """Filter and rank a window of raw archive records.

    Returns cluster representatives ordered by descending relevance (ties broken
    by most-recent ``published_at`` then headline). Pure and deterministic: the
    output depends only on ``records``, ``as_of``, ``company_name``, and
    ``config`` — never on wall-clock time or randomness.
    """
    cfg = config or NewsFilterConfig()

    in_window = _pit_drop(records, as_of)
    clusters = _cluster_records(in_window, company_name, cfg)

    scored: list[ScoredNewsRecord] = []
    for cluster in clusters:
        if not cluster.members:
            continue
        # Representative = earliest published_at (PIT-safe, drops republished copies).
        representative = min(
            cluster.members,
            key=lambda r: (_published_key(r), r.date, r.headline),
        )
        scored.append(
            ScoredNewsRecord(
                record=representative,
                cluster_size=len(cluster.members),
                relevance=_score_record(representative, company_name, cfg),
            )
        )

    # Per-day cap: keep the highest-scoring representatives per calendar day.
    per_day: dict[date, list[ScoredNewsRecord]] = {}
    for item in scored:
        per_day.setdefault(item.record.date, []).append(item)
    capped: list[ScoredNewsRecord] = []
    for day_items in per_day.values():
        day_items.sort(
            key=lambda s: (-s.relevance, _published_key(s.record), s.record.headline),
        )
        capped.extend(day_items[: max(cfg.per_day_cap, 0)])

    # Top-K across the window by descending relevance (deterministic tiebreak).
    capped.sort(
        key=lambda s: (-s.relevance, -_published_key(s.record).timestamp(), s.record.headline),
    )
    if cfg.top_k >= 0:
        capped = capped[: cfg.top_k]
    return capped


__all__ = [
    "NewsFilterConfig",
    "ScoredNewsRecord",
    "filter_news_window",
    "DEFAULT_PER_DAY_CAP",
    "DEFAULT_TOP_K",
    "DEFAULT_JACCARD_THRESHOLD",
    "DEFAULT_NGRAM_SIZE",
    "DEFAULT_SOURCE_WHITELIST",
    "DEFAULT_EVENT_KEYWORDS",
]
