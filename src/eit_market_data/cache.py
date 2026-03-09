"""Response cache backed by diskcache.

Caches LLM responses keyed by (model, agent, stock, month, prompt_hash)
so that interrupted runs can resume without re-querying the LLM.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import diskcache


class ResponseCache:
    """Persistent LLM response cache.

    Keys are formatted as: "{model}:{agent_name}:{stock}:{month}:{prompt_hash}"
    Values are serialized AgentResult dicts.
    """

    def __init__(self, cache_dir: str | Path = "artifacts/cache"):
        self._cache_dir = Path(cache_dir)
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._cache = diskcache.Cache(str(self._cache_dir))

    @staticmethod
    def make_key(
        model: str,
        agent_name: str,
        stock: str | None,
        month: str,
        prompt_hash: str,
    ) -> str:
        """Build a deterministic cache key."""
        stock_part = stock or "__MARKET__"
        return f"{model}:{agent_name}:{stock_part}:{month}:{prompt_hash}"

    def get(self, key: str) -> dict[str, Any] | None:
        """Retrieve a cached result, or None if not found."""
        val = self._cache.get(key)
        if val is None:
            return None
        if isinstance(val, str):
            return json.loads(val)
        return val

    def set(self, key: str, value: dict[str, Any]) -> None:
        """Store a result in the cache (no TTL — permanent for reproducibility)."""
        self._cache.set(key, json.dumps(value, default=str))

    def invalidate(self, key: str) -> bool:
        """Delete a specific key. Returns True if it existed."""
        return self._cache.delete(key)  # type: ignore[return-value]

    def invalidate_pattern(self, pattern: str) -> int:
        """Delete all keys containing the given substring. Returns count deleted."""
        count = 0
        for key in list(self._cache):
            if isinstance(key, str) and pattern in key:
                self._cache.delete(key)
                count += 1
        return count

    def clear(self) -> None:
        """Clear all cached entries."""
        self._cache.clear()

    def __len__(self) -> int:
        return len(self._cache)

    def close(self) -> None:
        self._cache.close()
