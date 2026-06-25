"""Single-source content hashing for snapshot metadata.

``_content_hash`` is the canonical 16-hex-char SHA-256 used for the snapshot
``input_hash`` and the per-section metadata hashes. ``local_collection`` used to
keep a byte-identical private copy (``_hash_blob``); both now import this.
"""

from __future__ import annotations

import hashlib
import json


def _content_hash(obj: object) -> str:
    """SHA-256 (first 16 hex chars) of the JSON-serializable object."""
    blob = json.dumps(obj, sort_keys=True, default=str).encode()
    return hashlib.sha256(blob).hexdigest()[:16]
