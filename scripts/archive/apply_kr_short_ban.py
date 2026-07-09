"""Stamp the KR short-sale-ban regime flag into KR monthly snapshots.

Each ``artifacts/kr/snapshots/<YYYY-MM>/snapshot.json`` gets
``metadata.short_sale_ban`` set to whether that month lies in a KR market-wide
short-selling ban window (see ``eit_market_data.kr.short_ban``). Idempotent:
re-running only rewrites the flag. The rest of the snapshot is preserved
byte-for-byte (same ``indent=2, sort_keys=True`` serialization the producer uses).

Usage:
    python scripts/apply_kr_short_ban.py [--snapshots-dir artifacts/kr/snapshots]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT / "src"))

from eit_market_data.kr.short_ban import is_kr_short_ban_month  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--snapshots-dir",
        default=str(_REPO_ROOT / "artifacts" / "kr" / "snapshots"),
        help="KR snapshots directory (default: artifacts/kr/snapshots).",
    )
    args = ap.parse_args()

    root = Path(args.snapshots_dir)
    snaps = sorted(root.glob("*/snapshot.json"))
    if not snaps:
        print(f"[warn] no snapshots found under {root}")
        return 0

    changed = 0
    flagged = 0
    for snap in snaps:
        month = snap.parent.name  # YYYY-MM
        data = json.loads(snap.read_text())
        md = data.setdefault("metadata", {})
        want = is_kr_short_ban_month(month)
        if md.get("short_sale_ban") != want:
            md["short_sale_ban"] = want
            snap.write_text(json.dumps(data, indent=2, sort_keys=True))
            changed += 1
        if want:
            flagged += 1
        print(f"  {month}: short_sale_ban={want}")

    print(f"\n{len(snaps)} snapshots, {flagged} in ban windows, {changed} rewritten.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
