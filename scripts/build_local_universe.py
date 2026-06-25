from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import bootstrap

PROJECT_ROOT = bootstrap()

from eit_market_data.local_collection import build_local_universe_manifest


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a local KR universe manifest.")
    parser.add_argument("--as-of", required=True, help="Reference date in YYYY-MM-DD.")
    parser.add_argument(
        "--kind",
        required=True,
        choices=["kospi200", "top100", "top200", "top300", "full"],
        help="Universe kind to generate.",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output CSV path.",
    )
    args = parser.parse_args()

    output_path = Path(args.output).expanduser()
    result = build_local_universe_manifest(
        as_of=date.fromisoformat(args.as_of),
        kind=args.kind,
        output_path=output_path,
    )
    print(result)


if __name__ == "__main__":
    main()
