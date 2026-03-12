from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

load_dotenv(PROJECT_ROOT / ".env")

from eit_market_data.local_collection import build_local_universe_manifest


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a local KR universe manifest.")
    parser.add_argument("--as-of", required=True, help="Reference date in YYYY-MM-DD.")
    parser.add_argument(
        "--kind",
        required=True,
        choices=["top100", "top300", "full"],
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
