# Contract

## Producer files

- `scripts/build_kr_snapshot.py`
- `src/eit_market_data/snapshot.py`
- `src/eit_market_data/kr/ci_safe_provider.py`
- `src/eit_market_data/kr/fundamental_provider.py`

## Consumer files

- `/home/seok436/projects/eit-research/src/eit/data/snapshot.py`
- `/home/seok436/projects/eit-research/src/eit/cli.py`
- `/home/seok436/projects/eit-research/docs/integrations/eit-market-data.md`

## Bundle layout

- `artifacts/snapshots/YYYY-MM/snapshot.json`
- `artifacts/snapshots/YYYY-MM/metadata.json`
- `artifacts/snapshots/YYYY-MM/manifest.json`
- `artifacts/snapshots/YYYY-MM/summary.json`

## Current ci_safe expectations

- Required: prices, DART fundamentals with `report_date`, filing text, macro, seed sector map
- Optional: benchmark prices, `market_cap`, `issued_shares`
- `manifest.json` should record `source_profile`, `field_coverage`, and warnings

## Primary commands

- Producer:
  - `python scripts/build_kr_snapshot.py --as-of YYYY-MM-DD --profile ci_safe --force`
- Consumer:
  - `eit build-snapshot YYYY-MM --market kr --bundle-dir ../eit-market-data/artifacts/snapshots`

