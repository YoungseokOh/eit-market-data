# PIT Checklist

## Hotspots in this repo

- `src/eit_market_data/snapshot.py`
- `src/eit_market_data/kr/dart_provider.py`
- `src/eit_market_data/kr/fundamental_provider.py`
- `src/eit_market_data/kr/ci_safe_provider.py`
- `src/eit_market_data/kr/pykrx_provider.py`

## Guardrails

- Price bars: drop any `bar.date > as_of`.
- Fundamentals: require `report_date <= as_of`.
- Macro: require observation date `<= as_of`.
- Sector and benchmark lookups: pass the same decision date through the chain.
- Bundle export: never “repair” missing fields with values from a later date.

## Tests to update

- Provider-level tests for future-date rejection
- Snapshot build tests for month-end decision date behavior
- Bundle loader/export tests when schema fields or nullable behavior change

## Smells

- `date.today()` in providers
- Hard-coded end dates
- Filtering by fiscal quarter only
- Recomputing benchmark or sector values with live data during bundle load

