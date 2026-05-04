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
- Partial current-month builds must pass an explicit `decision_date=as_of`; do not let
  `--force --as-of YYYY-MM-DD` silently use the calendar month-end business day.
- `execution_date` for a partial-month local snapshot should be the next business day
  after `decision_date`, not the first business day of the following month.

## Tests to update

- Provider-level tests for future-date rejection
- Snapshot build tests for month-end decision date behavior
- Snapshot build tests for explicit partial-month `decision_date`
- Bundle loader/export tests when schema fields or nullable behavior change

## Smells

- `date.today()` in providers
- Hard-coded end dates
- `month -> last_business_day(month)` paths used when the caller supplied an explicit `as_of`
- Filtering by fiscal quarter only
- Recomputing benchmark or sector values with live data during bundle load
