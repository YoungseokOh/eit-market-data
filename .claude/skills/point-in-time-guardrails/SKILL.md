---
name: point-in-time-guardrails
description: |
  Use whenever modifying SnapshotBuilder, provider fetch methods, bundle export/load logic,
  date filters, or any code that could leak future information into snapshots or backtests.
  Trigger on look-ahead bias concerns, as_of propagation, report_date handling, benchmark joins,
  sector map timing, or changes to snapshot schemas and validation.
---

# Point-in-Time Guardrails

Use this skill whenever data timing semantics might change.

## Core Rule

Every value in a snapshot must be knowable on or before `decision_date`.

## Workflow

1. Trace `as_of` from CLI or builder entrypoint to each provider call.
2. Reject any path that uses `date.today()` or a fixed future end date for runtime data.
3. Check filing and fundamentals by actual publication date, not fiscal period alone.
4. Add or update tests that assert no future-dated bars or quarters enter the snapshot.
5. If the change touches bundle fields consumed by `eit-research`, verify that contract too.

## Read Next

- Checklist and hotspots: `references/pit-checklist.md`
- Cross-repo bundle contract: `../kr-bundle-pipeline/references/contract.md`

