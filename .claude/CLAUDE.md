# CLAUDE.md

This repository keeps Claude-specific project instructions under `.claude/`.

## Repository Focus

`eit-market-data` is a standalone point-in-time market data layer for EIT.

- Runtime code lives under `src/eit_market_data/`
- Operational scripts live under `scripts/`
- Reusable project skills live under `.claude/skills/`
- Reusable project rules live under `.claude/rules/`

## Local Standards

- Keep `.claude/` as the canonical location for Claude project instructions.
- Put modular instructions under `.claude/rules/` instead of ad-hoc top-level markdown files.
- Follow `.claude/rules/commit-messages.md` for commit formatting in this repository.
- Commit subject format is `<type>: <message>`.
- Commit body is required and must be separated from the subject by one blank line.

## Imported Rules

- commit messages @rules/commit-messages.md
