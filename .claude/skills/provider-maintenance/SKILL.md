---
name: provider-maintenance
description: |
  Use when adding or changing market-data providers, provider factories, optional dependencies,
  scripts that call providers, or tests around DART, ECOS, FDR, pykrx, and snapshot assembly.
  Trigger on changes under src/eit_market_data/, pyproject optional dependencies, provider protocols,
  or new market-data fields and adapters.
---

# Provider Maintenance

Use this skill for provider evolution work in `eit-market-data`.

## Workflow

1. Identify which provider contract is changing:
   price, fundamentals, filing, macro, sector, benchmark, or factory wiring.
2. Update the provider implementation and its factory path together.
3. Add or update focused tests next to the touched provider.
4. Update scripts and docs only where the changed behavior is exposed.
5. If the provider touches timing semantics or KR bundle export, also use the related skills.

## DART Provider — Rate Limit Guard

> **DartProvider를 테스트할 때 실제 API를 반복 호출하지 않는다.**

- 코드 변경 후 검증은 빌드 **1회**로 제한한다
- DART 접속이 안 될 때 원인을 찾느라 curl/requests를 반복하면 IP 당일 차단됨
- 단위 테스트는 `SyntheticProvider` 또는 diskcache 시드 데이터로 대체한다
- 규칙 전문: `@rules/dart-api-limits.md`

## Read Next

- Provider map and touchpoints: `references/provider-map.md`
- For bundle-facing changes: `../kr-bundle-pipeline/SKILL.md`
- For timing safety: `../point-in-time-guardrails/SKILL.md`

## Commit Format

If you make a commit while using this skill, follow `@rules/commit-messages.md`:
`<type>: <message>` then a blank line then `<body>`.
