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

## pykrx Script Guard

- Scripts that call official pykrx paths must load project `.env` before importing/calling pykrx helpers.
- Missing `KRX_ID`/`KRX_PW` can surface as dataframe column errors, not only as explicit auth errors.
- If `crawl_kr_data_pykrx.py` reports missing `종가`, `BPS`, `PER`, or `KeyError('지수명')`,
  verify auth/session loading before editing normalizers.

## DART Provider — Rate Limit Guard

> **DartProvider를 테스트할 때 실제 API를 반복 호출하지 않는다.**

- 코드 변경 후 검증은 빌드 **1회**로 제한한다
- DART 접속이 안 될 때 원인을 찾느라 curl/requests를 반복하면 IP 당일 차단됨
- 단위 테스트는 `SyntheticProvider` 또는 diskcache 시드 데이터로 대체한다
- Broad local KR 수집 중 OpenDART timeout이 나면 live provider를 계속 고치며 재시도하지 말고
  `CacheOnlyDartProvider` / `--dart-mode cache_only`로 전환한다.
- DART provider/script 변경 시 broad live 수집을 기본값으로 만들지 않는다. live backfill은
  `universe 제한 + 5s+ delay + cached ticker skip + progress/resume + transient 즉시 중단`이 있어야 한다.
- strict provider 경로는 OpenDART timeout을 빈 fundamentals로 숨기면 안 된다. transient 원인이 보이게 raise하거나
  상위 collector가 cache-only 전환을 기록하게 한다.
- controlled backfill에서 `DART fundamentals returned empty`가 나오면 반복 `013` 가능성이 있으므로
  live 호출을 중단한다.
- default controlled backfill에서 `DART filing returned empty`는 `filing_empty`로 기록하고 계속한다.
  재무제표 cache coverage를 filing text section 추출 실패 때문에 막지 않는다.
- 최신 `[첨부정정]사업보고서`가 document API에서 `014`를 반환하면 같은 `as_of` 이전의 다음
  retrievable 사업보고서로 fallback한다. 이때 실제 `filing_date` freshness를 보고한다.
- `014`를 filing 결측으로 바로 분류하지 않는다. report 후보 목록과 `doc:<rcept_no>` cache를
  확인하고 fallback 후보가 없는 경우에만 실제 결측으로 본다.
- 모든 DART diskcache reader/writer는 공통 size limit 설정을 사용한다. 기본값은 50GB이며
  `EIT_DART_CACHE_SIZE_LIMIT_BYTES`로 조정한다.
- progress 상태와 실제 cache coverage를 같은 것으로 취급하지 않는다. backfill 변경 후에는
  actual cache key coverage와 latest quarter 분포를 검증한다.
- KRX ticker normalization은 알파뉴메릭 6자리 코드를 보존해야 한다. DART provider와 backfill
  script가 숫자만 추출하면 다른 종목으로 cache를 채울 수 있다.
- 규칙 전문: `@rules/dart-api-limits.md`

## Read Next

- Provider map and touchpoints: `references/provider-map.md`
- For bundle-facing changes: `../kr-bundle-pipeline/SKILL.md`
- For timing safety: `../point-in-time-guardrails/SKILL.md`

## Commit Format

If you make a commit while using this skill, follow `@rules/commit-messages.md`:
`<type>: <message>` then a blank line then `<body>`.
