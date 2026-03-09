# Commit Rules

## Format

```
<type>(<scope>): <subject>
```

- `type`: `feat` 또는 `fix` — **이 두 가지만 허용**
- `scope`: 선택사항, 변경 영역 명시
- `subject`: 영어 소문자, 현재 시제, 마침표 없음

## Types

| Type | 언제 사용 |
|------|----------|
| `feat` | 기능 추가, 데이터 추가, 스크립트 추가, 문서 추가, 의존성 추가 등 **새로 생기는 것** |
| `fix` | 버그 수정, 잘못된 값 수정, 누락 수정 등 **고치는 것** |

> `refactor`, `chore`, `docs`, `test` 등 다른 type은 사용하지 않는다.
> 판단이 애매하면: 없던 것이 생기면 `feat`, 있던 것이 잘못된 거면 `fix`.

## Scopes

| Scope | 대상 |
|-------|------|
| `kr` | `src/eit_market_data/kr/` |
| `us` | `src/eit_market_data/` US 프로바이더 |
| `snapshot` | `src/eit_market_data/snapshot.py` |
| `schema` | `src/eit_market_data/schemas/` |
| `scripts` | `scripts/` |
| `docs` | `docs/`, `README.md`, `CLAUDE.md` |

Scope는 생략 가능. 여러 영역이 섞이면 scope 없이 작성.

## Examples

```
feat(kr): add DartProvider for quarterly fundamentals
feat(kr): add get_kr_universe() for dynamic top-N universe
feat(scripts): add fetch_pykrx_all.py CLI data crawler
feat(docs): add DART/ECOS API key setup guide

fix(kr): move asyncio.Semaphore to __init__ to prevent event-loop errors
fix(kr): correct EPS unit — use native KRW not KRW thousands
fix(snapshot): pass as_of to fetch_sector_map to prevent look-ahead
fix(kr): reorder total_debt candidates to prefer aggregate terms
```

## Rules

1. **subject는 소문자로 시작, 마침표 없음**
2. **한 커밋 = 한 가지 변경 이유** — scope가 다른 변경은 커밋을 분리
3. **최소 통과 조건**: `python -c "import eit_market_data"` 성공 후 커밋
4. `--no-verify`, `--allow-empty` 사용 금지
