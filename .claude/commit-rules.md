# Commit Rules

## Format

```
<type>(<scope>): <subject>

[optional body — Korean or English OK]
[optional footer]
```

- `type`: 아래 표에서 선택
- `scope`: 선택사항, 변경 영역 명시
- `subject`: 영어 소문자, 현재 시제, 마침표 없음

## Types

| Type | 언제 사용 |
|------|----------|
| `feat` | 새로운 기능 추가 |
| `fix` | 버그 수정 |
| `refactor` | 동작 변경 없는 코드 개선 |
| `docs` | 문서 변경 (코드 변경 없음) |
| `chore` | 빌드 설정, 의존성, CI 등 |
| `test` | 테스트 추가 또는 수정 |

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

refactor(schema): rename EITConfig to SnapshotConfig
docs: update README with KR provider setup instructions
chore: add kr optional-dependencies to pyproject.toml
test(kr): add unit tests for DartProvider account map
```

## Rules

1. **subject는 소문자로 시작, 마침표 없음**
2. **한 커밋 = 한 가지 변경 이유** — scope가 다른 변경은 커밋을 분리
3. **최소 통과 조건**: `python -c "import eit_market_data"` 성공 후 커밋
4. `--no-verify`, `--allow-empty` 사용 금지
