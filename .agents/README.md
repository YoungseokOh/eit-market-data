# Codex Skills Bridge

이 저장소는 작업용 스킬 원본을 `.claude/skills/`에 유지합니다.
Codex에서 스캔하는 경로는 `.agents/skills`이므로, 본 디렉터리의 `skills` 항목은
`.claude/skills`를 가리키는 symlink입니다.

즉, 스킬 본문은 한 곳(`.claude/skills`)에서 관리하면서
`Codex`와 `Claude`가 동일한 내용에 접근할 수 있습니다.
