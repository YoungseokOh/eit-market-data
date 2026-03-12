# Commit Messages

- Use commit messages in this exact format:
  - `<type>: <message>`
  - blank line
  - `<body>`
- The body is required.
- The body should explain what changed and why.
- Do not use a scope in the subject line.
- Prefer concise commit types such as `feat`, `fix`, `docs`, `refactor`, `test`, `chore`, `ci`, `build`, and `perf`.

## Repository Examples

```text
feat: add kr ci-safe snapshot bundle support

add ci-safe kr providers, persist snapshot bundles, and expose the export path for eit-research
```

```text
fix: handle missing market cap in kr fundamentals

fall back to price-derived close data when official market snapshot fields are unavailable
```
