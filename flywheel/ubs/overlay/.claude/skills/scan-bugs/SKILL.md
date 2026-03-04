# /scan-bugs — Run Ultimate Bug Scanner

Scan code for bugs, security issues, and anti-patterns using UBS.

## Scan staged files (pre-commit)

```bash
ubs --staged --format=json
```

## Scan specific files

```bash
ubs --files "path/to/file1.go,path/to/file2.go" --format=json
```

## Full repo scan

```bash
ubs --format=json
```

## Interpret results

UBS outputs findings with severity levels. For each finding:

1. **Critical/High** — Fix before committing. These are real bugs.
2. **Medium** — Review and fix if straightforward.
3. **Low/Info** — Note but don't block on these.

## Tips

- Use `--format=toon` instead of `--format=json` for token-efficient output
- The `PreToolUse` hook automatically scans before `git commit` — review
  its output before proceeding
- For large repos, use `--staged` or `--files` to keep scans fast
- If a finding is a false positive, proceed with the commit and note it
