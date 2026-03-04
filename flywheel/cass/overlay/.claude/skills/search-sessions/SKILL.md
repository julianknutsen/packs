# /search-sessions — Search past agent sessions

Use `cass` to search past coding agent sessions for relevant solutions,
debugging history, and patterns.

## Basic search

```bash
cass search "error message or concept" --robot --limit 5 --fields minimal
```

Always use `--robot` for machine-readable output and `--fields minimal`
to keep token usage low.

## Expand a result

When a search result looks relevant, expand it for full context:

```bash
cass expand <session-id> --robot
```

## Search with filters

Filter by date range:

```bash
cass search "query" --robot --after 2026-01-01 --limit 10 --fields minimal
```

Filter by project/directory:

```bash
cass search "query" --robot --dir /path/to/project --limit 5 --fields minimal
```

## Tips

- Search before starting a new task — a previous session may have solved it
- Use specific error messages as search queries for best results
- Keep `--limit` low (3-5) for initial searches, expand if needed
- Use `--fields minimal` to avoid flooding your context window
- Expand only the most relevant results to get full session context
