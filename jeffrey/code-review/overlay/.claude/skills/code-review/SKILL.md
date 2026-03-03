---
name: code-review
description: >-
  Multi-pass code review: correctness, security, performance, and
  maintainability. Each finding gets severity, location, and suggestion.
---

# Code Review

Review the specified code (diff, file, or recent changes) using four
sequential passes. Do not combine passes; complete each one before
starting the next.

## Pass 1: Correctness

- Logic errors, off-by-one, null/nil dereferences
- Missing error handling or swallowed errors
- Race conditions, deadlocks, data races
- Incorrect API usage or contract violations
- Missing edge cases (empty input, boundary values, overflow)

## Pass 2: Security (OWASP Top 10)

- Injection (SQL, command, template, XSS)
- Broken authentication or session management
- Sensitive data exposure (secrets in code, logs, errors)
- Missing input validation at system boundaries
- Insecure deserialization
- Insufficient logging of security events

## Pass 3: Performance

- Algorithmic complexity (O(n^2) where O(n) exists)
- N+1 query/fetch patterns
- Unbounded memory growth (missing limits, leaking goroutines)
- Unnecessary allocations in hot paths
- Missing caching for expensive operations
- Blocking I/O on critical paths

## Pass 4: Maintainability

- Unclear naming (variables, functions, types)
- Premature abstractions or unnecessary complexity
- Missing or misleading comments
- Dead code or unreachable branches
- Test coverage gaps for the changed code
- Inconsistency with existing codebase conventions

## Output Format

For each finding:

```
[SEVERITY] file:line — description
  Suggestion: how to fix
```

Severity levels: CRITICAL, HIGH, MEDIUM, LOW, NIT

Summarize with a verdict: APPROVE, REQUEST CHANGES, or COMMENT.

---

*Based on Jeffrey Emanuel's Peer Code Reviewer (@doodlestein)*
