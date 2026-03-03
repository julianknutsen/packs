---
name: robot-mode
description: >-
  Design and implement an agent-optimized CLI interface for any project.
  JSON output, structured errors, exit codes, token-efficient.
---

# Robot-Mode Maker

Design and implement a "robot mode" CLI for this project, optimized for
use by AI coding agents.

## Requirements

1. **JSON Output:** Add `--json` flag to every command for
   machine-readable output. Stable key ordering, no omitted fields.

2. **Quick Start:** Running with no args shows help in ~100 tokens.
   Dense, scannable, no walls of text.

3. **Structured Errors:** Error responses include code, message, and
   suggestions for correction. Give 1-2 relevant correct examples in
   error messages showing how to do what the user likely intended.

4. **TTY Detection:** Auto-switch to JSON when output is piped
   (`!isatty(stdout)`). Human-readable when interactive.

5. **Exit Codes:** Meaningful codes:
   - 0 = success
   - 1 = not found
   - 2 = invalid arguments
   - 3 = permission denied
   - 4 = conflict/already exists

6. **Token Efficient:** Dense, minimal output that respects context
   window limits. No decorative borders or padding in JSON mode.

7. **Error Tolerance:** Be maximally flexible when the intent is clear
   but there is a minor syntax issue. Honor commands where intent is
   legible; include a note teaching correct syntax for next time.

## Process

1. Survey existing CLI commands and identify agent-unfriendly patterns.
2. Design the interface (flags, output schemas, error format) before
   implementing.
3. Implement incrementally, one command at a time.
4. Test output token counts to ensure efficiency.

## Output Schema

When `--json` is set, output only the requested format (JSON, TOML, CSV,
etc.). No prose, no explanation outside designated fields. Strict schema
adherence.

---

*Based on Jeffrey Emanuel's Robot-Mode Maker and CLI Error Tolerance (@doodlestein)*
