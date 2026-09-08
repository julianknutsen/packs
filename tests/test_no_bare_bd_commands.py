from __future__ import annotations

import ast
from pathlib import Path
import re
import shlex
import subprocess


REPO_ROOT = Path(__file__).resolve().parents[1]
THIS_FILE = Path(__file__).resolve()

BD_SUBCOMMANDS = (
    "admin",
    "ado",
    "assign",
    "audit",
    "backup",
    "batch",
    "blocked",
    "bootstrap",
    "branch",
    "children",
    "close",
    "comment",
    "comments",
    "compact",
    "completion",
    "config",
    "context",
    "cook",
    "count",
    "create",
    "create-form",
    "defer",
    "delete",
    "dep",
    "diff",
    "doctor",
    "dolt",
    "duplicate",
    "duplicates",
    "edit",
    "epic",
    "export",
    "federation",
    "find-duplicates",
    "flatten",
    "forget",
    "formula",
    "gate",
    "gc",
    "github",
    "gitlab",
    "graph",
    "heartbeat",
    "help",
    "history",
    "hooks",
    "human",
    "import",
    "info",
    "init",
    "init-safety",
    "jira",
    "kv",
    "label",
    "linear",
    "link",
    "list",
    "lint",
    "mail",
    "memories",
    "merge-slot",
    "meta",
    "metrics",
    "migrate",
    "migrate-personal",
    "mol",
    "note",
    "notion",
    "onboard",
    "orphans",
    "ping",
    "preflight",
    "prime",
    "priority",
    "promote",
    "prune",
    "purge",
    "q",
    "query",
    "quickstart",
    "ready",
    "recall",
    "reclaim",
    "recompute-blocked",
    "remember",
    "rename-prefix",
    "rename",
    "reopen",
    "repo",
    "restore",
    "rules",
    "search",
    "set-state",
    "setup",
    "show",
    "ship",
    "sql",
    "stale",
    "state",
    "status",
    "statuses",
    "supersede",
    "swarm",
    "sync",
    "tag",
    "todo",
    "type",
    "types",
    "unclaim",
    "undefer",
    "update",
    "upgrade",
    "vc",
    "version",
    "where",
    "worktree",
)
BD_SUBCOMMAND_PATTERN = "|".join(re.escape(command) for command in BD_SUBCOMMANDS)
BARE_BD_COMMAND = re.compile(rf"\bbd[ \t]+(?:{BD_SUBCOMMAND_PATTERN})\b")
BARE_BD_LEADING_FLAG = re.compile(r"\bbd[ \t]+-{1,2}[A-Za-z]")
BARE_BD_DYNAMIC_COMMAND = re.compile(r'''\bbd[ \t]+(?:["']?\$\{?[A-Za-z_]|\$\()''')
MULTILINE_BARE_BD_COMMAND = re.compile(
    rf"\bbd[ \t]*\r?\n[ \t]*(?:{BD_SUBCOMMAND_PATTERN})\b"
)
BARE_BD_GO_EXEC = re.compile(
    r'(?:exec\.Command(?:Context)?|dispatchExecCommand)\(\s*["\']bd["\']'
)
BARE_BD_PATH_CHECK = re.compile(r"\bcommand[ \t]+-v[ \t]+bd\b")
BARE_BD_SERIALIZED_ARGV = re.compile(
    rf'''(?:\[|\{{|=|:)\s*["']bd["']\s*,\s*["'](?:{BD_SUBCOMMAND_PATTERN})["']'''
)
GC_BD_ARGV_TAIL_MARKER = "gc-bd-argv-tail"
GC_BD_ARGV_TAIL_FIXTURE = Path("tests/test_gascity_pack_inference_gate.py")
GC_BD_ARGV_TAIL_LINES = {
    '*"bd show fi-root --json"*) # gc-bd-argv-tail: fake gc receives the wrapper\'s argv tail',
    '*"bd list --json --limit 1000"*) # gc-bd-argv-tail: fake gc receives the wrapper\'s argv tail',
    'assert "bd show fi-root --json" in args_path.read_text(encoding="utf-8")  # gc-bd-argv-tail',
}

# `gc lint`'s own diagnostic text, pinned so gastown cannot drift. These are
# not instructions to run anything; they are the linter's output quoted back at
# it, and the guard that reads them compares byte for byte, so they cannot be
# reworded to say `gc bd`.
#
# Two conditions, and the second is a grammar rather than a line list: the line
# must be exactly a `bd-unknown-flag` diagnostic, in that one file. A list was
# the first shape here and it does not survive the waiver growing -- the same
# twenty-four lines then have to be maintained in two files, and the copy that
# drifts fails as a bare-bd violation rather than as a stale waiver, which
# points at the wrong problem. Nothing that matches this grammar is a command
# anyone could run.
#
# The line number is optional because the waiver stopped carrying one: it is
# keyed on path and message so that moving a diagnostic within a file is not a
# CI failure. Both forms stay matchable, since gc lint's raw output still
# carries the number and gets pasted here before it is trimmed.
GC_LINT_DIAGNOSTIC_FILE = Path("tests/gastown_lint_upstream_defects.txt")
GC_LINT_DIAGNOSTIC = re.compile(
    r'[\w./-]+:(?:\d+:)? bd-unknown-flag: bd [a-z][a-z ]* uses unrecognized flag "[-\w]+"'
)


def tracked_files() -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
    )
    return [REPO_ROOT / path for path in result.stdout.decode().split("\0") if path]


def python_argv_violations(path: Path, text: str) -> list[str]:
    if path.suffix != ".py":
        return []
    tree = ast.parse(text, filename=str(path))
    violations = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.List, ast.Tuple)) or not node.elts:
            continue
        first = node.elts[0]
        if isinstance(first, ast.Constant) and first.value == "bd":
            violations.append(f"{path.relative_to(REPO_ROOT)}:{node.lineno}: argv starts with bd")
    return violations


def gc_routes_bd(line: str, bd_start: int) -> bool:
    prefix = line[:bd_start]
    gc_tokens = list(re.finditer(r"(?<![A-Za-z0-9_=/.-])gc\b", prefix))
    if not gc_tokens:
        return False
    command_prefix = prefix[gc_tokens[-1].end() :].strip()
    try:
        tokens = shlex.split(command_prefix)
    except ValueError:
        return False

    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token in {"--city", "--rig"}:
            if index + 1 >= len(tokens):
                return False
            index += 2
            continue
        if token.startswith("--city=") or token.startswith("--rig="):
            if token.endswith("="):
                return False
            index += 1
            continue
        return False
    return True


def intentional_gc_bd_argv_tail(relative: Path, line: str) -> bool:
    return (
        relative == GC_BD_ARGV_TAIL_FIXTURE
        and GC_BD_ARGV_TAIL_MARKER in line
        and line.strip() in GC_BD_ARGV_TAIL_LINES
    )


def intentional_gc_lint_verbatim(relative: Path, line: str) -> bool:
    return relative == GC_LINT_DIAGNOSTIC_FILE and bool(
        GC_LINT_DIAGNOSTIC.fullmatch(line.strip())
    )


def bare_bd_violations(path: Path, text: str) -> list[str]:
    violations = []
    relative = path.relative_to(REPO_ROOT)
    for line_number, line in enumerate(text.splitlines(), start=1):
        for pattern in (BARE_BD_COMMAND, BARE_BD_LEADING_FLAG, BARE_BD_DYNAMIC_COMMAND):
            for match in pattern.finditer(line):
                if gc_routes_bd(line, match.start()):
                    continue
                if intentional_gc_bd_argv_tail(relative, line):
                    continue
                if intentional_gc_lint_verbatim(relative, line):
                    continue
                violations.append(f"{relative}:{line_number}: {line.strip()}")
        if BARE_BD_GO_EXEC.search(line):
            violations.append(f"{relative}:{line_number}: direct bd argv")
        if BARE_BD_PATH_CHECK.search(line):
            violations.append(f"{relative}:{line_number}: checks bd instead of gc")
        if "BD_BIN" in line:
            violations.append(f"{relative}:{line_number}: BD_BIN bypasses gc bd routing")
    for match in MULTILINE_BARE_BD_COMMAND.finditer(text):
        line_start = text.rfind("\n", 0, match.start()) + 1
        first_line = text[line_start : text.find("\n", match.start())]
        if gc_routes_bd(first_line, match.start() - line_start):
            continue
        line_number = text.count("\n", 0, match.start()) + 1
        violations.append(f"{relative}:{line_number}: bd command is split across lines")
    for match in BARE_BD_SERIALIZED_ARGV.finditer(text):
        line_number = text.count("\n", 0, match.start()) + 1
        violations.append(f"{relative}:{line_number}: serialized argv starts with bd")
    violations.extend(python_argv_violations(path, text))
    return list(dict.fromkeys(violations))


def test_shipped_pack_assets_route_beads_commands_through_gc() -> None:
    violations = []
    for path in tracked_files():
        if path.resolve() == THIS_FILE:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        violations.extend(bare_bd_violations(path, text))

    assert not violations, "bare bd commands bypass store-aware routing:\n" + "\n".join(violations)


def test_detector_covers_shell_multiline_and_serialized_argv_forms() -> None:
    fixture = REPO_ROOT / "fixture.txt"

    assert bare_bd_violations(fixture, 'command = ["bd", "show"]')
    assert bare_bd_violations(fixture, "value=$(bd\n  list --json)")
    assert bare_bd_violations(fixture, "bd --dir /tmp/rig list --json")
    assert bare_bd_violations(fixture, 'bd "$verb" --json')
    assert bare_bd_violations(fixture, 'command = ["bd",\n "show"]')
    assert bare_bd_violations(fixture, "bd show x  # gc-bd-argv-tail")
    assert bare_bd_violations(fixture, "echo gc; bd show x")
    assert bare_bd_violations(fixture, "GC_BIN=gc bd show x")
    # The gc-lint-diagnostic exemption, proved narrow in both directions: the
    # grammar alone does not exempt a line elsewhere, and the file alone does
    # not exempt a line that is not a diagnostic.
    diagnostic = 'a/b.md:12: bd-unknown-flag: bd create uses unrecognized flag "--rig"'
    lineless = 'a/b.md: bd-unknown-flag: bd create uses unrecognized flag "--rig"'
    waiver = REPO_ROOT / "tests" / "gastown_lint_upstream_defects.txt"
    assert bare_bd_violations(fixture, diagnostic)
    assert bare_bd_violations(fixture, lineless)
    assert bare_bd_violations(waiver, "bd create --labels=x")
    assert not bare_bd_violations(waiver, diagnostic)
    # The form the waiver actually uses now. Without this the exemption could
    # regress to line-numbers-only and every waiver entry would read as a bare
    # bd command, which is a failure about the wrong file.
    assert not bare_bd_violations(waiver, lineless)

    assert not bare_bd_violations(fixture, 'command = ["gc", "bd", "show"]')
    assert not bare_bd_violations(fixture, "gc --city /tmp/city bd list --json")
    assert not bare_bd_violations(fixture, "gc --rig demo bd show demo-1")
