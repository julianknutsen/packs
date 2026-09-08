from __future__ import annotations

from pathlib import Path
import re
import subprocess


REPO_ROOT = Path(__file__).resolve().parents[1]
THIS_FILE = Path(__file__).resolve()

# There is no `gc-sling` wrapper: not on any PATH, not in the `gc` binary, not
# in any pack. Shipped guidance that tells an agent to run it sends that agent
# to a command that does not exist. This has now recurred three times (gci-5ok5,
# gci-t0x, gci-2lr), each time in prose a human had to catch by hand.
#
# The guard is on *invocations*, not on the string. Prose has to be able to name
# the thing it forbids -- "there is no `gc-sling` wrapper" is the correction, and
# `slack-full/adapter/rig_dispatch.go` calls the dispatch step "the gc-sling leg"
# -- so a bare mention is fine and only a runnable-looking command fails.
GC_SLING = re.compile(r"(?<![\w.-])gc-sling(?![\w-])")

# `gc-sling` sits where a command goes: at the start of a line, opening an
# inline-code span or a markdown bullet, after a shell separator or a `$`
# prompt, after a `key:`/`key =` that carries a command as its value (YAML
# `run:` steps, TOML `description = "..."` — the shape recurrence #2 shipped
# in), or after an ordered-list marker.
#
# A `key:` value is where prose that merely names the wrapper can also live,
# so this class leans on the repo's convention that a bare mention is
# backticked: "Rule: there is no `gc-sling` wrapper" ends in a backtick and is
# read as prose by TAKES_ARGUMENT below.
COMMAND_POSITION = re.compile(
    r"""(?: ^ | [`;&|($] | \$\( ) [ \t]* $     # line start, separator, $ prompt
      | [:=] [ \t]* ["']? [ \t]* $             # YAML run: / TOML description =
      | ^ [ \t]* (?: [-*>] | \d+[.)] ) [ \t]+ $  # bullet or ordered-list marker
    """,
    re.VERBOSE,
)

# ...and is followed by an argument. A trailing backtick, comma, or period is
# prose; whitespace then any non-comment token is an invocation.
TAKES_ARGUMENT = re.compile(r"^[ \t]+(?![#`]|$)\S")

# Serialized argv forms, e.g. `command = ["gc-sling", "polecat"]` and the Go
# `exec.Command("gc-sling", ...)` shape that `slack-full/adapter` dispatch code
# would use. `(` is in the class by design: without it the `CommandContext`
# variant passed only incidentally, on the comma after `ctx`.
GC_SLING_ARGV = re.compile(r"""(?:\[|\{|=|:|,|\()[ \t]*["']gc-sling["']""")

# Residual gap, accepted: prose that *directs* rather than shows -- "Use the
# `gc-sling` wrapper, it auto-injects --nudge", the second shape in the pre-fix
# template -- is not separable by regex from the corrective sentence this guard
# must let through. Humans still catch that one; the guard covers invocations.


def tracked_files() -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
    )
    return [REPO_ROOT / path for path in result.stdout.decode().split("\0") if path]


def gc_sling_violations(path: Path, text: str) -> list[str]:
    violations = []
    relative = path.relative_to(REPO_ROOT) if path.is_absolute() else path
    for line_number, line in enumerate(text.splitlines(), start=1):
        for match in GC_SLING.finditer(line):
            if not COMMAND_POSITION.search(line[: match.start()]):
                continue
            if not TAKES_ARGUMENT.match(line[match.end() :]):
                continue
            violations.append(f"{relative}:{line_number}: {line.strip()}")
        if GC_SLING_ARGV.search(line):
            violations.append(f"{relative}:{line_number}: serialized argv invokes gc-sling")
    return list(dict.fromkeys(violations))


def test_shipped_assets_never_invoke_a_gc_sling_wrapper() -> None:
    violations = []
    for path in tracked_files():
        if path.resolve() == THIS_FILE:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, FileNotFoundError):
            continue
        violations.extend(gc_sling_violations(path, text))

    assert not violations, (
        "gc-sling is not a real command; use `gc sling` and pass --nudge yourself:\n"
        + "\n".join(violations)
    )


def test_detector_separates_invocations_from_prose() -> None:
    fixture = Path("fixture.md")

    # The three shapes this bug has actually shipped in.
    assert gc_sling_violations(fixture, "gc-sling <rig-worker-agent> <bead-id>")
    assert gc_sling_violations(
        fixture, "  `gc-sling polecat <new-bead-id> --on mol-pr-start --var issue=<N>`"
    )
    assert gc_sling_violations(
        fixture, "gc-sling <rig-worker-agent> --on mol-decompose --var issue=<epic> --stdin"
    )
    assert gc_sling_violations(fixture, "- gc-sling $TARGET $BEAD")
    assert gc_sling_violations(fixture, 'command = ["gc-sling", "polecat"]')
    assert gc_sling_violations(fixture, "cd /tmp && gc-sling api-server/polecat x")

    # Shapes this repo ships that the first cut of the detector let through:
    # a TOML value (the same field and file type as recurrence #2, caught last
    # time only because that value happened to be a `"""` block), a YAML `run:`
    # step, an ordered-list line, a shell-prompt line, and Go `exec.Command(`.
    assert gc_sling_violations(
        fixture, 'description = "gc-sling polecat <bead> --on mol-pr-start"'
    )
    assert gc_sling_violations(fixture, "        run: gc-sling polecat $BEAD")
    assert gc_sling_violations(fixture, "1. gc-sling <pool> <bead-id>")
    assert gc_sling_violations(fixture, "$ gc-sling polecat BL-42")
    assert gc_sling_violations(fixture, 'cmd := exec.Command("gc-sling", "polecat", beadID)')
    assert gc_sling_violations(fixture, 'exec.CommandContext(ctx, "gc-sling", target)')

    # Prose that names the non-existent wrapper in order to forbid it, and the
    # in-tree comment that calls the dispatch step "the gc-sling leg". Guarding
    # the bare string would fail the very sentences that fix the bug.
    assert not gc_sling_violations(fixture, "There is no `gc-sling` wrapper: not on any PATH,")
    assert not gc_sling_violations(fixture, "// Failures at the gc-sling leg trigger a close")
    assert not gc_sling_violations(fixture, "`gc sling`, two words — there is no `gc-sling` wrapper")
    assert not gc_sling_violations(fixture, "command -v gc-sling || echo absent")
    # A `key:` value carrying the correction, now that `:` opens a command
    # position -- the backtick is what keeps it prose.
    assert not gc_sling_violations(fixture, "Rule: there is no `gc-sling` wrapper.")

    # The real command must not trip the guard.
    assert not gc_sling_violations(fixture, "gc sling <pool> <bead-id> --nudge")
