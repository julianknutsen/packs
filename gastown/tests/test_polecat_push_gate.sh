#!/usr/bin/env bash
# The mol-polecat-work push gate must FAIL CLOSED.
#
# A rail expressed only in prose is enforced by nothing. Before this gate, a
# bead whose DESCRIPTION said "no push — the mayor publishes" but which carried
# no auto_push key fell through to `git push`, because an absent key yields ""
# and "" != "false". These tests pin the three properties that keep that shut:
#   1. the ambiguity branch exists, and sits BEFORE `git push origin HEAD`;
#   2. its rail detection fires on the real rail wordings in use, and stays
#      quiet on prose that merely mentions pushing;
#   3. the explicit opt-out and the ambiguity halt stay distinguishable by
#      halt_reason, so a re-sling can tell "operator said no" from "nobody said";
#   4. every note this step writes APPENDS. Both halts hand the bead back to a
#      human and tell them to go and read what it says, so a halt that replaced
#      the notes would delete the instructions it is pointing at.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
FORMULA="$ROOT/gastown/formulas/mol-polecat-work.toml"

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

[[ -f "$FORMULA" ]] || fail "formula not found: $FORMULA"

# --- 1. structure -----------------------------------------------------------

test_gate_structure() {
    grep -qF 'halt_reason=auto_push_false' "$FORMULA" ||
        fail "the explicit auto_push=false opt-out halt is gone"
    grep -qF 'halt_reason=no_push_rail_unresolved' "$FORMULA" ||
        fail "the fail-closed ambiguity halt is gone"
    grep -qF 'gc mail send' "$FORMULA" ||
        fail "the ambiguity halt no longer escalates"

    # The ambiguity halt is worthless if it lands after the push.
    local ambiguity_line push_line
    ambiguity_line=$(grep -nF 'halt_reason=no_push_rail_unresolved' "$FORMULA" | head -1 | cut -d: -f1)
    push_line=$(grep -nF 'git push origin HEAD' "$FORMULA" | head -1 | cut -d: -f1)
    [[ "$ambiguity_line" -lt "$push_line" ]] ||
        fail "fail-closed halt (line $ambiguity_line) comes after git push (line $push_line)"
}

# --- 2. rail detection ------------------------------------------------------

# Lift the shipped filter/match pair straight out of the formula so this test
# can never drift from the gate it is testing.
EXCLUDE_RE=$(grep -oE "grep -Eiv '[^']+'" "$FORMULA" | head -1 | sed -E "s/^grep -Eiv '//; s/'\$//")
MATCH_RE=$(grep -oE "grep -Eic '[^']+'" "$FORMULA" | head -1 | sed -E "s/^grep -Eic '//; s/'\$//")

[[ -n "$EXCLUDE_RE" ]] || fail "could not lift the exclude pattern out of $FORMULA"
[[ -n "$MATCH_RE" ]] || fail "could not lift the match pattern out of $FORMULA"

rail_hits() {
    printf '%s\n' "$1" | grep -Eiv "$EXCLUDE_RE" | grep -Eic "$MATCH_RE" || true
}

assert_rail() {
    local want="$1" text="$2" got
    got=$(rail_hits "$text")
    if [[ "$want" == "halt" ]]; then
        [[ "$got" -gt 0 ]] || fail "expected a rail hit, got none: $text"
    else
        [[ "$got" -eq 0 ]] || fail "expected no rail hit, got $got: $text"
    fi
}

test_rail_detection() {
    # Rails actually in use across the rigs (verbatim shapes, 2026-08-06).
    assert_rail halt 'HALT branch-ready — the re-run is operator-attended (customer tenant).'
    assert_rail halt 'ACCEPTANCE: full offline suite green. HALT branch-ready — do NOT deploy.'
    assert_rail halt 'PLAIN rail: branch + HALT branch-ready, mayor publishes, no push.'
    assert_rail halt 'Halt at branch-ready; the operator publishes.'
    assert_rail halt 'branch-ready halt, then hand to the PL.'
    assert_rail halt 'Do not push this anywhere; the mayor publishes it.'
    assert_rail halt 'no-push rail applies to this arc.'
    assert_rail halt 'Rails: auto_push=false, base=main.'

    # The standing "polecats push a branch, never main" rule is a DIFFERENT
    # rail. Every polecat already obeys it, and half the beads in the city
    # quote it — matching on it would make the gate cry wolf.
    assert_rail push 'Polecats do not push to main, close beads, or wait around.'
    assert_rail push 'Never push directly to main; open a PR.'
    assert_rail push 'Do not push to origin/main under any circumstances.'

    # Descriptive prose about pushing is not an instruction not to push.
    # (gci-5bm: "commits locally and never pushes" is a bug report.)
    assert_rail push 'Establish why mirror-sync commits locally and never pushes.'
    assert_rail push 'The tag was never pushed, so the release is invisible.'
    assert_rail push 'Add a retry to the ingest job when the API 429s.'
    assert_rail push 'Refactor the emissions-factor loader and add tests.'
}

# --- 3. the halts must append their notes, never replace them ---------------

# `gc bd update --notes X` REPLACES the whole notes field; `--append-notes X`
# appends. Both halts below write a note onto a bead they did not create, and
# both then ask a human to act on what that bead says — the ambiguity halt asks
# whoever owns the rail to resolve it, and in the usual case the rail is written
# in the very notes a bare `--notes` would have just deleted. So the gate would
# destroy the evidence for its own escalation. This was observed twice on a live
# rig via the auto_push=false halt, destroying a project lead's instruction text
# (gci-3f5u); it is asserted here so the ambiguity halt cannot reintroduce it.
#
# Asserted over EXECUTABLE lines only. The step names `--notes` on purpose in
# its prose and in two recipe comments, and a negative check over the whole step
# text would fail on a correct patch — a guard that is red on correct input gets
# deleted for being noisy.
test_halt_notes_append() {
    python3 - "$FORMULA" <<'PY' || fail "submit-and-exit must append its notes, never replace them"
import sys
import tomllib

with open(sys.argv[1], "rb") as handle:
    steps = tomllib.load(handle)["steps"]
step = [s for s in steps if s["id"] == "submit-and-exit"]
if len(step) != 1:
    raise SystemExit("submit-and-exit step not found")

# The lines an agent RUNS: inside a bash fence, not a comment.
code, inside = [], False
for line in step[0]["description"].split("\n"):
    if line.startswith("```"):
        inside = line.startswith("```bash")
        continue
    if inside and not line.lstrip().startswith("#"):
        code.append(line)

problems = []
body = "\n".join(code)
if "--notes " in body or "--notes=" in body:
    problems.append("a bare --notes survives in a recipe: it REPLACES the bead's notes")

# Every `gc bd update` that writes a note must write it with --append-notes,
# and both halts must be among them.
invocations, current = [], None
for line in code:
    if current is not None:
        current.append(line)
        if not line.rstrip().endswith("\\"):
            invocations.append("\n".join(current))
            current = None
        continue
    if "gc bd update" in line:
        current = [line]
        if not line.rstrip().endswith("\\"):
            invocations.append("\n".join(current))
            current = None
if current is not None:
    invocations.append("\n".join(current))

for marker, what in (("halt_reason=auto_push_false", "the explicit opt-out halt"),
                     ("halt_reason=no_push_rail_unresolved", "the fail-closed ambiguity halt"),
                     ("Implemented:", "the refinery handoff")):
    hits = [i for i in invocations if marker in i]
    if len(hits) != 1:
        problems.append("expected exactly one gc bd update for %s, found %d" % (what, len(hits)))
        continue
    if "--append-notes" not in hits[0]:
        problems.append("%s does not use --append-notes" % what)

for p in problems:
    print("  " + p, file=sys.stderr)
raise SystemExit(1 if problems else 0)
PY
}

test_gate_structure
test_rail_detection
test_halt_notes_append

echo "polecat push gate tests passed"
