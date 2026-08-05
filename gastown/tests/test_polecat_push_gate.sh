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
#      halt_reason, so a re-sling can tell "operator said no" from "nobody said".
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

test_gate_structure
test_rail_detection

echo "polecat push gate tests passed"
