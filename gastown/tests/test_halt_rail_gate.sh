#!/usr/bin/env bash
set -euo pipefail

# test_halt_rail_gate.sh — the PLAIN-rail halt gate for the witness and refinery
# patrols (gastown/formulas/mol-{witness,refinery}-patrol.toml).
#
# THE DEFECT. A polecat on a no-push rail has a designed stopping point: it
# finishes, commits to its branch, writes `branch_ready` / `halt_reason`, clears
# its own assignee and hands the bead on for a human or the mayor to publish. The
# branch is deliberately unpublished and the session is deliberately gone. Two
# patrol steps could not tell that apart from a crash, because neither looked at
# the markers the polecat left: both read the same evidence — dead session,
# unmerged branch — and both concluded "repair this". Observed in one city on
# 2026-08-27: `recover-orphaned-beads` classified two held beads as orphans and
# PUSHED their branches; `find-work` then matched one on `has metadata.branch`
# alone, merged it to main and closed it. An operator-held bead was published
# with no approval anywhere in the chain.
#
# The rule was already written down as prose. No step executed it. That is the
# actual lesson, and it is why this file runs the SHIPPED TEXT: each case pulls
# the bash recipe out of the formula's own step description — after TOML escape
# processing, exactly the bytes an agent reads at pour time — and evaluates it
# verbatim against a stubbed `gc`. A retyped copy of the predicate would drift
# from the prose the agent actually follows, which is the defect itself.
#
# BOTH ARMS matter. A gate that only ever skips is as wrong as one that never
# skips: it would freeze orphan recovery and starve the merge queue. Every
# fixture below asserts either a fire or a deliberate hold, and the fixtures are
# bead shapes observed in a live city, not invented ones.
#
# Usage: bash gastown/tests/test_halt_rail_gate.sh

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
WITNESS_TOML="$ROOT/gastown/formulas/mol-witness-patrol.toml"
REFINERY_TOML="$ROOT/gastown/formulas/mol-refinery-patrol.toml"

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
BIN="$tmp/bin"
mkdir -p "$BIN"

# --- fixtures: the live shapes this patch was written from --------------------
# Each is one bead as `gc bd list --json` yields it.
f_det_efer='{"id":"det-efer","metadata":{"branch":"polecat/det-efer","branch_ready":true,"halt_reason":"auto_push_false","auto_push":"false"}}'
f_sb_adma='{"id":"sb-adma","metadata":{"branch":"polecat/sb-adma","branch_ready":true,"halt_reason":"mayor_publishes_no_push","target":"main"}}'
f_det_d3rn='{"id":"det-d3rn","metadata":{"auto_push":"false"}}'
f_sb_j2o7='{"id":"sb-j2o7","metadata":{"halt_reason":""}}'
f_notready='{"id":"gc-notready","metadata":{"branch":"polecat/gc-notready","branch_ready":false}}'
f_ordinary='{"id":"gc-ordinary","metadata":{"branch":"polecat/gc-ordinary","work_dir":"/w/gc-ordinary"}}'
f_armed='{"id":"gc-armed","metadata":{"branch":"polecat/gc-armed","auto_push":"true"}}'
f_string_meta='{"id":"gc-strmeta","metadata":"{\"halt_reason\":\"pl_lands_via_mayor\"}"}'
f_bool_false='{"id":"gc-boolfalse","metadata":{"branch":"polecat/gc-boolfalse","auto_push":false}}'
f_prose='{"id":"fph-26e6","metadata":{"halt_reason":"bead_spec_explicit_halt: operator re-runs live from their own machine, so the polecat must not"}}'
f_unreadable='{}'

# --- recipe extraction: run the shipped text, never a retyped copy -------------

# extract_block <file> <first-line-regex> <last-line-regex> — the bash recipe as
# it appears in the patch markdown, between those two anchor lines inclusive.
# step_text <formula.toml> <step-id> — one step description, as an agent reads
# it. Going through the TOML parser is the point: step text lives in a basic
# multi-line string where a backslash starts an escape sequence, so a legal
# escape is silently CONSUMED and the bytes the agent runs are not the bytes
# anyone reviewed (`printf '%s\n'` arrives holding a real newline). Extracting
# post-parse means every assertion below is made against the delivered text.
step_text() {
    python3 - "$1" "$2" <<'PYEOF'
import sys, tomllib
doc = tomllib.load(open(sys.argv[1], "rb"))
steps = [s for s in doc.get("steps", []) if s.get("id") == sys.argv[2]]
if len(steps) != 1:
    sys.exit("%s: expected exactly 1 step %r, found %d" % (sys.argv[1], sys.argv[2], len(steps)))
print(steps[0]["description"])
PYEOF
}

# extract_block <file> <first-line-regex> <last-line-regex> — inclusive.
extract_block() {
    awk -v a="$2" -v b="$3" '
        $0 ~ a { on = 1 }
        on     { print }
        on && $0 ~ b { exit }
    ' "$1"
}

step_text "$WITNESS_TOML" recover-orphaned-beads >"$tmp/witness.step" ||
    fail "could not read recover-orphaned-beads from $WITNESS_TOML"
step_text "$REFINERY_TOML" find-work >"$tmp/refinery.step" ||
    fail "could not read find-work from $REFINERY_TOML"

WITNESS_RECIPE=$(extract_block "$tmp/witness.step" '^HALT_GATE=' '^MARKER=')
REFINERY_RECIPE=$(extract_block "$tmp/refinery.step" '^HALT_GATE=' '^SKIPPED=')
[ -n "$WITNESS_RECIPE" ] || fail "recover-orphaned-beads carries no HALT_GATE recipe"
[ -n "$REFINERY_RECIPE" ] || fail "find-work carries no HALT_GATE recipe"

# --- drift guard --------------------------------------------------------------
# The `def halt_gate:` classifier is embedded in BOTH steps. The invocation line
# after it differs on purpose (one bead in the witness, a list in the refinery);
# the classifier must not. Two copies of a predicate is one copy too many unless
# something compares them on every run.

embedded_def() {
    awk '/^HALT_GATE=.def halt_gate:$/ { on = 1; sub(/^HALT_GATE=./, ""); }
         on { print }
         on && /^ +else "" end\)\};$/ { exit }' "$1"
}

test_the_two_shipped_gates_are_byte_identical() {
    local w r
    w=$(embedded_def "$tmp/witness.step")
    r=$(embedded_def "$tmp/refinery.step")
    [ -n "$w" ] || fail "recover-orphaned-beads has no HALT_GATE='def halt_gate:' embed"
    [ -n "$r" ] || fail "find-work has no HALT_GATE='def halt_gate:' embed"
    [ "$(printf '%s\n' "$w" | wc -l)" -ge 15 ] || fail "the embedded def looks truncated"
    case "$w" in
        *"'"*) fail "the def contains a single quote; it is embedded in a bash '...' string" ;;
    esac
    # Compare the whole BLOCK, never `grep -F "$w"`: a multi-line fixed pattern is
    # treated by grep as one pattern PER LINE, ORed, so it passes as long as any
    # single line matches — which is always. (Caught by mutation: a one-space edit
    # inside an embed survived the grep form.)
    [ "$w" = "$r" ] ||
        fail "the witness and refinery gates have drifted from each other"
    echo "ok: both shipped halt_gate defs are byte-identical"
}

# --- the executable lines the gate depends on ---------------------------------

test_the_patched_steps_carry_the_gate_wiring() {
    # Assert against the EXECUTABLE lines, not the prose around them: an earlier
    # revision of this checked for `limit=25` anywhere in the step, which the
    # paragraph EXPLAINING the limit satisfies on its own — so reverting the real
    # query to --limit=1 passed. (Caught by mutation.)
    grep -q 'echo "HALT-SKIP \$BEAD (\$MARKER)' "$tmp/witness.step" ||
        fail "the witness step does not report the beads it skipped"
    grep -q -- '--has-metadata-key=branch --limit=25 --json' "$tmp/refinery.step" ||
        fail "the find-work query is not widened past --limit=1"
    grep -q -- '--has-metadata-key=branch --limit=1 --json' "$tmp/refinery.step" &&
        fail "the find-work query still takes a single row, which one held bead can starve"
    grep -q 'select(.gate.publish_blocked | not)' "$tmp/refinery.step" ||
        fail "the find-work query has no publish_blocked filter"
    grep -q 'HALT-SKIP (no-publish rail, not merged):' "$tmp/refinery.step" ||
        fail "the refinery step does not report the beads it skipped"
    echo "ok: both steps carry the gate wiring on their executable lines"
}

# --- witness arm --------------------------------------------------------------

# stub `gc bd show <id> --json` from a single-bead fixture file
write_gc_show_stub() {
    cat >"$BIN/gc" <<'SH'
#!/usr/bin/env sh
case "$*" in
    *"bd show"*) printf '[%s]' "$(cat "$GC_FIXTURE")" ;;
    *"bd list"*) cat "$GC_FIXTURE" ;;
    *) printf '' ;;
esac
SH
    chmod +x "$BIN/gc"
}

# run_witness <bead-json> — evaluates the shipped witness recipe, exporting
# FINISHED / PUBLISH_BLOCKED / MARKER.
run_witness() {
    printf '%s' "$1" >"$tmp/fixture.json"
    # shellcheck disable=SC2034  # read by the eval'd recipe, not by this file
    BEAD="probe"
    export GC_FIXTURE="$tmp/fixture.json"
    # Stub only for the duration of the eval — the anchor-freshness case below
    # needs the REAL gc back on PATH afterwards.
    local saved_path="$PATH"
    PATH="$BIN:$PATH"
    eval "$WITNESS_RECIPE"
    PATH="$saved_path"
}

want_witness() {
    local label="$1" bead="$2" want_fin="$3" want_block="$4" want_marker="$5"
    run_witness "$bead"
    [ "$FINISHED" = "$want_fin" ] ||
        fail "$label: finished=$FINISHED, want $want_fin"
    [ "$PUBLISH_BLOCKED" = "$want_block" ] ||
        fail "$label: publish_blocked=$PUBLISH_BLOCKED, want $want_block"
    [ "$MARKER" = "$want_marker" ] ||
        fail "$label: marker='$MARKER', want '$want_marker'"
}

test_witness_skips_finished_and_halted() {
    # THE REGRESSION. det-efer and det-1aa0 were pushed and their worktrees
    # deleted by a witness salvage that read them as orphans.
    want_witness "det-efer (branch_ready + halt_reason + auto_push=false)" \
        "$f_det_efer" true true "halt_reason=auto_push_false"
    want_witness "sb-adma (mayor publishes)" \
        "$f_sb_adma" true true "halt_reason=mayor_publishes_no_push"
    want_witness "sb-j2o7 (halt_reason present but EMPTY)" \
        "$f_sb_j2o7" true true "halt_reason"
    want_witness "long prose halt_reason is truncated, not dropped" \
        "$f_prose" true true "halt_reason=bead_spec_explicit_halt: operator re-runs live from their ow"
    want_witness "metadata as a JSON STRING (rig variance)" \
        "$f_string_meta" true true "halt_reason=pl_lands_via_mayor"
    echo "ok: witness reports-only on every finished-and-halted shape"
}

test_witness_recovers_but_never_publishes_a_preemptive_hold() {
    # det-d3rn: auto_push=false set at CREATION, before any work. Not finished —
    # a worker that dies here really is dead and the bead must become schedulable
    # again — but the witness may never push it.
    want_witness "det-d3rn (pre-emptive hold, no branch yet)" \
        "$f_det_d3rn" false true "auto_push=false"
    want_witness "auto_push written as boolean false" \
        "$f_bool_false" false true "auto_push=false"
    echo "ok: pre-emptive holds are recoverable but publish-blocked"
}

test_witness_still_salvages_ordinary_orphans() {
    # The hold arm. A gate that muted these would freeze orphan recovery — the
    # witness's core job — which is a worse failure than the one being fixed.
    want_witness "ordinary orphan (branch + work_dir, no markers)" \
        "$f_ordinary" false false ""
    want_witness "auto_push=true (explicitly armed)" \
        "$f_armed" false false ""
    want_witness "branch_ready=false means NOT halted, not absent" \
        "$f_notready" false false ""
    echo "ok: ordinary orphans still salvage; branch_ready=false is not a halt"
}

test_witness_fails_open_on_an_unreadable_bead() {
    # One malformed bead must not freeze recovery for the whole rig.
    want_witness "unreadable bead" "$f_unreadable" false false ""
    echo "ok: gate fails open — an unreadable bead recovers as it does today"
}

# --- refinery arm -------------------------------------------------------------

# run_refinery <queue-json-array> — evaluates the shipped refinery recipe,
# exporting WORK and SKIPPED.
run_refinery() {
    printf '%s' "$1" >"$tmp/fixture.json"
    export GC_FIXTURE="$tmp/fixture.json"
    export GC_RIG="" GC_AGENT="rig/gastown.refinery"
    local saved_path="$PATH"
    PATH="$BIN:$PATH"
    eval "$REFINERY_RECIPE"
    PATH="$saved_path"
}

test_refinery_refuses_to_merge_a_halted_bead() {
    # THE SECOND FACE. det-1aa0 was merged to main at ff751f4 and closed by
    # automation because find-work matched on `has metadata.branch` alone.
    run_refinery "[$f_det_efer,$f_sb_adma]"
    [ -z "$WORK" ] || fail "an all-halted queue must yield no work, got '$WORK'"
    printf '%s' "$SKIPPED" | grep -q 'det-efer (halt_reason=auto_push_false)' ||
        fail "det-efer must be reported as skipped, got: $SKIPPED"
    printf '%s' "$SKIPPED" | grep -q 'sb-adma (halt_reason=mayor_publishes_no_push)' ||
        fail "sb-adma must be reported as skipped, got: $SKIPPED"
    echo "ok: refinery skips halted beads and names them"
}

test_refinery_is_not_starved_by_a_halted_bead_at_the_head() {
    # THE TRAP IN THE NAIVE FIX. Filtering a --limit=1 result would let one held
    # bead at the head mask every mergeable bead behind it: the refinery reports
    # IDLE while real work waits, forever, since the hold is deliberate.
    run_refinery "[$f_det_efer,$f_det_d3rn,$f_ordinary,$f_armed]"
    [ "$WORK" = "gc-ordinary" ] ||
        fail "the first MERGEABLE bead must be selected past two halted ones, got '$WORK'"
    printf '%s' "$SKIPPED" | grep -q 'det-d3rn (auto_push=false)' ||
        fail "the pre-emptive hold must also be skipped by the refinery: $SKIPPED"
    echo "ok: halted beads at the head of the queue do not starve the refinery"
}

test_refinery_merges_an_ordinary_queue_untouched() {
    # The hold arm: with no markers anywhere the gate must be invisible.
    run_refinery "[$f_ordinary,$f_armed]"
    [ "$WORK" = "gc-ordinary" ] || fail "unmarked queue must select its head, got '$WORK'"
    [ -z "$SKIPPED" ] || fail "unmarked queue must skip nothing, got: $SKIPPED"
    echo "ok: an unmarked queue is unaffected by the gate"
}

test_refinery_empty_queue_is_still_idle() {
    run_refinery "[]"
    [ -z "$WORK" ] || fail "empty queue must yield no work, got '$WORK'"
    [ -z "$SKIPPED" ] || fail "empty queue must skip nothing, got: $SKIPPED"
    echo "ok: an empty queue still reads as IDLE"
}

write_gc_show_stub() {
    cat >"$BIN/gc" <<'SH'
#!/usr/bin/env sh
case "$*" in
    *"bd show"*) printf '[%s]' "$(cat "$GC_FIXTURE")" ;;
    *"bd list"*) cat "$GC_FIXTURE" ;;
    *) printf '' ;;
esac
SH
    chmod +x "$BIN/gc"
}

# run_witness <bead-json> — evaluates the shipped witness recipe, exporting
# FINISHED / PUBLISH_BLOCKED / MARKER.
run_witness() {
    printf '%s' "$1" >"$tmp/fixture.json"
    # shellcheck disable=SC2034  # read by the eval'd recipe, not by this file
    BEAD="probe"
    export GC_FIXTURE="$tmp/fixture.json"
    # Stub only for the duration of the eval — the anchor-freshness case below
    # needs the REAL gc back on PATH afterwards.
    local saved_path="$PATH"
    PATH="$BIN:$PATH"
    eval "$WITNESS_RECIPE"
    PATH="$saved_path"
}

want_witness() {
    local label="$1" bead="$2" want_fin="$3" want_block="$4" want_marker="$5"
    run_witness "$bead"
    [ "$FINISHED" = "$want_fin" ] ||
        fail "$label: finished=$FINISHED, want $want_fin"
    [ "$PUBLISH_BLOCKED" = "$want_block" ] ||
        fail "$label: publish_blocked=$PUBLISH_BLOCKED, want $want_block"
    [ "$MARKER" = "$want_marker" ] ||
        fail "$label: marker='$MARKER', want '$want_marker'"
}

test_witness_skips_finished_and_halted() {
    # THE REGRESSION. det-efer and det-1aa0 were pushed and their worktrees
    # deleted by a witness salvage that read them as orphans.
    want_witness "det-efer (branch_ready + halt_reason + auto_push=false)" \
        "$f_det_efer" true true "halt_reason=auto_push_false"
    want_witness "sb-adma (mayor publishes)" \
        "$f_sb_adma" true true "halt_reason=mayor_publishes_no_push"
    want_witness "sb-j2o7 (halt_reason present but EMPTY)" \
        "$f_sb_j2o7" true true "halt_reason"
    want_witness "long prose halt_reason is truncated, not dropped" \
        "$f_prose" true true "halt_reason=bead_spec_explicit_halt: operator re-runs live from their ow"
    want_witness "metadata as a JSON STRING (rig variance)" \
        "$f_string_meta" true true "halt_reason=pl_lands_via_mayor"
    echo "ok: witness reports-only on every finished-and-halted shape"
}

test_witness_recovers_but_never_publishes_a_preemptive_hold() {
    # det-d3rn: auto_push=false set at CREATION, before any work. Not finished —
    # a worker that dies here really is dead and the bead must become schedulable
    # again — but the witness may never push it.
    want_witness "det-d3rn (pre-emptive hold, no branch yet)" \
        "$f_det_d3rn" false true "auto_push=false"
    want_witness "auto_push written as boolean false" \
        "$f_bool_false" false true "auto_push=false"
    echo "ok: pre-emptive holds are recoverable but publish-blocked"
}

test_witness_still_salvages_ordinary_orphans() {
    # The hold arm. A gate that muted these would freeze orphan recovery — the
    # witness's core job — which is a worse failure than the one being fixed.
    want_witness "ordinary orphan (branch + work_dir, no markers)" \
        "$f_ordinary" false false ""
    want_witness "auto_push=true (explicitly armed)" \
        "$f_armed" false false ""
    want_witness "branch_ready=false means NOT halted, not absent" \
        "$f_notready" false false ""
    echo "ok: ordinary orphans still salvage; branch_ready=false is not a halt"
}

test_witness_fails_open_on_an_unreadable_bead() {
    # One malformed bead must not freeze recovery for the whole rig.
    want_witness "unreadable bead" "$f_unreadable" false false ""
    echo "ok: gate fails open — an unreadable bead recovers as it does today"
}

# --- refinery arm -------------------------------------------------------------

# run_refinery <queue-json-array> — evaluates the shipped refinery recipe,
# exporting WORK and SKIPPED.
run_refinery() {
    printf '%s' "$1" >"$tmp/fixture.json"
    export GC_FIXTURE="$tmp/fixture.json"
    export GC_RIG="" GC_AGENT="rig/gastown.refinery"
    local saved_path="$PATH"
    PATH="$BIN:$PATH"
    eval "$REFINERY_RECIPE"
    PATH="$saved_path"
}

test_refinery_refuses_to_merge_a_halted_bead() {
    # THE SECOND FACE. det-1aa0 was merged to main at ff751f4 and closed by
    # automation because find-work matched on `has metadata.branch` alone.
    run_refinery "[$f_det_efer,$f_sb_adma]"
    [ -z "$WORK" ] || fail "an all-halted queue must yield no work, got '$WORK'"
    printf '%s' "$SKIPPED" | grep -q 'det-efer (halt_reason=auto_push_false)' ||
        fail "det-efer must be reported as skipped, got: $SKIPPED"
    printf '%s' "$SKIPPED" | grep -q 'sb-adma (halt_reason=mayor_publishes_no_push)' ||
        fail "sb-adma must be reported as skipped, got: $SKIPPED"
    echo "ok: refinery skips halted beads and names them"
}

test_refinery_is_not_starved_by_a_halted_bead_at_the_head() {
    # THE TRAP IN THE NAIVE FIX. Filtering a --limit=1 result would let one held
    # bead at the head mask every mergeable bead behind it: the refinery reports
    # IDLE while real work waits, forever, since the hold is deliberate.
    run_refinery "[$f_det_efer,$f_det_d3rn,$f_ordinary,$f_armed]"
    [ "$WORK" = "gc-ordinary" ] ||
        fail "the first MERGEABLE bead must be selected past two halted ones, got '$WORK'"
    printf '%s' "$SKIPPED" | grep -q 'det-d3rn (auto_push=false)' ||
        fail "the pre-emptive hold must also be skipped by the refinery: $SKIPPED"
    echo "ok: halted beads at the head of the queue do not starve the refinery"
}

test_refinery_merges_an_ordinary_queue_untouched() {
    # The hold arm: with no markers anywhere the gate must be invisible.
    run_refinery "[$f_ordinary,$f_armed]"
    [ "$WORK" = "gc-ordinary" ] || fail "unmarked queue must select its head, got '$WORK'"
    [ -z "$SKIPPED" ] || fail "unmarked queue must skip nothing, got: $SKIPPED"
    echo "ok: an unmarked queue is unaffected by the gate"
}

test_refinery_empty_queue_is_still_idle() {
    run_refinery "[]"
    [ -z "$WORK" ] || fail "empty queue must yield no work, got '$WORK'"
    [ -z "$SKIPPED" ] || fail "empty queue must skip nothing, got: $SKIPPED"
    echo "ok: an empty queue still reads as IDLE"
}

# --- TOML embeddability -------------------------------------------------------

test_replacement_text_survives_a_toml_basic_string() {
    # These blocks get pasted into a TOML basic multi-line string, where a
    # backslash STARTS AN ESCAPE SEQUENCE — and survival, not merely parsing, is
    # the bar. An ILLEGAL escape stops the file parsing (the pack dead on arrival
    # at pour time); a LEGAL one is silently consumed, so the text the agent runs
    # is not the text anyone reviewed. Both shapes were live in the first draft:
    # jq backslash-parenthesis interpolation in the refinery SKIPPED line, and a
    # printf format whose newline escape TOML ate.
    local p
    for p in "$WITNESS_PATCH" "$REFINERY_PATCH"; do
        python3 "$PACK/tests/toml_embeddable.py" "$p" ||
            fail "$(basename "$p") has a bash block that cannot be embedded in a TOML basic string"
    done
    echo "ok: every hunk block survives a TOML basic multi-line string byte-for-byte"
}

# --- end-to-end: the hunks apply, and what they produce still parses ----------

# step_text <formula.toml> <step-id> — one step description, as an agent reads it.
step_text() {
    python3 - "$1" "$2" <<'PYEOF'
import sys, tomllib
doc = tomllib.load(open(sys.argv[1], "rb"))
steps = [s for s in doc.get("steps", []) if s.get("id") == sys.argv[2]]
if len(steps) != 1:
    sys.exit("%s: expected exactly 1 step %r, found %d" % (sys.argv[1], sys.argv[2], len(steps)))
print(steps[0]["description"])
PYEOF
}

test_hunks_apply_to_the_live_formulas_and_the_result_parses() {
    # A patch nobody has applied is a patch nobody knows applies. This runs the
    # same Find/Replace structure a maintainer follows, against the real formula
    # TOMLs, and asserts the patched FILE still parses and still carries the
    # step — the failure mode that would take the whole pack down at pour time
    # rather than merely leaving the bug unfixed.
    #
    # Local-only: CI has no city and no formula tree.
    local formulas="/home/edward/gc/formulas"
    if [ ! -f "$formulas/mol-witness-patrol.toml" ]; then
        echo "skip: no local formula tree — hunk application is checked on the city box"
        return 0
    fi
    # A city box is legitimately in one of TWO states and the suite must stay
    # green — and keep its teeth — in both. BEFORE the live apply the hunks must
    # apply; AFTER it the live files already carry the gate and the anchors are
    # consumed by the very edit this pack exists to make. Re-applying in the
    # second state is not merely redundant, it is WRONG: W1 inserts after an
    # anchor that survives its own edit and W2 replaces one that is a prefix of
    # its replacement, so a second pass duplicates the block. Detect the state,
    # then run the SAME content assertions against whichever text is
    # authoritative — the applied result, or the live file that already is it.
    local mode
    if grep -q 'def halt_gate:' "$formulas/mol-witness-patrol.toml"; then
        mode="are already applied to"
        step_text "$formulas/mol-witness-patrol.toml" recover-orphaned-beads \
            >"$tmp/witness.step" || fail "could not read the patched witness step"
        step_text "$formulas/mol-refinery-patrol.toml" find-work \
            >"$tmp/refinery.step" || fail "could not read the patched refinery step"
    else
        mode="apply to"
        python3 "$PACK/tests/apply_hunks.py" \
            "$WITNESS_PATCH" "$formulas/mol-witness-patrol.toml" recover-orphaned-beads \
            >"$tmp/witness.step" 2>"$tmp/witness.err" ||
            fail "witness hunks did not apply: $(cat "$tmp/witness.err")"
        python3 "$PACK/tests/apply_hunks.py" \
            "$REFINERY_PATCH" "$formulas/mol-refinery-patrol.toml" find-work \
            >"$tmp/refinery.step" 2>"$tmp/refinery.err" ||
            fail "refinery hunk did not apply: $(cat "$tmp/refinery.err")"
    fi

    # apply_hunks.py parses the patched file itself and exits non-zero if it no
    # longer loads or lost the step; what remains to check is the content.
    local f
    for f in "$tmp/witness.step" "$tmp/refinery.step"; do
        grep -q 'def halt_gate:' "$f" || fail "$f: patched step lost the gate"
    done
    # Assert against the executable lines, not the prose around them: an earlier
    # version of this checked for `limit=25` anywhere in the step, which the
    # paragraph EXPLAINING the limit satisfies on its own — so reverting the
    # actual query to --limit=1 passed. (Caught by mutation.)
    grep -q 'echo "HALT-SKIP \$BEAD (\$MARKER)' "$tmp/witness.step" ||
        fail "witness step lost its halt-skip report line"
    grep -q -- '--has-metadata-key=branch --limit=25 --json' "$tmp/refinery.step" ||
        fail "refinery query lost the widened page limit"
    grep -q -- '--has-metadata-key=branch --limit=1 --json' "$tmp/refinery.step" &&
        fail "refinery step still carries the unfiltered --limit=1 query"
    grep -q 'select(.gate.publish_blocked | not)' "$tmp/refinery.step" ||
        fail "refinery query lost the publish_blocked filter"

    # The patched STEP TEXT must be structurally clean, not merely parseable.
    # A **Find** block fenced no wider than a ```bash recipe it contains is
    # truncated by FENCE's non-greedy match, so the replacement lands and the
    # TAIL of the real anchor is stranded after it: a stray closing fence and a
    # duplicated line. TOML parses it happily and every content assertion above
    # still passes — the corruption is only visible in the rendered step, which
    # is what the agent reads. Hunk W2 shipped that way (gci-gn81); apply_hunks.py
    # now refuses the malformed block, and these two assertions are the backstop
    # that catches the same shape arriving through any other route.
    for f in "$tmp/witness.step" "$tmp/refinery.step"; do
        local fences
        fences=$(grep -c '^```' "$f" || true)
        [ $((fences % 2)) -eq 0 ] ||
            fail "$f: $fences fence lines — odd count means a stranded fence from a truncated anchor"
    done
    local n_check
    n_check=$(grep -c '^Check the bead and worktree state, then act:$' "$tmp/witness.step" || true)
    [ "$n_check" = "1" ] ||
        fail "witness step repeats the Hunk W2 anchor tail $n_check times (expected 1) — truncated anchor"

    echo "ok: all four hunks $mode the live formulas; results parse, stay clean, and keep the gate"
}

# --- anchor freshness (local only) --------------------------------------------

test_patch_anchors_still_match_the_live_formula_text() {
    if ! command -v gc >/dev/null 2>&1; then
        echo "skip: gc not on PATH — anchor freshness is checked on the city box, not in CI"
        return 0
    fi
    local live
    live=$(gc formula show mol-refinery-patrol --json 2>/dev/null |
        jq -r '.steps[]|select(.id=="mol-refinery-patrol.find-work").description' 2>/dev/null || true)
    if [ -z "$live" ]; then
        echo "skip: could not read the live mol-refinery-patrol text"
        return 0
    fi
    # Once the live apply has run, the anchor is legitimately gone — this pack
    # consumed it. Assert the APPLIED shape instead, so the check keeps its
    # teeth (a reverted, half-applied or re-materialised formula still fails)
    # rather than going permanently red the moment the work lands.
    if printf '%s' "$live" | grep -q -- '--has-metadata-key=branch --limit=25 --json'; then
        printf '%s' "$live" | grep -q 'def halt_gate:' ||
            fail "the live find-work step has the widened query but no gate — half-applied"
        echo "ok: the live find-work step carries the applied gate (anchor consumed, as expected)"
        return 0
    fi
    printf '%s' "$live" | grep -q -- '--has-metadata-key=branch --limit=1 --json' ||
        fail "the find-work anchor has drifted from the live formula — re-anchor Hunk R1 before filing"
    echo "ok: find-work anchor still matches the live formula text"
}

write_gc_show_stub

test_the_two_shipped_gates_are_byte_identical
test_the_patched_steps_carry_the_gate_wiring
test_witness_skips_finished_and_halted
test_witness_recovers_but_never_publishes_a_preemptive_hold
test_witness_still_salvages_ordinary_orphans
test_witness_fails_open_on_an_unreadable_bead
test_refinery_refuses_to_merge_a_halted_bead
test_refinery_is_not_starved_by_a_halted_bead_at_the_head
test_refinery_merges_an_ordinary_queue_untouched
test_refinery_empty_queue_is_still_idle

echo "PASS: halt-rail gate"
