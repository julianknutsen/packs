#!/usr/bin/env bash
# mol-polecat-work's closing writes must APPEND their note, never replace it.
#
# `gc bd update` has two note flags and they are not two spellings of one thing:
#
#     --notes X          REPLACES the entire notes field with X
#     --append-notes X   appends X to it, newline-separated
#
# submit-and-exit used `--notes` in both of its writes, so the last act of every
# polecat was to delete every note anyone else had put on the bead it was
# finishing — the requester's instructions, an operator's hold, a prior
# polecat's handover. Observed twice on a live rig, one week apart, through the
# auto_push=false halt: the halt note replaced a project lead's instruction text
# and the record had to be restored by hand.
#
# It survived that long because it is the HAPPY path. No error, no diff, no
# second actor: a polecat that did its job correctly and handed off cleanly is
# the thing that destroys the record. `gc bd update --help` calls `--notes`
# "Additional notes", which is exactly the reading that made the bug.
#
# Four arms, because three of them can pass on a broken fix:
#
#   preserving  a write onto a bead that already has notes keeps them, in order.
#   recording   on an empty bead the note still lands, and lands alone. A "fix"
#               that simply stopped writing the note would satisfy `preserving`
#               and silently lose the handoff signal.
#   marker      branch_ready / halt_reason / the cleared assignee and
#               gc.routed_to survive untouched. They are what tells a deliberate
#               halt from a crash; a halt that stopped writing one of them would
#               be read as an orphan and handed to a refinery as ordinary
#               mergeable work — a notes bug turned into a publish bug.
#   mutation    the PRE-FIX text — the same shipped write with the flag put
#               back — must actually DESTROY the notes. Without this arm the
#               preserving arm could pass against a stub that models nothing,
#               which is how a green fixture lies.
#
# Every arm runs the SHIPPED TEXT, lifted out of the formula and evaluated. A
# retyped copy of the recipe would pass forever after the formula drifted away
# from it, and the formula is what an agent actually runs.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
FORMULA="$ROOT/gastown/formulas/mol-polecat-work.toml"

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

[[ -f "$FORMULA" ]] || fail "formula not found: $FORMULA"

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

# --- lift the shipped writes out of the formula -----------------------------

# Each `gc bd update` invocation in submit-and-exit is written to $TMP/write.<tag>
# so the arms below can run the real thing. The scan reads EXECUTABLE lines only:
# inside a ```bash fence, not a comment. The step names `--notes` on purpose in
# its prose and in two recipe comments, so a whole-text check would fail on a
# CORRECT formula — and a guard that is red on correct input gets deleted for
# being noisy.
python3 - "$FORMULA" "$TMP" <<'PY' || fail "submit-and-exit must append its notes, never replace them"
import sys
import tomllib

formula, out = sys.argv[1], sys.argv[2]
with open(formula, "rb") as handle:
    steps = tomllib.load(handle)["steps"]
step = [s for s in steps if s["id"] == "submit-and-exit"]
if len(step) != 1:
    raise SystemExit("submit-and-exit step not found")

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
# A FLOOR, not a count. The bare--notes scan above already catches any write
# that replaces, so this only has to catch a step that lost a write entirely —
# and a count that must be edited to stay green is a count that gets edited
# without being read. (mol-polecat-work grows a third write downstream.)
if body.count("--append-notes") < 2:
    problems.append("expected at least 2 appending writes, found %d" % body.count("--append-notes"))
if "Never use\n`--notes`" not in step[0]["description"]:
    problems.append("the append rule is no longer stated in the step prose")

# Collect each `gc bd update`, following backslash continuations.
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

for tag, marker, what in (("halt", "halt_reason=auto_push_false", "the auto_push=false halt"),
                          ("handoff", "Implemented:", "the refinery handoff")):
    hits = [i for i in invocations if marker in i]
    if len(hits) != 1:
        problems.append("expected exactly one gc bd update for %s, found %d" % (what, len(hits)))
        continue
    if "--append-notes" not in hits[0]:
        problems.append("%s does not use --append-notes" % what)
    with open("%s/write.%s" % (out, tag), "w", encoding="utf-8") as handle:
        handle.write(hits[0])

for p in problems:
    print("  " + p, file=sys.stderr)
raise SystemExit(1 if problems else 0)
PY

[[ -s "$TMP/write.halt" ]] || fail "could not lift the halt write out of $FORMULA"
[[ -s "$TMP/write.handoff" ]] || fail "could not lift the handoff write out of $FORMULA"

# --- the stub ---------------------------------------------------------------

# A closed PATH: what the recipe can reach is decided here, not by this box.
# `bash` and `env` are needed by the stub's own shebang, not by the recipe —
# `#!/usr/bin/env bash` resolves env by absolute path but then searches PATH for
# bash, so a closed PATH without it makes every stub fail to exec and every
# recipe "write nothing".
BIN="$TMP/bin"
mkdir -p "$BIN"
for tool in bash env cat printf sed; do
    resolved=$(command -v "$tool") || fail "missing required tool: $tool"
    ln -sf "$resolved" "$BIN/$tool"
done

# The one behaviour under test, and it is not a guess: verified against a live
# bead seeded with two note lines — `--notes` left one, `--append-notes` left
# two. The help text is pinned by this fixture rather than trusted.
cat > "$BIN/gc" <<'STUB'
#!/usr/bin/env bash
# Matched with the program name prepended so the case arm reads as the real
# invocation, `gc bd update`, rather than as a bare bd command.
case "gc $1 $2" in
  "gc bd update")
    shift 2
    printf '%s\n' "$*" >> "$TMPD/calls"
    while [ $# -gt 0 ]; do
      case "$1" in
        --notes)          printf '%s' "$2" > "$TMPD/notes"; shift 2 ;;
        --notes=*)        printf '%s' "${1#--notes=}" > "$TMPD/notes"; shift ;;
        --append-notes)   a=$2; shift 2
                          if [ -s "$TMPD/notes" ]; then printf '\n%s' "$a" >> "$TMPD/notes"
                          else printf '%s' "$a" > "$TMPD/notes"; fi ;;
        --append-notes=*) a=${1#--append-notes=}; shift
                          if [ -s "$TMPD/notes" ]; then printf '\n%s' "$a" >> "$TMPD/notes"
                          else printf '%s' "$a" > "$TMPD/notes"; fi ;;
        *) shift ;;
      esac
    done
    ;;
  *) echo "unstubbed gc call: $*" >&2; exit 99 ;;
esac
STUB
chmod +x "$BIN/gc"

# run <write-file> <seed-notes> -> resulting notes on stdout; calls in $TMP/calls
run() {
    : > "$TMP/calls"
    printf '%s' "$2" > "$TMP/notes"
    (
        export TMPD="$TMP" PATH="$BIN"
        WORK_BEAD_ID="det-nu7"
        BRANCH="polecat/det-nu7"
        # {{base_branch}} is a formula var; rendering it is what pouring does.
        eval "$(sed 's/{{base_branch}}/main/g' "$1")"
    ) >/dev/null 2>&1 || true
    cat "$TMP/notes"
}

# A dead environment is already caught by the recording arm — a stub that never
# ran writes no note. This is here for the DIAGNOSTIC: without it that failure
# reads as "the halt no longer records its own note" and sends the reader
# hunting the formula for a bug that is actually in their PATH.
ran() {
    [[ -s "$TMP/calls" ]] || fail "$1: the recipe never reached the gc stub — check the closed PATH above, not the formula"
}

# The shape that was destroyed: a PL's item text plus a change-discipline note.
PL_NOTES='item 7: land the intake fix on the det branch before the PL review.
change discipline: no schema edits without a determination doc.'

# --- 1. preserving + recording + marker --------------------------------------

test_halt_write() {
    local out calls
    out=$(run "$TMP/write.halt" "$PL_NOTES")
    ran "halt write"
    [[ "$out" == *"item 7: land the intake fix"* ]] ||
        fail "the halt destroyed the requester's item-7 line"
    [[ "$out" == *"change discipline: no schema"* ]] ||
        fail "the halt destroyed the change-discipline line"
    [[ "$out" == *"Branch ready: auto_push=false"* ]] ||
        fail "the halt no longer records its own note"
    # Order is the evidence that it APPENDED rather than rewrote both by luck.
    [[ "$(printf '%s' "$out" | sed -n 1p)" == "item 7: land the intake fix on the det branch before the PL review." ]] ||
        fail "the pre-existing notes are no longer first"
    [[ "$(printf '%s' "$out" | sed -n '$p')" == "Branch ready: auto_push=false (no push, no refinery handoff)" ]] ||
        fail "the halt note is not the last line"

    # Recording arm: dropping the note entirely would pass every check above.
    out=$(run "$TMP/write.halt" "")
    [[ "$out" == "Branch ready: auto_push=false (no push, no refinery handoff)" ]] ||
        fail "on a bead with no notes the halt note must land, and land alone (got: $out)"

    # Marker arm: the keys that tell a deliberate halt from a crash.
    run "$TMP/write.halt" "$PL_NOTES" >/dev/null
    calls=$(cat "$TMP/calls")
    local key
    for key in '--set-metadata branch_ready=true' \
               '--set-metadata halt_reason=auto_push_false' \
               '--set-metadata branch=polecat/det-nu7' \
               '--set-metadata target=main' \
               '--set-metadata gc.routed_to=' \
               '--assignee=' \
               '--status=open'; do
        [[ "$calls" == *"$key"* ]] ||
            fail "the halt no longer writes '$key' — a deliberate halt would read as a crash"
    done
}

test_handoff_write() {
    local out
    out=$(run "$TMP/write.handoff" "$PL_NOTES")
    ran "handoff write"
    [[ "$out" == *"item 7: land the intake fix"* ]] ||
        fail "the refinery handoff destroyed the requester's notes"
    [[ "$out" == *"Implemented:"* ]] ||
        fail "the refinery handoff no longer records its own note"
    [[ "$(printf '%s' "$out" | sed -n '$p')" == "Implemented: <brief summary>" ]] ||
        fail "the handoff note is not the last line"

    out=$(run "$TMP/write.handoff" "")
    [[ "$out" == "Implemented: <brief summary>" ]] ||
        fail "on a bead with no notes the handoff note must land, and land alone (got: $out)"

    [[ "$(cat "$TMP/calls")" == *'--set-metadata target=main'* ]] ||
        fail "the handoff no longer records the target for the refinery"
}

# --- 2. the mutation arm ------------------------------------------------------

# Put the flag back and run the identical stub. If the pre-fix text does NOT
# destroy the notes, the stub models nothing and every assertion above is
# vacuous — a green fixture proving nothing.
test_prefix_text_still_destroys_notes() {
    local out tag
    for tag in halt handoff; do
        sed 's/--append-notes/--notes/g' "$TMP/write.$tag" > "$TMP/prefix.$tag"
        grep -q -- '--notes ' "$TMP/prefix.$tag" ||
            fail "could not synthesise the pre-fix $tag write"
        out=$(run "$TMP/prefix.$tag" "$PL_NOTES")
        ran "pre-fix $tag write"
        [[ "$out" != *"item 7:"* ]] ||
            fail "the stub does not model --notes as a REPLACE — the preserving arm proves nothing"
    done
}

test_halt_write
test_handoff_write
test_prefix_text_still_destroys_notes

echo "polecat notes-append tests passed"
