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
#      the notes would delete the instructions it is pointing at;
#   5. the metadata probe decodes the shapes ledgers actually serve, and halts
#      on the rest. `jq`'s has() ERRORS on a string, so a bare has("auto_push")
#      reported "no key" on a ledger serving metadata as a JSON string and the
#      EXPLICIT auto_push=false opt-out silently stopped firing (gci-to0l).
#
# Properties 1-4 assert what the gate SAYS. The last block runs it: the gate's
# bash is lifted out of the shipped step and executed against stubbed `gc` and
# `git`, so what is tested is the text an agent reads at pour time, never a
# retyped copy. "The words are all present" is a weaker claim than "it does not
# push" for a gate whose failure mode is a push to a customer estate.
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
    grep -qF 'halt_reason=metadata_unreadable' "$FORMULA" ||
        fail "the fail-closed unreadable-metadata halt is gone (gci-to0l)"
    local mail_sends
    mail_sends=$(grep -cF 'gc mail send' "$FORMULA")
    [[ "$mail_sends" -eq 2 ]] ||
        fail "both fail-closed halts must escalate; found $mail_sends 'gc mail send'"

    # A halt that lands after the push is worthless.
    local ambiguity_line push_line unreadable_line optout_line
    ambiguity_line=$(grep -nF 'halt_reason=no_push_rail_unresolved' "$FORMULA" | head -1 | cut -d: -f1)
    push_line=$(grep -nF 'git push origin HEAD' "$FORMULA" | head -1 | cut -d: -f1)
    unreadable_line=$(grep -nF 'halt_reason=metadata_unreadable' "$FORMULA" | head -1 | cut -d: -f1)
    optout_line=$(grep -nF 'halt_reason=auto_push_false' "$FORMULA" | head -1 | cut -d: -f1)
    [[ "$ambiguity_line" -lt "$push_line" ]] ||
        fail "fail-closed halt (line $ambiguity_line) comes after git push (line $push_line)"
    [[ "$unreadable_line" -lt "$push_line" ]] ||
        fail "unreadable-metadata halt (line $unreadable_line) comes after git push (line $push_line)"
    # It must also precede the opt-out test: AUTO_PUSH is meaningless when the
    # probe that produced it failed, so reading it first is reading a guess.
    [[ "$unreadable_line" -lt "$optout_line" ]] ||
        fail "unreadable-metadata halt (line $unreadable_line) comes after the auto_push test (line $optout_line)"
}

# --- 1b. the metadata probe is shape-normalised (gci-to0l) ------------------

# The regression: `jq`'s has() errors on a string, prints nothing, and exits
# non-zero — so `(.[0].metadata // {}) | if has("auto_push") ...` yielded "" for
# a ledger serving metadata as a JSON string. "" is not "false", so outcome (a),
# the EXPLICIT opt-out, stopped firing and control reached git push. A bead that
# recorded its rail properly AS METADATA and said nothing in prose is exactly
# the bead the (b) prose scan cannot save.
#
# Asserted over the step's EXECUTABLE lines, for the same reason the notes check
# is: the step names the old probe shape in its prose, and a check over the whole
# step text would be red on a correct patch.
test_metadata_probe_shape() {
    python3 - "$FORMULA" <<'PY' || fail "the metadata probe must decode object/null/string and halt on the rest"
import sys
import tomllib

with open(sys.argv[1], "rb") as handle:
    steps = tomllib.load(handle)["steps"]
step = [s for s in steps if s["id"] == "submit-and-exit"][0]["description"]

code, inside = [], False
for line in step.split("\n"):
    if line.startswith("```"):
        inside = line.startswith("```bash")
        continue
    if inside and not line.lstrip().startswith("#"):
        code.append(line)
body = "\n".join(code)

problems = []
if "fromjson" not in body:
    problems.append("the probe does not decode a JSON-string metadata payload")
if body.count('has("auto_push")') != 1 or 'type != "object" then error' not in body:
    problems.append('has("auto_push") must be reached only on an object, and error otherwise')
if 'type == "null"   then {}' not in body and 'type == "null" then {}' not in body:
    # null is how every measured ledger says "this bead has no metadata", and it
    # is the majority shape — halting on it would halt the city, not gate it.
    problems.append("null metadata must read as empty, NOT as unreadable")
if '[ -z "$WORK_JSON" ]' not in body:
    # jq prints nothing for empty input and exits 0, so a dead ledger cannot be
    # caught on jq's exit status alone.
    problems.append("no explicit guard for an empty payload from the ledger read")
if body.count("halt_reason=") != 3:
    problems.append("expected exactly three halt_reason writes, found %d" % body.count("halt_reason="))

# Every halt hands the bead back the same way. A halt that forgets one of these
# leaves the bead assigned and routed, and the next sweep treats it as ordinary
# mergeable work — a publish-without-approval bug, worse than the one being fixed.
for marker in ("branch_ready=true", 'gc.routed_to=""', '--assignee=""', "--status=open"):
    if body.count(marker) < 3:
        problems.append("halt marker not written by all three halts: %s" % marker)

for p in problems:
    print("  " + p, file=sys.stderr)
raise SystemExit(1 if problems else 0)
PY
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
# appends. Every halt below writes a note onto a bead it did not create, and
# each then asks a human to act on what that bead says — the ambiguity halt asks
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
                     ("halt_reason=metadata_unreadable", "the unreadable-metadata halt"),
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

# --- 4. the shipped gate recipe, executed -----------------------------------

# Properties 1-3 assert what the gate SAYS. This one runs it. The gate's bash
# block is lifted out of the shipped step, rendered, and executed against
# stubbed `gc` and `git`, so what is tested is the SHIPPED TEXT an agent reads
# at pour time and never a retyped copy. Retyping is how a tested rule drifts
# from the rule that actually runs.

test_gate_execution() {
    if ! command -v jq >/dev/null 2>&1; then
        echo "  skip  jq not available (the gate's own scan needs it)"
        return 0
    fi

    local tmp
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' RETURN

    # Render the two formula variables the gate uses. Everything else verbatim.
    python3 - "$FORMULA" >"$tmp/gate.sh" <<'PY' || fail "could not lift the gate recipe out of $FORMULA"
import re
import sys
import tomllib

with open(sys.argv[1], "rb") as handle:
    steps = tomllib.load(handle)["steps"]
step = [s for s in steps if s["id"] == "submit-and-exit"][0]["description"]
blocks = [b for b in re.findall(r"^```bash\n(.*?)^```$", step, re.S | re.M)
          if "git push origin HEAD" in b]
if len(blocks) != 1:
    raise SystemExit("expected exactly 1 gate block, got %d" % len(blocks))
sys.stdout.write(blocks[0].replace("{{base_branch}}", "main")
                          .replace("{{binding_prefix}}", "gastown."))
PY

    bash -n "$tmp/gate.sh" || fail "the rendered gate is not valid bash"

    # run_gate <label> <bead-json> -> the action log on stdout
    run_gate() {
        local d="$tmp/run.$1"
        rm -rf "$d"; mkdir -p "$d/bin"
        printf '%s' "$2" >"$d/bead.json"
        : >"$d/log"
        cat >"$d/bin/gc" <<'GCSTUB'
#!/usr/bin/env bash
if [ "$1" = "bd" ] && [ "$2" = "show" ]; then cat "$GATE_DIR/bead.json"; exit 0; fi
case "$1" in
  bd)      printf 'BD_UPDATE %s\n' "$*" >>"$GATE_DIR/log" ;;
  mail)    printf 'MAIL %s\n' "$*" >>"$GATE_DIR/log" ;;
  runtime) printf 'DRAIN\n' >>"$GATE_DIR/log" ;;
esac
exit 0
GCSTUB
        cat >"$d/bin/git" <<'GITSTUB'
#!/usr/bin/env bash
case "$1 $2" in
  "branch --show-current") echo "polecat/test-1"; exit 0 ;;
  "push origin")           printf 'PUSH\n' >>"$GATE_DIR/log"; exit 0 ;;
  "ls-remote origin")      echo "deadbeef	refs/heads/polecat/test-1"; exit 0 ;;
  "rev-parse HEAD")        echo "deadbeef"; exit 0 ;;
esac
exit 0
GITSTUB
        chmod +x "$d/bin/gc" "$d/bin/git"
        GATE_DIR="$d" PATH="$d/bin:$PATH" WORK_BEAD_ID=test-1 \
            CURRENT_BRANCH=polecat/test-1 GC_RIG=testrig \
            bash "$tmp/gate.sh" >"$d/out" 2>&1 || true
        cat "$d/log"
    }

    saw() {
        case "$2" in
            *"$3"*) : ;;
            *) fail "$1: expected '$3' in the action log: $2" ;;
        esac
    }
    never() {
        case "$2" in
            *"$3"*) fail "$1: '$3' happened and must not have: $2" ;;
            *) : ;;
        esac
    }

    local log

    # (a) explicit opt-out — halts without reading the prose.
    log=$(run_gate a '[{"metadata":{"auto_push":"false"},"description":"","notes":""}]')
    saw   "(a) auto_push=false" "$log" "halt_reason=auto_push_false"
    never "(a) auto_push=false" "$log" "PUSH"

    # (b) absent metadata + a rail in the NOTES. Notes, not description, because
    # that is the half an earlier scan was blind to.
    log=$(run_gate b '[{"metadata":{},"description":null,"notes":"PLAIN rail: branch + HALT branch-ready, mayor publishes, no push."}]')
    saw   "(b) absent + rail" "$log" "halt_reason=no_push_rail_unresolved"
    saw   "(b) absent + rail" "$log" "MAIL"
    never "(b) absent + rail" "$log" "PUSH"

    # (c) absent metadata, no rail — the normal path must still push, or the
    # gate has stopped being a gate and started being a wall.
    log=$(run_gate c '[{"metadata":{},"description":"Add a retry to the ingest job when the API 429s.","notes":""}]')
    saw   "(c) absent + no rail" "$log" "PUSH"
    never "(c) absent + no rail" "$log" "halt_reason"

    # (c2) the standing "never push to main" rule is not a rail. Half the beads
    # in the city quote it; if it halted them the gate would be routed around.
    log=$(run_gate c2 '[{"metadata":{},"description":"Polecats do not push to main, close beads, or wait around.","notes":""}]')
    saw   "(c2) push-to-main quote" "$log" "PUSH"
    never "(c2) push-to-main quote" "$log" "halt_reason"

    # (d) explicit consent beats prose — an operator who set auto_push=true has
    # already read the rail, and the gate must not second-guess them.
    log=$(run_gate d '[{"metadata":{"auto_push":"true"},"description":"no push — the mayor publishes","notes":""}]')
    saw   "(d) auto_push=true" "$log" "PUSH"
    never "(d) auto_push=true" "$log" "halt_reason"

    # ---- metadata SHAPE (gci-to0l) -----------------------------------------
    # The regression these arms exist for: jq's has() errors on a string, so the
    # probe reported "no auto_push key" and the EXPLICIT opt-out stopped firing.
    # Against the pre-fix probe, arms (e), (g3) and (g4) all log PUSH.

    # (e) string-shaped metadata carrying the explicit opt-out. Nothing in the
    # prose asserts a rail, so (b) cannot save it: the metadata is the ONLY
    # record of the decision, which is the shape beads are being asked to adopt.
    log=$(run_gate e '[{"metadata":"{\"auto_push\":\"false\"}","description":"Refactor the emissions-factor loader and add tests.","notes":""}]')
    saw   "(e) string metadata + auto_push=false" "$log" "halt_reason=auto_push_false"
    never "(e) string metadata + auto_push=false" "$log" "PUSH"

    # (e2) string-shaped metadata with NO auto_push and no rail. Decoding must
    # not turn into halting: this is the normal path wearing a different shape,
    # and a gate that halts it halts every bead on a string-serving ledger.
    log=$(run_gate e2 '[{"metadata":"{\"gc.work_branch\":\"main\"}","description":"Add a retry to the ingest job when the API 429s.","notes":""}]')
    saw   "(e2) string metadata, no rail" "$log" "PUSH"
    never "(e2) string metadata, no rail" "$log" "halt_reason"

    # (f) null metadata — the shape every measured ledger serves for a bead with
    # none, and the majority of beads. It is "nothing recorded", not
    # "unreadable", so it must fall through to the prose scan and push. This is
    # the arm that stops the fail-closed default being widened into a wall.
    log=$(run_gate f '[{"metadata":null,"description":"Add a retry to the ingest job when the API 429s.","notes":""}]')
    saw   "(f) null metadata, no rail" "$log" "PUSH"
    never "(f) null metadata, no rail" "$log" "halt_reason"

    # (f2) null metadata WITH a rail in prose still reaches the ambiguity halt —
    # decoding null as {} must not skip (b).
    log=$(run_gate f2 '[{"metadata":null,"description":"","notes":"PLAIN rail: branch + HALT branch-ready, mayor publishes, no push."}]')
    saw   "(f2) null metadata + rail" "$log" "halt_reason=no_push_rail_unresolved"
    never "(f2) null metadata + rail" "$log" "PUSH"

    # (g) a shape the probe cannot decode at all. No prose rail, so only the
    # fail-closed default stands between this and a push.
    log=$(run_gate g '[{"metadata":[1,2],"description":"Refactor the emissions-factor loader and add tests.","notes":""}]')
    saw   "(g) array metadata" "$log" "halt_reason=metadata_unreadable"
    saw   "(g) array metadata" "$log" "MAIL"
    never "(g) array metadata" "$log" "PUSH"

    # (g2) a string that is not JSON at all.
    log=$(run_gate g2 '[{"metadata":"auto_push=false","description":"Refactor the loader.","notes":""}]')
    saw   "(g2) unparseable string metadata" "$log" "halt_reason=metadata_unreadable"
    never "(g2) unparseable string metadata" "$log" "PUSH"

    # (g3) the read itself failed — the ledger returned an empty array. Before
    # this fix that pushed: no bead record meant no auto_push, an empty prose
    # scan meant no rail, and the gate fell straight through.
    log=$(run_gate g3 '[]')
    saw   "(g3) empty bead payload" "$log" "halt_reason=metadata_unreadable"
    never "(g3) empty bead payload" "$log" "PUSH"

    # (g4) the ledger read produced NO output — a dead ledger, not a bead
    # without metadata. jq prints nothing for empty input and exits 0, so this
    # one cannot be caught on jq's exit status; it needs the explicit -z guard.
    log=$(run_gate g4 '')
    saw   "(g4) no output from the ledger read" "$log" "halt_reason=metadata_unreadable"
    never "(g4) no output from the ledger read" "$log" "PUSH"
}

test_gate_structure
test_metadata_probe_shape
test_rail_detection
test_halt_notes_append
test_gate_execution

echo "polecat push gate tests passed"
