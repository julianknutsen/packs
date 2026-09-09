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
MAYOR_PROMPT="$ROOT/gastown/agents/mayor/prompt.template.md"

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

[[ -f "$FORMULA" ]] || fail "formula not found: $FORMULA"
[[ -f "$MAYOR_PROMPT" ]] || fail "mayor prompt not found: $MAYOR_PROMPT"

# --- 1. structure -----------------------------------------------------------

# Scoped to the submit-and-exit gate block, like every sibling property. This
# one used to grep the WHOLE FILE: it counted `gc mail send` across every step
# and required exactly 2, and took first-occurrence line numbers for the
# ordering. Both are green today only by accident of the file's contents — a
# legitimate mail send in another step, or a prose mention of `git push origin
# HEAD` above the real one, turns the suite red on correct code, which is the
# "a guard that is red on correct input gets deleted for being noisy" failure
# the notes check below warns about.
#
# The mail assertion is also per-halt now rather than a total. A total of 2 is
# satisfiable by the WRONG two: moving the escalation off the ambiguity halt and
# onto the operator's explicit opt-out keeps the count at 2 while the fail-closed
# halt stops escalating. The opt-out is a decision a human already made and is
# deliberately silent; only the two halts that mean "the gate could not tell"
# page anyone.
test_gate_structure() {
    python3 - "$FORMULA" <<'PY' || fail "the gate's halts, their escalations and their order are wrong"
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
body = "\n".join(l for l in blocks[0].split("\n") if not l.lstrip().startswith("#"))

HALTS = {
    "metadata_unreadable": ("the fail-closed unreadable-metadata halt (gci-to0l)", True),
    "auto_push_false": ("the explicit auto_push=false opt-out halt", False),
    "no_push_rail_unresolved": ("the fail-closed ambiguity halt", True),
}

problems, at = [], {}
for reason, (what, escalates) in HALTS.items():
    marker = "halt_reason=" + reason
    if body.count(marker) != 1:
        problems.append("%s: expected exactly 1 %s, found %d"
                        % (what, marker, body.count(marker)))
        continue
    at[reason] = body.index(marker)
    # The halt's OWN region: its metadata write through to the exit that ends
    # it. Fail closed if that terminator is missing — an unbounded slice would
    # run to the end of the gate and borrow a neighbouring halt's escalation.
    end = re.compile(r"^[ \t]*exit 0[ \t]*$", re.M).search(body, at[reason])
    if end is None:
        problems.append("%s: no `exit 0` terminates it, so its region cannot be read"
                        % what)
        continue
    sends = body[at[reason]:end.start()].count("gc mail send")
    if escalates and sends != 1:
        problems.append("%s must escalate exactly once; its region has %d `gc mail send`"
                        % (what, sends))
    if not escalates and sends != 0:
        problems.append("%s is a decision a human already made and must not page anyone; "
                        "its region has %d `gc mail send`" % (what, sends))

# A halt that lands after the push is worthless, and the unreadable halt must
# also precede the opt-out TEST: AUTO_PUSH is meaningless when the probe that
# produced it failed, so reading it first is reading a guess.
if len(at) == len(HALTS):
    push = body.index("git push origin HEAD")
    for earlier, later, why in (
        ("metadata_unreadable", "auto_push_false",
         "the unreadable-metadata halt must come before the auto_push test"),
        ("auto_push_false", "no_push_rail_unresolved",
         "the explicit opt-out must be tested before the prose scan"),
    ):
        if at[earlier] >= at[later]:
            problems.append(why)
    for reason in ("metadata_unreadable", "no_push_rail_unresolved"):
        if at[reason] >= push:
            problems.append("%s comes after git push origin HEAD" % reason)

for p in problems:
    print("  " + p, file=sys.stderr)
raise SystemExit(1 if problems else 0)
PY
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
# Four halts write halt_reason in this step: the push gate's three
# (metadata_unreadable, auto_push_false, no_push_rail_unresolved) plus the
# branch-content gate's single write of "$HALT_REASON", which carries either
# no_commits or content_gate_error. The count is pinned, not floored, so a
# fifth unaccounted halt -- or a deleted one -- still reports here.
if body.count("halt_reason=") != 4:
    problems.append("expected exactly four halt_reason writes, found %d" % body.count("halt_reason="))
if "ascii_downcase" not in body:
    # `False` and `no` are hand-written by humans following the mayor prompt;
    # reading them as "any other value -> explicit consent" fails open on the
    # beads that tried hardest to say no.
    problems.append("the auto_push vocabulary is not case-folded")
if 'error("auto_push value is outside the vocabulary")' not in body:
    # An unrecognised value routes into the SAME unreadable halt rather than a
    # fourth one: a decision the gate cannot understand is not consent.
    problems.append("an auto_push value outside the vocabulary must not be read as consent")

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

# Lift the shipped strip/match pair straight out of the gate so this test can
# never drift from the code it is testing. The lift is ANCHORED: tomllib parses
# the formula, the submit-and-exit bash block containing `git push origin HEAD`
# is selected, comments are dropped, and the pattern must occur exactly ONCE in
# what remains. A position-based lift over the raw file (the first `grep -E..`
# anywhere in it) starts testing some other step's regex the day one is added
# above the gate, and stays green while doing it.
#
# The extraction is also non-fatal on purpose. Under `set -euo pipefail` an
# empty result aborted this whole suite at the assignment — before the guards
# below could run — so a lift that stopped matching printed nothing at all and
# read as a crash rather than as "the pattern moved". `|| true` inside the
# helper keeps the guards reachable so they can name what went missing.
lift_gate_pattern() {
    # stderr is left connected: the reason ("got 2 matches", "no gate block")
    # is the useful half of the failure, and stdout stays clean either way.
    python3 - "$FORMULA" "$1" <<'PY' || true
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
body = "\n".join(l for l in blocks[0].split("\n") if not l.lstrip().startswith("#"))
hits = re.findall(sys.argv[2], body)
if len(hits) != 1:
    raise SystemExit("expected exactly 1 match for %s, got %d" % (sys.argv[2], len(hits)))
sys.stdout.write(hits[0])
PY
}

STRIP_SED=$(lift_gate_pattern "sed -E '(s#[^#]*#[^#]*#g)'")
MATCH_RE=$(lift_gate_pattern "grep -Eic '([^']+)'")

[[ -n "$STRIP_SED" ]] ||
    fail "could not lift the push-to-main strip out of the submit-and-exit gate block in $FORMULA"
[[ -n "$MATCH_RE" ]] ||
    fail "could not lift the rail match pattern out of the submit-and-exit gate block in $FORMULA"

# The gate's own three scan stages, in the gate's order.
rail_hits() {
    printf '%s\n' "$1" | tr '[:upper:]' '[:lower:]' | sed -E "$STRIP_SED" | grep -Eic "$MATCH_RE" || true
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

    # The 7 wordings the line-dropping exclusion swallowed. Measured 2026-09-08
    # against the shipped patterns: every one scored ZERO hits, so the gate
    # pushed a bead whose prose forbids it. Rows 1-5 are the bare `origin`
    # alternative — origin is the remote the gate is about to push to, so
    # "do not push to origin" is a full rail, not the standing rule.
    assert_rail halt 'never push to origin — the customer tenant is frozen'
    assert_rail halt 'Do not push to origin; wait for the operator to take it.'
    assert_rail halt 'Do not push to origin; the mayor publishes.'
    assert_rail halt 'never push to origin/customer-prod, it is frozen'
    assert_rail halt 'do not push to origin until legal signs off'
    # Rows 6-7 are why narrowing the destination is not enough on its own: the
    # rail and the standing-rule quote share ONE line, so the line has to
    # survive with the quote cut out of it. A narrowing-only patch leaves these
    # two pushing.
    assert_rail halt 'Do not push to main - in fact do not push at all, the operator publishes.'
    assert_rail halt 'Polecats do not push to main. HALT branch-ready for this bead.'

    # The stripped destination must end on a word boundary, or the origin/main
    # prefix eats the rail off a branch whose name merely starts with it.
    assert_rail halt 'never push to origin/main-line-customer, frozen'
    assert_rail halt 'do not push to master_frozen, ever'
    # `/` is in that class too: these name branches UNDER the stripped
    # namespaces, so without it the strip removes the verb and the gate pushes.
    # Measured at 0 hits before `/` was protected.
    assert_rail halt 'never push to main/customer, the tenant is frozen'
    assert_rail halt 'do not push to origin/main/customer-prod, frozen'

    # The prose arms must cover the words the metadata vocabulary blesses, or a
    # rail is enforced in one channel and fail-open in the other. All four
    # scored ZERO hits before, measured 2026-09-09 against the shipped patterns.
    assert_rail halt 'You must not push this branch until the audit clears.'
    assert_rail halt 'You should not push this branch until the audit clears.'
    assert_rail halt 'Rails: auto_push=no, base=main.'
    assert_rail halt 'Rails: auto_push=0, base=main.'
    # ...and the standing-rule quote survives the new verbs, because the strip
    # runs first. These are the rows that would redden if the wider alternation
    # were bought by dropping the strip.
    assert_rail push 'Polecats must not push to main; open a PR.'
    assert_rail push 'Contributors should not push to master; open a PR.'
    # The consent half of the vocabulary must not be read as a rail by the
    # value arm's alternation.
    assert_rail push 'Rails: auto_push=true, base=main.'
    assert_rail push 'Rails: auto_push=1, base=main.'
    # Same past-tense guard as the `never pushes` row above. A modal cannot take
    # the -es/-ed form, so this shape is ungrammatical rather than idiomatic; the
    # guard is carried for consistency across the three verb arms and this row
    # pins it so a later edit cannot quietly drop it from one arm only.
    assert_rail push 'Establish why mirror-sync should not pushes twice.'

    # The strip is case-insensitive because the scan is downcased first, NOT
    # because sed was asked to be (its I flag is GNU-only, and this gate runs
    # wherever a polecat runs). If the downcase went away these two would halt
    # on the standing rule — cry wolf — while the halt row below stayed green,
    # so all three are needed to pin the direction.
    assert_rail push 'Do Not Push To Main; open a PR.'
    assert_rail push 'NEVER PUSH DIRECTLY TO MAIN; open a PR.'
    assert_rail halt 'Do Not Push; the Mayor Publishes.'

    # KNOWN RESIDUAL, deliberately not asserted either way. Two shapes, one
    # mechanism — the regex cannot read a sentence — recorded here so the next
    # reader does not have to rediscover them; auto_push=false ends both.
    #
    #   1. A rail whose only signal sits INSIDE the stripped phrase still reads
    #      as the standing rule: "never push to main or anywhere else" scores
    #      zero, and so does the dotted form "never push to main.customer, it is
    #      frozen" — `.` cannot join `-_/` in the protected class without
    #      reddening "Polecats never push to main. Open a PR." Separating these
    #      from "polecats do not push to main" needs sentence semantics, and the
    #      cheap cure (keep the line whenever the strip removes every signal)
    #      halts every bead quoting the standing rule, which is the failure the
    #      strip exists to prevent. Left as a push.
    #   2. The converse, opened by the wider verb alternation: a NORMATIVE
    #      sentence about another system — "Establish why the sync should not
    #      push twice" — halts, where the past-tense report ("...never pushes")
    #      does not. Left as a halt: a spurious halt costs a human round trip,
    #      a missed rail costs a push to a customer estate.
}

# --- 2b. the scan's shape: strip the phrase, and fail closed ----------------

# Two properties the row table cannot see, because it runs the lifted patterns
# rather than the pipeline they sit in:
#   1. the line-dropping `grep -Eiv` must not come back. It is what made the 7
#      wordings above score zero; re-adding it BESIDE the strip would leave
#      every row here green while the gate went back to pushing them.
#   2. the prose must be extracted in CHECKED stages. `grep -c` prints a count
#      whenever it runs, so a stage that dies inside one long pipe reaches the
#      match grep as an empty stream, scores 0, and pushes — which made the
#      fail-closed claim true only for a missing grep binary.
# Asserted over EXECUTABLE lines, like the notes and probe checks: the step
# describes the old shape in its prose on purpose.
test_rail_scan_shape() {
    python3 - "$FORMULA" <<'PY' || fail "the rail scan must strip the push-to-main phrase and fail closed"
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
body = "\n".join(l for l in blocks[0].split("\n") if not l.lstrip().startswith("#"))

problems = []
if "grep -Eiv" in body:
    problems.append("the line-dropping exclusion is back: strip the push-to-main PHRASE, not the line")

strip = re.findall(r"sed -E '(s#[^#]*#[^#]*#g)'", body)
if len(strip) != 1:
    problems.append("expected exactly one push-to-main strip, found %d" % len(strip))
else:
    if "(origin/)?" not in strip[0]:
        # Bare `origin` is the remote the gate is about to push to, so
        # "do not push to origin" is a rail, not the push-to-main rule.
        problems.append("the strip does not narrow `origin` to a branch under it")
    if "([^-[:alnum:]_/]|$)" not in strip[0]:
        # `/` belongs in the protected class with `-` and `_`: main/customer is
        # a branch UNDER main, so stripping the phrase there eats a real rail's
        # verb. The row table pins the behaviour; this pins the class itself so
        # a narrowing edit cannot silently drop one character of it.
        problems.append("the strip has no trailing word boundary over -_/: "
                        "origin/main-line-customer and main/customer lose their "
                        "rail to the origin/main prefix")
if "tr '[:upper:]' '[:lower:]'" not in body:
    problems.append("the scan is not downcased, so the strip misses 'Do Not Push To Main' "
                    "(sed's case-insensitive flag is GNU-only and this gate is not)")

# The scan runs between the "no metadata decision" test and the halt test.
scan = body.split('if [ -z "$AUTO_PUSH" ]; then', 1)[-1].split('if [ "${RAIL_HIT:-1}"', 1)[0]
guarded = [l.strip() for l in scan.split("\n") if l.strip().startswith(("if ! ", "elif ! "))]
for stage, what in (("jq -r", "the prose extraction"),
                    ("tr '[:upper:]'", "the downcase"),
                    ("sed -E", "the strip")):
    if not any(stage in g for g in guarded):
        problems.append("%s is not a checked stage: a mid-pipe failure there would "
                        "score 0 hits and push" % what)
if scan.count("RAIL_HIT=1") != len(guarded) or not guarded:
    problems.append("every checked stage must assert the rail on failure: %d guards, "
                    "%d RAIL_HIT=1" % (len(guarded), scan.count("RAIL_HIT=1")))
if '"${RAIL_HIT:-1}" -gt 0' not in body:
    problems.append("the empty-result default is gone: a grep that never ran must halt")

for p in problems:
    print("  " + p, file=sys.stderr)
raise SystemExit(1 if problems else 0)
PY
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

    local tmp REAL_JQ
    tmp=$(mktemp -d)
    REAL_JQ=$(command -v jq)
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

    # run_gate <label> <bead-json> [jq-calls-that-succeed] -> action log on stdout
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
        # Optional fault injection: let the first N jq calls through, then fail
        # them. The metadata probe is call 1; the prose extraction is call 2.
        if [[ -n "${3:-}" ]]; then
            cat >"$d/bin/jq" <<'JQSTUB'
#!/usr/bin/env bash
n=$(( $(cat "$GATE_DIR/jqcalls" 2>/dev/null || echo 0) + 1 ))
printf '%s' "$n" >"$GATE_DIR/jqcalls"
if [ "$n" -gt "$JQ_OK_CALLS" ]; then
  echo "jq: simulated mid-run failure" >&2
  exit 5
fi
exec "$REAL_JQ" "$@"
JQSTUB
            chmod +x "$d/bin/jq"
        fi
        GATE_DIR="$d" PATH="$d/bin:$PATH" WORK_BEAD_ID=test-1 \
            CURRENT_BRANCH=polecat/test-1 GC_RIG=testrig \
            JQ_OK_CALLS="${3:-}" REAL_JQ="$REAL_JQ" \
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

    # ---- the auto_push VOCABULARY is closed and case-folded -----------------
    # `auto_push` is hand-written by humans following the mayor prompt, so the
    # near misses are the interesting values. Under "any other value -> push,
    # explicit consent" every arm below except (i4)/(i6) pushed — a bead
    # carrying BOTH `auto_push=False` and a written rail pushed, because the
    # opt-out row short-circuits the prose scan.

    # (i) the opt-out in the wrong case, with a rail in the prose behind it.
    log=$(run_gate i '[{"metadata":{"auto_push":"False"},"description":"PLAIN rail: branch + HALT branch-ready, mayor publishes, no push.","notes":""}]')
    saw   "(i) auto_push=False" "$log" "halt_reason=auto_push_false"
    never "(i) auto_push=False" "$log" "PUSH"

    # (i2) the other spellings of no.
    log=$(run_gate i2 '[{"metadata":{"auto_push":"no"},"description":"Refactor the loader.","notes":""}]')
    saw   "(i2) auto_push=no" "$log" "halt_reason=auto_push_false"
    never "(i2) auto_push=no" "$log" "PUSH"
    log=$(run_gate i3 '[{"metadata":{"auto_push":false},"description":"Refactor the loader.","notes":""}]')
    saw   "(i3) auto_push JSON false" "$log" "halt_reason=auto_push_false"
    never "(i3) auto_push JSON false" "$log" "PUSH"

    # (i4) consent, spelled the ways a human spells it.
    log=$(run_gate i4 '[{"metadata":{"auto_push":"YES"},"description":"no push — the mayor publishes","notes":""}]')
    saw   "(i4) auto_push=YES" "$log" "PUSH"
    never "(i4) auto_push=YES" "$log" "halt_reason"

    # (i5) a key present with a null value is NOTHING RECORDED, not consent and
    # not unreadable: JSON's own spelling of "no value". It must fall through to
    # the prose scan — here to a rail, so the ambiguity halt.
    log=$(run_gate i5 '[{"metadata":{"auto_push":null},"description":"","notes":"PLAIN rail: branch + HALT branch-ready, mayor publishes, no push."}]')
    saw   "(i5) auto_push null + rail" "$log" "halt_reason=no_push_rail_unresolved"
    never "(i5) auto_push null + rail" "$log" "PUSH"

    # (i6) ... and with no rail it is the normal path, or a null-valued key
    # would become a wall.
    log=$(run_gate i6 '[{"metadata":{"auto_push":null},"description":"Add a retry to the ingest job when the API 429s.","notes":""}]')
    saw   "(i6) auto_push null, no rail" "$log" "PUSH"
    never "(i6) auto_push null, no rail" "$log" "halt_reason"

    # (i7) a value nobody can interpret is a decision the gate failed to read,
    # so it lands in the unreadable halt rather than being waved through as
    # "explicit consent". Note the prose here asserts nothing: the metadata is
    # the only record, which is the shape beads are being asked to adopt.
    log=$(run_gate i7 '[{"metadata":{"auto_push":"maybe"},"description":"Refactor the loader.","notes":""}]')
    saw   "(i7) auto_push=maybe" "$log" "halt_reason=metadata_unreadable"
    saw   "(i7) auto_push=maybe" "$log" "MAIL"
    never "(i7) auto_push=maybe" "$log" "PUSH"

    # ---- the scan's own failure is a rail ----------------------------------

    # (h) the PROSE extraction breaks after the probe has already succeeded.
    # This is the bead from arm (c), which pushes; the only difference is the
    # dead stage. `grep -c` prints a count whenever it runs, so before the scan
    # was split into checked stages a mid-pipe death arrived at the match grep
    # as an empty stream, counted 0 rails, and PUSHED — the fail-closed comment
    # held only for a grep binary that was missing outright.
    log=$(run_gate h '[{"metadata":{},"description":"Add a retry to the ingest job when the API 429s.","notes":""}]' 1)
    saw   "(h) prose extraction failed" "$log" "halt_reason=no_push_rail_unresolved"
    never "(h) prose extraction failed" "$log" "PUSH"

    # (h2) the same injection with the fault set BEYOND the scan: the stub is
    # in $PATH and counting for both arms, so (h)'s halt is the failure talking
    # and not the stub's mere presence.
    log=$(run_gate h2 '[{"metadata":{},"description":"Add a retry to the ingest job when the API 429s.","notes":""}]' 9)
    saw   "(h2) jq stub, no fault" "$log" "PUSH"
    never "(h2) jq stub, no fault" "$log" "halt_reason"
}

# --- 5. the mayor's read-back recipe, executed ------------------------------

# The mayor prompt tells an operator how to read `auto_push` back after writing
# it, and that recipe is a copy of the gate's probe kept in prose. It drifted
# once already — the same divergence was raised, patched in prose, and raised
# again next iteration — because nothing executed it. So this executes the
# SHIPPED text: the block is lifted out of the prompt with exactly one edit (the
# ledger read becomes a fixture read) and its answers are pinned.
#
# The three answers mirror the probe's three outcomes: a recorded value, `absent`
# (nothing recorded, so the bead falls through to the prose scan), and
# `unreadable` (the read itself failed, which the gate turns into the
# metadata_unreadable halt). The third is the one worth pinning: a failed read
# reported as a settled answer invites re-writing a bead whose state was never
# actually read.
test_mayor_readback_mirrors_probe() {
    if ! command -v jq >/dev/null 2>&1; then
        echo "  skip  jq not available (the recipe under test is a jq pipeline)"
        return 0
    fi

    local tmp
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' RETURN

    python3 - "$MAYOR_PROMPT" >"$tmp/readback.sh" <<'PY' || fail "could not lift the read-back recipe out of the mayor prompt"
import re
import sys

doc = open(sys.argv[1], encoding="utf-8").read()
blocks = [b for b in re.findall(r"^```bash\n(.*?)^```$", doc, re.S | re.M)
          if "WORK_JSON" in b and "auto_push" in b]
if len(blocks) != 1:
    raise SystemExit("expected exactly 1 read-back block, got %d" % len(blocks))
body = blocks[0]

# The ONLY edit. If the prompt stops spelling the read exactly this way the lift
# has to fail loudly rather than substitute nothing: an unsubstituted block would
# run the operator's real `gc bd show` from the test and pass or fail on whatever
# the live ledger happened to answer.
READ = "gc bd show <id> --json"
if body.count(READ) != 1:
    raise SystemExit("expected exactly 1 `%s` in the recipe, got %d"
                     % (READ, body.count(READ)))
sys.stdout.write(body.replace(READ, 'cat "$FIXTURE"'))
PY

    bash -n "$tmp/readback.sh" || fail "the lifted read-back recipe is not valid bash"

    # answers <label> <expected> <payload>
    answers() {
        local label="$1" want="$2" payload="$3" got
        printf '%s' "$payload" >"$tmp/fixture.json"
        got=$(FIXTURE="$tmp/fixture.json" bash "$tmp/readback.sh" 2>/dev/null)
        [[ "$got" == "$want" ]] \
            || fail "mayor read-back ($label): expected '$want', got '$got'"
    }

    # A recorded decision reads back as itself, through every metadata shape the
    # ledgers actually serve. The string-metadata row is the shape that made the
    # pre-fix one-liner exit 5 printing nothing.
    answers "object metadata, false" false '[{"metadata":{"auto_push":false}}]'
    answers "object metadata, true"  true  '[{"metadata":{"auto_push":true}}]'
    answers "string metadata"        false '[{"metadata":"{\"auto_push\":false}"}]'
    answers "string value"           no    '[{"metadata":{"auto_push":"no"}}]'

    # `absent` is "nothing recorded", which is not a halt: it falls through to
    # the prose scan. A null VALUE is JSON's own spelling of nothing recorded, so
    # it belongs here and not in the answers above — printing the bare word
    # `null` would read as a decision the operator never made.
    answers "no auto_push key" absent '[{"metadata":{}}]'
    answers "null metadata"    absent '[{"metadata":null}]'
    answers "null value"       absent '[{"metadata":{"auto_push":null}}]'

    # `unreadable` is the read failing, which the gate answers with the
    # metadata_unreadable halt. Each row below is permissive-by-default without
    # its guard: jq prints nothing and exits 0 on empty input, and `[] | .[0]`
    # is null, so an unguarded pipe answers a dead ledger and a bead-less payload
    # with silence and with `absent` respectively.
    answers "empty payload"                unreadable ''
    answers "no bead record"               unreadable '[]'
    answers "payload is not an array"      unreadable '{"metadata":{"auto_push":false}}'
    answers "bead record is not an object" unreadable '[7]'
    answers "undecodable string metadata"  unreadable '[{"metadata":"not json at all"}]'
    answers "metadata is a scalar"         unreadable '[{"metadata":7}]'
}

test_gate_structure
test_metadata_probe_shape
test_rail_detection
test_rail_scan_shape
test_halt_notes_append
test_gate_execution
test_mayor_readback_mirrors_probe

echo "polecat push gate tests passed"
