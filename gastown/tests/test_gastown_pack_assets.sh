#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
GASTOWN="$ROOT/gastown"

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

parse_toml() {
    python3 - "$@" <<'PY'
import sys
import tomllib

for path in sys.argv[1:]:
    with open(path, "rb") as handle:
        tomllib.load(handle)
PY
}

test_dog_assets_are_pack_local() {
    [[ -f "$GASTOWN/agents/dog/agent.toml" ]] || fail "missing dog agent config"
    [[ -f "$GASTOWN/agents/dog/prompt.template.md" ]] || fail "missing dog prompt"
    [[ -f "$GASTOWN/formulas/mol-shutdown-dance.toml" ]] || fail "missing shutdown dance formula"
    parse_toml "$GASTOWN/agents/dog/agent.toml" "$GASTOWN/formulas/mol-shutdown-dance.toml"
    grep -F 'wake_mode = "fresh"' "$GASTOWN/agents/dog/agent.toml" >/dev/null ||
        fail "dog agent should own wake_mode"
    grep -F 'work_dir = ".gc/agents/dogs/{{.AgentBase}}"' "$GASTOWN/agents/dog/agent.toml" >/dev/null ||
        fail "dog agent should own work_dir"
    ! grep -F 'fallback = true' "$GASTOWN/agents/dog/agent.toml" >/dev/null ||
        fail "gastown dog should be authoritative over fallback dog providers"
    ! grep -A3 -F '[[patches.agent]]' "$GASTOWN/pack.toml" | grep -F 'name = "dog"' >/dev/null ||
        fail "dog should not be split between pack-local agent and same-name patch"
    [[ ! -e "$GASTOWN/agents/dog/overlay/.gitkeep" ]] ||
        fail "dog overlay placeholder should not be present without an overlay contract"
}

test_retired_dog_formulas_are_not_reintroduced() {
    [[ ! -e "$GASTOWN/formulas/mol-dog-jsonl.toml" ]] || fail "mol-dog-jsonl formula should remain retired"
    [[ ! -e "$GASTOWN/formulas/mol-dog-reaper.toml" ]] || fail "mol-dog-reaper formula should remain retired"
    ! grep -R --exclude='test_gastown_pack_assets.sh' "mol-dog-jsonl\\|mol-dog-reaper" "$GASTOWN" >/dev/null ||
        fail "gastown pack should not advertise retired dog formulas"
}

test_shutdown_dance_contracts_are_executable() {
    local formula="$GASTOWN/formulas/mol-shutdown-dance.toml"

    ! grep -F '[vars.warrant_id]' "$formula" >/dev/null ||
        fail "warrant_id should be the claimed work bead, not a required formula var"
    grep -F 'gc bd show "$GC_BEAD_ID"' "$formula" >/dev/null ||
        fail "shutdown dance should inspect the claimed warrant bead"
    grep -F 'gc bd close "$GC_BEAD_ID"' "$formula" >/dev/null ||
        fail "shutdown dance should close the claimed warrant bead"
    ! grep -F '<wisp-id>' "$formula" >/dev/null ||
        fail "shutdown dance should not contain raw wisp placeholders"
    ! grep -F '<work-bead>' "$formula" >/dev/null ||
        fail "shutdown dance should not contain raw work bead placeholders"
    ! grep -F 'gc mail send {{requester}}/' "$formula" >/dev/null ||
        fail "routine dog requester reporting must use nudge, not mail"
    grep -F 'requester_endpoint="${requester%/}/"' "$formula" >/dev/null ||
        fail "shutdown dance should normalize requester endpoints"
    grep -F 'gc session nudge "$requester_endpoint" "DOG_DONE:' "$formula" >/dev/null ||
        fail "shutdown dance should notify requester with DOG_DONE nudges"
    ! grep -F 'gc session peek "{{target}}"' "$formula" >/dev/null ||
        fail "shutdown dance should use quoted target shell variables for peeks"
    ! grep -F 'gc session kill "{{target}}"' "$formula" >/dev/null ||
        fail "shutdown dance should use quoted target shell variables for kills"
    grep -F 'Verify the warrant bead exists and is not closed' "$formula" >/dev/null ||
        fail "receive step should verify the warrant is not closed rather than demanding open"
    grep -F 'Both `open` and `in_progress` are valid warrant states' "$formula" >/dev/null ||
        fail "receive step should explicitly accept open and in_progress warrant states"
    ! grep -F 'exists and is open' "$formula" >/dev/null ||
        fail "receive step must not regress to an open-only warrant instruction; claimed warrants are in_progress"
}

test_shutdown_dance_lifecycle_and_audit_contracts() {
    local formula="$GASTOWN/formulas/mol-shutdown-dance.toml"
    local prompt="$GASTOWN/agents/dog/prompt.template.md"

    ! grep -Fi 'burn' "$formula" >/dev/null ||
        fail "early-exit paths should drain-ack and exit, not burn a wisp that was never poured"
    [[ "$(grep -c 'gc runtime drain-ack' "$formula")" -ge 8 ]] ||
        fail "every early-exit path and the epitaph should end with gc runtime drain-ack"
    local malformed_branches malformed_closes malformed_drains
    malformed_branches="$(grep -c 'is missing target or reason' "$formula" || true)"
    malformed_closes="$(grep -A4 'is missing target or reason' "$formula" | grep -cF 'gc bd close "$GC_BEAD_ID"' || true)"
    malformed_drains="$(grep -A4 'is missing target or reason' "$formula" | grep -cF 'gc runtime drain-ack' || true)"
    [[ "$malformed_branches" -ge 1 ]] ||
        fail "shutdown dance should validate warrant target/reason metadata"
    [[ "$malformed_closes" -eq "$malformed_branches" ]] ||
        fail "every malformed-warrant branch must close the claimed warrant before exiting"
    [[ "$malformed_drains" -eq "$malformed_branches" ]] ||
        fail "every malformed-warrant branch must drain-ack before exiting, not leak the claimed warrant"
    grep -F 'MALFORMED_WARRANT' "$formula" >/dev/null ||
        fail "malformed warrants should close with a malformed-warrant audit reason"
    ! grep -E '^\[vars' "$formula" >/dev/null ||
        fail "warrant values come from bead metadata; the formula should not declare pour vars"
    grep -F 'EXECUTE_FAILED: kill did not take effect' "$formula" >/dev/null ||
        fail "kill failures should close the warrant as EXECUTE_FAILED, not Executed"
    grep -F 'DOG_DONE: $target - EXECUTE_FAILED (escalated)' "$formula" >/dev/null ||
        fail "kill failures should notify the requester with EXECUTE_FAILED, not EXECUTED"
    grep -F 'gone or shows fresh startup output' "$formula" >/dev/null ||
        fail "execute verification should treat gone-or-freshly-restarted as kill success"
    ! grep -F '{{requester}}' "$prompt" >/dev/null ||
        fail "dog prompt should use the normalized requester endpoint, not raw requester templates"
    ! grep -F 'nudge deacon/' "$prompt" >/dev/null ||
        fail "dog prompt should notify the warrant's requester, not a hardcoded deacon endpoint"
    grep -F 'gc session nudge "$requester_endpoint"' "$prompt" >/dev/null ||
        fail "dog prompt DOG_DONE guidance should use the normalized requester endpoint"
}

# Five role-surfaces resolve a work bead from the environment, by deliberately
# different rules, and the differences are load-bearing. Two properties decide
# which form is safe -- NOT "pooled vs singleton": the refinery and the deacon
# carry identical max_active_sessions = 1 + wake_mode = "fresh" pins, so pooling
# cannot tell them apart. What differs is (a) whether more than one session
# shares the env, and (b) whether the role rotates wisps IN-SESSION, which makes
# a spawn-fixed trigger go stale mid-loop:
#   shutdown dance (dog pool, max_active_sessions = 3) - resolve the CLAIM
#     first. The spawn trigger is fixed at wake and is not advanced by
#     `gc hook --claim`, so under contention it names a bead this session never
#     claimed - and the dance closes whatever it resolves.
#   deacon patrol formula (singleton, wake_mode = "fresh", pours the next wisp
#     then EXITS the turn) - may prefer the trigger: one process env holds
#     exactly one wisp for its whole life.
#   refinery patrol formula (singleton too, but re-reads the formula steps
#     in-session after burning) - bare ${GC_BEAD_ID:-} plus a live assignee
#     query, never the trigger: the wisp advances while the trigger does not.
#   refinery / witness / deacon prompt templates - bare ${GC_BEAD_ID:-} plus the
#     same live query. Exempt from the trigger rule by form, not by luck: a bare
#     resolution backed by the assignee query cannot mis-select on any role, so
#     it stays correct even on the singletons. Pinned below so the exemption is
#     gated rather than conventional.
# Pin the discriminator, and the rotation property it rests on, so the forms
# cannot silently converge on the permissive one.
test_work_bead_resolution_discriminator_is_pinned() {
    local dance="$GASTOWN/formulas/mol-shutdown-dance.toml"
    local deacon="$GASTOWN/formulas/mol-deacon-patrol.toml"
    local refinery="$GASTOWN/formulas/mol-refinery-patrol.toml"
    local refinery_prompt="$GASTOWN/agents/refinery/prompt.template.md"

    local claim_first='GC_BEAD_ID="${GC_BEAD_ID:-$(gc hook current --id-only 2>/dev/null)}"'
    local trigger_fallback='GC_BEAD_ID="${GC_BEAD_ID:-${GC_TRIGGER_WORK_BEAD_ID:?no work bead id in env}}"'

    # Order-sensitive by construction: presence greps alone would stay green on
    # a reordering. Record both forms in file order and require claim-then-
    # trigger at exactly the two normalization sites the formula prose names.
    local dance_order
    dance_order="$(awk -v claim="$claim_first" -v trig="$trigger_fallback" '
        index($0, claim) { printf "C"; next }
        index($0, trig)  { printf "T" }
    ' "$dance")"
    [[ "$dance_order" == "CTCT" ]] ||
        fail "shutdown dance must resolve the claimed warrant before the spawn trigger at exactly the two normalization sites (preamble and receive-warrant step 1); got '$dance_order'"

    # Nesting the resolver inside the trigger fallback is inert: ${A:-${B:-$(C)}}
    # never evaluates $(C) while B is non-empty, so a stale trigger still wins
    # and the dance closes a foreign bead.
    ! grep -F 'GC_TRIGGER_WORK_BEAD_ID:-$(gc hook current' "$dance" >/dev/null ||
        fail "shutdown dance must not nest the claim resolver inside the trigger fallback; that form is inert whenever the trigger is set"

    # The signature above matches two exact literals, so it is blind to a
    # NOVEL-form trigger resolution added to a third block (a bare
    # ${GC_TRIGGER_WORK_BEAD_ID:-} matches neither literal and leaves the
    # signature at CTCT). Bound the references instead of only their shape, so a
    # new one has to be added consciously rather than silently.
    local dance_trigger_refs
    dance_trigger_refs="$(grep -cF 'GC_TRIGGER_WORK_BEAD_ID' "$dance" || true)"
    [[ "$dance_trigger_refs" -eq 3 ]] ||
        fail "shutdown dance must carry exactly 3 GC_TRIGGER_WORK_BEAD_ID references (the prose paragraph plus the two fallback lines); got $dance_trigger_refs. A new one is a new resolution site: update the prose contract and this pin together."

    local deacon_trigger
    deacon_trigger="$(grep -cF 'CURRENT_WISP=${GC_BEAD_ID:-${GC_TRIGGER_WORK_BEAD_ID:-}}' "$deacon" || true)"
    [[ "$deacon_trigger" -eq 1 ]] ||
        fail "deacon patrol should keep exactly one trigger-preferring wisp resolution; got $deacon_trigger"
    grep -F 'max_active_sessions = 1' "$GASTOWN/agents/deacon/agent.toml" >/dev/null ||
        fail "the deacon's trigger-first wisp resolution is safe only while the deacon is a singleton; agent.toml no longer pins max_active_sessions = 1"
    grep -F 'wake_mode = "fresh"' "$GASTOWN/agents/deacon/agent.toml" >/dev/null ||
        fail "the deacon's trigger-first wisp resolution is safe only on a fresh wake; agent.toml no longer pins wake_mode"

    # The third precondition lives in the formula, not in agent.toml, so neither
    # pin above can ever catch its loss: the deacon pours the successor, burns
    # this wisp, and EXITS the turn, so one process env holds exactly one wisp
    # for its whole life and the restarted session gets a fresh trigger. An
    # in-session loop here revives the stale-trigger failure with both config
    # pins still green.
    grep -F 'IDLE: no work, exiting turn.' "$deacon" >/dev/null ||
        fail "the deacon's trigger-first wisp resolution is safe only while each iteration ends by exiting the turn; mol-deacon-patrol.toml no longer emits the IDLE exit signal"
    grep -F 'the restarted session resumes from it' "$deacon" >/dev/null ||
        fail "the deacon's trigger-first wisp resolution is safe only while the successor wisp is resumed by a RESTARTED session (fresh trigger); mol-deacon-patrol.toml no longer hands the successor to a restarted session"
    ! grep -F 're-read formula steps to begin' "$deacon" >/dev/null ||
        fail "the deacon now rotates wisps in-session like the refinery, so its spawn trigger goes stale mid-loop; mol-deacon-patrol.toml must drop the trigger-preferring resolution for the bare \${GC_BEAD_ID:-} plus live assignee query"

    # The refinery's opposite rule rests on the opposite property, so pin that
    # too rather than leaving it asserted only in a comment: it re-reads the
    # formula steps in-session after burning, advancing the wisp while the
    # spawn-fixed trigger stays put.
    grep -F 're-read formula steps to begin' "$refinery" >/dev/null ||
        fail "the refinery no longer rotates wisps in-session; that rotation is the recorded reason it must never resolve a wisp from the spawn trigger, so re-derive the per-role rule and this discriminator before relaxing either form"

    # Every environment-resolved wisp assignment in the refinery must be the
    # bare query-backed form. Comparing the two counts catches conversion in
    # either direction without freezing the number of call sites.
    local path env_assignments bare_assignments
    for path in "$refinery" "$refinery_prompt"; do
        ! grep -F 'GC_TRIGGER_WORK_BEAD_ID' "$path" >/dev/null ||
            fail "the refinery rotates wisps in-session, so a spawn-fixed trigger goes stale mid-loop; it must resolve every wisp from \$GC_BEAD_ID plus the live assignee query, never the trigger: $path"
        env_assignments="$(grep -cF 'CURRENT_WISP=${GC_BEAD_ID' "$path" || true)"
        bare_assignments="$(grep -cF 'CURRENT_WISP=${GC_BEAD_ID:-}' "$path" || true)"
        [[ "$env_assignments" -ge 1 ]] ||
            fail "refinery should resolve its current wisp from \$GC_BEAD_ID: $path"
        [[ "$env_assignments" -eq "$bare_assignments" ]] ||
            fail "every refinery wisp resolution must be the bare \${GC_BEAD_ID:-} form backed by the live assignee query ($bare_assignments of $env_assignments): $path"
    done

    # The remaining two surfaces of the census in the header comment. They are
    # correct today by form -- bare plus the live query cannot mis-select on a
    # singleton -- so gate that exemption instead of leaving it to convention: a
    # harmonization edit that copies the deacon formula's trigger-preferring
    # line into either template goes red here rather than passing silently.
    local template
    for template in "$GASTOWN/agents/witness/prompt.template.md" \
                    "$GASTOWN/agents/deacon/prompt.template.md"; do
        ! grep -F 'GC_TRIGGER_WORK_BEAD_ID' "$template" >/dev/null ||
            fail "singleton prompt templates stay exempt from the per-role trigger rules by using the bare \${GC_BEAD_ID:-} form; they must not resolve a wisp from the spawn trigger: $template"
        env_assignments="$(grep -cF 'CURRENT_WISP=${GC_BEAD_ID' "$template" || true)"
        bare_assignments="$(grep -cF 'CURRENT_WISP=${GC_BEAD_ID:-}' "$template" || true)"
        [[ "$env_assignments" -ge 1 ]] ||
            fail "prompt template should resolve its current wisp from \$GC_BEAD_ID: $template"
        [[ "$env_assignments" -eq "$bare_assignments" ]] ||
            fail "every prompt-template wisp resolution must be the bare \${GC_BEAD_ID:-} form backed by the live assignee query ($bare_assignments of $env_assignments): $template"
    done
}

test_composition_is_documented() {
    # The retired maintenance pack is gone: the runtime composes the builtin
    # core pack via explicit city.toml includes, and gastown owns the only
    # mol-shutdown-dance. The docs must describe that model, not the old
    # fallback/ordering workarounds.
    grep -F 'builtin core pack' "$GASTOWN/README.md" >/dev/null ||
        fail "README should attribute mechanical housekeeping to the builtin core pack"
    ! grep -F '[imports.maintenance]' "$GASTOWN/README.md" >/dev/null ||
        fail "README should not reference the retired maintenance pack import"
    ! grep -Fi 'implicit maintenance' "$GASTOWN/README.md" >/dev/null ||
        fail "README should not describe implicit maintenance injection"
    grep -F 'gc formula show mol-shutdown-dance' "$GASTOWN/README.md" >/dev/null ||
        fail "README should document how to verify the effective shutdown-dance formula"
    grep -F 'builtin core' "$GASTOWN/pack.toml" >/dev/null ||
        fail "pack.toml should attribute mechanical housekeeping to the builtin core pack"
    ! grep -F '[imports.maintenance]' "$GASTOWN/pack.toml" >/dev/null ||
        fail "pack.toml should not reference the retired maintenance pack import"
}

test_polecat_startup_uses_standard_hook_claim() {
    local agent prompt propulsion
    agent="$GASTOWN/agents/polecat/agent.toml"
    prompt="$GASTOWN/agents/polecat/prompt.template.md"
    propulsion="$GASTOWN/template-fragments/propulsion.template.md"

    grep -F 'gc hook --claim --json' "$agent" >/dev/null ||
        fail "polecat nudge should call the standard hook claim path"
    grep -F 'default_sling_formula = "mol-polecat-work"' "$agent" >/dev/null ||
        fail "plain polecat sling must compile the implementation workflow instead of routing a bare task"
    grep -F 'gc hook --claim --json' "$prompt" >/dev/null ||
        fail "polecat prompt should call the standard hook claim path"
    grep -F 'gc hook --claim --json' "$propulsion" >/dev/null ||
        fail "polecat propulsion fragment should call the standard hook claim path"
    grep -F 'After closing any formula step bead, immediately run' "$prompt" >/dev/null ||
        fail "polecat prompt must require hook continuation after each formula step"
    grep -F 'After closing a step bead,' "$propulsion" >/dev/null ||
        fail "polecat propulsion fragment must require hook continuation after each formula step"
    ! grep -F 'run `gc hook` or' "$prompt" >/dev/null ||
        fail "polecat prompt must not regress to an unclaimed hook/work-query choice"
    ! grep -F 'run `gc hook` or' "$propulsion" >/dev/null ||
        fail "polecat propulsion fragment must not regress to an unclaimed hook/work-query choice"
}

test_review_leg_contract_forbids_synthetic_mutation() {
    local formula prompt
    formula="$GASTOWN/formulas/mol-review-leg.toml"
    prompt="$GASTOWN/agents/polecat/prompt.template.md"

    grep -F 'Do not create synthetic/test beads' "$formula" >/dev/null ||
        fail "review-leg formula must forbid synthetic test beads"
    grep -F 'Do not create test beads' "$formula" >/dev/null ||
        fail "review-leg load-assignment must forbid test bead creation"
    grep -F 'The only allowed bead mutations are the formula-prescribed' "$formula" >/dev/null ||
        fail "review-leg formula must define allowed mutation boundary"
    grep -F 'treat that text as' "$formula" >/dev/null ||
        fail "review-leg formula must treat plans/checklists as review subject matter"
    grep -F 'Do not start cities, spawn sessions, route extra work' "$formula" >/dev/null ||
        fail "review-leg formula must forbid executing reviewed checklist items"
    grep -F 'Formula-specific non-implementation assignments may explicitly tell you' "$prompt" >/dev/null ||
        fail "polecat prompt must allow formula-specific review/control close steps"
    ! grep -F '`gc bd close`, `gc bd close`' "$prompt" >/dev/null ||
        fail "polecat prompt must not duplicate its close prohibition"
    grep -F 'Default implementation formula: `mol-polecat-work`' "$prompt" >/dev/null ||
        fail "polecat prompt must describe mol-polecat-work as the default implementation formula"
    ! grep -F '**You MUST NOT close beads. EVER. No exceptions.**' "$prompt" >/dev/null ||
        fail "polecat prompt must not globally forbid review-leg close steps"
}

test_witness_wisp_queries_pin_include_infra() {
    local prompt formula total flagged
    prompt="$GASTOWN/agents/witness/prompt.template.md"
    formula="$GASTOWN/formulas/mol-witness-patrol.toml"

    # Wisp roots are ephemeral, so gc bd list skips the wisps tier unless
    # --include-infra is passed: a wisp-reconcile query without it returns []
    # even when a wisp is assigned, and the witness pours a duplicate. That
    # regressed once already, so pin the flag rather than trust the comments.
    # Deliberately witness-scoped: the refinery and deacon patrol queries
    # still carry the bare form and are tracked separately in #252, so a
    # pack-wide assertion would fail here instead of guarding this contract.
    total=$(grep -h -- '--type=molecule' "$prompt" "$formula" |
        grep -c -F 'gc bd list' || true)
    flagged=$(grep -h -- '--type=molecule' "$prompt" "$formula" |
        grep -F 'gc bd list' | grep -c -- '--include-infra' || true)

    # -ge, not -eq: the flagged/total assertion below owns the contract, so an
    # exact count only adds a cardinality pin -- and a legitimate sixth query,
    # or a prose line that happens to name all three tokens counted above,
    # then fails the suite with nothing wrong. Measured: -ge holds every
    # regression mode red (flag stripped from a prompt query, from the formula
    # query, a query site deleted, an unflagged sixth site) while dropping
    # both false positives. A prose line carrying the query tokens but not the
    # flag still fails -- loud, in the safe direction. Requiring --json on
    # counted lines would silence that last one too, but it is not a
    # substitute: it stops counting any query that does not pipe to jq, so an
    # unflagged new site written without --json passes silently.
    [[ "$total" -ge 5 ]] ||
        fail "expected at least 5 witness --type=molecule wisp queries (4 prompt + 1 formula), found $total"
    [[ "$flagged" -eq "$total" ]] ||
        fail "witness --type=molecule wisp queries must pass --include-infra ($flagged/$total do)"
}

test_witness_handoff_recovery_is_guarded_and_fail_closed() {
    local witness polecat refinery block signature writers

    witness="$GASTOWN/formulas/mol-witness-patrol.toml"
    polecat="$GASTOWN/formulas/mol-polecat-work.toml"
    refinery="$GASTOWN/formulas/mol-refinery-patrol.toml"
    parse_toml "$witness" "$polecat" "$refinery"

    # Witness Step 3a completes the refinery handoff for a polecat that died
    # between submit-and-exit steps 5 and 6 (gastownhall/gascity-packs#276).
    # It mutates a bead assigned to a dead actor and then deletes its worktree,
    # so every property below is one that an edit can silently invert while the
    # formula still parses and the rest of this suite stays green.

    # Placement. A presence-only grep survives moving 3a after 3b, which makes
    # it dead code for every bead 3b has already returned to pool. A sequence
    # signature pins order, count, and cardinality together, and pins the
    # precondition (Step 3's on-main close) rather than 3a alone.
    signature=$(awk '
        /^gc bd close <bead> --force$/ { print "step3-close" }
        /^\*\*Step 3a:/               { print "step3a" }
        /^\*\*Step 3b:/               { print "step3b" }
    ' "$witness" | tr '\n' ' ')
    [[ "$signature" == "step3-close step3a step3b " ]] ||
        fail "witness recovery must run Step 3's on-main close, then Step 3a, then Step 3b (got: $signature)"

    # Discriminator. metadata.target is a mint-time sling input that nothing
    # ever unsets and that survives both refinery rejection paths, so keying on
    # it fires 3a for beads that never submitted -- shipping an already-rejected
    # or half-finished tip to a refinery whose only merge gate is tests-pass.
    block=$(awk '/^\*\*Step 3a:/{f=1} /^\*\*Step 3b:/{f=0} f' "$witness")
    printf '%s\n' "$block" |
        grep -F 'if [ "$HANDOFF_STAGE" = "target_recorded" ] && [ -n "$BRANCH_ON_ORIGIN" ]; then' >/dev/null ||
        fail "Step 3a must key the handoff on handoff_stage and a branch that is really on origin"
    ! printf '%s\n' "$block" | grep -F '[ -n "$BEAD_TARGET" ]' >/dev/null ||
        fail "Step 3a must not treat metadata.target as a completion signal"
    # Both halves of the backstop, in one pin. ls-remote patterns match ref
    # tails, so the bare "$BRANCH" form is truthy for a branch that is not on
    # origin whenever a tail-colliding ref (archive/polecat/<id>) survives it;
    # the fully-qualified form measures the property the message below names.
    printf '%s\n' "$block" |
        grep -F '[ -n "$BRANCH" ] && BRANCH_ON_ORIGIN=$(git ls-remote --heads origin "refs/heads/$BRANCH"' >/dev/null ||
        fail "Step 3a must guard the empty branch and query the fully-qualified ref: ls-remote patterns match ref tails"

    # The claim guard and the fail-closed arm. bd refuses a cross-actor
    # --assignee write against the dead polecat's live in_progress claim
    # without --force; unchecked, the witness would then mail success, delete
    # the worktree, and skip the 3b reset that used to recover the bead.
    printf '%s\n' "$block" | grep -F -- '--set-metadata gc.routed_to="" --force' >/dev/null ||
        fail "Step 3a's cross-actor reassignment must pass --force"
    # Failure policy, pinned separately from the ordering signature below so a
    # change to either reports as itself. delete-source runs after the
    # reassignment has already succeeded, so it is best-effort like the
    # wake/nudge: leaving it bare invites a future editor to read it as
    # load-bearing and abort a handoff that in fact completed.
    printf '%s\n' "$block" |
        grep -F 'gc workflow delete-source <bead> --apply || true' >/dev/null ||
        fail "Step 3a's subtree cleanup must state its best-effort failure policy (|| true), as the wake/nudge do"
    printf '%s\n' "$block" |
        grep -F 'REFINERY_TARGET="${GC_RIG:+$GC_RIG/}{{binding_prefix}}refinery"' >/dev/null ||
        fail "Step 3a must use submit-and-exit step 6's conditional rig prefix for the assignee write"

    # Ordering inside the block, again as a signature: the reassignment's exit
    # status is the if condition, the subtree cleanup and the success mail,
    # wake, nudge, and worktree removal all sit inside the success arm ahead of
    # a real else, and the subtree cleanup precedes the signal so the refinery
    # is never woken mid-cleanup. Substring pins alone stay green if the mail
    # is hoisted above the if or the else is dropped -- and every statement
    # ordered after the if is a statement the failure arm cannot reach, which
    # is what makes falling through to Step 3b a true no-op.
    signature=$(printf '%s\n' "$block" | awk '
        /^  if gc bd update <bead> /                  { print "guarded-update" }
        /^    gc workflow delete-source <bead> --apply/ { print "delete-source" }
        /gc mail send mayor\/ -s "ORPHAN_HANDED_OFF/  { print "mail" }
        /gc session wake "\$REFINERY_TARGET"/         { print "wake" }
        /gc session nudge "\$REFINERY_TARGET"/        { print "nudge" }
        /git worktree remove <worktree-path> --force/ { print "worktree-remove" }
        /^  else$/                                    { print "else" }
    ' | tr '\n' ' ')
    [[ "$signature" == "guarded-update delete-source mail wake nudge worktree-remove else " ]] ||
        fail "Step 3a must check the reassignment's exit status first, then delete the subtree and mail/wake/nudge/clean up in the success arm, and fall through in an else (got: $signature)"

    # The marker contract spans three formulas: one writer, three clearers.
    # Losing any clearer silently restores the stale-marker over-trigger that
    # keying on handoff_stage exists to prevent.
    writers=$(cat "$polecat" "$witness" "$refinery" |
        grep -c -F -- '--set-metadata handoff_stage=target_recorded' || true)
    [[ "$writers" -eq 1 ]] ||
        fail "handoff_stage must be written only by submit-and-exit step 5 (found $writers writers)"
    grep -F 'gc bd update "$WORK_BEAD_ID" --unset-metadata handoff_stage' "$polecat" >/dev/null ||
        fail "workspace-setup must clear a stale handoff_stage on every fresh attempt"
    [[ $(grep -c -F -- '--unset-metadata handoff_stage' "$refinery") -eq 2 ]] ||
        fail "both refinery rejection paths must clear handoff_stage"

    # 3b keeps its own recovery for everything that falls through.
    grep -F 'gc workflow delete-source <bead> --apply && gc workflow reopen-source <bead>' "$witness" >/dev/null ||
        fail "Step 3b must still reopen the source bead for fall-through recoveries"
}

test_boot_wisp_queries_pin_include_infra() {
    local prompt formula total flagged
    prompt="$GASTOWN/agents/boot/prompt.template.md"
    formula="$GASTOWN/formulas/mol-boot-patrol.toml"

    # Same contract as the witness guard above, scoped to boot: boot's patrol
    # loop reconciles to exactly one open wisp, and without --include-infra
    # every one of its queries returns [] regardless of status, so the surplus
    # burn never runs and each cycle pours a fresh wisp while its predecessor
    # leaks. Boot shipped with the bare form on all three sites one commit
    # after the witness fix, so scope this per-agent rather than widening the
    # witness test: a pack-wide assertion is red either way (measured 10/21 at
    # this commit) because the deacon and refinery sites are still bare and
    # tracked separately in #252.
    total=$(grep -h -- '--type=molecule' "$prompt" "$formula" |
        grep -c -F 'gc bd list' || true)
    flagged=$(grep -h -- '--type=molecule' "$prompt" "$formula" |
        grep -F 'gc bd list' | grep -c -- '--include-infra' || true)

    # -ge for the same reason as the witness guard: the flagged/total assertion
    # owns the contract, so the count is a floor that catches a deleted query
    # site, not a cardinality pin that a legitimate fourth query would break.
    [[ "$total" -ge 3 ]] ||
        fail "expected at least 3 boot --type=molecule wisp queries (5 at this commit: 2 prompt + 3 formula), found $total"
    [[ "$flagged" -eq "$total" ]] ||
        fail "boot --type=molecule wisp queries must pass --include-infra ($flagged/$total do)"
}

test_boot_patrol_burn_resolves_current_wisp() {
    local prompt formula asset name burn_lines bare

    prompt="$GASTOWN/agents/boot/prompt.template.md"
    formula="$GASTOWN/formulas/mol-boot-patrol.toml"

    # gc never sets GC_BEAD_ID for a named session: the session env builder
    # exports GC_SESSION_ID/NAME/ALIAS/TEMPLATE/ORIGIN/AGENT, and GC_BEAD_ID is
    # exported only into ralph check scripts. Boot has no hook-claim block to
    # export it, so a bare "$GC_BEAD_ID" burn target expands to the empty
    # string: "burn this wisp" reclaims nothing and leaves the wisp behind on
    # every cycle -- the same leak the --include-infra guard above exists for,
    # arriving by a different route. Every other patrol prompt resolves through
    # CURRENT_WISP with an in-progress fallback query, and so does every other
    # patrol formula except the witness's, which burns an agent-resolved
    # <this-wisp-id> placeholder instead. So pin that idiom on both boot assets
    # rather than the burn line alone: the deletion of either half is what
    # makes the target silently empty.
    for asset in "$prompt" "$formula"; do
        name=$(basename "$asset")

        grep -qF 'CURRENT_WISP=${GC_BEAD_ID:-}' "$asset" ||
            fail "$name must seed CURRENT_WISP from \$GC_BEAD_ID"
        grep -q -- 'CURRENT_WISP=\$(gc bd list .*--status=in_progress .*--include-infra' "$asset" ||
            fail "$name must fall back to an in-progress wisp query when \$GC_BEAD_ID is unset"
        grep -qF 'gc bd mol burn "$CURRENT_WISP" --force' "$asset" ||
            fail "$name must burn the resolved \$CURRENT_WISP"
    done

    # No burn call anywhere in boot's assets may address GC_BEAD_ID directly.
    # This is the assertion that survives a rewrite of the block above, and it
    # is what catches a *new* bare burn site rather than a mutated one. Same
    # prose tradeoff as the guards above: a doc line carrying both tokens fails
    # loudly, in the safe direction.
    burn_lines=$(grep -h -F 'gc bd mol burn' "$prompt" "$formula" || true)
    [[ -n "$burn_lines" ]] ||
        fail "expected boot assets to carry gc bd mol burn calls, found none"
    bare=$(printf '%s\n' "$burn_lines" | grep -c -F 'GC_BEAD_ID' || true)
    [[ "$bare" -eq 0 ]] ||
        fail "boot burn targets must resolve through \$CURRENT_WISP, not a bare \$GC_BEAD_ID ($bare do not)"
}

test_boot_deacon_observation_query_sees_wisps_tier() {
    local prompt formula asset name lines unflagged

    prompt="$GASTOWN/agents/boot/prompt.template.md"
    formula="$GASTOWN/formulas/mol-boot-patrol.toml"

    # The tier census above cannot protect this site. Reverting the
    # deacon-observation query to its blind pre-fix form drops --type=molecule
    # too, so the line leaves the numerator and the denominator together (5/5
    # -> 4/4) and the -ge 3 floor absorbs the loss: whole suite green, while
    # the decision table's wisp-keyed rows ("young wisp -> backoff", "very
    # stale wisp -> warrant") go back to being untriggerable because a patrol
    # wisp never shows in the deacon's work. Pin the site by its distinctive
    # text instead of by tier membership. The prompt's quick-reference row is
    # deliberately untyped -- it asks about deacon work generally -- so it
    # matches no counter at all and this is the only guard that reaches it.
    for asset in "$prompt" "$formula"; do
        name=$(basename "$asset")

        # || true is load-bearing: the suite runs under set -euo pipefail, so
        # an unguarded grep would kill the run with no diagnostic on exactly
        # the deletion arm the next assertion exists to report.
        lines=$(grep -- 'gc bd list --assignee=.*deacon' "$asset" || true)
        [[ -n "$lines" ]] ||
            fail "$name must keep a deacon-observation query"

        # Every matching line, not merely one of them: a grep -q over the whole
        # match set passes as soon as any deacon query carries the flag, so a
        # new blind one added beside a good one reads as covered. That is the
        # same addition shape the bare-burn census above exists to catch.
        unflagged=$(printf '%s\n' "$lines" | grep -c -v -- '--include-infra' || true)
        [[ "$unflagged" -eq 0 ]] ||
            fail "$name deacon-observation queries must pass --include-infra ($unflagged do not)"
    done
}

test_refinery_direct_merge_is_worktree_safe_and_fail_closed() {
    local formula direct_block
    formula="$GASTOWN/formulas/mol-refinery-patrol.toml"

    direct_block=$(python3 - "$formula" <<'PY'
import sys
text = open(sys.argv[1], encoding="utf-8").read()
start = text.index('**If MERGE_STRATEGY = "direct"')
end = text.index('**If MERGE_STRATEGY = "mr"')
print(text[start:end])
PY
)

    [[ "$direct_block" == *'git worktree add --detach "$MERGE_WT" "origin/$TARGET"'* ]] ||
        fail "direct refinery merge must use a detached target worktree"
    [[ "$direct_block" == *'+refs/heads/${TARGET}:refs/remotes/origin/${TARGET}'* ]] ||
        fail "direct refinery merge refspecs must brace TARGET for zsh-safe expansion"
    [[ "$direct_block" == *'git -C "$MERGE_WT" push origin "HEAD:$TARGET"'* ]] ||
        fail "direct refinery merge must push the verified merge worktree HEAD"
    [[ "$direct_block" == *'[ "$MERGED_SHA" != "$REMOTE" ]'* ]] ||
        fail "direct refinery merge must compare merged SHA to origin target"
    [[ "$direct_block" == *'STOP. Do not mutate bead state.'* ]] ||
        fail "direct refinery merge must fail closed before metadata writes"
    ! printf '%s\n' "$direct_block" | grep -E '^[[:space:]]*git checkout \$TARGET([[:space:]]|$)' >/dev/null ||
        fail "direct refinery merge must not checkout target branch in the active worktree"

    python3 - "$formula" <<'PY' || fail "direct refinery merge must verify origin before setting merged metadata"
import sys
text = open(sys.argv[1], encoding="utf-8").read()
start = text.index('**If MERGE_STRATEGY = "direct"')
end = text.index('**If MERGE_STRATEGY = "mr"')
block = text[start:end]
verify = block.index('[ "$MERGED_SHA" != "$REMOTE" ]')
metadata = block.index('--set-metadata merge_result=merged')
if verify >= metadata:
    raise SystemExit(1)
PY
}

test_refinery_idle_exit_closes_patrol_wisp() {
    local formula idle_block
    formula="$GASTOWN/formulas/mol-refinery-patrol.toml"
    idle_block=$(python3 - "$formula" <<'PY'
import sys
text = open(sys.argv[1], encoding="utf-8").read()
start = text.index('If NO work found:')
end = text.index('[[steps]]', start)
print(text[start:end])
PY
)
    [[ "$idle_block" == *'gc bd update "$CURRENT_WISP" --claim'* ]] ||
        fail "idle refinery exit must claim its patrol wisp before closing"
    [[ "$idle_block" == *'gc bd close "$CURRENT_WISP" --reason "Idle: no branch-backed work assigned."'* ]] ||
        fail "idle refinery exit must close its patrol wisp with an audit reason"
    [[ "$idle_block" == *'gc runtime drain-ack'* ]] ||
        fail "idle refinery exit must drain after closing its patrol wisp"
    [[ "$idle_block" != *'--status=closed'* ]] ||
        fail "idle refinery exit must not close through update --status=closed"
}

test_refinery_idle_exit_uses_the_session_bound_wisp() {
    local formula idle_script tmp log
    formula="$GASTOWN/formulas/mol-refinery-patrol.toml"
    idle_script=$(python3 - "$formula" <<'PY'
import sys

text = open(sys.argv[1], encoding="utf-8").read()
start = text.index("If NO work found:")
start = text.index("```bash", start) + len("```bash")
end = text.index("gc runtime drain-ack", start) + len("gc runtime drain-ack")
print(text[start:end])
PY
)

    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' RETURN
    log="$tmp/gc.log"
    mkdir "$tmp/bin"
    cat >"$tmp/bin/gc" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$GC_TEST_LOG"
case "$*" in
    'hook current --id-only') echo 'sl-wisp-rt8' ;;
    'bd update sl-wisp-rt8 --claim') ;;
    'bd close sl-wisp-rt8 --reason Idle: no branch-backed work assigned.') ;;
    'runtime drain-ack') ;;
    'bd list'*) echo 'unrelated-molecule' ;;
    *) echo "unexpected gc invocation: $*" >&2; exit 1 ;;
esac
EOF
    chmod +x "$tmp/bin/gc"

    # GC_BEAD_ID отсутствует, а в ledger могут быть другие назначенные wisps.
    # Для закрытия допустим только claim текущей сессии, а не произвольная молекула.
    env -u GC_BEAD_ID -u GC_TRIGGER_BEAD_ID \
        GC_AGENT='slovo/gastown.refinery' GC_TEST_LOG="$log" PATH="$tmp/bin:$PATH" \
        bash -c "$idle_script"

    grep -Fx 'hook current --id-only' "$log" >/dev/null ||
        fail "idle refinery exit must resolve the current session claim"
    ! grep -F 'bd list' "$log" >/dev/null ||
        fail "idle refinery exit must not select an arbitrary assigned molecule"
    grep -Fx 'bd close sl-wisp-rt8 --reason Idle: no branch-backed work assigned.' "$log" >/dev/null ||
        fail "idle refinery exit must close only the session-bound wisp"
    ! grep -F 'unrelated-molecule' "$log" >/dev/null ||
        fail "idle refinery exit must preserve unrelated wisp evidence"
}

test_prime_prompts_are_city_generic_and_compact() {
    local mayor propulsion awareness
    mayor="$GASTOWN/agents/mayor/prompt.template.md"
    propulsion="$GASTOWN/template-fragments/propulsion.template.md"
    awareness="$GASTOWN/template-fragments/operational-awareness.template.md"

    ! grep -E 'hq-|gt-|anthropics/|Wyvern game' "$mayor" >/dev/null ||
        fail "mayor prompt must not hardcode demo cities, rigs, prefixes, or organizations"
    ! grep -E '\{\{ \.IssuePrefix \}\}|\{\{ \.RigName \}\}' "$mayor" >/dev/null ||
        fail "city-scoped mayor prompt must not render rig-scoped variables"
    ! grep -F '**Rig lifecycle commands:**' "$mayor" >/dev/null ||
        fail "mayor prompt should not duplicate the rig lifecycle quick-reference"
    [[ $(grep -c '^## Handoff$' "$mayor") -eq 1 ]] ||
        fail "mayor prompt should describe handoff once"

    grep -F 'gc hook --claim --json' "$propulsion" >/dev/null ||
        fail "propulsion roles should use the standard hook claim path"
    ! grep -E '\{\{ \.(WorkQuery|AssignedReadyQuery|RoutedPoolQuery) \}\}' "$propulsion" >/dev/null ||
        fail "mayor, crew, and dog propulsion should not inline generated work-query blobs"
    ! grep -F '{{ .WorkQuery }}' "$GASTOWN/agents/dog/prompt.template.md" >/dev/null ||
        fail "dog prompt should not expose the generated pool query"
    grep -F 'gc hook --claim --json' "$GASTOWN/agents/dog/prompt.template.md" >/dev/null ||
        fail "dog prompt should use atomic hook claim"
    ! grep -F 'port 3307' "$awareness" >/dev/null ||
        fail "operational awareness must not hardcode a Dolt port"
    grep -F 'gc dolt status' "$awareness" >/dev/null ||
        fail "operational awareness should direct agents to the effective Dolt port"
    grep -F 'Never probe a guessed or fixed Dolt port.' "$awareness" >/dev/null ||
        fail "operational awareness must forbid guessed Dolt endpoints"
    grep -F 'configured endpoint and the exact probe target' "$awareness" >/dev/null ||
        fail "operational awareness must require configured and probed endpoint evidence"
    grep -F 'endpoint is unknown and stop' "$awareness" >/dev/null ||
        fail "operational awareness must fail closed when endpoint discovery fails"
}

test_dog_assets_are_pack_local
test_retired_dog_formulas_are_not_reintroduced
test_shutdown_dance_contracts_are_executable
test_shutdown_dance_lifecycle_and_audit_contracts
test_work_bead_resolution_discriminator_is_pinned
test_composition_is_documented
test_polecat_startup_uses_standard_hook_claim
test_review_leg_contract_forbids_synthetic_mutation
test_prime_prompts_are_city_generic_and_compact
test_witness_wisp_queries_pin_include_infra
test_witness_handoff_recovery_is_guarded_and_fail_closed
test_boot_wisp_queries_pin_include_infra
test_boot_patrol_burn_resolves_current_wisp
test_boot_deacon_observation_query_sees_wisps_tier
test_refinery_direct_merge_is_worktree_safe_and_fail_closed
test_refinery_idle_exit_closes_patrol_wisp
test_refinery_idle_exit_uses_the_session_bound_wisp

echo "gastown pack asset tests passed"
