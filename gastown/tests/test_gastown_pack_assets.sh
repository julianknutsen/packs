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

# Count the bare wisp assignments in $1 that are actually BACKED by the live
# assignee query $2 on one of the next few lines. Counting the two halves
# separately would not do it: a file can carry the bare form everywhere and no
# query at all, which is what the form-only comparison this replaced scored as
# a pass. The window is deliberately small -- the fallback is
# `if [ -z "$CURRENT_WISP" ]; then` on the next line and the query on the one
# after -- so an unrelated query further down the file cannot pair with it.
count_paired_wisp_resolutions() {
    awk -v query="$2" '
        index($0, "CURRENT_WISP=${GC_BEAD_ID:-}") { window = 3; next }
        window > 0 {
            if (index($0, query)) { paired++; window = 0; next }
            window--
        }
        END { print paired + 0 }
    ' "$1"
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

# Six role-surfaces resolve a work bead from the environment, by deliberately
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
#   refinery prompt template  - same rule as the refinery formula.
#   witness prompt template   - same rule.
#   deacon prompt template    - same rule, even though the deacon FORMULA may
#     prefer the trigger: the template is read on wakes the formula does not
#     drive, so it cannot rely on the formula's one-wisp-per-env property.
# The three prompt templates are safe by the PAIR they use, not by the bare
# form alone: providers are not required to export $GC_BEAD_ID at all
# (the claude provider does not), so the bare half can resolve nothing and the
# live assignee query is the load-bearing half. That query reaches wisp roots
# only with --include-infra, since they are ephemeral and gc bd list hides the
# wisps tier without it. Both halves are pinned below, together, so the
# exemption is gated rather than conventional.
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
    # bare form AND be backed by the live query on the following lines. Pairing
    # the two, rather than comparing counts of the bare form against itself,
    # catches conversion in either direction AND the loss of the query -- which
    # a count of the bare form alone cannot see: measured, deleting the whole
    # `if [ -z "$CURRENT_WISP" ]` fallback block at every non-witness surface
    # left this suite green while no refinery or deacon surface could resolve a
    # wisp at all. Neither count freezes the number of call sites.
    local wisp_query='gc bd list --assignee="$GC_AGENT" --status=in_progress --type=molecule --include-infra --limit=1 --json'
    local path env_assignments paired
    for path in "$refinery" "$refinery_prompt"; do
        ! grep -F 'GC_TRIGGER_WORK_BEAD_ID' "$path" >/dev/null ||
            fail "the refinery rotates wisps in-session, so a spawn-fixed trigger goes stale mid-loop; it must resolve every wisp from \$GC_BEAD_ID plus the live assignee query, never the trigger: $path"
        env_assignments="$(grep -cF 'CURRENT_WISP=${GC_BEAD_ID' "$path" || true)"
        paired="$(count_paired_wisp_resolutions "$path" "$wisp_query")"
        [[ "$env_assignments" -ge 1 ]] ||
            fail "refinery should resolve its current wisp from \$GC_BEAD_ID: $path"
        [[ "$paired" -eq "$env_assignments" ]] ||
            fail "every refinery wisp resolution must be the bare \${GC_BEAD_ID:-} form immediately backed by the live \`--include-infra\` assignee query ($paired of $env_assignments are): $path"
    done

    # The remaining two surfaces of the census in the header comment. Their
    # exemption rests on the PAIR, not on the bare form: a provider need not
    # export $GC_BEAD_ID (the claude provider does not), so the bare half alone
    # resolves nothing and the query is what makes the exemption true. Gate both
    # halves instead of leaving either to convention: a harmonization edit that
    # copies the deacon formula's trigger-preferring line into either template,
    # or that drops the fallback query, goes red here rather than passing
    # silently.
    local template
    for template in "$GASTOWN/agents/witness/prompt.template.md" \
                    "$GASTOWN/agents/deacon/prompt.template.md"; do
        ! grep -F 'GC_TRIGGER_WORK_BEAD_ID' "$template" >/dev/null ||
            fail "singleton prompt templates stay exempt from the per-role trigger rules by using the bare \${GC_BEAD_ID:-} form; they must not resolve a wisp from the spawn trigger: $template"
        env_assignments="$(grep -cF 'CURRENT_WISP=${GC_BEAD_ID' "$template" || true)"
        paired="$(count_paired_wisp_resolutions "$template" "$wisp_query")"
        [[ "$env_assignments" -ge 1 ]] ||
            fail "prompt template should resolve its current wisp from \$GC_BEAD_ID: $template"
        [[ "$paired" -eq "$env_assignments" ]] ||
            fail "every prompt-template wisp resolution must be the bare \${GC_BEAD_ID:-} form immediately backed by the live \`--include-infra\` assignee query ($paired of $env_assignments are): $template"
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

test_wisp_queries_pin_include_infra() {
    local total flagged
    local -a surfaces=(
        "$GASTOWN/agents/witness/prompt.template.md"
        "$GASTOWN/formulas/mol-witness-patrol.toml"
        "$GASTOWN/agents/refinery/prompt.template.md"
        "$GASTOWN/formulas/mol-refinery-patrol.toml"
        "$GASTOWN/agents/deacon/prompt.template.md"
        "$GASTOWN/formulas/mol-deacon-patrol.toml"
    )

    # Wisp roots are ephemeral, so gc bd list skips the wisps tier unless
    # --include-infra is passed: a wisp query without it returns [] even when a
    # wisp is assigned, so the caller pours a duplicate, or reads no current
    # wisp and never burns. That regressed once already, so pin the flag rather
    # than trust the comments.
    #
    # This was witness-scoped when it was written, because the refinery and
    # deacon queries still carried the blind form and were tracked separately in
    # #252. They no longer do. The pair pin above covers eight of the ten
    # newly-flagged sites, but it scans only the four files that resolve into
    # CURRENT_WISP, so mol-deacon-patrol.toml's query and the deacon prompt's
    # ASSIGNED_WISP query would be left pinned by nothing. The census below is
    # therefore the whole set of surfaces that resolve a wisp.
    #
    # --assignee= is what selects the class, and it is a property of the class
    # rather than an incidental token: every wisp RESOLUTION is scoped to this
    # agent. It is what keeps mol-refinery-patrol's prose line -- which names
    # `gc bd list --type=molecule --status=closed` for predecessor context, not
    # a resolution, and is deliberately left blind -- out of the count. Do not
    # narrow on --json or --limit instead: those are formatting choices, so an
    # unflagged new resolution site written without them would pass silently.
    total=$(grep -h -- '--type=molecule' "${surfaces[@]}" |
        grep -F 'gc bd list' | grep -c -F -- '--assignee=' || true)
    flagged=$(grep -h -- '--type=molecule' "${surfaces[@]}" |
        grep -F 'gc bd list' | grep -F -- '--assignee=' |
        grep -c -- '--include-infra' || true)

    # -ge, not -eq: the flagged/total assertion below owns the contract, so an
    # exact count only adds a cardinality pin -- and a legitimate new query
    # would then fail the suite with nothing wrong. Measured: -ge holds every
    # regression mode red (flag stripped from any one query, a query site
    # deleted, an unflagged new site) while dropping that false positive. The
    # floor is what catches deletion, which the ratio alone cannot: delete one
    # query site and 14/14 satisfies -eq, delete them all and 0/0 does too.
    [[ "$total" -ge 15 ]] ||
        fail "expected at least 15 --type=molecule wisp resolution queries across the witness, refinery and deacon surfaces, found $total"
    [[ "$flagged" -eq "$total" ]] ||
        fail "every --type=molecule wisp resolution query must pass --include-infra; wisp roots are ephemeral and gc bd list hides that tier without it ($flagged/$total do)"
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
test_wisp_queries_pin_include_infra
test_refinery_direct_merge_is_worktree_safe_and_fail_closed

echo "gastown pack asset tests passed"
