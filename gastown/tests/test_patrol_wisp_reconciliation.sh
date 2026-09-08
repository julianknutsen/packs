#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
GASTOWN="$ROOT/gastown"
TMP=$(mktemp -d "${TMPDIR:-/tmp}/patrol-wisp-test.XXXXXX")
trap 'rm -rf "$TMP"' EXIT

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

role_prompt() {
    printf '%s/agents/%s/prompt.template.md\n' "$GASTOWN" "$1"
}

role_formula() {
    printf '%s/formulas/mol-%s-patrol.toml\n' "$GASTOWN" "$1"
}

test_assets_use_ephemeral_query_contract() {
    local role asset handoff
    for role in deacon witness refinery; do
        for asset in "$(role_prompt "$role")" "$(role_formula "$role")"; do
            ! grep -E 'gc bd list .*--type=molecule.*--status=(open|in_progress)|gc bd list .*--status=(open|in_progress).*--type=molecule' "$asset" >/dev/null ||
                fail "$asset must not use durable-tier gc bd list for patrol roots"
        done
        asset=$(role_formula "$role")
        grep -F "'ephemeral=true AND (status=open OR status=in_progress)'" "$asset" >/dev/null ||
            fail "$asset must query both open and in-progress ephemeral patrol roots"
        for asset in "$(role_formula "$role")" "$GASTOWN/template-fragments/patrol-wisp.template.md"; do
            grep -F '($w.dependency_count // -1) == 0' "$asset" >/dev/null ||
                fail "$asset must fail closed when dependency_count is absent or nonzero"
            grep -F '($w.dependent_count // -1) == 0' "$asset" >/dev/null ||
                fail "$asset must fail closed when dependent_count is absent or nonzero"
        done
        grep -F '{{ template "patrol-wisp-ledger" . }}' "$(role_prompt "$role")" >/dev/null ||
            fail "$role prompt must include the shared patrol-wisp ledger contract"
        handoff="$TMP/$role-static-handoff.sh"
        extract_formula_handoff "$role" "$handoff"
        grep -F 'if [ -z "${GC_AGENT:-}" ]; then' "$handoff" >/dev/null ||
            fail "$role formula handoff must reject an empty GC_AGENT"
        ! grep -F 'gc runtime drain-ack' "$handoff" >/dev/null ||
            fail "$role formula handoff must preserve provider state on every failure"
    done

    local shared="$GASTOWN/template-fragments/patrol-wisp.template.md"
    grep -F 'Patrol roots are ephemeral molecule rows' "$shared" >/dev/null ||
        fail "shared contract must document the ephemeral tier"
    grep -F 'Never burn a surplus `in_progress` or materialized root' "$shared" >/dev/null ||
        fail "shared contract must preserve active/materialized surplus"
    grep -F 'if [ -z "${GC_AGENT:-}" ]; then' "$shared" >/dev/null ||
        fail "shared startup reconciliation must reject an empty GC_AGENT"
}

extract_prompt_startup() {
    local role=$1 output=$2
    python3 - "$(role_prompt "$role")" \
        "$GASTOWN/template-fragments/patrol-wisp.template.md" "$output" "$role" <<'PY'
import pathlib
import re
import sys

source = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8")
shared = pathlib.Path(sys.argv[2]).read_text(encoding="utf-8")
marker = f"PATROL_FORMULA=mol-{sys.argv[4]}-patrol"
marker_pos = source.index(marker)
start = source.rfind("```bash\n", 0, marker_pos) + len("```bash\n")
end = source.index("\n```", start)
block = source[start:end]
match = re.search(
    r'\{\{ define "patrol-wisp-startup" \}\}\n(.*?)\n\{\{ end \}\}',
    shared,
    flags=re.S,
)
if match is None:
    raise SystemExit("patrol-wisp-startup template not found")
block = block.replace('{{ template "patrol-wisp-startup" . }}', match.group(1))
while "{{" in block:
    left = block.index("{{")
    right = block.index("}}", left) + 2
    block = block[:left] + "fixture" + block[right:]
pathlib.Path(sys.argv[3]).write_text(block + "\n", encoding="utf-8")
PY
}

extract_formula_handoff() {
    local role=$1 output=$2
    python3 - "$(role_formula "$role")" "$output" <<'PY'
import pathlib
import re
import sys
import tomllib

with open(sys.argv[1], "rb") as handle:
    formula = tomllib.load(handle)
description = next(step["description"] for step in formula["steps"] if step["id"] == "next-iteration")
blocks = re.findall(r"```bash\n(.*?)```", description, flags=re.S)
selected = []
started = False
for block in blocks:
    if "PATROL_FORMULA=" in block:
        started = True
    if started:
        selected.append(block)
    if started and 'gc bd mol burn "$CURRENT_WISP" --force' in block:
        break
if not selected:
    raise SystemExit("next-iteration patrol handoff block not found")
text = "\n".join(selected)
text = re.sub(r"\{\{[^{}]+\}\}", "fixture", text)
pathlib.Path(sys.argv[2]).write_text(text + "\n", encoding="utf-8")
PY
}

make_fake_gc() {
    mkdir -p "$TMP/bin" "$TMP/state"
    cat >"$TMP/bin/gc" <<'SH'
#!/usr/bin/env bash
set -euo pipefail

printf '%q ' "$@" >>"$GC_TEST_LOG"
printf '\n' >>"$GC_TEST_LOG"

if [[ ${1:-} == runtime && ${2:-} == drain-ack ]]; then
    printf 'DRAIN_ACK\n' >>"$GC_TEST_ACTIONS"
    exit 0
fi
if [[ ${1:-} != bd ]]; then
    exit 0
fi
shift
case ${1:-} in
    --rig=*) shift ;;
    --rig)
        shift
        shift
        ;;
esac

case "${1:-} ${2:-}" in
    "query --json")
        if [[ ${GC_TEST_QUERY_FAIL:-0} == 1 ]]; then
            exit 7
        fi
        shopt -s nullglob
        rows=("$GC_TEST_STATE_DIR"/*.json)
        if ((${#rows[@]} == 0)); then
            printf '[]\n'
        else
            jq -s '[.[] | .[0] | select(.status == "open" or .status == "in_progress")]' "${rows[@]}"
        fi
        ;;
    "show "*)
        cat "$GC_TEST_STATE_DIR/$2.json"
        ;;
    "mol burn")
        printf 'BURN %s\n' "$3" >>"$GC_TEST_ACTIONS"
        jq '.[0].status = "closed"' "$GC_TEST_STATE_DIR/$3.json" \
            >"$GC_TEST_STATE_DIR/$3.json.tmp"
        mv "$GC_TEST_STATE_DIR/$3.json.tmp" "$GC_TEST_STATE_DIR/$3.json"
        ;;
    "mol wisp")
        count=1
        if [[ -f "$GC_TEST_STATE_DIR/.pour-count" ]]; then
            count=$(( $(<"$GC_TEST_STATE_DIR/.pour-count") + 1 ))
        fi
        printf '%s\n' "$count" >"$GC_TEST_STATE_DIR/.pour-count"
        id="fixture-wisp-new-$count"
        printf 'POUR %s\n' "$id" >>"$GC_TEST_ACTIONS"
        jq -n --arg id "$id" --arg formula "$3" \
            '[{id:$id,title:$formula,status:"open",issue_type:"molecule",assignee:"",
               created_at:"2026-07-23T01:00:00Z",ephemeral:true,
               dependency_count:0,dependent_count:0}]' \
            >"$GC_TEST_STATE_DIR/$id.json"
        printf '{"new_epic_id":"%s"}\n' "$id"
        ;;
    "update "*)
        id=$2
        shift 2
        status=
        assignee=
        while (($#)); do
            case $1 in
                --status=*) status=${1#--status=} ;;
                --status)
                    shift
                    status=${1:-}
                    ;;
                --assignee=*) assignee=${1#--assignee=} ;;
                --assignee)
                    shift
                    assignee=${1:-}
                    ;;
            esac
            shift
        done
        state="$GC_TEST_STATE_DIR/$id.json"
        if [[ -n "$status" ]]; then
            jq --arg status "$status" '.[0].status = $status' "$state" >"$state.tmp"
            mv "$state.tmp" "$state"
        fi
        if [[ -n "$assignee" ]]; then
            jq --arg assignee "$assignee" '.[0].assignee = $assignee' "$state" >"$state.tmp"
            mv "$state.tmp" "$state"
        fi
        printf 'UPDATE %s status=%s assignee=%s\n' "$id" "$status" "$assignee" >>"$GC_TEST_ACTIONS"
        ;;
    "formula show")
        ;;
    *)
        ;;
esac
SH
    chmod +x "$TMP/bin/gc"
}

write_fixture() {
    local output=$1 agent=$2 formula=$3 rows=$4
    printf '%s\n' "$rows" |
        jq --arg agent "$agent" --arg formula "$formula" '
          map(. + {
            assignee: (.assignee // $agent),
            title: (.title // $formula),
            issue_type: (.issue_type // "molecule"),
            ephemeral: (.ephemeral // true),
            dependency_count: (.dependency_count // 0),
            dependent_count: (.dependent_count // 0)
          })' >"$output"
    jq -c '.[]' "$output" | while IFS= read -r row; do
        local id
        id=$(printf '%s\n' "$row" | jq -r '.id')
        printf '[%s]\n' "$row" >"$TMP/state/$id.json"
    done
}

run_block() {
    local script=$1 agent=$2 current=${3:-} query_fail=${4:-0}
    PATH="$TMP/bin:$PATH" \
        GC_AGENT="$agent" \
        GC_RIG=fixture \
        GC_BEAD_ID="$current" \
        GC_TEST_QUERY_FAIL="$query_fail" \
        GC_TEST_LOG="$TMP/calls.log" \
        GC_TEST_ACTIONS="$TMP/actions.log" \
        GC_TEST_STATE_DIR="$TMP/state" \
        bash "$script"
}

reset_fake_state() {
    : >"$TMP/calls.log"
    : >"$TMP/actions.log"
    rm -f "$TMP/state"/*.json
    rm -f "$TMP/state/.pour-count"
}

test_startup_reuses_existing_open_root() {
    local role agent formula script
    for role in deacon witness refinery; do
        reset_fake_state
        agent="fixture/gastown.$role"
        formula="mol-$role-patrol"
        script="$TMP/$role-startup.sh"
        extract_prompt_startup "$role" "$script"
        bash -n "$script"
        write_fixture "$TMP/query.json" "$agent" "$formula" \
            '[{"id":"wisp-existing","status":"open","created_at":"2026-07-23T00:00:00Z"},
              {"id":"wrong-title","status":"open","title":"mol-other-patrol"},
              {"id":"wrong-agent","status":"open","assignee":"fixture/someone-else"}]'
        run_block "$script" "$agent" >"$TMP/$role-startup.out"
        grep -F 'Resuming patrol wisp wisp-existing' "$TMP/$role-startup.out" >/dev/null ||
            fail "$role startup did not reuse the existing open ephemeral root"
        ! grep -E '^(POUR|BURN) ' "$TMP/actions.log" >/dev/null ||
            fail "$role startup poured or burned instead of reusing the sole existing root"
        grep -E '^UPDATE wisp-existing status=in_progress ' "$TMP/actions.log" >/dev/null ||
            fail "$role startup did not durably mark the reused root current"
    done
}

test_startup_adopts_unassigned_post_pour_root() {
    local role agent formula script
    for role in deacon witness refinery; do
        reset_fake_state
        agent="fixture/gastown.$role"
        formula="mol-$role-patrol"
        script="$TMP/$role-startup-unassigned.sh"
        extract_prompt_startup "$role" "$script"
        write_fixture "$TMP/query.json" "$agent" "$formula" \
            '[{"id":"post-pour-orphan","status":"open","assignee":"",
               "created_at":"2026-07-23T00:00:00Z"}]'

        run_block "$script" "$agent" >"$TMP/$role-startup-unassigned.out"
        ! grep -E '^(POUR|BURN) ' "$TMP/actions.log" >/dev/null ||
            fail "$role startup poured over or burned an unassigned post-pour root"
        grep -E '^UPDATE post-pour-orphan status=in_progress assignee=fixture/gastown\.'"$role"'$' \
            "$TMP/actions.log" >/dev/null ||
            fail "$role startup did not adopt the unassigned post-pour root"
    done
}

test_startup_burns_only_revalidated_empty_open_surplus() {
    local role=refinery
    local agent=fixture/gastown.refinery
    local formula=mol-refinery-patrol
    local script="$TMP/refinery-startup-surplus.sh"
    reset_fake_state
    extract_prompt_startup "$role" "$script"
    write_fixture "$TMP/query.json" "$agent" "$formula" \
        '[{"id":"current","status":"in_progress","created_at":"2026-07-23T00:00:00Z"},
          {"id":"empty-open","status":"open","created_at":"2026-07-23T00:01:00Z"},
          {"id":"materialized-open","status":"open","created_at":"2026-07-23T00:02:00Z","dependent_count":1},
          {"id":"other-active","status":"in_progress","created_at":"2026-07-23T00:03:00Z"}]'
    run_block "$script" "$agent" current >"$TMP/refinery-surplus.out"
    [[ $(grep -c '^BURN ' "$TMP/actions.log") -eq 1 ]] ||
        fail "startup cleanup must burn exactly one safe empty surplus: $(cat "$TMP/actions.log")"
    grep -Fx 'BURN empty-open' "$TMP/actions.log" >/dev/null ||
        fail "startup cleanup did not burn the revalidated empty/open surplus"
    ! grep -E 'BURN (materialized-open|other-active|current)' "$TMP/actions.log" >/dev/null ||
        fail "startup cleanup burned active/current/materialized work"
    grep -F 'Preserving non-empty or active surplus patrol root materialized-open' "$TMP/refinery-surplus.out" >/dev/null ||
        fail "startup cleanup must surface preserved materialized surplus"
    grep -F 'Preserving non-empty or active surplus patrol root other-active' "$TMP/refinery-surplus.out" >/dev/null ||
        fail "startup cleanup must surface preserved active surplus"
}

test_cycle_reuses_one_successor_and_preserves_unsafe_surplus() {
    local role agent formula script
    for role in deacon witness refinery; do
        reset_fake_state
        agent="fixture/gastown.$role"
        formula="mol-$role-patrol"
        script="$TMP/$role-handoff.sh"
        extract_formula_handoff "$role" "$script"
        bash -n "$script"
        write_fixture "$TMP/query.json" "$agent" "$formula" \
            '[{"id":"current","status":"in_progress","created_at":"2026-07-23T00:00:00Z"},
              {"id":"queued","status":"open","created_at":"2026-07-23T00:01:00Z"},
              {"id":"empty-extra","status":"open","created_at":"2026-07-23T00:02:00Z"},
              {"id":"materialized-extra","status":"open","created_at":"2026-07-23T00:03:00Z","dependency_count":1},
              {"id":"active-extra","status":"in_progress","created_at":"2026-07-23T00:04:00Z"}]'
        run_block "$script" "$agent" current >"$TMP/$role-handoff.out"
        ! grep -E '^POUR ' "$TMP/actions.log" >/dev/null ||
            fail "$role handoff poured despite an existing queued successor"
        grep -Fx 'BURN empty-extra' "$TMP/actions.log" >/dev/null ||
            fail "$role handoff did not clean the safe empty surplus"
        grep -Fx 'BURN current' "$TMP/actions.log" >/dev/null ||
            fail "$role handoff did not burn its resolved current root"
        ! grep -E 'BURN (queued|materialized-extra|active-extra)' "$TMP/actions.log" >/dev/null ||
            fail "$role handoff burned the queued, materialized, or active root"
        grep -E '^UPDATE queued status=in_progress ' "$TMP/actions.log" >/dev/null ||
            fail "$role handoff did not durably promote its queued successor"
    done
}

test_cycle_adopts_unassigned_post_pour_successor() {
    local role agent formula script
    for role in deacon witness refinery; do
        reset_fake_state
        agent="fixture/gastown.$role"
        formula="mol-$role-patrol"
        script="$TMP/$role-handoff-unassigned.sh"
        extract_formula_handoff "$role" "$script"
        write_fixture "$TMP/query.json" "$agent" "$formula" \
            '[{"id":"current","status":"in_progress",
               "created_at":"2026-07-23T00:00:00Z"},
              {"id":"post-pour-orphan","status":"open","assignee":"",
               "created_at":"2026-07-23T00:01:00Z"}]'

        run_block "$script" "$agent" current >"$TMP/$role-handoff-unassigned.out"
        ! grep -E '^POUR ' "$TMP/actions.log" >/dev/null ||
            fail "$role handoff poured over an unassigned post-pour successor"
        grep -E '^UPDATE post-pour-orphan status= assignee=fixture/gastown\.'"$role"'$' \
            "$TMP/actions.log" >/dev/null ||
            fail "$role handoff did not claim the unassigned successor before burning"
        grep -Fx 'BURN current' "$TMP/actions.log" >/dev/null ||
            fail "$role handoff did not burn the prior current root"
        grep -E '^UPDATE post-pour-orphan status=in_progress assignee=fixture/gastown\.'"$role"'$' \
            "$TMP/actions.log" >/dev/null ||
            fail "$role handoff did not promote the adopted successor"
    done
}

test_all_open_empty_bead_id_survives_immediate_handoffs() {
    # A controlled supervisor restart adopted the same provider sessions while
    # open patrol roots grew 6 -> 9; every root was open and GC_BEAD_ID empty.
    local role agent formula startup handoff
    for role in deacon witness refinery; do
        reset_fake_state
        agent="fixture/gastown.$role"
        formula="mol-$role-patrol"
        startup="$TMP/$role-all-open-startup.sh"
        handoff="$TMP/$role-all-open-handoff.sh"
        extract_prompt_startup "$role" "$startup"
        extract_formula_handoff "$role" "$handoff"
        write_fixture "$TMP/query.json" "$agent" "$formula" \
            '[{"id":"a-open","status":"open","created_at":"2026-07-23T00:00:00Z"},
              {"id":"z-open","status":"open","created_at":"2026-07-23T00:00:00Z"}]'

        run_block "$startup" "$agent" >"$TMP/$role-all-open-startup.out"
        [[ $(jq -r '.[0].status' "$TMP/state/a-open.json") == in_progress ]] ||
            fail "$role startup did not deterministically promote the ID-first all-open survivor"
        [[ $(jq -r '.[0].status' "$TMP/state/z-open.json") == closed ]] ||
            fail "$role startup did not safely close the empty all-open duplicate"

        # GC_BEAD_ID stays empty, matching the adopted persistent sessions seen
        # in production. The durable in_progress marker must resolve CURRENT.
        run_block "$handoff" "$agent" >"$TMP/$role-all-open-handoff-1.out"
        [[ $(jq -r '.[0].status' "$TMP/state/fixture-wisp-new-1.json") == in_progress ]] ||
            fail "$role handoff did not promote its first successor"

        # The same provider immediately re-reads the formula without a restart
        # or GC_BEAD_ID update. A second cycle must still resolve the successor.
        run_block "$handoff" "$agent" >"$TMP/$role-all-open-handoff-2.out"
        [[ $(jq -r '.[0].status' "$TMP/state/fixture-wisp-new-2.json") == in_progress ]] ||
            fail "$role immediate second cycle did not resolve/promote its successor"
        ! grep -F 'Could not resolve current' "$TMP/$role-all-open-handoff-1.out" \
            "$TMP/$role-all-open-handoff-2.out" >/dev/null ||
            fail "$role repeated the empty-GC_BEAD_ID current-root failure"
    done
}

test_query_failure_is_fail_closed() {
    local script="$TMP/witness-startup-query-failure.sh"
    reset_fake_state
    extract_prompt_startup witness "$script"
    printf '[]\n' >"$TMP/query.json"
    if PATH="$TMP/bin:$PATH" \
        GC_AGENT=fixture/gastown.witness \
        GC_RIG=fixture \
        GC_BEAD_ID= \
        GC_TEST_QUERY_FAIL=1 \
        GC_TEST_LOG="$TMP/calls.log" \
        GC_TEST_ACTIONS="$TMP/actions.log" \
        GC_TEST_STATE_DIR="$TMP/state" \
        bash "$script" >"$TMP/query-failure.out" 2>&1; then
        fail "startup must fail when the ephemeral query fails"
    fi
    [[ ! -s "$TMP/actions.log" ]] ||
        fail "query failure must not pour or burn: $(cat "$TMP/actions.log")"
}

test_formula_query_failure_preserves_provider_and_roots() {
    local role agent formula script
    for role in deacon witness refinery; do
        reset_fake_state
        agent="fixture/gastown.$role"
        formula="mol-$role-patrol"
        script="$TMP/$role-handoff-query-failure.sh"
        extract_formula_handoff "$role" "$script"
        write_fixture "$TMP/query.json" "$agent" "$formula" \
            '[{"id":"current","status":"in_progress","created_at":"2026-07-23T00:00:00Z"},
              {"id":"queued","status":"open","created_at":"2026-07-23T00:01:00Z"}]'
        if run_block "$script" "$agent" current 1 >"$TMP/$role-handoff-query-failure.out" 2>&1; then
            fail "$role formula handoff must fail when its ephemeral query fails"
        fi
        [[ ! -s "$TMP/actions.log" ]] ||
            fail "$role query failure must not drain, pour, burn, or update: $(cat "$TMP/actions.log")"
        [[ $(jq -r '.[0].status' "$TMP/state/current.json") == in_progress ]] ||
            fail "$role query failure mutated the current patrol root"
        [[ $(jq -r '.[0].status' "$TMP/state/queued.json") == open ]] ||
            fail "$role query failure mutated the queued patrol root"
    done
}

test_empty_agent_reconciliation_has_no_mutation() {
    local role formula script
    for role in deacon witness refinery; do
        reset_fake_state
        formula="mol-$role-patrol"
        script="$TMP/$role-handoff-empty-agent.sh"
        extract_formula_handoff "$role" "$script"
        write_fixture "$TMP/query.json" "" "$formula" \
            '[{"id":"current","status":"in_progress","created_at":"2026-07-23T00:00:00Z"},
              {"id":"queued","status":"open","created_at":"2026-07-23T00:01:00Z"}]'
        if run_block "$script" "" current >"$TMP/$role-handoff-empty-agent.out" 2>&1; then
            fail "$role formula handoff must reject an empty GC_AGENT"
        fi
        [[ ! -s "$TMP/actions.log" ]] ||
            fail "$role empty-agent failure must not drain, pour, burn, or update: $(cat "$TMP/actions.log")"
        [[ $(jq -r '.[0].status' "$TMP/state/current.json") == in_progress ]] ||
            fail "$role empty-agent failure mutated the current patrol root"
        [[ $(jq -r '.[0].status' "$TMP/state/queued.json") == open ]] ||
            fail "$role empty-agent failure mutated the queued patrol root"
    done
}

test_nonempty_current_root_is_never_burned() {
    local role agent formula script count_field rows
    for role in deacon witness refinery; do
        agent="fixture/gastown.$role"
        formula="mol-$role-patrol"
        script="$TMP/$role-handoff-nonempty-current.sh"
        extract_formula_handoff "$role" "$script"
        for count_field in dependency_count dependent_count; do
            reset_fake_state
            rows=$(jq -nc --arg field "$count_field" '
              [{id:"current",status:"in_progress",created_at:"2026-07-23T00:00:00Z"},
               {id:"queued",status:"open",created_at:"2026-07-23T00:01:00Z"}]
              | .[0][$field] = 1')
            write_fixture "$TMP/query.json" "$agent" "$formula" "$rows"
            if run_block "$script" "$agent" current \
                >"$TMP/$role-handoff-$count_field.out" 2>&1; then
                fail "$role formula handoff must fail closed for nonzero current $count_field"
            fi
            [[ ! -s "$TMP/actions.log" ]] ||
                fail "$role nonzero current $count_field must not drain, pour, burn, or update: $(cat "$TMP/actions.log")"
            [[ $(jq -r '.[0].status' "$TMP/state/current.json") == in_progress ]] ||
                fail "$role nonzero current $count_field was burned"
            [[ $(jq -r '.[0].status' "$TMP/state/queued.json") == open ]] ||
                fail "$role nonzero current $count_field promoted the successor"
        done
    done
}

make_fake_gc
test_assets_use_ephemeral_query_contract
test_startup_reuses_existing_open_root
test_startup_adopts_unassigned_post_pour_root
test_startup_burns_only_revalidated_empty_open_surplus
test_cycle_reuses_one_successor_and_preserves_unsafe_surplus
test_cycle_adopts_unassigned_post_pour_successor
test_all_open_empty_bead_id_survives_immediate_handoffs
test_query_failure_is_fail_closed
test_formula_query_failure_preserves_provider_and_roots
test_empty_agent_reconciliation_has_no_mutation
test_nonempty_current_root_is_never_burned

echo "patrol wisp reconciliation tests passed"
