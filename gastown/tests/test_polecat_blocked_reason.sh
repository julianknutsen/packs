#!/usr/bin/env bash
# The polecat's escalation stop must record blocked_reason on the bead before it
# flips status, and must refuse to flip at all if it cannot. This suite extracts
# the shipped POLECAT_BLOCKED_CONTRACT region from the prompt and executes it,
# so it tests the real artifact rather than prose about it.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
PROMPT="$ROOT/gastown/agents/polecat/prompt.template.md"

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

# Extracts the contract verbatim. WITNESS_TARGET is set by the surrounding
# prompt snippet (it carries a {{ .BindingPrefix }} template var), so the tests
# supply it from the environment the way the rendered prompt would.
extract_contract() {
    sed -n '/# BEGIN POLECAT_BLOCKED_CONTRACT/,/# END POLECAT_BLOCKED_CONTRACT/p' "$PROMPT" >"$1"
    [[ -s "$1" ]] || fail "POLECAT_BLOCKED_CONTRACT region not found in $PROMPT"
    grep -q 'blocked_reason' "$1" || fail "extracted contract does not mention blocked_reason"
}

# Logs every gc invocation in order to $GC_CALLS. The update operation persists
# blocked_reason only when GC_STUB_PERSIST=1, and status only when
# GC_STUB_STATUS_PERSIST=1. The stub also models append-notes so the suite can
# prove that an existing task record is preserved rather than overwritten.
write_gc_stub() {
    local bin="$1"
    mkdir -p "$bin"
    cat >"$bin/gc" <<'SH'
#!/usr/bin/env bash
noun="${1:-}"
verb="${2:-}"
{
    printf 'CALL:%s:%s' "$noun" "$verb"
    for arg in "${@:3}"; do printf ' %s' "$arg"; done
    printf '\n'
} >>"$GC_CALLS"
if [[ "$noun" == "bd" && "$verb" == "update" ]]; then
    for arg in "$@"; do
        case "$arg" in
            blocked_reason=*)
                [[ "${GC_STUB_PERSIST:-1}" == "1" ]] &&
                    printf '%s' "${arg#blocked_reason=}" >"$GC_STATE/reason"
                ;;
            --append-notes)
                append_next=1
                ;;
            --status=blocked)
                if [[ "${GC_STUB_STATUS_FAIL:-0}" == "1" ]]; then
                    exit 1
                fi
                [[ "${GC_STUB_STATUS_PERSIST:-1}" == "1" ]] &&
                    printf '%s' "${arg#--status=}" >"$GC_STATE/status"
                ;;
            --status=*)
                echo "unsupported status: ${arg#--status=}" >&2
                exit 2
                ;;
            *)
                if [[ "${append_next:-0}" == "1" ]]; then
                    printf '\n%s' "$arg" >>"$GC_STATE/notes"
                    append_next=0
                fi
                ;;
        esac
    done
elif [[ "$noun" == "bd" && "$verb" == "show" ]]; then
    reason=""
    status=""
    [[ -f "$GC_STATE/reason" ]] && reason="$(cat "$GC_STATE/reason")"
    [[ -f "$GC_STATE/status" ]] && status="$(cat "$GC_STATE/status")"
    jq -n --arg r "$reason" --arg s "$status" \
        '[{status:$s,metadata:(if $r == "" then {} else {blocked_reason:$r} end)}]'
elif [[ "$noun" == "mail" && "$verb" == "send" ]]; then
    [[ "${GC_STUB_MAIL_FAIL:-0}" == "1" ]] && exit 1
elif [[ "$noun" == "runtime" && "$verb" == "drain-ack" ]]; then
    [[ "${GC_STUB_DRAIN_FAIL:-0}" == "1" ]] && exit 1
fi
exit 0
SH
    chmod +x "$bin/gc"
}

# Runs the shipped contract against the stub. Echoes the exit code; the call log
# and persisted state land in $GC_CALLS / $GC_STATE for the caller to assert on.
run_contract() {
    local bead="$1" reason="$2" persist="$3" status_persist="${4:-1}" status_fail="${5:-0}"
    local drain_fail="${6:-0}" mail_fail="${7:-0}" rc=0
    BLOCKED_BEAD="$bead" BLOCKED_REASON="$reason" \
        WITNESS_TARGET="helm/witness" GC_STUB_PERSIST="$persist" \
        GC_STUB_STATUS_PERSIST="$status_persist" GC_STUB_STATUS_FAIL="$status_fail" \
        GC_STUB_DRAIN_FAIL="$drain_fail" GC_STUB_MAIL_FAIL="$mail_fail" \
        GC_CALLS="$GC_CALLS" GC_STATE="$GC_STATE" PATH="$BIN:$PATH" \
        bash "$CONTRACT" >"$OUT" 2>&1 || rc=$?
    echo "$rc"
}

setup() {
    TMP=$(mktemp -d)
    BIN="$TMP/bin"
    GC_STATE="$TMP/state"
    GC_CALLS="$TMP/calls"
    CONTRACT="$TMP/contract.sh"
    OUT="$TMP/out"
    mkdir -p "$BIN" "$GC_STATE"
    : >"$GC_CALLS"
    printf '%s' "existing task notes" >"$GC_STATE/notes"
    write_gc_stub "$BIN"
    extract_contract "$CONTRACT"
}

# A refusal must be a no-op. Not "mostly a no-op" — the bead must not be
# touched at all, so the contract may not issue a single gc command.
test_missing_reason_or_bead_is_refused_and_issues_no_gc_command() {
    local rc
    for case_ in "ki-0aq|" "|a real reason"; do
        setup
        rc=$(run_contract "${case_%%|*}" "${case_#*|}" 1)
        [[ "$rc" == "2" ]] || fail "refusal for [$case_] exited $rc, want 2"
        [[ ! -s "$GC_CALLS" ]] ||
            fail "refusal for [$case_] issued gc commands: $(tr '\n' ';' <"$GC_CALLS")"
        grep -q 'BLOCK_REFUSED' "$OUT" || fail "refusal for [$case_] printed no BLOCK_REFUSED"
    done
}

# The whole point of the read-back: a write that does not stick must not be
# allowed to become a silently blocked bead.
test_unpersisted_reason_never_flips_status() {
    setup
    local rc
    rc=$(run_contract "ki-0aq" "needs an AWS profile that this worktree has no access to" 0)

    [[ "$rc" == "1" ]] || fail "unpersisted reason exited $rc, want 1"
    ! grep -q -- '--status=' "$GC_CALLS" ||
        fail "status was flipped despite the reason not persisting"
    [[ ! -f "$GC_STATE/status" ]] || fail "bead status changed despite the reason not persisting"
    [[ "$(grep -c '^CALL:mail:send' "$GC_CALLS")" == "1" ]] ||
        fail "want exactly one witness escalation, got $(grep -c '^CALL:mail:send' "$GC_CALLS")"
    ! grep -q '^CALL:runtime:drain-ack' "$GC_CALLS" ||
        fail "drain-acked after refusing to block — reports idle and hides the failure"
}

# Ordering is the contract. The reason must be on the bead before the flip is
# even attempted, with the read-back in between.
test_recorded_path_writes_reason_strictly_before_the_flip() {
    setup
    local rc reason_line show_line status_line
    rc=$(run_contract "ki-0aq" "upstream API returns 501; needs the v2 endpoint enabled" 1)

    [[ "$rc" == "0" ]] || fail "recorded path exited $rc, want 0: $(cat "$OUT")"
    reason_line=$(grep -n -- 'blocked_reason=' "$GC_CALLS" | head -1 | cut -d: -f1)
    show_line=$(grep -n '^CALL:bd:show' "$GC_CALLS" | head -1 | cut -d: -f1)
    status_line=$(grep -n -- '--status=blocked' "$GC_CALLS" | head -1 | cut -d: -f1)

    [[ -n "$reason_line" ]] || fail "the reason was never written"
    [[ -n "$status_line" ]] || fail "the status was never flipped"
    (( reason_line < show_line )) || fail "read-back ($show_line) preceded the write ($reason_line)"
    (( show_line < status_line )) ||
        fail "status flip ($status_line) was not gated on the read-back ($show_line)"
    [[ "$(cat "$GC_STATE/status")" == "blocked" ]] || fail "bead did not reach status=blocked"
    grep -q '^existing task notes$' "$GC_STATE/notes" ||
        fail "the blocked record replaced pre-existing task notes"
    grep -q '^BLOCKED: upstream API returns 501' "$GC_STATE/notes" ||
        fail "the blocked record was not appended to task notes"
    grep -q '^CALL:runtime:drain-ack' "$GC_CALLS" || fail "recorded path did not drain-ack"
    [[ "$(grep -c '^CALL:mail:send' "$GC_CALLS")" == "1" ]] ||
        fail "recorded path did not use exactly one witness mail"
}

test_failed_or_unpersisted_status_never_drain_acks() {
    local rc
    for mode in fail unpersisted; do
        setup
        if [[ "$mode" == "fail" ]]; then
            rc=$(run_contract "ki-0aq" "needs operator credentials" 1 1 1)
        else
            rc=$(run_contract "ki-0aq" "needs operator credentials" 1 0 0)
        fi
        [[ "$rc" == "1" ]] || fail "$mode status transition exited $rc, want 1"
        ! grep -q '^CALL:runtime:drain-ack' "$GC_CALLS" ||
            fail "$mode status transition drain-acked"
        [[ "$(grep -c '^CALL:mail:send' "$GC_CALLS")" == "1" ]] ||
            fail "$mode status transition did not escalate exactly once"
    done
}

test_failed_witness_mail_or_drain_ack_never_reports_success() {
    local rc

    setup
    rc=$(run_contract "ki-0aq" "needs operator credentials" 1 1 0 0 1)
    [[ "$rc" == "1" ]] || fail "failed witness mail exited $rc, want 1"
    ! grep -q '^CALL:runtime:drain-ack' "$GC_CALLS" ||
        fail "drain-acked after witness mail failed"
    [[ "$(grep -c '^CALL:mail:send' "$GC_CALLS")" == "1" ]] ||
        fail "failed witness mail was retried"

    setup
    rc=$(run_contract "ki-0aq" "needs operator credentials" 1 1 0 1)
    [[ "$rc" == "1" ]] || fail "failed drain acknowledgement exited $rc, want 1"
    [[ "$(grep -c '^CALL:runtime:drain-ack' "$GC_CALLS")" == "1" ]] ||
        fail "failed drain acknowledgement was not attempted exactly once"
    [[ "$(grep -c '^CALL:mail:send' "$GC_CALLS")" == "1" ]] ||
        fail "drain failure exceeded the one-mail budget"
}

test_missing_reason_or_bead_is_refused_and_issues_no_gc_command
test_unpersisted_reason_never_flips_status
test_recorded_path_writes_reason_strictly_before_the_flip
test_failed_or_unpersisted_status_never_drain_acks
test_failed_witness_mail_or_drain_ack_never_reports_success

echo "polecat blocked-reason contract tests passed"
