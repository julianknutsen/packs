#!/bin/sh
# gc <binding> claim — atomically claim and verify one routed work bead.

set -eu

acknowledge_drain() {
    if ! command -v gc >/dev/null 2>&1; then
        return 1
    fi
    gc runtime drain-ack >/dev/null 2>&1
}

acknowledge_drain_or_report() {
    if ! acknowledge_drain; then
        echo "DRAIN_ACK_FAILED gc runtime drain-ack did not complete" >&2
        return 1
    fi
}

if [ -z "${GC_PACK_DIR:-}" ]; then
    echo "CONFIG_REJECTED gc gascity claim: missing Gas City pack context" >&2
    acknowledge_drain_or_report || true
    exit 1
fi

while [ "$#" -gt 0 ]; do
    case "$1" in
        gc|gascity|claim|--city=*|--rig=*)
            shift
            ;;
        --city|--rig)
            if [ "$#" -lt 2 ]; then
                echo "gc ${GC_PACK_NAME:-gascity} claim: missing value for $1" >&2
                exit 2
            fi
            shift 2
            ;;
        *)
            break
            ;;
    esac
done

if [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ]; then
    cat "$GC_PACK_DIR/commands/claim/help.md"
    exit 0
fi

if [ "$#" -ne 0 ]; then
    echo "gc ${GC_PACK_NAME:-gascity} claim: no arguments accepted" >&2
    exit 2
fi

if ! command -v gc >/dev/null 2>&1; then
    echo "CONFIG_REJECTED gc ${GC_PACK_NAME:-gascity} claim: gc binary not in PATH" >&2
    exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
    echo "CONFIG_REJECTED gc ${GC_PACK_NAME:-gascity} claim: python3 not in PATH" >&2
    acknowledge_drain_or_report || true
    exit 1
fi

json_pick() {
    python3 -c '
import json
import sys

path = sys.argv[1]
try:
    data = json.load(sys.stdin)
except Exception:
    print("")
    raise SystemExit(0)

if isinstance(data, list):
    data = data[0] if data else {}
if not isinstance(data, dict):
    print("")
    raise SystemExit(0)

if path.startswith("metadata:"):
    metadata = data.get("metadata") or {}
    value = metadata.get(path.split(":", 1)[1], "") if isinstance(metadata, dict) else ""
else:
    value = data.get(path, "")

if value is None:
    value = ""
print(value if isinstance(value, str) else str(value))
' "$1"
}

json_child_ids() {
    python3 -c '
import json
import sys

try:
    data = json.load(sys.stdin)
except Exception:
    raise SystemExit(0)

children = data.get("children", []) if isinstance(data, dict) else []
for child in children:
    if isinstance(child, dict) and isinstance(child.get("id"), str) and child["id"]:
        print(child["id"])
'
}

EXPECTED_ASSIGNEE="${BEADS_ACTOR:-${GC_SESSION_NAME:-${GC_SESSION_ID:-${GC_AGENT:-}}}}"
EXPECTED_ROUTE="${GC_TEMPLATE:-${GC_AGENT:-}}"

if [ -z "$EXPECTED_ASSIGNEE" ]; then
    echo "CONFIG_REJECTED gc ${GC_PACK_NAME:-gascity} claim: missing expected assignee" >&2
    acknowledge_drain_or_report || true
    exit 1
fi

# The claim hook writes the OCCUPANT identity (the session bead id) for
# unaliased pool workers and the alias for aliased ones; older binaries wrote
# the slot-derived session name. All of these are OUR identities, so verify
# membership in the session's own identity set instead of insisting on one
# precomputed form -- a strict single-form compare rejected the session's own
# claim forever after the writer moved to the occupant id (gascity ga-jrnou).
claim_assignee_is_ours() {
    _candidate="$1"
    for _own in "${BEADS_ACTOR:-}" "${GC_ALIAS:-}" "${GC_SESSION_ID:-}" "${GC_SESSION_NAME:-}" "${GC_AGENT:-}"; do
        if [ -n "$_own" ] && [ "$_candidate" = "$_own" ]; then
            return 0
        fi
    done
    return 1
}

claim_file="$(mktemp)"
show_file="$(mktemp)"
convoy_file="$(mktemp)"
member_file="$(mktemp)"
err_file="$(mktemp)"
cleanup() {
    rm -f "$claim_file" "$show_file" "$convoy_file" "$member_file" "$err_file"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

max_attempts=3
claim_try=0
work_id=""
while [ "$claim_try" -lt "$max_attempts" ]; do
    claim_try=$((claim_try + 1))
    if gc hook --claim --drain-ack --json >"$claim_file" 2>"$err_file"; then
        claim_code=0
    else
        claim_code=$?
    fi

    claim_action="$(json_pick action <"$claim_file")"
    work_id="$(json_pick bead_id <"$claim_file")"
    claim_assignee="$(json_pick assignee <"$claim_file")"
    claim_route="$(json_pick route <"$claim_file")"

    if [ "$claim_code" -eq 0 ] && [ "$claim_action" = "drain" ]; then
        cat "$claim_file"
        exit 0
    fi

    if [ "$claim_code" -eq 0 ] && [ "$claim_action" = "work" ] && [ -n "$work_id" ]; then
        break
    fi

    work_id=""
    if [ -s "$err_file" ]; then
        printf 'CLAIM_RETRY %s/%s gc hook --claim failed: %s\n' \
            "$claim_try" "$max_attempts" "$(sed -n '1p' "$err_file")" >&2
    else
        printf 'CLAIM_RETRY %s/%s unexpected gc hook --claim result\n' \
            "$claim_try" "$max_attempts" >&2
    fi
    if [ "$claim_try" -lt "$max_attempts" ]; then
        sleep 2
    fi
done

if [ -z "$work_id" ]; then
    printf 'CLAIM_REJECTED gc hook --claim returned no workable bead after %s attempts\n' \
        "$max_attempts" >&2
    exit 1
fi

hook_assignee="$claim_assignee"
hook_route="$claim_route"
verified=0
verify_try=0
while [ "$verify_try" -lt "$max_attempts" ]; do
    verify_try=$((verify_try + 1))
    if ! gc bd show "$work_id" --json >"$show_file" 2>"$err_file"; then
        if [ -s "$err_file" ]; then
            printf 'CLAIM_RETRY %s/%s bead read failed for %s: %s\n' \
                "$verify_try" "$max_attempts" "$work_id" "$(sed -n '1p' "$err_file")" >&2
        else
            printf 'CLAIM_RETRY %s/%s bead read failed for %s\n' \
                "$verify_try" "$max_attempts" "$work_id" >&2
        fi
    else
        claim_id="$(json_pick id <"$show_file")"
        claim_status="$(json_pick status <"$show_file")"
        show_assignee="$(json_pick assignee <"$show_file")"
        show_route="$(json_pick metadata:gc.routed_to <"$show_file")"
        claim_assignee="$hook_assignee"
        claim_route="$hook_route"
        [ -n "$show_assignee" ] && claim_assignee="$show_assignee"
        [ -n "$show_route" ] && claim_route="$show_route"

        if [ -z "$claim_id" ] || [ -z "$claim_status" ] || [ -z "$claim_assignee" ]; then
            printf 'CLAIM_RETRY %s/%s incomplete bead record for %s\n' \
                "$verify_try" "$max_attempts" "$work_id" >&2
        elif [ "$claim_id" != "$work_id" ]; then
            printf 'CLAIM_REJECTED verification failed for %s\n' "$work_id" >&2
            break
        elif [ "$claim_status" != "open" ] && [ "$claim_status" != "in_progress" ]; then
            printf 'CLAIM_REJECTED unexpected status for %s: %s\n' \
                "$work_id" "$claim_status" >&2
            break
        elif ! claim_assignee_is_ours "$claim_assignee"; then
            printf 'CLAIM_REJECTED assignee mismatch for %s\n' "$work_id" >&2
            break
        elif [ -n "$EXPECTED_ROUTE" ] && [ -n "$claim_route" ] && [ "$claim_route" != "$EXPECTED_ROUTE" ]; then
            printf 'CLAIM_REJECTED route mismatch for %s\n' "$work_id" >&2
            break
        else
            verified=1
            break
        fi
    fi

    if [ "$verify_try" -lt "$max_attempts" ]; then
        sleep 1
    fi
done

if [ "$verified" -ne 1 ]; then
    printf 'CLAIM_REJECTED verification failed for %s after %s attempts\n' \
        "$work_id" "$verify_try" >&2
    exit 1
fi

restore_explicit_run_pointer() {
    run_id="${GASWORKS_RUN_ID:-}"
    session_id="${GC_SESSION_ID:-}"
    if [ -z "$run_id" ] || [ -z "$session_id" ]; then
        return
    fi
    if ! gc bd update "$session_id" --set-metadata "gc.current_run_id=$run_id" \
        >/dev/null 2>"$err_file"; then
        printf 'RUN_POINTER_FAILED could not restore explicit run %s on session %s: %s\n' \
            "$run_id" "$session_id" "$(sed -n '1p' "$err_file")" >&2
    fi
}

declare_source_file() {
    source_file="$1"
    source_store="$(json_pick metadata:gc.source_store_ref <"$source_file")"
    source_bead="$(json_pick metadata:gc.source_bead_id <"$source_file")"
    if [ "$source_store" != "city:" ]; then
        return
    fi
    if [ -z "$source_bead" ]; then
        echo "WORK_REF_SKIPPED city source is missing gc.source_bead_id" >&2
        return
    fi
    if ! "$observer_bin" declare-work \
        -beads-project "$beads_project" -work-item "$source_bead" \
        >/dev/null 2>"$err_file"; then
        printf 'WORK_REF_FAILED could not declare %s/%s: %s\n' \
            "$beads_project" "$source_bead" "$(sed -n '1p' "$err_file")" >&2
    fi
}

declare_explicit_work() {
    if [ -z "${GASWORKS_RUN_ID:-}" ]; then
        return
    fi
    beads_project="${GC_BEADS_PROJECT_ID:-}"
    if [ -z "$beads_project" ]; then
        echo "WORK_REF_SKIPPED explicit run has no GC_BEADS_PROJECT_ID" >&2
        return
    fi
    observer_bin="${GASWORKS_OBSERVER_BIN:-gasworks-observer}"
    if ! command -v "$observer_bin" >/dev/null 2>&1; then
        printf 'WORK_REF_SKIPPED observer binary not found: %s\n' "$observer_bin" >&2
        return
    fi

    declare_source_file "$show_file"

    input_convoy="$(json_pick metadata:gc.input_convoy_id <"$show_file")"
    if [ -z "$input_convoy" ]; then
        return
    fi
    if ! gc convoy status "$input_convoy" --json >"$convoy_file" 2>"$err_file"; then
        printf 'WORK_REF_FAILED could not read input convoy %s: %s\n' \
            "$input_convoy" "$(sed -n '1p' "$err_file")" >&2
        return
    fi
    json_child_ids <"$convoy_file" | while IFS= read -r member_id; do
        if ! gc bd show "$member_id" --json >"$member_file" 2>"$err_file"; then
            printf 'WORK_REF_FAILED could not read convoy member %s: %s\n' \
                "$member_id" "$(sed -n '1p' "$err_file")" >&2
            continue
        fi
        declare_source_file "$member_file"
    done
}

restore_explicit_run_pointer
declare_explicit_work

python3 - "$show_file" <<'PY'
import json
import sys

bead = json.load(open(sys.argv[1], encoding="utf-8"))
if isinstance(bead, list):
    bead = bead[0] if bead else {}
metadata = bead.get("metadata") or {}
if not isinstance(metadata, dict):
    metadata = {}
print(json.dumps({
    "action": "work",
    "bead_id": bead["id"],
    "root_bead_id": metadata.get("gc.root_bead_id", ""),
    "continuation_group": metadata.get("gc.continuation_group", ""),
    "bead": bead,
}, separators=(",", ":")))
PY
