#!/bin/sh
# Idempotently retire a Gastown task artifact after a remotely durable
# terminal handoff.
#
# Usage: task-artifact-cleanup.sh [work-bead-id]
#
# Required environment:
#   GC_CITY_PATH  physical Gas City root
#   GC_RIG        rig name
#   GC_RIG_ROOT   canonical rig repository

set -eu

if [ "$#" -gt 1 ]; then
    echo "usage: task-artifact-cleanup.sh [work-bead-id]" >&2
    exit 2
fi
WORK=${1:-}

if [ -z "${GC_CITY_PATH:-}" ] || [ -z "${GC_RIG:-}" ] ||
   [ -z "${GC_RIG_ROOT:-}" ]; then
    echo "ARTIFACT_CLEANUP_BLOCKED GC_CITY_PATH, GC_RIG, and GC_RIG_ROOT are required" >&2
    exit 2
fi
case "$GC_RIG" in
    "."|".."|*[!A-Za-z0-9._-]*)
        echo "ARTIFACT_CLEANUP_BLOCKED unsafe rig name: $GC_RIG" >&2
        exit 2
        ;;
esac

for required_command in gc git jq; do
    if ! command -v "$required_command" >/dev/null 2>&1; then
        echo "ARTIFACT_CLEANUP_BLOCKED missing command: $required_command" >&2
        exit 2
    fi
done

if [ -z "$WORK" ]; then
    PENDING_JSON=$(gc bd --rig "$GC_RIG" list \
        --status=closed \
        --has-metadata-key=artifact_cleanup_state \
        --limit=0 \
        --json) || {
        echo "ARTIFACT_CLEANUP_BLOCKED could not list pending cleanup work" >&2
        exit 1
    }
    WORK=$(printf '%s' "$PENDING_JSON" | jq -r '
        if type != "array" then
            error("expected a work bead array")
        else
            [
              .[]
              | select(type == "object")
              | select(((.metadata // {}) | type) == "object")
              | select(.metadata.artifact_cleanup_state == "pending")
              | select(
                  .metadata.merge_result == "merged" or
                  .metadata.merge_result == "already_merged" or
                  .metadata.merge_result == "pull_request" or
                  .metadata.merge_result == "mr_merged"
                )
            ]
            | sort_by(.updated_at // "")
            | .[0].id // empty
        end
    ') || {
        echo "ARTIFACT_CLEANUP_BLOCKED invalid pending cleanup response" >&2
        exit 1
    }
    if [ -z "$WORK" ]; then
        echo "ARTIFACT_CLEANUP_IDLE no closed pending artifacts"
        exit 0
    fi
fi

case "$WORK" in
    "."|".."|*[!A-Za-z0-9._-]*)
        echo "ARTIFACT_CLEANUP_BLOCKED unsafe work bead id: $WORK" >&2
        exit 2
        ;;
esac

CITY_ROOT=$(CDPATH= cd -- "$GC_CITY_PATH" 2>/dev/null && pwd -P) || {
    echo "ARTIFACT_CLEANUP_BLOCKED city root is missing: $GC_CITY_PATH" >&2
    exit 2
}
RIG_ROOT=$(CDPATH= cd -- "$GC_RIG_ROOT" 2>/dev/null && pwd -P) || {
    echo "ARTIFACT_CLEANUP_BLOCKED rig root is missing: $GC_RIG_ROOT" >&2
    exit 2
}
RIG_NAMESPACE="$CITY_ROOT/.gc/worktrees/$GC_RIG"
RIG_NAMESPACE_REAL=$(CDPATH= cd -- "$RIG_NAMESPACE" 2>/dev/null && pwd -P) || {
    echo "ARTIFACT_CLEANUP_BLOCKED rig worktree namespace is missing: $RIG_NAMESPACE" >&2
    exit 2
}
if [ "$RIG_NAMESPACE_REAL" != "$RIG_NAMESPACE" ]; then
    echo "ARTIFACT_CLEANUP_BLOCKED rig worktree namespace is redirected: $RIG_NAMESPACE" >&2
    exit 2
fi

artifact_git_common_dir() {
    repo_dir=$1
    common_dir=$(git -C "$repo_dir" rev-parse \
        --path-format=absolute --git-common-dir 2>/dev/null) || return 1
    (CDPATH= cd -- "$common_dir" 2>/dev/null && pwd -P)
}

valid_object_id() {
    object_id=$1
    case "$object_id" in
        ""|*[!0-9a-fA-F]*) return 1 ;;
    esac
    [ "${#object_id}" -ge 40 ] && [ "${#object_id}" -le 64 ]
}

valid_branch() {
    [ -n "$1" ] &&
        git -C "$RIG_ROOT" check-ref-format "refs/heads/$1" >/dev/null 2>&1
}

task_artifact_path_shape() {
    shaped_candidate=$1
    shaped_bead=$2
    shaped_canonical="$RIG_NAMESPACE_REAL/artifacts/worktrees/$shaped_bead"
    shaped_worktrees_parent=
    shaped_provider_home=
    shaped_provider_root=
    shaped_provider_name=

    [ "$(basename -- "$shaped_candidate")" = "$shaped_bead" ] || return 1
    [ "$(basename -- "$(dirname -- "$shaped_candidate")")" = worktrees ] ||
        return 1
    if [ "$shaped_candidate" = "$shaped_canonical" ]; then
        return 0
    fi

    shaped_worktrees_parent=$(dirname -- "$shaped_candidate")
    shaped_provider_home=$(dirname -- "$shaped_worktrees_parent")
    shaped_provider_root=$(dirname -- "$shaped_provider_home")
    shaped_provider_name=$(basename -- "$shaped_provider_home")
    [ "$shaped_provider_root" = "$RIG_NAMESPACE_REAL/polecats" ] || return 1
    case "$shaped_provider_name" in
        ""|"."|".."|*[!A-Za-z0-9._-]*) return 1 ;;
    esac
}

validate_task_artifact() {
    candidate=$1
    bead_id=$2
    candidate_real=
    candidate_top=
    candidate_common=
    rig_common=

    [ -n "$candidate" ] && [ -d "$candidate" ] || return 1
    candidate_real=$(CDPATH= cd -- "$candidate" 2>/dev/null && pwd -P) ||
        return 1
    task_artifact_path_shape "$candidate_real" "$bead_id" || return 1

    candidate_top=$(git -C "$candidate_real" rev-parse \
        --show-toplevel 2>/dev/null) || return 1
    candidate_top=$(CDPATH= cd -- "$candidate_top" 2>/dev/null && pwd -P) ||
        return 1
    [ "$candidate_top" = "$candidate_real" ] || return 1
    candidate_common=$(artifact_git_common_dir "$candidate_real") || return 1
    rig_common=$(artifact_git_common_dir "$RIG_ROOT") || return 1
    [ "$candidate_common" = "$rig_common" ] || return 1

    printf '%s\n' "$candidate_real"
}

bd_show_work() {
    gc bd --rig "$GC_RIG" show "$WORK" --json
}

bd_update_work() {
    gc bd --rig "$GC_RIG" update "$WORK" "$@"
}

read_work_record() {
    current_json=$(bd_show_work) || return 1
    printf '%s' "$current_json" | jq -ceS --arg work "$WORK" '
        if type == "array" and length == 1 and (.[0] | type) == "object" and
           (.[0].id == $work) and
           ((.[0].metadata // {}) | type) == "object"
        then .[0]
        else error("expected exactly the requested work bead object")
        end
    '
}

record_cleanup_state() {
    state=$1
    bd_update_work --set-metadata artifact_cleanup_state="$state"
}

complete_without_path() {
    bd_update_work \
        --unset-metadata artifact_dir \
        --unset-metadata work_dir \
        --set-metadata artifact_cleanup_state=complete
    echo "ARTIFACT_CLEANUP_COMPLETE work=$WORK artifact=absent"
}

WORK_RECORD=$(read_work_record) || {
    echo "ARTIFACT_CLEANUP_BLOCKED invalid work bead response for $WORK" >&2
    exit 1
}

STATUS=$(printf '%s' "$WORK_RECORD" | jq -r '.status // empty')
META=$(printf '%s' "$WORK_RECORD" | jq -c '.metadata // {}')
HANDOFF_RESULT=$(printf '%s' "$META" | jq -r '.merge_result // empty')
CLEANUP_STATE=$(printf '%s' "$META" | jq -r '.artifact_cleanup_state // empty')
EXPECTED_ARTIFACT_SHA=$(printf '%s' "$META" |
    jq -r '.artifact_source_sha // empty')
BRANCH=$(printf '%s' "$META" | jq -r '.branch // empty')
TARGET=$(printf '%s' "$META" | jq -r '.merged_target // empty')
MERGED_SHA=$(printf '%s' "$META" | jq -r '.merged_sha // empty')
PR_HEAD_SHA=$(printf '%s' "$META" | jq -r '.pr_head_sha // empty')
TASK_ARTIFACT=$(printf '%s' "$META" | jq -r '
    if ((.artifact_dir // "") | length) > 0
    then .artifact_dir
    else (.work_dir // empty)
    end')

TERMINAL=false
if [ "$STATUS" = closed ]; then
    case "$HANDOFF_RESULT" in
        merged|already_merged|pull_request|mr_merged)
            TERMINAL=true
            ;;
    esac
fi

if [ "$TERMINAL" != true ]; then
    if [ "$CLEANUP_STATE" != pending ]; then
        record_cleanup_state pending || {
            echo "ARTIFACT_CLEANUP_BLOCKED could not record pending state for $WORK" >&2
            exit 1
        }
    fi
    echo "ARTIFACT_CLEANUP_DEFERRED work=$WORK status=${STATUS:-unknown} result=${HANDOFF_RESULT:-unknown}"
    exit 0
fi

if [ -z "$TASK_ARTIFACT" ]; then
    if [ "$CLEANUP_STATE" = complete ]; then
        echo "ARTIFACT_CLEANUP_COMPLETE work=$WORK artifact=absent"
        exit 0
    fi
    complete_without_path
    exit 0
fi

if [ ! -e "$TASK_ARTIFACT" ]; then
    # Crash-safe retry: removal may have succeeded before its metadata update.
    if ! task_artifact_path_shape "$TASK_ARTIFACT" "$WORK"; then
        record_cleanup_state blocked || true
        echo "ARTIFACT_CLEANUP_BLOCKED missing artifact path has an unsafe layout for $WORK: $TASK_ARTIFACT" >&2
        exit 1
    fi
    complete_without_path
    exit 0
fi

SAFE_ARTIFACT=$(validate_task_artifact "$TASK_ARTIFACT" "$WORK") || {
    record_cleanup_state blocked || true
    echo "ARTIFACT_CLEANUP_BLOCKED unsafe artifact path for $WORK: $TASK_ARTIFACT" >&2
    exit 1
}
if [ -z "$EXPECTED_ARTIFACT_SHA" ]; then
    record_cleanup_state blocked || true
    echo "ARTIFACT_CLEANUP_BLOCKED missing artifact_source_sha for $WORK" >&2
    exit 1
fi
LOCAL_SHA=$(git -C "$SAFE_ARTIFACT" rev-parse HEAD 2>/dev/null) || {
    record_cleanup_state blocked || true
    echo "ARTIFACT_CLEANUP_BLOCKED unreadable artifact HEAD for $WORK" >&2
    exit 1
}
if [ "$LOCAL_SHA" != "$EXPECTED_ARTIFACT_SHA" ]; then
    record_cleanup_state blocked || true
    echo "ARTIFACT_CLEANUP_BLOCKED artifact HEAD mismatch for $WORK" >&2
    exit 1
fi
ARTIFACT_STATUS=$(git -C "$SAFE_ARTIFACT" status \
    --porcelain --untracked-files=all) || {
    record_cleanup_state blocked || true
    echo "ARTIFACT_CLEANUP_BLOCKED could not inspect artifact status for $WORK" >&2
    exit 1
}
if [ -n "$ARTIFACT_STATUS" ]; then
    record_cleanup_state blocked || true
    echo "ARTIFACT_CLEANUP_BLOCKED artifact is dirty for $WORK" >&2
    exit 1
fi

remote_branch_matches() {
    durable_branch=$1
    durable_sha=$2
    durable_record=$(git -C "$RIG_ROOT" ls-remote \
        --exit-code --heads origin "refs/heads/$durable_branch" 2>/dev/null) ||
        return 1
    # An exact ref lookup must produce one <oid> <ref> record.
    set -- $durable_record
    if [ "$#" -eq 2 ] &&
        [ "$1" = "$durable_sha" ] &&
        [ "$2" = "refs/heads/$durable_branch" ]; then
        return 0
    fi
    return 2
}

merged_target_contains() {
    durable_target=$1
    durable_sha=$2
    git -C "$RIG_ROOT" fetch --no-tags origin \
        "+refs/heads/$durable_target:refs/remotes/origin/$durable_target" \
        >/dev/null 2>&1 || return 1
    if GIT_GRAFT_FILE=/dev/null \
        git --no-replace-objects -C "$RIG_ROOT" merge-base \
            --is-ancestor "$durable_sha" \
            "refs/remotes/origin/$durable_target" 2>/dev/null
    then
        return 0
    fi
    return 2
}

# Prove the handoff is durable from the remote itself. Metadata selects the
# expected evidence, but cannot establish it: stale or manually-edited bead
# fields must never be enough to retire the only reachable task worktree.
case "$HANDOFF_RESULT" in
    pull_request)
        EXPECTED_BRANCH="polecat/$WORK"
        if [ "$BRANCH" != "$EXPECTED_BRANCH" ] ||
           ! valid_branch "$BRANCH" ||
           ! valid_object_id "$PR_HEAD_SHA"; then
            record_cleanup_state blocked || true
            echo "ARTIFACT_CLEANUP_BLOCKED incomplete PR publication evidence for $WORK" >&2
            exit 1
        fi
        if remote_branch_matches "$BRANCH" "$PR_HEAD_SHA"; then
            :
        else
            REMOTE_STATUS=$?
            if [ "$REMOTE_STATUS" -eq 1 ]; then
                record_cleanup_state pending || true
                echo "ARTIFACT_CLEANUP_BLOCKED could not read origin/$BRANCH for $WORK; cleanup remains pending" >&2
            else
                record_cleanup_state blocked || true
                echo "ARTIFACT_CLEANUP_BLOCKED origin/$BRANCH does not match the validated PR head for $WORK" >&2
            fi
            exit 1
        fi
        ;;
    merged|already_merged)
        EXPECTED_BRANCH="polecat/$WORK"
        if [ "$BRANCH" != "$EXPECTED_BRANCH" ] ||
           ! valid_branch "$BRANCH" ||
           ! valid_object_id "$EXPECTED_ARTIFACT_SHA" ||
           ! valid_branch "$TARGET" ||
           ! valid_object_id "$MERGED_SHA"; then
            record_cleanup_state blocked || true
            echo "ARTIFACT_CLEANUP_BLOCKED incomplete direct-merge evidence for $WORK" >&2
            exit 1
        fi
        if remote_branch_matches "$BRANCH" "$EXPECTED_ARTIFACT_SHA"; then
            :
        else
            REMOTE_STATUS=$?
            if [ "$REMOTE_STATUS" -eq 1 ]; then
                record_cleanup_state pending || true
                echo "ARTIFACT_CLEANUP_BLOCKED could not read origin/$BRANCH for $WORK; cleanup remains pending" >&2
            else
                record_cleanup_state blocked || true
                echo "ARTIFACT_CLEANUP_BLOCKED origin/$BRANCH does not retain the artifact source for $WORK" >&2
            fi
            exit 1
        fi
        if merged_target_contains "$TARGET" "$MERGED_SHA"; then
            :
        else
            REMOTE_STATUS=$?
            if [ "$REMOTE_STATUS" -eq 1 ]; then
                record_cleanup_state pending || true
                echo "ARTIFACT_CLEANUP_BLOCKED could not refresh origin/$TARGET for $WORK; cleanup remains pending" >&2
            else
                record_cleanup_state blocked || true
                echo "ARTIFACT_CLEANUP_BLOCKED merged SHA is not durable on origin/$TARGET for $WORK" >&2
            fi
            exit 1
        fi
        ;;
    mr_merged)
        EXPECTED_BRANCH="polecat/$WORK"
        if [ "$BRANCH" != "$EXPECTED_BRANCH" ] ||
           ! valid_branch "$BRANCH" ||
           ! valid_object_id "$PR_HEAD_SHA" ||
           ! valid_branch "$TARGET" ||
           ! valid_object_id "$MERGED_SHA"; then
            record_cleanup_state blocked || true
            echo "ARTIFACT_CLEANUP_BLOCKED incomplete verified-PR evidence for $WORK" >&2
            exit 1
        fi
        if remote_branch_matches "$BRANCH" "$PR_HEAD_SHA"; then
            :
        else
            REMOTE_STATUS=$?
            if [ "$REMOTE_STATUS" -eq 1 ]; then
                record_cleanup_state pending || true
                echo "ARTIFACT_CLEANUP_BLOCKED could not read origin/$BRANCH for $WORK; cleanup remains pending" >&2
            else
                record_cleanup_state blocked || true
                echo "ARTIFACT_CLEANUP_BLOCKED origin/$BRANCH does not match the merged PR head for $WORK" >&2
            fi
            exit 1
        fi
        if merged_target_contains "$TARGET" "$MERGED_SHA"; then
            :
        else
            REMOTE_STATUS=$?
            if [ "$REMOTE_STATUS" -eq 1 ]; then
                record_cleanup_state pending || true
                echo "ARTIFACT_CLEANUP_BLOCKED could not refresh origin/$TARGET for $WORK; cleanup remains pending" >&2
            else
                record_cleanup_state blocked || true
                echo "ARTIFACT_CLEANUP_BLOCKED merged SHA is not durable on origin/$TARGET for $WORK" >&2
            fi
            exit 1
        fi
        ;;
esac

# Narrow the state race before deletion. Any concurrent reopen, reassignment,
# path/SHA edit, or other bead mutation makes this attempt fail closed.
CURRENT_WORK_RECORD=$(read_work_record) || {
    echo "ARTIFACT_CLEANUP_BLOCKED could not refresh work bead $WORK before removal" >&2
    exit 1
}
if [ "$CURRENT_WORK_RECORD" != "$WORK_RECORD" ]; then
    echo "ARTIFACT_CLEANUP_BLOCKED work bead changed before artifact removal for $WORK" >&2
    exit 1
fi
CURRENT_SAFE_ARTIFACT=$(validate_task_artifact \
    "$SAFE_ARTIFACT" "$WORK" 2>/dev/null) || {
    record_cleanup_state blocked || true
    echo "ARTIFACT_CLEANUP_BLOCKED artifact changed before removal for $WORK" >&2
    exit 1
}
CURRENT_ARTIFACT_SHA=$(git -C "$SAFE_ARTIFACT" rev-parse HEAD 2>/dev/null) || {
    record_cleanup_state blocked || true
    echo "ARTIFACT_CLEANUP_BLOCKED artifact HEAD became unreadable before removal for $WORK" >&2
    exit 1
}
CURRENT_ARTIFACT_STATUS=$(git -C "$SAFE_ARTIFACT" status \
    --porcelain --untracked-files=all) || {
    record_cleanup_state blocked || true
    echo "ARTIFACT_CLEANUP_BLOCKED could not re-inspect artifact status for $WORK" >&2
    exit 1
}
if [ "$CURRENT_SAFE_ARTIFACT" != "$SAFE_ARTIFACT" ] ||
   [ "$CURRENT_ARTIFACT_SHA" != "$EXPECTED_ARTIFACT_SHA" ] ||
   [ -n "$CURRENT_ARTIFACT_STATUS" ]; then
    record_cleanup_state blocked || true
    echo "ARTIFACT_CLEANUP_BLOCKED artifact changed before removal for $WORK" >&2
    exit 1
fi

if ! git -C "$RIG_ROOT" worktree remove "$SAFE_ARTIFACT"; then
    record_cleanup_state blocked || true
    echo "ARTIFACT_CLEANUP_BLOCKED clean artifact could not be removed for $WORK" >&2
    exit 1
fi

complete_without_path
