#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
GASTOWN="$ROOT/gastown"
POLECAT_FORMULA="$GASTOWN/formulas/mol-polecat-work.toml"
WITNESS_FORMULA="$GASTOWN/formulas/mol-witness-patrol.toml"
SHUTDOWN_FORMULA="$GASTOWN/formulas/mol-shutdown-dance.toml"
REFINERY_FORMULA="$GASTOWN/formulas/mol-refinery-patrol.toml"
WORKTREE_SETUP="$GASTOWN/assets/scripts/worktree-setup.sh"
ARTIFACT_CLEANUP="$GASTOWN/assets/scripts/task-artifact-cleanup.sh"

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

extract_validator() {
    local output=$1
    python3 - "$POLECAT_FORMULA" "$output" <<'PY'
import pathlib
import sys
import tomllib

formula_path = pathlib.Path(sys.argv[1])
output_path = pathlib.Path(sys.argv[2])
with formula_path.open("rb") as handle:
    formula = tomllib.load(handle)
workspace = next(step for step in formula["steps"] if step["id"] == "workspace-setup")
description = workspace["description"]
begin = description.index("# BEGIN ARTIFACT_WORKTREE_VALIDATOR")
end = description.index("# END ARTIFACT_WORKTREE_VALIDATOR", begin)
block = description[begin:end].splitlines()[1:]
output_path.write_text("\n".join(block) + "\n", encoding="utf-8")
PY
}

extract_shutdown_probe() {
    local output=$1
    python3 - "$SHUTDOWN_FORMULA" "$output" <<'PY'
import pathlib
import sys
import tomllib

formula_path = pathlib.Path(sys.argv[1])
output_path = pathlib.Path(sys.argv[2])
with formula_path.open("rb") as handle:
    formula = tomllib.load(handle)
execute = next(step for step in formula["steps"] if step["id"] == "execute")
description = execute["description"]
begin = description.index("# BEGIN SHUTDOWN_PROGRESS_PROBE")
end = description.index("# END SHUTDOWN_PROGRESS_PROBE", begin)
block = description[begin:end].splitlines()[1:]
output_path.write_text("\n".join(block) + "\n", encoding="utf-8")
PY
}

extract_witness_artifact_safety_gate() {
    local output=$1
    python3 - "$WITNESS_FORMULA" "$output" <<'PY'
import pathlib
import sys
import tomllib

formula_path = pathlib.Path(sys.argv[1])
output_path = pathlib.Path(sys.argv[2])
with formula_path.open("rb") as handle:
    formula = tomllib.load(handle)
recovery = next(
    step for step in formula["steps"] if step["id"] == "recover-orphaned-beads"
)
description = recovery["description"]
begin = description.index("# BEGIN WITNESS_ARTIFACT_SAFETY_GATE")
end = description.index("```", begin)
block = description[begin:end].splitlines()[1:]
script = "\n".join(block).replace("<bead>", "ac-status")
output_path.write_text(script + "\n", encoding="utf-8")
PY
}

extract_workspace_creation() {
    local output=$1
    python3 - "$POLECAT_FORMULA" "$output" <<'PY'
import pathlib
import re
import sys
import tomllib

formula_path = pathlib.Path(sys.argv[1])
output_path = pathlib.Path(sys.argv[2])
with formula_path.open("rb") as handle:
    formula = tomllib.load(handle)
workspace = next(step for step in formula["steps"] if step["id"] == "workspace-setup")
blocks = re.findall(r"```bash\n(.*?)```", workspace["description"], re.DOTALL)
if len(blocks) < 4:
    raise SystemExit(f"workspace-setup has {len(blocks)} bash blocks; expected at least 4")
script = "\n".join(blocks[:4])
script = script.replace("{{convoy_id}}", "convoy-test")
script = script.replace("{{base_branch}}", "main")
if "{{" in script:
    raise SystemExit("workspace creation extraction retained an unresolved template")
output_path.write_text(script, encoding="utf-8")
PY
}

init_repo() {
    local repo=$1
    git init -q "$repo"
    git -C "$repo" config user.email artifact-test@example.com
    git -C "$repo" config user.name "Artifact Test"
    printf 'fixture\n' >"$repo/fixture.txt"
    git -C "$repo" add fixture.txt
    git -C "$repo" commit -qm "fixture"
}

test_workspace_creation_uses_canonical_sibling() {
    local tmp rig remote city provider canonical setup calls bd_subcommand
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' RETURN
    rig="$tmp/rig"
    remote="$tmp/remote.git"
    city="$tmp/city"
    provider="$city/.gc/worktrees/demo/polecats/gastown.nux"
    canonical="$city/.gc/worktrees/demo/artifacts/worktrees/ac-safe1"
    setup="$tmp/workspace-create.sh"
    calls="$tmp/gc-calls"
    bd_subcommand=$(printf '%s%s' b d)

    init_repo "$rig"
    git -C "$rig" branch -M main
    git init -q --bare "$remote"
    git -C "$remote" symbolic-ref HEAD refs/heads/main
    git -C "$rig" remote add origin "$remote"
    git -C "$rig" push -qu origin main
    mkdir -p "$(dirname "$provider")"
    git -C "$rig" worktree add -qb provider-home "$provider" main
    extract_workspace_creation "$setup"

    (
        gc() {
            printf '%s\n' "$*" >>"$calls"
            case "$*" in
                "convoy status convoy-test --json")
                    printf '%s\n' '{"children":[{"id":"ac-safe1"}]}'
                    ;;
                "$bd_subcommand show ac-safe1 --json")
                    printf '%s\n' '[{"metadata":{}}]'
                    ;;
                "$bd_subcommand update ac-safe1 --set-metadata artifact_dir="*)
                    return 0
                    ;;
                "runtime drain-ack")
                    fail "canonical workspace creation unexpectedly drain-acked"
                    ;;
                *)
                    fail "unexpected gc call during workspace creation: $*"
                    ;;
            esac
        }
        export GC_CITY_PATH="$city"
        export GC_RIG=demo
        export GC_RIG_ROOT="$rig"
        cd "$provider"
        # shellcheck source=/dev/null
        source "$setup"
    )

    [[ -e "$canonical/.git" ]] ||
        fail "formula did not create the canonical sibling task worktree"
    [[ "$(git -C "$canonical" rev-parse --show-toplevel)" == "$canonical" ]] ||
        fail "canonical artifact path is not a Git worktree root"
    [[ ! -e "$provider/worktrees/ac-safe1" ]] ||
        fail "formula created a nested task worktree under the provider home"
    grep -qF \
        "$bd_subcommand update ac-safe1 --set-metadata artifact_dir=$canonical --unset-metadata work_dir" \
        "$calls" ||
        fail "formula did not record the exact canonical artifact_dir"
}

test_worktree_setup_ignores_gc_without_mutating_tracked_or_global_ignores() {
    local tmp rig city provider global_config global_ignore exclude
    local tracked_before config_before global_before status
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' RETURN
    rig="$tmp/rig"
    city="$tmp/city"
    provider="$city/.gc/worktrees/demo/polecats/gastown.nux"
    global_config="$tmp/global.gitconfig"
    global_ignore="$tmp/global-ignore"

    init_repo "$rig"
    printf '/tracked-only\n' >"$rig/.gitignore"
    git -C "$rig" add .gitignore
    git -C "$rig" commit -qm "tracked ignore fixture"

    printf 'global-only\n' >"$global_ignore"
    GIT_CONFIG_GLOBAL="$global_config" \
        git config --global core.excludesFile "$global_ignore"
    tracked_before=$(git -C "$rig" hash-object .gitignore)
    config_before=$(git hash-object "$global_config")
    global_before=$(git hash-object "$global_ignore")

    mkdir -p "$provider/.gc"
    printf '{"runtime":"scaffold"}\n' >"$provider/.gc/settings.json"
    GIT_CONFIG_GLOBAL="$global_config" \
        "$WORKTREE_SETUP" "$rig" "$provider" gastown.nux

    [[ -e "$provider/.git" ]] ||
        fail "worktree setup did not replace the scaffold with a Git worktree"
    [[ -f "$provider/.gc/settings.json" ]] ||
        fail "worktree setup did not preserve staged .gc runtime metadata"
    status=$(GIT_CONFIG_GLOBAL="$global_config" \
        git -C "$provider" status --porcelain --untracked-files=all)
    [[ -z "$status" ]] ||
        fail ".gc runtime metadata remained visible to git status: $status"
    [[ "$(git -C "$provider" hash-object .gitignore)" == "$tracked_before" ]] ||
        fail "worktree setup mutated the tracked .gitignore"
    [[ "$(git hash-object "$global_config")" == "$config_before" ]] ||
        fail "worktree setup mutated global Git configuration"
    [[ "$(git hash-object "$global_ignore")" == "$global_before" ]] ||
        fail "worktree setup mutated the user's global excludes file"

    exclude=$(git -C "$provider" rev-parse --git-path info/exclude)
    [[ "$(grep -cxF '.gc/' "$exclude")" -eq 1 ]] ||
        fail "repository-local exclude does not contain exactly one .gc/ entry"

    # Existing worktrees take the idempotent fast path. That path must still
    # repair an installation created before .gc/ became a runtime exclude.
    sed -i '\|^\.gc/$|d' "$exclude"
    GIT_CONFIG_GLOBAL="$global_config" \
        "$WORKTREE_SETUP" "$rig" "$provider" gastown.nux
    [[ "$(grep -cxF '.gc/' "$exclude")" -eq 1 ]] ||
        fail "existing-worktree fast path did not restore exactly one .gc/ entry"
}

test_worktree_sync_ignores_codex_skills_but_rejects_unrelated_dirt() {
    local tmp rig remote city provider skill_root exclude head_before status stderr
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' RETURN
    rig="$tmp/rig"
    remote="$tmp/remote.git"
    city="$tmp/city"
    provider="$city/.gc/worktrees/demo/refinery"
    skill_root="$tmp/core-skills"
    stderr="$tmp/sync.stderr"

    init_repo "$rig"
    git -C "$rig" branch -M main
    git init -q --bare "$remote"
    git -C "$remote" symbolic-ref HEAD refs/heads/main
    git -C "$rig" remote add origin "$remote"
    git -C "$rig" push -qu origin main

    "$WORKTREE_SETUP" "$rig" "$provider" refinery

    # Gas City materializes Codex skills as symlinks under .agents/skills.
    # These runtime-owned entries must not poison the strict provider sync.
    mkdir -p \
        "$skill_root/gc-mail" \
        "$skill_root/gc-work" \
        "$provider/.agents/skills"
    printf '%s\n' '# gc-mail' >"$skill_root/gc-mail/SKILL.md"
    printf '%s\n' '# gc-work' >"$skill_root/gc-work/SKILL.md"
    ln -s "$skill_root/gc-mail" "$provider/.agents/skills/core.gc-mail"
    ln -s "$skill_root/gc-work" "$provider/.agents/skills/core.gc-work"

    "$WORKTREE_SETUP" "$rig" "$provider" refinery --sync
    [[ -L "$provider/.agents/skills/core.gc-mail" &&
       -L "$provider/.agents/skills/core.gc-work" ]] ||
        fail "strict sync removed materialized Codex skill symlinks"
    [[ -z "$(git -C "$provider" status --porcelain --untracked-files=all)" ]] ||
        fail "materialized Codex skill symlinks remain visible to git status"

    exclude=$(git -C "$provider" rev-parse --git-path info/exclude)
    [[ "$(grep -cxF '.agents/skills/' "$exclude")" -eq 1 ]] ||
        fail "repository-local exclude does not contain exactly one .agents/skills/ entry"

    # The targeted runtime exclude must not weaken the fail-closed guard for
    # unrelated source dirt.
    head_before=$(git -C "$provider" rev-parse HEAD)
    printf '%s\n' 'preserve me' >"$provider/dirty.txt"
    set +e
    "$WORKTREE_SETUP" "$rig" "$provider" refinery --sync 2>"$stderr"
    status=$?
    set -e
    [[ "$status" -ne 0 ]] ||
        fail "strict sync accepted unrelated provider worktree dirt"
    grep -qF "refusing to sync dirty provider worktree at $provider" "$stderr" ||
        fail "dirty sync did not report the fail-closed refusal"
    [[ -f "$provider/dirty.txt" ]] ||
        fail "dirty sync discarded unrelated provider worktree dirt"
    [[ "$(git -C "$provider" rev-parse HEAD)" == "$head_before" ]] ||
        fail "dirty sync moved provider HEAD"
}

test_worktree_sync_preserves_task_state_and_only_fast_forwards_stable_branch() {
    local tmp rig remote city provider stable hash task_sha detached_sha
    local stable_sha foreign foreign_head foreign_exclude_before status
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' RETURN
    rig="$tmp/rig"
    remote="$tmp/remote.git"
    city="$tmp/city"
    provider="$city/.gc/worktrees/demo/polecats/gastown.nux"
    foreign="$city/.gc/worktrees/demo/polecats/gastown.foreign"

    init_repo "$rig"
    git -C "$rig" branch -M main
    git init -q --bare "$remote"
    git -C "$remote" symbolic-ref HEAD refs/heads/main
    git -C "$rig" remote add origin "$remote"
    git -C "$rig" push -qu origin main

    "$WORKTREE_SETUP" "$rig" "$provider" gastown.nux
    hash=$(printf '%s' "$provider" | git -C "$rig" hash-object --stdin | cut -c1-12)
    stable="gc-gastown.nux-$hash"
    [[ "$(git -C "$provider" branch --show-current)" == "$stable" ]] ||
        fail "new provider did not use its path-derived stable branch"

    # Normal --sync is idempotent.
    "$WORKTREE_SETUP" "$rig" "$provider" gastown.nux --sync
    "$WORKTREE_SETUP" "$rig" "$provider" gastown.nux --sync
    [[ "$(git -C "$provider" branch --show-current)" == "$stable" ]] ||
        fail "idempotent sync left the stable provider branch"

    # A clean task branch may contain unique work. Sync must preserve its ref,
    # switch back to the stable provider branch, and advance only that stable
    # branch to freshly fetched origin/HEAD.
    git -C "$provider" checkout -qb polecat/task-preserved
    printf 'task work\n' >"$provider/task.txt"
    git -C "$provider" add task.txt
    git -C "$provider" commit -qm "task work"
    task_sha=$(git -C "$provider" rev-parse HEAD)
    printf 'main advance\n' >"$rig/main-b.txt"
    git -C "$rig" add main-b.txt
    git -C "$rig" commit -qm "main B"
    git -C "$rig" push -qu origin main

    "$WORKTREE_SETUP" "$rig" "$provider" gastown.nux --sync
    [[ "$(git -C "$provider" branch --show-current)" == "$stable" ]] ||
        fail "sync did not switch a preserved task branch to the stable branch"
    [[ "$(git -C "$rig" rev-parse refs/heads/polecat/task-preserved)" == "$task_sha" ]] ||
        fail "sync rewrote the preserved task branch"
    [[ "$(git -C "$provider" rev-parse HEAD)" == \
       "$(git -C "$rig" rev-parse refs/remotes/origin/main)" ]] ||
        fail "stable provider branch was not fast-forwarded to fresh origin/HEAD"
    [[ "$(git -C "$rig" show refs/heads/polecat/task-preserved:task.txt)" == "task work" ]] ||
        fail "preserved task branch lost its committed work"

    # A detached HEAD that is reachable from a real ref is safe to leave; the
    # ref remains the durable owner when sync switches back to stable.
    git -C "$provider" checkout -q --detach "$task_sha"
    "$WORKTREE_SETUP" "$rig" "$provider" gastown.nux --sync
    [[ "$(git -C "$provider" branch --show-current)" == "$stable" ]] ||
        fail "sync did not leave a ref-reachable detached HEAD safely"
    [[ "$(git -C "$rig" rev-parse refs/heads/polecat/task-preserved)" == "$task_sha" ]] ||
        fail "ref-reachable detached state was not preserved"

    # An unreferenced detached commit is the only durable copy of its work.
    # Sync must fail without switching or rewriting it.
    git -C "$provider" checkout -q --detach
    printf 'detached unique\n' >"$provider/detached.txt"
    git -C "$provider" add detached.txt
    git -C "$provider" commit -qm "detached unique"
    detached_sha=$(git -C "$provider" rev-parse HEAD)
    set +e
    "$WORKTREE_SETUP" "$rig" "$provider" gastown.nux --sync
    status=$?
    set -e
    [[ "$status" -ne 0 ]] ||
        fail "sync accepted an unreferenced detached provider HEAD"
    [[ "$(git -C "$provider" rev-parse HEAD)" == "$detached_sha" ]] ||
        fail "failed detached sync rewrote the unique HEAD"
    [[ -z "$(git -C "$provider" branch --show-current)" ]] ||
        fail "failed detached sync switched away from the unique HEAD"

    # Give the test-only detached commit a durable ref, then verify dirty
    # source state also blocks before any checkout or branch movement.
    git -C "$rig" branch audit-detached-preserved "$detached_sha"
    git -C "$provider" checkout -q "$stable"
    stable_sha=$(git -C "$provider" rev-parse HEAD)
    printf 'dirty\n' >"$provider/dirty.txt"
    set +e
    "$WORKTREE_SETUP" "$rig" "$provider" gastown.nux --sync
    status=$?
    set -e
    [[ "$status" -ne 0 && -f "$provider/dirty.txt" ]] ||
        fail "sync accepted or discarded dirty provider work"
    [[ "$(git -C "$provider" rev-parse HEAD)" == "$stable_sha" ]] ||
        fail "dirty sync moved the stable branch"
    rm -f "$provider/dirty.txt"

    # Unique/diverged work on the stable branch is not infrastructure state;
    # preserve it for recovery instead of rebasing or resetting it.
    printf 'stable unique\n' >"$provider/stable-unique.txt"
    git -C "$provider" add stable-unique.txt
    git -C "$provider" commit -qm "stable unique"
    stable_sha=$(git -C "$provider" rev-parse HEAD)
    printf 'main diverges\n' >"$rig/main-c.txt"
    git -C "$rig" add main-c.txt
    git -C "$rig" commit -qm "main C"
    git -C "$rig" push -qu origin main
    set +e
    "$WORKTREE_SETUP" "$rig" "$provider" gastown.nux --sync
    status=$?
    set -e
    [[ "$status" -ne 0 ]] ||
        fail "sync accepted unique/diverged stable provider work"
    [[ "$(git -C "$provider" rev-parse HEAD)" == "$stable_sha" ]] ||
        fail "failed diverged sync rewrote the stable branch"

    # A Git worktree at the configured path is not sufficient: it must share
    # the exact rig repository common dir. Refuse foreign repositories before
    # touching their excludes, branch, or HEAD.
    init_repo "$foreign"
    foreign_head=$(git -C "$foreign" rev-parse HEAD)
    foreign_exclude_before=$(git hash-object "$foreign/.git/info/exclude")
    set +e
    "$WORKTREE_SETUP" "$rig" "$foreign" gastown.foreign --sync
    status=$?
    set -e
    [[ "$status" -ne 0 ]] ||
        fail "sync accepted a foreign repository at the provider path"
    [[ "$(git -C "$foreign" rev-parse HEAD)" == "$foreign_head" ]] ||
        fail "foreign-worktree refusal moved its HEAD"
    [[ "$(git hash-object "$foreign/.git/info/exclude")" == "$foreign_exclude_before" ]] ||
        fail "foreign-worktree refusal mutated its local excludes"
}

test_shutdown_probe_scopes_rig_and_fails_closed() {
    local tmp probe calls output bd_subcommand
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' RETURN
    probe="$tmp/probe.sh"
    calls="$tmp/calls"
    output="$tmp/output"
    bd_subcommand=$(printf '%s%s' b d)
    extract_shutdown_probe "$probe"

    (
        gc() {
            printf '%s\n' "$*" >> "$calls"
            case "$*" in
                "session list --state=all --json")
                    printf '%s\n' '{"sessions":[{"id":"session-1","name":"demo/gastown.worker","template":"gastown.polecat","rig":"demo","alias":"demo/gastown.worker","agent_name":"demo/gastown.worker","session_name":"demo--worker","work_dir":"/missing/provider"}]}'
                    ;;
                "$bd_subcommand --rig demo list --status=in_progress --json --limit=0")
                    printf '%s\n' '[]'
                    ;;
                *)
                    return 1
                    ;;
            esac
        }
        target="demo/gastown.worker"
        # shellcheck source=/dev/null
        source "$probe"
    ) > "$output"

    rg -q "^${bd_subcommand} --rig demo list --status=in_progress --json --limit=0$" "$calls" ||
        fail "shutdown probe did not route the target work query to its rig store"
    rg -q '^progress_signal=none$' "$output" ||
        fail "successful exact lookup with no claimed work did not produce progress=none"

    : > "$calls"
    (
        gc() {
            printf '%s\n' "$*" >> "$calls"
            case "$*" in
                "session list --state=all --json")
                    printf '%s\n' '{"sessions":[{"id":"s1","name":"demo/gastown.worker","template":"demo/gastown.polecat"},{"id":"s2","alias":"demo/gastown.worker","template":"demo/gastown.polecat"}]}'
                    ;;
                *)
                    return 1
                    ;;
            esac
        }
        target="demo/gastown.worker"
        # shellcheck source=/dev/null
        source "$probe"
    ) > "$output"

    rg -q '^progress_signal=unknown$' "$output" ||
        fail "ambiguous target-session lookup did not fail closed"
    if rg -q '^bd ' "$calls"; then
        fail "ambiguous target-session lookup queried an arbitrary bead store"
    fi
}

test_validator_rejects_provider_and_unrelated_paths() {
    local tmp validator rig city_a city_b provider bead valid got plain
    local alias_root foreign foreign_wt cross_city other_rig wrong_namespace
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' RETURN
    validator="$tmp/validator.sh"
    extract_validator "$validator"
    # shellcheck source=/dev/null
    source "$validator"

    rig="$tmp/rig"
    city_a="$tmp/city-a"
    city_b="$tmp/city-b"
    provider="$city_a/.gc/worktrees/demo/polecats/gastown.nux"
    bead="ac-safe1"
    valid="$provider/worktrees/$bead"
    init_repo "$rig"
    git -C "$rig" worktree add -qb provider "$provider" HEAD
    mkdir -p "$provider/worktrees"
    git -C "$rig" worktree add -qb "polecat/$bead" "$valid" HEAD

    if validate_artifact_worktree \
        "$provider" "$city_a" demo "$rig" "$bead" existing >/dev/null; then
        fail "persistent provider home was accepted as a task artifact"
    fi

    alias_root="$tmp/provider-alias/worktrees"
    mkdir -p "$alias_root"
    ln -s "$provider" "$alias_root/$bead"
    if validate_artifact_worktree \
        "$alias_root/$bead" "$city_a" demo "$rig" "$bead" existing >/dev/null; then
        fail "symlink resolving to provider home was accepted"
    fi

    plain="$provider/worktrees/ac-plain"
    mkdir -p "$plain"
    if validate_artifact_worktree \
        "$plain" "$city_a" demo "$rig" ac-plain existing >/dev/null; then
        fail "plain nested directory was accepted as a Git worktree root"
    fi

    if validate_artifact_worktree \
        "$valid" "$city_a" demo "$rig" ac-wrong existing >/dev/null; then
        fail "worktree for a different bead id was accepted"
    fi
    if validate_artifact_worktree \
        "$tmp/missing" "$city_a" demo "$rig" "$bead" existing >/dev/null; then
        fail "missing artifact directory was accepted"
    fi

    foreign="$tmp/foreign"
    foreign_wt="$tmp/foreign-parent/worktrees/$bead"
    init_repo "$foreign"
    mkdir -p "$(dirname "$foreign_wt")"
    git -C "$foreign" worktree add -qb foreign-task "$foreign_wt" HEAD
    if validate_artifact_worktree \
        "$foreign_wt" "$city_a" demo "$rig" "$bead" existing >/dev/null; then
        fail "same-shaped worktree from a foreign repository was accepted"
    fi

    cross_city="$city_b/.gc/worktrees/demo/artifacts/worktrees/$bead"
    mkdir -p "$(dirname "$cross_city")"
    git -C "$rig" worktree add -qb cross-city "$cross_city" HEAD
    if validate_artifact_worktree \
        "$cross_city" "$city_a" demo "$rig" "$bead" existing >/dev/null; then
        fail "same-repository worktree from another city was accepted"
    fi

    other_rig="$city_a/.gc/worktrees/other/artifacts/worktrees/$bead"
    mkdir -p "$(dirname "$other_rig")"
    git -C "$rig" worktree add -qb other-rig "$other_rig" HEAD
    if validate_artifact_worktree \
        "$other_rig" "$city_a" demo "$rig" "$bead" existing >/dev/null; then
        fail "same-city worktree from another rig was accepted"
    fi

    wrong_namespace="$city_a/.gc/worktrees/demo/refinery/worktrees/$bead"
    mkdir -p "$(dirname "$wrong_namespace")"
    git -C "$rig" worktree add -qb wrong-namespace "$wrong_namespace" HEAD
    if validate_artifact_worktree \
        "$wrong_namespace" "$city_a" demo "$rig" "$bead" existing >/dev/null; then
        fail "same-city/rig worktree from the wrong namespace was accepted"
    fi

    got=$(validate_artifact_worktree \
        "$valid" "$city_a" demo "$rig" "$bead" legacy-only) ||
        fail "valid per-bead legacy worktree was rejected"
    [[ "$got" == "$(cd "$valid" && pwd -P)" ]] ||
        fail "validator did not return the physical valid worktree path"
}

test_workspace_metadata_reads_fail_closed_without_pipefail() {
    local mode tmp rig remote city provider canonical setup calls bd_subcommand
    bd_subcommand=$(printf '%s%s' b d)
    for mode in command-failure malformed empty ambiguous; do
        tmp=$(mktemp -d)
        rig="$tmp/rig"
        remote="$tmp/remote.git"
        city="$tmp/city"
        provider="$city/.gc/worktrees/demo/polecats/gastown.nux"
        canonical="$city/.gc/worktrees/demo/artifacts/worktrees/ac-safe1"
        setup="$tmp/workspace-create.sh"
        calls="$tmp/gc-calls"

        init_repo "$rig"
        git -C "$rig" branch -M main
        git init -q --bare "$remote"
        git -C "$remote" symbolic-ref HEAD refs/heads/main
        git -C "$rig" remote add origin "$remote"
        git -C "$rig" push -qu origin main
        mkdir -p "$(dirname "$provider")"
        git -C "$rig" worktree add -qb "provider-$mode" "$provider" main
        extract_workspace_creation "$setup"

        if (
            set +e
            set +u
            set +o pipefail
            gc() {
                printf '%s\n' "$*" >>"$calls"
                case "$*" in
                    "convoy status convoy-test --json")
                        printf '%s\n' '{"children":[{"id":"ac-safe1"}]}'
                        ;;
                    "$bd_subcommand show ac-safe1 --json")
                        case "$mode" in
                            command-failure) return 1 ;;
                            malformed) printf '%s\n' '{' ;;
                            empty) printf '%s\n' '[]' ;;
                            ambiguous) printf '%s\n' '[{"metadata":{}},{"metadata":{}}]' ;;
                        esac
                        ;;
                    "runtime drain-ack")
                        return 0
                        ;;
                    "$bd_subcommand update "*)
                        fail "metadata read mode $mode reached a metadata update"
                        ;;
                    *)
                        fail "unexpected gc call for metadata read mode $mode: $*"
                        ;;
                esac
            }
            export GC_CITY_PATH="$city"
            export GC_RIG=demo
            export GC_RIG_ROOT="$rig"
            cd "$provider"
            # shellcheck source=/dev/null
            source "$setup"
        ); then
            fail "metadata read mode $mode did not stop workspace creation"
        fi

        [[ ! -e "$canonical" ]] ||
            fail "metadata read mode $mode created a replacement artifact"
        if grep -q "^${bd_subcommand} update " "$calls"; then
            fail "metadata read mode $mode mutated work bead metadata"
        fi
        rm -rf "$tmp"
    done
}

test_workspace_collision_fails_closed_without_alternate() {
    local tmp rig remote city provider canonical setup calls bd_subcommand
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' RETURN
    rig="$tmp/rig"
    remote="$tmp/remote.git"
    city="$tmp/city"
    provider="$city/.gc/worktrees/demo/polecats/gastown.nux"
    canonical="$city/.gc/worktrees/demo/artifacts/worktrees/ac-safe1"
    setup="$tmp/workspace-create.sh"
    calls="$tmp/gc-calls"
    bd_subcommand=$(printf '%s%s' b d)

    init_repo "$rig"
    git -C "$rig" branch -M main
    git init -q --bare "$remote"
    git -C "$remote" symbolic-ref HEAD refs/heads/main
    git -C "$rig" remote add origin "$remote"
    git -C "$rig" push -qu origin main
    mkdir -p "$(dirname "$provider")" "$canonical"
    printf 'preserve me\n' >"$canonical/recovery.txt"
    git -C "$rig" worktree add -qb provider-collision "$provider" main
    extract_workspace_creation "$setup"

    if (
        set +e
        set +u
        set +o pipefail
        gc() {
            printf '%s\n' "$*" >>"$calls"
            case "$*" in
                "convoy status convoy-test --json")
                    printf '%s\n' '{"children":[{"id":"ac-safe1"}]}'
                    ;;
                "$bd_subcommand show ac-safe1 --json")
                    printf '%s\n' '[{"metadata":{}}]'
                    ;;
                "runtime drain-ack")
                    return 0
                    ;;
                "$bd_subcommand update "*)
                    fail "canonical collision reached a metadata update"
                    ;;
                *)
                    fail "unexpected gc call during collision test: $*"
                    ;;
            esac
        }
        export GC_CITY_PATH="$city"
        export GC_RIG=demo
        export GC_RIG_ROOT="$rig"
        cd "$provider"
        # shellcheck source=/dev/null
        source "$setup"
    ); then
        fail "invalid canonical-path collision did not stop workspace creation"
    fi

    grep -qF 'preserve me' "$canonical/recovery.txt" ||
        fail "canonical collision contents were not preserved"
    if find "$city/.gc/worktrees/demo/artifacts" \
        -maxdepth 1 -name '.artifact-worktree-*' -print -quit | grep -q .; then
        fail "canonical collision created a random alternate artifact"
    fi
}

test_workspace_rejects_redirected_artifact_root_before_creation() {
    local tmp rig remote city provider redirected setup calls bd_subcommand
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' RETURN
    rig="$tmp/rig"
    remote="$tmp/remote.git"
    city="$tmp/city"
    provider="$city/.gc/worktrees/demo/polecats/gastown.nux"
    redirected="$tmp/redirected-artifacts"
    setup="$tmp/workspace-create.sh"
    calls="$tmp/gc-calls"
    bd_subcommand=$(printf '%s%s' b d)

    init_repo "$rig"
    git -C "$rig" branch -M main
    git init -q --bare "$remote"
    git -C "$remote" symbolic-ref HEAD refs/heads/main
    git -C "$rig" remote add origin "$remote"
    git -C "$rig" push -qu origin main
    mkdir -p "$(dirname "$provider")" "$redirected"
    git -C "$rig" worktree add -qb provider-redirect "$provider" main
    ln -s "$redirected" "$city/.gc/worktrees/demo/artifacts"
    extract_workspace_creation "$setup"

    if (
        set +e
        set +u
        set +o pipefail
        gc() {
            printf '%s\n' "$*" >>"$calls"
            case "$*" in
                "convoy status convoy-test --json")
                    printf '%s\n' '{"children":[{"id":"ac-safe1"}]}'
                    ;;
                "$bd_subcommand show ac-safe1 --json")
                    printf '%s\n' '[{"metadata":{}}]'
                    ;;
                "runtime drain-ack")
                    return 0
                    ;;
                "$bd_subcommand update "*)
                    fail "redirected artifact root reached a metadata update"
                    ;;
                *)
                    fail "unexpected gc call during redirect test: $*"
                    ;;
            esac
        }
        export GC_CITY_PATH="$city"
        export GC_RIG=demo
        export GC_RIG_ROOT="$rig"
        cd "$provider"
        # shellcheck source=/dev/null
        source "$setup"
    ); then
        fail "redirected artifact root did not stop workspace creation"
    fi

    [[ ! -e "$redirected/worktrees" ]] ||
        fail "workspace setup wrote through a redirected artifact root"
}

write_cleanup_gc_stub() {
    local bin=$1
    mkdir -p "$bin"
cat >"$bin/gc" <<'SH'
#!/bin/sh
printf '%s\n' "$*" >>"$GC_STUB_CALLS"
bd_subcommand=$(printf '%s%s' b d)
case "$*" in
    "$bd_subcommand --rig "*" list --status=closed --has-metadata-key=artifact_cleanup_state --limit=0 --json")
        cat "$GC_STUB_BEAD_JSON"
        ;;
    "$bd_subcommand --rig "*" show "*" --json")
        if [ -n "${GC_STUB_BEAD_JSON_SECOND:-}" ]; then
            count=0
            [ ! -f "$GC_STUB_SHOW_COUNT" ] ||
                count=$(cat "$GC_STUB_SHOW_COUNT")
            count=$((count + 1))
            printf '%s\n' "$count" >"$GC_STUB_SHOW_COUNT"
            if [ "$count" -ge 2 ]; then
                source_json=$GC_STUB_BEAD_JSON_SECOND
            else
                source_json=$GC_STUB_BEAD_JSON
            fi
        else
            source_json=$GC_STUB_BEAD_JSON
        fi
        jq --arg work "$5" '[.[] | select(.id == $work)]' "$source_json"
        ;;
    "$bd_subcommand --rig "*" update "*)
        exit "${GC_STUB_UPDATE_EXIT:-0}"
        ;;
    *)
        echo "unexpected gc call: $*" >&2
        exit 1
        ;;
esac
SH
    chmod +x "$bin/gc"
}

write_git_status_failure_wrapper() {
    local bin=$1
    mkdir -p "$bin"
cat >"$bin/git" <<'SH'
#!/bin/sh
for arg in "$@"; do
    if [ "$arg" = status ]; then
        count=0
        [ ! -f "$GIT_STATUS_STUB_COUNT" ] ||
            count=$(cat "$GIT_STATUS_STUB_COUNT")
        count=$((count + 1))
        printf '%s\n' "$count" >"$GIT_STATUS_STUB_COUNT"
        if [ "$count" -eq "$GIT_STATUS_STUB_FAIL_ON" ]; then
            echo "injected git status failure" >&2
            exit 73
        fi
        break
    fi
done
exec "$GIT_STATUS_STUB_REAL" "$@"
SH
    chmod +x "$bin/git"
}

write_cleanup_bead_json() {
    local output=$1 bead=$2 status=$3 result=$4 sha=$5 artifact=$6
    local state=${7:-} branch=${8:-polecat/$bead} target=${9:-main}
    local delivered_sha=${10:-$sha}
    jq -n \
        --arg bead "$bead" \
        --arg status "$status" \
        --arg result "$result" \
        --arg sha "$sha" \
        --arg artifact "$artifact" \
        --arg state "$state" \
        --arg branch "$branch" \
        --arg target "$target" \
        --arg delivered "$delivered_sha" \
        '[{
          id: $bead,
          status: $status,
          metadata: ({
            merge_result: $result,
            artifact_source_sha: $sha,
            artifact_dir: $artifact,
            branch: $branch,
            merged_target: $target
          } + (if $result == "pull_request"
               then {pr_head_sha: $delivered}
               elif $result == "mr_merged"
               then {pr_head_sha: $delivered, merged_sha: $delivered}
               else {merged_sha: $delivered}
               end)
            + (if $state == "" then {} else {artifact_cleanup_state: $state} end))
        }]' >"$output"
}

test_cleanup_command_is_idempotent_and_mr_retryable() {
    local tmp rig remote city canonical sweep_artifact nonterminal_artifact
    local bin bead_json calls sha nonterminal_sha result output
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' RETURN
    rig="$tmp/rig"
    remote="$tmp/remote.git"
    city="$tmp/city"
    canonical="$city/.gc/worktrees/demo/artifacts/worktrees/ac-safe1"
    bin="$tmp/bin"
    bead_json="$tmp/bead.json"
    calls="$tmp/calls"

    init_repo "$rig"
    git -C "$rig" branch -M main
    git init -q --bare "$remote"
    git -C "$remote" symbolic-ref HEAD refs/heads/main
    git -C "$rig" remote add origin "$remote"
    git -C "$rig" push -qu origin main
    mkdir -p "$(dirname "$canonical")" "$city/.gc/worktrees/demo/polecats"
    git -C "$rig" worktree add -qb cleanup-task "$canonical" HEAD
    sha=$(git -C "$canonical" rev-parse HEAD)
    git -C "$rig" push -qu origin HEAD:refs/heads/polecat/ac-safe1
    write_cleanup_gc_stub "$bin"

    write_cleanup_bead_json \
        "$bead_json" ac-safe1 blocked pull_request "$sha" "$canonical"
    GC_STUB_BEAD_JSON="$bead_json" GC_STUB_CALLS="$calls" \
        GC_CITY_PATH="$city" GC_RIG=demo GC_RIG_ROOT="$rig" \
        PATH="$bin:$PATH" "$ARTIFACT_CLEANUP" ac-safe1
    [[ -d "$canonical" ]] ||
        fail "non-terminal MR cleanup removed the artifact"
    grep -qF 'artifact_cleanup_state=pending' "$calls" ||
        fail "non-terminal MR cleanup did not record a pending retry marker"

    # Closed status alone cannot make pending, obsolete, or unknown result
    # values terminal. Keep this table dynamic so future aliases cannot
    # accidentally enter the deletion gate.
    for result in pull_request_pending pull_request_merged unknown_result; do
        : >"$calls"
        write_cleanup_bead_json \
            "$bead_json" ac-safe1 closed "$result" "$sha" "$canonical"
        output=$(
            GC_STUB_BEAD_JSON="$bead_json" GC_STUB_CALLS="$calls" \
                GC_CITY_PATH="$city" GC_RIG=demo GC_RIG_ROOT="$rig" \
                PATH="$bin:$PATH" "$ARTIFACT_CLEANUP" ac-safe1
        )
        [[ -d "$canonical" ]] ||
            fail "closed non-terminal result $result removed the artifact"
        [[ "$output" == *"ARTIFACT_CLEANUP_DEFERRED"* ]] ||
            fail "closed non-terminal result $result was not deferred"
        grep -qF 'artifact_cleanup_state=pending' "$calls" ||
            fail "closed non-terminal result $result did not remain pending"
    done

    : >"$calls"
    write_cleanup_bead_json \
        "$bead_json" ac-safe1 closed merged "$sha" "$canonical" pending
    GC_STUB_BEAD_JSON="$bead_json" GC_STUB_CALLS="$calls" \
        GC_CITY_PATH="$city" GC_RIG=demo GC_RIG_ROOT="$rig" \
        PATH="$bin:$PATH" "$ARTIFACT_CLEANUP" ac-safe1
    [[ ! -e "$canonical" ]] ||
        fail "verified terminal MR cleanup did not remove the artifact"
    grep -qF -- '--unset-metadata artifact_dir --unset-metadata work_dir --set-metadata artifact_cleanup_state=complete' "$calls" ||
        fail "terminal cleanup did not clear paths and record completion"

    : >"$calls"
    # Simulate the crash window where removal landed but the bead still names
    # the now-missing path. A retry must converge without another deletion.
    GC_STUB_BEAD_JSON="$bead_json" GC_STUB_CALLS="$calls" \
        GC_CITY_PATH="$city" GC_RIG=demo GC_RIG_ROOT="$rig" \
        PATH="$bin:$PATH" "$ARTIFACT_CLEANUP" ac-safe1
    grep -qF 'artifact_cleanup_state=complete' "$calls" ||
        fail "missing-path retry did not converge to complete"

    # Simulate a crash after terminal closure but before the immediate cleanup
    # invocation. The durable pending marker lets the next refinery scan find
    # and retire one closed artifact without an explicit bead argument. An
    # older non-terminal row must not starve this actionable cleanup.
    nonterminal_artifact="$city/.gc/worktrees/demo/artifacts/worktrees/ac-nonterminal"
    git -C "$rig" worktree add -q --detach "$nonterminal_artifact" HEAD
    nonterminal_sha=$(git -C "$nonterminal_artifact" rev-parse HEAD)
    sweep_artifact="$city/.gc/worktrees/demo/artifacts/worktrees/ac-sweep"
    git -C "$rig" worktree add -q --detach "$sweep_artifact" HEAD
    sha=$(git -C "$sweep_artifact" rev-parse HEAD)
    git -C "$rig" push -qu origin HEAD:refs/heads/polecat/ac-sweep
    write_cleanup_bead_json \
        "$bead_json" ac-sweep closed merged "$sha" "$sweep_artifact" pending
    jq \
        --arg artifact "$nonterminal_artifact" \
        --arg sha "$nonterminal_sha" \
        '[{
          id: "ac-nonterminal",
          status: "closed",
          updated_at: "2026-07-26T18:00:00Z",
          metadata: {
            merge_result: "pull_request_pending",
            artifact_cleanup_state: "pending",
            artifact_source_sha: $sha,
            artifact_dir: $artifact,
            branch: "polecat/ac-nonterminal",
            merged_target: "main"
          }
        }, (.[0] | .updated_at = "2026-07-26T19:00:00Z")]' \
        "$bead_json" >"$bead_json.next"
    mv "$bead_json.next" "$bead_json"
    : >"$calls"
    GC_STUB_BEAD_JSON="$bead_json" GC_STUB_CALLS="$calls" \
        GC_CITY_PATH="$city" GC_RIG=demo GC_RIG_ROOT="$rig" \
        PATH="$bin:$PATH" "$ARTIFACT_CLEANUP"
    [[ ! -e "$sweep_artifact" ]] ||
        fail "no-argument pending sweep did not close the pre-invocation crash window"
    [[ -d "$nonterminal_artifact" ]] ||
        fail "no-argument sweep removed the older non-terminal artifact"
    grep -qF 'show ac-sweep --json' "$calls" ||
        fail "no-argument sweep did not select the newer terminal row"
    ! grep -qF 'show ac-nonterminal --json' "$calls" ||
        fail "older non-terminal cleanup row starved the actionable row"
    grep -qF 'list --status=closed --has-metadata-key=artifact_cleanup_state' "$calls" ||
        fail "no-argument cleanup did not query the durable pending queue"
}

test_cleanup_command_rejects_cross_city_and_dirty_artifacts() {
    local tmp rig city_a city_b cross_city cross_rig wrong_namespace wrong_bead
    local symlink_path symlink_target foreign dirty provider legacy
    local remote bin bead_json calls sha status
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' RETURN
    rig="$tmp/rig"
    remote="$tmp/remote.git"
    city_a="$tmp/city-a"
    city_b="$tmp/city-b"
    cross_city="$city_b/.gc/worktrees/demo/artifacts/worktrees/ac-safe1"
    cross_rig="$city_a/.gc/worktrees/other/artifacts/worktrees/ac-cross-rig"
    wrong_namespace="$city_a/.gc/worktrees/demo/refinery/worktrees/ac-namespace"
    wrong_bead="$city_a/.gc/worktrees/demo/artifacts/worktrees/ac-other"
    symlink_path="$city_a/.gc/worktrees/demo/artifacts/worktrees/ac-link"
    symlink_target="$tmp/symlink-target/worktrees/ac-link"
    foreign="$city_a/.gc/worktrees/demo/artifacts/worktrees/ac-foreign"
    dirty="$city_a/.gc/worktrees/demo/artifacts/worktrees/ac-dirty"
    provider="$city_a/.gc/worktrees/demo/polecats/gastown.nux"
    legacy="$provider/worktrees/ac-legacy"
    bin="$tmp/bin"
    bead_json="$tmp/bead.json"
    calls="$tmp/calls"

    init_repo "$rig"
    git -C "$rig" branch -M main
    git init -q --bare "$remote"
    git -C "$remote" symbolic-ref HEAD refs/heads/main
    git -C "$rig" remote add origin "$remote"
    git -C "$rig" push -qu origin main
    mkdir -p \
        "$(dirname "$cross_city")" \
        "$(dirname "$cross_rig")" \
        "$(dirname "$wrong_namespace")" \
        "$(dirname "$symlink_target")" \
        "$(dirname "$dirty")" \
        "$city_a/.gc/worktrees/demo/polecats"
    git -C "$rig" worktree add -qb cross-city-cleanup "$cross_city" HEAD
    git -C "$rig" worktree add -qb cross-rig-cleanup "$cross_rig" HEAD
    git -C "$rig" worktree add -qb wrong-namespace-cleanup "$wrong_namespace" HEAD
    git -C "$rig" worktree add -qb wrong-bead-cleanup "$wrong_bead" HEAD
    git -C "$rig" worktree add -qb symlink-cleanup "$symlink_target" HEAD
    ln -s "$symlink_target" "$symlink_path"
    init_repo "$foreign"
    git -C "$rig" worktree add -qb dirty-cleanup "$dirty" HEAD
    git -C "$rig" worktree add -qb legacy-provider "$provider" HEAD
    mkdir -p "$(dirname "$legacy")"
    git -C "$rig" worktree add -qb legacy-cleanup "$legacy" HEAD
    printf 'uncommitted\n' >"$dirty/local.txt"
    write_cleanup_gc_stub "$bin"

    assert_cleanup_rejected() {
        local bead=$1 artifact=$2 label=$3 artifact_sha
        artifact_sha=$(git -C "$artifact" rev-parse HEAD)
        write_cleanup_bead_json \
            "$bead_json" "$bead" closed merged "$artifact_sha" "$artifact"
        : >"$calls"
        set +e
        GC_STUB_BEAD_JSON="$bead_json" GC_STUB_CALLS="$calls" \
            GC_CITY_PATH="$city_a" GC_RIG=demo GC_RIG_ROOT="$rig" \
            PATH="$bin:$PATH" "$ARTIFACT_CLEANUP" "$bead"
        status=$?
        set -e
        [[ "$status" -ne 0 && -e "$artifact" ]] ||
            fail "$label artifact was removed or accepted"
        if grep -qF -- '--unset-metadata artifact_dir' "$calls"; then
            fail "$label rejection cleared artifact metadata"
        fi
    }

    assert_cleanup_rejected ac-safe1 "$cross_city" "cross-city same-repository"
    assert_cleanup_rejected ac-cross-rig "$cross_rig" "cross-rig same-repository"
    assert_cleanup_rejected ac-namespace "$wrong_namespace" "wrong namespace"
    assert_cleanup_rejected ac-wrong "$wrong_bead" "wrong bead"
    assert_cleanup_rejected ac-link "$symlink_path" "redirected symlink"
    assert_cleanup_rejected ac-foreign "$foreign" "foreign repository"

    : >"$calls"
    sha=$(git -C "$dirty" rev-parse HEAD)
    write_cleanup_bead_json \
        "$bead_json" ac-dirty closed merged "$sha" "$dirty"
    set +e
    GC_STUB_BEAD_JSON="$bead_json" GC_STUB_CALLS="$calls" \
        GC_CITY_PATH="$city_a" GC_RIG=demo GC_RIG_ROOT="$rig" \
        PATH="$bin:$PATH" "$ARTIFACT_CLEANUP" ac-dirty
    status=$?
    set -e
    [[ "$status" -ne 0 && -d "$dirty" ]] ||
        fail "dirty artifact was removed"
    grep -qF 'artifact_cleanup_state=blocked' "$calls" ||
        fail "dirty artifact did not record a retryable blocked state"

    : >"$calls"
    sha=$(git -C "$legacy" rev-parse HEAD)
    git -C "$rig" push -qu origin HEAD:refs/heads/polecat/ac-legacy
    write_cleanup_bead_json \
        "$bead_json" ac-legacy closed merged "$sha" "$legacy"
    GC_STUB_BEAD_JSON="$bead_json" GC_STUB_CALLS="$calls" \
        GC_CITY_PATH="$city_a" GC_RIG=demo GC_RIG_ROOT="$rig" \
        PATH="$bin:$PATH" "$ARTIFACT_CLEANUP" ac-legacy
    [[ ! -e "$legacy" && -d "$provider" ]] ||
        fail "valid in-place legacy artifact cleanup did not preserve its provider home"
}

test_cleanup_requires_live_remote_evidence_and_stable_state() {
    local tmp rig remote city bin bead_json second_json calls show_count
    local artifact source_sha delivered_sha target_sha remote_record status
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' RETURN
    rig="$tmp/rig"
    remote="$tmp/remote.git"
    city="$tmp/city"
    bin="$tmp/bin"
    bead_json="$tmp/bead.json"
    second_json="$tmp/bead-second.json"
    calls="$tmp/calls"
    show_count="$tmp/show-count"

    init_repo "$rig"
    git -C "$rig" branch -M main
    git init -q --bare "$remote"
    git -C "$remote" symbolic-ref HEAD refs/heads/main
    git -C "$rig" remote add origin "$remote"
    git -C "$rig" push -qu origin main
    mkdir -p "$city/.gc/worktrees/demo/artifacts/worktrees" \
        "$city/.gc/worktrees/demo/polecats"
    write_cleanup_gc_stub "$bin"

    # A stale merged_sha can independently name an unrelated commit that is
    # genuinely present on the target. That corroborates terminal state but
    # proves no relation from the pre-rebase artifact source. Local cleanup is
    # safe only because the exact remote source ref remains the recovery copy.
    artifact="$city/.gc/worktrees/demo/artifacts/worktrees/ac-unrelated"
    git -C "$rig" worktree add -q --detach "$artifact" origin/main
    printf 'artifact source\n' >"$artifact/artifact-source"
    git -C "$artifact" add artifact-source
    git -C "$artifact" commit -qm "artifact source"
    source_sha=$(git -C "$artifact" rev-parse HEAD)
    git -C "$artifact" push -qu origin \
        HEAD:refs/heads/polecat/ac-unrelated
    printf 'unrelated target delivery\n' >"$rig/unrelated-target"
    git -C "$rig" add unrelated-target
    git -C "$rig" commit -qm "unrelated target delivery"
    git -C "$rig" push -qu origin main
    target_sha=$(git -C "$rig" rev-parse HEAD)
    if git -C "$rig" merge-base --is-ancestor "$source_sha" "$target_sha"; then
        fail "stale-merged fixture unexpectedly relates artifact source to target"
    fi
    write_cleanup_bead_json \
        "$bead_json" ac-unrelated closed merged \
        "$source_sha" "$artifact" "" polecat/ac-unrelated main "$target_sha"
    : >"$calls"
    GC_STUB_BEAD_JSON="$bead_json" GC_STUB_CALLS="$calls" \
        GC_CITY_PATH="$city" GC_RIG=demo GC_RIG_ROOT="$rig" \
        PATH="$bin:$PATH" "$ARTIFACT_CLEANUP" ac-unrelated
    [[ ! -e "$artifact" ]] ||
        fail "cleanup did not retire stale-merged fixture's local artifact"
    remote_record=$(git -C "$rig" ls-remote --exit-code --heads origin \
        refs/heads/polecat/ac-unrelated)
    [[ "$remote_record" == \
       "$source_sha"$'\t'"refs/heads/polecat/ac-unrelated" ]] ||
        fail "cleanup removed the unrelated fixture's durable source ref"

    # A clean, metadata-matching artifact is not disposable when its claimed
    # merge SHA is absent from the independently refreshed target. This is the
    # only-reachable-worktree regression: detach and remove its local branch so
    # no ref retains the unique commit.
    artifact="$city/.gc/worktrees/demo/artifacts/worktrees/ac-unreachable"
    git -C "$rig" worktree add -qb unreachable-task "$artifact" HEAD
    printf 'only reachable from the task worktree\n' >"$artifact/unique"
    git -C "$artifact" add unique
    git -C "$artifact" commit -qm unique
    source_sha=$(git -C "$artifact" rev-parse HEAD)
    git -C "$artifact" checkout -q --detach
    git -C "$rig" branch -D unreachable-task >/dev/null
    target_sha=$(git -C "$rig" rev-parse refs/remotes/origin/main)
    write_cleanup_bead_json \
        "$bead_json" ac-unreachable closed merged \
        "$source_sha" "$artifact" "" polecat/ac-unreachable main "$target_sha"
    set +e
    GC_STUB_BEAD_JSON="$bead_json" GC_STUB_CALLS="$calls" \
        GC_CITY_PATH="$city" GC_RIG=demo GC_RIG_ROOT="$rig" \
        PATH="$bin:$PATH" "$ARTIFACT_CLEANUP" ac-unreachable
    status=$?
    set -e
    [[ "$status" -ne 0 && -d "$artifact" ]] ||
        fail "cleanup removed the only reachable worktree without target evidence"

    # A durable source ref is still insufficient when the claimed merge commit
    # is not reachable from the freshly fetched target.
    artifact="$city/.gc/worktrees/demo/artifacts/worktrees/ac-bad-target"
    git -C "$rig" worktree add -q --detach "$artifact" origin/main
    source_sha=$(git -C "$artifact" rev-parse HEAD)
    git -C "$rig" push -qu origin \
        refs/remotes/origin/main:refs/heads/polecat/ac-bad-target
    printf 'not delivered to target\n' >"$rig/not-delivered"
    git -C "$rig" add not-delivered
    git -C "$rig" commit -qm "not delivered"
    delivered_sha=$(git -C "$rig" rev-parse HEAD)
    write_cleanup_bead_json \
        "$bead_json" ac-bad-target closed merged \
        "$source_sha" "$artifact" "" polecat/ac-bad-target main "$delivered_sha"
    : >"$calls"
    set +e
    GC_STUB_BEAD_JSON="$bead_json" GC_STUB_CALLS="$calls" \
        GC_CITY_PATH="$city" GC_RIG=demo GC_RIG_ROOT="$rig" \
        PATH="$bin:$PATH" "$ARTIFACT_CLEANUP" ac-bad-target
    status=$?
    set -e
    [[ "$status" -ne 0 && -d "$artifact" ]] ||
        fail "cleanup accepted a merge SHA absent from the refreshed target"
    grep -qF 'artifact_cleanup_state=blocked' "$calls" ||
        fail "target ancestry mismatch was not marked blocked"

    # Current-main MR semantics close at publication. The artifact still
    # points at the pre-rebase source SHA, so cleanup must prove the validated
    # rebased PR head through the exact remote source ref instead.
    artifact="$city/.gc/worktrees/demo/artifacts/worktrees/ac-pr"
    git -C "$rig" worktree add -q --detach "$artifact" HEAD
    source_sha=$(git -C "$artifact" rev-parse HEAD)
    printf 'rebased publication\n' >"$rig/published"
    git -C "$rig" add published
    git -C "$rig" commit -qm "published PR head"
    delivered_sha=$(git -C "$rig" rev-parse HEAD)
    write_cleanup_bead_json \
        "$bead_json" ac-pr closed pull_request \
        "$source_sha" "$artifact" "" polecat/ac-pr main "$delivered_sha"

    set +e
    : >"$calls"
    GC_STUB_BEAD_JSON="$bead_json" GC_STUB_CALLS="$calls" \
        GC_CITY_PATH="$city" GC_RIG=demo GC_RIG_ROOT="$rig" \
        PATH="$bin:$PATH" "$ARTIFACT_CLEANUP" ac-pr
    status=$?
    set -e
    [[ "$status" -ne 0 && -d "$artifact" ]] ||
        fail "PR cleanup accepted a missing origin source ref"

    git -C "$rig" push -qu origin \
        refs/remotes/origin/main:refs/heads/polecat/ac-pr
    set +e
    : >"$calls"
    GC_STUB_BEAD_JSON="$bead_json" GC_STUB_CALLS="$calls" \
        GC_CITY_PATH="$city" GC_RIG=demo GC_RIG_ROOT="$rig" \
        PATH="$bin:$PATH" "$ARTIFACT_CLEANUP" ac-pr
    status=$?
    set -e
    [[ "$status" -ne 0 && -d "$artifact" ]] ||
        fail "PR cleanup accepted a mismatched origin source ref"

    git -C "$rig" push -qf origin HEAD:refs/heads/polecat/ac-pr
    GC_STUB_BEAD_JSON="$bead_json" GC_STUB_CALLS="$calls" \
        GC_CITY_PATH="$city" GC_RIG=demo GC_RIG_ROOT="$rig" \
        PATH="$bin:$PATH" "$ARTIFACT_CLEANUP" ac-pr
    [[ ! -e "$artifact" ]] ||
        fail "PR cleanup rejected the exact remotely durable validated head"
    remote_record=$(git -C "$rig" ls-remote --exit-code --heads origin \
        refs/heads/polecat/ac-pr)
    [[ "$remote_record" == \
       "$delivered_sha"$'\t'"refs/heads/polecat/ac-pr" ]] ||
        fail "PR cleanup removed the validated remote recovery source ref"

    # Verified terminal MR cleanup likewise retires only the local artifact;
    # the exact validated remote source ref remains recovery evidence.
    artifact="$city/.gc/worktrees/demo/artifacts/worktrees/ac-mr"
    git -C "$rig" worktree add -q --detach "$artifact" origin/main
    source_sha=$(git -C "$artifact" rev-parse HEAD)
    git -C "$rig" push -qu origin \
        "$source_sha:refs/heads/polecat/ac-mr"
    write_cleanup_bead_json \
        "$bead_json" ac-mr closed mr_merged \
        "$source_sha" "$artifact" "" polecat/ac-mr main "$source_sha"
    GC_STUB_BEAD_JSON="$bead_json" GC_STUB_CALLS="$calls" \
        GC_CITY_PATH="$city" GC_RIG=demo GC_RIG_ROOT="$rig" \
        PATH="$bin:$PATH" "$ARTIFACT_CLEANUP" ac-mr
    [[ ! -e "$artifact" ]] ||
        fail "verified MR cleanup did not retire the local artifact"
    remote_record=$(git -C "$rig" ls-remote --exit-code --heads origin \
        refs/heads/polecat/ac-mr)
    [[ "$remote_record" == \
       "$source_sha"$'\t'"refs/heads/polecat/ac-mr" ]] ||
        fail "verified MR cleanup removed its remote recovery source ref"

    # Even valid remote evidence is stale if the bead changes between the
    # initial decision and the final pre-removal read.
    artifact="$city/.gc/worktrees/demo/artifacts/worktrees/ac-race"
    git -C "$rig" worktree add -q --detach "$artifact" origin/main
    source_sha=$(git -C "$artifact" rev-parse HEAD)
    git -C "$rig" push -qu origin \
        refs/remotes/origin/main:refs/heads/polecat/ac-race
    write_cleanup_bead_json \
        "$bead_json" ac-race closed merged \
        "$source_sha" "$artifact" "" polecat/ac-race main "$source_sha"
    write_cleanup_bead_json \
        "$second_json" ac-race open merged \
        "$source_sha" "$artifact" "" polecat/ac-race main "$source_sha"
    : >"$calls"
    : >"$show_count"
    set +e
    GC_STUB_BEAD_JSON="$bead_json" \
        GC_STUB_BEAD_JSON_SECOND="$second_json" \
        GC_STUB_SHOW_COUNT="$show_count" \
        GC_STUB_CALLS="$calls" \
        GC_CITY_PATH="$city" GC_RIG=demo GC_RIG_ROOT="$rig" \
        PATH="$bin:$PATH" "$ARTIFACT_CLEANUP" ac-race
    status=$?
    set -e
    [[ "$status" -ne 0 && -d "$artifact" ]] ||
        fail "cleanup removed an artifact after the bead was reopened"
    if grep -qF -- '--unset-metadata artifact_dir' "$calls"; then
        fail "stale-state rejection cleared artifact metadata"
    fi

    # A one-row response must also be the requested row, not merely any object
    # returned by a misrouted or corrupt metadata lookup.
    jq '.[] .id = "ac-other"' "$bead_json" >"$second_json"
    : >"$calls"
    set +e
    GC_STUB_BEAD_JSON="$second_json" GC_STUB_CALLS="$calls" \
        GC_CITY_PATH="$city" GC_RIG=demo GC_RIG_ROOT="$rig" \
        PATH="$bin:$PATH" "$ARTIFACT_CLEANUP" ac-race
    status=$?
    set -e
    [[ "$status" -ne 0 && -d "$artifact" ]] ||
        fail "cleanup accepted a different one-row bead response"
}

test_git_status_failures_preserve_artifacts_and_provider_state() {
    local tmp rig remote city provider artifact bin bead_json calls
    local fail_on source_sha stable_head stable_branch status real_git
    local status_count
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' RETURN
    rig="$tmp/rig"
    remote="$tmp/remote.git"
    city="$tmp/city"
    provider="$city/.gc/worktrees/demo/polecats/gastown.nux"
    artifact="$city/.gc/worktrees/demo/artifacts/worktrees/ac-status"
    bin="$tmp/bin"
    bead_json="$tmp/bead.json"
    calls="$tmp/calls"
    status_count="$tmp/status-count"
    real_git=$(command -v git)

    init_repo "$rig"
    git -C "$rig" branch -M main
    git init -q --bare "$remote"
    git -C "$remote" symbolic-ref HEAD refs/heads/main
    git -C "$rig" remote add origin "$remote"
    git -C "$rig" push -qu origin main
    "$WORKTREE_SETUP" "$rig" "$provider" gastown.nux
    stable_head=$(git -C "$provider" rev-parse HEAD)
    stable_branch=$(git -C "$provider" branch --show-current)

    write_git_status_failure_wrapper "$bin"
    : >"$status_count"
    set +e
    GIT_STATUS_STUB_REAL="$real_git" \
        GIT_STATUS_STUB_COUNT="$status_count" \
        GIT_STATUS_STUB_FAIL_ON=1 \
        PATH="$bin:$PATH" \
        "$WORKTREE_SETUP" "$rig" "$provider" gastown.nux --sync
    status=$?
    set -e
    [[ "$status" -ne 0 ]] ||
        fail "provider sync interpreted a failed git status as clean"
    [[ "$(git -C "$provider" rev-parse HEAD)" == "$stable_head" &&
       "$(git -C "$provider" branch --show-current)" == "$stable_branch" ]] ||
        fail "provider sync moved state after git status failed"

    mkdir -p "$(dirname "$artifact")"
    git -C "$rig" worktree add -q --detach "$artifact" origin/main
    source_sha=$(git -C "$artifact" rev-parse HEAD)
    git -C "$rig" push -qu origin \
        "$source_sha:refs/heads/polecat/ac-status"
    write_cleanup_gc_stub "$bin"
    write_cleanup_bead_json \
        "$bead_json" ac-status closed merged \
        "$source_sha" "$artifact" "" polecat/ac-status main "$source_sha"

    # Exercise both the initial dirtiness check and the final race-narrowing
    # recheck. Either command failure must preserve the artifact.
    for fail_on in 1 2; do
        : >"$calls"
        : >"$status_count"
        set +e
        GC_STUB_BEAD_JSON="$bead_json" GC_STUB_CALLS="$calls" \
            GC_CITY_PATH="$city" GC_RIG=demo GC_RIG_ROOT="$rig" \
            GIT_STATUS_STUB_REAL="$real_git" \
            GIT_STATUS_STUB_COUNT="$status_count" \
            GIT_STATUS_STUB_FAIL_ON="$fail_on" \
            PATH="$bin:$PATH" "$ARTIFACT_CLEANUP" ac-status
        status=$?
        set -e
        [[ "$status" -ne 0 && -d "$artifact" ]] ||
            fail "artifact cleanup removed work after git status check $fail_on failed"
        grep -qF 'artifact_cleanup_state=blocked' "$calls" ||
            fail "git status failure $fail_on did not record blocked cleanup"
    done
}

test_witness_status_failure_blocks_artifact_removal() {
    local tmp rig remote city artifact gate bin real_git source_sha
    local status_count output
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' RETURN
    rig="$tmp/rig"
    remote="$tmp/remote.git"
    city="$tmp/city"
    artifact="$city/.gc/worktrees/demo/artifacts/worktrees/ac-status"
    gate="$tmp/witness-artifact-safety-gate.sh"
    bin="$tmp/bin"
    status_count="$tmp/status-count"
    output="$tmp/output"
    real_git=$(command -v git)

    init_repo "$rig"
    git -C "$rig" branch -M main
    git init -q --bare "$remote"
    git -C "$remote" symbolic-ref HEAD refs/heads/main
    git -C "$rig" remote add origin "$remote"
    git -C "$rig" push -qu origin main
    mkdir -p "$(dirname "$artifact")"
    git -C "$rig" worktree add -q --detach "$artifact" origin/main
    source_sha=$(git -C "$artifact" rev-parse HEAD)
    git -C "$rig" push -qu origin \
        "$source_sha:refs/heads/polecat/ac-status"

    extract_witness_artifact_safety_gate "$gate"
    write_git_status_failure_wrapper "$bin"
    : >"$status_count"
    (
        export GIT_STATUS_STUB_REAL="$real_git"
        export GIT_STATUS_STUB_COUNT="$status_count"
        export GIT_STATUS_STUB_FAIL_ON=1
        export PATH="$bin:$PATH"
        GC_CITY_PATH="$city"
        GC_RIG=demo
        GC_RIG_ROOT="$rig"
        RECOVERY_MODE=reset
        RECOVERY_BLOCKED=false
        WORKTREE="$artifact"
        CURRENT_CANONICAL_ARTIFACT="$artifact"
        CURRENT_ARTIFACT="$artifact"
        BRANCH=polecat/ac-status
        validate_recovery_artifact_worktree() {
            printf '%s\n' "$1"
        }
        gc() {
            printf 'mail=%s\n' "$*"
        }
        # shellcheck source=/dev/null
        source "$gate"
        printf 'artifact_safe=%s recovery_blocked=%s\n' \
            "$ARTIFACT_SAFE" "$RECOVERY_BLOCKED"
        if [ "$ARTIFACT_SAFE" = true ]; then
            git -C "$rig" worktree remove "$artifact"
        fi
    ) >"$output" 2>&1

    rg -q '^artifact_safe=false recovery_blocked=true$' "$output" ||
        fail "witness treated a failed git status probe as safe"
    rg -q '^mail=mail send mayor/ -s RECOVERY_BLOCKED: artifact status unreadable for ac-status ' "$output" ||
        fail "witness status failure did not escalate to mayor"
    [[ -d "$artifact" ]] ||
        fail "witness status failure allowed artifact removal"
}

test_metadata_migration_and_consumers_are_canonical_first() {
    python3 - \
        "$POLECAT_FORMULA" \
        "$WITNESS_FORMULA" \
        "$SHUTDOWN_FORMULA" \
        "$REFINERY_FORMULA" \
        "$GASTOWN/agents/polecat/prompt.template.md" \
        "$GASTOWN/agents/witness/prompt.template.md" <<'PY'
import pathlib
import sys
import tomllib

(
    polecat_path,
    witness_path,
    shutdown_path,
    refinery_path,
    polecat_prompt_path,
    witness_prompt_path,
) = (
    pathlib.Path(arg) for arg in sys.argv[1:]
)

with polecat_path.open("rb") as handle:
    polecat = tomllib.load(handle)
workspace = next(step["description"] for step in polecat["steps"] if step["id"] == "workspace-setup")
submit = next(step["description"] for step in polecat["steps"] if step["id"] == "submit-and-exit")

required_workspace = (
    'BEAD_JSON=$(gc bd show "$WORK_BEAD_ID" --json) ||',
    'expected exactly one work bead object',
    ".artifact_dir // empty",
    ".work_dir // empty",
    "validate_artifact_worktree",
    '--set-metadata artifact_dir="$WORKTREE"',
    "--unset-metadata work_dir",
    'cd -- "$WORKTREE"',
    'ARTIFACT_HOME="$CITY_ROOT/.gc/worktrees/$GC_RIG/artifacts"',
    'EXPECTED_BRANCH="polecat/$WORK_BEAD_ID"',
    'git merge-base --is-ancestor HEAD "origin/{{base_branch}}"',
    "Canonical artifact_dir is missing or unsafe",
    'rig_namespace="$city_root/.gc/worktrees/$rig_name"',
    'provider_root" = "$rig_namespace_real/polecats',
    "canonical-only",
    "legacy-only",
    "do not create an alternate artifact",
)
for fragment in required_workspace:
    if fragment not in workspace:
        raise SystemExit(f"workspace contract missing: {fragment}")
if workspace.index(".artifact_dir // empty") > workspace.index(".work_dir // empty"):
    raise SystemExit("legacy task work_dir is read before canonical artifact_dir")
if workspace.index("validate_artifact_worktree") > workspace.index('cd -- "$WORKTREE"'):
    raise SystemExit("workspace enters the task path before defining/using its validator")
if "--set-metadata work_dir=" in workspace:
    raise SystemExit("workspace still writes deprecated task work_dir")
for forbidden in (
    'WORKTREE_PATH=$(pwd)/worktrees/',
    'WORKTREE_PATH="$PWD/worktrees/',
    'WORKTREE_PATH="${PWD}/worktrees/',
):
    if forbidden in workspace:
        raise SystemExit(f"workspace still creates provider-nested task artifacts: {forbidden}")
if "GC_WORK_DIR" in workspace:
    raise SystemExit("workspace relies on convergence-only GC_WORK_DIR")
if ".artifact-worktree-" in workspace or "WORKTREE_GENERATION" in workspace:
    raise SystemExit("workspace can create a non-canonical random artifact generation")
if 'git branch -D "$EXPECTED_BRANCH"' in workspace or 'git branch -D "$BRANCH"' in workspace:
    raise SystemExit("empty-branch recovery can delete an unrecorded work branch")
if "--unset-metadata artifact_source_sha" not in submit:
    raise SystemExit("polecat submission does not retire stale refinery artifact proof")
if "--unset-metadata artifact_cleanup_state" not in submit:
    raise SystemExit("polecat submission does not retire stale cleanup state")
canonical_guard = workspace.index("Canonical artifact_dir is missing or unsafe")
legacy_fallback = workspace.index('elif [ -n "$LEGACY_WORK_DIR" ]')
if canonical_guard > legacy_fallback:
    raise SystemExit("invalid canonical artifact_dir can fall through to legacy/new creation")

with witness_path.open("rb") as handle:
    witness = tomllib.load(handle)
witness_description = witness["description"]
recovery = next(step["description"] for step in witness["steps"] if step["id"] == "recover-orphaned-beads")
for fragment in (
    "metadata.artifact_dir",
    "validate_recovery_artifact_worktree",
    '--set-metadata artifact_dir="$WORKTREE"',
    "--unset-metadata work_dir",
    'git ls-remote --exit-code --heads origin "refs/heads/$BRANCH"',
    'merge-base --is-ancestor',
    'git -C "$GC_RIG_ROOT" worktree remove "$WORKTREE"',
    "confirm_orphan_still_unowned",
    "Canonical artifact metadata is present but invalid",
    'cd "$GC_RIG_ROOT"',
    'rig_namespace="$city_real/.gc/worktrees/$rig_name"',
    'provider_root" = "$rig_namespace_real/polecats',
    'WORKTREE_STATUS=$(git -C "$WORKTREE" status --porcelain)',
    "blocking cleanup and escalating for manual recovery",
    "exact remote branch remains recovery evidence",
):
    if fragment not in recovery:
        raise SystemExit(f"witness recovery contract missing: {fragment}")
for forbidden in ("rm -rf <worktree-path>",):
    if forbidden in recovery:
        raise SystemExit(f"witness recovery retained unsafe legacy behavior: {forbidden}")
if 'if [ -n "$BRANCH" ]; then' not in recovery:
    raise SystemExit("witness recovery can run remote/merge checks with an empty branch")
if 'worktree remove "$WORKTREE" --force' in recovery:
    raise SystemExit("witness artifact cleanup force-removes a worktree")
if '[ -z "$(git -C "$WORKTREE" status --porcelain)" ]' in recovery:
    raise SystemExit("witness cleanup still treats git status failure as clean")
for fragment in (
    "target is canonical",
    "exact successfully\n  handed-off source ref is retained",
    "recovery never deletes that ref",
):
    if fragment not in witness_description:
        raise SystemExit(f"witness role contract missing retained-ref policy: {fragment}")
if recovery.index("confirm_orphan_still_unowned") > recovery.index(
    "--set-metadata artifact_dir="
):
    raise SystemExit("witness migration mutates metadata before its fresh owner fence")
if recovery.index("confirm_orphan_still_unowned") > recovery.index(
    'checkout -b "$EXPECTED_BRANCH"'
):
    raise SystemExit("witness branch recovery runs before its fresh owner fence")

with shutdown_path.open("rb") as handle:
    shutdown = tomllib.load(handle)
execute = next(step["description"] for step in shutdown["steps"] if step["id"] == "execute")
if execute.index(".metadata.artifact_dir // empty") > execute.index(".metadata.work_dir // empty"):
    raise SystemExit("shutdown dance does not read canonical artifact_dir first")
if 'validate_progress_artifact_dir' not in execute:
    raise SystemExit("shutdown dance lacks the artifact path validator")
if 'find "$artifact_dir"' not in execute or 'find "$work_dir"' in execute:
    raise SystemExit("shutdown dance can scan an unvalidated legacy/provider work_dir")
for fragment in (
    'progress="unknown"',
    'gc bd --rig "$target_rig" list',
    'gc bd --rig "$target_rig" show',
    'if [ "$progress" = "unknown" ]; then',
    'canonical_path="$rig_namespace_real/artifacts/worktrees/$bead_id"',
    '[ "$candidate_real" != "$provider_real/worktrees/$bead_id" ]',
):
    if fragment not in execute:
        raise SystemExit(f"shutdown fail-closed rig lookup contract missing: {fragment}")

with refinery_path.open("rb") as handle:
    refinery = tomllib.load(handle)
rebase = next(step["description"] for step in refinery["steps"] if step["id"] == "rebase")
find_work = next(step["description"] for step in refinery["steps"] if step["id"] == "find-work")
merge = next(step["description"] for step in refinery["steps"] if step["id"] == "merge-push")
if '--set-metadata artifact_source_sha="$ARTIFACT_SOURCE_SHA"' not in rebase:
    raise SystemExit("refinery does not durably capture the pre-rebase artifact SHA")
if "--set-metadata artifact_cleanup_state=pending" not in rebase:
    raise SystemExit("refinery does not arm the durable cleanup retry marker")
if "gc gastown task-artifact-cleanup" not in find_work:
    raise SystemExit("refinery does not retry a closed pending artifact before new work")
for fragment in (
    "**Successful task-artifact cleanup (direct and mr):**",
    'gc gastown task-artifact-cleanup "$WORK"',
    '--set-metadata pr_head_sha="$PR_HEAD_SHA"',
    'if [ "$PR_HEAD_SHA" != "$EXPECTED_PR_HEAD" ]',
    "artifact_cleanup_state=pending",
    "artifact_cleanup_state=complete",
    "ARTIFACT_CLEANUP_DEFERRED",
    "MR reconciliation MUST run",
    "retains that source ref after local cleanup",
):
    if fragment not in merge:
        raise SystemExit(f"refinery artifact cleanup contract missing: {fragment}")
if 'delete_merged_branches = "true": `git push origin --delete $BRANCH`' in merge:
    raise SystemExit("successful direct cleanup can still delete its recovery source ref")
if "leak the merged branch" in merge:
    raise SystemExit("refinery still describes an intentionally retained source ref as leaked")
polecat_prompt = polecat_prompt_path.read_text(encoding="utf-8")
witness_prompt = witness_prompt_path.read_text(encoding="utf-8")
if "`metadata.artifact_dir`" not in polecat_prompt:
    raise SystemExit("polecat prompt does not document canonical artifact_dir")
for fragment in (
    "$GC_CITY_PATH/.gc/worktrees/$GC_RIG/artifacts/worktrees/<bead-id>",
    'gc bd formula show mol-polecat-work --rig "$GC_RIG"',
    "Execute its `workspace-setup` step before reading or editing task source",
    "Do not search\nthe filesystem for formula files",
    "<provider-home>/worktrees/<bead-id>",
    "same-repository paths in another city, rig, or namespace are rejected",
):
    if fragment not in polecat_prompt:
        raise SystemExit(f"polecat prompt lacks fail-closed workspace guidance: {fragment}")
if "<home>/worktrees/vg-1jp" in polecat_prompt:
    raise SystemExit("polecat prompt still advertises the legacy provider-nested path")
if "new nested worktree is created" in workspace:
    raise SystemExit("workspace recipe still describes canonical artifacts as nested worktrees")
if "`metadata.artifact_dir`" not in witness_prompt:
    raise SystemExit("witness prompt does not document canonical artifact_dir")
for forbidden in (
    "Delete branches after merge",
    "previous location is disposable",
    "git worktree remove <path> --force",
):
    if forbidden in witness_prompt:
        raise SystemExit(f"witness prompt retains unsafe source-ref guidance: {forbidden}")
PY
}

test_validator_rejects_provider_and_unrelated_paths
test_workspace_creation_uses_canonical_sibling
test_workspace_metadata_reads_fail_closed_without_pipefail
test_workspace_collision_fails_closed_without_alternate
test_workspace_rejects_redirected_artifact_root_before_creation
test_worktree_setup_ignores_gc_without_mutating_tracked_or_global_ignores
test_worktree_sync_ignores_codex_skills_but_rejects_unrelated_dirt
test_worktree_sync_preserves_task_state_and_only_fast_forwards_stable_branch
test_shutdown_probe_scopes_rig_and_fails_closed
test_cleanup_command_is_idempotent_and_mr_retryable
test_cleanup_command_rejects_cross_city_and_dirty_artifacts
test_cleanup_requires_live_remote_evidence_and_stable_state
test_git_status_failures_preserve_artifacts_and_provider_state
test_witness_status_failure_blocks_artifact_removal
if rg -n 'worktree remove .*--force' "$ARTIFACT_CLEANUP" >/dev/null; then
    fail "artifact cleanup contains a force-removal path"
fi
test_metadata_migration_and_consumers_are_canonical_first

echo "polecat artifact_dir tests passed"
