#!/usr/bin/env bash
set -euo pipefail

# Shared-drain worktree gate.
#
# A same-session shared drain fans every convoy member out to an item formula
# whose implementation step reads the authoritative worktree from `work_dir`
# metadata on the source anchor. The drain step itself is a control bead, so no
# agent prose ever runs that could create that worktree. This check is the
# writer: it creates or reuses ONE deterministic worktree per drain and persists
# its path on each item's source anchor, so item N sees the commits items 1..N-1
# made.
#
# Formula-check mode reads $GC_BEAD_ID and resolves everything from beads. The
# three-argument mode takes <launcher-root> <drain-control-id> <source-anchor-id>
# and is what manual smoke runs and the tests use.

fail() {
  echo "gc-shared-worktree: $*" >&2
  exit 1
}

command -v gc >/dev/null 2>&1 || fail "gc is required on PATH"
command -v git >/dev/null 2>&1 || fail "git is required on PATH"
command -v python3 >/dev/null 2>&1 || fail "python3 is required on PATH"

metadata_value() {
  # metadata_value <json> <key> -> prints metadata[key] or empty.
  # `gc bd show --json` marshals a one-element list on the routed path and a
  # bare object elsewhere, so both shapes have to unwrap.
  printf '%s' "$1" | python3 -c '
import json
import sys

key = sys.argv[1]
try:
    data = json.load(sys.stdin)
except Exception:
    print("")
    raise SystemExit(0)
if isinstance(data, list):
    data = data[0] if data else {}
metadata = data.get("metadata") if isinstance(data, dict) else {}
value = metadata.get(key, "") if isinstance(metadata, dict) else ""
print(value if isinstance(value, str) else "")
' "$2"
}

if [ "$#" -eq 0 ]; then
  BEAD_ID="${GC_BEAD_ID:-}"
  [ -n "$BEAD_ID" ] || fail "GC_BEAD_ID is required in formula-check mode"
  STEP_JSON="$(gc bd show "$BEAD_ID" --json 2>/dev/null)" || fail "gc bd show $BEAD_ID failed"
  ROOT_ID="$(metadata_value "$STEP_JSON" "gc.root_bead_id")"
  [ -n "$ROOT_ID" ] || fail "step $BEAD_ID is missing gc.root_bead_id"
  ROOT_JSON="$(gc bd show "$ROOT_ID" --json 2>/dev/null)" || fail "gc bd show $ROOT_ID failed"
  LAUNCHER_RAW="$(metadata_value "$ROOT_JSON" "gc.work_dir")"
  # gc.work_dir reaches the item run's workflow root from a reconcile tick that
  # fires after the first step is in_progress, and the drain path never stamps
  # it, so an early attempt can legitimately read it empty. The reconciler also
  # refuses the back-fill for pooled/ephemeral claimant sessions; there the
  # writer is the prepare step's operator, whose prompt has it record its
  # verified rig root on the workflow root. Fail closed and let
  # the check's retry budget cover the gap: inferring a launcher root from $PWD
  # would build the drain's worktree in whatever repository this check happened
  # to run from, which is far worse than one retry.
  [ -n "$LAUNCHER_RAW" ] || fail "workflow root $ROOT_ID has no gc.work_dir yet"
  DRAIN_CONTROL_ID="$(metadata_value "$ROOT_JSON" "gc.drain_control_id")"
  SOURCE_ANCHOR_ID="$(metadata_value "$ROOT_JSON" "gc.drain_member_id")"
  DRAIN_INDEX="$(metadata_value "$ROOT_JSON" "gc.drain_index")"
  case "$DRAIN_INDEX" in
    "" | *[!0-9]*) fail "workflow root $ROOT_ID has invalid gc.drain_index: $DRAIN_INDEX" ;;
  esac
elif [ "$#" -eq 3 ]; then
  LAUNCHER_RAW="$1"
  DRAIN_CONTROL_ID="$2"
  SOURCE_ANCHOR_ID="$3"
else
  fail "usage: prepare-shared-worktree.sh [<launcher-root> <drain-control-id> <source-anchor-id>]"
fi

case "$DRAIN_CONTROL_ID" in
  "" | *[!A-Za-z0-9._-]*) fail "invalid drain control id: $DRAIN_CONTROL_ID" ;;
esac
case "$SOURCE_ANCHOR_ID" in
  "" | *[!A-Za-z0-9._-]*) fail "invalid source anchor id: $SOURCE_ANCHOR_ID" ;;
esac

WORK_DIR="$(cd "$LAUNCHER_RAW" 2>/dev/null && pwd -P)" || fail "workflow work dir does not exist: $LAUNCHER_RAW"
REPO_ROOT_RAW="$(git -C "$WORK_DIR" rev-parse --show-toplevel 2>/dev/null)" || fail "workflow work dir is not inside a git worktree: $WORK_DIR"
REPO_ROOT="$(cd "$REPO_ROOT_RAW" 2>/dev/null && pwd -P)" || fail "repository root does not exist: $REPO_ROOT_RAW"
LAUNCHER_ROOT="$REPO_ROOT"

canonical_common_dir() {
  repo="$1"
  raw="$(git -C "$repo" rev-parse --git-common-dir 2>/dev/null)" || return 1
  python3 - "$repo" "$raw" <<'PY'
from pathlib import Path
import sys

repo = Path(sys.argv[1])
path = Path(sys.argv[2])
if not path.is_absolute():
    path = repo / path
print(path.resolve(strict=True))
PY
}

LAUNCHER_COMMON_DIR="$(canonical_common_dir "$LAUNCHER_ROOT")" || fail "cannot resolve launcher git common dir"

# One worktree per drain, keyed by the drain control bead, beside do-work's
# per-source-anchor worktrees inside the launcher checkout. Bead ids are unique,
# so the shared- prefix cannot collide with a per-anchor directory.
WORKTREE="$LAUNCHER_ROOT/worktrees/shared-$DRAIN_CONTROL_ID"

SHOW_JSON="$(gc bd show "$SOURCE_ANCHOR_ID" --json 2>/dev/null)" || fail "gc bd show $SOURCE_ANCHOR_ID failed"
CURRENT_WORK_DIR="$(metadata_value "$SHOW_JSON" "work_dir")"

if [ -n "$CURRENT_WORK_DIR" ]; then
  CURRENT_WORK_DIR="$(cd "$CURRENT_WORK_DIR" 2>/dev/null && pwd -P)" || fail "source anchor $SOURCE_ANCHOR_ID has a missing work_dir"
  [ "$CURRENT_WORK_DIR" = "$WORKTREE" ] || fail "source anchor $SOURCE_ANCHOR_ID points at a different worktree: $CURRENT_WORK_DIR"
fi

if [ ! -e "$WORKTREE" ]; then
  [ -z "$CURRENT_WORK_DIR" ] || fail "recorded shared worktree is missing: $WORKTREE"
  # Base the shared worktree on the freshly fetched remote default branch, never
  # on the launcher's local HEAD: the launcher checkout can be behind origin, and
  # a drain based on a stale commit redoes work that already landed. Read the
  # local ref first and only touch the network if it is missing.
  #
  # Both reads end in `|| true` because an unset ref makes `git symbolic-ref`
  # exit 128, and under `set -euo pipefail` that status propagates out of the
  # assignment and kills the script -- silently, before the refresh below can
  # run. The empty string is the answer this branch is written to handle.
  DEFAULT_BRANCH=$(git -C "$LAUNCHER_ROOT" symbolic-ref --short refs/remotes/origin/HEAD 2>/dev/null | sed 's|^origin/||' || true)
  if [ -z "$DEFAULT_BRANCH" ]; then
    # refs/remotes/origin/HEAD is written by `git clone` and refreshed by
    # `git remote set-head origin --auto`. It is NOT written by `git init` plus
    # `git fetch`, which is how actions/checkout and several of our own
    # checkouts are built, so this refresh is load-bearing rather than
    # defensive. The fetch below still guarantees the base is current.
    git -C "$LAUNCHER_ROOT" remote set-head origin --auto >/dev/null 2>&1 || true
    DEFAULT_BRANCH=$(git -C "$LAUNCHER_ROOT" symbolic-ref --short refs/remotes/origin/HEAD 2>/dev/null | sed 's|^origin/||' || true)
  fi
  [ -n "$DEFAULT_BRANCH" ] || fail "cannot resolve the remote default branch in $LAUNCHER_ROOT"

  # Exec checks run under a sandboxed gate environment with no SSH agent and no
  # inherited GIT_* configuration, so a credentialed fetch can fail in ways that
  # are invisible unless git's own diagnosis is carried into the failure.
  if ! GIT_ERR="$(git -C "$LAUNCHER_ROOT" fetch --prune origin "$DEFAULT_BRANCH" 2>&1 >/dev/null)"; then
    fail "cannot fetch origin/$DEFAULT_BRANCH in $LAUNCHER_ROOT: $GIT_ERR"
  fi

  mkdir -p "$(dirname "$WORKTREE")"
  # Drop registrations whose directories are gone. Our worktrees live inside the
  # launcher checkout, so a stray clean or `rm -rf` leaves a live registration
  # that makes every later `worktree add` refuse for the whole retry budget.
  # Prune only forgets worktrees that are already missing from disk.
  git -C "$LAUNCHER_ROOT" worktree prune >/dev/null 2>&1 || true
  if ! GIT_ERR="$(git -C "$LAUNCHER_ROOT" worktree add --detach "$WORKTREE" "origin/$DEFAULT_BRANCH" 2>&1 >/dev/null)"; then
    fail "failed to create shared worktree $WORKTREE: $GIT_ERR"
  fi
fi

# Reuse skips the fetch on purpose. Later items must see the commits earlier
# items made in this worktree, which is the whole point of a shared drain.
[ -d "$WORKTREE" ] || fail "shared worktree path is not a directory: $WORKTREE"
WORKTREE="$(cd "$WORKTREE" && pwd -P)"
[ "$WORKTREE" != "$LAUNCHER_ROOT" ] || fail "shared worktree must differ from launcher checkout"
INSIDE="$(git -C "$WORKTREE" rev-parse --is-inside-work-tree 2>/dev/null)" || fail "shared path is not a git worktree: $WORKTREE"
[ "$INSIDE" = "true" ] || fail "shared path is not a git worktree: $WORKTREE"
WORKTREE_ROOT_RAW="$(git -C "$WORKTREE" rev-parse --show-toplevel 2>/dev/null)" || fail "cannot resolve shared worktree root"
WORKTREE_ROOT="$(cd "$WORKTREE_ROOT_RAW" && pwd -P)"
[ "$WORKTREE_ROOT" = "$WORKTREE" ] || fail "shared path is not the worktree root: path=$WORKTREE root=$WORKTREE_ROOT"
WORKTREE_COMMON_DIR="$(canonical_common_dir "$WORKTREE")" || fail "cannot resolve shared worktree git common dir"
[ "$WORKTREE_COMMON_DIR" = "$LAUNCHER_COMMON_DIR" ] || fail "shared worktree belongs to a different repository"

# The SDK stamps the real drain member on the item root as gc.drain_member_id.
# Never persist work_dir anywhere else -- a synthetic drain-unit convoy is not
# the source anchor the implementation step reads.
gc bd update "$SOURCE_ANCHOR_ID" --set-metadata "work_dir=$WORKTREE" >/dev/null || fail "failed to persist work_dir on $SOURCE_ANCHOR_ID"
UPDATED_JSON="$(gc bd show "$SOURCE_ANCHOR_ID" --json 2>/dev/null)" || fail "failed to read back source anchor $SOURCE_ANCHOR_ID"
RECORDED_WORK_DIR="$(metadata_value "$UPDATED_JSON" "work_dir")"
[ "$RECORDED_WORK_DIR" = "$WORKTREE" ] || fail "source anchor $SOURCE_ANCHOR_ID did not retain the shared worktree"

printf '%s\n' "$WORKTREE"
