#!/bin/sh
# worker-worktree.sh — make a worker session's working directory a git
# worktree of its rig, before the first turn.
#
# Built to run as a Gas City agent `pre_start` command. gc creates `work_dir`,
# then runs pre_start with cwd = $GC_DIR and the session environment, then
# stages skills/hooks into the same directory and starts the provider there.
#
#   # agents/<name>/agent.toml (or a [[patches.agent]] entry)
#   work_dir  = ".worktrees/{{.Rig}}/lane-{{.AgentBase}}"
#   pre_start = ["sh {{.CityRoot}}/.gc/scripts/worker-worktree.sh"]
#
# Inputs (flags win over environment):
#   --workdir DIR    directory to turn into a worktree   (default $GC_DIR, else $PWD)
#   --rig-root DIR   the rig checkout whose repository to use (default $GC_RIG_ROOT)
#   --bead ID        bead whose branch to reuse or create (default $GC_TRIGGER_BEAD_ID;
#                    empty leaves the worktree detached at --base)
#   --base REF       start point for a new branch (default <remote>/HEAD, else <remote>/main)
#   --remote NAME    remote to fetch and match branches on (default origin)
#   --no-fetch       do not fetch before resolving refs
#
# Contract:
#   * The rig root's working tree is never touched: the script only reads
#     refs, fetches, and registers worktrees from it.
#   * Nothing is deleted. A work dir that is not a worktree of this repository
#     and is not empty is moved to <workdir>.aside-<utc stamp>; a worktree with
#     tracked modifications is moved aside the same way before a fresh one
#     replaces it. Untracked files (staged skills, hooks, node_modules) do not
#     count as modifications and ride along on a branch switch.
#   * Bead branch: exactly one branch (local or on the remote) whose name
#     contains the bead id is reused. None: a new branch named <bead id> is
#     created from --base. Several: fail closed and list them.
#   * A bead branch already checked out in another worktree is not stolen: the
#     work dir is left detached at that branch's tip and a WARN line names the
#     other worktree.
#   * Idempotent. Re-running on a work dir that is already on the right branch
#     changes nothing (besides the fetch).
#   * On success prints one line: WORKTREE <dir> <branch|detached> <head sha>.

set -eu

log() { printf 'worker-worktree: %s\n' "$*" >&2; }
warn() { printf 'worker-worktree: WARN %s\n' "$*" >&2; }
die() { printf 'worker-worktree: ERROR %s\n' "$*" >&2; exit 1; }

WORKDIR="${GC_DIR:-}"
RIG_ROOT="${GC_RIG_ROOT:-}"
BEAD="${GC_TRIGGER_BEAD_ID:-}"
BASE=""
REMOTE="origin"
FETCH=1

while [ "$#" -gt 0 ]; do
    case "$1" in
        --workdir) [ "$#" -ge 2 ] || die "--workdir needs a value"; WORKDIR="$2"; shift 2 ;;
        --workdir=*) WORKDIR="${1#--workdir=}"; shift ;;
        --rig-root) [ "$#" -ge 2 ] || die "--rig-root needs a value"; RIG_ROOT="$2"; shift 2 ;;
        --rig-root=*) RIG_ROOT="${1#--rig-root=}"; shift ;;
        --bead) [ "$#" -ge 2 ] || die "--bead needs a value"; BEAD="$2"; shift 2 ;;
        --bead=*) BEAD="${1#--bead=}"; shift ;;
        --base) [ "$#" -ge 2 ] || die "--base needs a value"; BASE="$2"; shift 2 ;;
        --base=*) BASE="${1#--base=}"; shift ;;
        --remote) [ "$#" -ge 2 ] || die "--remote needs a value"; REMOTE="$2"; shift 2 ;;
        --remote=*) REMOTE="${1#--remote=}"; shift ;;
        --no-fetch) FETCH=0; shift ;;
        -h|--help) sed -n '2,40p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) die "unknown argument: $1" ;;
    esac
done

[ -n "$WORKDIR" ] || WORKDIR="$PWD"
[ -n "$RIG_ROOT" ] || die "no rig root: pass --rig-root or set GC_RIG_ROOT"
[ -d "$RIG_ROOT" ] || die "rig root is not a directory: $RIG_ROOT"
command -v git >/dev/null 2>&1 || die "git is required on PATH"

# Resolve to absolute, physical paths so worktree bookkeeping compares cleanly.
abs() { (cd "$1" 2>/dev/null && pwd -P); }
RIG_ROOT="$(abs "$RIG_ROOT")" || die "cannot enter rig root"
case "$WORKDIR" in
    /*) ;;
    *) WORKDIR="$PWD/$WORKDIR" ;;
esac

RIG_COMMON="$(git -C "$RIG_ROOT" rev-parse --git-common-dir 2>/dev/null)" \
    || die "rig root is not a git repository: $RIG_ROOT"
case "$RIG_COMMON" in
    /*) ;;
    *) RIG_COMMON="$RIG_ROOT/$RIG_COMMON" ;;
esac
RIG_COMMON="$(abs "$RIG_COMMON")" || die "cannot resolve the rig's git dir"

# The work dir must not be the rig root itself or sit inside its checkout.
if [ -d "$WORKDIR" ]; then
    WORKDIR_ABS="$(abs "$WORKDIR")"
    [ "$WORKDIR_ABS" != "$RIG_ROOT" ] || die "work dir is the rig root; give the agent a work_dir outside it"
    case "$WORKDIR_ABS/" in
        "$RIG_ROOT"/*) die "work dir $WORKDIR_ABS is inside the rig root $RIG_ROOT" ;;
    esac
    WORKDIR="$WORKDIR_ABS"
fi

git_rig() { git -C "$RIG_ROOT" "$@"; }

if [ "$FETCH" -eq 1 ]; then
    if ! git_rig fetch --quiet --prune "$REMOTE" 2>/dev/null; then
        warn "fetch from $REMOTE failed; continuing with the refs already present"
    fi
fi

if [ -z "$BASE" ]; then
    if BASE="$(git_rig symbolic-ref --quiet --short "refs/remotes/$REMOTE/HEAD" 2>/dev/null)"; then
        :
    elif git_rig show-ref --verify --quiet "refs/remotes/$REMOTE/main"; then
        BASE="$REMOTE/main"
    elif git_rig show-ref --verify --quiet "refs/remotes/$REMOTE/master"; then
        BASE="$REMOTE/master"
    else
        BASE="HEAD"
        warn "no $REMOTE/HEAD, $REMOTE/main or $REMOTE/master; new branches start at the rig root's HEAD"
    fi
fi
git_rig rev-parse --verify --quiet "$BASE^{commit}" >/dev/null || die "base ref does not resolve: $BASE"

# --- bead branch resolution -------------------------------------------------
# TARGET_LOCAL: local branch to check out.  TARGET_REMOTE: remote branch to track
# when no local branch exists.  TARGET_NEW: branch to create from BASE.
TARGET_LOCAL=""
TARGET_REMOTE=""
TARGET_NEW=""
if [ -n "$BEAD" ]; then
    candidates="$(
        {
            git_rig for-each-ref --format='%(refname:short)' refs/heads
            git_rig for-each-ref --format='%(refname:short)' "refs/remotes/$REMOTE" \
                | grep -v "^$REMOTE/HEAD\$" | sed "s|^$REMOTE/||"
        } | grep -F -- "$BEAD" | sort -u
    )" || true
    count="$(printf '%s\n' "$candidates" | grep -c . || true)"
    if [ "$count" -gt 1 ]; then
        die "several branches name bead $BEAD; pass --bead with a unique id or --base and no bead: $(printf '%s\n' "$candidates" | tr '\n' ' ')"
    elif [ "$count" -eq 1 ]; then
        if git_rig show-ref --verify --quiet "refs/heads/$candidates"; then
            TARGET_LOCAL="$candidates"
        else
            TARGET_REMOTE="$candidates"
        fi
    else
        TARGET_NEW="$BEAD"
    fi
fi

# Where (if anywhere) is a local branch checked out?  Prints the worktree path.
checked_out_at() {
    git_rig worktree list --porcelain | awk -v want="refs/heads/$1" '
        $1 == "worktree" { wt = $2 }
        $1 == "branch" && $2 == want { print wt }
    '
}

move_aside() {
    stamp="$(date -u +%Y%m%dT%H%M%SZ)"
    aside="$1.aside-$stamp"
    n=0
    while [ -e "$aside" ]; do n=$((n + 1)); aside="$1.aside-$stamp-$n"; done
    if git -C "$1" rev-parse --git-common-dir >/dev/null 2>&1 \
        && [ "$(abs "$(git -C "$1" rev-parse --git-common-dir)")" = "$RIG_COMMON" ]; then
        git_rig worktree move "$1" "$aside" >/dev/null 2>&1 || mv "$1" "$aside"
    else
        mv "$1" "$aside"
    fi
    warn "moved aside $1 -> $aside ($2); nothing was deleted"
}

# --- classify the work dir ----------------------------------------------------
IS_WORKTREE=0
if [ -d "$WORKDIR" ] && [ -e "$WORKDIR/.git" ]; then
    wd_common="$(git -C "$WORKDIR" rev-parse --git-common-dir 2>/dev/null || true)"
    case "$wd_common" in
        "") ;;
        /*) ;;
        *) wd_common="$WORKDIR/$wd_common" ;;
    esac
    if [ -n "$wd_common" ] && [ -d "$wd_common" ] && [ "$(abs "$wd_common")" = "$RIG_COMMON" ]; then
        IS_WORKTREE=1
    fi
fi

if [ "$IS_WORKTREE" -eq 0 ] && [ -d "$WORKDIR" ]; then
    if [ -n "$(ls -A "$WORKDIR" 2>/dev/null)" ]; then
        move_aside "$WORKDIR" "not a worktree of $RIG_ROOT"
    fi
fi

if [ "$IS_WORKTREE" -eq 1 ]; then
    if [ -n "$(git -C "$WORKDIR" status --porcelain --untracked-files=no 2>/dev/null)" ]; then
        move_aside "$WORKDIR" "tracked modifications present"
        IS_WORKTREE=0
    fi
fi

# --- act ----------------------------------------------------------------------
add_worktree() {
    # $1 = mode: local|remote|new|detached
    [ -d "$WORKDIR" ] || mkdir -p "$WORKDIR"
    case "$1" in
        local)    git_rig worktree add --quiet "$WORKDIR" "$TARGET_LOCAL" ;;
        remote)   git_rig worktree add --quiet --track -b "$TARGET_REMOTE" "$WORKDIR" "$REMOTE/$TARGET_REMOTE" ;;
        new)      git_rig worktree add --quiet -b "$TARGET_NEW" "$WORKDIR" "$BASE" ;;
        detached) git_rig worktree add --quiet --detach "$WORKDIR" "$2" ;;
    esac
}

switch_worktree() {
    # $1 = mode as above; the work dir is a clean worktree already.
    case "$1" in
        local)    git -C "$WORKDIR" switch --quiet "$TARGET_LOCAL" ;;
        remote)   git -C "$WORKDIR" switch --quiet -c "$TARGET_REMOTE" --track "$REMOTE/$TARGET_REMOTE" ;;
        new)      git -C "$WORKDIR" switch --quiet -c "$TARGET_NEW" "$BASE" ;;
        detached) git -C "$WORKDIR" switch --quiet --detach "$2" ;;
    esac
}

MODE=""
DETACH_AT=""
if [ -n "$TARGET_LOCAL" ]; then
    elsewhere="$(checked_out_at "$TARGET_LOCAL" | grep -v -x -F -- "$WORKDIR" || true)"
    if [ -n "$elsewhere" ]; then
        MODE="detached"
        DETACH_AT="$TARGET_LOCAL"
        warn "branch $TARGET_LOCAL is checked out at $elsewhere; leaving $WORKDIR detached at its tip. Create your own branch before committing, or work in that worktree."
    else
        MODE="local"
    fi
elif [ -n "$TARGET_REMOTE" ]; then
    MODE="remote"
elif [ -n "$TARGET_NEW" ]; then
    MODE="new"
else
    MODE="detached"
    DETACH_AT="$BASE"
fi

if [ "$IS_WORKTREE" -eq 1 ]; then
    current="$(git -C "$WORKDIR" rev-parse --abbrev-ref HEAD 2>/dev/null || echo HEAD)"
    case "$MODE" in
        local)  [ "$current" = "$TARGET_LOCAL" ] || switch_worktree local ;;
        remote) switch_worktree remote ;;
        new)    if git_rig show-ref --verify --quiet "refs/heads/$TARGET_NEW"; then
                    # Created by a concurrent run between resolution and now.
                    TARGET_LOCAL="$TARGET_NEW"; switch_worktree local
                else
                    switch_worktree new
                fi ;;
        detached)
                if [ -n "$BEAD" ]; then
                    switch_worktree detached "$DETACH_AT"
                elif [ "$current" = "HEAD" ]; then
                    # No bead and already detached: refresh to the base tip.
                    switch_worktree detached "$DETACH_AT"
                fi
                # No bead and on a branch: leave the lane where a previous
                # session put it; the worker owns that branch.
                ;;
    esac
else
    if [ "$MODE" = "detached" ]; then
        add_worktree detached "$DETACH_AT"
    else
        add_worktree "$MODE"
    fi
fi

WORKDIR="$(abs "$WORKDIR")"
branch="$(git -C "$WORKDIR" rev-parse --abbrev-ref HEAD 2>/dev/null || echo HEAD)"
[ "$branch" != "HEAD" ] || branch="detached"
sha="$(git -C "$WORKDIR" rev-parse --short HEAD 2>/dev/null || echo unknown)"
printf 'WORKTREE %s %s %s\n' "$WORKDIR" "$branch" "$sha"
