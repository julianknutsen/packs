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
#   WORKER_WORKTREE_LOCK_WAIT  seconds to wait for another run on the same repository (default 120)
#
# Contract:
#   * One path, serialized. Every run on a repository takes the lock
#     <git common dir>/worker-worktree.lock (mkdir) before it reads or changes
#     anything and releases it on exit. A stale lock (owner dead or a zombie, or
#     no owner published for two minutes) is cleared only under a second,
#     atomic reclaim lock and only after re-checking it is still the same stale
#     lock, so two contenders cannot both clear it and a live lock is never
#     removed; a reclaim lock stuck for two minutes fails the run closed. Two
#     sessions preparing lanes at once therefore cannot race on branch
#     creation, on the same lane, or on the aside destination.
#   * The work dir is resolved on the real filesystem: it must exist, or its
#     parent must exist and its last component must be a plain name (no `.`,
#     `..`, or deeper missing levels). Symlinks resolve. The rig root itself,
#     anything inside it, and any ancestor of it are refused before any change.
#   * The rig root's working tree is never touched: the script only reads
#     refs, fetches, and registers worktrees from it.
#   * Nothing is deleted. A non-empty work dir that is not a git checkout is
#     moved to <workdir>.aside-<utc stamp>; a worktree of this repository with
#     tracked modifications is moved aside the same way (through `git worktree
#     move`, so registration follows; if git refuses, e.g. locked, the script
#     fails in place); a branch switch that would overwrite an ignored file
#     (`git switch --no-overwrite-ignore`) moves the worktree aside instead. A
#     checkout of ANOTHER repository at the work dir is refused, not moved.
#     Untracked files (staged skills, hooks, node_modules) do not count as
#     modifications and ride along on a branch switch.
#   * Bead branch: exactly one branch (local or on the remote) whose name
#     contains the bead id as a whole token — `gp-abc1`, `fix/gp-abc1-x`, never
#     `gp-abc10` — is reused. None: a new branch named <bead id> is created
#     from --base. Several: fail closed and list them.
#   * A bead branch already checked out in another worktree is not stolen: the
#     work dir is left detached at that branch's tip and a WARN line says so.
#     Create your own branch in this work dir before committing.
#   * No bead: the work dir is detached at --base, whatever branch it was on
#     (the branch itself is kept; nothing is deleted).
#   * Idempotent for a clean work dir already on the requested branch (or
#     detached at the requested commit): re-running changes nothing besides
#     the fetch. The result is verified before the script reports success.
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
LOCK_WAIT="${WORKER_WORKTREE_LOCK_WAIT:-120}"

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
        -h|--help) sed -n '2,60p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) die "unknown argument: $1" ;;
    esac
done

[ -n "$WORKDIR" ] || WORKDIR="$PWD"
[ -n "$RIG_ROOT" ] || die "no rig root: pass --rig-root or set GC_RIG_ROOT"
[ -d "$RIG_ROOT" ] || die "rig root is not a directory: $RIG_ROOT"
command -v git >/dev/null 2>&1 || die "git is required on PATH"

# abs DIR: absolute, symlink-resolved path of an existing directory, with `..`
# resolved physically (cd -P), so "<symlink>/.." lands where the kernel puts it.
# Callers check the exit status; a failure is never silently folded into a path.
abs() { (cd -P "$1" 2>/dev/null && pwd -P); }

# --- resolve and check paths, before anything else --------------------------------

RIG_ROOT="$(abs "$RIG_ROOT")" || die "cannot enter rig root: $RIG_ROOT"

case "$WORKDIR" in
    /*) ;;
    *) WORKDIR="$PWD/$WORKDIR" ;;
esac
while [ "${WORKDIR%/}" != "$WORKDIR" ] && [ "$WORKDIR" != "/" ]; do WORKDIR="${WORKDIR%/}"; done
if [ -d "$WORKDIR" ]; then
    WORKDIR="$(abs "$WORKDIR")" || die "cannot enter work dir: $WORKDIR"
else
    leaf="${WORKDIR##*/}"
    parent="${WORKDIR%/*}"
    [ -n "$parent" ] || parent="/"
    case "$leaf" in
        ""|.|..) die "work dir must end in a plain directory name: $WORKDIR" ;;
    esac
    [ -d "$parent" ] || die "parent of work dir does not exist: $parent (gc creates work_dir before pre_start; create the parent first)"
    parent="$(abs "$parent")" || die "cannot enter parent of work dir: $parent"
    WORKDIR="$parent/$leaf"
fi

[ "$WORKDIR" != "$RIG_ROOT" ] || die "work dir is the rig root; give the agent a work_dir outside it"
case "$WORKDIR/" in
    "$RIG_ROOT"/*) die "work dir $WORKDIR is inside the rig root $RIG_ROOT" ;;
esac
case "$RIG_ROOT/" in
    "$WORKDIR"/*) die "work dir $WORKDIR is an ancestor of the rig root $RIG_ROOT" ;;
esac

RIG_COMMON="$(git -C "$RIG_ROOT" rev-parse --git-common-dir 2>/dev/null)" \
    || die "rig root is not a git repository: $RIG_ROOT"
case "$RIG_COMMON" in
    /*) ;;
    *) RIG_COMMON="$RIG_ROOT/$RIG_COMMON" ;;
esac
RIG_COMMON="$(abs "$RIG_COMMON")" || die "cannot resolve the rig's git dir"

git_rig() { git -C "$RIG_ROOT" "$@"; }

# --- lock: one run per repository at a time -----------------------------------------
# mkdir is the atomic acquire. A stale lock (owner pid dead or a zombie, or no
# owner published for over two minutes) is cleared only by the contender that
# wins a second mkdir, the reclaim lock, and only after it re-reads the lock
# and finds it still stale: a contender that observed the stale lock, lost the
# race, and arrives late finds a live owner (or no lock) under the reclaim lock
# and does nothing. So a live lock is never removed by a late contender. A
# reclaim lock left by a crash is never removed automatically (removing it by
# age would reopen the same race one level down): after two minutes the script
# fails closed and names the directory for an operator. Test-only pauses
# (WORKER_WORKTREE_TEST_PAUSE_BEFORE_RECLAIM / _AFTER_ACQUIRE, seconds) exist so
# the two-contender interleavings can be exercised deterministically.

LOCK="$RIG_COMMON/worker-worktree.lock"
LOCKED=0
release_lock() {
    if [ "$LOCKED" -eq 1 ]; then
        rm -f "$LOCK/pid" 2>/dev/null || true
        rmdir "$LOCK" 2>/dev/null || true
        LOCKED=0
    fi
}

# lock_is_stale: the lock at $LOCK has a dead/zombie owner, or no owner for
# over two minutes (a run died between mkdir and the pid write; a live run
# publishes within milliseconds). Sets `owner`.
lock_is_stale() {
    [ -d "$LOCK" ] || return 1
    owner="$(cat "$LOCK/pid" 2>/dev/null || true)"
    if [ -n "$owner" ]; then
        ! kill -0 "$owner" 2>/dev/null || owner_is_zombie "$owner"
    else
        [ -n "$(find "$LOCK" -maxdepth 0 -mmin +2 2>/dev/null)" ]
    fi
}

RECLAIM="$LOCK.reclaim"
# reclaim_stale_lock: clear $LOCK if, under the reclaim lock, it is still stale.
# Returns 0 when something was cleared, 1 otherwise (lost the reclaim race, the
# lock turned out live or gone, or the reclaim lock itself is stuck).
reclaim_stale_lock() {
    if ! mkdir "$RECLAIM" 2>/dev/null; then
        # Someone else is reclaiming. A reclaim lock older than two minutes is a
        # crash residue; it is not removed here (two contenders removing it would
        # race exactly like the primary lock) — fail closed and name it.
        if [ -n "$(find "$RECLAIM" -maxdepth 0 -mmin +2 2>/dev/null)" ]; then
            die "stale reclaim lock $RECLAIM is older than two minutes; remove it by hand after checking no worker-worktree run is alive"
        fi
        return 1
    fi
    cleared=1
    if lock_is_stale; then
        warn "clearing stale lock $LOCK (owner pid ${owner:-none})"
        rm -f "$LOCK/pid" 2>/dev/null || true
        rmdir "$LOCK" 2>/dev/null && cleared=0
    fi
    rmdir "$RECLAIM" 2>/dev/null || true
    return "$cleared"
}
trap 'release_lock' EXIT
trap 'release_lock; exit 129' HUP
trap 'release_lock; exit 130' INT
trap 'release_lock; exit 143' TERM

owner_is_zombie() {
    case "$(ps -o stat= -p "$1" 2>/dev/null | tr -d ' ')" in
        Z*) return 0 ;;
        *) return 1 ;;
    esac
}

waited=0
while :; do
    if mkdir "$LOCK" 2>/dev/null; then
        LOCKED=1
        printf '%s\n' "$$" > "$LOCK/pid"
        [ -z "${WORKER_WORKTREE_TEST_PAUSE_AFTER_ACQUIRE:-}" ] || sleep "$WORKER_WORKTREE_TEST_PAUSE_AFTER_ACQUIRE"
        break
    fi
    if lock_is_stale; then
        if [ -n "${WORKER_WORKTREE_TEST_PAUSE_BEFORE_RECLAIM:-}" ]; then
            log "TEST pausing before reclaim"
            sleep "$WORKER_WORKTREE_TEST_PAUSE_BEFORE_RECLAIM"
        fi
        if reclaim_stale_lock; then
            continue
        fi
    fi
    # Live lock, or a reclaim that did not clear anything: bounded wait.
    [ "$waited" -lt "$LOCK_WAIT" ] || die "another worker-worktree run (pid ${owner:-unknown}) has held $LOCK for ${LOCK_WAIT}s"
    [ "$waited" -gt 0 ] || log "waiting for another run on this repository (pid ${owner:-unknown})"
    sleep 1
    waited=$((waited + 1))
done

# --- refs -------------------------------------------------------------------------------

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
# Pin the base to a commit once, in the rig: a symbolic name such as HEAD would
# otherwise be re-read inside the lane, where it means something else.
BASE_SHA="$(git_rig rev-parse --verify --quiet "$BASE^{commit}")" || die "base ref does not resolve: $BASE"

# --- helpers ------------------------------------------------------------------------------

# bead_branches: every branch name (local, and remote with the remote prefix
# stripped) containing BEAD as a whole token, one per line, de-duplicated.
bead_branches() {
    # Escape the id for an ERE, then require a non-alphanumeric boundary (or
    # the string edge) on both sides so gp-abc1 never matches gp-abc10.
    id_re="$(printf '%s' "$BEAD" | sed 's/[][\\.^$*+?(){}|/]/\\&/g')"
    {
        git_rig for-each-ref --format='%(refname:short)' refs/heads
        git_rig for-each-ref --format='%(refname:short)' "refs/remotes/$REMOTE" \
            | while IFS= read -r ref; do
                [ "$ref" != "$REMOTE/HEAD" ] || continue
                printf '%s\n' "${ref#"$REMOTE/"}"
            done
    } | grep -E -- "(^|[^A-Za-z0-9])${id_re}([^A-Za-z0-9]|\$)" | sort -u || true
}

# checked_out_at BRANCH: the worktree path holding BRANCH, if any. Paths may
# contain spaces, so keep the whole line after the "worktree " key.
checked_out_at() {
    git_rig worktree list --porcelain | awk -v want="refs/heads/$1" '
        /^worktree / { wt = $0; sub(/^worktree /, "", wt) }
        $1 == "branch" && $2 == want { print wt }
    '
}

# is_our_worktree DIR: DIR is a git checkout whose common dir is this rig's.
is_our_worktree() {
    [ -e "$1/.git" ] || return 1
    c="$(git -C "$1" rev-parse --git-common-dir 2>/dev/null)" || return 1
    case "$c" in
        /*) ;;
        *) c="$1/$c" ;;
    esac
    c="$(abs "$c")" || return 1
    [ "$c" = "$RIG_COMMON" ]
}

# move_aside DIR REASON: relocate DIR out of the way without deleting anything.
# A worktree of this repository goes through `git worktree move` so its
# registration follows it; if git refuses (locked, or otherwise), fail closed.
# Anything else that is a git checkout belongs to another repository and is
# refused. Plain content is moved with mv.
move_aside() {
    stamp="$(date -u +%Y%m%dT%H%M%SZ)"
    aside="$1.aside-$stamp"
    n=0
    while [ -e "$aside" ]; do n=$((n + 1)); aside="$1.aside-$stamp-$n"; done
    if is_our_worktree "$1"; then
        if ! out="$(git_rig worktree move "$1" "$aside" 2>&1)"; then
            die "git refused to move the worktree $1 aside ($2): $out"
        fi
    elif [ -e "$1/.git" ]; then
        die "$1 is a git checkout of another repository; refusing to move or replace it ($2)"
    else
        mv "$1" "$aside"
    fi
    warn "moved aside $1 -> $aside ($2); nothing was deleted"
}

# --- classify the work dir ------------------------------------------------------------------
IS_WORKTREE=0
if [ -d "$WORKDIR" ]; then
    if is_our_worktree "$WORKDIR"; then
        IS_WORKTREE=1
        if [ -n "$(git -C "$WORKDIR" status --porcelain --untracked-files=no 2>/dev/null)" ]; then
            move_aside "$WORKDIR" "tracked modifications present"
            IS_WORKTREE=0
        fi
    elif [ -n "$(ls -A "$WORKDIR" 2>/dev/null)" ]; then
        move_aside "$WORKDIR" "not a worktree of $RIG_ROOT"
    fi
fi

# --- resolve what the work dir should have checked out --------------------------------------
# Runs AFTER classification so a worktree just moved aside (which still holds
# its branch) is seen as "checked out elsewhere" and the detached fallback
# applies instead of a failing worktree add.
# MODE: local (check out TARGET), remote (track TARGET from the remote),
# new (create TARGET from BASE_SHA), detached (detach at DETACH_AT, a commit).
MODE=""
TARGET=""
DETACH_AT=""
if [ -z "$BEAD" ]; then
    MODE="detached"
    DETACH_AT="$BASE_SHA"
else
    candidates="$(bead_branches)"
    count="$(printf '%s\n' "$candidates" | grep -c . || true)"
    if [ "$count" -gt 1 ]; then
        die "several branches name bead $BEAD; pass --bead with a unique id, or --base and no bead: $(printf '%s\n' "$candidates" | tr '\n' ' ')"
    elif [ "$count" -eq 0 ]; then
        MODE="new"
        TARGET="$BEAD"
    else
        TARGET="$candidates"
        if git_rig show-ref --verify --quiet "refs/heads/$TARGET"; then
            elsewhere="$(checked_out_at "$TARGET" | grep -v -x -F -- "$WORKDIR" || true)"
            if [ -n "$elsewhere" ]; then
                MODE="detached"
                DETACH_AT="$(git_rig rev-parse --verify "refs/heads/$TARGET^{commit}")"
                warn "branch $TARGET is checked out at $elsewhere; leaving $WORKDIR detached at its tip. Create your own branch in this work dir before committing."
            else
                MODE="local"
            fi
        else
            MODE="remote"
        fi
    fi
fi

# --- act --------------------------------------------------------------------------------------
add_worktree() {
    [ -d "$WORKDIR" ] || mkdir -p "$WORKDIR"
    case "$MODE" in
        local)    git_rig worktree add --quiet "$WORKDIR" "$TARGET" ;;
        remote)   git_rig worktree add --quiet --track -b "$TARGET" "$WORKDIR" "$REMOTE/$TARGET" ;;
        new)      git_rig worktree add --quiet -b "$TARGET" "$WORKDIR" "$BASE_SHA" ;;
        detached) git_rig worktree add --quiet --detach "$WORKDIR" "$DETACH_AT" ;;
    esac
}

# switch_worktree: change what a clean worktree of ours has checked out. Never
# overwrite an ignored file the target tracks; on any refusal the caller moves
# the worktree aside and adds a fresh one.
switch_worktree() {
    case "$MODE" in
        local)    git -C "$WORKDIR" switch --quiet --no-overwrite-ignore "$TARGET" ;;
        remote)   git -C "$WORKDIR" switch --quiet --no-overwrite-ignore -c "$TARGET" --track "$REMOTE/$TARGET" ;;
        new)      git -C "$WORKDIR" switch --quiet --no-overwrite-ignore -c "$TARGET" "$BASE_SHA" ;;
        detached) git -C "$WORKDIR" switch --quiet --no-overwrite-ignore --detach "$DETACH_AT" ;;
    esac
}

if [ "$IS_WORKTREE" -eq 1 ]; then
    current="$(git -C "$WORKDIR" rev-parse --abbrev-ref HEAD 2>/dev/null || echo HEAD)"
    need_switch=1
    case "$MODE" in
        local)    [ "$current" != "$TARGET" ] || need_switch=0 ;;
        detached) if [ "$current" = "HEAD" ] && [ "$(git -C "$WORKDIR" rev-parse HEAD)" = "$DETACH_AT" ]; then
                      need_switch=0
                  fi ;;
    esac
    if [ "$need_switch" -eq 1 ]; then
        if ! out="$(switch_worktree 2>&1)"; then
            printf '%s\n' "$out" >&2
            move_aside "$WORKDIR" "branch switch refused (ignored files in the way, or the git error above)"
            IS_WORKTREE=0
        fi
    fi
fi
if [ "$IS_WORKTREE" -eq 0 ]; then
    out="$(add_worktree 2>&1)" || die "could not create the worktree at $WORKDIR: $out"
fi

# --- verify before reporting ----------------------------------------------------------------
is_our_worktree "$WORKDIR" || die "$WORKDIR is not a worktree of $RIG_ROOT after preparation"
branch="$(git -C "$WORKDIR" rev-parse --abbrev-ref HEAD 2>/dev/null || echo HEAD)"
case "$MODE" in
    local|remote|new) [ "$branch" = "$TARGET" ] || die "$WORKDIR is on $branch, expected $TARGET" ;;
    detached)         [ "$branch" = "HEAD" ] || die "$WORKDIR is on $branch, expected a detached HEAD"
                      [ "$(git -C "$WORKDIR" rev-parse HEAD)" = "$DETACH_AT" ] || die "$WORKDIR is detached at $(git -C "$WORKDIR" rev-parse --short HEAD), expected $DETACH_AT" ;;
esac
[ "$branch" != "HEAD" ] || branch="detached"
sha="$(git -C "$WORKDIR" rev-parse --short HEAD 2>/dev/null || echo unknown)"
printf 'WORKTREE %s %s %s\n' "$WORKDIR" "$branch" "$sha"
