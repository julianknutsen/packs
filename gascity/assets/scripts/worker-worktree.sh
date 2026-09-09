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
#     refs, fetches, and registers worktrees from it. A work dir that is the
#     rig root, inside it, or an ancestor of it is refused before any change.
#   * Nothing is deleted. A work dir that is not a worktree of this repository
#     and is not empty is moved to <workdir>.aside-<utc stamp>; a worktree with
#     tracked modifications is moved aside the same way before a fresh one
#     replaces it; a branch switch that would overwrite an ignored file
#     (`git switch --no-overwrite-ignore`) moves the worktree aside instead.
#     Untracked files (staged skills, hooks, node_modules) do not count as
#     modifications and ride along on a branch switch. A worktree git refuses
#     to move (locked) is left in place and the script fails.
#   * Bead branch: exactly one branch (local or on the remote) whose name
#     contains the bead id as a whole token — `gp-abc1`, `fix/gp-abc1-x`, never
#     `gp-abc10` — is reused. None: a new branch named <bead id> is created
#     from --base. Several: fail closed and list them. If another session
#     creates the branch first, this one falls back the same way it would have
#     had the branch existed at the start (checkout, or detached when it is
#     checked out elsewhere).
#   * A bead branch already checked out in another worktree is not stolen: the
#     work dir is left detached at that branch's tip and a WARN line says so.
#     Create your own branch in this work dir before committing.
#   * No bead: the work dir is detached at --base, whatever branch it was on
#     (the branch itself is kept; nothing is deleted).
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
        -h|--help) sed -n '2,50p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) die "unknown argument: $1" ;;
    esac
done

[ -n "$WORKDIR" ] || WORKDIR="$PWD"
[ -n "$RIG_ROOT" ] || die "no rig root: pass --rig-root or set GC_RIG_ROOT"
[ -d "$RIG_ROOT" ] || die "rig root is not a directory: $RIG_ROOT"
command -v git >/dev/null 2>&1 || die "git is required on PATH"

# abs DIR: absolute, symlink-resolved path of an existing directory.
abs() { (cd "$1" 2>/dev/null && pwd -P); }

# canon PATH: the same for a path that need not exist yet — resolve the deepest
# existing ancestor and re-append the rest, so a not-yet-created work dir is
# compared on the real filesystem location it will occupy.
canon() {
    p="$1"
    case "$p" in
        /*) ;;
        *) p="$PWD/$p" ;;
    esac
    rest=""
    while [ ! -d "$p" ]; do
        base="${p##*/}"
        parent="${p%/*}"
        [ -n "$parent" ] || parent="/"
        [ "$parent" != "$p" ] || die "cannot resolve path: $1"
        rest="/$base$rest"
        p="$parent"
    done
    printf '%s%s\n' "$(abs "$p")" "$rest"
}

RIG_ROOT="$(abs "$RIG_ROOT")" || die "cannot enter rig root"
WORKDIR="$(canon "$WORKDIR")"

# Refuse before anything else: the rig root itself, anything inside it, and any
# ancestor of it (moving an ancestor aside would move the rig).
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

# --- helpers -------------------------------------------------------------------

# bead_branches: every branch name (local, and remote with the remote prefix
# stripped) containing BEAD as a whole token, one per line, de-duplicated.
bead_branches() {
    # Escape the id for an ERE, then require a non-alphanumeric boundary (or
    # the string edge) on both sides so gp-abc1 never matches gp-abc10.
    id_re="$(printf '%s' "$BEAD" | sed 's/[][\\.^$*+?(){}|/]/\\&/g')"
    {
        git_rig for-each-ref --format='%(refname:short)' refs/heads
        git_rig for-each-ref --format='%(refname:short)' "refs/remotes/$REMOTE" \
            | grep -v "^$REMOTE/HEAD\$" | sed "s|^$REMOTE/||"
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

is_our_worktree() {
    [ -e "$1/.git" ] || return 1
    c="$(git -C "$1" rev-parse --git-common-dir 2>/dev/null || true)"
    [ -n "$c" ] || return 1
    case "$c" in
        /*) ;;
        *) c="$1/$c" ;;
    esac
    [ -d "$c" ] && [ "$(abs "$c")" = "$RIG_COMMON" ]
}

# move_aside DIR REASON: relocate DIR out of the way without deleting anything.
# A registered worktree goes through `git worktree move` so its registration
# follows it; if git refuses (locked, or otherwise), fail closed rather than
# mv a registered worktree out from under git.
move_aside() {
    stamp="$(date -u +%Y%m%dT%H%M%SZ)"
    aside="$1.aside-$stamp"
    n=0
    while [ -e "$aside" ]; do n=$((n + 1)); aside="$1.aside-$stamp-$n"; done
    if is_our_worktree "$1"; then
        if ! out="$(git_rig worktree move "$1" "$aside" 2>&1)"; then
            die "git refused to move the worktree $1 aside ($2): $out"
        fi
    else
        mv "$1" "$aside"
    fi
    warn "moved aside $1 -> $aside ($2); nothing was deleted"
}

# --- bead branch resolution -----------------------------------------------------
# MODE: local (check out TARGET), remote (track TARGET from the remote),
# new (create TARGET from BASE), detached (detach at DETACH_AT).
resolve_mode() {
    MODE=""
    TARGET=""
    DETACH_AT=""
    if [ -z "$BEAD" ]; then
        MODE="detached"
        DETACH_AT="$BASE"
        return
    fi
    candidates="$(bead_branches)"
    count="$(printf '%s\n' "$candidates" | grep -c . || true)"
    if [ "$count" -gt 1 ]; then
        die "several branches name bead $BEAD; pass --bead with a unique id, or --base and no bead: $(printf '%s\n' "$candidates" | tr '\n' ' ')"
    fi
    if [ "$count" -eq 0 ]; then
        MODE="new"
        TARGET="$BEAD"
        return
    fi
    TARGET="$candidates"
    if git_rig show-ref --verify --quiet "refs/heads/$TARGET"; then
        elsewhere="$(checked_out_at "$TARGET" | grep -v -x -F -- "$WORKDIR" || true)"
        if [ -n "$elsewhere" ]; then
            MODE="detached"
            DETACH_AT="$TARGET"
            warn "branch $TARGET is checked out at $elsewhere; leaving $WORKDIR detached at its tip. Create your own branch in this work dir before committing."
        else
            MODE="local"
        fi
    else
        MODE="remote"
    fi
}

# --- classify the work dir --------------------------------------------------------
IS_WORKTREE=0
if [ -d "$WORKDIR" ] && is_our_worktree "$WORKDIR"; then
    IS_WORKTREE=1
fi

if [ "$IS_WORKTREE" -eq 0 ] && [ -d "$WORKDIR" ] && [ -n "$(ls -A "$WORKDIR" 2>/dev/null)" ]; then
    move_aside "$WORKDIR" "not a worktree of $RIG_ROOT"
fi

if [ "$IS_WORKTREE" -eq 1 ] && [ -n "$(git -C "$WORKDIR" status --porcelain --untracked-files=no 2>/dev/null)" ]; then
    move_aside "$WORKDIR" "tracked modifications present"
    IS_WORKTREE=0
fi

# --- act ----------------------------------------------------------------------------
add_worktree() {
    [ -d "$WORKDIR" ] || mkdir -p "$WORKDIR"
    case "$MODE" in
        local)    git_rig worktree add --quiet "$WORKDIR" "$TARGET" ;;
        remote)   git_rig worktree add --quiet --track -b "$TARGET" "$WORKDIR" "$REMOTE/$TARGET" ;;
        new)      git_rig worktree add --quiet -b "$TARGET" "$WORKDIR" "$BASE" ;;
        detached) git_rig worktree add --quiet --detach "$WORKDIR" "$DETACH_AT" ;;
    esac
}

# switch_worktree: change what a clean worktree has checked out. Never
# overwrite an ignored file the target tracks; on any refusal the caller moves
# the worktree aside and adds a fresh one.
switch_worktree() {
    case "$MODE" in
        local)    git -C "$WORKDIR" switch --quiet --no-overwrite-ignore "$TARGET" ;;
        remote)   git -C "$WORKDIR" switch --quiet --no-overwrite-ignore -c "$TARGET" --track "$REMOTE/$TARGET" ;;
        new)      git -C "$WORKDIR" switch --quiet --no-overwrite-ignore -c "$TARGET" "$BASE" ;;
        detached) git -C "$WORKDIR" switch --quiet --no-overwrite-ignore --detach "$DETACH_AT" ;;
    esac
}

resolve_mode

# A branch this run meant to create may have been created by a concurrent run
# between resolution and the git call; re-resolving turns that into the
# checkout-or-detached path the contract promises instead of a failure.
attempt() {
    # $1 = add|switch
    tries=0
    while :; do
        tries=$((tries + 1))
        if [ "$1" = add ]; then
            if out="$(add_worktree 2>&1)"; then return 0; fi
        else
            if out="$(switch_worktree 2>&1)"; then return 0; fi
        fi
        if [ "$MODE" = "new" ] && [ "$tries" -lt 3 ] && git_rig show-ref --verify --quiet "refs/heads/$TARGET"; then
            log "branch $TARGET appeared while preparing $WORKDIR; re-resolving"
            resolve_mode
            continue
        fi
        printf '%s\n' "$out" >&2
        return 1
    done
}

if [ "$IS_WORKTREE" -eq 1 ]; then
    current="$(git -C "$WORKDIR" rev-parse --abbrev-ref HEAD 2>/dev/null || echo HEAD)"
    need_switch=1
    case "$MODE" in
        local)    [ "$current" != "$TARGET" ] || need_switch=0 ;;
        detached) if [ "$current" = "HEAD" ] \
                     && [ "$(git -C "$WORKDIR" rev-parse HEAD)" = "$(git_rig rev-parse "$DETACH_AT^{commit}")" ]; then
                      need_switch=0
                  fi ;;
    esac
    if [ "$need_switch" -eq 1 ] && ! attempt switch; then
        move_aside "$WORKDIR" "branch switch refused (ignored files in the way, or git error above)"
        attempt add || die "could not prepare $WORKDIR after moving the previous worktree aside"
    fi
else
    attempt add || die "could not create the worktree at $WORKDIR"
fi

WORKDIR="$(abs "$WORKDIR")"
branch="$(git -C "$WORKDIR" rev-parse --abbrev-ref HEAD 2>/dev/null || echo HEAD)"
[ "$branch" != "HEAD" ] || branch="detached"
sha="$(git -C "$WORKDIR" rev-parse --short HEAD 2>/dev/null || echo unknown)"
printf 'WORKTREE %s %s %s\n' "$WORKDIR" "$branch" "$sha"
