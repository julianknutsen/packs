#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
SCRIPT="$ROOT/gastown/assets/scripts/worktree-setup.sh"

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

git_c() {
    git -c user.email=a@a.com -c user.name=a "$@"
}

# new_upstream_and_rig — a bare "upstream" repo plus a rig clone of it, with
# one commit on main and origin/HEAD already configured (the common case:
# the rig was cloned from an origin that already existed).
new_upstream_and_rig() {
    local base="$1"
    git_c init -q --bare "$base/upstream.git"
    git_c init -q -b main "$base/seed"
    (cd "$base/seed" && git_c commit -q --allow-empty -m init \
        && git_c remote add origin "$base/upstream.git" \
        && git_c push -q origin main)
    git_c clone -q "$base/upstream.git" "$base/rig"
}

test_sync_pulls_when_branch_lacks_origin_tracking() {
    local base="$1"
    local rig="$base/rig" wt="$base/wt-notrack"

    # Reproduce #299's precondition directly: a worktree branch created by
    # the no-start-point fallback (the path taken when origin/HEAD isn't
    # configured at creation time -- e.g. a rig set up locally and given a
    # remote afterwards) has no branch.<name>.remote/.merge config at all.
    git_c -C "$rig" worktree add -q "$wt" -b notrack
    if git_c -C "$wt" rev-parse --abbrev-ref --symbolic-full-name '@{u}' >/dev/null 2>&1; then
        fail "test setup bug: notrack must have no upstream tracking configured"
    fi

    # The branch WAS pushed at some point (e.g. by a prior refinery run), and
    # origin has since moved ahead of it -- the exact "we hit this in
    # production" scenario from the issue.
    git_c -C "$wt" push -q origin notrack
    (cd "$base" && git_c clone -q "$base/upstream.git" advance \
        && cd advance && git_c checkout -q notrack \
        && git_c commit -q --allow-empty -m "advanced upstream" \
        && git_c push -q origin notrack)
    local advanced_sha
    advanced_sha=$(git_c -C "$base/advance" rev-parse notrack)

    sh "$SCRIPT" "$rig" "$wt" notrack --sync

    local wt_sha
    wt_sha=$(git_c -C "$wt" rev-parse HEAD)
    [ "$wt_sha" = "$advanced_sha" ] ||
        fail "sync should have pulled the advanced commit despite missing tracking config; want $advanced_sha got $wt_sha"
}

test_sync_is_noop_without_origin_counterpart() {
    local base="$1"
    local rig="$base/rig" wt="$base/wt-unpushed"

    git_c -C "$rig" worktree add -q "$wt" -b unpushed
    local before_sha
    before_sha=$(git_c -C "$wt" rev-parse HEAD)

    # Never pushed to origin -- nothing to sync. Must not error and must
    # leave the worktree exactly where it was.
    sh "$SCRIPT" "$rig" "$wt" unpushed --sync

    local after_sha
    after_sha=$(git_c -C "$wt" rev-parse HEAD)
    [ "$after_sha" = "$before_sha" ] ||
        fail "an unpushed branch with no origin counterpart must be left untouched, got $before_sha -> $after_sha"
}

test_sync_still_works_with_configured_tracking() {
    local base="$1"
    local rig="$base/rig" wt="$base/wt-tracked"

    # The already-working case: a branch created from an explicit
    # origin-tracking start point (the DEFAULT_REF path this script's own
    # creation logic normally takes) gets real tracking config for free.
    # The explicit fetch/pull-by-name form must keep working for it too.
    git_c -C "$rig" worktree add -q "$wt" -b tracked refs/remotes/origin/main
    if ! git_c -C "$wt" rev-parse --abbrev-ref --symbolic-full-name '@{u}' >/dev/null 2>&1; then
        fail "test setup bug: tracked must have upstream tracking configured"
    fi

    git_c -C "$wt" push -q origin tracked
    (cd "$base" && git_c clone -q "$base/upstream.git" advance2 \
        && cd advance2 && git_c checkout -q tracked \
        && git_c commit -q --allow-empty -m "advanced tracked" \
        && git_c push -q origin tracked)
    local advanced_sha
    advanced_sha=$(git_c -C "$base/advance2" rev-parse tracked)

    sh "$SCRIPT" "$rig" "$wt" tracked --sync

    local wt_sha
    wt_sha=$(git_c -C "$wt" rev-parse HEAD)
    [ "$wt_sha" = "$advanced_sha" ] ||
        fail "sync should still pull for a normally-tracked branch; want $advanced_sha got $wt_sha"
}

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
new_upstream_and_rig "$tmp"

test_sync_pulls_when_branch_lacks_origin_tracking "$tmp"
test_sync_is_noop_without_origin_counterpart "$tmp"
test_sync_still_works_with_configured_tracking "$tmp"

echo "worktree-setup sync tests passed"
