#!/bin/sh
# gc <binding> pr review — sling a coding agent the mol-pr-review formula
# to self-review an outgoing PR against an 11-category scorecard.
#
# Usage:
#   gc <binding> pr review <pr-number-or-url> [--rig <name>] [--agent <name>]
#
# Environment (set by gc):
#   GC_CITY_PATH   absolute city root
#   GC_PACK_DIR    absolute pack directory
#   GC_PACK_NAME   pack name ("pr-pipeline")
#   GC_CITY_NAME   city workspace name
#   GC_RIG         current rig (when running inside a rig session)

set -eu

if [ -z "${GC_PACK_DIR:-}" ]; then
    echo "gc pr-pipeline pr review: missing Gas City pack context" >&2
    exit 1
fi

if [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ] || [ -z "${1:-}" ]; then
    cat "$GC_PACK_DIR/commands/pr/review/help.md"
    [ -z "${1:-}" ] && exit 2 || exit 0
fi

PR="$1"
shift

# Accept bare integer or full URL of the form https://github.com/<owner>/<repo>/pull/<integer>.
case "$PR" in
    https://github.com/*/pull/*)
        # Verify the segment after /pull/ is a positive integer (strip any
        # trailing /files, ?query, or #fragment).
        PR_NUM="${PR##*/pull/}"
        PR_NUM="${PR_NUM%%/*}"
        PR_NUM="${PR_NUM%%\?*}"
        PR_NUM="${PR_NUM%%#*}"
        case "$PR_NUM" in
            ''|*[!0-9]*)
                echo "gc pr-pipeline pr review: PR URL must end in /pull/<integer> (got: $PR)" >&2
                exit 2
                ;;
        esac
        ;;
    *[!0-9]*)
        echo "gc pr-pipeline pr review: <pr> must be a positive integer or a GitHub PR URL (got: $PR)" >&2
        exit 2
        ;;
esac

RIG=""
AGENT="polecat"
# Keep in step with [vars.max_diff_lines] in formulas/mol-pr-review.formula.toml.
MAX_DIFF_LINES="${GC_PR_MAX_DIFF_LINES:-5000}"

while [ $# -gt 0 ]; do
    case "$1" in
        --rig)        RIG="$2"; shift 2 ;;
        --rig=*)      RIG="${1#--rig=}"; shift ;;
        --agent)      AGENT="$2"; shift 2 ;;
        --agent=*)    AGENT="${1#--agent=}"; shift ;;
        --max-diff-lines)   MAX_DIFF_LINES="$2"; shift 2 ;;
        --max-diff-lines=*) MAX_DIFF_LINES="${1#--max-diff-lines=}"; shift ;;
        *)
            echo "gc pr-pipeline pr review: unknown argument: $1" >&2
            exit 2
            ;;
    esac
done

if [ -z "$RIG" ]; then
    RIG="${GC_RIG:-}"
fi

if [ -z "$RIG" ]; then
    echo "gc pr-pipeline pr review: --rig <name> required (or set GC_RIG)" >&2
    exit 2
fi

if ! command -v gc >/dev/null 2>&1; then
    echo "gc pr-pipeline pr review: gc binary not in PATH" >&2
    exit 1
fi

# --- Budget preflight -------------------------------------------------------
# Refuse an oversized PR HERE, before `gc sling` starts an agent. This is the
# only point in the pipeline that is genuinely pre-spend: once the sling runs,
# a model session exists and step 1 has already read the PR metadata.
#
# The formula carries the same check as defence in depth for direct slings that
# bypass this command, but the formula's copy cannot be pre-spend by
# construction -- it executes inside the very session whose cost it is trying
# to avoid.
#
# A malformed limit REFUSES rather than falling through. The earlier draft
# interpolated this value straight into a `[` test, where a non-numeric value
# made the test error and the oversized PR proceed -- a guard that fails open
# on bad configuration is worse than no guard, because it reads as protection.
case "$MAX_DIFF_LINES" in
    ''|*[!0-9]*)
        echo "gc pr-pipeline pr review: --max-diff-lines must be a non-negative integer (got: $MAX_DIFF_LINES)" >&2
        exit 2
        ;;
esac

if [ "$MAX_DIFF_LINES" -ne 0 ]; then
    if ! PR_SIZE=$(gh pr view "$PR" --json additions,deletions,changedFiles \
                     --jq '"\(.additions + .deletions) \(.changedFiles)"' 2>/dev/null); then
        echo "gc pr-pipeline pr review: could not read PR size for #$PR, refusing" >&2
        echo "  rather than reviewing on an unknown budget. An unreadable size is not zero." >&2
        exit 1
    fi
    PR_LINES="${PR_SIZE%% *}"
    PR_FILES="${PR_SIZE##* }"
    case "$PR_LINES" in
        ''|*[!0-9]*)
            echo "gc pr-pipeline pr review: PR size did not parse as a number ('$PR_LINES'), refusing" >&2
            exit 1
            ;;
    esac
    if [ "$PR_LINES" -gt "$MAX_DIFF_LINES" ]; then
        echo "gc pr-pipeline pr review: REFUSED, PR #$PR changes $PR_LINES lines across $PR_FILES files," >&2
        echo "  over the max-diff-lines budget of $MAX_DIFF_LINES. No agent was started and no model" >&2
        echo "  budget was spent." >&2
        echo "" >&2
        echo "  This describes the REQUEST, not a failed run: retrying it unchanged cannot succeed" >&2
        echo "  and costs the same budget again. Split the PR, review a subset of paths, or raise" >&2
        echo "  the budget deliberately with --max-diff-lines <new-limit>." >&2
        exit 3
    fi
fi

exec gc sling "$RIG/$AGENT" mol-pr-review --formula \
    --var "pr=$PR" --var "max_diff_lines=$MAX_DIFF_LINES"
