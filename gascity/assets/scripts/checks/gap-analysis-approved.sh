#!/usr/bin/env bash
set -euo pipefail

gmol() {   # root_id -> molecule-member JSON array
    # `gc bd list --metadata-field` is a collection query carrying no bead id,
    # so on a city that relocates the graph class bd has nothing to route on and
    # refuses the read -- and the `2>/dev/null` below turned that refusal into an
    # empty set, so this gate never saw gap_analysis.verdict and looped until Ralph
    # ran out of attempts.
    #
    # `gc ready` is the federating reader: city store, rig stores and the
    # relocated graph store, across both tiers. It takes exactly one --status
    # and has no --all, so the member set is the union of one leg per status.
    # The four legs are independent reads, so run them concurrently: a check
    # gate has a 10m budget and each `gc ready` costs ~17s on a loaded city.
    # A leg that fails is reported on stderr and fails the function rather than
    # contributing an empty set -- silent starvation is the bug being fixed.
    local root="$1" tmp st rc=0
    tmp="$(mktemp -d)" || return 1
    for st in open in_progress blocked closed; do
        { gc ready --metadata-field "gc.root_bead_id=$root" --status "$st" --limit 0 --json \
            >"$tmp/$st.json" || printf '%s\n' "$st" >>"$tmp/failed"; } &
    done
    wait
    if [ -s "$tmp/failed" ]; then
        echo "gmol: gc ready failed for status: $(tr '\n' ' ' <"$tmp/failed")" >&2
        rc=1
    fi
    # unique_by sorts by id, so the union comes back in bead-id order. The
    # verdict extractors below take `| last`, which must mean "most recently
    # updated" -- without this re-sort the gate picks a verdict by id hash and
    # can sit on a stale `iterate` forever while a newer `done` is ignored.
    jq -s 'map(select(type=="array")) | add // [] | unique_by(.id) | sort_by(.updated_at // "")' "$tmp"/*.json || rc=1
    rm -rf "$tmp"
    return "$rc"
}

ROOT_ID="${GC_BEAD_ID:-}"
ATTEMPT="${GC_ITERATION:-}"

if [ -z "$ROOT_ID" ]; then
  echo "gap check: GC_BEAD_ID is required" >&2
  exit 1
fi

if [ -z "$ATTEMPT" ]; then
  ATTEMPT="0"
fi

metadata_value() {
  local json="$1"
  local key="$2"
  printf '%s\n' "$json" | jq -r --arg key "$key" '
    (if type == "array" then (.[0] // {}) else . end)
    | .metadata[$key] // empty
  ' 2>/dev/null
}

GC_ERR="$(mktemp)"
ROOT_JSON="$(gc bd show "$ROOT_ID" --json 2>"$GC_ERR" || true)"
[ -n "$ROOT_JSON" ] || echo "gap check: note: gc bd show $ROOT_ID failed: $(head -c 400 "$GC_ERR" | tr '\n' ' ')" >&2
PARENT_ROOT="$(metadata_value "$ROOT_JSON" "gc.root_bead_id")"
if [ -z "$PARENT_ROOT" ]; then
  PARENT_ROOT="$ROOT_ID"
fi

MATCHES="$(gmol "$PARENT_ROOT")"

VERDICT="$(printf '%s\n' "$MATCHES" | jq -r --arg attempt "$ATTEMPT" '
  [
    .[]
    | select((.metadata["gc.attempt"] // "") == $attempt)
    | select((.metadata["gap_analysis.verdict"] // "") != "")
    | .metadata["gap_analysis.verdict"]
  ] | last // ""
' 2>/dev/null)"

REPORT="$(printf '%s\n' "$MATCHES" | jq -r --arg attempt "$ATTEMPT" '
  [
    .[]
    | select((.metadata["gc.attempt"] // "") == $attempt)
    | select((.metadata["gap_analysis.report_path"] // "") != "")
    | .metadata["gap_analysis.report_path"]
  ] | last // ""
' 2>/dev/null)"

if [ "$VERDICT" != "done" ]; then
  echo "Gap analysis needs another iteration: ${VERDICT:-missing verdict}"
  exit 1
fi

if [ -n "$REPORT" ]; then
  if [ ! -f "$REPORT" ] && [ -n "${GC_WORK_DIR:-}" ] && [ -f "$GC_WORK_DIR/$REPORT" ]; then
    REPORT="$GC_WORK_DIR/$REPORT"
  fi
  if [ -f "$REPORT" ] && grep -Eiq '(^|[^[:alpha:]])severity[^[:alpha:]]*(critical|blocker|major)([^[:alpha:]]|$)' "$REPORT"; then
    echo "Gap analysis report still contains critical/blocker/major findings: $REPORT"
    exit 1
  fi
fi

echo "Gap analysis approved"
exit 0
