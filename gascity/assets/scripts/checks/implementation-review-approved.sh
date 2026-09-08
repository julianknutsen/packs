#!/usr/bin/env bash
set -euo pipefail

gmol() {   # root_id -> molecule-member JSON array
    # `gc bd list --metadata-field` is a collection query carrying no bead id,
    # so on a city that relocates the graph class bd has nothing to route on and
    # refuses the read -- and the `2>/dev/null` below turned that refusal into an
    # empty set, so this gate never saw code_review.verdict and looped until Ralph
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
  echo "review check: GC_BEAD_ID is required" >&2
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

ROOT_JSON="$(gc bd show "$ROOT_ID" --json 2>/dev/null || true)"
PARENT_ROOT="$(metadata_value "$ROOT_JSON" "gc.root_bead_id")"
if [ -z "$PARENT_ROOT" ]; then
  PARENT_ROOT="$ROOT_ID"
fi
PARENT_JSON="$ROOT_JSON"
if [ "$PARENT_ROOT" != "$ROOT_ID" ]; then
  PARENT_JSON="$(gc bd show "$PARENT_ROOT" --json 2>/dev/null || true)"
fi
STEP_ID="$(metadata_value "$ROOT_JSON" "gc.step_id")"
SCOPE_REF="$(metadata_value "$ROOT_JSON" "gc.scope_ref")"
if [ -z "$SCOPE_REF" ]; then
  SCOPE_REF="$(metadata_value "$ROOT_JSON" "gc.step_ref")"
fi

MATCHES="$(gmol "$PARENT_ROOT")"

VERDICT="$(printf '%s\n' "$MATCHES" | jq -r --arg attempt "$ATTEMPT" '
  [
    .[]
    | select((.metadata["gc.attempt"] // "") == $attempt)
    | select((.metadata["code_review.verdict"] // "") != "")
    | .metadata["code_review.verdict"]
  ] | last // ""
' 2>/dev/null)"

REPORT="$(printf '%s\n' "$MATCHES" | jq -r --arg attempt "$ATTEMPT" '
  [
    .[]
    | select((.metadata["gc.attempt"] // "") == $attempt)
    | select((.metadata["code_review.report_path"] // "") != "")
    | .metadata["code_review.report_path"]
  ] | last // ""
' 2>/dev/null)"

# Convergence guard (opt-in): the apply lane may record how many fixes it
# applied this iteration and how many remaining findings this loop can still
# act on. An `iterate` verdict with zero applied fixes and zero actionable
# remaining findings is a livelock (nothing can change on the next identical
# iteration), so it is treated as approve-with-notes. Both keys must be
# present and exactly "0"; when the apply lane does not write them, behavior
# is unchanged.
APPLIED_COUNT="$(printf '%s\n' "$MATCHES" | jq -r --arg attempt "$ATTEMPT" '
  [
    .[]
    | select((.metadata["gc.attempt"] // "") == $attempt)
    | select((.metadata["code_review.applied_count"] // "") != "")
    | .metadata["code_review.applied_count"]
  ] | last // ""
' 2>/dev/null)"

ACTIONABLE_REMAINING="$(printf '%s\n' "$MATCHES" | jq -r --arg attempt "$ATTEMPT" '
  [
    .[]
    | select((.metadata["gc.attempt"] // "") == $attempt)
    | select((.metadata["code_review.actionable_remaining"] // "") != "")
    | .metadata["code_review.actionable_remaining"]
  ] | last // ""
' 2>/dev/null)"

REVIEW_MODE="$(metadata_value "$ROOT_JSON" "gc.var.review_mode")"
if [ -z "$REVIEW_MODE" ]; then
  REVIEW_MODE="$(metadata_value "$PARENT_JSON" "gc.var.review_mode")"
fi
if [ "$REVIEW_MODE" = "report" ]; then
  REPORT_MODE_PATH="$(metadata_value "$PARENT_JSON" "gc.build.code_review_report_path")"
  if [ -z "$REPORT_MODE_PATH" ]; then
    REPORT_MODE_PATH="$(metadata_value "$PARENT_JSON" "gc.build.review_report_path")"
  fi
  if [ -z "$REPORT_MODE_PATH" ]; then
    REPORT_MODE_PATH="$(metadata_value "$PARENT_JSON" "gc.var.report_path")"
  fi
  if [ -z "$REPORT_MODE_PATH" ]; then
    REPORT_MODE_PATH="$(printf '%s\n' "$MATCHES" | jq -r --arg attempt "$ATTEMPT" '
      [
        .[]
        | select((.metadata["gc.attempt"] // "") == $attempt)
        | (
            .metadata["code_review.review_report_path"] //
            .metadata["code_review.report_path"] //
            .metadata["code_review.output_path"] //
            ""
          )
        | select(. != "")
      ] | last // ""
    ' 2>/dev/null)"
  fi
  if [ -n "$REPORT_MODE_PATH" ]; then
    echo "Implementation review report mode satisfied: $REPORT_MODE_PATH"
    exit 0
  fi
  echo "Implementation review report mode needs a review report path"
  exit 1
fi

LANE_STATUS="$(printf '%s\n' "$MATCHES" | jq -r \
  --arg root "$PARENT_ROOT" \
  --arg attempt "$ATTEMPT" \
  --arg scope "$SCOPE_REF" \
  --arg step "$STEP_ID" '
  def current_loop:
    select(.metadata["gc.root_bead_id"] == $root)
    | select(($attempt == "") or ((.metadata["gc.attempt"] // "") == $attempt))
    | select(
        if $attempt != "" and $step != "" then
          ((.metadata["gc.ralph_step_id"] // "") == $step) or
          ((.metadata["gc.step_id"] // "") == $step) or
          (((.metadata["gc.scope_ref"] // "") | startswith($step + ".iteration.")))
        elif $attempt != "" and $scope != "" then
          ((.metadata["gc.scope_ref"] // "") == $scope) or
          ((.metadata["gc.step_ref"] // "") == $scope)
        elif $step != "" then
          ((.metadata["gc.ralph_step_id"] // "") == $step) or
          (((.metadata["gc.scope_ref"] // "") | startswith($step + ".iteration.")))
        elif $scope != "" then
          ((.metadata["gc.scope_ref"] // "") == $scope)
        else
          true
        end
      );
  def approved($value):
    (($value // "") | ascii_downcase) as $v
    | ($v == "approve" or $v == "approved" or $v == "pass" or $v == "done");
  [
    .[]
    | current_loop
    | .metadata
    | {
        acceptance: (."code_review.acceptance_verdict" // ""),
        test_evidence: (."code_review.test_evidence_verdict" // ""),
        simplicity: (."code_review.simplicity_verdict" // "")
      }
  ] as $rows
  | {
      acceptance: ([$rows[].acceptance | select(. != "")] | last // ""),
      test_evidence: ([$rows[].test_evidence | select(. != "")] | last // ""),
      simplicity: ([$rows[].simplicity | select(. != "")] | last // "")
    } as $latest
  | if ($latest.acceptance != "" or $latest.test_evidence != "" or $latest.simplicity != "") then
      if (approved($latest.acceptance) and approved($latest.test_evidence) and approved($latest.simplicity)) then
        "approved"
      else
        "iterate: acceptance=\($latest.acceptance // "<missing>") test_evidence=\($latest.test_evidence // "<missing>") simplicity=\($latest.simplicity // "<missing>")"
      end
    else
      ""
    end
' 2>/dev/null)"

if [ "$VERDICT" != "done" ]; then
  case "$VERDICT" in
    approved|pass)
      ;;
    "")
      if [ "$LANE_STATUS" = "approved" ]; then
        echo "Implementation review approved from lane verdicts"
        exit 0
      fi
      echo "Implementation review needs another iteration: ${LANE_STATUS:-missing verdict}"
      exit 1
      ;;
    *)
      if [ "$APPLIED_COUNT" = "0" ] && [ "$ACTIONABLE_REMAINING" = "0" ]; then
        echo "Implementation review converged: verdict=$VERDICT with applied_count=0 and actionable_remaining=0 (no actionable delta; approving with notes)"
        exit 0
      fi
      echo "Implementation review needs another iteration: $VERDICT"
      exit 1
      ;;
  esac
fi

echo "Implementation review approved"
exit 0
