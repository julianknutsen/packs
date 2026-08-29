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
    # unique_by sorts by id, so the union comes back in bead-id order -- and
    # that is the only order there is. `gc ready --json` emits no `updated_at`,
    # so the re-sort this line used to carry compared every row equal, left the
    # id order untouched, and still claimed to mean "most recently updated".
    #
    # The verdict selection below does not depend on this order: it partitions
    # the candidates by ownership and then reduces them to one value by value,
    # never by position. The lane-status aggregation further down does still
    # take the id-last value per key; see the note there.
    jq -s 'map(select(type=="array")) | add // [] | unique_by(.id)' "$tmp"/*.json || rc=1
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

# The one approval vocabulary. Both consumers read this single definition and
# both match case-insensitively: the jq lane-status helper, which receives it
# via --argjson, and the bash dispatch at the bottom of the file. A spelling
# added here reaches every consumer at once.
APPROVAL_VERDICTS=(approve approved pass done)
APPROVAL_VERDICTS_JSON="$(printf '%s\n' "${APPROVAL_VERDICTS[@]}" \
  | jq -Rsc 'split("\n") | map(select(. != ""))')"

is_approved() {
  local candidate known
  candidate="$(printf '%s' "${1-}" | tr '[:upper:]' '[:lower:]')"
  [ -n "$candidate" ] || return 1
  for known in "${APPROVAL_VERDICTS[@]}"; do
    if [ "$candidate" = "$known" ]; then
      return 0
    fi
  done
  return 1
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

# One bead owns the loop verdict: the apply step that closes out the review.
# Review lanes report their own `code_review.<lane>_verdict` and are contracted
# not to write the bare `code_review.verdict`, so a candidate carrying any
# `code_review.*_verdict` key is a lane and is dropped -- unless dropping the
# lanes would leave nothing, in which case every candidate is kept. Narrowing
# may never starve the gate.
#
# The survivors are then reduced to one value by value, never by position: an
# approval if any survivor carries one, otherwise the lexicographically
# smallest of the distinct non-approving values. Both are invariant under a
# permutation of bead ids -- the property the old `| last` could not hold.
#
# jq emits two lines: the selected verdict, then an optional ambiguity note.
VERDICT_SELECTION="$(printf '%s\n' "$MATCHES" | jq -r \
  --arg attempt "$ATTEMPT" \
  --argjson approvals "$APPROVAL_VERDICTS_JSON" '
  def is_approval($value):
    (($value // "") | ascii_downcase) as $v
    | any($approvals[]; . == $v);
  [
    .[]
    | select((.metadata["gc.attempt"] // "") == $attempt)
    | select((.metadata["code_review.verdict"] // "") != "")
    | {
        value: .metadata["code_review.verdict"],
        lane: (
          [(.metadata // {}) | keys[] | select(test("^code_review\\..+_verdict$"))]
          | length > 0
        )
      }
  ] as $candidates
  | ($candidates | map(select(.lane | not))) as $owners
  | (if ($owners | length) > 0 then $owners else $candidates end) as $surviving
  | ($surviving | map(.value) | unique) as $values
  | ($values | map(select(is_approval(.)))) as $approving
  | (
      if ($approving | length) > 0 then $approving[0] else ($values[0] // "") end
    ) as $verdict
  | (
      if ($owners | length) > 1 then
        "review check: \($owners | length) owner-shaped beads carry code_review.verdict at attempt \($attempt) (values: \($values | join(", "))); selected \"\($verdict)\""
      else
        ""
      end
    ) as $note
  | "\($verdict)\n\($note)"
' 2>/dev/null)"
VERDICT="$(printf '%s\n' "$VERDICT_SELECTION" | sed -n '1p')"
VERDICT_NOTE="$(printf '%s\n' "$VERDICT_SELECTION" | sed -n '2p')"
if [ -n "$VERDICT_NOTE" ]; then
  echo "$VERDICT_NOTE" >&2
fi

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
    # This `| last` picks a report *path* out of the same id-ordered union, not
    # a loop decision. Report mode is behavior-preserved here, so the id-order
    # dependency is documented rather than changed.
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
  --arg step "$STEP_ID" \
  --argjson approvals "$APPROVAL_VERDICTS_JSON" '
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
  # Same vocabulary the bash dispatch uses, handed in via --argjson approvals.
  def approved($value):
    (($value // "") | ascii_downcase) as $v
    | any($approvals[]; . == $v);
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
  # Each key still takes the id-last non-empty value out of the id-ordered
  # union gmol returns. That dependency is real; this change deliberately
  # leaves the lane-status path behavior-preserving and does not repair it.
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

if [ -z "$VERDICT" ]; then
  if [ "$LANE_STATUS" = "approved" ]; then
    echo "Implementation review approved from lane verdicts"
    exit 0
  fi
  echo "Implementation review needs another iteration: ${LANE_STATUS:-missing verdict}"
  exit 1
fi

if is_approved "$VERDICT"; then
  echo "Implementation review approved"
  exit 0
fi

echo "Implementation review needs another iteration: $VERDICT"
exit 1
