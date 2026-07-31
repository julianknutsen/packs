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
    # unique_by sorts by id, so the union comes back in bead-id order.
    #
    # `updated_at` is `omitempty` on the reader's bead struct -- absent only on
    # a bead never updated since it was created, which no verdict carrier can
    # be: it got its verdict key from `gc bd update`. So the re-sort this line
    # used to carry was live, and the `| last` it fed really did mean "most
    # recently updated".
    #
    # It is gone because the verdict selection below no longer decides by
    # position at all. It reads recency where recency is used -- narrowing to
    # the newest `updated_at` explicitly, by value -- instead of staging it in
    # the row order for a later `| last` to consume. That is what makes the
    # selection invariant under a permutation of bead ids. The lane-status
    # aggregation further down does still take the id-last value per key, and
    # dropping the re-sort changed what that means; see the note there.
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
# added here reaches every consumer at once -- in any case, because each
# consumer downcases the entry as well as the candidate. Matching only the
# candidate would have made a mixed-case entry silently unmatchable everywhere,
# which is the same class of defect as the split vocabulary this replaced.
APPROVAL_VERDICTS=(approve approved pass done)
APPROVAL_VERDICTS_JSON="$(printf '%s\n' "${APPROVAL_VERDICTS[@]}" \
  | jq -Rsc 'split("\n") | map(select(. != "")) | map(ascii_downcase)')"

is_approved() {
  local candidate known
  candidate="$(printf '%s' "${1-}" | tr '[:upper:]' '[:lower:]')"
  [ -n "$candidate" ] || return 1
  for known in "${APPROVAL_VERDICTS[@]}"; do
    if [ "$candidate" = "${known,,}" ]; then
      return 0
    fi
  done
  return 1
}

GC_ERR="$(mktemp)"
trap 'rm -f "$GC_ERR"' EXIT
if ! ROOT_JSON="$(gc bd show "$ROOT_ID" --json 2>"$GC_ERR")"; then
  echo "review check: note: gc bd show $ROOT_ID failed: $(tail -c 400 "$GC_ERR" | tr '\n' ' ')" >&2
fi
PARENT_ROOT="$(metadata_value "$ROOT_JSON" "gc.root_bead_id")"
if [ -z "$PARENT_ROOT" ]; then
  PARENT_ROOT="$ROOT_ID"
fi
PARENT_JSON="$ROOT_JSON"
if [ "$PARENT_ROOT" != "$ROOT_ID" ]; then
  if ! PARENT_JSON="$(gc bd show "$PARENT_ROOT" --json 2>"$GC_ERR")"; then
    echo "review check: note: gc bd show $PARENT_ROOT failed: $(tail -c 400 "$GC_ERR" | tr '\n' ' ')" >&2
  fi
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
# may never starve the gate. That fallback decides from lane beads alone, so it
# announces itself on stderr whenever it reduces more than one candidate.
#
# The survivors are then reduced to one value in two steps, neither of them
# positional. First recency: keep only the survivors carrying the newest
# `updated_at`, so a later pass overrules an earlier one exactly as it did
# before this file stopped sorting the union. Then, among values recency could
# not separate, prefer a NON-approving one. Resolving an unresolvable dispute
# toward "approve" ships unaddressed findings; resolving it toward "iterate"
# costs one more loop iteration in a state that should not occur anyway.
# Both steps are invariant under a permutation of bead ids -- the property the
# old `| last` could not hold.
#
# jq emits two lines: the selected verdict, then an optional ambiguity note.
VERDICT_SELECTION="$(printf '%s\n' "$MATCHES" | jq -r \
  --arg attempt "$ATTEMPT" \
  --argjson approvals "$APPROVAL_VERDICTS_JSON" '
  def is_approval($value):
    (($value // "") | ascii_downcase) as $v
    | any($approvals[]; . == $v);
  # Narrow to the newest rows -- but only when every row is dated. `updated_at`
  # is omitempty, and ranking a partially dated set would silently rank the
  # undated rows oldest, which is a guess, not a reading. Selecting by max
  # value rather than by position keeps ties whole for the reduction below.
  def newest($rows):
    ($rows | map(select((.updated_at // "") != "")) | length) as $dated
    | if $dated > 0 and $dated == ($rows | length)
      then ($rows | map(.updated_at) | max) as $max
        | ($rows | map(select(.updated_at == $max)))
      else $rows
      end;
  # Reduce to one value, fail-closed: the lexicographically smallest distinct
  # non-approving value if there is one, else the smallest approval.
  def decide($rows):
    ($rows | map(.value) | unique) as $vals
    | ($vals | map(select(is_approval(.) | not))) as $blocking
    | if ($blocking | length) > 0 then $blocking[0] else ($vals[0] // "") end;
  [
    .[]
    | select((.metadata["gc.attempt"] // "") == $attempt)
    | select((.metadata["code_review.verdict"] // "") != "")
    | {
        value: .metadata["code_review.verdict"],
        updated_at: (.updated_at // ""),
        lane: (
          [(.metadata // {}) | keys[] | select(test("^code_review\\..+_verdict$"))]
          | length > 0
        )
      }
  ] as $candidates
  | ($candidates | map(select(.lane | not))) as $owners
  | (if ($owners | length) > 0 then $owners else $candidates end) as $surviving
  | ($surviving | map(.value) | unique) as $values
  | newest($surviving) as $current
  | decide($current) as $verdict
  | (
      if (($current | map(.value) | unique | length) > 1) then
        "fail-closed among \($current | map(.value) | unique | length) values"
      elif (($current | length) < ($surviving | length)) then
        "newest updated_at"
      else
        "unanimous"
      end
    ) as $basis
  | (
      if ($owners | length) > 1 then
        "review check: \($owners | length) owner-shaped beads carry code_review.verdict at attempt \($attempt) (values: \($values | join(", "))); selected \"\($verdict)\" (\($basis))"
      elif ($owners | length) == 0 and ($surviving | length) > 1 then
        "review check: no owner-shaped bead at attempt \($attempt); reduced \($surviving | length) lane candidates (values: \($values | join(", "))); selected \"\($verdict)\" (\($basis))"
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
    # a loop decision. Dropping the re-sort in gmol did change it -- from
    # recency-last to id-last -- so this is a documented behavior change, not a
    # preservation. The two orders diverge only when one attempt carries more
    # than one row with a report path. Report mode is left unrepaired here
    # deliberately; the residual is recorded rather than claimed away.
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
  # union gmol returns. Dropping the re-sort in gmol changed that from
  # recency-last to id-last: a behavior change, not a preservation. The two
  # diverge only when one attempt carries the same lane key twice, in which
  # case this path can now report a stale value where it used to report the
  # fresh one. That is a real residual, recorded and deliberately left
  # unrepaired here rather than described as equivalent.
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
