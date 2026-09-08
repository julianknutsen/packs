{{ define "patrol-wisp-ledger" }}
## Patrol Wisp Ledger Contract

Patrol roots are ephemeral molecule rows. `gc bd list` reads the durable tier
and can return `[]` even while an assigned patrol root exists. Always discover
patrol roots with:

```bash
gc bd ${GC_RIG:+--rig="$GC_RIG"} query --json \
  'ephemeral=true AND (status=open OR status=in_progress)' --limit=0
```

Then filter the JSON to the exact patrol formula title and either the canonical
`$GC_AGENT` assignee or an unassigned `open` root. The latter is the recoverable
crash window after `mol wisp` commits but before the following assignment.
Prefer `$GC_BEAD_ID`, then an assigned `in_progress` root, then an assigned
`open` root, then the oldest unassigned `open` root (with ID as the final
tie-breaker). Reuse that root instead of pouring a duplicate. Before doing
patrol work, revalidate the survivor and promote it to `in_progress` with the
canonical assignee. At handoff, promote the successor after burning the prior
current root. These durable markers are required because persistent providers
can re-read the formula with an empty `$GC_BEAD_ID`.

Cleanup is deliberately conservative. Before burning a surplus root, re-read
it and verify all of these are still true: it is `open`, ephemeral, a molecule,
has the exact patrol title, is assigned to `$GC_AGENT` or still unassigned, and
both dependency counts are zero. Never burn a surplus `in_progress` or materialized root.
Preserve it and report the conflict instead.
{{ end }}

{{ define "patrol-wisp-startup" }}
# Reconcile open/in-progress roots from the ephemeral tier.
if [ -z "${GC_AGENT:-}" ]; then
  echo "GC_AGENT is empty; refusing patrol-root reconciliation."
  exit 1
fi
if ! PATROL_QUERY=$(gc bd ${GC_RIG:+--rig="$GC_RIG"} query --json \
  'ephemeral=true AND (status=open OR status=in_progress)' --limit=0); then
  echo "Could not query patrol roots; refusing to pour a duplicate."
  exit 1
fi
if ! PATROL_ROOTS=$(printf '%s\n' "$PATROL_QUERY" | jq -c \
  --arg agent "$GC_AGENT" --arg formula "$PATROL_FORMULA" --arg current "${GC_BEAD_ID:-}" '
  [.[] | select(.ephemeral == true)
    | select((.issue_type // .type // "") == "molecule")
    | select((.title // "") == $formula)
    | select((.assignee // "") == $agent
        or ((.assignee // "") == "" and .status == "open"))
    | . + {_patrol_rank: [
        (if $current != "" and .id == $current then 0 else 1 end),
        (if .status == "in_progress" then 0 else 1 end),
        (if (.assignee // "") == $agent then 0 else 1 end),
        (.created_at // ""), (.id // "")
      ]}]
  | sort_by(._patrol_rank)'); then
  echo "Could not parse patrol roots; refusing to pour a duplicate."
  exit 1
fi
WISP=$(printf '%s\n' "$PATROL_ROOTS" | jq -r '.[0].id // empty')
for extra in $(printf '%s\n' "$PATROL_ROOTS" | jq -r --arg keep "$WISP" \
  '.[] | select(.id != $keep) | .id'); do
  EXTRA_STATE=$(gc bd ${GC_RIG:+--rig="$GC_RIG"} show "$extra" --json 2>/dev/null || printf '[]')
  if printf '%s\n' "$EXTRA_STATE" | jq -e --arg id "$extra" --arg agent "$GC_AGENT" \
    --arg formula "$PATROL_FORMULA" '
      (.[0] // {}) as $w
      | $w.id == $id and $w.ephemeral == true
        and ($w.issue_type // $w.type // "") == "molecule"
        and ($w.title // "") == $formula
        and (($w.assignee // "") == $agent or ($w.assignee // "") == "")
        and $w.status == "open"
        and ($w.dependency_count // -1) == 0
        and ($w.dependent_count // -1) == 0' >/dev/null; then
    gc bd ${GC_RIG:+--rig="$GC_RIG"} mol burn "$extra" --force ||
      { echo "Could not burn empty surplus patrol root $extra."; exit 1; }
  else
    echo "Preserving non-empty or active surplus patrol root $extra for inspection."
  fi
done

# Reuse and durably mark the survivor. Only a truly empty result may pour.
if [ -n "$WISP" ]; then
  WISP_STATE=$(gc bd ${GC_RIG:+--rig="$GC_RIG"} show "$WISP" --json 2>/dev/null || printf '[]')
  if ! printf '%s\n' "$WISP_STATE" | jq -e --arg id "$WISP" --arg agent "$GC_AGENT" \
    --arg formula "$PATROL_FORMULA" '
      (.[0] // {}) as $w
      | $w.id == $id and $w.ephemeral == true
        and ($w.issue_type // $w.type // "") == "molecule"
        and ($w.title // "") == $formula
        and (($w.assignee // "") == $agent
          or (($w.assignee // "") == "" and $w.status == "open"))
        and ($w.status == "open" or $w.status == "in_progress")' >/dev/null; then
    echo "Selected patrol root changed during reconciliation; refusing to continue."
    exit 1
  fi
else
  before_pour_patrol_root
  WISP=$(pour_patrol_root)
  if [ -z "$WISP" ]; then
    echo "Could not create patrol root."
    exit 1
  fi
fi
if ! gc bd ${GC_RIG:+--rig="$GC_RIG"} update "$WISP" \
  --status=in_progress --assignee="$GC_AGENT"; then
  echo "Could not mark patrol root $WISP current."
  exit 1
fi
echo "Resuming patrol wisp $WISP"
{{ end }}
