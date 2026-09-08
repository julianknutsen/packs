# gc gastown task-artifact-cleanup

Idempotently retire a Gastown task worktree after its work bead reaches a
verified terminal handoff.

Usage:

```bash
gc gastown task-artifact-cleanup [work-bead-id]
```

The command requires `GC_CITY_PATH`, `GC_RIG`, and `GC_RIG_ROOT`. It accepts
only the exact canonical artifact path for that city and rig, or the exact
historical polecat-provider layout for an intentionally adopted legacy
artifact. It never force-removes a worktree.

Before removal it requires a closed work bead, an exact supported
`merge_result`, a clean worktree, and an exact `artifact_source_sha` match. It
then applies the evidence matrix for that exact result:

- `merged` / `already_merged` requires the exact artifact source SHA at
  `origin/polecat/<bead>` and independently refreshes the recorded target to
  prove `merged_sha` is reachable there as terminal-state corroboration;
- publication-only `pull_request` requires the source ref to equal the
  validated `pr_head_sha`, but makes no merge or target-ancestry claim; and
- `mr_merged` requires that same exact validated PR-head ref plus refreshed
  target reachability for `merged_sha`.

The target checks do not prove a source-to-merge relation. In every accepted
case the exact source ref remains independent recovery evidence after local
cleanup. Mutable bead metadata alone is never sufficient deletion evidence.

The pending `merge_result=pull_request_pending`, obsolete
`merge_result=pull_request_merged`, and unknown result values are not terminal,
even on a closed bead. Non-terminal handoffs are marked
`artifact_cleanup_state=pending`; a later verified transition calls the same
command again. Successful or already-completed cleanup records
`artifact_cleanup_state=complete`.
With no bead ID, the command retries at most one closed artifact still marked
`pending` whose result is exactly `merged`, `already_merged`, `pull_request`,
or `mr_merged`. Filtering terminal results before ordering the sweep prevents
an older non-terminal record from starving a newer actionable cleanup. The
refinery runs that bounded sweep before looking for new work.
