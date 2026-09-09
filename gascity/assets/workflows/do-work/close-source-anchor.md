
Resolve `<source-anchor-id>` from the workflow ROOT, which is the SINGLE SOURCE
OF TRUTH for every step of this lane: read the claimed step bead's
`gc.root_bead_id`, then read that root's `gc.source_anchor_id` metadata.
`prepare-worktree` resolved the anchor deterministically and stamped it there,
so DO NOT re-derive it — close EXACTLY that id, and use exactly that value as
`<source-anchor-id>` everywhere below (worktree resolution AND every
`gc bd show` / `bd update --set-metadata` that names the source anchor).

FALLBACK — only if the root has NO `gc.source_anchor_id` (e.g. a hand-run step
where `prepare-worktree` never ran): derive it exactly as `prepare-worktree`
does — read the root's `gc.input_convoy_id`, unwrap a one-element list, and if
that convoy has `gc.synthetic_kind=drain-unit-convoy` use its
`gc.drain_member_id`, otherwise use the input convoy id itself. Never infer the
anchor from dependency ids such as the `prepare-worktree` or implementation
step bead.

Read `work_dir` from the source anchor and verify the implementation commit and
summary evidence are present in that worktree. Write per-item summary to
{{summary_path}} when set. If `summary_path` is not set, first use
`gc.implementation.summary_path` from the preceding implementation step when it
is present; otherwise use `{{artifact_root}}/task-<source-anchor-id>-summary.md`.

When reading beads with `gc bd show --json`, handle both an object and a
one-element list before reading metadata. `gc.work_dir` is the launcher rig
root, not the implementation worktree. If the source anchor `work_dir` is
missing, equals the launcher root, or points at a worktree without the
implementation commit, fail this step instead of closing the source anchor.

On success, close only `<source-anchor-id>` with `gc.outcome=pass`. Include the
verified commit and summary path in the source-anchor close reason. Read the
source anchor back with `gc bd show <source-anchor-id> --json` and verify
`status=closed` and `gc.outcome=pass`; if either check fails, fix the source
anchor before closing this step. Do not close this step with pass while the source anchor remains open. Then close this step. Do not close the drain-unit
convoy, parent convoy, or broader workflow root from this step.
