Close the Superpowers implementation source anchor.

Resolve `<source-anchor-id>` from the workflow ROOT, which is the SINGLE SOURCE
OF TRUTH for every step of this lane: read the claimed step bead's
`gc.root_bead_id`, then read that root's `gc.source_anchor_id` metadata. The
inherited `prepare-worktree` resolved the anchor deterministically and stamped
it there, so DO NOT re-derive it — close EXACTLY that id.

FALLBACK — only if the root has NO `gc.source_anchor_id` (e.g. a hand-run step
where `prepare-worktree` never ran): derive it exactly as the inherited
`prepare-worktree` does — read the root's `gc.input_convoy_id`, unwrap a
one-element list, and if that convoy has `gc.synthetic_kind=drain-unit-convoy`
use its `gc.drain_member_id`, otherwise use the input convoy id itself. Never
infer the anchor from dependency ids such as the `prepare-worktree` or
implementation step bead.

Read `work_dir`, verify the task summary exists, verify the expected task commit
or clean working-tree evidence exists, and confirm the source anchor still
matches the current drained item.

On success, close only the source anchor with `gc.outcome=pass`. Read the
source anchor back and verify it is closed before closing this step. Do not close
the drain-unit convoy, parent convoy, workflow root, or post-implementation
review steps.

Do not invoke provider-native subagents or upstream plugin runtime commands.
