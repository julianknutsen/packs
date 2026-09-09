Implement this Compound Engineering shared-drain item with {{implementation_target}}.

Run inside the existing shared worktree lifecycle. The `prepare-shared-worktree`
step has already recorded that worktree on the source anchor as `work_dir`: read
it, require an absolute existing path that is not the launcher checkout, `cd`
there, and verify `pwd -P` before reading, editing, verifying, or committing
anything. A missing or unusable `work_dir` fails this step; it does not license
working in the launcher checkout.

Resolve the assigned drain member from metadata, validate ownership and context
path {{context_path}} when set, apply the smallest implementation changes for
this item, run focused verification, write an item summary to
`{{artifact_root}}/task-<source-anchor-id>-summary.md`, and close only the
source anchor on success. Record the summary path, changed files, and
verification result on the source anchor before closing it.

Do not invoke provider-native subagents. This Gas City lane is the work
delegation mechanism for ce-work.

Artifact validation: this step is gated by `.gc/scripts/checks/build-artifact-valid.sh`, which validates the summary recorded at `gc.implementation.summary_path` (fallbacks `gc.build.implementation_summary_path`, then `gc.var.summary_path`) against schema `gc.build.implementation-summary.v1`. On repair attempts (`gc.attempt` greater than 1), read the validator errors from `gc.attempt_log` on the validation loop control bead (the dependent of this step bead) and repair the summary in place instead of rewriting it. Two bounded repair attempts follow the first failure; exhausting them closes this stage with `gc.outcome=fail` and machine-readable validation errors that block downstream stages. Never ask questions in headless mode; record unresolved ambiguity inside the summary.
