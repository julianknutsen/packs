Inspect the Superpowers shared-drain implementation task.

Resolve `gc.drain_member_id`, `gc.drain_index`, the existing shared
worktree, the approved requirements, approved plan, decomposition artifact, and
this task bead. The task bead describes the work unit only; the drained
Superpowers implementation workflow supplies the execution procedure.

Use the installed `executing-plans`, `test-driven-development`, and
`using-git-worktrees` skills as guidance for this task. Confirm the task can run
in the shared-drain lane, identify the first test behavior to drive, and write a
compact task context note for the following implementation steps.

Do not edit source files in the launcher checkout. Do not invoke
provider-native subagents or upstream plugin runtime commands.

Artifact validation: this step is gated by `.gc/scripts/checks/build-artifact-valid.sh`, which validates the summary recorded at `gc.implementation.summary_path` (fallbacks `gc.build.implementation_summary_path`, then `gc.var.summary_path`) against schema `gc.build.implementation-summary.v1`. On repair attempts (`gc.attempt` greater than 1), read the validator errors from `gc.attempt_log` on the validation loop control bead (the dependent of this step bead) and repair the summary in place instead of rewriting it. Two bounded repair attempts follow the first failure; exhausting them closes this stage with `gc.outcome=fail` and machine-readable validation errors that block downstream stages. Never ask questions in headless mode; record unresolved ambiguity inside the summary.
