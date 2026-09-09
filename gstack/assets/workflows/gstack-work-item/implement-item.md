Implement the assigned gstack shared-drain item.

Gas City owns the shared drain and source-anchor lifecycle. It also owns the
lane's worktree: read `work_dir` from the source anchor, require an absolute
existing path distinct from the launcher checkout, `cd` there, and verify
`pwd -P` before any source read, edit, proof run, or commit. Fail this step if
`work_dir` is missing or does not resolve — never fall back to the launcher
checkout.

Read only the assigned item scope, implement the smallest complete change, run
focused proof, and record intended behavior, first verification command, changed
files, proof command, and remaining risks.

Close with `gc.outcome=pass` only after verification.

Do not invoke provider-native subagents. You are the single item lane.

Artifact validation: this step is gated by `.gc/scripts/checks/build-artifact-valid.sh`, which validates the summary recorded at `gc.implementation.summary_path` (fallbacks `gc.build.implementation_summary_path`, then `gc.var.summary_path`) against schema `gc.build.implementation-summary.v1`. On repair attempts (`gc.attempt` greater than 1), read the validator errors from `gc.attempt_log` on the validation loop control bead (the dependent of this step bead) and repair the summary in place instead of rewriting it. Two bounded repair attempts follow the first failure; exhausting them closes this stage with `gc.outcome=fail` and machine-readable validation errors that block downstream stages. Never ask questions in headless mode; record unresolved ambiguity inside the summary.
