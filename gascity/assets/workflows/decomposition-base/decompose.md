This is the `decomposition-base` methodology contract decomposition step.

Concrete methodology packs override this step to translate `{{plan_path}}` into
their native task shape, using optional context from `{{context_path}}`. Write
or record `{{decomposition_path}}` when the caller supplied an explicit
decomposition artifact path, and record the resolved path on workflow root
metadata as `gc.build.decomposition_path` before closing. The output must still
be an implementation convoy that downstream implementation formulas can drain
without knowing the planning methodology.

Dependency orientation contract: wire ordering edges as
`gc bd dep add <dependent> <prerequisite>` — the first argument WAITS, the
second is what it waits for (predecessor blocks successor; successor depends
on predecessor). Never wire a declared sequence temporally
(`gc bd dep add <earlier> <later>` is inverted and drains back-to-front).
When the artifact declares work items with an ID/Bead/Depends On table (rows
in execution order, Depends On referencing earlier rows only, no Status
column), the validation gate verifies the live edges against that declaration
and fails with the exact `gc bd dep add`/`gc bd dep remove` repair commands.

Artifact validation: this step is gated by `.gc/scripts/checks/build-artifact-valid.sh`, which validates the artifact recorded at `gc.build.decomposition_path` (fallback `gc.var.decomposition_path`) against schema `gc.build.decomposition.v1`. On repair attempts (`gc.attempt` greater than 1), read the validator errors from `gc.attempt_log` on the validation loop control bead (the dependent of this step bead) and repair the artifact in place instead of rewriting it. Two bounded repair attempts follow the first failure; exhausting them closes this stage with `gc.outcome=fail` and machine-readable validation errors that block downstream stages. Never ask questions in headless mode; record unresolved ambiguity inside the artifact.
