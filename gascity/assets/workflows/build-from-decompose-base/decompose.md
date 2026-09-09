This is the `build-from-decompose-base` decomposition stage.

Read the approved requirements from `{{requirements_path}}`, the approved implementation plan from `{{plan_path}}`, and the plan-review verdict from `{{plan_review_path}}`. Use the selected decomposition methodology `{{decomposition_formula}}` when translating the plan into durable work items.

Create or adopt an implementation convoy for the work units. The convoy must contain only runnable implementation beads for this continuation; do not reuse any original request, planning, or workflow-control convoy.

Write the decomposition artifact to `{{decomposition_path}}` when supplied; otherwise write it under `{{artifact_root}}` as the default decomposition artifact. The decomposition must include work item IDs, requirement and plan traceability, expected files or formula assets, verification expectations, dependencies, skipped work, and blocked work with rationale.

Declare work-item dependencies in a machine-readable table with ID, Bead, and Depends On columns (rows in intended execution order; every Depends On entry references an earlier row; no Status column), and wire the live edges to match: `gc bd dep add <dependent> <prerequisite>` — the first argument WAITS, the second is what it waits for. Never wire a declared sequence temporally (`gc bd dep add <earlier> <later>` is inverted and drains back-to-front). The validation gate verifies live edge orientation against the declared table and fails with the exact `gc bd dep add`/`gc bd dep remove` repair commands; repair the edges, not just the artifact text.

Record the implementation convoy ID on the workflow root bead as both:

- `gc.input_convoy_id=<implementation-convoy-id>` for the drain contract.
- `gc.build.implementation_convoy_id=<implementation-convoy-id>` for continuation reporting.

Close this step only after the decomposition artifact is recorded and both convoy metadata fields are set. Verify the recorded implementation convoy is not the original launch or workflow-control convoy.

Artifact validation: this stage is gated by `.gc/scripts/checks/build-artifact-valid.sh`, which validates the artifact recorded at `gc.build.decomposition_path` (fallback `gc.var.decomposition_path`) against schema `gc.build.decomposition.v1`. On repair attempts (`gc.attempt` greater than 1), read the validator errors from `gc.attempt_log` on the validation loop control bead (the dependent of this step bead) and repair the artifact in place instead of rewriting it. Two bounded repair attempts follow the first failure; exhausting them closes this stage with `gc.outcome=fail` and machine-readable validation errors that block downstream stages. Never ask questions in headless mode; record unresolved ambiguity inside the artifact.
