
Write the review verdict report to {{report_path}} with pass/fail, findings,
missing evidence, and recommended fixes for subject {{subject_path}}.

The requested review authority is `{{review_mode}}`: in `report` mode, write
findings and verdicts without mutating code; in `agent` mode, also include a
structured fix handoff for the caller's review-fix formula to apply; in
`interactive` mode, safe fixes may be negotiated or applied with every change
and reason recorded in the report. The interaction posture is
`{{interaction_mode}}`.

Use `status: approved` only when there are no findings. Otherwise use
`status: changes_required` or `status: blocked`; never leave the report at
`draft`, `questions`, or `superseded` when the review is done — callers that
derive a machine verdict from this report (for example the github-pr-review
adapter) only recognize those three terminal statuses.

When `status` is not `approved`, the `## Findings` section must contain a
Markdown table with exactly these columns, one row per finding, so callers can
parse verdicts and severities without re-reading prose:

| ID | Severity | Title | Evidence | Required Fix |
| --- | --- | --- | --- | --- |
| rev-1 | major | Missing null check | src/foo.ts:42 dereferences without a guard | Add a null check before use |

`Severity` must be `minor`, `major`, or `blocker`. Use `status: blocked` only
when at least one row is `blocker` severity; use `status: changes_required`
otherwise.

Artifact validation: this step is gated by `.gc/scripts/checks/build-artifact-valid.sh`, which validates the report recorded at `gc.build.review_report_path` (fallback `gc.var.report_path`) against schema `gc.build.review.v1`. On repair attempts (`gc.attempt` greater than 1), read the validator errors from `gc.attempt_log` on the validation loop control bead (the dependent of this step bead) and repair the report in place instead of rewriting it. Two bounded repair attempts follow the first failure; exhausting them closes this stage with `gc.outcome=fail` and machine-readable validation errors that block downstream stages. Never ask questions in headless mode; record unresolved ambiguity inside the report.

## Required artifact location

`{{report_path}}` is relative to the durable rig root, not the current
per-bead worktree. Read the rig root from `$GC_RIG_ROOT` and write the report
to `$GC_RIG_ROOT/{{report_path}}`; do not write it under the current directory
or `$GC_WORK_DIR`. The inference gate reads the artifact from that durable
location after this disposable worktree is removed.

Before closing, resolve the workflow-root id from the claimed bead's
`gc.root_bead_id`, then record the rig-relative path on that root:

```bash
gc bd update "<workflow-root-id>" \
  --set-metadata 'gc.build.review_report_path={{report_path}}'
```

From `$GC_RIG_ROOT`, run the artifact validator with the claimed bead id. Fix
any error before setting `gc.outcome=pass`:

```bash
GC_BEAD_ID=<claimed-step-id> .gc/scripts/checks/build-artifact-valid.sh
```
