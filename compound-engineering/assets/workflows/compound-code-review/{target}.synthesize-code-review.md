Synthesize the Compound Engineering code-review lanes.

Merge correctness, testing, maintainability, standards, CE always-on agent
outputs, selected conditional reviewer outputs, skipped conditional no-op
artifacts, deployment-verification notes, and gap-analysis findings into one
implementation-review report. Count skipped conditional artifacts as coverage
records, not reviewer failures. Deduplicate overlaps, suppress non-actionable
noise, classify required fixes, and write the approval verdict used by
`.gc/scripts/checks/implementation-review-approved.sh`. Required fixes must be
specific enough for the single apply step to resolve them directly.

Verdict contract (livelock guard): `iterate` requires an actionable delta —
at least one blocking finding the apply lane can resolve against the reviewed
tree inside this loop, OR a non-empty apply plan for this iteration. If every
finding is non-blocking, suppressed, human-ratified, or out of scope for this
loop, and nothing is applicable this iteration, the verdict MUST be `approve`
with `status: approved` (approve-with-notes: findings stay recorded and
out-of-loop work is routed to beads/escalation; nothing gates this loop). A
blocking finding fixable only outside this loop never justifies `iterate`.
Convergence guard: if the reviewed tree is byte-identical to the previous
iteration AND the finding set is unchanged, return `approve` — iterating on
unchanged input is futile.

Read the review context from `gc.build.code_review_context_path` and all lane
artifacts from `{{artifact_root}}/code-review/`. Write the synthesized report to
`gc.build.code_review_report_path`, which should be
`{{artifact_root}}/code-review/review-report.md`.

The synthesized report must be valid for `gc.build.review.v1`: start with YAML
front matter containing `schema: gc.build.review.v1`, `workflow`,
`methodology`, `producer`, `status`, and `trace`; include a Markdown coverage
table; and include `## Verdict`, `## Findings`, and `## Verification`
sections. Use `status: changes_required` when required fixes remain, and use
schema-allowed coverage statuses only (`covered`, `blocked`, `deferred`,
`not_applicable`, `out_of_scope`, `superseded`). Do not use `violated`,
`resolved`, `approved`, or `changes_required` as coverage row statuses. Include
`rationale: <why this id is not covered>` on every non-`covered` coverage row.

Use this front matter shape exactly. Do not use dotted YAML keys such as
`workflow.id`, and do not make `trace` a list:

```yaml
---
schema: gc.build.review.v1
workflow:
  id: <workflow-root-id>
  formula: compound-review
methodology:
  pack: compound-engineering
  name: compound-review
producer:
  formula: compound-code-review
  stage: synthesize-code-review
  attempt: 1
status: changes_required
trace:
  upstream:
    - path: <relative input artifact path>
      hash: sha256:<input artifact digest>
      ids: [<finding-or-lane-id>]
  coverage:
    - id: <finding-or-lane-id>
      status: covered
---
```

The Markdown coverage table must have `ID` and `Status` columns, and its rows
must exactly match `trace.coverage`.

Close with `gc.outcome=pass`,
`code_review.review_verdict=approve|iterate`, and
`code_review.review_report_path=<synthesized report path>`. Do not set
`code_review.verdict` or `code_review.report_path`; the apply-review-findings
lane owns the final loop verdict consumed by the approval check.

Do not invoke provider-native subagents. Synthesis happens in this Gas City lane.
