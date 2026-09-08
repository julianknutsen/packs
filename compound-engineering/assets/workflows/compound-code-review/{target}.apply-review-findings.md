Apply required Compound Engineering code-review findings.

Read the synthesized review report and resolve required findings in this single
lane. Preserve traceability to the reviewer, finding id, severity, file anchors,
and acceptance criteria.

Read the synthesized report from `gc.build.code_review_report_path`. Write the
review-fix summary to `gc.build.review_fix_summary_path`, which should be
`{{artifact_root}}/code-review/apply-summary.md`.

Use implementation target {{implementation_target}} for any code changes.
Close this lane only after the review-fix artifact records changed files, tests
run, resolved findings, and blockers. If there are no required fixes, record a
no-op review-fix artifact instead of editing code.

If the synthesized report approves the implementation with no required fixes,
perform a no-op pass, update workflow root metadata with
`gc.build.code_review_status=approved`, and close with
`code_review.verdict=done`. If required fixes remain after processing, update
workflow root metadata with `gc.build.code_review_status=draft` and close with
`code_review.verdict=iterate`.

Loop-verdict contract (livelock guard): `code_review.verdict=iterate` is
permitted ONLY when this lane produced an actionable delta: it applied at
least one fix this iteration, OR at least one blocking finding remains that
the next iteration of THIS loop can resolve against the reviewed tree. If the
applied set for this iteration is empty AND every remaining finding is
non-blocking, suppressed, human-ratified, or resolvable only outside this loop
(unmerged branches, integration folds, human decisions), the loop has
converged: update workflow root metadata with
`gc.build.code_review_status=approved` and close with
`code_review.verdict=done` (approve-with-notes — findings stay recorded in the
report and routed to beads/escalation; they do not gate this loop). Re-minting
the same review lanes on unchanged code cannot produce a different outcome;
never answer that state with `iterate`. Also record
`code_review.applied_count=<fixes applied this iteration>` and
`code_review.actionable_remaining=<remaining findings this loop can still
resolve>` so the approval check can detect convergence deterministically.

Always close with `gc.outcome=pass`,
`code_review.report_path=<review fix summary path>`, and
`code_review.output_path=<review fix summary path>`.

Do not invoke provider-native subagents. This graph lane is the delegation
mechanism.
