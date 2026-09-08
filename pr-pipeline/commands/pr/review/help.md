Self-review an outgoing PR against an 11-category scorecard before
requesting external review. Catches structural and correctness defects
that a careful maintainer review would flag.

This dispatches a coding agent to a rig with the `mol-pr-review` formula.
The agent fetches the PR diff, scores findings across 11 categories,
pre-flags 7 recurring fixup themes, and writes a scorecard report to
`.gc/pr-pipeline/reviews/pr-<N>.md`. **No fixes are applied** — apply
fixes by re-iterating the development loop, not by extending this formula.

Scope: `mol-pr-review` reviews **outgoing** PRs (your own PR before
submitting it). Reviewing **incoming** PRs (someone else's PR; review +
merge) is a separate, maintainer-side concern, not covered here.

Usage:
  gc <binding> pr review <pr-number-or-url> [flags]

Arguments:
  <pr>                PR number (in current repo) or GitHub PR URL.

Flags:
  --rig <name>        Rig to review inside (defaults to $GC_RIG).
  --agent <name>      Worker agent name (default: "polecat").

Examples:
  gc <binding> pr review 1234 --rig api-server
  gc <binding> pr review https://github.com/owner/repo/pull/1234

Direct sling (skip this command):
  gc sling api-server/polecat mol-pr-review --formula --var pr=1234

Output:
  Report at <repo-root>/.gc/pr-pipeline/reviews/pr-<N>.md
  Root-bead notes record `verdict:` (block | request_changes | approve |
  too_large). `too_large` means the PR exceeded --max-diff-lines and was
  refused WITHOUT being reviewed; it is not a judgement about the code.

Budget:
  --max-diff-lines <n>   Refuse a PR whose additions+deletions exceed <n>,
                         before any agent is started. Default 5000; 0 disables.
                         Env: GC_PR_MAX_DIFF_LINES. Exits 3 on refusal.
                         Exit 3 describes the REQUEST, not a failed run: an
                         identical retry cannot succeed and costs the same
                         budget again. Dead-letter it, do not retry it.
                         The formula re-measures the fetched diff as well, so
                         a PR that grows after the check is still refused
                         before the diff reaches the model.

Decision policy (mechanical):
  Unresolved blocker in cat 1-4   → verdict block
  Unresolved blocker in cat 5-8   → verdict request_changes
  Major in cat 1-8 unmitigated    → verdict request_changes
  Only minors / nits              → verdict approve
