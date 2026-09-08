# PR Pipeline

Author-side and review-side PR discipline distributed as a Gas City pack.

Encodes the planning, blast-radius, scorecard-review, and pre-push gating
workflows that careful contributors run by hand, so any city that imports
this pack gets the same discipline as platform-native formulas and
commands.

## Status

**v0.1.0** — initial release. Ships six formulas: four with matching
wrapper commands, plus two dispatched on demand. The four wrapper-command
formulas:

| Formula | Command | Purpose |
|---------|---------|---------|
| `mol-pr-start`        | `gc <binding> pr plan <issue>`         | Issue → structured plan with BLOCKING gates |
| `mol-pr-blast-radius` | `gc <binding> pr blast-radius "<scope>"` | Map impact surface (callers, configs, concurrency) |
| `mol-pr-review`       | `gc <binding> pr review <pr-number>`   | Outgoing-PR self-review against an 11-category scorecard |
| `mol-pr-ship`         | `gc <binding> pr ship`                 | Pre-push gate (simplify → review-iterate → contributor check); STOPS at report |

All four wrapper-command formulas are read-only by default for filesystem
changes outside their own output paths. `mol-pr-ship` may modify the diff
during simplify and review-iteration stages; everything else writes only
to `.gc/pr-pipeline/<sub>/`. None of them push or open PRs — those
decisions stay with the caller.

Two further formulas ship without a wrapper command — they are dispatched
on demand (typically by a maintenance PL) rather than run by hand:

| Formula | Dispatch | Purpose |
|---------|----------|---------|
| `mol-pr-triage`     | `gc sling <rig>/<agent> mol-pr-triage --formula` | Scan/classify open issues into a ranked work-queue |
| `mol-pr-from-issue` | `gc sling <rig>/<agent> mol-pr-from-issue --formula --var issue_number=<N>` | Author-side macro chain: issue → plan → implement → ship gate → (optional) open PR |

`mol-pr-from-issue` is slung as a routed molecule, so its GitHub-issue
input is named **`issue_number`** rather than `issue` — the bare `issue`
var name is a reserved formulas-v2 alias for the routed work bead and
would be clobbered. It defaults to halt-at-branch-ready (`auto_push`
absent → no push, no PR); pass `--var auto_push=true` to authorize the
eligibility-gated push.

## Scope

This pack is the **author side** — planning, building, and shipping the
PRs your city sends out. Reviewing and merging **incoming** PRs that
arrive at your repo is a separate, maintainer-side concern and out of
scope here.

The one maintainer-side artifact shipped here is the adoption-review
comment template (`templates/adoption-review-comment.md`); it reuses the
same 11-category scorecard `mol-pr-review` applies, so review feedback
keeps a consistent shape in both directions.

## Testing across the layer boundary

**[`docs/testing-across-the-layer-boundary.md`](docs/testing-across-the-layer-boundary.md)**
is the standard behind the scorecard's evidence category, and the one to read
before writing or reviewing a fix anywhere in the dolt / beads / gascity / packs
stack.

It answers two questions the scorecard assumes you have already settled. First,
when does a test actually demonstrate that a fix repairs a live build, rather
than repairing a graph of stubs? A tier ladder, a fidelity claim owed per
replaced seam, and a second mutation that perturbs the stubs instead of only
reverting the fix. Second, which of the four repositories owns a given defect,
with a routing checklist and three open issues on this tracker that name
artifacts living in `gastownhall/gascity` rather than here.

## Usage

In your city's `pack.toml`:

```toml
[imports.pr-pipeline]
source = "../packs/pr-pipeline"   # path; or git URL when published
```

### Plan a PR for an issue

```sh
gc pr-pipeline pr plan 1234 --rig api-server
```

The formula reads the issue, runs BLOCKING gates (competing-PR and
architectural-refactor checks), maps blast radius, checks repo
conventions, writes a structured plan to
`.gc/pr-pipeline/plans/issue-1234.md`, and audits the plan against 19
recurring review findings. **No code is written.**

### Map blast radius for a freeform scope

```sh
gc pr-pipeline pr blast-radius "FuncXYZ in pkg/foo" --rig api-server
```

For changes that don't start from an issue — refactors, hotfixes,
exploratory deltas — `mol-pr-blast-radius` is a standalone entry point
with the same analysis shape the planner runs inline.

### Self-review an outgoing PR

```sh
gc pr-pipeline pr review 1234 --rig api-server
```

Scorecard against 11 categories (behavioral correctness, contract
fidelity, blast radius, concurrency, error handling, security, resource
lifecycle, release safety, test evidence, architectural consistency,
debuggability). Pre-flags 7 recurring fixup themes. Verdict: `block`,
`request_changes`, `approve`, or `too_large`.

A PR whose additions+deletions exceed `--max-diff-lines` (default 5000, `0`
disables) is refused before any agent is started, exiting 3 and recording
`verdict: too_large`. That refusal describes the request, not a failed run:
retrying it unchanged cannot succeed and spends the same budget again, so
supervising loops must not treat it as retryable.

### Run the pre-push gate

```sh
gc pr-pipeline pr ship --rig api-server
```

Four-stage pipeline: simplify → iterate an adversarial multi-reviewer
panel (parallel reviewer subagents + adversarial synthesis) until clean →
mechanical gates (build/vet/test/docs) → readiness report. **STOPS at
the report.** Push and PR-open are explicit caller actions this formula
never performs.

### Override the worker agent

Default agent for all wrappers is `polecat`. Override with `--agent`:

```sh
gc pr-pipeline pr plan 1234 --rig api-server --agent claude
```

Or sling directly without the wrapper:

```sh
gc sling api-server/polecat mol-pr-start --formula --var issue=1234
```

## Pack contents

```
pr-pipeline/
├── pack.toml
├── docs/
│   └── testing-across-the-layer-boundary.md   tier ladder + layer map
├── formulas/
│   ├── mol-pr-start.formula.toml          6-step planner
│   ├── mol-pr-blast-radius.formula.toml   5-step impact mapper
│   ├── mol-pr-review.formula.toml         4-step outgoing-PR scorecard
│   ├── mol-pr-ship.formula.toml           5-step pre-push gate
│   ├── mol-pr-triage.formula.toml         issue scan → ranked work-queue
│   └── mol-pr-from-issue.formula.toml     7-step issue → branch-ready PR
├── commands/
│   └── pr/
│       ├── plan/         (run.sh + help.md)
│       ├── blast-radius/
│       ├── review/
│       └── ship/
└── templates/
    └── adoption-review-comment.md         canonical reviewer-side
                                           comment template (Form 1: no
                                           maintainer changes; Form 2:
                                           maintainer fixups present)
```

### Shared templates

`templates/adoption-review-comment.md` is the canonical structure for the
synthesis comment a maintainer posts when adopting a contributor PR. Two
forms cover all four merge paths a maintainer-side adoption workflow
distinguishes (no maintainer changes vs. maintainer fixups present).

The template documents:

- The two literal comment shapes (Form 1 / Form 2)
- All inputs the renderer needs (PR metadata, synthesis + scorecard from
  the review run, maintainer-fixup log/diff, final tip SHA, model list,
  iteration count)
- Rendering rules per `{model-rendered}` field (the orchestration is
  mechanical; the prose is the model's job)
- Validation gates enforced before posting (verbatim opener, footer
  literal, section order, SHA prefix match, iteration-count match)
- A fail-stop fallback for malformed renders (write rejected text and
  STOP — never post a partial comment)
- Path-specific adjustments for the four merge paths (A / B / C / D)

The reviewer-side counterpart to this pack's `mol-pr-review`
author-side scorecard. Both produce structured findings against the same
11-category scorecard so contributors get consistent feedback shape
regardless of direction.

The full workflow for each formula lives in step descriptions. A coding
agent (polecat or equivalent) follows them in sequence; gates can
short-circuit with an early exit.

## Why formula-shaped, not agent-as-directory

This pack ships **formulas**, not standing agents. Each formula is a
bounded workflow ("plan one PR, exit" / "score one PR, exit") rather
than a long-lived role like mayor or polecat. The consumer city's
existing coding worker (whatever it's named) runs the formula — no
extra agent deployment required.

Standing roles (mayor, polecat, witness, refinery) belong in their own
packs as `agents/<name>/` directories. Bounded workflows belong as
`formulas/mol-<name>.formula.toml` with the workflow inlined in step
descriptions.
