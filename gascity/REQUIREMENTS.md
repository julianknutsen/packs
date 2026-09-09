# Gas City Build Methodology Base Requirements

Schema: `gc.build-methodology-base.requirements.v1`

| Field | Value |
| --- | --- |
| Status | Pilot |
| Scope | Normative base formula contract and default Gas City implementation for build methodology packs |
| Formula ledger | `formulas/REQUIREMENTS.md` |
| Reference implementation | `build-basic` |
| Implementations to validate later | `compound-engineering`, `superpowers`, `bmad`, `gstack` |

This ledger is the compatibility contract for the Gas City build methodology
family. It is not just documentation for the current base formulas. Every
concrete methodology pack that extends this base must preserve these user-facing
and artifact-facing behaviors unless this ledger is intentionally updated.

## Purpose

Gas City needs one approachable default factory and one stable base contract.
New users should be able to run `build-basic` and see the whole software-factory
lifecycle: requirements, plan, plan review, decomposition, implementation,
review, fix loop, final report, and optional publish. Methodology pack authors
should be able to map upstream frameworks into Gas City without changing the
experience users expect from the raw framework.

The base pack provides orchestration infrastructure: durable formulas, fanouts,
drains, convoy identity, artifacts, gates, and mode handling. It must not hide
work inside provider-native subagents or hardcode methodology-specific roles.

## How To Reconcile

For every base-pack formula, prompt asset, adapter, or public override change:

1. Read this document and `formulas/REQUIREMENTS.md`.
2. Update the affected requirements before or with the implementation change.
3. Update formula graphs, workflow assets, tests, and docs so the requirement,
   graph, prompt, artifact shape, and validation agree.
4. If the change affects derived packs, record the compatibility expectation in
   this ledger and the affected formula row before considering those packs
   reconciled.
5. Do not let an external toolkit behavior become a base requirement unless it
   is reusable across methodology implementations.

## Vocabulary

- **Base anchor** - A mandatory lifecycle stage in `build-base`: `prepare`,
  `requirements`, `plan`, `plan-review`, `decompose`, implementation,
  `review`, `finalize`, and `publish`.
- **Base formula** - A formula in this pack that defines reusable methodology
  behavior or an adapter surface.
- **Build-basic** - The beginner-friendly reference implementation of
  `build-base`; it is powerful enough to demonstrate factories, but avoids
  provider-native subagents and excessive methodology ceremony.
- **Convoy-step implementation** - A custom implementation strategy that
  consumes the decomposition convoy as a whole instead of using the default
  drain implementation. It may sequence, batch, or fan out internally only when
  it records observable evidence for each work item.
- **Drain policy** - The default implementation strategy for a convoy.
  Supported base values are `separate` and `same-session`.
- **Gas City fanout** - A durable formula expansion or graph branch that
  replaces upstream subagent/task-tool dispatch while preserving reviewer or
  worker perspective.
- **Artifact validator** - The shared base-pack validator invoked by
  formula-specific check steps to validate front matter, structural markdown,
  coverage, traceability, and schema identity.
- **Coverage matrix** - Machine-readable and human-readable accounting for
  every upstream requirement/story/example/acceptance ID, with status and
  rationale when not covered.
- **Interaction mode** - `interaction_mode`; controls whether the workflow may
  ask humans questions or request approval.
- **Methodology implementation** - A pack such as Compound Engineering,
  Superpowers, BMAD, or gstack that imports this pack as `gc` and implements
  the base contract with its own formulas and prompt assets.
- **Path-shadow override** - A stable file path that `build-basic` users can
  shadow in a city/local pack to customize prompts without learning formula
  step overrides.
- **Schema repair** - A bounded route back to the artifact-producing stage using
  validator errors as repair context.
- **Review mode** - `review_mode`; controls whether review is report-only,
  agent handoff, or interactive top-level review behavior.
- **Stage selector** - A formula variable such as `planning_formula` or
  `code_review_formula` that lets adapters choose methodology-specific
  formulas without changing the top-level graph.

## Normative Compatibility Contract

`build-base` is the virtual lifecycle contract. `build-basic` is the reference
implementation, not the whole requirement. Derived methodology packs may add
top-level stages only as anchored extensions:

- the base anchors remain mandatory and in order;
- each added stage declares its insertion point, gate, artifact or metadata
  output, and whether the behavior is methodology-specific or reusable;
- added stages must not rename, skip, or reorder base anchors;
- added stages must preserve downstream artifact, traceability, mode, review,
  and publish contracts.

## Global Invariants

- `build-base` is internal and not user-runnable.
- `build-basic` is cataloged and extends `build-base`.
- `build-from-*-base` formulas are internal continuation suffix contracts. Each
  one validates its prerequisite inputs, performs one stage or handoff, and
  delegates to the next suffix through an inherited handoff step.
- `build-from-*` formulas are cataloged default Gas City wrappers around the
  matching continuation suffix bases.
- All formulas use `contract = "graph.v2"` and do not redeclare reserved
  runtime variables: `issue`, `bead_id`, or `convoy_id`.
- Raw-framework subagents are converted to Gas City fanouts, drains, or formula
  steps. The base pack must not preserve subagent functionality as opaque
  provider-native dispatch.
- Stage selectors are the compatibility surface for planning, decomposition,
  implementation, shared-drain implementation items, code review, and fix
  loops.
- `interaction_mode` is first-class and must be honored by planning, review,
  decomposition, fix, and publish gates.
- `review_mode` is separate from `interaction_mode` and must be honored by
  review entrypoints and adapters.
- Producer-stage artifact validation is explicit graph/check behavior, not
  prompt-only guidance.
- Decomposition is required before implementation. Implementations consume a
  convoy, not an unstructured prompt, unless the decomposition intentionally
  produces exactly one work item.
- Publishing and external side effects are opt-in through explicit variables or
  gated adapter steps.

## Base Stage Artifact Contracts

Every base stage has a named artifact or metadata contract. Derived packs may
add artifacts, but the base minimum must remain discoverable at the standard
paths.

| Stage | Required behavior |
| --- | --- |
| `prepare` | Validates and records artifact root, context inputs, drain policy, stage selectors, publish variables, selected formula names, selected modes, and resolved artifact paths on root metadata. |
| `requirements` | Produces an approved requirements artifact with the strict base shape defined below and stable behavior requirement IDs. |
| `plan` | Produces an implementation plan or engineering design traceable to approved requirement IDs. |
| `plan-review` | Produces an approval verdict. It approves the plan, requests concrete changes, asks questions only when the mode permits, or blocks with a reason. |
| `decompose` | Produces durable work units and records implementation convoy identity on root metadata. |
| `implement` | Executes the selected implementation strategy and writes per-item or explicitly mapped implementation evidence. |
| `implement-same-session` | Executes the selected same-session implementation strategy, preserving shared context and item traceability. |
| `review` | Produces a review verdict/report with required fixes or approval. |
| `finalize` | Writes the final workflow report covering requirements, plan, decomposition, implementation, review attempts, risk, drift, publish status, and next action. |
| `publish` | No-ops unless authorized. When authorized, records push and PR status or a blocked publish reason. |

## Requirements Artifact Contract

The requirements stage must produce a strict, ARCP-inspired requirements
artifact. Derived packs may add methodology-specific sections, but they must not
replace, rename, or omit the base sections.

Required sections:

- `Problem Statement`
- `W6H`
- `User Stories`
- `Technical Stories` or `None`
- `Behavior Requirements` with stable IDs and `SHALL` language
- `Example Mapping`
- `Acceptance Criteria`
- `Out Of Scope`
- `Open Questions`

Requirements are the behavioral source of truth. Any code or formula change
must result in new or updated requirements when behavior changes, and any
requirements change must be implemented or explicitly marked out of scope.

User stories, technical stories, behavior requirements, example mapping entries,
and acceptance criteria require stable IDs. Local project artifacts may use
local IDs such as `US-001`, `TS-001`, `BR-001`, `EX-001`, and `AC-001`.
Pack-owned requirements may use longer scoped IDs such as `GC-METH-BR-001`.
When a technical-stories section is intentionally `None`, no technical-story ID
is required for that section.

## Artifact Layout And Schemas

The base artifact layout is stable:

| Artifact | Path |
| --- | --- |
| Requirements | `<artifact_root>/requirements.md` |
| Implementation plan | `<artifact_root>/implementation-plan.md` |
| Decomposition | `<artifact_root>/decomposition.md` |
| Implementation summary | `<artifact_root>/implementation-summary.md` |
| Review report | `<artifact_root>/reviews/attempt-<n>/report.md` |
| Review fixes | `<artifact_root>/reviews/attempt-<n>/fixes.md` |
| Final report | `<artifact_root>/final-report.md` |
| GitHub issue adapter data | `<artifact_root>/github/issues/...` |
| GitHub PR adapter data | `<artifact_root>/github/pulls/...` |

The base pack owns machine-readable schemas for base artifacts. The human
requirements ledger is normative, and schema files are the enforcement target
for formula checks. Base schemas are expected at stable paths:

Formula checks owned by this pack use paths relative to the defining formula
layer, such as `../assets/scripts/checks/build-artifact-valid.sh`. Gas City
resolves those paths to the winning pack layer before instantiation. They are
not launcher- or store-relative `.gc/scripts` paths, and a target rig is not
required to install a copy of the validator.

| Schema | Expected path |
| --- | --- |
| `gc.build.requirements.v1` | `gascity/schemas/build/requirements.v1.yaml` |
| `gc.build.plan.v1` | `gascity/schemas/build/plan.v1.yaml` |
| `gc.build.decomposition.v1` | `gascity/schemas/build/decomposition.v1.yaml` |
| `gc.build.implementation-summary.v1` | `gascity/schemas/build/implementation-summary.v1.yaml` |
| `gc.build.review.v1` | `gascity/schemas/build/review.v1.yaml` |
| `gc.build.final-report.v1` | `gascity/schemas/build/final-report.v1.yaml` |

Derived packs may add stricter methodology-specific schemas or extension fields,
but they must not relax the base schema or replace required base sections,
fields, statuses, traceability, or coverage.

Schema IDs are immutable compatibility contracts. Breaking schema changes create
a new schema ID such as `gc.build.requirements.v2`; they do not mutate the
meaning of a published `v1` schema.

Base artifacts use lightweight YAML front matter. Required schema values are:

- `gc.build.requirements.v1`
- `gc.build.plan.v1`
- `gc.build.decomposition.v1`
- `gc.build.implementation-summary.v1`
- `gc.build.review.v1`
- `gc.build.final-report.v1`

Minimum front-matter fields:

- `schema`
- `workflow.id`
- `workflow.formula`
- `methodology.pack`
- `methodology.name`
- `producer.formula`
- `producer.stage`
- `producer.attempt`
- `status`
- `trace`

Base artifacts do not require owner, stage-owner, persona, or role fields.
Producer metadata is neutral execution metadata, not a role model. Every base
artifact, including review attempts, fix artifacts, and final reports, records
both the top-level workflow formula and the direct producer formula.

Example front matter:

```yaml
schema: gc.build.requirements.v1
workflow:
  id: build-20260609-001
  formula: build-basic
methodology:
  pack: gascity
  name: build-basic
producer:
  formula: planning-base
  stage: requirements
  attempt: 1
status: approved
trace:
  upstream: []
  coverage: []
```

## Artifact Validation And Repair

The base pack provides one shared artifact validator. Formula-specific check
steps invoke it with the schema ID and artifact path instead of each formula
implementing its own validation behavior.

Example invocations:

```text
validate-artifact --schema gc.build.requirements.v1 --path requirements.md
validate-artifact --schema gc.build.plan.v1 --path implementation-plan.md
validate-artifact --schema gc.build.review.v1 --path reviews/attempt-1/report.md
```

The validator checks both YAML front matter and structural markdown. It validates
machine-readable fields, schema identity, approval status, producer metadata,
traceability, upstream hashes, coverage IDs and statuses, required sections,
required section order where the schema defines order, and markdown coverage
matrix consistency. It does not judge product quality, architectural taste, or
whether the model's reasoning is correct.

Producer-stage validation runs after every artifact-producing stage. Validation
failure routes back to the producing stage for schema repair using validator
errors as repair context. Repair is bounded and never infinite. The base default
is two repair attempts per artifact stage:

- `requirements_schema_repair_attempts = 2`
- `plan_schema_repair_attempts = 2`
- `decomposition_schema_repair_attempts = 2`
- `implementation_summary_schema_repair_attempts = 2`
- `review_schema_repair_attempts = 2`
- `final_report_schema_repair_attempts = 2`

Derived packs may lower or raise repair-attempt counts, but they must remain
finite. `interaction_mode=headless` still follows automatic schema repair and
must not ask the user. When repair attempts are exhausted, the workflow records
`blocked` with machine-readable validation errors.

## Traceability And Drift

Traceability is practical and bidirectional:

- user-story, technical-story, behavior-requirement, example, and acceptance
  IDs flow into the plan, decomposition, implementation summary, review reports,
  fix artifacts, and final report;
- plans cite the requirement IDs they satisfy;
- decompositions cite the plan sections and requirement IDs they implement;
- implementation summaries cite work item IDs and requirement IDs;
- review reports cite requirement IDs when approving or requesting changes;
- final reports summarize every requirement ID as satisfied, unsatisfied,
  blocked, or out of scope.

Every downstream artifact contains a coverage matrix that accounts for every
upstream ID. The YAML front matter is authoritative; the markdown body mirrors
the coverage matrix for humans.

Allowed coverage statuses:

- `covered`
- `not_applicable`
- `deferred`
- `blocked`
- `out_of_scope`
- `superseded`

Every non-`covered` status requires a rationale in YAML. The markdown matrix
must contain the same IDs and statuses as YAML, but the human-readable evidence
or rationale prose does not need to match byte-for-byte.

Example trace shape:

```yaml
trace:
  upstream:
    - path: requirements.md
      hash: sha256:...
  coverage:
    - id: BR-001
      status: covered
    - id: AC-004
      status: blocked
      rationale: "Missing deployment credentials."
```

Downstream artifacts record upstream paths and content hashes or revision IDs:

- if requirements change after the plan, the plan is drifted;
- if the plan changes after decomposition, the decomposition is drifted;
- if decomposition changes after implementation starts, remaining work must be
  reconciled or the workflow must block with a drift reason;
- review and final-report stages must surface drift instead of proceeding
  silently.

## Approval States

Base approval states are:

- `draft`
- `questions`
- `approved`
- `changes_required`
- `blocked`
- `superseded`

Downstream stages consume only `approved` upstream artifacts. `questions`
requires interaction unless `interaction_mode=headless`, where it becomes
`blocked`. `changes_required` loops to the producer or the review/fix loop.
`blocked` stops downstream execution. `superseded` cannot be consumed. Derived
packs may define their own criteria, but they must map outcomes into these
states.

## Mode Contracts

`interaction_mode` values:

| Value | Required behavior |
| --- | --- |
| `interactive` | The workflow may ask questions and request approvals. It asks one material question at a time and includes a recommendation. |
| `autonomous` | The workflow does not block on ordinary planning questions. It records assumptions and may block only for unsafe actions, missing required evidence, or external-state requirements. |
| `headless` | The workflow never asks questions. Missing required input fails closed with machine-readable blocked reasons. |

`review_mode` values:

| Value | Required behavior |
| --- | --- |
| `report` | Review writes findings and verdicts only. It never mutates code. This is the default for PR reviews, comments, and gap analysis. |
| `agent` | Review writes a structured handoff for an implementation/fix loop. The caller applies fixes. |
| `interactive` | Review preserves raw top-level review behavior. It may negotiate and apply safe fixes when allowed, recording changes and reasons. |

Planning, review, decomposition, fix, and publish gates must honor both modes.
Adapters must pass modes through to selected formulas and stop if the selected
formula does not support the requested mode.

## Review And Fix Loop Contract

`build-base` owns high-level review/fix loop semantics. The default verdict
states are `approved`, `changes_required`, and `blocked`.

When review returns `changes_required`, the workflow runs `review_fix_formula`
and repeats review/fix until:

- review returns `approved`;
- review or fix returns `blocked`;
- the configured maximum iteration count is reached.

Each review attempt writes `reviews/attempt-<n>/report.md`. Each fix attempt
writes `reviews/attempt-<n>/fixes.md`. The final report includes all attempts,
verdicts, fixes, unresolved risks, and the terminal state.

If review/fix cannot reach approval because evidence is missing, a drain failed,
review returned `blocked`, report mode forbids mutation, or max iterations were
exhausted, the workflow must preserve the failure as a healable restart state.
It records `gc.build.repair_status`, `gc.restart.entrypoint`,
`gc.restart.reason`, relevant artifact paths, and a failing workflow outcome
rather than converting the run to pass during finalization or no-op publish.

`fix-loop-base` owns the default implementation shape:

- plan fixes from review findings;
- apply fixes through the selected implementation path;
- re-run the selected review formula;
- stop only on approval, block, or maximum iteration termination.

## Drain And Implementation Strategy Contract

Derived packs are not required to support both `separate` and `same-session`.
They declare `allowed_drain_policies` and may implement only one policy.

Derived packs may override the default drain with a custom convoy-step
implementation. If they do, they must preserve:

- input convoy identity;
- work item traceability;
- implementation evidence;
- review/fix handoff;
- final report traceability;
- observable formula/fanout work instead of opaque provider-native subagents.

A custom convoy-step may sequence, batch, or fan out internally, but it must
record per-work-item evidence or an explicit mapping from work item IDs to the
work performed.

## Formula Boundary Contract

Structured repeatable phases should become formulas or formula expansions.
Small local checklists may remain in prompt assets. A phase should become a
separate formula or expansion when it has any of these properties:

- multiple roles or personas;
- reviewer fanout/fanin;
- retry or loop behavior;
- durable intermediate artifacts;
- independent approval gates;
- meaningful resumability value.

Hard control-flow gates belong in formula checks or explicit graph steps.
Prompt rubrics are for qualitative criteria. Formula checks or graph steps own
schema validation, approved/blocked detection, file existence, review verdict
evaluation, drift detection, maximum iteration termination, and publication
authorization.

Schema validation is handled by the shared artifact validator. Formula-specific
steps decide when to invoke validation, how to route repair, and which schema ID
and artifact path apply.

## Build-Basic Reference Contract

`build-basic` demonstrates beginner-friendly power, not a minimal toy flow. It
must include:

- strict requirements;
- traceability;
- design/plan review;
- durable decomposition;
- an explicit implementation strategy;
- small review fanout;
- a fix loop;
- a final report;
- optional publish.

Default `build-basic` review lanes are:

- acceptance/correctness;
- test evidence;
- simplicity/maintainability.

Security, performance, release, docs, and product-architecture lanes are not
default lanes. Derived packs or city overrides may add them when appropriate.

## Build-Basic Path-Shadow Override Contract

For `build-basic` only, path-shadow-based overrides are a stable public API for
major prompt pieces. Users must be able to customize common behavior by placing
simple files in their city directory or local pack, without learning formula
step overrides.

Required override pieces:

- `requirements`
- `plan`
- `plan-review`
- `decompose`
- `implement`
- `implement-same-session`
- `review`
- one file per review lane
- `synthesize-review`
- `apply-review-findings`
- `finalize`
- `publish`

Each review lane has exactly one stable override file. Low-level graph
mechanics, check scripts, and dependency edges remain formula overrides.

`build-basic` must provide a machine-readable override registry for test and
compliance purposes. Runtime behavior must not depend on this registry. The
registry lists stable override paths, and tests verify that required pieces are
listed and that listed paths exist. Derived packs may skip this registry.

## Continuation Entrypoint Contract

The base pack may expose cataloged continuation entrypoints for advanced
factory workflows that already possess approved upstream artifacts. A
continuation entrypoint is valid only when it declares its prerequisite inputs,
validates them before side effects, and then runs the suffix of the build graph
without silently regenerating skipped stages.

Continuation bases are nested suffixes:

| Base | Prerequisite | Does | Delegates to |
| --- | --- | --- | --- |
| `build-from-requirements-base` | initial requirements context | produce or reuse requirements | `build-from-plan-base` |
| `build-from-plan-base` | approved requirements | produce plan and plan-review | `build-from-decompose-base` |
| `build-from-decompose-base` | approved requirements, plan, and plan-review | create or adopt implementation convoy | `build-from-convoy-base` |
| `build-from-convoy-base` | implementation convoy | drain implementation work and record evidence | `build-from-review-base` |
| `build-from-review-base` | implementation evidence | review, repair or restart handoff, finalize, optionally publish | terminal |

Each suffix may be launched directly when its prerequisite inputs already
exist. A suffix must not silently rerun skipped upstream stages. Concrete
methodology packs may extend the suffix base that matches their entrypoint and
override selector defaults, routes, drain formulas, or review expansions while
preserving the downstream suffix handoff.

## Methodology Metadata Contract

Top-level build formulas declare compatibility metadata in one formal metadata
area:

```toml
[metadata.gc.methodology]
```

Required on:

- `build-base`;
- `build-basic`;
- `build-from-*-base`;
- `build-from-*`;
- concrete derived top-level build formulas.

Not required on:

- helper formulas;
- GitHub adapters.

GitHub adapters pass through and validate selected formula compatibility rather
than declaring the full methodology metadata themselves.

Allowed metadata vocabulary:

| Field | Allowed values |
| --- | --- |
| `allowed_drain_policies` | `separate`, `same-session` |
| `implementation_strategy` | `drain`, `convoy-step` |
| `interaction_modes` | `interactive`, `autonomous`, `headless` |
| `review_modes` | `report`, `agent`, `interactive` |

Unknown values fail lint/tests. Derived packs choose subsets of the base
vocabulary. `allowed_drain_policies` may be empty only when
`implementation_strategy="convoy-step"`.

## GitHub Adapter Contract

GitHub adapters do not define the full methodology metadata block, but they must
expose, pass through, and validate selector compatibility.

Issue-fix adapters pass through:

- planning formula selector;
- decomposition formula selector;
- implementation formula selector;
- code review formula selector;
- review/fix loop formula selector;
- interaction mode;
- review mode where applicable;
- drain policy or implementation strategy.

PR-review adapters pass through:

- code review formula selector;
- review mode;
- interaction mode where applicable.

Adapters must stop with a clear blocked reason when selected formulas do not
support requested modes, drain policies, or implementation strategies.

## User Stories

### GC-METH-US-001: Run A Beginner-Friendly Default Factory

As a new automated-factory user, I want to run `build-basic` from requirements
through reviewed implementation, so I can learn the Gas City factory model from
a complete but approachable workflow.

Acceptance criteria:

- `build-basic` is cataloged and extends `build-base`.
- The workflow preserves the full base stage sequence.
- The starter review uses the default three review lanes and a fix loop.
- The workflow writes the standard artifact layout.

### GC-METH-US-002: Implement A Raw Toolkit With Gas City Infrastructure

As a methodology pack author, I want a stable base formula contract, so I can
map upstream planning, implementation, review, QA, and release behavior into
Gas City while preserving the raw toolkit experience.

Acceptance criteria:

- Base formulas expose selector variables for stage substitution.
- Derived packs can declare supported modes and implementation strategies.
- Upstream subagent-like behavior becomes durable Gas City fanouts, drains, or
  formulas.
- Derived packs can add anchored stages without breaking base anchors.

### GC-METH-US-003: Reuse Adapters With Methodology-Specific Review

As a GitHub workflow user, I want issue-fix and PR-review adapters to call the
selected methodology formulas, so GitHub workflows use the same methodology as
direct build workflows.

Acceptance criteria:

- `github-pr-review` accepts and validates `code_review_formula`.
- `github-issue-fix-base` accepts the build-stage selectors needed for
  planning, decomposition, implementation, review, and fix loops.
- Adapters own snapshot, comment, and publication lifecycle.
- Adapters stop when selected formulas cannot satisfy requested modes.

### GC-METH-US-004: Keep Requirements Beside Implementation

As a maintainer, I want requirements to live next to formula implementation, so
behavior changes are reviewed against intended outcomes instead of TOML diffs
alone.

Acceptance criteria:

- This ledger is updated for base contract changes.
- `formulas/REQUIREMENTS.md` is updated for formula behavior changes.
- Tests fail when formula coverage drifts.

### GC-METH-US-005: Customize Build-Basic Without Formula Expertise

As a beginner using `build-basic`, I want to customize major prompts by
shadowing simple files, so I can add local review or planning criteria without
learning formula step overrides.

Acceptance criteria:

- Major `build-basic` prompt pieces have stable path-shadow overrides.
- Each review lane has exactly one stable override file.
- The override registry exists for tests/compliance and is not a runtime
  dependency.

### GC-METH-US-006: Run Headless In Automation

As a CI or automation user, I want `interaction_mode=headless` to never ask
questions, so the workflow fails closed with structured blocked reasons instead
of hanging.

Acceptance criteria:

- Required missing input produces `blocked`.
- Questions are not emitted in headless mode.
- Final reports include machine-readable blocked reasons.

## Technical Stories

### GC-METH-TS-001: Keep Base Formula Coverage Complete

As the test suite, I need every base-pack formula to have a requirements row,
because a formula graph edit is a behavior change even if no Go or Python code
changes.

Proof expectation: `gascity/tests/test_formula_assets.py` compares
`formulas/REQUIREMENTS.md` to the formula set.

### GC-METH-TS-002: Preserve Third-Party Compatibility

As external methodology implementations, derived packs need base formula stage
IDs, selector variables, modes, drain semantics, metadata vocabulary, and
artifact paths to stay stable unless a requirements row intentionally changes
them.

Proof expectation: formula tests validate base stage contracts and third-party
extension defaults.

### GC-METH-TS-003: Detect Artifact Drift

As the workflow controller and reviewers, downstream stages need upstream
artifact paths and hashes so stale plans, decompositions, implementation
summaries, reviews, and final reports cannot silently pass.

Proof expectation: schema checks and graph gates validate artifact status,
hashes, and approved states before downstream consumption.

### GC-METH-TS-004: Validate Methodology Metadata

As adapter formulas, selected top-level methodology formulas need formal
compatibility metadata so unsupported modes, drain policies, and implementation
strategies fail before work starts.

Proof expectation: lint/tests reject unknown metadata vocabulary and unsupported
selector combinations.

### GC-METH-TS-005: Enforce Build-Basic Override Coverage

As the `build-basic` compliance suite, the override registry needs to list every
required stable override path and prove each listed path exists.

Proof expectation: tests compare the registry to required pieces and prompt
asset paths.

### GC-METH-TS-006: Validate Artifacts With Shared Schemas

As formula checks, artifact-producing stages need a shared validator and
base-owned schemas so validation behavior is deterministic and consistent across
the base and derived methodology packs.

Proof expectation: schema checks validate front matter and structural markdown
after each producer stage, then route repair or block with machine-readable
errors.

### GC-METH-TS-007: Enforce Coverage Matrix Consistency

As downstream stages and reviewers, artifacts need authoritative YAML coverage
plus a mirrored markdown matrix so machines can gate behavior and humans can
read the same traceability story.

Proof expectation: validation fails when YAML and markdown coverage IDs or
statuses diverge, or when non-covered statuses omit rationale.

### GC-METH-TS-008: Preserve Neutral Producer Metadata

As the base SDK contract, artifacts need audit metadata without encoding
hardcoded roles or upstream-framework personas.

Proof expectation: validation requires `workflow.formula`, `producer.formula`,
`producer.stage`, `producer.attempt`, `methodology.pack`, and
`methodology.name`, and does not require owner or role fields.

## Behavior Requirements

| ID | Trace | Requirement |
| --- | --- | --- |
| GC-METH-BR-001 | GC-METH-US-001 | WHEN a user launches the default build workflow, THE base pack SHALL provide `build-basic` as the cataloged concrete implementation of `build-base`. |
| GC-METH-BR-002 | GC-METH-US-002 | WHEN a methodology pack extends the base contract, THE pack SHALL preserve the base anchors in order and SHALL NOT rename, skip, or reorder them. |
| GC-METH-BR-003 | GC-METH-US-002 | WHEN a methodology pack adds top-level stages, THE added stages SHALL declare insertion point, gate, artifact or metadata output, and whether the behavior is methodology-specific or reusable. |
| GC-METH-BR-004 | GC-METH-US-002 | WHEN an upstream toolkit uses subagents, task tools, or plugin commands for worker or reviewer fanout, THE Gas City implementation SHALL model that behavior as formulas, expansion children, drains, or fanout/fanin lanes. |
| GC-METH-BR-005 | GC-METH-US-002 | WHEN base stage IDs, selector variables, mode semantics, artifact contracts, metadata vocabulary, or drain semantics change, THE change SHALL update this ledger and the formula ledger before derived packs are considered reconciled. |
| GC-METH-BR-006 | GC-METH-US-001 | WHEN the `prepare` stage runs, THE workflow SHALL validate and record artifact root, context inputs, selectors, modes, drain or implementation strategy, publish variables, and resolved artifact paths. |
| GC-METH-BR-007 | GC-METH-US-004 | WHEN the `requirements` stage completes, THE workflow SHALL produce the required base sections, stable behavior IDs, `SHALL` requirements, and an approval state. |
| GC-METH-BR-008 | GC-METH-US-004 | WHEN the `plan` stage completes, THE workflow SHALL produce an implementation plan or engineering design traceable to approved requirement IDs. |
| GC-METH-BR-009 | GC-METH-US-004 | WHEN `plan-review` completes, THE workflow SHALL produce a verdict mapped to `approved`, `questions`, `changes_required`, or `blocked`. |
| GC-METH-BR-010 | GC-METH-US-001 | BEFORE implementation starts, THE workflow SHALL run decomposition and record durable work units plus implementation convoy identity. |
| GC-METH-BR-011 | GC-METH-US-001 | WHEN implementation completes, THE workflow SHALL write implementation evidence mapped to work item IDs and requirement IDs. |
| GC-METH-BR-012 | GC-METH-US-001 | WHEN review completes, THE workflow SHALL write a review report with an `approved`, `changes_required`, or `blocked` verdict. |
| GC-METH-BR-013 | GC-METH-US-001 | IF review returns `changes_required`, THEN THE workflow SHALL run the selected review-fix formula and repeat review/fix until approval, block, or maximum iteration termination. |
| GC-METH-BR-014 | GC-METH-US-001 | WHEN the workflow finalizes, THE final report SHALL summarize requirements, plan, decomposition, implementation, review attempts, fixes, drift, risk, publish status, and next action. |
| GC-METH-BR-015 | GC-METH-US-001 | WHEN publish is not explicitly authorized, THE publish stage SHALL no-op and record `not_published`. |
| GC-METH-BR-016 | GC-METH-US-001 | WHEN publish is authorized, THE publish stage SHALL record push status, PR status, or a blocked publish reason. |
| GC-METH-BR-051 | GC-METH-US-001 | IF review/fix cannot reach approval because evidence is missing, a drain failed, review is blocked, report mode forbids mutation, or maximum iterations are exhausted, THEN finalization SHALL record a failing blocked outcome plus `gc.build.repair_status` and `gc.restart.*` metadata rather than closing the workflow as pass; publish no-op SHALL preserve that outcome. |
| GC-METH-BR-017 | GC-METH-TS-003 | WHEN a downstream artifact consumes an upstream artifact, THE downstream artifact SHALL record the upstream path and content hash or revision ID. |
| GC-METH-BR-018 | GC-METH-TS-003 | IF upstream artifacts drift after downstream work starts, THEN review and finalization SHALL surface drift and SHALL NOT silently proceed. |
| GC-METH-BR-019 | GC-METH-US-006 | WHEN `interaction_mode=interactive`, THE workflow MAY ask one material question at a time and SHALL include a recommendation. |
| GC-METH-BR-020 | GC-METH-US-006 | WHEN `interaction_mode=autonomous`, THE workflow SHALL avoid ordinary planning blocks, record assumptions, and block only for unsafe actions, missing required evidence, or external-state requirements. |
| GC-METH-BR-021 | GC-METH-US-006 | WHEN `interaction_mode=headless`, THE workflow SHALL never ask questions and SHALL fail closed with machine-readable blocked reasons when required input is missing. |
| GC-METH-BR-022 | GC-METH-US-003 | WHEN `review_mode=report`, THE review workflow SHALL write findings and verdicts without mutating code. |
| GC-METH-BR-023 | GC-METH-US-003 | WHEN `review_mode=agent`, THE review workflow SHALL produce a structured fix handoff for the caller to apply. |
| GC-METH-BR-024 | GC-METH-US-003 | WHEN `review_mode=interactive`, THE review workflow MAY negotiate or apply safe fixes and SHALL record changes and reasons. |
| GC-METH-BR-025 | GC-METH-US-002 | WHEN a derived pack supports default drain implementation, THE pack SHALL declare supported values in `allowed_drain_policies`. |
| GC-METH-BR-026 | GC-METH-US-002 | WHEN a derived pack uses `implementation_strategy="convoy-step"`, THE pack MAY declare no drain policies but SHALL preserve convoy identity, item traceability, implementation evidence, review/fix handoff, and final report traceability. |
| GC-METH-BR-027 | GC-METH-US-002 | WHEN a custom convoy-step sequences, batches, or fans out work internally, THE implementation SHALL record per-work-item evidence or an explicit item-to-work mapping. |
| GC-METH-BR-028 | GC-METH-US-002 | WHEN a phase has multiple personas, fanout/fanin, retry behavior, durable artifacts, approval gates, or resumability value, THE behavior SHALL be modeled as a formula or formula expansion rather than only prompt text. |
| GC-METH-BR-029 | GC-METH-TS-003 | WHEN a gate controls execution, THE gate SHALL live in formula checks or explicit graph steps; prompt rubrics SHALL be limited to qualitative guidance. |
| GC-METH-BR-030 | GC-METH-US-005 | WHEN a user customizes `build-basic`, THE major prompt pieces SHALL be shadowable through stable public file paths. |
| GC-METH-BR-031 | GC-METH-US-005 | WHEN a `build-basic` review lane is customized, THE lane SHALL have exactly one stable override file. |
| GC-METH-BR-032 | GC-METH-TS-005 | WHEN `build-basic` defines override paths, THE pack SHALL provide a machine-readable registry for tests/compliance and SHALL NOT make runtime behavior depend on that registry. |
| GC-METH-BR-033 | GC-METH-TS-004 | WHEN a top-level build formula implements the methodology contract, THE formula SHALL declare `[metadata.gc.methodology]` compatibility metadata. |
| GC-METH-BR-034 | GC-METH-TS-004 | WHEN methodology metadata contains unknown mode, drain, or implementation-strategy vocabulary, lint/tests SHALL fail. |
| GC-METH-BR-035 | GC-METH-US-003 | WHEN a GitHub issue-fix adapter runs, THE adapter SHALL pass and validate planning, decomposition, implementation, code review, fix-loop, interaction-mode, review-mode, and implementation-strategy selectors. |
| GC-METH-BR-036 | GC-METH-US-003 | WHEN a GitHub PR-review adapter runs, THE adapter SHALL pass and validate code-review formula, review mode, and interaction mode where applicable. |
| GC-METH-BR-037 | GC-METH-US-003 | IF an adapter-selected formula does not support the requested mode, drain policy, or implementation strategy, THEN THE adapter SHALL stop with a clear blocked reason. |
| GC-METH-BR-038 | GC-METH-TS-001 | WHEN a base-pack formula exists, THE formula ledger SHALL contain a stable requirement row for that formula. |
| GC-METH-BR-039 | GC-METH-TS-006 | WHEN an artifact-producing stage completes, THE workflow SHALL run an explicit formula check that invokes the shared artifact validator for the expected schema and path. |
| GC-METH-BR-040 | GC-METH-TS-006 | WHEN artifact validation fails, THE workflow SHALL route back to the producing stage for schema repair using validator errors as repair context. |
| GC-METH-BR-041 | GC-METH-TS-006 | WHEN schema repair attempts are exhausted, THE workflow SHALL stop downstream execution with `blocked` and machine-readable validation errors. |
| GC-METH-BR-042 | GC-METH-TS-006 | WHEN a derived pack changes schema repair attempt counts, THE configured count SHALL remain finite. |
| GC-METH-BR-043 | GC-METH-TS-006 | WHEN the shared validator runs, THE validator SHALL check required YAML front matter and structural markdown shape without making product or architecture judgment calls. |
| GC-METH-BR-044 | GC-METH-US-004 | WHEN requirements include user stories, technical stories, behavior requirements, example mapping entries, or acceptance criteria, EACH item SHALL have a stable ID unless the technical-stories section is explicitly `None`. |
| GC-METH-BR-045 | GC-METH-TS-007 | WHEN a downstream artifact is produced, THE artifact SHALL include a YAML coverage matrix that accounts for every upstream ID using only base coverage statuses. |
| GC-METH-BR-046 | GC-METH-TS-007 | WHEN a coverage item is not `covered`, THE YAML coverage entry SHALL include a rationale. |
| GC-METH-BR-047 | GC-METH-TS-007 | WHEN an artifact includes a markdown coverage matrix, THE markdown matrix SHALL match YAML coverage IDs and statuses while allowing different prose. |
| GC-METH-BR-048 | GC-METH-TS-008 | WHEN a base artifact is produced, THE artifact SHALL record neutral `workflow`, `methodology`, and `producer` metadata, including top-level workflow formula and direct producer formula. |
| GC-METH-BR-049 | GC-METH-TS-008 | WHEN base artifact schemas are defined, THE schemas SHALL NOT require owner, stage-owner, persona, or role fields. |
| GC-METH-BR-050 | GC-METH-TS-006 | WHEN a base schema ID is published, THE meaning of that schema ID SHALL remain immutable; breaking changes SHALL use a new schema ID. |
| GC-METH-BR-051 | GC-METH-US-001 | WHEN prerequisite inputs already exist for a build stage, THE base pack SHALL provide reusable `build-from-*-base` continuation suffixes that validate those prerequisites, perform only their owned stage or handoff, and delegate to the next suffix without silently rerunning skipped upstream stages. |
| GC-METH-BR-052 | GC-METH-US-002 | WHEN a methodology pack needs a continuation entrypoint, THE pack SHOULD extend the matching `build-from-*-base` suffix and override selectors, routes, drain formulas, or review expansions instead of copying the suffix graph. |
| GC-METH-BR-053 | GC-METH-US-001 | WHEN a user wants the built-in Gas City continuation behavior, THE base pack SHALL provide cataloged `build-from-*` wrappers that extend the matching suffix bases. |

## Scenario Ledger

| ID | Scenario | Required behavior | Evidence |
| --- | --- | --- | --- |
| GC-METH-001 | Base stage sequence | `build-base` defines the stable stage sequence `prepare -> requirements -> plan -> plan-review -> decompose -> implementation -> review -> finalize -> publish`. | `formulas/build-base.formula.toml`; `tests/test_formula_assets.py::FormulaAssetTests::test_build_base_is_full_lifecycle_virtual_contract` |
| GC-METH-002 | Default implementation | `build-basic` extends `build-base`, is cataloged, preserves the base stage sequence, uses beginner-friendly prompts, and uses starter review fanout through `build-basic-review`. | `formulas/build-basic.formula.toml`; `formulas/build-basic-review.formula.toml`; `tests/test_formula_assets.py::FormulaAssetTests::test_build_basic_extends_full_lifecycle_base` |
| GC-METH-003 | Stage selector compatibility | `build-base`, `github-issue-fix-base`, and `github-pr-review` expose methodology selector vars with defaults that point at the base implementation. | `tests/test_formula_assets.py::FormulaAssetTests::test_entrypoint_adapters_expose_methodology_formula_vars` |
| GC-METH-004 | Virtual stage contracts | `planning-base`, `decomposition-base`, `implementation-base`, `implementation-item-base`, `code-review-base`, and `fix-loop-base` are internal, non-catalog base contracts with shadowable step assets. | `tests/test_formula_assets.py::FormulaAssetTests::test_methodology_stage_contracts_are_virtual_and_shadowable` |
| GC-METH-005 | Requirements artifact shape | Requirements artifacts include the base sections, stable `SHALL` behavior requirements, example mapping, acceptance criteria, out-of-scope, and open questions. | `assets/workflows/build-basic/requirements.md`; this ledger |
| GC-METH-006 | Traceability and drift | Downstream artifacts carry upstream paths and hashes, and review/finalization surface drift. | this ledger; future schema/gate tests |
| GC-METH-007 | Review/fix loop | Review verdicts drive fix-loop iterations and per-attempt report/fix artifacts until approval, block, or maximum iterations. | `formulas/build-basic-review.formula.toml`; `formulas/fix-loop-base.formula.toml` |
| GC-METH-008 | Mode handling | `interactive`, `autonomous`, and `headless` interaction modes plus `report`, `agent`, and `interactive` review modes remain distinct and are propagated through selectors. | this ledger; `README.md`; `tests/test_formula_assets.py::FormulaAssetTests::test_entrypoint_adapters_expose_methodology_formula_vars` |
| GC-METH-009 | Drain and convoy-step compatibility | Derived packs may declare supported drain policies or replace drains with a convoy-step implementation while preserving convoy evidence. | this ledger; derived pack formula metadata |
| GC-METH-010 | Build-basic path-shadow overrides | `build-basic` exposes stable major prompt override paths and one override file per review lane. | this ledger; future override-registry tests |
| GC-METH-011 | Methodology metadata | Top-level build formulas declare `[metadata.gc.methodology]`; GitHub adapters validate selected formula compatibility. | this ledger; `formulas/build-base.formula.toml`; `README.md`; `tests/test_formula_assets.py::FormulaAssetTests::test_entrypoint_adapters_expose_methodology_formula_vars` |
| GC-METH-012 | External implementation compatibility | Compound Engineering, Superpowers, BMAD, and gstack import this pack as `gc`, extend `build-base`, replace raw subagent dispatch with Gas City formulas/fanouts, and preserve base artifact/mode contracts. | `compound-engineering/REQUIREMENTS.md`; `superpowers/REQUIREMENTS.md`; `bmad/REQUIREMENTS.md`; `gstack/REQUIREMENTS.md`; `tests/test_derived_pack_compatibility.py::DerivedPackCompatibilityTests`; `docs/design/build-methodology-framework-audit.md` |
| GC-METH-013 | Shared artifact validation | Formula-specific check steps invoke one shared validator after producer stages and route failed validation back for bounded repair. | this ledger; future schema/gate tests |
| GC-METH-014 | Coverage matrix consistency | YAML coverage is authoritative, markdown coverage mirrors IDs/statuses, and all non-covered statuses include rationale. | this ledger; future schema/gate tests |
| GC-METH-015 | Neutral artifact metadata | Artifacts record workflow, methodology, and producer metadata without owner or role fields. | this ledger; future schema/gate tests |
| GC-METH-016 | Nested continuation suffixes | `build-from-requirements-base -> build-from-plan-base -> build-from-decompose-base -> build-from-convoy-base -> build-from-review-base` form a nested suffix chain. Each suffix validates its prerequisite, performs its owned work, and hands off to the next suffix. Cataloged `build-from-*` wrappers expose the default Gas City behavior. | `formulas/build-from-*-base.formula.toml`; `formulas/build-from-*.formula.toml`; `tests/test_formula_assets.py::FormulaAssetTests::test_build_continuation_bases_form_nested_suffix_chain`; `tests/test_formula_assets.py::FormulaAssetTests::test_default_continuation_entrypoints_extend_suffix_bases` |

## Deferred Follow-Up Requirements

| ID | Status | Follow-up condition |
| --- | --- | --- |
| GC-METH-012 | covered | Concrete implementation ledgers exist for `compound-engineering`, `superpowers`, `bmad`, and `gstack`, and `tests/test_derived_pack_compatibility.py` proves import-as-`gc`, anchored `build-base` extension with base anchors in order, methodology metadata vocabulary, selector defaults, drain or convoy-step strategy, providerless route targets, the shared claim protocol, the absence of provider-native subagent dispatch, and the pack-local ledgers for all four packs. |

## Evidence Index

- `python3 -m pytest gascity/tests/test_formula_assets.py -q`
- `python3 -m pytest gascity/tests/test_derived_pack_compatibility.py -q`
- `python3 -m pytest -q`
- `gascity/tests/test_formula_assets.py::FormulaAssetTests::test_base_formula_requirements_cover_formula_set`
- `gascity/tests/test_formula_assets.py::FormulaAssetTests::test_build_base_is_full_lifecycle_virtual_contract`
- `gascity/tests/test_formula_assets.py::FormulaAssetTests::test_third_party_build_packs_extend_base_and_vendor_sources`
- `gascity/tests/test_derived_pack_compatibility.py::DerivedPackCompatibilityTests`

## Maintenance Rules

- Requirements rows use stable IDs and should not be renumbered.
- Formula requirements describe behavior, artifact contracts, graph contracts,
  fanout/fanin, drain semantics, mode behavior, metadata, and side effects, not
  TOML formatting.
- Behavior-changing code, formula, or prompt edits require a corresponding
  requirements edit.
- Requirements changes require implementation or an explicit out-of-scope or
  follow-up record.
- Derived-pack implementation ledgers live in `compound-engineering`,
  `superpowers`, `bmad`, and `gstack`; keep them in sync with
  `tests/test_derived_pack_compatibility.py`.
