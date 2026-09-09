# Base Formula Requirements

Schema: `gc.base-formulas.requirements.v1`

| Field | Value |
| --- | --- |
| Status | Pilot |
| Scope | Formula behavior for the Gas City build methodology base pack |
| Parent ledger | `../REQUIREMENTS.md` |

This ledger covers every formula in the base pack. The rows are grouped by
methodology contract, default implementation, implementation utilities, review
and fix utilities, GitHub adapters, and publication helpers.

## Purpose

The formula files are the executable architecture for the build methodology
base pack. Each row below names the user-visible or pack-author-visible
behavior that must remain true when the formula graph, vars, routing,
expansion, drain semantics, or step assets change.

## How To Reconcile

For every formula change:

1. Update the row for the changed formula.
2. Update `../REQUIREMENTS.md` when the change affects the base methodology
   contract or derived pack compatibility.
3. Update tests when a formula is added, removed, promoted to catalog, or
   changes stage IDs, selector vars, drain policy, or publication behavior.
4. Keep upstream toolkit-specific behavior out of this base ledger unless it is
   a generic compatibility rule for all implementations.

## Vocabulary

- **Virtual contract** - An internal base formula that defines behavior and
  artifact shape for external methodology packs to override.
- **Concrete default** - A runnable formula in this pack that implements the
  virtual contract without requiring a third-party toolkit.
- **Adapter** - A formula that owns external state such as GitHub snapshots,
  sticky comments, or PR publication while delegating planning/review stages to
  methodology formulas.
- **Selector variable** - A formula variable naming another methodology formula
  to invoke from a base or adapter workflow.

## Global Invariants

- Every formula uses `contract = "graph.v2"`.
- No formula declares reserved runtime vars `issue`, `bead_id`, or `convoy_id`.
- Cataloged formulas are user-runnable; internal/base/helper formulas are not
  cataloged unless deliberately promoted.
- Methodology packs override behavior through formula extension, expansion, or
  selector variables, not by preserving raw provider-native subagent dispatch.
- Targeted implementation formulas consume the core graph target. Targetless
  report and adapter formulas take explicit vars.
- Report formulas write durable report artifacts; adapters own external
  comment/publication lifecycle.
- `build-base`, `build-basic`, `build-from-*-base`, `build-from-*`, and
  concrete derived top-level build formulas declare formal
  `[metadata.gc.methodology]` compatibility metadata.
- Planning, decomposition, implementation, review, fix, and publish formulas
  preserve `interaction_mode` and `review_mode` semantics defined by
  `../REQUIREMENTS.md`.
- Artifact-producing formulas invoke the shared base artifact validator through
  explicit formula check steps and route failed validation back for bounded
  schema repair.
- Formula checks validate against base-owned schemas and preserve neutral
  `workflow`, `methodology`, and `producer` artifact metadata.
- Structured repeatable worker or reviewer behavior is expressed as formulas,
  expansions, drains, or fanout/fanin lanes; small local rubrics may remain in
  prompt assets.
- `build-basic` owns stable path-shadow override files for its major prompt
  pieces, including exactly one override file per default review lane.

## User Stories

### GC-BF-US-001: Author A Methodology Pack

As a methodology pack author, I want each base formula to state what it owns, so
I can decide which stages to extend, expand, or leave inherited.

Acceptance criteria:

- Every formula has a row in this ledger.
- Rows classify the launch shape and required behavior.
- Rows distinguish virtual contracts from concrete defaults and adapters.

### GC-BF-US-002: Review Formula Graph Changes

As a maintainer, I want formula requirements beside the formula files, so I can
review graph changes against behavior rather than inspecting TOML in isolation.

Acceptance criteria:

- Formula rows cite tests or formula files as evidence.
- Tests fail when formula coverage drifts.

## Technical Stories

### GC-BF-TS-001: Enforce Formula Ledger Coverage

As the test suite, I need a formula coverage guard so a new formula cannot be
added without a requirements row.

Proof expectation: `test_base_formula_requirements_cover_formula_set` checks
this ledger against the `FORMULAS` constant and formula directory.

## Behavior Requirements

| ID | Trace | Requirement |
| --- | --- | --- |
| GC-BF-BR-001 | GC-BF-TS-001 | WHEN a formula exists in `gascity/formulas`, THE formula ledger SHALL contain a row naming that formula. |
| GC-BF-BR-002 | GC-BF-US-001 | WHEN a formula is a virtual methodology contract, THE row SHALL name the stage or utility contract that derived packs may override. |
| GC-BF-BR-003 | GC-BF-US-001 | WHEN a formula is a concrete default, THE row SHALL name how it implements or extends the virtual contract. |
| GC-BF-BR-004 | GC-BF-US-002 | WHEN a formula owns external side effects, THE row SHALL name the gate or authorization behavior. |
| GC-BF-BR-005 | GC-BF-US-001 | WHEN a formula participates in the full build lifecycle, THE row SHALL preserve the artifact, approval-state, traceability, mode, and drift contracts in `../REQUIREMENTS.md`. |
| GC-BF-BR-006 | GC-BF-US-001 | WHEN a formula represents repeatable fanout/fanin, retry, approval, or resumable behavior, THE row SHALL describe it as durable Gas City graph behavior rather than provider-native subagent behavior. |
| GC-BF-BR-007 | GC-BF-US-001 | WHEN a top-level build formula is added or changed, THE formula SHALL declare methodology compatibility metadata or the row SHALL document why the formula is not a top-level build formula. |
| GC-BF-BR-008 | GC-BF-US-002 | WHEN an adapter delegates to selected methodology formulas, THE row SHALL name selector pass-through, compatibility validation, and blocked-mode behavior. |
| GC-BF-BR-009 | GC-BF-US-001 | WHEN a `build-basic` prompt asset is a major user customization point, THE formula row SHALL preserve its stable path-shadow override contract. |
| GC-BF-BR-010 | GC-BF-US-002 | WHEN a formula produces a base artifact, THE formula SHALL invoke shared artifact validation for the expected schema and path. |
| GC-BF-BR-011 | GC-BF-US-002 | WHEN shared artifact validation fails, THE formula graph SHALL route back to the producer for bounded repair or stop as `blocked` after finite attempts. |
| GC-BF-BR-012 | GC-BF-US-002 | WHEN formula output includes traceability, THE formula SHALL preserve YAML coverage as the machine-readable source and mirrored markdown coverage for humans. |
| GC-BF-BR-013 | GC-BF-US-002 | WHEN a continuation review, drain, or repair stage cannot reach approval, THE formula graph SHALL record `gc.build.repair_status`, `gc.restart.*` metadata, and a failing blocked outcome instead of allowing finalize or publish no-op to close the workflow as pass. |

## Scenario Ledger

### Methodology Contracts

| ID | Formula | Type | Required behavior | Evidence |
| --- | --- | --- | --- | --- |
| GC-BF-001 | `build-base` | Virtual targeted full-lifecycle contract | Defines the stable build stage sequence, selector variables, mode variables, methodology metadata, implementation strategy selection, artifact validation/repair gates, review/fix loop, finalization, and optional publication contract that concrete methodology packs extend. | `build-base.formula.toml`; `../tests/test_formula_assets.py` |
| GC-BF-002 | `planning-base` | Virtual targetless planning contract | Produces approved requirements and implementation-plan artifacts through prepare, requirements, plan, and plan-review stages while preserving strict artifact shape, approval states, hashes, coverage, validator repair, and mode behavior for adapters. | `planning-base.formula.toml`; `../tests/test_formula_assets.py` |
| GC-BF-003 | `decomposition-base` | Virtual targetless decomposition contract | Converts an approved plan into durable work units and implementation convoy identity that downstream drain or convoy-step implementation strategies consume, after validating decomposition schema, coverage, and upstream hashes. | `decomposition-base.formula.toml`; `../tests/test_formula_assets.py` |
| GC-BF-004 | `implementation-base` | Virtual targeted implementation contract | Executes one implementation source anchor with prepare-worktree, implement, and close-source-anchor stages while preserving source-anchor close, work-item evidence, requirement coverage, and neutral producer metadata. | `implementation-base.formula.toml`; `../tests/test_formula_assets.py` |
| GC-BF-005 | `implementation-item-base` | Virtual targeted shared-drain item contract | Executes exactly one item in a shared single-lane drain while preserving shared-session context, item sequencing, item evidence, requirement coverage, and neutral producer metadata. | `implementation-item-base.formula.toml`; `../tests/test_formula_assets.py` |
| GC-BF-006 | `code-review-base` | Virtual targetless report contract | Writes validated review output over a supplied subject, maps verdicts to base approval states, honors `review_mode`, preserves coverage/drift evidence, and leaves external lifecycle ownership to callers. | `code-review-base.formula.toml`; `../tests/test_formula_assets.py` |
| GC-BF-007 | `fix-loop-base` | Virtual targetless review-fix contract | Turns failed review findings into planned fixes, applies fixes with the selected implementation path, records per-attempt validated fix artifacts, and re-runs the selected review formula up to the iteration limit. | `fix-loop-base.formula.toml`; `../tests/test_formula_assets.py` |

### Default Implementation

| ID | Formula | Type | Required behavior | Evidence |
| --- | --- | --- | --- | --- |
| GC-BF-008 | `build-basic` | Cataloged concrete default | Extends `build-base`, preserves the full stage sequence, declares methodology metadata, uses built-in Gas City planning/decomposition/implementation helpers, writes a validated canonical implementation summary before review, exposes stable path-shadow overrides, validates artifacts through shared checks, and expands review through `build-basic-review`. | `build-basic.formula.toml`; `../tests/test_formula_assets.py`; `../tests/test_formula_assets.py::FormulaAssetTests::test_producer_stages_gate_artifacts_with_bounded_repair` |
| GC-BF-009 | `build-basic-review` | Expansion review loop | Runs starter review fanout across acceptance/correctness, test evidence, and simplicity/maintainability lanes, provides one override file per lane, synthesizes findings, applies required fixes, validates review/fix artifacts, and loops until approved, blocked, or attempts exhaust. | `build-basic-review.formula.toml`; `../tests/test_formula_assets.py` |

### Continuation Entrypoints

| ID | Formula | Type | Required behavior | Evidence |
| --- | --- | --- | --- | --- |
| GC-BF-025 | `build-from-review-base` | Virtual targetless review suffix | Validates implementation evidence, runs the selected code-review, repairs or records a restart handoff for review findings, finalizes without converting blocked repair states to pass, and optionally publishes. Higher suffixes hand off to this base through `prepare-review`. | `build-from-review-base.formula.toml`; `../tests/test_formula_assets.py::FormulaAssetTests::test_build_continuation_bases_form_nested_suffix_chain`; `../tests/test_formula_assets.py::FormulaAssetTests::test_build_from_review_blocked_results_are_healable_not_passed`; `../tests/test_formula_assets.py::FormulaAssetTests::test_producer_stages_gate_artifacts_with_bounded_repair` |
| GC-BF-026 | `build-from-convoy-base` | Virtual targetless implementation suffix | Validates or records an implementation convoy, drains implementation work through the selected drain policy, records implementation evidence, and hands off to `build-from-review-base`. | `build-from-convoy-base.formula.toml`; `../tests/test_formula_assets.py::FormulaAssetTests::test_build_continuation_bases_form_nested_suffix_chain` |
| GC-BF-027 | `build-from-decompose-base` | Virtual targetless decompose suffix | Validates existing approved requirements, plan, and plan-review artifacts; starts at `decompose`; creates or adopts an implementation convoy; and hands off to `build-from-convoy-base` without rerunning requirements, plan, or plan-review. | `build-from-decompose-base.formula.toml`; `../tests/test_formula_assets.py::FormulaAssetTests::test_build_from_decompose_base_is_reusable_suffix_contract`; `../tests/test_formula_assets.py::FormulaAssetTests::test_producer_stages_gate_artifacts_with_bounded_repair` |
| GC-BF-028 | `build-from-plan-base` | Virtual targetless plan suffix | Validates approved requirements, produces or reuses an implementation plan and plan-review verdict, and hands off to `build-from-decompose-base`. | `build-from-plan-base.formula.toml`; `../tests/test_formula_assets.py::FormulaAssetTests::test_build_continuation_bases_form_nested_suffix_chain`; `../tests/test_formula_assets.py::FormulaAssetTests::test_producer_stages_gate_artifacts_with_bounded_repair` |
| GC-BF-029 | `build-from-requirements-base` | Virtual targetless requirements suffix | Produces or reuses requirements, records the requirements artifact, and hands off to `build-from-plan-base`. | `build-from-requirements-base.formula.toml`; `../tests/test_formula_assets.py::FormulaAssetTests::test_build_continuation_bases_form_nested_suffix_chain`; `../tests/test_formula_assets.py::FormulaAssetTests::test_producer_stages_gate_artifacts_with_bounded_repair` |
| GC-BF-030 | `build-from-review` | Cataloged default review continuation | Extends `build-from-review-base` with the built-in Gas City methodology defaults. | `build-from-review.formula.toml`; `../tests/test_formula_assets.py::FormulaAssetTests::test_default_continuation_entrypoints_extend_suffix_bases` |
| GC-BF-031 | `build-from-convoy` | Cataloged default convoy continuation | Extends `build-from-convoy-base` with the built-in Gas City methodology defaults. | `build-from-convoy.formula.toml`; `../tests/test_formula_assets.py::FormulaAssetTests::test_default_continuation_entrypoints_extend_suffix_bases` |
| GC-BF-032 | `build-from-decompose` | Cataloged default decompose continuation | Extends `build-from-decompose-base` with the built-in Gas City methodology defaults. | `build-from-decompose.formula.toml`; `../tests/test_formula_assets.py::FormulaAssetTests::test_build_from_decompose_is_suffix_continuation_entrypoint` |
| GC-BF-033 | `build-from-plan` | Cataloged default plan continuation | Extends `build-from-plan-base` with the built-in Gas City methodology defaults. | `build-from-plan.formula.toml`; `../tests/test_formula_assets.py::FormulaAssetTests::test_default_continuation_entrypoints_extend_suffix_bases` |
| GC-BF-034 | `build-from-requirements` | Cataloged default requirements continuation | Extends `build-from-requirements-base` with the built-in Gas City methodology defaults. | `build-from-requirements.formula.toml`; `../tests/test_formula_assets.py::FormulaAssetTests::test_default_continuation_entrypoints_extend_suffix_bases` |

### Implementation Utilities

| ID | Formula | Type | Required behavior | Evidence |
| --- | --- | --- | --- | --- |
| GC-BF-010 | `implement` | Cataloged targeted implementation entrypoint | Validates the input convoy, drains implementation work using an allowed policy, waits for completion, writes validated item-mapped implementation summary evidence, and optionally delegates publishing. | `implement.formula.toml`; `../tests/test_formula_assets.py` |
| GC-BF-011 | `do-work` | Targeted implementation item helper | Extends `implementation-base`, prepares one item worktree, implements owned work with the selected implementation target, and closes the source anchor after implementation succeeds. | `do-work.formula.toml`; `../tests/test_formula_assets.py` |
| GC-BF-012 | `do-work-item` | Targeted shared-drain item helper | Extends `implementation-item-base`, prepares or reuses the one deterministic shared drain worktree and persists `work_dir` on the item's source anchor before running exactly one shared-drain item with the selected implementation target, and stays internal/single-lane. Any formula that redeclares `implement-item` must restate `needs = ["prepare-shared-worktree"]`, because a child step override replaces the inherited step wholesale and otherwise drops the edge. | `do-work-item.formula.toml`; `../assets/scripts/checks/prepare-shared-worktree.sh`; `../tests/test_formula_assets.py`; `../../tests/test_prepare_shared_worktree.py` |
| GC-BF-013 | `same-session-implement` | Targeted internal shared-drain helper | Documents and executes the pack-facing same-session policy by draining through `do-work-item` with exclusive member access and single-lane sequencing. | `same-session-implement.formula.toml`; `../tests/test_formula_assets.py` |

### Review And Fix Utilities

| ID | Formula | Type | Required behavior | Evidence |
| --- | --- | --- | --- | --- |
| GC-BF-014 | `review` | Cataloged targetless report | Extends `code-review-base`, honors `review_mode`, and writes a validated implementation review for a diff, branch note, summary, or artifact path with a base verdict state. | `review.formula.toml`; `../tests/test_formula_assets.py` |
| GC-BF-015 | `gap-analysis` | Cataloged targetless report | Validates context and writes a report comparing implementation artifacts to approved requirements and design, including requirement coverage, upstream hashes, and drift risk. | `gap-analysis.formula.toml`; `../tests/test_formula_assets.py` |
| GC-BF-016 | `fix-convoy` | Targeted internal fix-convoy helper | Synthesizes, validates, and creates fix convoys from failed gap-analysis or review reports. | `fix-convoy.formula.toml`; `../tests/test_formula_assets.py` |

### Design And Publish Utilities

| ID | Formula | Type | Required behavior | Evidence |
| --- | --- | --- | --- | --- |
| GC-BF-017 | `design-review` | Cataloged targeted design review | Reviews and finalizes a design document through a body scope and cleanup finalizer while preserving notification semantics. | `design-review.formula.toml`; `../tests/test_formula_assets.py` |
| GC-BF-018 | `publish` | Targetless internal publication helper | Runs publish preflight, no-ops when push/PR authorization is absent, pushes only with positive authorization, and opens a PR only with positive authorization after push behavior is resolved. | `publish.formula.toml`; `../tests/test_formula_assets.py` |

### GitHub Adapters

| ID | Formula | Type | Required behavior | Evidence |
| --- | --- | --- | --- | --- |
| GC-BF-019 | `github-issue-triage-base` | Targetless base adapter | Accepts only canonical GitHub issue URLs, snapshots issue state, keys triage by issue-body hash, validates triage report schema, gates sensitive output, and creates or updates the sticky triage comment. | `github-issue-triage-base.formula.toml`; `../tests/test_formula_assets.py` |
| GC-BF-020 | `github-issue-triage` | Cataloged public adapter | Extends `github-issue-triage-base` without changing the base adapter stack. | `github-issue-triage.formula.toml`; `../tests/test_formula_assets.py` |
| GC-BF-021 | `github-issue-fix-base` | Targetless base adapter | Accepts only canonical GitHub issue URLs, runs/reuses triage, gates fix eligibility, passes and validates planning/decomposition/implementation/review/fix selectors plus modes, optionally publishes PRs, and owns sticky issue-fix status. | `github-issue-fix-base.formula.toml`; `../tests/test_formula_assets.py` |
| GC-BF-022 | `github-issue-fix` | Cataloged public adapter | Extends `github-issue-fix-base` without changing the base issue-fix graph. | `github-issue-fix.formula.toml`; `../tests/test_formula_assets.py` |
| GC-BF-023 | `github-issue-fix-design-review-work` | Expansion design-review helper | Reviews issue-fix implementation plans through implementation-realism and testing-risk lanes, synthesizes findings, applies required changes, and finalizes only after approval. | `github-issue-fix-design-review-work.formula.toml`; `../tests/test_formula_assets.py` |
| GC-BF-024 | `github-pr-review` | Cataloged targetless PR adapter | Accepts only canonical GitHub PR URLs, snapshots PR state, reuses current head-SHA review when possible, validates and delegates to `code_review_formula`, honors review/interaction mode pass-through where applicable, gates posting, updates a sticky normal comment, and never mutates code or submits formal review events. | `github-pr-review.formula.toml`; `../tests/test_formula_assets.py` |

## Evidence Index

- `python3 -m pytest gascity/tests/test_formula_assets.py -q`
- `gascity/tests/test_formula_assets.py::FormulaAssetTests::test_base_formula_requirements_cover_formula_set`
- `gascity/tests/test_formula_assets.py::FormulaAssetTests::test_methodology_stage_contracts_are_virtual_and_shadowable`
- `gascity/tests/test_formula_assets.py::FormulaAssetTests::test_build_continuation_bases_form_nested_suffix_chain`
- `gascity/tests/test_formula_assets.py::FormulaAssetTests::test_default_continuation_entrypoints_extend_suffix_bases`
- `gascity/tests/test_formula_assets.py::FormulaAssetTests::test_build_from_decompose_is_suffix_continuation_entrypoint`
- `gascity/tests/test_formula_assets.py::FormulaAssetTests::test_build_from_decompose_base_is_reusable_suffix_contract`
- `gascity/tests/test_formula_assets.py::FormulaAssetTests::test_build_basic_v2_uses_approachable_factory_techniques`

## Maintenance Rules

- Add or update exactly one `GC-BF-*` row when a formula is added, removed, or
  behaviorally changed.
- Keep rows phrased as behavior contracts: graph shape, launch shape, artifacts,
  side effects, stage selectors, drain semantics, and extension expectations.
- Do not let external toolkit-specific behavior into this base ledger unless it
  becomes a shared requirement for all methodology implementations.
