Use the built-in Gas City `create-beads` decomposition flow.

Create task beads or a decomposition artifact from the approved requirements
and implementation plan. Preserve traceability from each work item back to the
relevant acceptance criteria and plan section, and record the implementation convoy
that the `implement` formula will drain.

Create the decomposition artifact at the path recorded on the workflow root
bead as `gc.build.decomposition_path` (fallback
`gc.var.decomposition_path`). The artifact must be Markdown with YAML front
matter, not JSON. Its front matter must declare
`schema: gc.build.decomposition.v1`, the workflow id/formula, the methodology
pack/name, the producer formula/stage/attempt, `status`, and `trace` with
upstream and coverage entries. Include a Markdown coverage table whose
ID/status pairs exactly match `trace.coverage`.
The validator only recognizes a Markdown table with an `ID` column and a
`Status` column. Use this shape:

| ID | Status |
| --- | --- |
| REQ-001 | covered |

Use mapping objects for front matter; do not use scalar shortcuts such as
`workflow: build-basic`. The top-level YAML shape must be:

- `schema: gc.build.decomposition.v1`
- `workflow: {id: <workflow-root-id>, formula: build-basic}`
- `methodology: {pack: gascity, name: build-basic}`
- `producer: {formula: build-basic, stage: decompose, attempt: <positive integer>}`
- `status: approved` or another schema-allowed status
- `trace: {upstream: [...], coverage: [...]}`

Trace front matter must use the validator shape exactly:

- `trace.upstream[]` entries must include `path` and `hash`; do not use
  `id`/`title`/`type` entries as the upstream shape.
- For the requirements and plan artifacts, use their recorded paths and
  scheme-qualified hashes such as `sha256:<digest>` or `git:<revision>`.
- If an upstream entry lists `ids`, every listed id must appear exactly once in
  `trace.coverage` and in the Markdown coverage table with the same status.
- Coverage statuses are not artifact statuses. Use `covered` for satisfied
  requirements; do not use `approved` in `trace.coverage[].status` or the
  Markdown coverage table.

Include the required schema sections:

- Summary
- Selected Downstream Formulas
- Implementation Convoy
- Work Items

The Work Items section must include a machine-readable dependency table with
ID, Bead, and Depends On columns, one row per work item, rows in intended
execution order:

| ID | Bead | Depends On |
| --- | --- | --- |
| WI-1 | <WI-1-bead-id> | - |
| WI-2 | <WI-2-bead-id> | WI-1 |

- Every Depends On entry must reference an EARLIER row's ID; use commas for
  several prerequisites and `-` for none.
- Do not add a Status column to this table; the validator reads any table with
  ID and Status columns as the coverage matrix.
- The validation gate verifies live bead edges against this table: each row's
  bead must depend on the beads of its Depends On entries, and no earlier
  row's bead may depend on a later row's bead. Orientation failures name the
  exact `gc bd dep add` / `gc bd dep remove` repair commands; repair the
  EDGES with those commands, not just the artifact text.

Create work-item beads first, then create a new implementation convoy for those
work units. Do not reuse the source or launch convoy from `gc.var.convoy_id`.

Use the convoy creation flow exactly:

1. Create each work item with `gc bd create ...` and capture the returned work-item
   bead IDs. Do not use `gc bd create --root-bead`; `--root-bead` is not a
   create flag. Attach the workflow root relationship with metadata such as
   `gc.root_bead_id=<workflow-root-id>`.
2. Create and link the implementation convoy in one command:
   `gc convoy create <name> <work-item-id...> --json`.
3. Parse `<implementation-convoy-id>` from that JSON response, then verify the
   convoy with `gc convoy list --json`.

Do not create an empty convoy. Do not call `gc convoy add` for newly-created beads.
The freshly-created IDs may not be visible to that path yet. Do not call `gc bd show <implementation-convoy-id>`.
Convoy IDs are not bd issue IDs.

Wire work-item dependencies immediately after creating the convoy, exactly as
declared in the dependency table:

- The command is `gc bd dep add <dependent> <prerequisite>`: the FIRST
  argument is the work item that must WAIT, the SECOND is what it waits for.
  Read it as "<dependent> depends on <prerequisite>".
- For the declared order WI-1 then WI-2 (WI-2 depends on WI-1):
  - RIGHT: `gc bd dep add <WI-2-bead> <WI-1-bead>` (WI-2 needs WI-1)
  - WRONG: `gc bd dep add <WI-1-bead> <WI-2-bead>` — that is the temporal
    misreading ("WI-1 comes before WI-2"); it makes WI-1 wait on WI-2, and a
    chain wired this way drains back-to-front.
- For a strict sequential chain, run one command per consecutive pair, always
  naming the LATER work item first.
- Self-check before closing: the first work item must show no blocking
  dependencies in `gc bd dep list <WI-1-bead> --json`, every later work item
  must list exactly its declared prerequisites, and for a strict chain only
  the first of the new work items may be ready.

Record the decomposition output on the workflow root bead with
`gc bd update "<workflow-root-id>" --set-metadata "gc.build.decomposition_path=<absolute path>"`.
Do not use `gc bd update --metadata 'key=value'`; `--metadata` only accepts a JSON
object.

Then set both `gc.input_convoy_id=<implementation-convoy-id>` and
`gc.build.implementation_convoy_id=<implementation-convoy-id>` on the workflow
root bead with a quoted command like:

`gc bd update "<workflow-root-id>" --set-metadata "gc.input_convoy_id=<implementation-convoy-id>" --set-metadata "gc.build.implementation_convoy_id=<implementation-convoy-id>"`

before closing, verify both metadata fields exist on the workflow root and point
to the new implementation convoy.
Before closing this step, set the claimed step outcome with
`gc bd update "<claimed-step-id>" --set-metadata "gc.outcome=pass"`, then close
with `gc bd close "<claimed-step-id>" --reason "<concise reason>"`. Do not pass
`--metadata` or `--set-metadata` to `gc bd close`.

Artifact validation: this stage is gated by `.gc/scripts/checks/build-artifact-valid.sh`, which validates the artifact recorded at `gc.build.decomposition_path` (fallback `gc.var.decomposition_path`) against schema `gc.build.decomposition.v1`. On repair attempts (`gc.attempt` greater than 1), read the validator errors from `gc.attempt_log` on the validation loop control bead (the dependent of this step bead) and repair the artifact in place instead of rewriting it. Two bounded repair attempts follow the first failure; exhausting them closes this stage with `gc.outcome=fail` and machine-readable validation errors that block downstream stages. Never ask questions in headless mode; record unresolved ambiguity inside the artifact.
