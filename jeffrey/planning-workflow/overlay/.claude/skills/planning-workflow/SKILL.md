---
name: planning-workflow
description: >-
  Multi-phase planning process: understand, decompose, design, validate,
  then implement. No code until the plan is approved.
---

# Planning Workflow

Spend 85% or more of your effort on planning before writing any code.

## Process

Follow these phases in order. Do not skip ahead.

### Phase 1: Understand

Read all relevant documentation, code, and context. Identify the problem
space, constraints, and requirements. List what you know and what you
need to clarify. Ask questions if anything is ambiguous.

### Phase 2: Decompose

Break the task into ordered steps. Each step should be small enough to
verify independently. Identify dependencies between steps. Flag any
steps that carry risk or uncertainty.

### Phase 3: Design

For each step, define the interface, data structures, and contracts.
Specify inputs, outputs, and error conditions. Consider edge cases.
Write pseudo-code or signatures, not implementations.

### Phase 4: Validate

Review the plan against the original requirements. Check for gaps,
contradictions, and missed edge cases. Verify the step ordering makes
sense. Confirm the design handles error paths.

### Phase 5: Implement

Only after the plan is reviewed and approved, write code. Follow the
plan step by step. Test each step before moving to the next.

## Rules

- No code before Phase 5.
- If you discover a gap during implementation, stop and update the plan.
- Every step must have a clear definition of "done."

---

*Based on Jeffrey Emanuel's planning workflow (@doodlestein)*
