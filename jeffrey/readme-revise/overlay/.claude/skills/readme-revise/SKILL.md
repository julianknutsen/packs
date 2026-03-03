---
name: readme-revise
description: >-
  Evaluate and polish README and documentation. Ensure accuracy,
  structure, runnable examples, then de-slopify.
---

# README Reviser

Update the README and other documentation to reflect all recent changes
to the project.

## Evaluation Checklist

Before making changes, evaluate the current state:

- [ ] **One-sentence purpose:** Can a reader understand what this project
  does in one sentence?
- [ ] **Install instructions:** Are they tested and current?
- [ ] **Examples:** Are code examples runnable with the current version?
- [ ] **Architecture:** For complex projects, is there a high-level
  overview?
- [ ] **Contributing:** Are contribution guidelines present?
- [ ] **API reference:** Are public APIs documented?
- [ ] **Changelog:** Are recent changes noted?

## Rules

- Frame all updates as if they were always present. Do not say "we added
  X" or "X is now Y." Describe the current state as it is.
- Add any new commands, options, or features.
- Remove documentation for features that no longer exist.
- Ensure all examples actually work.

## Polish

After updating content, apply de-slopify rules:

- Remove em dashes; use semicolons, commas, or recast sentences.
- Remove filler openers and hedging language.
- Use direct, concise prose.
- No "Here's why" or "Let's dive in" patterns.

## Iteration

Review the result, identify the single biggest improvement remaining,
apply it, and repeat until the documentation is clean, accurate, and
professional.

---

*Based on Jeffrey Emanuel's README Reviser (@doodlestein)*
