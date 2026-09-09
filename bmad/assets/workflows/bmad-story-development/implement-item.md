Run the BMAD story-development loop for this shared-drain item.

The whole loop runs in the drain's shared worktree, not the launcher checkout.
Read `work_dir` from the source anchor — it must be an absolute existing
directory that is not the launcher checkout — then `cd` there and confirm
`pwd -P` matches before any child lane reads, edits, tests, or commits story
code. If `work_dir` is missing or does not resolve, fail this stage instead of
running the loop where it lands.

The child lanes replace BMAD quick-dev's native sub-agent/task handoff:
implement story, self-check, acceptance audit, and apply findings. Use the
implementation-review approval check to decide whether another loop iteration
is needed.

Do not invoke provider-native subagents. Re-run or continue only through this
Gas City graph stage's child steps.
