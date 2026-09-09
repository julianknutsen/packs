# Citadel Codex gates (gp-mj2q)

Afik approved the isolated profile and gate rollout on 2026-09-06
(message 1788737744.207359): “we should always use codex with astra”.
Use the city profile with `gpt-6-astra` and `xhigh` reasoning. Existing default
profile settings are preserved; the default model remains `gpt-5.6-sol`.

## Profile compatibility and measurements

Codex CLI 0.153.3 requires `~/.codex/city.config.toml` for `-p city`.
It rejects a same-name legacy `[profiles.city]` table in `config.toml`.
The requested legacy table was tested and removed; the base file is
byte-identical to its backup, `~/.codex/config.toml.bak-20260906-185000`.
See [OpenAI profile documentation](https://learn.chatgpt.com/docs/config-file/config-advanced#profiles)
and the installed CLI's `codex --help`.

The city overlay sets `model = "gpt-6-astra"` and
`model_reasoning_effort = "xhigh"`. It disables `features.plugins`,
`features.apps`, `features.recommended_plugins`, and `features.remote_plugin`,
and sets `enabled = false` for all eleven configured plugins and both configured
MCP servers (`node_repl` and `computer-use`). The system and user skills remain
available. Future explicitly configured MCP servers also need a disabled entry
in this overlay.

Measured in `/tmp` on 2026-09-06, before shortening ego-browser's description:

| Fixed prompt: `reply OK` | Default | City |
| --- | ---: | ---: |
| Preamble characters | 19,176 | 11,966 |
| Skills block characters | 7,210 | 3,554 |
| Plugin catalogue characters | 3,387 | 0 |
| Input tokens | 15,679 | 13,125 |
| Cached input tokens | 11,136 | 11,136 |

Both runs returned `OK`. Preamble size is the sum of model-visible text lengths
from `codex [-p city] -C /tmp debug prompt-input 'reply OK'`, excluding the last
user prompt, including the environment message. It excludes hidden model base
instructions and tool schemas. Tokens are the reported `turn.completed.usage`
from `codex [-p city] exec --skip-git-repo-check -C /tmp --json 'reply OK'` with
stdin redirected from `/dev/null`. The profile comparison also changes the model;
it is an observed before/after result, not a tokenizer-controlled benchmark.

Raw local evidence is in `~/city/assets/ops/gp-mj2q/`. `codex -p city mcp list`
confirmed both servers disabled. The rendered city prompt has no recommended
plugin catalogue or plugin skill entries.

## Gate helper

Install `gascity/assets/scripts/codex-gate.sh` at
`~/city/assets/ops/mayor-tools/codex-gate.sh`. Provision the city profile first.

```bash
codex-gate.sh review --base origin/main -C "$REPO" --output "$REVIEW_OUTPUT"
codex-gate.sh exec "$PROMPT_FILE" -C "$REPO" --output "$REVIEW_OUTPUT"
```

Both modes use `-p city -m gpt-6-astra`, read-only execution, `-C`,
`--skip-git-repo-check`, and `/dev/null` stdin. `--model MODEL` explicitly
overrides both the normal model and the optional review model. Review mode
resolves REF and HEAD to commit IDs, computes their merge base, and asks
`codex exec` for a QUICK review of that immutable delta. Native `codex review`
formats its own findings and did not honor the requested VERDICT line in a live
test, so both helper modes use exec's separate final-answer file.
The caller's output file receives that answer; `FILE.log` holds the transcript.
The helper accepts exactly one final standalone `VERDICT: CLEAN` or
`VERDICT: BLOCK` line. Exit codes are 0 for CLEAN, 1 for BLOCK, 2 for invalid
verdict/usage; Codex invocation failures retain their exit status and leave the
answer file empty. A stale CLEAN answer is cleared before each invocation.
Review mode rejects empty committed deltas and worktrees with staged, modified,
or untracked files before invoking Codex. Commit changes before review and keep
prompt/output files outside the reviewed worktree.

Codex QUICK reviews supplement the required Fable adversarial review for
Codex-built changes. Publishing a PR does not authorize merging it.

## Local ego-browser override and provider handoff

The ego-browser description is now `Use when a task needs a real browser session
on citadel`: 977 → 55 characters. `~/.agents/skills/ego-browser` is a local
directory with the edited SKILL.md and links to the app's other resources.
The body and ego app's source file are unchanged. The original directory symlink
was saved at `~/.agents/ego-browser.bak-20260906-185347`, outside skill discovery.
Claude-side skills, including superpowers, were not modified.

The mayor owns `city.toml`. Its `codex-astra` provider already pins the model;
its command and resume command still need `-p city` to select the new profile.
The plain `codex` provider also needs the explicit Astra model and city profile
if it is used for city work. No city.toml change is included in this task.
