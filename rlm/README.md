# RLM Pack

Recursive Language Model sidecar for Gas City.

This pack adds an optional long-context helper without changing the city's
primary agent provider. Agents keep using Claude/Codex/Gemini for normal work
and can call `gc rlm ask` when a task needs repo-wide or log-wide synthesis.
Phase 1 supports OpenAI-compatible backends only.

## What it provides

- `gc rlm install` to create a pack-owned runtime under `.gc/rlm/`
- `gc rlm ask` for bounded long-context analysis over files, globs, or stdin
- `gc rlm status` to inspect policy, runtime health, and recent runs
- `gc rlm uninstall` to clear broken state or remove the runtime
- `gc doctor` checks for Python, runtime health, and Docker when sandboxed execution is configured
- `rlm-usage` prompt fragment for agent guidance

## Include It

```toml
[packs.rlm]
source = "https://github.com/julianknutsen/packs.git"
ref = "main"
path = "rlm"

[workspace]
includes = ["rlm"]
global_fragments = ["rlm-usage"]
```

You can also inject the fragment only into selected rig agents:

```toml
[[rigs.overrides]]
agent = "worker"
inject_fragments_append = ["rlm-usage"]
```

## Install

Sandboxed Docker mode with a remote OpenAI backend:

```bash
gc rlm install \
  --backend openai \
  --model gpt-5-mini \
  --environment docker \
  --allow-remote-backend
```

Trusted local mode against a local OpenAI-compatible endpoint:

```bash
gc rlm install \
  --backend openai \
  --model local-model \
  --base-url http://127.0.0.1:8000/v1 \
  --backend-api-key-env '' \
  --environment local
```

The install command creates `.gc/rlm/` with a Python virtualenv, config,
logs, cache, and a Docker image for sandboxed execution when Docker mode is
enabled.

## Use It

```bash
gc rlm ask --path . --prompt "Map the major config loading and pack expansion paths."
gc rlm ask --glob 'docs/**/*.md' --prompt "What pack constraints are documented here?"
gc rlm ask --path /var/log/my-service --prompt "Find the first error pattern that explains the crash loop."
```

JSON output is available for agent-side post-processing:

```bash
gc rlm ask --path . --prompt "Summarize the pack surface." --output json
```

## Notes

- Docker is the default execution path.
- `local` mode is intentionally explicit and has no hard security boundary.
- Non-loopback remote model access requires `--allow-remote-backend` at install time.
- By default the staged corpus respects `.gitignore`, skips binary files, and
  deny-lists common secret-like patterns such as `.env*` and `*.pem`.
