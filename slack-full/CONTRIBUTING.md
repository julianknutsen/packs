# Contributing to slack-pack

slack-pack is a Slack provider extension for Gas City. When this directory is
mirrored to `gastownhall/gascity-packs/slack/`, the root repo's contributing
guide is the source of truth — start there, then read the slack-pack-specific
notes below.

For Gas City's own contributor workflow (build, hooks, docs), see the in-tree
guide at [../../CONTRIBUTING.md](../../CONTRIBUTING.md).

## Build flow

Three pieces ship with this pack:

- **Pack scripts** live in `scripts/` and are pure Python. They run via the
  `gc slack <command>` shims under `commands/` and have no compile step.
- **Adapter** (the Slack-side HTTP/UDS bridge) is the Go binary whose source
  lives at `adapter/main.go` (colocated with the pack). It is its own Go
  module so it can travel intact when the pack is mirrored upstream.
- **Operator CLI** (`gc-slack-cli`) is the second Go binary that backs the
  `gc slack <cmd>` verb surface (import-app, map-channel, map-rig,
  post-message, sync-commands, enable-room-launch). Source lives at
  `cli/main.go` + `cli/cmd/`; like the adapter it is its own Go module so
  it can travel intact upstream.

Build the adapter with:

```bash
cd examples/slack-pack/adapter
go build -o gc-slack-adapter
```

In deployed packs you normally don't build by hand: the `[[service]]`
command is `adapter/run.sh`, which rebuilds `gc-slack-adapter` from
source whenever the binary is missing (build artifacts are gitignored,
so `gc import install` re-materializes the pack cache without them —
run.sh self-heals instead of stranding the service).

Build the operator CLI with:

```bash
cd examples/slack-pack/cli
go build -o gc-slack-cli .
```

The pack's `commands/<cmd>.sh` wrappers exec `$GC_PACK_DIR/cli/gc-slack-cli`,
so the CLI binary must live at that path — i.e. inside the installed pack's
`cli/` subdirectory — when operators invoke `gc slack <cmd>`.

## Test flow

Run pack tests (pytest, no external deps beyond `pytest` itself):

```bash
pytest examples/slack-pack/tests/
```

Run adapter tests:

```bash
cd examples/slack-pack/adapter
go test -race ./...
```

Run CLI tests:

```bash
cd examples/slack-pack/cli
go test -race ./...
```

CI runs all three on every PR that touches `examples/slack-pack/**` (see
`.github/workflows/slack-pack.yml`).

## Secret handling

slack-pack reads Slack credentials from environment variables only. Never
commit `.env` files or tokens. The README's "Adapter env contract" section
documents the full env-var contract; use a `.env` file outside the repo or a
secret manager and source it before running adapter / scripts.

## Pull requests

- Keep PRs scoped to slack-pack (or paired adapter changes when needed).
- Update `CHANGELOG.md` for any user-visible change — add bullets under a
  new `[Unreleased]` section, and the next release tag promotes them.
- Run `pytest`, `go test -race ./...` in `adapter/`, and
  `go test -race ./...` in `cli/` locally before opening the PR.
