# slack-full (v0.0.1 preview — scaffold)

A Slack provider extension for Gas City. Modeled directly on the
upstream `discord` pack
(https://github.com/gastownhall/gascity-packs/tree/main/discord) so
the same primitives can be ported one at a time.

This pack lives at `slack-full/` in the
[`gastownhall/gascity-packs`](https://github.com/gastownhall/gascity-packs)
catalog. A city opts in by path-importing its `pack.toml` (see
"Install" below).

## Tiering

`slack-full` is **Tier 3** of the Slack pack family — workspace-grade
orchestration. It is the original, full-surface pack; the two smaller
tiers were later extracted from it for cities that need less.

- **[slack-mini](../slack-mini)** (Tier 1) — single-file adapter, one
  outbound verb (`gc slack-mini post-message`), `app_mention` → mayor.
  No channel bindings, no state. Reach for it when you only need the
  mayor to speak into one channel.
- **[slack-channel](../slack-channel)** (Tier 2) — the "team channel ↔
  session graph" middle tier: channel bindings, per-session identities,
  handle aliases, reactions, threaded replies. No multi-rig routing,
  slash commands, peer fanout, or launcher mode.
- **slack-full** (Tier 3, this pack) — workspace-grade orchestration:
  multi-rig routing, slash commands + interactions, peer fanout,
  launcher mode, bidirectional file attachments, and per-session
  identity overrides.

### Choosing a tier

- Need the mayor to post status into a single channel and nothing
  else? → **slack-mini**.
- Need a handful of named sessions sharing one or more channels with
  distinct identities, but no slash commands or cross-rig routing? →
  **slack-channel**.
- Need slash commands, interactive modals/buttons, peer fanout across a
  room, launcher-mode session spawning, or multi-rig channel routing? →
  **slack-full** (this pack).

See the [Slack pack tiering design memo](../docs/design/slack-pack-tiering.md)
for the full rationale.

> **Scope: not yet at parity with the discord pack.** The discord pack
> ships ~350K LOC of provider-agnostic Python state-machine logic.
> Slack pack is a feature-by-feature port; today's surface is enough
> to run a multi-session oversight loop end-to-end (DMs + rooms +
> peer fanout + identity overrides + bidirectional file
> attachments), but several discord-pack features are still missing
> (see "Not yet implemented" below). If you need parity today, use
> the discord pack.

## Status

Implemented:

- [x] `gc slack bind-dm` — bind a Slack DM channel to one named session
- [x] `gc slack bind-room` — bind a room to multiple sessions; flags
      `--enable-peer-fanout`, `--allow-untargeted-publication`,
      `--max-peer-triggered-publishes`, `--max-total-peer-deliveries`,
      `--default-handle`, `--handle HANDLE=SESSION` (creates a
      launcher-mode group + participants under the hood)
- [x] `gc slack reply-current` — reply to the latest Slack event in the
      current session, by default through gc's `/extmsg/outbound` so
      transcript recording + peer fanout fire (`--via adapter` keeps the
      old direct-to-adapter path for diagnostics)
- [x] `gc slack publish` — publish to a session's saved binding (target
      session required, no event-scan fallback — fail-fast when the
      session has no active binding)
- [x] `gc slack publish-to-channel` — publish to an arbitrary channel
      ID (no session binding required; useful for one-shot ops posts)
- [x] `gc slack status` — read-only diagnostics (adapters, bindings,
      recent traffic). `--session SID` for one-session detail,
      `--since 5m` for a time window, `--json` for scripting.
- [x] `gc slack react` — add an emoji reaction to a Slack message
- [x] `gc slack identity` — register/unregister a per-session
      `chat:write.customize` identity (display name + icon) so each
      bound session posts under its own persona
- [x] `gc slack handle-alias` — register/unregister a cross-channel
      `@handle` → session-id alias used by the address-by-handle
      protocol
- [x] `gc slack upload` — bidirectional file attachments
      (`/publish-file` outbound + auto-download of inbound files into
      `$INBOUND_FILE_STORE/<channel>/<ts>-<filename>`, scrubbed by an
      in-process retention janitor)
- [x] `template-fragments/slack-v0.template.md` — composable prompt
      fragment for any agent in a slack-bound session
- [x] Pack-owned intake service (`[[service]]` proxy_process). Phase A:
      adapter is the same Go binary, but gc supervises it via UDS for
      `/publish` while the public Slack webhook still terminates at
      adapter TCP `:8775` (Funnel unchanged). See "Adapter as a
      proxy_process service" below for the cutover.

Slash-command / launcher / projection surface (operator CLI verbs):

- [x] `gc slack import-app` — register a Slack app manifest with the gc
      city ([`manifest/README.md`](./manifest/README.md#importing-into-gc-gc-slack-import-app))
- [x] `gc slack map-channel` — bind a Slack channel to a session;
      backs the adapter's `/slack/interactions` slash-command
      dispatcher. (Note: the legacy `--rig` flag on this verb is
      deprecated as of gc-cby.25; use `gc slack map-rig` for
      rig→channel bindings. The flag still works for back-compat
      and emits a stderr deprecation warning.)
- [x] `gc slack map-rig` — bind a rig to a set of channels as the
      fall-through default for slash-command resolution. Per-channel
      `map-channel --session` bindings override rig defaults; channel
      mapping wins. Cross-store conflict detection refuses contradictory
      writes in either direction. `--remove` drops the entire rig
      record; `--remove-channels c1,c2` drops just the listed channels
      (the record itself is deleted if the set becomes empty). Both
      removal paths are idempotent.
- [x] `gc slack sync-commands` — register/refresh the city's slash
      commands against the Slack app (read by the adapter's
      `/slack/interactions` dispatcher).
- [x] `gc slack sync-subteam-aliases` — reconcile
      `<city>/.gc/slack/subteam-aliases.json` (the Slack User Group
      ("subteam") id → gc handle map) with the workspace's live User
      Groups via Slack `usergroups.list`. Like `sync-commands`, it diffs
      live against on-disk and supports `--dry-run` and
      `--output text|json`; unlike it, the write is a non-destructive
      MERGE — existing entries are preserved, handles are refreshed where
      the live name differs, new User Groups are added, and entries with
      no matching live group are kept and reported as "local-only" rather
      than dropped. The bot token (`--token` / `$SLACK_BOT_TOKEN`) must
      carry the `usergroups:read` scope; without it the verb reports a
      readable `missing_scope` error and writes nothing. After a write it
      prints the same SIGHUP reminder as the other registry-writing verbs.
- [x] `gc slack post-message` — post (or `--update <ts>`) a Block Kit
      message to a channel via `chat.postMessage`/`chat.update`. The
      one-shot verb is shipped; automatic workflow-status projection
      from gc events is not yet wired.
- [x] `gc slack retry-peer-fanout` (walks `extmsg.peer_fanout_failed`
      events, dedupes against successful `extmsg.peer_fanout_retried`
      events, re-issues each notification via the gc retry endpoint with
      a small cooldown between attempts)

Not yet implemented (planned):

- [ ] `gc slack enable-room-launch` — the CLI verb writes the
      `(workspace_id, channel_id) → pool_template` mapping today, but the
      adapter-side `@@<handle>` thread-scoped session spawn is still a
      stub: an `@@<handle>` post in an enabled channel currently gets an
      ephemeral "launcher not yet available" reply rather than spawning a
      session (tracked under the gc-cby epic).

The adapter exposes `POST /slack/interactions` (HMAC-verified) for
slash-command, `block_actions`, and `view_submission` dispatch.

`block_actions` (button clicks, select-menu finalizations, datepickers,
etc.) routes through the same channel-binding flow as slash commands:
the action's originating channel (`payload.channel.id` falling back to
`payload.container.channel_id`) must be bound via `gc slack
map-channel` (session target) or `gc slack map-rig` (rig target — the
rig branch is recorded but routing is deferred to gc-cby.18).

`view_submission` (modal submits) carries no channel context, so the
modal opener MUST set `view.private_metadata` to the JSON string
`{"session_id":"<gc-session-id>"}` when calling `views.open`/`views.push`.
On submit, the adapter strict-decodes that field (extra keys rejected,
session_id length-capped) and posts a system-reminder to that session
describing the `callback_id` plus `view.state.values`. Any decode
failure responds `{"response_action":"clear"}` so Slack closes the
modal stack and the user knows the submit did not process.

The adapter's workspace gate (`SLACK_WORKSPACE_ID` / `cfg.accountID`)
applies on both branches — `payload.team.id` must match. `response_url`
(valid ~30 minutes / 5 uses on `block_actions`) is forwarded to the
agent in the system-reminder; persistent storage of `response_url` for
later use is out of scope.

Other Slack interaction types (`shortcut`, `message_action`,
`view_closed`, `block_suggestion`) return an ephemeral "unsupported
interaction type" reply — they need separate routing logic and are
tracked under the gc-cby epic.

## Binaries

This pack ships **two** Go binaries, each with its own `go.mod` so
either can travel intact when the pack is mirrored upstream:

- **Adapter** — `slack-full/adapter/gc-slack-adapter`,
  built from `adapter/main.go`. The public-facing webhook receiver
  (Slack → `:8775` over Tailscale Funnel) and outbound publisher
  (`/publish` over the controller-managed UDS). Long-running; gc
  supervises it via the `[[service]]` block in `pack.toml`.

- **Operator CLI** — `slack-full/cli/gc-slack-cli`, built
  from `cli/main.go` + `cli/cmd/`. Backs the `gc slack <cmd>` verb
  surface (import-app, map-channel, map-rig, sync-commands,
  sync-subteam-aliases, enable-room-launch, post-message). One-shot per invocation; not
  supervised. The pack's `commands/<cmd>.sh` wrappers exec it at
  `$GC_PACK_DIR/cli/gc-slack-cli` so operator command-line ergonomics
  stay identical to the pre-relocation gc-binary verbs.

Build commands and CI for both binaries are documented in
[CONTRIBUTING.md](./CONTRIBUTING.md#build-flow).

## Architecture (current)

The Go adapter at `slack-full/adapter/gc-slack-adapter`
(built from `adapter/main.go` colocated with this pack) is the
public-facing webhook receiver and outbound publisher. This
pack adds CLI surface around it: `bind-dm` writes to gc's
`/extmsg/bind` and to the pack's local config; `reply-current` reads
recent gc events to find the conversation, then POSTs to gc's
`/extmsg/outbound` (which calls the registered HTTP adapter's
`/publish` endpoint internally and emits `ExtMsgOutbound` so peer
fanout fires for bind-room sessions). `--via adapter` is available
for adapter-only diagnostics that bypass gc.

```
                   ┌──── public ────┐
Slack  ──HMAC──▶  Go adapter :8775  ──▶ gc /extmsg/inbound
                  Go adapter :8766  ◀── gc /extmsg/outbound  ◀── gc slack reply-current
                                    ◀── (--via adapter) ─────── gc slack reply-current
                   └────────────────┘
```

The `:8775` public port above is the reference Funnel-fronted
deployment (`LISTEN_PUBLIC=:8775`, with a Tailscale Funnel rule
`:443 → :8775`). The bare adapter default is `:8765` — see the
`LISTEN_PUBLIC` row in "Adapter env contract" below.

The adapter's `/publish` endpoint **requires session attribution**: every
request must carry a `session_id` (or, for older gc binaries, the legacy
`metadata.source_session_id`). A request with neither is rejected with HTTP
400 and nothing is posted to Slack:

```json
{"error": "publish requires session attribution: provide session_id or metadata.source_session_id"}
```

This fails closed against identity-less channel-root posts under the default
bot identity. System/bot status notifications that intentionally post as the bot
go through `gc slack post-message` (direct to Slack `chat.postMessage`),
which bypasses `/publish` entirely and is unaffected by this guard.

## Install

```toml
# city.toml
[imports.slack-full]
source = "/path/to/gascity-packs/slack-full"
```

Then `gc reload` (or wait for the supervisor to pick up the change).
Verify the commands appear:

```
gc slack --help
gc slack bind-dm --help
gc slack reply-current --help
```

## Verify

Assuming the adapter is up and the four must-set vars
(`SLACK_WORKSPACE_ID`, `SLACK_BOT_TOKEN`, `SLACK_SIGNING_SECRET`,
`GC_CITY_NAME`) are exported (sourced from
`${XDG_CONFIG_HOME:-$HOME/.config}/gc-slack-adapter/env` — see
"Adapter env contract" below):

```
# Bind a DM channel to a session. Replace D0XXXXXXXXX with the DM
# channel ID Slack assigned to your bot's IM with the human user.
gc slack bind-dm D0XXXXXXXXX my-session.cos

# From inside that session (or any session that has seen recent
# extmsg.inbound on a slack conversation):
echo "*my-session.cos:* ack via slack pack" > /tmp/reply.txt
gc slack reply-current --body-file /tmp/reply.txt
```

The reply should land in your Slack DM.

To bind a room (public or private channel) to multiple sessions so
that two or more agents are visible peers and a human can join the
conversation:

```
gc slack bind-room C0XXXXXXXXX \
    my-pack.session-a my-pack.session-b \
    --enable-peer-fanout \
    --binding-owner my-pack.session-b
```

Both sessions then receive an inbound system reminder for every human
message in the channel; `extmsg.inbound` events list both as
conversation members. When the publishing session calls
`gc slack reply-current` (default `--via gc`), gc records the publish
in the conversation transcript and fans out a peer-publication
reminder to the other bound sessions so they see what their peer just
said.

`--binding-owner SESSION` is what makes outbound publishes (and
therefore `gc slack reply-current --via gc`) actually work. Without
it, peer fanout still fires on inbound, but `/extmsg/outbound` has
no `SessionBindingRecord` to resolve the conversation through and the
publish is rejected. The owner must be one of the participants —
prefer the session that "owns" the room from gc's perspective. Pass
the gc session id (e.g. `gc-77139`) when alias resolution semantics
matter; for stable named sessions, the alias works too.

## Adapter as a proxy_process service

Phase A of the in-pack adapter (tracked as bd `gc-5rz`) lets gc
supervise the adapter as part of the city's services. The adapter
binds a Unix domain socket for the `/publish` endpoint that gc reaches
via the extmsg HTTP adapter, and gc reverse-proxies `/svc/slack/*` to
that UDS. The public Slack webhook (`/slack/events`) still terminates
at the adapter's TCP `:8775` so Tailscale Funnel and Slack's signing
secret verification are unchanged. The same binary runs in both modes
— the legacy `nohup ./run.sh` deployment is preserved as a rollback
target.

### What gc injects vs. what stays in the env file

`proxy_process` injects the controller-managed env at start time:

- `GC_SERVICE_NAME=slack`
- `GC_SERVICE_SOCKET=/tmp/gcsvc-<uid>/<hash>/slack-*.sock`
- `GC_SERVICE_URL_PREFIX=/svc/slack`
- `GC_SERVICE_STATE_ROOT=.../.gc/services/slack`
- `GC_SERVICE_RUN_ROOT=.../.gc/services/slack/run`

Note: `GC_API_BASE_URL` and `GC_CITY_NAME` are NOT injected by the
controller for `proxy_process` services — they must be present in the
env file you source before `gc start`, so the supervisor inherits them
and passes them down to the spawned adapter.

When `GC_SERVICE_SOCKET` is set, the adapter:
- skips its `LISTEN_INTERNAL` TCP listener and binds the UDS instead;
- computes its self-registration `CallbackURL` as
  `$GC_API_BASE_URL + $GC_SERVICE_URL_PREFIX` (gc's extmsg HTTP adapter
  appends `/publish` itself when calling out, so the registered base
  URL must NOT include `/publish`);
- still binds public TCP for `/slack/events`;
- still serves `/healthz` on the UDS so the controller's `health_path`
  probe succeeds.

### Adapter env contract

The full adapter env contract — what the binary at
`slack-full/adapter/main.go` reads — is enumerated in the
package docstring at the top of that file. Summary:

**Must-set** (no default; adapter exits at startup if missing):

| Var                    | Purpose                                                            |
| ---------------------- | ------------------------------------------------------------------ |
| `SLACK_WORKSPACE_ID`   | Slack team id (e.g. `T0XXXXXXXXX`).                                |
| `SLACK_BOT_TOKEN`      | `xoxb-…` token. Scopes: `chat:write`, `reactions:write`, `files:write`, and `chat:write.customize` for per-session identity overrides. |
| `SLACK_SIGNING_SECRET` | Used to verify HMAC signatures on `/slack/events` requests.        |
| `GC_CITY_NAME`         | gc city the adapter posts inbound + session-message traffic to. Matches `[workspace].name` in `city.toml`. No fallback default — the adapter fails fast rather than silently route to a wrong city. |

**Optional override** (sane default; set to override):

| Var                            | Default                                          | Purpose                                                                         |
| ------------------------------ | ------------------------------------------------ | ------------------------------------------------------------------------------- |
| `LISTEN_PUBLIC`                | `:8765`                                          | Public listener for `/slack/events` (bind `0.0.0.0` if fronted by a tunnel).    |
| `LISTEN_INTERNAL`              | `127.0.0.1:8766`                                 | Loopback listener for `/publish`. Ignored under proxy_process mode.             |
| `INTERNAL_CALLBACK_URL`        | `http://127.0.0.1:8766`                          | URL advertised to gc at self-registration. Ignored under proxy_process mode.    |
| `GC_API_BASE_URL`              | `http://127.0.0.1:9443`                          | Base URL for gc's HTTP API.                                                     |
| `ADAPTER_PROVIDER`             | `slack`                                          | Provider name in conversation refs + adapter registration.                      |
| `REGISTER_ON_START`            | `true`                                           | Set `false` to skip `/extmsg/adapters` registration (tests, diagnostics).       |
| `HANDLE_PREFIX`                | `@`                                              | Leading address token for keyword routing. Empty disables routing.              |
| `IDENTITY_STORE_PATH`          | `/tmp/gc-slack-adapter/identities.json`          | JSON file backing the per-session `chat:write.customize` identity registry.    |
| `HANDLE_ALIAS_STORE_PATH`      | `/tmp/gc-slack-adapter/handle-aliases.json`      | JSON file backing the cross-channel handle → session-id alias registry.        |
| `SLACK_SUBTEAM_ALIAS_FILE`     | `<GC_CITY_PATH>/.gc/slack/subteam-aliases.json` (or `/tmp/gc-slack-adapter/subteam-aliases.json`) | JSON map of Slack User Group ("subteam") IDs → gc handles, hand-edited or written by `gc slack sync-subteam-aliases`. Required to route the unlabeled `<!subteam^Sxxx>` mention shape; the labeled `<!subteam^Sxxx\|@handle>` / `<!subteam^Sxxx\|handle>` shape is gated by `HANDLE_ALIAS_STORE_PATH` instead. Read-only at runtime; SIGHUP or restart to reload. |
| `SLACK_USER_ALIAS_FILE`        | `<GC_CITY_PATH>/.gc/slack/slack-user-aliases.json` (or `/tmp/gc-slack-adapter/slack-user-aliases.json`) | JSON map of bare gc handle → raw Slack target ID — a user (`Uxxxx`/`Wxxxx`) or User Group (`Sxxxx`). The **outbound** inverse of `SLACK_SUBTEAM_ALIAS_FILE`: `/publish` rewrites `@handle` body tokens to Slack mention syntax (`<@Uxxxx>` / `<!subteam^Sxxxx>`) so they render as clickable, notifying mentions instead of literal text. Only handles present in the map are rewritten — unmapped handles stay literal (fail-safe, no surprise pings). The `@` is only treated as a mention at a left word boundary, so email-like `user@host` strings are left intact. Hand-edited (operator-curated allowlist); read-only at runtime, SIGHUP or restart to reload. |
| `INBOUND_FILE_STORE`           | `/tmp/gc-slack-adapter/inbound`                  | Directory for downloaded inbound Slack file attachments.                        |
| `INBOUND_FILE_TTL`             | `168h` (7 days)                                  | Janitor retention. `0` disables sweeping.                                       |
| `INBOUND_FILE_SWEEP_INTERVAL`  | `1h`                                             | Janitor scan period. `0` disables sweeping.                                     |
| `SLACK_DISPATCH_CONCURRENCY`   | `50`                                             | Cap on in-flight inbound-dispatch goroutines (slash-command, slack-event, alias-resolved). On saturation the adapter drops the dispatch with a `dispatch queue full` log line; the inbound POST itself is not affected. Must be a positive integer; 0/negative/non-numeric values fail at startup. (sec-S-04) |

**Permissions:** `IDENTITY_STORE_PATH`, `HANDLE_ALIAS_STORE_PATH`, and
`INBOUND_FILE_STORE` are written with `0o700` directories and `0o600`
files so contents are readable only by the adapter's UID. On startup
the adapter additionally tightens any pre-existing files/directories
that are looser (one-shot migration for legacy `/tmp/gc-slack-adapter/`
trees from earlier versions). Operators using a custom-mode parent
(e.g. setgid for shared-group access) should set the perms before
adapter start; the tightener preserves setuid/setgid/sticky bits and
never loosens an operator-tighter mode. For multi-tenant hosts,
override these paths to a host-private location explicitly created
with `0o700`. The proxy_process Unix domain socket
(`GC_SERVICE_SOCKET`) is also chmod'd to `0o600` after bind as
defense-in-depth on top of its `0o700` controller-managed parent
directory at `/tmp/gcsvc-<uid>/<hash>/`.

**Controller-injected** (proxy_process mode only — gc sets these when
the adapter runs as a `[[service]]`; do not set them by hand):
`GC_SERVICE_NAME`, `GC_SERVICE_SOCKET`, `GC_SERVICE_URL_PREFIX`,
`GC_SERVICE_STATE_ROOT`, `GC_SERVICE_RUN_ROOT`. See
"What gc injects vs. what stays in the env file" above.

### SIGHUP-driven reload

Five of the adapter's registries are written by `gc slack` CLI
commands and read by the adapter at startup:

| Registry             | File                          | CLI writer                      |
| -------------------- | ----------------------------- | ------------------------------- |
| Apps                 | `apps.json`                   | `gc slack import-app`           |
| Channel mappings     | `channel_mappings.json`       | `gc slack map-channel`          |
| Rig mappings         | `rig_mappings.json`           | `gc slack map-rig`              |
| Room launch mappings | `room_launch_mappings.json`   | `gc slack enable-room-launch`   |
| Subteam aliases      | `subteam-aliases.json`        | `gc slack sync-subteam-aliases` |

Send `SIGHUP` to the running adapter to pick up CLI-driven changes
without a full restart:

```
pkill -HUP gc-slack-adapter
```

Reload is all-or-nothing across the five files — a single parse
failure (corrupt JSON, unknown `target_kind`, missing required field,
file >10 MiB) aborts the cycle with the live in-memory state
untouched. Errors are logged at WARN; the adapter keeps serving.

A missing file is a **no-op** (live state is preserved). To clear a
registry, write an empty JSON object instead of removing the file:

```
echo '{}' > <city>/.gc/slack/channel_mappings.json
pkill -HUP gc-slack-adapter
```

The other three registries — `IDENTITY_STORE_PATH`,
`HANDLE_ALIAS_STORE_PATH`, and the thread-session store — are
**adapter-owned**: written in-process by the adapter itself (via
`/identity`, `/handle-alias`, and the launcher), not by the CLI, so
they do not participate in SIGHUP reload. Do not hand-edit them while
the adapter is running — it rewrites them from its in-memory state at
will, so manual edits are silently overwritten. The five files in the
table above are the operator-editable surface; everything else under
the store paths belongs to the adapter.

`apps.json` is the one file written from **both** sides (the CLI's
`import-app` and the adapter's OAuth callback). The adapter
generation-stamps its reload snapshots so a SIGHUP racing an OAuth
callback re-stages instead of rolling the in-memory view back to the
pre-callback state; if the log shows `apps registry commit skipped`,
re-send the SIGHUP.

**Consumer-specific** (referenced by deployment scripts and prompts in
sibling tooling, not by the adapter binary): variables consumed by
`deliver-rollup.sh`, `resolve_rig_channel.py`, etc., live with the
consumer pack and are documented there. The adapter has no opinion on
what handles map to which sessions — that's `/handle-alias` registry
content, set by the consumer at deploy time.

### Slack secrets — placement

Recommended placement for the four must-set vars (and any overrides):
`${XDG_CONFIG_HOME:-$HOME/.config}/gc-slack-adapter/env` (`0600`
perms). Source the file before `gc start` so the adapter inherits
the env via `os.Environ()`. Under systemd-managed deployments, drop a
unit override that points
`EnvironmentFile=-…/gc-slack-adapter/env` at the same path so the
supervisor passes the env to its `proxy_process` children. Phase B
will move the env-file path into pack config; not yet wired.

```
SLACK_WORKSPACE_ID=T0XXXXXXXXX
SLACK_BOT_TOKEN=xoxb-...
SLACK_SIGNING_SECRET=...
GC_CITY_NAME=<your-city-name>
```

### Cutover sequence

```
# 1. Build the adapter binary in place (source colocated with the pack).
#    Optional: the [[service]] command is adapter/run.sh, which rebuilds
#    gc-slack-adapter itself when the binary is missing (e.g. after
#    `gc import install` re-materialized the pack cache git-only).
( cd slack-full/adapter && go build -o gc-slack-adapter )

# 2. Source the secrets so the supervisor inherits them.
#    Optional: run.sh sources this same file itself at service start
#    (override the path with GC_SLACK_ADAPTER_ENV — if you set it, the
#    file must exist: run.sh refuses to start on ambient credentials
#    rather than post to whatever workspace is in the environment).
set -a; source "${XDG_CONFIG_HOME:-$HOME/.config}/gc-slack-adapter/env"; set +a

# 3. Stop any manually-managed adapter that may still be running
pkill -f gc-slack-adapter || true

# 4. Reload the city so the [[service]] block from slack-full registers
gc reload   # or: gc supervisor reload

# 5. Verify the service is ready
gc service list                            # expect: slack proxy_process ready
curl --unix-socket "$(gc service show slack --json | jq -r .socket)" \
     http://x/healthz                      # expect: 200, first line "ok",
                                           # then dispatch_dropped_total=N
                                           # (inbound events dropped because
                                           # the dispatch pool was saturated;
                                           # a growing N means raise
                                           # SLACK_DISPATCH_CONCURRENCY)

# 6. Verify outbound publish through gc (replace the placeholders).
#    GC_API_BASE_URL defaults to http://127.0.0.1:8372 on a single-host
#    deployment; GC_CITY_NAME is the directory-name of your city.
curl -s -X POST -H "Content-Type: application/json" -H "X-GC-Request: cutover" \
  -d '{"session_id":"<bound-session>","conversation":{"scope_id":"<city>","provider":"slack","account_id":"T0XXXXXXXXX","conversation_id":"D0XXXXXXXXX","kind":"dm"},"text":"*cutover:* hello"}' \
  "${GC_API_BASE_URL:-http://127.0.0.1:8372}/v0/city/${GC_CITY_NAME:-<city>}/extmsg/outbound" \
  | jq '.Receipt.Delivered'                # expect: true

# 7. Verify inbound — send a Slack DM to the bot, then:
gc events --city "${GC_CITY_NAME:-<city>}" --type extmsg.inbound --since 2m
```

Rollback: remove (or comment out) the `[[service]]` block in
`pack.toml`, `gc reload`, then restart the manual adapter via the
legacy script:

```
( cd slack-full/adapter \
    && nohup ./run.sh > /tmp/gc-slack-adapter/run.log 2>&1 & disown )
```

The adapter ignores `$GC_SERVICE_SOCKET` when unset and falls back to
TCP-only mode, so the same binary serves both deployments.

### Known foot-guns

- **Two adapters running.** If you forget step 3 and the manual
  adapter stays up while gc starts the proxy_process one, both will
  call `/extmsg/adapters` to register. The second registration
  overwrites the first; outbound publishes go through whichever one
  registered last (last-write-wins). Symptom: outbound succeeds but
  through the wrong process. Stop the manual one and reload.
- **Slack signing key missing.** The adapter Fatals at start with
  `missing required env vars: SLACK_SIGNING_SECRET`. Under
  proxy_process this shows up as the service stuck in `degraded`
  state with the env-var name in the reason field. Source the env
  file in the supervisor's launching shell.
- **Funnel rule out-of-band.** Tailscale Funnel `:443 → :8775` is not
  declared in the city. If you reboot the host or `tailscale funnel
reset`, traffic stops landing at the adapter. Re-add the rule
  manually until Phase C lands.

## Where the work that's still missing comes from

The discord pack ships ~350K LOC of Python service code; the bulk of
that is provider-agnostic state-machine logic for room peer fanout,
launcher mode, slash-command intake, and workflow status projection.
A meaningful chunk should eventually become a shared `extmsg-pack-lib`
or similar so slack/discord/teams/etc. don't all reimplement it.

For now we're porting feature-by-feature, against real usage from the
oversight-rig pack.
