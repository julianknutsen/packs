# Changelog

All notable changes to slack-full (formerly slack-pack) are documented in
this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- Inbound files recorded inside Slack itself — voice clips (file subtype
  `slack_audio`) and video clips (`slack_video`) — arrive with an empty
  `mimetype` AND `filetype`, which the adapter passed through verbatim.
  The inbound payload then omitted `mime_type`, a REQUIRED property of
  gc's `extmsg.ExternalAttachment`, so gc rejected the whole message
  with 422 and the recording plus its text caption silently never
  reached the bound session. The adapter now derives a media type for
  every inbound file (Slack's `mimetype` → file-name extension → Slack
  `filetype` code → Slack-native subtype → `application/octet-stream`)
  and always serializes `mime_type` (no longer `omitempty`), so a
  payload can never again be rejected for a missing key.
  Scope: this covers the legacy `/extmsg/inbound` delivery path. Files
  shared in a company/multi-bot room take the company hydration path,
  which never posts to `/extmsg/inbound` and still renders Slack's raw
  `filetype`, so a Slack-native recording there is unchanged by this fix
  and still lists an empty filetype.
- The `[[service]]` proxy_process command pointed straight at
  `adapter/gc-slack-adapter`, a gitignored build artifact — but
  `gc import install` re-materializes the pack cache git-only, so every
  pack pin bump wiped the binary and left the slack service dead until
  someone rebuilt it by hand. The service command is now
  `adapter/run.sh` (checked in, survives every materialization): it
  sources the secrets env file, execs an existing binary untouched,
  and on a missing binary finds a Go toolchain (PATH plus
  Homebrew/system fallbacks for minimal supervisor environments),
  rebuilds from the colocated sources with loud stderr logging,
  publishes via PID-suffixed temp file + atomic `mv` (safe under
  concurrent supervisor restarts), and execs.

  Env-file handling distinguishes the two cases. An absent *default*
  (`~/.config/gc-slack-adapter/env`) warns and continues, since
  supervised deployments legitimately inject the env another way and
  the adapter fail-fasts on missing required keys itself. An absent
  path you set *explicitly* in `GC_SLACK_ADAPTER_ENV` is fatal: the
  adapter validates that credentials are present, never where they
  came from, so continuing would boot it against whatever Slack token
  happens to be in the ambient environment.

  Because the rebuild now sits on the service startup path, it is
  hardened for the minimal supervisor environments it targets: the
  toolchain search also covers `/usr/bin/go` and `/bin/go` (where a
  distro Go lands, and which a restricted PATH hides); `GOCACHE` and
  `GOPATH` are defaulted under `TMPDIR` when HOME, `XDG_CACHE_HOME`
  and `GOCACHE` are all unset, which `go build` otherwise rejects
  outright; and the toolchain is checked against `go.mod`'s `go`
  directive up front, so a too-old Go under `GOTOOLCHAIN=local` fails
  with a named remedy instead of a raw compiler error (the default
  `GOTOOLCHAIN=auto`, which can fetch the required toolchain itself,
  warns and proceeds).

### Changed

- **Behavior change on upgrade:** the slack service now *exits 1 at
  startup* when `GC_SLACK_ADAPTER_ENV` is set to a path that does not
  exist, where it previously logged one warning and started on the
  ambient environment. Pointing the variable at a nonexistent path is
  this pack's own established idiom for "no env file", so a unit file or
  wrapper using it that way for the **service** will stop starting after
  this pin bump. Audit before upgrading with
  `grep -r GC_SLACK_ADAPTER_ENV` across your unit files and wrappers, and
  either create the file or unset the variable to fall back to the
  default. The strict rule is service-only: `slack_chat_*` commands
  (`scripts/slack_intake_common.py`) still treat a missing path as "no
  env file".
- Renamed the pack directory from `slack-pack/` to `slack-full/` and the
  `pack.toml` name from `slack` to `slack-full` as part of the Slack pack
  tiering split (`gc-yrw`). This pack is now **Tier 3** of the Slack
  family; the smaller [slack-mini](../slack-mini) (Tier 1) and
  [slack-channel](../slack-channel) (Tier 2) packs were extracted from it.
  The user-facing verb surface (`gc slack <cmd>`) and the registered
  `slack` service name are unchanged — only the catalog directory and pack
  name moved, so there is no collision with `slack-mini` / `slack-channel`.
  See the [tiering design memo](../docs/design/slack-pack-tiering.md) and
  the README "Tiering" section for the decision tree. (`gc-yrw.5`)
- Moved CLI commands (`gc slack import-app`, `map-channel`, `map-rig`,
  `sync-commands`, `enable-room-launch`, `post-message`) from the gc
  binary into a new in-pack Go module at `examples/slack-pack/cli/`
  (module `github.com/sjarmak/gc-slack-cli`). User-facing surface
  (`gc slack <cmd>`) is unchanged — pack wrappers under
  `commands/<cmd>.sh` exec the new binary at
  `$GC_PACK_DIR/cli/gc-slack-cli` so operator command-line ergonomics
  stay identical to the pre-relocation gc-binary verbs. The pack now
  ships two Go binaries (adapter + cli), each in its own go.mod;
  `gc slack status` continues to dispatch to the existing Python
  implementation under `scripts/slack_chat_status.py`. Build flow
  documented in [CONTRIBUTING.md](./CONTRIBUTING.md#build-flow).
  (`gc-coe10`)

### Added

- Busy-reaction lifecycle on targeted inbounds plus `reactions.remove`
  support (`hq-xizo`). When an inbound message explicitly addresses an
  agent (`@handle:` prefix or a mapped User Group mention) and is
  successfully dispatched, the adapter now adds a busy reaction to the
  inbound Slack message (`BUSY_REACTION`, default `hourglass`;
  set-but-empty disables) instead of the previous `eyes`, and removes
  it when the agent's reply is published back into the same
  conversation/thread — the channel-native replacement for Slack
  Assistant-mode `assistant.threads.setStatus`, which this adapter
  deliberately does not use. Pending marks live in a bounded in-memory
  registry keyed by (conversation, thread) and expire after 30 minutes.
  The `/react` endpoint gains an optional `"remove": true` field that
  issues `reactions.remove` instead of `reactions.add`
  (backward-compatible; `no_reaction` is treated as benign delivery,
  mirroring `already_reacted` on add). The ⚠️ alias-dispatch-failure
  reaction is unchanged.
- `gc slack retry-peer-fanout` — operational recovery for peer-fanout.
  Walks recent `extmsg.peer_fanout_failed` events (added in this change
  too), filters by `--since` / `--conversation` / `--max`, deduplicates
  against successful `extmsg.peer_fanout_retried` events, and re-issues
  each notification via the new
  `POST /v0/city/<cityName>/extmsg/peer-fanout/retry` endpoint with a
  small cooldown between attempts. The endpoint emits an
  `extmsg.peer_fanout_retried` audit event per attempt with the
  `original_seq`, so re-running on the same set is a no-op
  (`gc-cby.7`).
- SIGHUP-driven reload for the four CLI-written registry files
  (`apps.json`, `channel_mappings.json`, `rig_mappings.json`,
  `room_launch_mappings.json`). Operators can now run
  `gc slack import-app`, `gc slack map-channel`, `gc slack map-rig`, or
  `gc slack enable-room-launch` and signal the adapter with
  `pkill -HUP gc-slack-adapter` (or any other SIGHUP delivery) to pick
  up the new bindings without a service restart (`gc-cby.23`). Reload is
  all-or-nothing across the four registries — a single parse failure
  aborts the cycle with the live state untouched. A missing file is a
  no-op (preserves state); operators clear by writing an empty `{}`
  document, NOT by `rm`.

### Changed

- The trailing reminder printed by `gc slack map-rig`,
  `gc slack map-channel`, and `gc slack enable-room-launch` now leads
  with the SIGHUP path (`pkill -HUP gc-slack-adapter`) and offers
  `gc service restart slack` as the fallback, since SIGHUP avoids the
  startup gap.
- Adapter Go source relocated from `examples/oversight-rig/adapter/`
  to `examples/slack-pack/adapter/` (`gc-28a`). The pack is now
  self-contained for upstream extraction into a separate
  `gascity-packs` repo. No behavioral change; the binary path
  (`examples/slack-pack/adapter/gc-slack-adapter`) is unchanged, so
  the supervised `proxy_process` service picks up the new build at
  next restart with byte-identical functionality.
- Build flow simplified to a single command:
  `cd examples/slack-pack/adapter && go build -o gc-slack-adapter`.

### Fixed

- DM inbounds now pass a fail-closed membership gate before any
  processing (`hq-xizo`). Slack can deliver `message.im` events for DM
  conversations the bot is not actually a member of — notably with
  Assistant/Agent mode enabled — which would forward a private
  human-to-human conversation to agents. The adapter now verifies the
  bot can access the conversation via `conversations.info` and
  silently drops the event otherwise (never revealing the bot saw it);
  the drop log carries channel id and reason only, never the body.
  Verdicts are cached per channel (allowed: 5 minutes,
  `channel_not_found`: 30 seconds); transient API failures also drop
  the event but are never cached, so the next event retries.
  Channel/group/mpim inbounds skip the gate — they are governed by
  explicit operator bindings.
- The attachments block injected into the address-by-handle
  system-reminder now instructs the receiving agent to Read a
  `file://` path only if relevant and to never paste the paths into
  its reply (`hq-xizo`). Bare or pasted local image paths trigger
  Claude Code auto-attach, which re-reads the image every turn and
  can wedge the session with Anthropic HTTP 400s. Count and per-file
  lines are unchanged.
- Inbound messages whose text carries the `*Sent using* <@USER_ID>`
  trailer that Slack appends to user-token MCP-integration posts are
  now dropped before the forward to gc (`hq-xizo`). Those posts arrive
  with an empty `bot_id` and no subtype, so the existing bot/system
  guard did not catch them; without this check a bound channel can
  loop agent→Slack→adapter→agent (observed live 2026-07-16). The drop
  is logged with channel, ts, and text length only — never the body.
- Inbound Slack events are no longer lost when the forward to gc's
  `extmsg/inbound` endpoint fails (`hq-xizo`). The adapter 200-acks
  Slack before forwarding, so a failed POST used to drop the message
  silently — Slack never redelivers after a 200. Each decoded inbound
  is now persisted to a disk spool (`INBOUND_SPOOL_DIR`, defaulting to
  `$GC_SERVICE_STATE_ROOT/data/inbound-spool` in proxy_process mode)
  with atomic fsync'd writes before the first attempt, retried with
  backoff (5 attempts over ~2 minutes), dead-lettered to the spool's
  `dead/` subdirectory on exhaustion (the final log line keeps the
  `inbound POST failed` substring external log-watchers key on), and
  replayed at startup if a crash interrupted the retries. Duplicate
  redelivery is safe — gc dedups on the message's `dedup_key`.
- Codex review round 3 on this branch: the leaf no-follow defense is
  real again — os.Root.OpenFile resolves an in-root leaf symlink
  itself even with O_NOFOLLOW, so openBeneath now Lstat-rejects a
  symlinked leaf and fstat-compares the opened file against that
  Lstat via os.SameFile (same pattern as the root check); the busy
  mark is recorded BEFORE the forward and cancelled on delivery
  failure, closing the fast-reply window where gc hands the message
  to the agent while deliverInbound is still returning; redelivery
  admission is one locked operation — mark() itself rejects a
  recently-cleared message and coalesces a concurrent copy of the
  same event, so exactly one add lifecycle runs per message; startup
  replay collects only paths and lets the bounded workers read and
  decode entries, so a huge backlog cannot hold every payload in
  memory at once. One finding is an ACCEPTED TRADEOFF, not a code
  fix: retries/replays are at-least-once and can duplicate a turn on
  an ambiguous failure, because gc core does not yet consume the
  extmsg dedup_key (docs/phase5-ledger-readiness.md); the alternative
  is losing acked Slack messages. Documented in the spool package
  comment; disappears when core honors dedup_key.
- Codex review round 2 on this branch (1 P1 + 5 P2, all fixed):
  un-spooled retries (spooling disabled or the write failed) HOLD
  their dispatch slot across the retry sleeps again — the semaphore
  is the only bound on sleeping retry goroutines without a durable
  entry, so the early release now applies only to spooled messages;
  `INBOUND_SPOOL_DIR=` (set-but-empty) is honored as the documented
  persistence opt-out instead of being replaced by the state-root
  default; a threaded `/publish-file` reply clears the busy mark
  exactly like a text publish; a failed alias dispatch removes the
  busy emoji it added (takeExact — never touching a newer re-target's
  mark); a Slack redelivery of an event whose reply already consumed
  the mark no longer re-adds an hourglass nothing would ever clear
  (5-minute cleared-message tombstones); and startup spool replay
  runs through a bounded 4-worker pool instead of one goroutine per
  entry.
- Codex review round on this branch (8 findings, all fixed):
  - The app manifest now grants `im:read` — the DM membership gate's
    `conversations.info` probe requires it, and without it the gate
    failed closed on `missing_scope` and dropped every legitimate DM.
    The gate's drop log now names the missing scope and the fix.
  - `openBeneath` rejects a symlinked upload root (Lstat + pinned-fd
    `os.SameFile` identity check): `os.OpenRoot` follows a root-level
    symlink, which would have re-anchored confinement at an
    attacker-chosen directory and weakened the old walk's
    O_NOFOLLOW-on-root TOCTOU defense.
  - The spool entry now outlives forward success until the targeted
    alias dispatch completes (removed on dispatch success,
    dead-lettered on dispatch failure), and startup replay re-resolves
    `ExplicitTarget` and redoes the dispatch — a crash between forward
    and dispatch used to lose the targeted copy silently. Both legs
    are at-least-once (forward deduped by `dedup_key`; a crash after
    dispatch can duplicate the session message).
  - Spooled retries no longer hold a dispatch-semaphore slot across
    their ~2-minute sleep schedule: the slot is released before the
    first retry sleep (release is now idempotent), so a gc outage
    cannot pin all slots on sleeping goroutines and starve admission —
    fresh events would have been dropped un-spooled.
  - Busy-reaction lifecycle races: a thread-reply inbound registers
    its mark under both the thread root and its own ts so either reply
    threading shape clears the emoji; a reply landing while the async
    `reactions.add` is still in flight defers the removal to the add's
    completer instead of firing a remove that would lose the race and
    strand the emoji; and re-tagging the same thread removes the
    displaced mark's emoji instead of silently forgetting it.
  - Concurrent DM-gate cache misses for the same channel coalesce onto
    one in-flight `conversations.info` probe (waiters adopt the
    winner's verdict) instead of fanning a DM burst into N identical
    API calls.

### Security

- Default adapter state under `/tmp/gc-slack-adapter/*` is no longer
  world-readable on shared hosts (`gc-ywe.6`). Concretely: the
  identity registry, handle-alias registry, and inbound file store now
  create directories with mode `0o700` and files with mode `0o600`
  (previously `0o755`/`0o644`). Pre-fix installs are migrated on
  startup by a one-shot tightener that walks the three configured
  store paths and chmods only-if-strictly-looser; setuid, setgid, and
  sticky bits are preserved so operator-customized layouts (e.g.
  setgid for shared-group access) survive intact. Operators who
  deliberately set tighter perms (e.g. `0o400` read-only) are also
  left alone. As defense-in-depth, the proxy_process Unix domain
  socket is chmod'd to `0o600` after bind on top of its
  `0o700` controller-managed parent directory at
  `/tmp/gcsvc-<uid>/<hash>/`.

## [0.1.0] - 2026-05-03

Initial preview. Feature-by-feature port of the upstream `discord` pack
shape; today's surface is enough to run a multi-session oversight loop
end-to-end (DMs, rooms, peer fanout, identity overrides, bidirectional
file attachments).

### Added

- `gc slack bind-dm` — bind a Slack DM channel to one named session.
- `gc slack bind-room` — bind a room to multiple sessions, with
  `--enable-peer-fanout`, `--allow-untargeted-publication`,
  `--max-peer-triggered-publishes`, `--max-total-peer-deliveries`,
  `--default-handle`, `--handle HANDLE=SESSION`, and
  `--binding-owner`.
- `gc slack reply-current` — reply to the latest Slack event in the
  current session, routed through gc's `/extmsg/outbound` so transcript
  recording and peer fanout fire (`--via adapter` keeps the direct path
  for diagnostics).
- `gc slack publish` — publish to a session's saved binding (target
  session required, no event-scan fallback).
- `gc slack publish-to-channel` — publish to an arbitrary channel ID
  with no session binding required.
- `gc slack status` — read-only diagnostics over adapters, bindings,
  and recent traffic. Supports `--session SID`, `--since`, and
  `--json`.
- `gc slack react` — add an emoji reaction to a Slack message.
- `gc slack identity` — register and unregister per-session
  `chat:write.customize` identities so each bound session posts under
  its own persona.
- `gc slack handle-alias` — register and unregister cross-channel
  `@handle` to session-id aliases used by the address-by-handle
  protocol.
- `gc slack upload` — bidirectional file attachments
  (`/publish-file` outbound, auto-download of inbound files into
  `$INBOUND_FILE_STORE/<channel>/<ts>-<filename>`, scrubbed by an
  in-process retention janitor).
- `template-fragments/slack-v0.template.md` — composable prompt
  fragment for any agent in a slack-bound session.
- Pack-owned intake service (`[[service]]` proxy_process) supervising
  the adapter via UDS for `/publish`, with the public Slack webhook
  still terminating at adapter TCP `:8775`.
- Native `SessionID` field on `PublishRequest` (replacing the prior
  metadata workaround).
- Scope banner and host-agnostic README copy for upstream-prep
  readiness.
- Adapter env contract documented in the package docstring and in the
  pack README, categorized as must-set / optional-override /
  controller-injected / consumer-specific.

### Changed

- **Breaking (standalone deployments only):** `GC_CITY_NAME` is now
  required. The adapter previously fell back to a hardcoded city name
  when the env var was unset, silently routing inbound traffic to the
  wrong destination. Any standalone (`run.sh`-style) deployment must
  set `GC_CITY_NAME` explicitly. `proxy_process`-supervised deployments
  are unaffected as long as the env file sourced before `gc start`
  defines it.

### Provenance

This release was developed in-tree at `examples/slack-pack/` (with the
adapter at `examples/oversight-rig/adapter/`). Key gascity commits:

- `cfd6d7de` — initial Slack extmsg adapter (Path B).
- `8495e4d7` — pack scaffold + `bind-dm` + `reply-current`.
- `4aa07108` — `bind-room` with peer-fanout policy plumbing.
- `39d92543` — route `reply-current` through `gc /extmsg/outbound`.
- `c1e1f6a1` — adapter UDS mode + `[[service]]` proxy_process
  (`gc-5rz` Phase A).
- `111641dd` — `gc slack status` read-only diagnostics.
- `3edeb3d0` — `gc slack publish` to session bindings.
- `b8abb72d` — identity DELETE + bidirectional file attachments + new
  commands.
- `bfd64511` — native `SessionID` in `PublishRequest`, drop metadata
  workaround (`gc-kvt`).
- `010bc588` — strip host-specific references and add scope banner
  (`gc-ywe.1`, `gc-ywe.4`).
- `3db27544` — document adapter env contract and remove the
  `ds-research` `GC_CITY_NAME` fallback (`gc-ywe.2`).

[0.1.0]: https://github.com/gastownhall/gascity/commits/main/examples/slack-pack
