# Changelog

All notable changes to slack-full (formerly slack-pack) are documented in
this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- `gc slack reply-current` now inherits the thread from the latest
  inbound (gp-i62): a thread-reply inbound's transcript entry carries
  the Slack `thread_ts` in `ReplyToMessageID`, and the reply anchors
  there by default — including when `--conversation-id` names the same
  conversation explicitly, which previously always posted at channel
  level (live burns 2026-08-09, #gastown + #fundraising). An unthreaded
  inbound keeps the channel-level reply; an explicit target naming a
  *different* conversation never borrows the inbound's thread anchor;
  a failed lookup degrades to channel level with a stderr warning
  (`--via adapter` must survive a gc outage). New `--no-thread` flag
  forces a channel-level post. `--thread-current` now anchors at the
  thread ROOT when the latest inbound was itself a thread reply —
  Slack threads hang off the parent ts, so anchoring at the child
  stranded the reply. Retires the fleet-memory workaround of routing
  threaded replies through `publish-to-channel --thread-ts`.

### Added

- Accidental-mrkdwn guard on the send path (gp-o42): `gc slack
  reply-current` (legacy and company paths), `publish`,
  `publish-to-channel`, `upload --initial-comment`, and `delegate` now
  neutralize tildes that would pair into unintended Slack strikethrough
  ("~$58.5k … ~$16.5k" rendered half a runway summary struck through).
  Slack mrkdwn has no escape sequence, so accidental delimiter tildes
  are substituted with the visually identical U+223C TILDE OPERATOR —
  but only on lines where a pair could actually form: lone tildes keep
  their ASCII byte (`cd ~/repo` survives copy-paste), code spans are
  never touched, a tilde before an optionally-signed digit/currency
  (`~$5k`, `~-$13.5k`, `~9/2`) is never a delimiter, and deliberate
  tight-wrapped `~word~` strikethrough still renders. `*bold*`,
  `_italics_`, and bullets are unaffected. New `--raw` flag on all five
  commands sends the body verbatim. Pure script-side change — no
  adapter restart needed.

  Two limits worth knowing. "Lone tilde" is per rendered line, so two
  home-relative paths on one line (`rsync ~/a ~/b`) are a pairable pair
  and both get substituted; wrap twin paths in a code span, or use
  `--raw`, when the exact bytes matter. And `gc slack post-message` is
  deliberately out of scope: its milestone summary, field values, and
  rollup items are rendered as mrkdwn by the Go Block Kit renderer,
  downstream of this Python guard, so a payload carrying
  "~$58.5k … ~$16.5k" still strikes through (titles are `plain_text`
  and unaffected). That surface predates this change and guarding it
  would need a second implementation of the heuristic in Go plus a
  `gc-slack-cli` rebuild; tracked as gp-x5bdy.

  Scope is this pack only. The sibling packs `slack-channel` (same
  `publish` / `publish-to-channel` / `reply-current` verbs) and
  `slack-mini` (`post-message`) carry no guard, so the same `gc slack`
  verb behaves differently depending on which pack a city installed.
  Porting was left out deliberately to keep this change script-side
  with no adapter rebuild; guard-port vs. documented exclusion is
  tracked as gp-cbxzk.

### Fixed

- The adapter builds and its tests pass on darwin again. The confined-open
  walk called `syscall.Openat`, which exists only on Linux — darwin's
  libSystem-based `syscall` package exports neither `Openat` nor
  `SYS_OPENAT` — so the entire adapter failed to compile on macOS. The
  walk now binds `golang.org/x/sys/unix`; every flag constant and syscall
  it swaps is a value-identical mirror on Linux, so behavior there is
  unchanged. Three `readConfinedFile` tests also failed on macOS, where
  `TMPDIR` lives under `/var`, itself a symlink to `/private/var`:
  `confineFileUploadPath` EvalSymlinks-resolves only the *root* and
  documents the path as caller-pre-resolved, so a fixture handing it a
  raw `t.TempDir()` broke that contract and made every path — including
  genuinely in-root ones — read as an escape. The fixtures now resolve
  their temp dir once through `tempDirResolved`. A harness defect, not a
  confinement one: no production behavior changed.
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

- The adapter module takes its first dependency, `golang.org/x/sys`
  (hash-pinned in `adapter/go.sum`), because it is the only maintained
  source of a darwin-capable `Openat`. The module's zero-dependency
  property was traded away deliberately; the rejected alternatives and
  the reason are recorded in `adapter/confined_open.go`. One
  operator-visible consequence: `adapter/run.sh`'s build-on-missing
  self-heal used to rebuild the binary with no module downloads at all,
  and now needs either a warm module cache or GOPROXY egress the first
  time it builds. On a host behind an egress allowlist, pre-warm with
  `go mod download` in `slack-full/adapter/` before relying on the
  self-heal — a cold cache there fails the rebuild with `module lookup
  disabled by GOPROXY=off`, and the remedy `run.sh` prints on failure
  hits the same wall.
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

- Outbound publishes carrying an idempotency key are stamped with a
  low-visibility reference marker (`_ref:<12 hex>_`) so a later reader can
  find the exact message in Slack. The in-process dedup cache only lives
  for its TTL and does not survive a restart, so it cannot provide durable
  delivery evidence; the marker can. The readback contract — what a marker's
  absence does and does not mean, and where a reader has to look — is stated
  normatively in the `referenceMarker` doc comment in `adapter/main.go`, and
  this entry deliberately does not restate it, so there is one place to edit.
  Two properties worth knowing without opening the source: absence always
  means UNKNOWN, never undelivered (all history predating this change is
  unmarked); and a marker on a threaded reply is not reachable from
  `conversations.history`, which does not return thread replies.

  Note on Slack's ceiling, since the obvious assumption is wrong: Slack does
  not reject text over 40,000 — it truncates from the end and still answers
  `ok:true`. The marker is the tail of the text, so an oversized post would
  deliver a mangled partial marker under a receipt that looks perfect. The
  advertised `MaxMessageLength` therefore reserves the marker's 20 bytes, and
  `handlePublish` enforces the same budget itself rather than trusting an
  advertisement nothing in gc reads today. Over the budget the caller's text
  is posted **unstamped** rather than trimmed: the message is the caller's,
  the bookkeeping is ours, and an honest absence is already something the
  contract requires readers to tolerate. When Slack reports a truncation
  anyway, the publish receipt carries `metadata["truncated"]="true"`
  (`dr-3msk6.3`).
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
