# Discord Pack

Workspace-hosted Discord provider extension for Gas City.

This pack keeps Discord outside core `gc`. The pack owns the Discord-facing
services, state, commands, and prompt fragments under `.gc/services/discord/`.

`discord` replaces the older `discord-intake` pack surface. Do not include
both packs in the same workspace: they intentionally share the
`discord-interactions` and `discord-admin` service identities, so loading both
at once will collide.

Current slices:

- Discord app metadata import and bot-token storage
- public Interactions endpoint plus tenant-visible admin/status surface
- private gateway worker for inbound DMs and bot-mentioned room messages
- slash-command `/gc fix` intake with modal-backed summary/context capture
- guild/channel/role policy enforcement and guild command sync
- workflow status projection back to Discord
- DM/room chat bindings stored in pack state
- normalized `<discord-event>` delivery into exact named sessions
- root-room launcher mode with `@@handle` and thread-scoped sessions
- explicit `gc discord publish` for human-visible replies through saved bindings
- safer `gc discord reply-current` for replying to the latest Discord turn in-session
- bridge-local room peer fanout after a successful publish
- `gc discord retry-peer-fanout` for redriving failed peer targets without reposting to Discord
- shared prompt fragment at `prompts/shared/discord-v0.md.tmpl`

## Include It

```toml
[packs.discord]
source = "https://github.com/julianknutsen/packs.git"
ref = "main"
path = "discord"

[workspace]
includes = ["discord"]
```

## Migration

If you currently use `discord-intake`, migrate by switching the workspace
include to `discord`, then re-import app credentials and recreate mappings in
the new state root:

```bash
gc discord import-app ...
gc discord map-channel ...
gc discord map-rig ...
gc discord sync-commands <guild-id>
```

Re-running `gc discord sync-commands <guild-id>` after the migration is
required. Discord keeps the old registered command shape until you sync again,
so the new command schema does not take effect by itself. After the sync, the
`/gc fix` `rig` option is no longer required by Discord. Omitting `rig` now
routes by channel mapping when one exists and otherwise fails closed with the
normal "no channel mapping" rejection.
Until you re-run `sync-commands`, Discord still enforces the old `rig`-required
schema at the API layer, so users will see a Discord validation error before
the new channel-fallback behavior can run.

The old pack stored state under `.gc/services/discord-intake/`; this pack uses
`.gc/services/discord/`.

## Publication

After the workspace starts, `gc service list` should show:

- `discord-interactions` with public publication
- `discord-admin` with tenant publication
- `discord-gateway` as a private worker

Open the tenant-visible `discord-admin` URL to get the published interactions
URL and current app state.

## App Import

Create a Discord app in the Developer Portal, then import the app metadata and
bot token:

```bash
gc discord import-app \
  --application-id 123456789012345678 \
  --public-key 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef \
  --bot-token "$DISCORD_BOT_TOKEN"
```

After import, point the app's Interactions Endpoint URL at:

```text
https://<discord-interactions-public-url>/v0/discord/interactions
```

## Slash-Command Intake

Map Discord channels or rig names to workflow targets:

```bash
gc discord map-channel 123456789012345678 223456789012345678 product/polecat \
  --fix-formula mol-discord-fix-issue

gc discord map-rig 123456789012345678 mission-control mission-control/polecat
```

The default `mol-discord-fix-issue` workflow expects a `rig/polecat` sling
target. If you need a different pool contract, use a different formula.

Register the guild-scoped `/gc` command after the bot is installed:

```bash
gc discord sync-commands 123456789012345678
```

## Session Chat Control Plane

Direct bindings still exist for exact session routing:

```bash
gc discord bind-dm 123456789012345678 sky
gc discord bind-room --guild-id 223456789012345678 323456789012345678 sky lawrence
gc discord bind-room --guild-id 223456789012345678 --enable-ambient-read 323456789012345678 sky lawrence
gc discord bind-room --guild-id 223456789012345678 --enable-peer-fanout 323456789012345678 corp--sky corp--priya
```

Launcher mode is the new room-first UX:

```bash
gc discord enable-room-launch --guild-id 223456789012345678 323456789012345678
gc discord enable-room-launch --guild-id 223456789012345678 --response-mode respond_all --default-handle corp/sky 323456789012345678
```

In a launcher room, `@@handle` launches a thread-scoped session for that agent.
The session receives the root-room message immediately, but the visible Discord
thread is only created when the agent explicitly replies with
`gc discord reply-current`.

Publish a human-visible reply through a saved binding:

```bash
gc discord publish --binding room:323456789012345678 --body-file ./reply.txt
gc discord publish --binding room:323456789012345678 --trigger 423456789012345678 --body "On it."
gc discord publish --binding room:323456789012345678 --conversation-id 523456789012345678 --trigger 423456789012345678 --body "Reply in the thread"
gc discord publish --binding room:323456789012345678 --source-event-kind discord_human_message --source-ingress-receipt-id in-423456789012345678 --source-session corp--sky --body-file ./reply.txt

# Preferred agent reply path for the current Discord turn
gc discord reply-current --body-file ./reply.txt

# Repair failed peer deliveries without reposting to Discord
gc discord retry-peer-fanout discord-publish-123
```

Inbound behavior in v0:

- DMs to the bot route through the matching `bind-dm` binding
- configured launcher rooms accept `@@handle` without a bot mention
- launcher rooms may also use `respond_all` with a default qualified handle
- in `respond_all` launcher rooms, top-level replies still require an explicit `@@handle`
- the first launcher-backed agent reply creates the Discord thread automatically
- follow-up messages inside that managed thread continue to the same agent session without requiring a bot mention
- thread retargeting to a second agent is not shipped in this slice yet
- guild and thread messages route only when the bot is explicitly mentioned
- ambient-read room bindings are the exception: the bound room or bound thread accepts unmentioned messages, but only when one or more exact `@session_name` targets are present
- thread messages inherit the parent room binding when the thread itself is not bound
- inherited thread routing still requires a bot mention unless the thread itself is bound with ambient read
- ambient-read rooms stay targeted-only even when the bot is mentioned; the bot mention only gets the message into the bridge
- `@sky` inside the message targets that session name exactly
- untargeted room messages fan out to every bound participant session
- untargeted ambient-read room messages are ignored instead of broadcasting
- agent normal output remains private; only explicit publish commands speak back to humans
- agents should prefer `gc discord reply-current --body-file ...` when answering the latest Discord turn
- direct `gc discord publish` only participates in peer fanout when explicit source metadata is supplied
- bot-authored Discord messages remain ignored on inbound; peer fanout is local bridge behavior
- peer fanout for room publishes is opt-in per binding and disabled by default
- peer-triggered publishes only fan out when they explicitly mention target `@session_name` values

## Inspect Status

```bash
gc discord status
gc discord status --json
```

## Workflow Helper

The formula uses the message helper to project status back to Discord:

```bash
gc discord post-message --request-id dc-123-fix --body "Started work"
```
