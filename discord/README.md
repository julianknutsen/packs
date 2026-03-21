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
- explicit `gc discord publish` for human-visible replies through saved bindings
- safer `gc discord reply-current` for replying to the latest Discord turn in-session
- shared prompt fragment at `prompts/shared/discord-v0.md.tmpl`

Not shipped yet in this pack:

- room peer fanout between sessions after a publication

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

Bind Discord conversations to exact permanent session names:

```bash
gc discord bind-dm 123456789012345678 sky
gc discord bind-room --guild-id 223456789012345678 323456789012345678 sky lawrence
```

Publish a human-visible reply through a saved binding:

```bash
gc discord publish --binding room:323456789012345678 --body-file ./reply.txt
gc discord publish --binding room:323456789012345678 --trigger 423456789012345678 --body "On it."
gc discord publish --binding room:323456789012345678 --conversation-id 523456789012345678 --trigger 423456789012345678 --body "Reply in the thread"

# Preferred agent reply path for the current Discord turn
gc discord reply-current --body-file ./reply.txt
```

Inbound behavior in v0:

- DMs to the bot route through the matching `bind-dm` binding
- guild and thread messages route only when the bot is explicitly mentioned
- thread messages inherit the parent room binding when the thread itself is not bound
- `@sky` inside the message targets that session name exactly
- untargeted room messages fan out to every bound participant session
- agent normal output remains private; only explicit publish commands speak back to humans
- agents should prefer `gc discord reply-current --body-file ...` when answering the latest Discord turn

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
