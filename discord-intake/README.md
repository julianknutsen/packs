# Discord Intake Pack

Workspace-hosted Discord slash-command intake for Gas City.

This pack is superseded by the newer `discord` pack. Do not include both
`discord-intake` and `discord` in the same workspace: they declare the same
published service identities and will collide. New work should move to the
`discord` pack and re-import state under `.gc/services/discord/`.

This pack keeps `gastown-hosted` generic. It runs the Discord-facing service
inside the workspace and exports it through the normal published-service path:

- `discord-interactions` is the public Discord Interactions endpoint
- `discord-admin` is the tenant-visible setup and status surface
- both services share `.gc/services/discord-intake/`

The current slice ships:

- Discord app metadata import and bot-token storage
- Interactions signature validation using Discord's public key
- durable receipt, modal, request, and workflow-link persistence
- slash-command `/gc fix` handling with modal-backed summary/context capture
- per-conversation idempotency for `/gc fix`
- guild, channel, and optional role allowlist enforcement
- guild-scoped command sync for `/gc fix`
- rig bead creation plus `gc sling <target> <bead> --on <formula>` dispatch
- Discord status updates when work begins and when the workflow completes
- `mol-discord-fix-issue` workflow for TDD bugfix intake from Discord

This first Discord slice is intentionally intake-only. It proves the published
workspace-service model without taking on the separate `discord-session`
runtime or GitHub-specific PR automation.

If a dispatched workflow gets wedged and you need to retry the same Discord
conversation before cancel or retry automation exists, release the intake lock
manually:

```bash
gc discord-intake release-workflow 123456789012345678 223456789012345678
# or
gc discord-intake release-workflow --request-id dc-interaction-fix
```

## Include It

```toml
[packs.discord-intake]
source = "https://github.com/julianknutsen/packs.git"
ref = "main"
path = "discord-intake"

[workspace]
includes = ["discord-intake"]
```

## Publication

This pack expects helper-backed published services. After the workspace starts,
`gc service list` should show:

- `discord-interactions` with public publication
- `discord-admin` with tenant publication

Open the tenant-visible `discord-admin` URL to get the published interactions
URL and current app state.

## App Import

Create a Discord app in the Developer Portal, then import the app metadata and
bot token:

```bash
gc discord-intake import-app \
  --application-id 123456789012345678 \
  --public-key 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef \
  --bot-token "$DISCORD_BOT_TOKEN"
```

After import, point the app's Interactions Endpoint URL at:

```text
https://<discord-interactions-public-url>/v0/discord/interactions
```

The bot token is required for this slice because the workflow posts back to
Discord when work starts and when it completes.

## Channel Mapping

Map Discord channels to workflow targets:

```bash
gc discord-intake map-channel 123456789012345678 223456789012345678 product/polecat \
  --fix-formula mol-discord-fix-issue
```

That stores dispatch config locally under `.gc/services/discord-intake/data/`.

## Command Sync

Register the guild-scoped `/gc fix` command after the bot is installed:

```bash
gc discord-intake sync-commands 123456789012345678
```

Discord command delivery stays broad at the platform edge. Access control is
enforced inside the workspace service with the configured guild, channel, and
role allowlists.

## Inspect Status

```bash
gc discord-intake status
gc discord-intake status --json
```

## Workflow Helper

The formula uses the message helper to project status back to Discord:

```bash
gc discord-intake post-message --request-id dc-123-fix --body "Started work"
```
