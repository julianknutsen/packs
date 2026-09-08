# slack-pack manifest

Canonical Slack app manifest for the slack-pack. `app.json` is the
**source of truth** for the Slack app's display info, OAuth scopes,
event subscriptions, and (eventually) slash commands. Use it to
install the slack-pack into a fresh workspace without clicking through
the Slack web UI scope-by-scope.

Schema reference: <https://api.slack.com/reference/manifests>

## What's declared

- **`display_information`** — bot name, short/long description, brand color.
- **`features.bot_user`** — bot display name + `always_online`.
- **`features.app_home`** — Messages tab enabled (DM intake), Home tab off.
- **`features.slash_commands`** — empty list. Slash commands will be
  populated by [`gc slack sync-commands`](../README.md) (gc-cby.2) once
  that command lands. Until then, the slack-pack does not expose any
  `/gc …` shortcuts in Slack.
- **`oauth_config.scopes.bot`** — the minimal scope set the live
  adapter (`examples/slack-pack/adapter/main.go`) requires today.
  Each `*:history` scope pairs with the matching `message.*` event
  subscription below — Slack rejects an install whose subscriptions
  exceed its scopes, so the two lists must move together. The
  `*:read` scopes are the exception: they back outbound API calls, not
  event delivery, so they add no `message.*` subscription (see the
  company-rooms note under "Agent identity apps").
  - `channels:history` — read public channel messages (pairs with
    `message.channels`)
  - `channels:read` — company rooms: verify switchboard membership of
    public directory rooms (`conversations.info` /
    `conversations.members`); no paired event subscription
  - `chat:write` — post messages
  - `chat:write.customize` — per-session display-name + avatar overrides
  - `commands` — placeholder for the slash-command surface; required
    for `sync-commands` to register `/gc …` later
  - `files:read` — download user-uploaded files referenced by inbound
    `message` events
  - `files:write` — upload files via `gc slack upload`
  - `groups:history` — read private channel messages (pairs with
    `message.groups`)
  - `groups:read` — company rooms: verify switchboard membership of
    private directory rooms; no paired event subscription
  - `im:history` — read DM history (pairs with `message.im`)
  - `im:read` — DM privacy gate: `conversations.info` membership probe
    before a `message.im` inbound is processed (`adapter/dm_gate.go`);
    without it the gate fails closed and every DM is dropped with
    `missing_scope`
  - `mpim:history` — read multi-party DM messages (pairs with
    `message.mpim`)
  - `reactions:write` — `gc slack react` emoji ack
  - `users:read` — company rooms: resolve a company bot author's
    `bot_id` → `user_id` (`bots.info`) for peer trust; no paired event
    subscription
- **`settings.event_subscriptions.bot_events`** — the events the
  adapter actually dispatches on (see `processSlackEvent` in
  `adapter/main.go`):
  - `app_mention` — @-mentions in any channel
  - `message.channels` — public channel messages
  - `message.groups` — private channel messages
  - `message.im` — DMs
  - `message.mpim` — multi-party DMs

## What's NOT declared (intentionally)

- **`file_shared`** event — files arrive embedded in `message` events;
  the adapter doesn't subscribe to `file_shared` separately.
- **Concrete `slash_commands` entries** — see above; gc-cby.2 owns this.
- **Interactivity / Socket Mode / Org Deploy / Token Rotation** —
  disabled. The adapter uses HTTP event POSTs (Tailscale Funnel
  terminates `/slack/events`); see `adapter/SETUP.md`.

## Install paths

### Manual (web UI)

1. Go to <https://api.slack.com/apps> → **Create New App** → **From an
   app manifest**.
2. Pick your workspace.
3. Paste the contents of [`app.json`](./app.json) into the JSON tab.
   **Note:** the committed manifest declares `event_subscriptions` but
   deliberately omits `request_url` (it is deployment-specific), and
   Slack's validator rejects `bot_events` without one — "event
   subscription requires a request URL". Either add
   `"request_url": "https://<your-funnel-host>/slack/events"` inside
   the `event_subscriptions` block before pasting (the adapter must
   already be running there — Slack sends a `url_verification`
   challenge on save, which the adapter answers), or delete the
   `event_subscriptions` block for creation and add the subscriptions +
   URL in the UI once the endpoint is live (step 6). When you are only
   **updating scopes on an existing app**, skip the manifest entirely:
   OAuth & Permissions → add the scopes → reinstall — no URL
   validation fires.
4. **Create**. Slack provisions the bot user, scopes, and (if included)
   event subscriptions in one step.
5. **Install to Workspace** to mint the bot token (`xoxb-…`).
6. Continue with `adapter/SETUP.md` from **Step 2 → Event Subscriptions
   Request URL** onward — you still need to plug in your Tailscale
   Funnel URL and copy the signing secret.

### Importing into gc (`gc slack import-app`)

After running the manual install above to mint the app at Slack, capture
the assigned **app id** (`A0…`, found at api.slack.com → your app →
**Basic Information**) and import the manifest into the gc city:

```bash
gc slack import-app examples/slack-pack/manifest/app.json \
  --workspace-id T0123456 \
  --app-id       A0123456
```

This validates the manifest's bot scopes against the set the slack-pack
adapter and downstream commands require, then persists a typed app
record at `<cityPath>/.gc/slack/apps.json` (composite key
`(workspace_id, app_id)`). Re-importing the same `(workspace_id,
app_id)` updates the record in place — the registry never grows from
idempotent re-imports.

`import-app` does **not** call Slack — provisioning the app at Slack is
still a one-time manual step (or, eventually, gc-cby.9's OAuth install
flow). What this command does is establish the foundation that
[`sync-commands`](../README.md) (gc-cby.2),
[`map-channel` / `map-rig`](../README.md) (gc-cby.3 / .4), and friends
read from.

The on-disk shape is described by
[`schema/apps.schema.json`](../schema/apps.schema.json) — that file is
the contract between the gc CLI (writer) and the slack-pack adapter
(reader).

## Required secrets after install

After installing the manifest, the adapter needs three values exported
to its env (see `adapter/SETUP.md` for the full file template):

| Variable                | Source                                                |
| ----------------------- | ----------------------------------------------------- |
| `SLACK_BOT_TOKEN`       | OAuth & Permissions → Bot User OAuth Token (`xoxb-…`) |
| `SLACK_SIGNING_SECRET`  | Basic Information → App Credentials → Signing Secret  |
| `SLACK_WORKSPACE_ID`    | Basic Information → App Credentials → Team ID (`T0…`) |

These are workspace-specific and stay out of git. The manifest is the
only piece of Slack-app config that's checked in.

## Agent identity apps

`agent-app.json` is the manifest template for a **company-rooms agent
identity app** — one minimal Slack app per named directory agent (see
`docs/company-rooms.md`). Its bot user is the agent's real, mentionable
`<@U…>` identity in company rooms, and its bot token is the agent's
outbound sending identity for `gc slack delegate` / `reply-current`.
It is deliberately minimal: bot user, App Home Messages tab enabled,
interactivity/Socket Mode/token rotation off. For **room** traffic the
switchboard (`app.json`) stays the single admission owner — the agent app
observes no room events.

The **per-agent DM phase (Phase 4)** turns each agent app into the
admission owner of its own DMs. The template now declares:

- `oauth_config.scopes.bot`: `chat:write`, **`im:history`** (read the DM
  channel for hydration), and **`reactions:write`** (the agent app is the
  visible-ack actor on its own DM channel — the switchboard token is never
  used on a DM channel).
- `settings.event_subscriptions.bot_events`: **`message.im`**, pointed at
  the **same public funnel `request_url` as the switchboard**. As with
  `app.json`, the committed template omits `request_url` (it is
  deployment-specific); add
  `"request_url": "https://<your-funnel-host>/slack/events"` inside the
  `event_subscriptions` block before pasting/updating.

Scope + event changes require a per-app **reinstall** (18 apps + the pilot
step). **Delayed Events must be enabled on every agent app** — the
degraded-mode 503-redrive recovery depends on Slack's redelivery horizon
exactly as rooms do.

The template ships two `REPLACE-agent-name` placeholders
(`display_information.name` and `features.bot_user.display_name`).
Replace both with the agent's directory slug before creating each app.

> **Never distribute an agent identity app on the Slack Marketplace.**
> These are internal to one visibility boundary; public distribution
> demotes `conversations.*` into a stricter rate tier.

### Per agent

1. **Create + install.** Copy `agent-app.json`, substitute both
   `REPLACE-agent-name` placeholders with the agent slug, then
   <https://api.slack.com/apps> → **Create New App** → **From an app
   manifest** → pick the workspace → paste the edited JSON → **Create**
   → **Install to Workspace** to mint the bot token (`xoxb-…`).
2. **Harvest identifiers into the directory TOML.** Grab the **app id**
   (`A0…`, Basic Information) and the **bot user id** (`U0…` — run
   `auth.test` with this app's `xoxb-…` token; its `user_id` is the bot
   user id). Write both into the agent's `[[agents]]` entry in the
   company-directory TOML (`name` / `app_id` / `bot_user_id`), then
   `gc slack import-company-directory --file <rooms.toml>`.
3. **Register the signing secret, THEN wire the Request URL (order
   matters).** Harvest the app's signing secret (Basic Information → App
   Credentials → Signing Secret) and register it:

   ```bash
   gc slack register-agent-app --team-id <T…> --api-app-id <A…> --signing-secret <secret>
   ```

   This writes a `{team_id, api_app_id, signing_secret}` record into
   `agent_apps.json` (a NEW registry, 0600, loaded at startup and on
   SIGHUP — distinct from `apps.json`). Confirm `/healthz`
   `registered_agent_apps` incremented **with no join warning**
   (`registered_agent_apps_join_warnings=0`) — a warning means the
   `api_app_id` has no matching `agents[].app_id` in the directory, and
   the app will admit nothing until you fix the directory. Only **after**
   registration, set the `message.im` Request URL in the manifest and
   save: the verification handshake fires on save and verifies via the
   trial path, so **registration MUST precede the Request URL, else the
   handshake 401s.**
4. **Drop the bot token into the company secrets dir.** Write the
   `xoxb-…` token to `secrets/bot-token-<agent>.txt` under the adapter
   state root (`SLACK_COMPANY_SECRETS_DIR`, else
   `<GC_CITY_PATH>/.gc/slack/secrets/`). The token loader **refuses**
   any file that is not mode `0600`, whose directory is not `0700`, or
   that is a symlink — this is validation, not trust, so set the modes
   when you write the file:

   ```bash
   install -d -m 0700 "$SLACK_COMPANY_SECRETS_DIR"
   install -m 0600 /dev/null "$SLACK_COMPANY_SECRETS_DIR/bot-token-<agent>.txt"
   printf '%s' "$AGENT_BOT_TOKEN" > "$SLACK_COMPANY_SECRETS_DIR/bot-token-<agent>.txt"
   ```
5. **Invite the bot to its rooms.** In each directory room the agent is
   a member of, `/invite @<agent>`. A member bot that has not joined a
   room can neither be delivered its events nor post into it (private
   rooms always require membership; this design never uses
   `chat:write.public`, so public rooms behave identically).
6. **Reinstall, enable Delayed Events, then bind a DM session.**
   Reinstall the app to the workspace to pick up the new scopes
   (reinstall does **not** rotate the signing secret; the bot token
   rotates only if you explicitly reissue it — if it did, re-drop
   `bot-token-<agent>.txt`). Enable **Delayed Events** on the app. Then
   bind a DM session and reload:

   ```bash
   gc slack bind-company-dm --agent ollie --session ollie   # per-agent singleton
   kill -HUP <adapter-pid>                                   # or restart
   ```

   `bind-company-dm` writes/updates one `{agent, session[, city]}` row in
   `dm_bindings.json` (singleton per agent; omit `--city` for the
   adapter's own city). To unbind — including clearing a **stale row**
   left by an agent rename so the redrive-recovery flow can rebind the
   session — use `gc slack bind-company-dm --remove <agent>` (the agent
   need not still be in the directory), then `kill -HUP <adapter-pid>`.

   DM the app as a human: you should see 👀 → the session's reply → ✅ in
   the DM. Pilot **OLLIE only**, prove replay + spoof against the live
   endpoint, then roll the remaining 17.

**Ordering recap (the whole fleet depends on it).** Deploy the Phase 4
adapter + CLI first; then per app: `register-agent-app` → manifest
Request URL → reinstall → Delayed Events → `dm_bindings` import → SIGHUP.
`register-agent-app` must precede the Request URL (the save-time handshake
verifies via the trial path), and the DM binding must precede live traffic
(an unbound agent records a `failed_dm_unbound` target, drained later with
`gc slack company-redrive`).

**App Home Messages tab.** The template enables it
(`app_home.messages_tab_enabled: true`,
`messages_tab_read_only_enabled: false`): it is a hard Slack prerequisite
for a human to DM a bot, so the per-agent DM phase is dead-on-arrival
without it. Leave it on.

### Group DMs (Phase 4b)

The **group-DM phase** lets a human wake agents by mentioning them in a
multi-party DM (mpim). Wake policy is **mention-only**: a human message
that natively mentions member agents wakes exactly the mentioned, homed
agents (via `dm_bindings`); an unmentioned or bot-authored message wakes
nobody (terminal `mpim_no_mention` / `mpim_bot_author`). There is no
ambient set and no delegation authority from an mpim root. The template
adds:

- `oauth_config.scopes.bot`: **`mpim:history`** (read the group-DM channel
  for hydration and the membership probe).
- `settings.event_subscriptions.bot_events`: **`message.mpim`**, pointed
  at the same public funnel `request_url` as `message.im`.

The switchboard already subscribes to `message.mpim`; the adapter absorbs
switchboard-signed mpim events (200, no receipt, no legacy dispatch), so
**no switchboard manifest change is needed.**

**Rollout ordering — the adapter deploy MUST precede the manifest update
(spec §Rollout).** Until the mpim-aware adapter is deployed, a member
app's `message.mpim` event falls into the legacy dispatch path,
N-plicated once per member app. So:

0. **Deploy the adapter's mpim branch FIRST.** Do not add `mpim:history` /
   `message.mpim` to any agent app before the adapter that owns mpim
   admission is live.
1. **Capture the live mpim wire fixture** (one app, one test group) so the
   mention extractor is validated against real `message.mpim` rich_text
   synthesis before the fleet trusts it (the committed fixture is marked
   replaced with a LIVE capture 2026-07-19 — rich_text mention synthesis confirmed in mpim).
2. **Bulk manifest update via the config token**: add `mpim:history` +
   `message.mpim` to all 18 agent apps; each reinstall activates the
   scope. (No new `register-agent-app` or `dm_bindings` step — mpim reuses
   the per-agent registration and DM bindings from Phase 4.)
3. **Pilot** the existing ollie + olivia group DM: mention each in turn
   (exclusive wakes), both (two targets on one receipt), an unmentioned
   message (no wake), and confirm the non-winner agent's reply passes its
   own guard.
4. **Fleet-wide.**

An agent that is mentioned but not actually in the group records a
`failed_mpim_not_member` target (the membership probe), and one that has
no `dm_bindings` row records `failed_dm_unbound`; both drain later with
`gc slack company-redrive` once the agent is invited / bound.

### Switchboard changes for company rooms

Enabling company rooms also touches the switchboard app (`app.json`),
and both changes require operator follow-up:

- **Scope additions require a reinstall.** `app.json` now declares
  `channels:read`, `groups:read` (per-room membership checks) and
  `users:read` (`bots.info` author resolution). These are read-only API
  scopes with **no** paired event subscription, so they do not change
  the switchboard's `bot_events`. Adding scopes to an installed app is
  not live until you **reinstall** the switchboard app to the workspace
  (re-mint the bot token afterward).
- **Enable Delayed Events on the switchboard app.** This is a required
  operator step, not optional: it extends Slack's event redelivery to
  hourly retries for 24 hours, which is what makes a receipt-store
  outage recoverable rather than lossy (see `docs/company-rooms.md` →
  Durable Admission).

## Pairing with `sync-commands` (gc-cby.2)

When `gc slack sync-commands` arrives, it will treat this `app.json` as
the desired state and reconcile Slack's live app definition against it
— adding new slash commands, updating descriptions, and removing
deprecated entries. Edits to slash commands SHOULD land here first;
running `sync-commands` propagates them to Slack.

## Validating local edits

```bash
python3 -c "import json; json.load(open('examples/slack-pack/manifest/app.json'))"
```

The pytest suite in `examples/slack-pack/tests/test_manifest.py`
asserts the file parses + carries the required top-level keys; run it
with `pytest examples/slack-pack/tests/test_manifest.py`.
