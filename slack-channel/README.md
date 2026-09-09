# slack-channel

**Bridge Slack channels and DMs to your Gas City sessions.** Bind a channel
to one or more gc sessions and every message there is delivered to them;
reply, publish, and react back; give each session its own Slack identity;
address a session from any channel with `@handle`.

slack-channel is **Tier 2** of the Slack pack family — the "team channel ↔
session graph" middle tier. It is built on slack-mini's single-file kernel
and adds the state Tier 1 deliberately omits. It does **not** include the
multi-rig routing, slash-command intake, peer fanout, or file upload of
`slack-full` (Tier 3).

> Pick exactly one Slack tier per city. The tiers are alternatives, not
> layers you stack.

## What you get

- **Bound channels:** `bind-room C0123 sess-a sess-b` — every human message
  in `C0123` is delivered to both sessions (not just `@`-mentions).
- **Address by handle:** `@mayor: status?` in any channel routes to the
  session aliased to `mayor`.
- **Reply / publish:** `reply-current` threads under the message that just
  arrived; `publish` posts into a session's bound channel;
  `publish-to-channel` posts anywhere by id.
- **Per-session identity:** each session posts under its own username +
  avatar (`chat:write.customize`).
- **React:** `react --emoji eyes` drops a receipt on the latest inbound.

## Verbs

All verbs are `gc slack-channel <verb>`. Run any with `--help` for full
flags.

| Verb | Purpose |
| --- | --- |
| `bind-dm <channel> <session...>` | Bind a DM to one or more sessions. |
| `bind-room <channel> <session...>` | Bind a channel to one or more sessions. |
| `publish --body <text>` | Post into the current session's bound channel. |
| `publish-to-channel --channel <id> --body <text>` | Post into any channel by id. |
| `reply-current --body <text> [--thread-current]` | Reply into the session's latest inbound conversation. |
| `react [--emoji <name>]` | React on the session's latest inbound message. |
| `identity --as <name> [--avatar-emoji <e>]` | Set the session's Slack username + avatar. |
| `handle-alias --handle <h> --session <id>` | Map `@<h>` to a session. |

## Install

## Choose a transport

Inbound messages reach the adapter one of two ways. Pick one before you
create the app — the choice is baked into the app manifest.

| | **Socket Mode** | **HTTP (Events API)** |
| --- | --- | --- |
| Public ingress | none | public HTTPS URL required |
| How events arrive | outbound WebSocket the adapter opens to Slack | Slack POSTs to your Request URL |
| Credentials | app-level token (`xapp-…`) | signing secret |
| Manifest | [`manifest/app-socket.json`](./manifest/app-socket.json) | [`manifest/app.json`](./manifest/app.json) |
| Interactivity (opt-in) | not supported | `/slack/interactions` |

**Socket Mode is the right default if you cannot expose an inbound port** —
behind a corporate firewall, on a laptop, or anywhere a tunnel is blocked.
It needs only outbound HTTPS to `slack.com`. Bindings, handle aliases,
identities, and every outbound verb behave identically on both.

The one thing Socket Mode cannot carry is interactivity. Tier 2 ships it
disabled, so this only matters if you deliberately turned on
`/slack/interactions` — that half still needs a public URL.

### 1. Create the Slack app

Create an app from the shipped manifest — `manifest/app-socket.json` for
Socket Mode, `manifest/app.json` for HTTP:

1. <https://api.slack.com/apps> → **Create New App** → **From a manifest**.
2. Pick your workspace, paste the manifest, create.
3. **Install to Workspace** and copy the **Bot User OAuth Token** (`xoxb-…`).
4. From **Basic Information**:
   - **Socket Mode:** under **App-Level Tokens**, **Generate Token and
     Scopes**, add the **`connections:write`** scope, and copy the token
     (`xapp-…`). App-level tokens cannot be declared in a manifest, so this
     step is manual.
   - **HTTP:** copy the **Signing Secret**.

The manifest requests the scopes Tier 2 needs: `app_mentions:read`,
`channels:history`, `groups:history`, `im:history`, `mpim:history`,
`chat:write`, `chat:write.public`, `chat:write.customize`,
`reactions:write`; and subscribes to `app_mention` plus `message.*`.

> `chat:write.customize` is what makes per-session `identity` overrides
> visible. Without it, Slack ignores the username/avatar and posts fall
> through under the default bot identity.

### 2. Configure the city

Import the pack in your city's `pack.toml`:

```toml
[imports.slack-channel]
source = "../packs/slack-channel"
```

Provide the adapter's environment:

```sh
SLACK_BOT_TOKEN=xoxb-...          # from step 1
SLACK_APP_TOKEN=xapp-...          # Socket Mode only — selects the transport
SLACK_SIGNING_SECRET=...          # HTTP only
SLACK_WORKSPACE_ID=T0123ABCD      # your Slack team id
GC_CITY_NAME=<your-city-name>     # the gc city to bridge into
GC_CITY_PATH=/path/to/your/city   # on-disk city root (for the registries)
```

Setting `SLACK_APP_TOKEN` is the whole switch: the adapter runs Socket
Mode, never binds `LISTEN_PUBLIC`, and stops requiring
`SLACK_SIGNING_SECRET` (Socket Mode has no request signatures to verify).

`GC_CITY_PATH` is where the three registries live
(`<GC_CITY_PATH>/.gc/slack-channel/`). Override the directory with
`SLACK_CHANNEL_REGISTRY_DIR` if needed.

### 3. Start

**Socket Mode — nothing to expose.** The adapter dials out to Slack, so
there is no listener to publish, no TLS to terminate, and no Request URL to
register. Start your city; the log line to look for is
`socket mode: connected`.

**HTTP — expose the events endpoint.** The adapter listens for Slack events
on public TCP (`LISTEN_PUBLIC`, default `0.0.0.0:8775`). Terminate TLS in
front of it and give Slack a public URL —
[Tailscale Funnel](https://tailscale.com/kb/1223/funnel) is the easy path:

```sh
tailscale funnel 8775
```

Then in **Event Subscriptions**, set the Request URL to
`https://<your-funnel-host>/slack/events`. Slack sends a one-time
`url_verification` challenge, which the adapter answers automatically.

gc supervises the adapter as a `proxy_process` service (named
`slack-channel`); building the binary is a one-time `go build` in `adapter/`
(see [Build](#build)). Start your city.

## Usage walkthrough

```sh
# Bind a channel to the PL and reviewer sessions.
gc slack-channel bind-room C0123 sess-pl sess-reviewer

# Give the PL session its own identity (call once at session start).
gc slack-channel identity --session sess-pl --as "Gas City PL" --avatar-emoji robot_face

# Let humans address the mayor from any channel.
gc slack-channel handle-alias --handle mayor --session sess-mayor

# A human posts in C0123 → both bound sessions receive it. The PL replies
# in-thread:
gc slack-channel reply-current --body "on it — see PR #42" --thread-current

# Drop a receipt on the message that just arrived.
gc slack-channel react --emoji eyes

# Unprompted status post into the bound channel.
gc slack-channel publish --body "nightly build is green"
```

## Registries

The adapter owns three on-disk registries under
`<GC_CITY_PATH>/.gc/slack-channel/`. JSON Schemas live in [`schema/`](./schema):

| File | Schema | Contents |
| --- | --- | --- |
| `channel_mappings.json` | [channel_mappings.schema.json](./schema/channel_mappings.schema.json) | channel → bound sessions |
| `identities.json` | [identities.schema.json](./schema/identities.schema.json) | session → username/avatar |
| `handle_aliases.json` | [handle_aliases.schema.json](./schema/handle_aliases.schema.json) | handle → session |

The adapter is the only writer and reader; the verbs mutate them by POSTing
to the adapter, never by touching the files directly.

## Build

The adapter is a small multi-file Go module with no external dependencies:

```sh
cd adapter
go build -o gc-slack-channel-adapter ./...
go test ./...
```

The built binary is git-ignored; the `[[service]]` block runs it in place.

## Configuration reference

| Variable | Required | Default | Purpose |
| --- | :---: | --- | --- |
| `SLACK_BOT_TOKEN` | ✓ | — | Bot token for `chat.postMessage` + `reactions.add`. |
| `SLACK_SIGNING_SECRET` | HTTP only | — | HMAC secret for verifying Slack requests. Required unless `SLACK_APP_TOKEN` is set. |
| `SLACK_APP_TOKEN` | Socket Mode | — | App-level token (`xapp-…`) with `connections:write`. Setting it selects Socket Mode: no public listener, no signing secret. |
| `SLACK_WORKSPACE_ID` | ✓ | — | Slack team id (extmsg account id + registry key). |
| `GC_CITY_NAME` | ✓ | — | gc city to bridge into. |
| `GC_CITY_PATH` | ✓* | — | City root; registry dir defaults under it. |
| `SLACK_CHANNEL_REGISTRY_DIR` | | `<GC_CITY_PATH>/.gc/slack-channel` | Override the registry directory. |
| `LISTEN_PUBLIC` | | `0.0.0.0:8775` | Public bind for `/slack/events` + `/slack/interactions`. Not bound in Socket Mode. |
| `LISTEN_INTERNAL` | | `127.0.0.1:8776` | TCP bind for the verb endpoints when not a gc proxy_process. |
| `REGISTER_ON_START` | | `true` | Self-register as an extmsg adapter on start. |
| `SLACK_CHANNEL_INBOUND_TARGET` | | `mayor` | Fallback session for an unbound, unaliased `app_mention`. |
| `SLACK_API_BASE` | | `https://slack.com/api` | Slack web API origin (override for relays/tests). |
| `GC_API_BASE_URL` | | `http://127.0.0.1:9443` | gc API base. |

\* Either `GC_CITY_PATH` or `SLACK_CHANNEL_REGISTRY_DIR` must be set so the
registries have a home. `GC_SERVICE_SOCKET`, `GC_SERVICE_URL_PREFIX`, and
`GC_API_BASE_URL` are injected by gc when the adapter runs as a
`proxy_process` service.

### Optional: interactive payloads

To enable the basic modal-button ack, turn on **Interactivity** in the Slack
app and point its Request URL at `https://<your-funnel-host>/slack/interactions`.
The adapter verifies the signature and acks with `200` so Slack dismisses
the interaction; it does no custom modal handling (that is Tier 3).

## Security posture

A few deliberate choices in the shipped manifest and adapter are worth
understanding before you install:

- **`chat:write.public` (manifest scope).** This lets the bot post to any
  *public* channel without first being invited — `publish-to-channel
  --channel <id>` reaches any public channel in the workspace, and a bound
  session can publish to a public channel it was bound to without a join
  step. The blast radius is "every public channel in this workspace." If
  that is broader than you want, drop `chat:write.public` from
  [`manifest/app.json`](./manifest/app.json) and invite the bot to each
  channel it should post in (Slack then requires `conversations.join` /
  manual membership instead).
- **`token_rotation_enabled: false` (manifest setting).** The adapter
  authenticates with a long-lived bot token (`SLACK_BOT_TOKEN`) rather than
  rotating, expiring tokens. Rotation would require the adapter to persist
  and refresh credentials; for a single-workspace Tier-2 bridge the static
  token is simpler and the token never leaves the adapter (verbs never see
  it). Treat the token as a secret, scope it to one workspace, and rotate it
  by hand if it is exposed. Enable rotation in the Slack app settings if your
  deployment policy requires it.
- **Internal verb listener is loopback / dev-only.** When the adapter is
  *not* run as a gc `proxy_process` service (`GC_SERVICE_SOCKET` unset), the
  verb endpoints are served over plain TCP on `127.0.0.1:8776`
  (`LISTEN_INTERNAL`) with no authentication — anything that can reach that
  loopback port can mutate the registries. This mode is for local
  development only. In production gc runs the adapter as a `proxy_process`
  service and the verb endpoints bind a `0600` Unix-domain socket
  (`GC_SERVICE_SOCKET`) that only the gc supervisor can reach; do not expose
  `127.0.0.1:8776` beyond loopback.

## Choosing a tier

- **slack-mini** (Tier 1) — one outbound verb, `app_mention` → mayor. No
  registries.
- **slack-channel** (Tier 2, this pack) — channel bindings, per-session
  identity, handle aliases. Single workspace, single rig.
- **slack-full** (Tier 3) — multi-rig routing, slash commands, peer fanout,
  file upload, modal flows.

The bot token, signing secret, and workspace id carry over unchanged when
you move between tiers.
