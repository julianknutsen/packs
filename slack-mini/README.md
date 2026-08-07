# slack-mini

**Talk to your Gas City mayor from Slack.** Add a bot to your workspace,
`@`-mention it from any channel, and the mention bridges to your gc mayor
session. Reply back with one verb.

slack-mini is **Tier 1** of the Slack pack family — the smallest surface
that gets a human talking to gc over Slack. It ships a single-file adapter
and one outbound verb. No channel bindings, no per-session identity, no
multi-rig routing — those live in `slack-channel` (Tier 2) and `slack-full`
(Tier 3). See the [slack-pack tiering design memo](../docs/design/slack-pack-tiering.md)
(landing separately).

> Pick exactly one Slack tier per city. The tiers are alternatives, not
> layers you stack.

## What you get

- **Inbound:** `@gc-mayor what's the convoy status?` in any channel the bot
  is in → delivered to your gc mayor session.
- **Outbound:** `gc slack-mini post-message --channel C0123 --text "…"` →
  posts to a channel, optionally in a thread.

## Choose a transport

Inbound mentions reach the adapter one of two ways. Pick one before you
create the app — the choice is baked into the app manifest.

| | **Socket Mode** | **HTTP (Events API)** |
| --- | --- | --- |
| Public ingress | none | public HTTPS URL required |
| How events arrive | outbound WebSocket the adapter opens to Slack | Slack POSTs to your Request URL |
| Credentials | app-level token (`xapp-…`) | signing secret |
| Manifest | [`manifest/app-socket.json`](./manifest/app-socket.json) | [`manifest/app.json`](./manifest/app.json) |

**Socket Mode is the right default if you cannot expose an inbound port** —
behind a corporate firewall, on a laptop, or anywhere a tunnel is blocked.
It needs only outbound HTTPS to `slack.com`. Choose HTTP when you already
have public ingress and would rather not manage another token.

Everything downstream is identical: same mention handling, same bridge into
gc, same outbound verb.

## Install in 3 minutes

### 1. Create the Slack app (1 min)

Create an app from the shipped manifest — `manifest/app-socket.json` for
Socket Mode, `manifest/app.json` for HTTP:

1. <https://api.slack.com/apps> → **Create New App** → **From a manifest**.
2. Pick your workspace, paste the manifest, create.
3. **Install to Workspace** and copy the **Bot User OAuth Token**
   (`xoxb-…`).
4. From **Basic Information**:
   - **Socket Mode:** under **App-Level Tokens**, **Generate Token and
     Scopes**, add the **`connections:write`** scope, and copy the token
     (`xapp-…`). App-level tokens cannot be declared in a manifest, so this
     step is manual.
   - **HTTP:** copy the **Signing Secret**.

Both manifests request only three bot scopes — `app_mentions:read`,
`chat:write`, `chat:write.public` — and subscribe to the `app_mention`
event. They differ only in `socket_mode_enabled`.

### 2. Configure the city (1 min)

Import the pack in your city's `pack.toml`:

```toml
[imports.slack-mini]
source = "../packs/slack-mini"
```

Provide the adapter's environment (e.g. in your city service env or
`~/.config/gc-slack-mini-adapter/env`):

**Socket Mode:**

```sh
SLACK_BOT_TOKEN=xoxb-...          # from step 1
SLACK_APP_TOKEN=xapp-...          # from step 1 — selects Socket Mode
SLACK_WORKSPACE_ID=T0123ABCD      # your Slack team id
GC_CITY_NAME=<your-city-name>     # the gc city to bridge into
```

**HTTP:**

```sh
SLACK_BOT_TOKEN=xoxb-...          # from step 1
SLACK_SIGNING_SECRET=...          # from step 1
SLACK_WORKSPACE_ID=T0123ABCD      # your Slack team id
GC_CITY_NAME=<your-city-name>     # the gc city to bridge into
```

That's the full required set either way — four variables. Setting
`SLACK_APP_TOKEN` is the whole switch: the adapter runs Socket Mode, never
binds `LISTEN_PUBLIC`, and stops requiring `SLACK_SIGNING_SECRET` (Socket
Mode has no request signatures to verify).

### 3. Start (1 min)

**Socket Mode — nothing to expose.** The adapter dials out to Slack, so
there is no listener to publish, no TLS to terminate, and no Request URL to
register. Start your city and `@`-mention the bot. The log line to look for
is `socket mode: connected`.

**HTTP — expose the events endpoint.** The adapter listens for Slack events
on public TCP (`LISTEN_PUBLIC`, default `0.0.0.0:8775`). Terminate TLS in
front of it and give Slack a public URL —
[Tailscale Funnel](https://tailscale.com/kb/1223/funnel) is the easy path:

```sh
tailscale funnel 8775
```

Then in the Slack app's **Event Subscriptions**, set the Request URL to
`https://<your-funnel-host>/slack/events`. Slack sends a one-time
`url_verification` challenge, which the adapter answers automatically.

gc supervises the adapter as a `proxy_process` service (named
`slack-mini`); building the binary is a one-time `go build` in `adapter/`
(see [Build](#build)). Start your city and `@`-mention the bot.

## Replying in a thread

When the mayor session handles an inbound mention, the conversation carries
the Slack message `ts` as its reply-to id. Answer in the same thread with:

```sh
gc slack-mini post-message --channel C0123 \
  --thread-ts 1700000000.0001 --text "on it — see PR #42"
```

## Build

The adapter's only dependency is
[`github.com/coder/websocket`](https://github.com/coder/websocket) (itself
dependency-free), used for the Socket Mode transport:

```sh
cd adapter
go build -o gc-slack-mini-adapter ./...
```

The built binary is git-ignored; the `[[service]]` block runs it in place.

## Configuration reference

| Variable | Required | Default | Purpose |
| --- | :---: | --- | --- |
| `SLACK_BOT_TOKEN` | ✓ | — | Bot token for `chat.postMessage` and the inbound POST. |
| `SLACK_SIGNING_SECRET` | HTTP only | — | HMAC secret for verifying Slack requests. Required unless `SLACK_APP_TOKEN` is set. |
| `SLACK_WORKSPACE_ID` | ✓ | — | Slack team id (the extmsg account id). |
| `GC_CITY_NAME` | ✓ | — | gc city to bridge into. |
| `SLACK_APP_TOKEN` | Socket Mode | — | App-level token (`xapp-…`) with `connections:write`. Setting it selects Socket Mode: no public listener, no signing secret. |
| `LISTEN_PUBLIC` | | `0.0.0.0:8775` | Public bind for `/slack/events`. Not bound in Socket Mode. |
| `LISTEN_INTERNAL` | | `127.0.0.1:8776` | TCP bind for `/post-message` when not run as a gc proxy_process (no `GC_SERVICE_SOCKET`). |
| `REGISTER_ON_START` | | `true` | Self-register as an extmsg adapter on start. |
| `SLACK_MINI_INBOUND_TARGET` | | `mayor` | Session handle inbound mentions address. |
| `SLACK_API_BASE` | | `https://slack.com/api` | Slack web API origin (override for relays/tests). |
| `GC_API_BASE_URL` | | `http://127.0.0.1:9443` | gc API base. |

`GC_SERVICE_SOCKET`, `GC_SERVICE_URL_PREFIX`, and `GC_API_BASE_URL` are
injected by gc when the adapter runs as a `proxy_process` service.

## Socket Mode notes

- **Egress only.** The adapter needs outbound HTTPS to `slack.com` and a
  WebSocket to `wss-*.slack.com`. Nothing inbound.
- **Proxies are honoured.** The connection is dialled through Go's default
  HTTP client, so `HTTPS_PROXY` / `HTTP_PROXY` / `NO_PROXY` apply — useful
  on networks that force egress through a proxy.
- **Reconnects are normal.** Slack recycles connections routinely and warns
  ~10s ahead; the adapter opens a replacement while the old connection
  drains, so mentions are not dropped. Reconnect failures back off from 1s
  to a 30s cap.
- **Redelivery is safe.** Every envelope is acked before bridging, and each
  bridged message carries a `dedup_key` of `slack-<ts>`, so a redelivered
  event does not produce a duplicate in gc.
- **`invalid_auth` on start** almost always means the app-level token is
  missing the `connections:write` scope, or a bot token was pasted into
  `SLACK_APP_TOKEN`.

## Upgrading to a larger tier

To gain channel bindings, per-session identity, or multi-rig routing, swap
this pack for `slack-channel` or `slack-full`. The bot token, signing
secret, and workspace id carry over unchanged — but note that Socket Mode
is currently implemented at Tier 1 only, so the larger tiers still need
public ingress. See the tiering memo's
migration section.
