# Changelog

All notable changes to slack-mini are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Socket Mode transport** (`adapter/socketmode.go`), an ingress-free
  alternative to the public Events API listener: the adapter opens an
  outbound WebSocket to Slack instead of receiving webhooks, so the pack
  runs on networks that cannot accept inbound connections (corporate
  firewalls, laptops, anywhere a tunnel is blocked).
  - Selected by `SLACK_APP_TOKEN` (an `xapp-…` app-level token with the
    `connections:write` scope). When set, `LISTEN_PUBLIC` is never bound
    and `SLACK_SIGNING_SECRET` is no longer required — Socket Mode has no
    request signatures, so the trust boundary is the app-token-authenticated
    connection the adapter itself opens.
  - Supervised reconnect with capped exponential backoff (1s → 30s). On
    Slack's advance disconnect warning a replacement connection is opened
    while the old one drains and keeps acking, so mentions are not dropped.
  - Envelopes are acked before bridging; events from a workspace other than
    `SLACK_WORKSPACE_ID` are dropped rather than filed under the wrong
    account.
  - Dialled through Go's default HTTP client, so `HTTPS_PROXY`/`NO_PROXY`
    are honoured.
- `manifest/app-socket.json`, the Socket Mode variant of the app manifest
  (identical to `manifest/app.json` but with `socket_mode_enabled: true`).

### Changed

- The adapter now depends on `github.com/coder/websocket` v1.8.15, which
  has no dependencies of its own. The HTTP transport is unchanged and still
  verifies request signatures exactly as before.

## [0.1.0] — Tier 1 extraction

Initial release. slack-mini is Tier 1 of the Slack pack family — the
minimum viable Slack→gc surface, extracted from slack-pack per the
slack-pack tiering design memo (`docs/design/slack-pack-tiering.md`,
landing separately; `gc-yrw.1`).

### Added

- Single-file Slack adapter (`adapter/main.go`, module
  `github.com/sjarmak/gc-slack-mini-adapter`):
  - Public Slack Events API receiver at `/slack/events`, HMAC-verified
    with `SLACK_SIGNING_SECRET` and a 5-minute replay window.
  - Handles `app_mention` only; each verified mention is bridged to gc by
    POSTing `/v0/city/{city}/extmsg/inbound`, addressed to the mayor
    session (override with `SLACK_MINI_INBOUND_TARGET`).
  - Outbound `/post-message` endpoint on the gc-proxied UDS, posting plain
    text to Slack via `chat.postMessage` with the workspace bot token.
  - Self-registers as an extmsg adapter on start (`REGISTER_ON_START`).
- `gc slack-mini post-message` verb — a bash wrapper
  (`commands/post-message.sh`) that relays to the adapter through gc's
  `/svc/slack-mini` reverse proxy. No operator CLI binary at this tier.
- Minimal Slack app manifest (`manifest/app.json`): scopes
  `app_mentions:read`, `chat:write`, `chat:write.public`; subscribes only
  to the `app_mention` bot event.
- `pack.toml` declaring the adapter as a `proxy_process` service named
  `slack-mini`.

### Notes

- No on-disk registries at Tier 1 (no channel bindings, per-session
  identity, apps, rig, or room state). Those arrive in slack-channel
  (Tier 2) and slack-full (Tier 3).
- Pick exactly one Slack tier per city.
