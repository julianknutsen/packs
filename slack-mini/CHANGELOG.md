# Changelog

All notable changes to slack-mini are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- Codex review round on this branch (1 P1 + 4 P2, all fixed): the
  decoded mention is spooled synchronously BEFORE the events endpoint
  200-acks Slack (the old ack → async users.info → spool order lost
  acked mentions to a crash in the enrichment window; enrichment and
  delivery stay async, and startup replay now finishes enrichment
  too); spool entries are written atomically (same-dir temp file +
  fsync + rename) so a torn write can no longer dead-letter an acked
  message with no valid payload left; Markdown link destinations and
  bare URLs are converted/protected BEFORE the emphasis passes (a
  destination like `.../pkg/__init__.py` was being rewritten into a
  broken href); `[text](url)` parsing handles balanced parentheses in
  the destination (`.../Function_(mathematics)`); and multi-backtick
  GFM code spans (``…`` with a backtick inside) pass through
  verbatim like single-backtick spans.

### Added

- Inbound mentions resolve the sender's display name via `users.info`
  (cached in-memory for an hour; failures negative-cached for 5 minutes)
  so gc's injected reminder shows a human name instead of a raw user id
  like `U0AN32RPBFT` (hq-fh9). The app manifest now requests the
  `users:read` bot scope — existing installs must re-approve the app for
  name resolution to work; until then the adapter falls back to raw ids.
- The adapter registers a Tier-1 `reply_instructions` template with gc, so
  the inbound nudge advertises the real
  `gc slack-mini post-message --channel … --thread-ts …` verb (with the
  message's thread id filled in) instead of the non-existent
  `gc slack reply-current` (hq-fh9).

### Changed

- `/post-message` converts common GitHub-flavored Markdown (`**bold**`,
  `__bold__`, `~~strike~~`, `[text](url)`, `#`-headings) to Slack mrkdwn
  before `chat.postMessage`; fenced and inline code pass through verbatim
  (hq-fh9).

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
