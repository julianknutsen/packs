Publish a message into the conversation a session is bound to.

Differs from `reply-current`:
  * target session is required (defaults to the current session if
    omitted, but never falls back to "any session running").
  * always uses the session's saved binding — no inbound-event scan,
    no event-driven fallback chain. The session must have an active
    extmsg binding or this command fails fast.
  * intent is explicit: "send X to the channel session SID is bound
    to."

Use `reply-current` when you want to thread under an inbound that just
arrived. Use `publish` when you have something to say unconditionally.

Examples:
  gc slack publish --session gc-82783 --body "*status:* nightly run done"
  gc slack publish --session gc-83347 --body-file /tmp/digest.md
  gc slack publish --session gc-82783 --body "..." --idempotency-key cron-2026-05-02
  gc slack publish --session gc-82783 --body "diag" --via adapter   # bypass gc

Routes through `/v0/city/<name>/extmsg/outbound` by default so peer
fanout + transcript recording fire. Pass `--via adapter` for
adapter-only diagnostics that bypass gc; peers in a bind-room won't
see those.

Formatting guard: tildes that would accidentally pair into Slack
strikethrough (e.g. "~$58.5k … ~$16.5k" — tilde as "approximately") are
neutralized by default with a visually identical substitute (U+223C).
Deliberate tight-wrapped `~word~` strikethrough, code spans, lone
tildes, and every other formatting character pass through untouched.
Pass --raw to send the body byte-for-byte verbatim.
