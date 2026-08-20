Delegate the current company-room turn to a peer agent.

Posts `<@peer> <your body>` into the human root's thread as *your own*
identity app, carrying a content-addressed nonce in Slack message metadata
so the peer's result can be correlated back to a durable delegation record.
The body is entity-escaped (`&`, `<`, `>`); the service-constructed
`<@peer>` is the only live mention — bare `@channel` / `@here` / `#channel`
text stays inert.

Context is resolved from the immutable turn record named by `--turn-ref` in
the authenticated Slack reminder: the room, team/channel, human root thread,
and your acting agent all come from that exact delivery even if another room
wakes the same session while you work. `$GC_SESSION_NAME` must be set (hard
error otherwise). Legacy pre-rollout pointers remain readable; post-rollout
turns fail closed when `--turn-ref` is omitted.

Durability and safety:

- A posting intent is persisted before the provider POST
  (`prepared → posting → published | failed | expired`, bounded attempts,
  120s retry deadline, 24h TTL). A crash mid-post is recovered lazily on the
  next company verb by scanning ingress receipts for the nonce — never by
  reposting (`chat.postMessage` is not idempotent).
- At most one pending delegation is allowed per
  `(team, channel, thread_root, responder, requester)`, enforced under a
  cross-process lock. A TTL-expired pending delegation counts as
  not-pending.
- On success the delegation record is materialized before the command
  reports `posted_ts`. A definitive provider rejection fails the intent; a
  timeout/5xx parks it for automatic recovery (reported as `parked`).

Flags
-----

  --to <agent>         Target agent name (directory slug). Required.
  --body <text>        Delegation body (or use --body-file).
  --body-file <path>   Read the body from a file.
  --cancel             Expire your own pending delegation to --to (recover a
                       wedged tuple without waiting out the TTL).
  --origin-ts <ts>     Pin a specific turn when a newer wake overwrote the
                       pointer.
  --turn-ref <ref>     Immutable turn reference from the Slack reminder.

Examples
--------

  gc slack delegate --turn-ref gct-0123456789abcdef0123 --to riley --body "please review PR 42"
  gc slack delegate --turn-ref gct-0123456789abcdef0123 --to riley --body-file /tmp/ask.txt
  gc slack delegate --turn-ref gct-0123456789abcdef0123 --cancel --to riley

Routes to: scripts/slack_company_outbound.py delegate

Formatting guard: tildes that would accidentally pair into Slack
strikethrough (e.g. "~$58.5k … ~$16.5k" — tilde as "approximately") are
neutralized by default with a visually identical substitute (U+223C).
Deliberate tight-wrapped `~word~` strikethrough, code spans, lone
tildes, and every other formatting character pass through untouched.
Pass --raw to send the body byte-for-byte verbatim.
