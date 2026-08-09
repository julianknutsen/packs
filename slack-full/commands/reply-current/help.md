Reply to the latest Slack inbound event seen by the current session.

The session's recent transcript is scanned for the most recent
`extmsg.inbound` system-reminder. The reply is published through the
local Slack adapter's /publish endpoint to the same conversation.

When that latest inbound was a thread reply, the reply inherits its
thread_ts and lands in the same thread — including when
--conversation-id names the same conversation explicitly. An
unthreaded inbound keeps the channel-level reply. Use --no-thread to
force a channel-level post, or --reply-to <ts> to anchor elsewhere.

Examples:
  gc slack reply-current --body "ack"
  gc slack reply-current --body-file /tmp/reply.txt
  gc slack reply-current --turn-ref gct-0123456789abcdef0123 --body-file /tmp/reply.txt
  gc slack reply-current --conversation-id D0B0TTS550F --body "explicit channel"
  gc slack reply-current --body "top-level on purpose" --no-thread

If the session has no inbound history, --conversation-id is required.

Company reminders include an immutable `turn_ref` and an exact command. Copy
that command: `--turn-ref` binds the reply to the originating channel/thread
even if a newer message in another channel wakes the same agent session.
Post-rollout company turns fail closed when the flag is omitted.

Formatting guard: tildes that would accidentally pair into Slack
strikethrough (e.g. "~$58.5k … ~$16.5k" — tilde as "approximately") are
neutralized by default with a visually identical substitute (U+223C).
Deliberate tight-wrapped `~word~` strikethrough, code spans, lone
tildes, and every other formatting character pass through untouched.
"Lone" counts per line, though: two home-relative paths on one line
(`rsync ~/a ~/b`) can pair, so both are substituted — put twin paths in
a code span, or pass --raw to send the body byte-for-byte verbatim.
