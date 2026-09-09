Reply into the conversation of the latest inbound message delivered to this
session.

The adapter remembers, per session, the last Slack message it bridged in.
`reply-current` posts back into that same channel — the natural "answer the
thing that just pinged me" verb. With `--thread-current` it threads the
reply under that message; with `--reply-to <ts>` it threads under a specific
ts; with neither it posts unthreaded into the channel.

If the adapter has seen no inbound for this session (e.g. just restarted),
it falls back to the session's single channel binding. If there is neither a
recent inbound nor a single binding, use `publish-to-channel`.

Usage:
  gc slack-channel reply-current [--session <id>]
                                 (--body <text> | --body-file <path>)
                                 [--thread-current | --reply-to <ts>]
                                 [--conversation-id <id>]
                                 [--idempotency-key <key>]

Flags:
  --session         Override the session id (default: $GC_SESSION_ID).
  --body            Message text. Mutually exclusive with --body-file.
  --body-file       Read the message body from a file (must be a regular file).
  --thread-current  Thread under the latest inbound message. Mutually
                    exclusive with --reply-to.
  --conversation-id Assert which conversation the reply is for. Optional and
                    redundant — the target is always the session's latest
                    inbound — but if given it must match, so a stale reply
                    fails loudly instead of landing in the wrong channel.
                    Accepted for parity with slack-full and discord, whose
                    reply-current take the same flag; gc's injected reply
                    instruction passes it.
  --reply-to        Slack message ts to thread under.
  --idempotency-key Caller-supplied key to dedupe retries. When omitted, the
                    adapter derives a deterministic key from the resolved
                    session, channel, thread anchor and body, so a retry of
                    the same reply replays the original receipt instead of
                    posting a duplicate after a delivered-but-timed-out POST.

Examples:
  gc slack-channel reply-current --body "on it" --thread-current
  gc slack-channel reply-current --body "see PR #42"

On success, prints the adapter's JSON receipt {"ok":true,"ts":...}.

Requires: GC_CITY_NAME + GC_API_BASE_URL, plus `jq` and `curl` on PATH. The
slack-channel service must be running.
