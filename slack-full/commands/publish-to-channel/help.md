# gc slack publish-to-channel

Publish a message into a Slack channel by explicit channel id, bypassing
gc's binding lookup. Used by mayor / chief-of-staff to reply into channels
they have no binding for, after receiving a `Slack address-by-handle`
system reminder via the cross-channel `@mayor:` / `@cos:` routing.

## How it differs from `gc slack publish`

`gc slack publish` resolves the target channel from the session's active
binding. If the session has no binding, the call fails fast.

`gc slack publish-to-channel` takes the channel id directly. No binding
required. The session id still flows through so the adapter applies the
matching identity override (so mayor's reply still appears as "Mayor"
with the configured avatar).

## Usage

```
gc slack publish-to-channel --conversation-id <chan-id> \
                            [--thread-ts <ts>] \
                            [--session <sid>] \
                            [--kind room|dm|thread] \
                            (--body "..." | --body-file <path>)
```

## Flags

- `--conversation-id ID` (required) — Slack channel id (`C…`, `G…`, `D…`).
- `--thread-ts TS` — message ts to thread under (optional).
- `--session SID` — session id to attribute the publish to. Defaults
  to `$GC_SESSION_ID`.
- `--kind` — conversation kind for the envelope (`room` default).
- `--idempotency-key KEY` — caller-supplied dedup key (optional).
- `--body STR` / `--body-file PATH` — message body. One required.

## Typical mayor / cos reply flow

After receiving a system reminder like:

```
Slack address-by-handle: @mayor addressed you from channel C0B1NSK4N3T (Slack ts 1234.5678) by user U0B1N5KD6HF.
```

Reply with:

```bash
tmpfile=$(mktemp); cat > "$tmpfile" <<EOF
ack — looking now
EOF
gc slack publish-to-channel \
  --conversation-id C0B1NSK4N3T \
  --thread-ts 1234.5678 \
  --body-file "$tmpfile"
```

The reply threads under the human's message and posts as your
registered Slack identity.

Formatting guard: tildes that would accidentally pair into Slack
strikethrough (e.g. "~$58.5k … ~$16.5k" — tilde as "approximately") are
neutralized by default with a visually identical substitute (U+223C).
Deliberate tight-wrapped `~word~` strikethrough, code spans, lone
tildes, and every other formatting character pass through untouched.
Pass --raw to send the body byte-for-byte verbatim.
