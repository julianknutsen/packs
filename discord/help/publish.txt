Publish a human-visible Discord message through a saved chat binding.

For agent replies to the current Discord turn, prefer:
  gc discord reply-current --body-file ./reply.txt

Examples:
  gc discord publish --binding room:123456789012345678 --body "hello humans"
  gc discord publish --binding room:123456789012345678 --trigger 223456789012345678 --body-file ./reply.txt
  gc discord publish --binding room:123456789012345678 --conversation-id 323456789012345678 --trigger 223456789012345678 --body "Reply in thread"
  gc discord publish --binding launch-room:123456789012345678 --source-event-kind discord_human_message --source-ingress-receipt-id in-223456789012345678 --body-file ./reply.txt
  gc discord publish --binding room:123456789012345678 --source-event-kind discord_human_message --source-ingress-receipt-id in-223456789012345678 --source-session corp--sky --body-file ./reply.txt

`--conversation-id` overrides the destination channel or thread for this send.
Use it when the saved room binding is the parent channel but the inbound message
arrived from a thread.

`--reply-to` overrides the message id used for Discord reply threading. If
omitted, `--trigger` is used as the reply target when present.

Direct `publish` is primarily for operator-controlled sends or cross-binding
publishes. Peer fanout only participates when you also supply source metadata
such as `--source-event-kind` plus `--source-ingress-receipt-id` or
`--root-ingress-receipt-id`. For agent replies to the latest Discord turn,
prefer `gc discord reply-current --body-file ...`.

Launcher-backed replies normally should use `reply-current`, not direct
`publish`. When source context includes a room-launch id, the bridge will
create the managed Discord thread on first publish and then post into that
thread.

For multi-line or generated content, prefer `--body-file` over inline `--body`.
Do not pipe publish output through filters that can hide failures. Treat a
publish as successful only when the returned JSON contains `record.remote_message_id`.

If the command exits with status `2`, the Discord post succeeded but peer
fanout for other bound sessions was partial or needs operator attention.
Inspect `record.peer_delivery` and use `gc discord retry-peer-fanout` if needed.
