Reply to the latest Discord event seen by the current session.

This is the safest agent-facing Discord reply path. It resolves the latest
`<discord-event>` from the current session transcript, reuses its
`publish_binding_id`, `publish_conversation_id`, and reply threading metadata,
then publishes the provided body back to Discord.

For launcher-backed root-room turns, the first successful `reply-current`
automatically creates the Discord thread before posting the message. The agent
does not need to create or target that thread manually.

Examples:
  gc discord reply-current --body-file ./reply.txt
  gc discord reply-current --session corp--sky --body-file ./reply.txt

Prefer `--body-file` for agent replies. It avoids fragile shell quoting and
makes multi-line responses safe.

If you use `--session`, the override is treated as the source session identity
for peer-fanout attribution as well as transcript lookup.

In peer-fanout-enabled rooms, replying to a `discord_peer_publication` with an
exact `@session_name` mention can route that publication to another bound
session. Untargeted peer-triggered replies stay human-visible only.

If the command exits with status `2`, the Discord reply was posted but peer
fanout was only partially delivered. Inspect `record.peer_delivery` and ask an
operator to run `gc discord retry-peer-fanout <publish-id>` if repair is needed.
