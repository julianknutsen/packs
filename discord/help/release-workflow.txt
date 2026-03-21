Release a stuck workflow lock for a Discord conversation.

This is an operator recovery command. It does not touch the bead; it only
clears the intake-side workflow lock so `/gc fix` can be accepted again for the
same channel or thread.

Example:
  gc discord release-workflow 123456789012345678 223456789012345678
  gc discord release-workflow --request-id dc-interaction-fix

Arguments:
  <guild_id>         Discord guild id
  <conversation_id>  Channel id or thread id used for the workflow key

Flags:
  --request-id <id> Release the workflow key recorded on an existing request
  --command <name>  slash command to unlock, default: fix
