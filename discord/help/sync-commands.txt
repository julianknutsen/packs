Register or replace the guild-scoped `/gc` command for one or more Discord
guilds.

Examples:
  gc discord sync-commands 123456789012345678
  gc discord sync-commands 123456789012345678 223456789012345678

Arguments:
  <guild_id>...   one or more Discord guild ids

The command payload is intentionally small in this slice:

- `/gc fix` opens a modal for summary and additional context
- a fallback `prompt` option is also registered for clients that skip the
  modal round trip
- role, guild, and channel policy is enforced by the workspace service after
  Discord delivers the slash command
