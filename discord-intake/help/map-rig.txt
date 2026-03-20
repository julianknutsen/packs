Map a Discord guild rig name to a workflow dispatch target.

Example:
  gc discord-intake map-rig 123456789012345678 mission-control mission-control/polecat \
    --fix-formula mol-discord-fix-issue

Arguments:
  <guild_id>    Discord guild id
  <rig_name>    Rig name as used in /gc fix <rig>
  <target>      rig/pool sling target

Flags:
  --fix-formula <name>  formula to use for `/gc fix`, default: mol-discord-fix-issue

Users type `/gc fix mission-control "summary"` in any channel. The rig
parameter routes the request to the configured target regardless of which
channel the command is invoked from.
