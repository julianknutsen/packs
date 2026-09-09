Bind a Discord channel or thread to one or more named sessions.

Examples:
  gc discord bind-room 123456789012345678 sky lawrence
  gc discord bind-room --guild-id 223456789012345678 123456789012345678 sky lawrence
  gc discord bind-room --guild-id 223456789012345678 --enable-ambient-read 123456789012345678 sky lawrence
  gc discord bind-room --guild-id 223456789012345678 --enable-ambient-read --allow-untargeted-ambient-delivery 123456789012345678 randy
  gc discord bind-room --guild-id 223456789012345678 --enable-peer-fanout 123456789012345678 corp--sky corp--priya
  gc discord bind-room --guild-id 223456789012345678 --enable-peer-fanout --allow-untargeted-peer-fanout 123456789012345678 corp--sky corp--priya
  gc discord bind-room --app ollie --guild-id 223456789012345678 123456789012345678 teams.lead
  gc discord bind-room --app olivia --guild-id 223456789012345678 123456789012345678 teams.pm

This stores the binding under `.gc/services/discord/data/config.json`.
Use exact permanent session names.
`--app <name>` binds the room through that named bot. Multiple named apps can
bind the same room independently; each receives and replies through its own
Discord identity.
Direct `bind-room` routing is mutually exclusive with `gc discord enable-room-launch`
for the same room.

Ambient read is disabled by default. When enabled, messages in this bound room
or bound thread no longer need to mention the bot, but they still must
explicitly target one or more `@session_name` values to route. Parent-room
thread inheritance still requires a bot mention unless the thread itself is
also bound. Ambient-read bindings remain targeted-only even when the bot is
mentioned directly, unless `--allow-untargeted-ambient-delivery` is enabled on
an ambient-read room with exactly one bound session. In that sticky single-agent
mode, every visible message routes to the one bound session, including messages
that contain a non-exact shorthand mention.

Ambient read consumes unmentioned guild messages. Discord therefore requires
the app's `Message Content Intent` to be enabled in the Developer Portal
before ambient-read routing will work reliably.

Peer fanout is disabled by default. When enabled, the bridge can reinject one
session's room publish to other bound sessions as `discord_peer_publication`
events without re-reading bot messages from Discord.

Peer-fanout-enabled room bindings require lowercase canonical session names.
Useful flags:

- `--enable-ambient-read` / `--disable-ambient-read`
- `--allow-untargeted-ambient-delivery` / `--disallow-untargeted-ambient-delivery`
- `--enable-peer-fanout` / `--disable-peer-fanout`
- `--allow-untargeted-peer-fanout` / `--disallow-untargeted-peer-fanout`
- `--max-peer-triggered-publishes-per-root N`
- `--max-total-peer-deliveries-per-root N`
- `--max-peer-triggered-publishes-per-session-per-minute N`

Bot-authored inbound messages are ignored by default (`reason=bot_message`). A room
binding can opt in so other operators' agents (e.g. a shared guild of mayor bots) can
reach the bound session like a human author would — normal targeting rules still apply
(mention-only rooms still require this bot's @mention):

- `--allow-bot-author USER_ID` — accept this bot author (repeatable; additive)
- `--remove-bot-author USER_ID` / `--clear-bot-authors`
- `--enable-guild-bot-authors` / `--disable-guild-bot-authors` — wildcard: accept any
  bot able to post in the bound channel. The trust boundary is guild membership: only
  bots your guild admins admitted can post there, and a newly admitted bot needs no
  per-binding config. Use only in private, membership-controlled guilds.
- `--max-accepted-bot-author-messages-per-minute N` — anti-loop cap per (binding,
  author); default 6, `0` disables acceptance. Over-limit messages are ignored with
  `reason=bot_author_rate_limited`. The gateway's own messages are never accepted.

Example — trust all mayor bots in a private ops guild:
  gc discord bind-room --guild-id 2234... --enable-guild-bot-authors 1234... mayor
