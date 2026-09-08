# gc-slack-adapter setup

End-to-end walkthrough to get a Gas City session talking to a Slack
workspace via DMs (or rooms) — outbound posts from the session land in
Slack, inbound human replies route back to the bound session.
Estimated time: ~20 min of clicking + a few one-line commands.

## Architecture: two listeners, only one Funneled

The adapter binds **two separate ports** by design:

- **`:8765` — public listener** — serves only `/slack/events` (HMAC-verified)
  and `/healthz`. This is the one Tailscale Funnel exposes to the public
  internet.
- **`127.0.0.1:8766` — internal listener** — serves only `/publish`.
  Bound to localhost so it is **physically unreachable from outside the
  machine**. gc reaches it via the loopback interface.

Why this split: gc and Slack are different trust zones. The Slack endpoint
is authenticated by signing secret. The publish endpoint is authenticated
by network locality (only local processes can reach localhost). If both
endpoints lived on the public port, anyone on the internet who guessed
the URL could POST publish requests and make your bot say arbitrary things.

> **Port-collision override.** If `:8765` or `:8766` is already in use
> on your host, point the adapter at free ports via env vars:
>
>   - `LISTEN_PUBLIC=:8775`              # Funnel forwards to this
>   - `LISTEN_INTERNAL=127.0.0.1:8776`
>   - `INTERNAL_CALLBACK_URL=http://127.0.0.1:8776`
>
> Wherever this guide references `8765`/`8766`, substitute the
> overridden values. The defaults work unchanged on hosts where the
> documented ports are free.

## Step 1 — Tailscale Funnel public URL

You need a stable public HTTPS URL Slack can POST to. Tailscale Funnel
gives you one for free.

```bash
# Confirm Funnel is enabled for your tailnet
tailscale funnel status

# Expose ONLY the public listener port (:8765) — NOT 8766
tailscale funnel --bg --https=443 8765
```

Tailscale prints the public URL — something like
`https://<machine>.<tailnet>.ts.net`. **Copy this URL — you need it for
the Slack app's Event Subscriptions Request URL.**

Verify isolation: the publish endpoint is NOT exposed:

```bash
# This should return 404 (publish is not on the public listener):
curl -s -o /dev/null -w "%{http_code}\n" \
  -X POST https://<your-tailnet>.ts.net/publish

# This should return ok (proves the public listener works):
curl -s https://<your-tailnet>.ts.net/healthz
```

## Step 2 — Create the Slack app

1. Go to https://api.slack.com/apps → **Create New App** → **From scratch**.
2. Name it `gc-oversight` (or whatever). Pick your personal workspace.
3. In the left sidebar:

   **OAuth & Permissions** →
   - Bot Token Scopes — add:
     - `chat:write` (post messages)
     - `im:history` (read DM history for inbound replies **and** to verify
       outbound ones — see below)
     - `im:read` (open DM channel)
     - `users:read` (resolve display names — optional but useful)

     A history scope is required for every conversation kind you bind, not
     just DMs: `/publish` confirms each post by reading it back, so a token
     without history for a bound channel reports every publish as
     `Delivered:false` / `readback_auth` even though the message is visible in
     Slack. Add `channels:history` (public channels), `groups:history`
     (private channels) and `mpim:history` (group DMs) when you bind rooms.
     `slack-full/schema/apps.schema.json` is the authoritative list for
     manifest-driven installs.
   - Click **Install to Workspace** at the top, approve.
   - Copy the **Bot User OAuth Token** (`xoxb-...`) — you need it.

   **Basic Information** →
   - Scroll to **App Credentials**.
   - Copy the **Signing Secret** — you need it.
   - Note the **Team ID** under App Credentials (looks like
     `T0XXXXXXXXX`) — that's your `SLACK_WORKSPACE_ID`.

   **Event Subscriptions** →
   - Toggle **Enable Events** → ON.
   - Request URL: paste
     `https://<your-tailnet>.ts.net/slack/events`
     (Slack will verify it; the adapter handles the challenge
     automatically. The adapter must be running for this to succeed —
     start it after Step 4 and come back here.)
   - **Subscribe to bot events** — add:
     - `message.im` (DMs to your bot)
   - Save changes.

   **App Home** →
   - Show Tabs → enable **Messages Tab**.
   - **Allow users to send Slash commands and messages from the messages
     tab** → ON.

## Step 3 — Find your DM channel ID with the bot

After installing the app, open Slack, click on the app's name in the
sidebar to start a DM conversation. Send any message to it (e.g.
"hello"). Then in your terminal:

```bash
curl -sS -H "Authorization: Bearer xoxb-YOUR-TOKEN" \
  "https://slack.com/api/conversations.list?types=im&limit=200" \
  | jq -r '.channels[] | [.id, .user] | @tsv'
```

This lists DM channel IDs and the user IDs they're with. Find the one
where the user is your own user ID (you can find it via:
`curl -sS -H "Authorization: Bearer xoxb-YOUR-TOKEN" https://slack.com/api/auth.test | jq`).

**Copy the DM channel ID** (looks like `D0XXXXXXXXX`) — you need it
for the bind step below.

## Step 4 — Configure and start the adapter

Create the env file (`$XDG_CONFIG_HOME` defaults to `$HOME/.config`):

```bash
mkdir -p "${XDG_CONFIG_HOME:-$HOME/.config}/gc-slack-adapter"
cat > "${XDG_CONFIG_HOME:-$HOME/.config}/gc-slack-adapter/env" <<'EOF'
SLACK_WORKSPACE_ID=T0XXXXXXXXX
SLACK_BOT_TOKEN=xoxb-YOUR-BOT-TOKEN-HERE
SLACK_SIGNING_SECRET=YOUR-SIGNING-SECRET-HERE
GC_CITY_NAME=<your-city-name>

# Defaults shown — only override if you have a port conflict.
# LISTEN_PUBLIC=:8765                      # Funnel exposes this
# LISTEN_INTERNAL=127.0.0.1:8766           # gc-only, localhost
# INTERNAL_CALLBACK_URL=http://127.0.0.1:8766

# Optional override; defaults to http://127.0.0.1:9443 (no /v0/... suffix).
# Under proxy_process supervision you don't set this — but you do need
# it sourced before `gc start` so the supervisor inherits it for the
# spawned adapter (it is NOT auto-injected by the controller).
# GC_API_BASE_URL=http://127.0.0.1:8372

# Bound goroutine fan-out on inbound dispatch paths (slash-command,
# slack-event, alias-resolved). Each in-flight dispatch holds an
# http.Client with a 10s timeout, so unbounded fan-out scales memory
# and FD pressure with traffic. Default 50; raise for high-traffic
# workspaces, lower to harden against bursts. Must be a positive
# integer; 0/negative/non-numeric values fail at startup. sec-S-04.
# SLACK_DISPATCH_CONCURRENCY=50
EOF
chmod 600 "${XDG_CONFIG_HOME:-$HOME/.config}/gc-slack-adapter/env"
```

Note: `PUBLIC_URL` is no longer needed by the adapter itself (we register
with the internal callback URL). You only need the public URL to plug
into the Slack app's Event Subscriptions config.

Run the adapter (replace `<gascity-repo>` with your local checkout
path):

```bash
cd <gascity-repo>/examples/slack-pack/adapter
./run.sh
```

You should see:

```
starting gc-slack-adapter public=:8765 internal=127.0.0.1:8766 gc=http://127.0.0.1:8372 city=<your-city-name>
registered with gc as provider=slack account=T0XXXXXXXXX callback=http://127.0.0.1:8766/publish (LOCALHOST ONLY)
public listener serving on :8765 (Slack events)
internal listener serving on 127.0.0.1:8766 (gc publish only)
```

If registration fails, check that `gc supervisor` is running and the
city name matches.

Now go back to **Step 2 → Event Subscriptions** and click **Verify** on
the Request URL — Slack will POST a challenge, the adapter will respond,
and Slack should show ✓ Verified.

## Step 5 — Bind a session to your Slack DM

The session that should receive Slack DMs needs to be created and
bound to the DM channel ID from Step 3. Names are placeholders below;
substitute the names your consumer pack actually uses (the
`oversight-rig` example pack uses `oversight-rig.chief-of-staff`
under the alias `cos`).

```bash
CITY_DIR=<your-city-dir>     # e.g. ~/gas-city
CITY_NAME=<your-city-name>   # the directory's basename, by default

# Create the session
gc --city "$CITY_DIR" session new <pack>.<role> \
  --no-attach --alias <short-alias> --title "<role> (slack)"
# Output: Session gc-XXXXX created

# Capture the session ID
SID=$(gc --city "$CITY_DIR" session list --json \
  | jq -r '.[] | select(.alias == "<short-alias>") | .id')
echo "Session: $SID"

# Bind to Slack DM
curl -sS -X POST "${GC_API_BASE_URL:-http://127.0.0.1:8372}/v0/city/${CITY_NAME}/extmsg/bind" \
  -H 'Content-Type: application/json' \
  -d "$(jq -n \
    --arg sid "$SID" \
    --arg acct "$SLACK_WORKSPACE_ID" \
    --arg chan "D0XXXXXXXXX" \
    '{session_id: $sid, conversation: {provider: "slack", account_id: $acct, id: $chan}}')"
```

Replace `D0XXXXXXXXX` with your DM channel ID from Step 3. For
multi-session rooms with peer fanout, see `gc slack bind-room` in the
slack-pack README instead.

## Step 6 — Wire env vars for any consumer pack scripts

If your consumer pack has scripts that need to know the bound session
id and the API endpoint, export the relevant vars wherever the gc
supervisor inherits its env from (`~/.profile`, `~/.bashrc`, or a
systemd `EnvironmentFile=`):

```bash
export GC_API_BASE_URL=http://127.0.0.1:8372
export GC_CITY_NAME=<your-city-name>
# Consumer-pack-specific session id captured in Step 5:
export GC_OVERSIGHT_SESSION_ID="$SID"
export GC_PACK_DIR=<gascity-repo>/examples/<your-consumer-pack>
```

Restart the supervisor so the new env propagates.

## Step 7 — Test end-to-end

From inside any session whose DM is bound, post and watch for the
roundtrip:

```bash
gc session attach $SID
# Inside the session:
gc slack reply-current --body 'hello from <pack>.<role>'
```

You should see:
- The adapter logs a `publish:` line
- A Slack DM appears in your channel (under the session's identity if
  one is registered via `gc slack identity`, otherwise the default bot
  identity)

Reply to the Slack message with "ack". You should see:
- Slack POSTs to `/slack/events`
- The adapter logs an `inbound:` line
- The bound session receives the message (visible via
  `gc session peek $SID`)

## Running the adapter as a service

For always-on, install as a systemd user service (replace
`<gascity-repo>` with your checkout path):

```bash
mkdir -p ~/.config/systemd/user
cat > ~/.config/systemd/user/gc-slack-adapter.service <<EOF
[Unit]
Description=gc Slack adapter
After=network-online.target

[Service]
Type=simple
EnvironmentFile=-${XDG_CONFIG_HOME:-$HOME/.config}/gc-slack-adapter/env
ExecStart=<gascity-repo>/examples/slack-pack/adapter/run.sh
Restart=on-failure
RestartSec=5

[Install]
WantedBy=default.target
EOF
systemctl --user daemon-reload
systemctl --user enable --now gc-slack-adapter.service
journalctl --user -u gc-slack-adapter -f
```

If you'd rather have the gc supervisor manage the adapter (Phase A
`proxy_process`), follow the cutover sequence in the slack-pack README
instead — that path eliminates the standalone systemd unit and lets
gc reverse-proxy `/publish` over a UDS while the public Slack
endpoint stays bound to TCP `:8765`.

## Troubleshooting

- **Adapter starts but Slack URL verify fails**: confirm Tailscale Funnel
  is up (`tailscale funnel status`), check `curl https://<your-url>/healthz`
  returns ok, and confirm the Slack app's Request URL exactly matches
  `https://<your-url>/slack/events` (note the path).
- **"register adapter" fails on startup**: gc supervisor needs to be
  running; verify `gc cities` lists your city name.
- **Outbound publish errors**: check `gc supervisor logs` for `publish:`
  failures. If you see `channel_not_found` the bot isn't a member of the
  channel — for DMs this shouldn't happen since you DM'd it; for a
  channel, invite the bot. If you see `missing_scope` from `files.*`,
  the bot lacks `files:write` (outbound) or `files:read` (inbound).
- **Inbound replies don't reach the bound session**: check the signing
  secret is correct; check the `message.im` event subscription is active
  in the Slack app config; confirm the session is bound (look up
  `extmsg/bindings` via `gc slack status --session <SID>`).
