Bind a Slack room/channel (public, private, or multi-party DM) to one or
more named sessions, optionally creating a conversation group with a
peer-fanout policy and per-session participant handles.

Each session bound to the room receives an inbound system-reminder when
a human posts in the channel. When peers publish through the gc
outbound API, every other bound session is also notified — that's how
mayor and project-leads end up visible to each other inside one
conversation while a human watches.

Examples
--------

Plain ambient binding (every session sees inbound, default-routed to
the first session for explicit-target resolution):

  gc slack bind-room C0123ROOM01 oversight-rig.mayor geo/oversight-rig.project-lead

Enable peer-fanout policy with caps (governs peer-triggered publishes):

  gc slack bind-room C0123ROOM01 \
      oversight-rig.mayor geo/oversight-rig.project-lead \
      --enable-peer-fanout \
      --allow-untargeted-publication \
      --max-peer-triggered-publishes 8 \
      --max-total-peer-deliveries 24

Override participant handles (used by `@@handle` routing):

  gc slack bind-room C0123ROOM01 \
      oversight-rig.mayor geo/oversight-rig.project-lead \
      --default-handle mayor \
      --handle mayor=oversight-rig.mayor \
      --handle geo-pl=geo/oversight-rig.project-lead

Underlying calls
----------------

1. POST /v0/city/<name>/extmsg/groups   (mode=launcher; with fanout policy if any flag set)
2. POST /v0/city/<name>/extmsg/participants for each session
3. Reconcile gc's authoritative direct-binding table so it cannot shadow the
   group: POST /extmsg/unbind for a group-only room, or POST /extmsg/bind with
   `replace=true` when `--binding-owner` is set.

Omitting `--binding-owner` declares the room group-only and removes any direct
binding, including one created separately. Pass the owner on every re-run that
must preserve outbound publication.

The pack records the binding under
`.gc/services/slack/data/config.json` so other slack-pack commands can
resolve the room without re-querying gc. The local record is written only after
the authoritative reconciliation succeeds.
