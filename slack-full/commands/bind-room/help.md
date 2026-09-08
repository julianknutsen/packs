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

Every run must declare who owns the room's direct binding — either
`--binding-owner SESSION` or `--group-only`. See "Binding authority" below.

Group-routed room (every session sees inbound, default-routed to the
first session for explicit-target resolution; no direct binding):

  gc slack bind-room C0123ROOM01 oversight-rig.mayor geo/oversight-rig.project-lead \
      --group-only

Room with an outbound publisher (the shape oversight-rig uses):

  gc slack bind-room C0123ROOM01 \
      oversight-rig.mayor geo/oversight-rig.project-lead \
      --binding-owner gc-77139

Enable peer-fanout policy with caps (governs peer-triggered publishes):

  gc slack bind-room C0123ROOM01 \
      oversight-rig.mayor geo/oversight-rig.project-lead \
      --group-only \
      --enable-peer-fanout \
      --allow-untargeted-publication \
      --max-peer-triggered-publishes 8 \
      --max-total-peer-deliveries 24

Override participant handles (used by `@@handle` routing):

  gc slack bind-room C0123ROOM01 \
      oversight-rig.mayor geo/oversight-rig.project-lead \
      --group-only \
      --default-handle mayor \
      --handle mayor=oversight-rig.mayor \
      --handle geo-pl=geo/oversight-rig.project-lead

Underlying calls
----------------

1. POST /v0/city/<name>/extmsg/groups   (mode=launcher; with fanout policy if any flag set)
2. POST /v0/city/<name>/extmsg/participants for each session
3. Reconcile gc's authoritative direct-binding table so it cannot shadow the
   group: POST /extmsg/unbind for a `--group-only` room, or POST /extmsg/bind
   with `replace=true` when `--binding-owner` is set.

Binding authority
-----------------

gc resolves an inbound message against a direct binding *before* the group
route, so a stale direct binding silently shadows the group. Step 3 makes the
binding table agree with the topology you declare — and because that step
writes authoritative state, exactly one of these is required on every run:

  --binding-owner SESSION   keep (or hand over) the room's outbound publisher.
                            Any other direct binding for the room is replaced.
  --group-only              declare the room group-routed. Removes EVERY active
                            direct binding for the conversation — including one
                            created by other tooling or another operator.

Passing neither is an error, not a default: `--group-only`'s sweep is
destructive, and an operator who forgets `--binding-owner` on a re-run would
otherwise sever the room's outbound publishing without being told. The removed
bindings are printed to stderr and returned as `unbound_bindings`.

The pack records the binding under
`.gc/services/slack/data/config.json` so other slack-pack commands can
resolve the room without re-querying gc. The local record is written only after
the authoritative reconciliation succeeds.
