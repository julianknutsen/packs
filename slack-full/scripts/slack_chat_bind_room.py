#!/usr/bin/env python3
"""Bind a Slack room/channel to one or more named gc sessions.

Creates a launcher-mode conversation group rooted at the Slack channel
and adds each named session as a participant. With ``--enable-peer-fanout``
or any of the related fanout flags, the group is created with a fanout
policy preserved on the group record.

Why a group instead of a binding per session: gc bindings are 1:1 by
conversation (a second ``Bind`` call returns ``ErrBindingConflict``).
Memberships are 1:N and are what drives peer-fanout system reminders
(``extmsgNotifyMembers``). The simplest way to create N memberships
through the public API today is via group participants.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from typing import Any

import slack_intake_common as common


# Reply protocol delivered to each newly-bound participant. Sent on every
# bind (idempotent — duplicate nudges are harmless). Without this, a
# participant session that respawned after the original bind would lose
# the protocol contract and revert to its baseline prompt's reply path.
PROTOCOL_NUDGE_TEMPLATE = """<system-reminder>
Slack channel binding established for {conversation_id}.
You are a slack-bound agent in this conversation.

Reply protocol when you receive a `New message in shared conversation slack/...` reminder:

  1. **FIRST**: react with writing_hand — BEFORE you compose anything, BEFORE you
     read context, BEFORE you think about the reply.
       gc slack react --emoji writing_hand
     This signals to the human that you are actively working on the message.
     The adapter already placed 👀 on the message when it enqueued the inbound
     (transport-level ack: "queued for the agent"). Your ✍️ is the agent-level
     signal: "I am processing." Replying first means the human waits in silence
     until your full reply lands, even when you have an instant answer.

  2. THEN compose your reply to a tmpfile.

  3. THEN publish as a threaded reply (NOT publish-to-channel):
       gc slack reply-current --body-file <tmpfile> --thread-current

  4. THEN ack so the inbound is marked read:
       gc transcript read --ack

The order is non-negotiable even when you have an instant answer. Even
when the inbound is a re-ping of an active thread. Even when it's a
"ping" or "ack". React first, every time.

Use `gc slack publish-to-channel --conversation-id ... --no-thread` ONLY
for explicit top-level status broadcasts initiated by you, never as a
reply to an inbound. The writing_hand react is for inbound-replies only —
proactive posts (e.g. surfacing slung work completion) skip the react and
just publish-to-thread.
</system-reminder>
"""


def deliver_protocol_nudge(session_id: str, conversation_id: str) -> None:
    """Send the slack reply-protocol nudge to a session via `gc session nudge`.

    Best-effort — failures (session asleep, unknown target, gc binary
    missing) are logged to stderr and do not abort the bind. The nudge is
    idempotent so re-delivery on every bind is safe.
    """
    body = PROTOCOL_NUDGE_TEMPLATE.format(conversation_id=conversation_id)
    try:
        result = subprocess.run(
            ["gc", "session", "nudge", session_id, body],
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        sys.stderr.write(
            f"warn: protocol nudge to {session_id} failed: {exc}\n"
        )
        return
    if result.returncode != 0:
        sys.stderr.write(
            f"warn: protocol nudge to {session_id} returned "
            f"rc={result.returncode}: {result.stderr.strip()[:200]}\n"
        )


def _slack_workspace_id() -> str:
    val = os.environ.get("SLACK_WORKSPACE_ID", "").strip()
    if not val:
        raise SystemExit("SLACK_WORKSPACE_ID must be set in the slack adapter env")
    return val


def _parse_handle_overrides(values: list[str]) -> dict[str, str]:
    """Parse repeated ``--handle handle=session`` flags.

    Returns a map of session_name -> handle. Raises SystemExit on
    malformed input. Handles are normalized to lowercase on the gc
    side; we don't pre-normalize here because the API does it for us.
    """
    overrides: dict[str, str] = {}
    for raw in values:
        if "=" not in raw:
            raise SystemExit(f"--handle expects HANDLE=SESSION, got: {raw!r}")
        handle, _, session = raw.partition("=")
        handle = handle.strip()
        session = session.strip()
        if not handle or not session:
            raise SystemExit(f"--handle expects HANDLE=SESSION, got: {raw!r}")
        if session in overrides:
            raise SystemExit(f"--handle session {session!r} specified twice")
        overrides[session] = handle
    return overrides


def _default_handle_for_session(session_name: str) -> str:
    """Derive a participant handle from a session alias or id.

    For aliases like ``geo/oversight-rig.project-lead``, take the last
    dot-separated segment of the path tail (``project-lead``) and
    prefix with the directory (``geo-project-lead``). For unstructured
    ids like ``gc-83347`` we fall back to the raw id; the caller is
    expected to override via ``--handle`` in that case.
    """
    if "/" in session_name:
        head, tail = session_name.split("/", 1)
        last = tail.rsplit(".", 1)[-1]
        return f"{head}-{last}"
    if "." in session_name:
        return session_name.rsplit(".", 1)[-1]
    return session_name


def build_conversation_ref(
    *, conversation_id: str, kind: str, workspace_id: str, scope_id: str
) -> dict[str, str]:
    return {
        "scope_id": scope_id,
        "provider": "slack",
        "account_id": workspace_id,
        "conversation_id": conversation_id,
        "kind": kind,
    }


def build_fanout_policy(args: argparse.Namespace) -> dict[str, Any] | None:
    """Translate CLI flags into a FanoutPolicy dict, or None if no flag set."""
    any_set = (
        args.enable_peer_fanout
        or args.allow_untargeted_publication
        or args.max_peer_triggered_publishes
        or args.max_total_peer_deliveries
    )
    if not any_set:
        return None
    return {
        "enabled": bool(args.enable_peer_fanout),
        "allow_untargeted_publication": bool(args.allow_untargeted_publication),
        "max_peer_triggered_publishes": int(args.max_peer_triggered_publishes or 0),
        "max_total_peer_deliveries": int(args.max_total_peer_deliveries or 0),
    }


def build_participants(
    sessions: list[str],
    overrides: dict[str, str],
    default_handle: str,
) -> list[tuple[str, str]]:
    """Return [(handle, session_name), ...] in the order sessions were given.

    Raises SystemExit on duplicate handles or empty input.
    """
    if not sessions:
        raise SystemExit("at least one session is required")
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for session in sessions:
        handle = overrides.get(session) or _default_handle_for_session(session)
        if not handle:
            raise SystemExit(f"could not derive handle for session {session!r}")
        if handle in seen:
            raise SystemExit(
                f"duplicate handle {handle!r}; pass --handle to disambiguate")
        seen.add(handle)
        out.append((handle, session))
    if default_handle and default_handle not in seen:
        raise SystemExit(
            f"--default-handle {default_handle!r} does not match any participant handle "
            f"({sorted(seen)})")
    return out


def _recorded_binding_owner(binding_key: str) -> str:
    """Best-effort read of the owner this pack last recorded for the room.

    Only used to make the fail-closed error below name a session instead of
    saying "something may be bound". gc has no conversation-scoped binding
    read — ``GET /extmsg/bindings`` is keyed by ``session_id`` and
    ``resolveActiveBinding`` is internal — so the local record is the only
    owner name available to this script, and it is a hint, not authority: it
    can be stale, and a binding created by other tooling never appears in it.
    Every failure mode here (no city path, corrupt state, unreadable state,
    absent record) degrades to "unknown owner", never to a wrong answer or an
    abort. OSError is caught alongside GCAPIError because load_pack_config
    only wraps json.JSONDecodeError, so a config.json that exists but cannot
    be read raises straight through it — and a hint that cannot be produced
    must never turn the curated refusal into a traceback.
    """
    try:
        cfg = common.load_pack_config()
    except (common.GCAPIError, OSError):
        return ""
    if not isinstance(cfg, dict):
        return ""
    record = (cfg.get("bindings") or {}).get(binding_key) or {}
    if not isinstance(record, dict):
        return ""
    return str(record.get("binding_owner") or "")


def resolve_binding_authority(args: argparse.Namespace, binding_key: str) -> str:
    """Return the declared binding owner, or "" for an explicit group-only room.

    Fails closed. Reconciliation writes gc's authoritative direct-binding
    table, and the group-only arm is destructive: ``/extmsg/unbind`` with a
    conversation-only body matches *every* active direct binding for the room
    (gc ``binding_service.go``'s empty filter), including bindings this pack
    never created. That is the right thing to do when an operator declares the
    room group-only and the wrong thing to do by default, so it may not be
    reachable by *omitting* a flag — an operator who forgets ``--binding-owner``
    on a re-run would silently sever the room's outbound publishing.

    So exactly one of ``--binding-owner`` / ``--group-only`` is required. The
    check runs before any API call, so a rejected invocation has mutated
    nothing.
    """
    binding_owner = args.binding_owner.strip()
    if binding_owner and args.group_only:
        raise SystemExit(
            "--binding-owner and --group-only are mutually exclusive: "
            "--binding-owner names the session that keeps the direct binding, "
            "--group-only removes it")
    if binding_owner:
        return binding_owner
    if args.group_only:
        return ""
    recorded = _recorded_binding_owner(binding_key)
    owner_note = (
        f"this pack last recorded {recorded!r} as the owner of {binding_key}; "
        if recorded
        else ""
    )
    raise SystemExit(
        "refusing to reconcile the direct binding without an explicit "
        "declaration: pass --binding-owner SESSION to keep (or hand over) the "
        "room's outbound publisher, or --group-only to declare the room "
        "group-routed and remove every active direct binding for it. "
        f"{owner_note}"
        "--group-only removes bindings created by other tooling too, so it is "
        "never assumed")


def reconcile_authoritative_binding(
    conversation: dict[str, str], binding_owner: str
) -> dict[str, Any]:
    """Make extmsg's direct-binding table agree with the room topology.

    Inbound resolution consults a direct binding before its group route. A
    group-only room must therefore have no direct binding, while a room with a
    publisher owner must replace any stale direct binding with that owner.

    Returns a normalized record of what was reconciled::

        {"action": "bind"|"unbind", "binding": {...}|None, "unbound": [...]}

    The two API responses have different shapes — ``/extmsg/bind`` answers with
    the binding it created, ``/extmsg/unbind`` with ``{"unbound": [...]}`` — and
    flattening one into a field named for the other is how the removed bindings
    stopped being legible to the operator who removed them.
    """
    try:
        if binding_owner:
            binding = common.gc_post(
                "/extmsg/bind",
                {
                    "session_id": binding_owner,
                    "conversation": conversation,
                    "replace": True,
                },
            )
            return {"action": "bind", "binding": binding or None, "unbound": []}
        response = common.gc_post("/extmsg/unbind", {"conversation": conversation})
        unbound = (response or {}).get("unbound") or []
        return {"action": "unbind", "binding": None, "unbound": unbound}
    except common.GCAPIError as exc:
        raise SystemExit(f"reconcile authoritative binding: {exc}") from exc


def report_unbound(unbound: list[Any]) -> None:
    """Tell the operator which direct bindings the group-only sweep removed.

    A removal nobody can see is a removal nobody can undo: the sweep can take
    out a binding another pack or operator created, and until now the API's
    answer was dropped on the floor.
    """
    for record in unbound:
        if isinstance(record, dict):
            detail = f"{record.get('SessionID') or '?'} (binding {record.get('ID') or '?'})"
        else:
            detail = str(record)
        sys.stderr.write(f"unbound direct binding: {detail}\n")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Bind a Slack room/channel to one or more named gc sessions",
    )
    parser.add_argument("conversation_id", help="Slack channel id (e.g. C0123ROOM01)")
    parser.add_argument("session_names", nargs="+", help="gc session name or id")
    parser.add_argument("--kind", default="room", choices=("room",),
                        help="Conversation kind. Default: room")
    parser.add_argument("--mode", default="launcher", choices=("launcher",),
                        help="Group mode. Default: launcher")
    parser.add_argument("--default-handle", default="",
                        help="Default participant handle for untargeted messages "
                             "(must match one of the participant handles)")
    parser.add_argument("--handle", action="append", default=[],
                        metavar="HANDLE=SESSION",
                        help="Override the handle assigned to a session (repeatable)")
    parser.add_argument("--enable-peer-fanout", action="store_true",
                        help="Set FanoutPolicy.enabled = true on the group")
    parser.add_argument("--allow-untargeted-publication", action="store_true",
                        help="Set FanoutPolicy.allow_untargeted_publication = true")
    parser.add_argument("--max-peer-triggered-publishes", type=int, default=0,
                        help="Cap peer-triggered publishes per inbound (0 = unlimited)")
    parser.add_argument("--max-total-peer-deliveries", type=int, default=0,
                        help="Cap total peer deliveries per inbound (0 = unlimited)")
    parser.add_argument("--no-protocol-nudge", action="store_true",
                        help="Skip auto-delivery of the slack reply-protocol nudge "
                             "(react first, threaded reply, ack) to each newly-bound "
                             "participant. By default the nudge is sent on every bind, "
                             "which is idempotent and safe — disable only if a caller "
                             "is composing its own protocol delivery.")
    parser.add_argument("--binding-owner", default="",
                        metavar="SESSION",
                        help="Bind this session to the conversation as the publisher "
                             "for /extmsg/outbound, replacing any stale direct binding. "
                             "Required on every bind-room run that must preserve outbound "
                             "publishing; pass --group-only instead to declare the room "
                             "group-routed. Should refer semantically to one "
                             "of the participants. Pass the gc-id (e.g. gc-77139) when the "
                             "binding will be looked up by gc-id (e.g. resolve_rig_channel.py); "
                             "pass the participant alias when the rest of the system reads "
                             "the binding by alias. The script does NOT cross-check the owner "
                             "against the participant list — this is intentional so gc-ids "
                             "can be used alongside alias-based participants.")
    parser.add_argument("--group-only", action="store_true",
                        help="Declare the room purely group-routed: remove EVERY active "
                             "direct binding for the conversation so none can shadow the "
                             "group route, including bindings created by other tooling. "
                             "Mutually exclusive with --binding-owner; one of the two is "
                             "required, because this removal is destructive and must not be "
                             "reachable by forgetting a flag.")
    args = parser.parse_args(argv)

    binding_key = f"{args.kind}:{args.conversation_id}"
    # Before any API call: a rejected declaration must not have created a
    # group, upserted a participant, or touched the binding table.
    binding_owner = resolve_binding_authority(args, binding_key)

    workspace_id = _slack_workspace_id()
    city = common.gc_city_name()
    overrides = _parse_handle_overrides(args.handle)
    participants = build_participants(args.session_names, overrides, args.default_handle)
    default_handle = args.default_handle or participants[0][0]
    conv = build_conversation_ref(
        conversation_id=args.conversation_id,
        kind=args.kind,
        workspace_id=workspace_id,
        scope_id=city,
    )
    fanout_policy = build_fanout_policy(args)

    group_body: dict[str, Any] = {
        "root_conversation": conv,
        "mode": args.mode,
        "default_handle": default_handle,
    }
    if fanout_policy is not None:
        group_body["fanout_policy"] = fanout_policy

    try:
        group = common.gc_post("/extmsg/groups", group_body)
    except common.GCAPIError as exc:
        raise SystemExit(f"ensure group: {exc}") from exc
    group_id = group.get("ID", "")
    if not group_id:
        raise SystemExit(f"ensure group: response missing ID: {group!r}")

    participant_records: list[dict[str, Any]] = []
    for handle, session in participants:
        try:
            res = common.gc_post(
                "/extmsg/participants",
                {"group_id": group_id, "handle": handle, "session_id": session, "public": True},
            )
        except common.GCAPIError as exc:
            raise SystemExit(f"upsert participant {handle}={session}: {exc}") from exc
        participant_records.append(res)

    reconciled = reconcile_authoritative_binding(conv, binding_owner)
    report_unbound(reconciled["unbound"])

    cfg = common.load_pack_config()
    cfg.setdefault("bindings", {})
    cfg["bindings"][binding_key] = {
        "kind": args.kind,
        "conversation": conv,
        "group_id": group_id,
        "default_handle": default_handle,
        "fanout_policy": fanout_policy,
        "participants": [
            {"handle": h, "session_name": s} for h, s in participants
        ],
        "binding_owner": binding_owner or None,
        "binding_record": (reconciled["binding"] or {}).get("ID", "") or None,
    }
    common.save_pack_config(cfg)

    # Deliver the protocol nudge to each participant session so respawned
    # sessions don't lose the reply contract. Best-effort, opt-out via flag.
    nudged: list[str] = []
    nudge_failures: list[str] = []
    if not args.no_protocol_nudge:
        for _, session in participants:
            try:
                deliver_protocol_nudge(session, args.conversation_id)
                nudged.append(session)
            except Exception as exc:  # noqa: BLE001 — best-effort, never abort bind
                nudge_failures.append(f"{session}: {exc}")

    print(json.dumps({
        "binding_key": binding_key,
        "group_id": group_id,
        "default_handle": default_handle,
        "fanout_policy": fanout_policy,
        "participants": participant_records,
        "binding_owner": binding_owner or None,
        "binding_record": reconciled["binding"],
        "unbound_bindings": reconciled["unbound"],
        "protocol_nudge": {
            "delivered_to": nudged,
            "failures": nudge_failures,
            "skipped": args.no_protocol_nudge,
        },
    }, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
