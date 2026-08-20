#!/usr/bin/env python3
"""Reply to the latest Slack inbound event seen by the current session.

The lookup order:

1. If --conversation-id is supplied, reply to that conversation.
2. Otherwise, scan recent extmsg.inbound events for one targeting the
   current session, and reply to the same conversation.
3. If neither yields a target, fall back to the session's saved
   extmsg binding.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import sys
from typing import Any

import slack_intake_common as common
import slack_mrkdwn


def _derive_idempotency_key(
    *, session_id: str, conversation_id: str, reply_to: str, body: str
) -> str:
    """Derive a stable idempotency key from the reply's identifying fields.

    When the caller does not pass ``--idempotency-key``, a retry of the
    *same* logical reply (same session, conversation, thread anchor and
    body) must reuse the same key so the adapter replays the original
    receipt instead of posting a duplicate after a delivered-but-timed-out
    POST (gpk-lbhl). The fingerprint is deterministic, matching the bead's
    "client-supplied idempotency key / message fingerprint" contract.
    """
    fingerprint = "\x00".join((session_id, conversation_id, reply_to, body))
    digest = hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()
    return f"reply-current:{digest}"


def _load_body(args: argparse.Namespace) -> str:
    if args.body and args.body_file:
        raise SystemExit("pass --body OR --body-file, not both")
    body = ""
    if args.body:
        body = args.body
    elif args.body_file:
        body = pathlib.Path(args.body_file).read_text(encoding="utf-8")
    else:
        raise SystemExit("either --body or --body-file is required")
    if not getattr(args, "raw", False):
        # gp-o42: tilde pairs render as strikethrough in Slack mrkdwn.
        body = slack_mrkdwn.escape_accidental_mrkdwn(body)
    return body


def _slack_kind_from_channel_id(cid: str, fallback: str = "dm") -> str:
    """Map a Slack channel id prefix to gc's conversation kind.

    Public channels are "C", private channels and multi-party DMs are
    "G", direct messages are "D". Anything else falls back to the
    user-supplied default (usually "dm"). Mirrors the adapter's
    slackKindFromChannelType in main.go.
    """
    if not cid:
        return fallback
    head = cid[:1].upper()
    if head in ("C", "G"):
        return "room"
    if head == "D":
        return "dm"
    return fallback


def _resolve_conversation(
    args: argparse.Namespace, session_id: str
) -> dict[str, str]:
    """Pick which Slack conversation to publish into."""
    explicit = (args.conversation_id or "").strip()
    user_set_kind = args.kind is not None
    if explicit:
        workspace = os.environ.get("SLACK_WORKSPACE_ID", "").strip()
        if not workspace:
            raise SystemExit("SLACK_WORKSPACE_ID must be set when using --conversation-id")
        kind = args.kind if user_set_kind else _slack_kind_from_channel_id(explicit, _DEFAULT_KIND)
        return {
            "scope_id": common.gc_city_name(),
            "provider": "slack",
            "account_id": workspace,
            "conversation_id": explicit,
            "kind": kind,
        }
    event = common.find_latest_inbound_for_session(session_id)
    if event is not None:
        payload = event.get("payload") or {}
        cid = (payload.get("conversation_id") or "").strip()
        if cid:
            kind = args.kind if user_set_kind else _slack_kind_from_channel_id(cid, _DEFAULT_KIND)
            return {
                "scope_id": common.gc_city_name(),
                "provider": "slack",
                "account_id": os.environ.get("SLACK_WORKSPACE_ID", ""),
                "conversation_id": cid,
                "kind": kind,
            }
    binding = common.look_up_binding(session_id)
    if binding:
        return {
            "scope_id": binding.get("scope_id", common.gc_city_name()),
            "provider": binding.get("provider", "slack"),
            "account_id": binding.get("account_id", os.environ.get("SLACK_WORKSPACE_ID", "")),
            "conversation_id": binding.get("conversation_id", ""),
            "kind": binding.get("kind", _DEFAULT_KIND),
        }
    raise SystemExit(
        "no inbound event and no binding found for this session; "
        "pass --conversation-id explicitly")


_DEFAULT_KIND = "dm"


def _maybe_company_reply(args: argparse.Namespace) -> int | None:
    """Company-context path: post via the acting agent's own token.

    Additive and inert for non-company sessions. Whenever this session has an
    active company current-turn pointer we divert by the turn ``kind``:

      * ``peer_delegation`` → post a delegation result into the human root
        thread (``gc_delegation_result`` gate, requester the only live mention);
      * ``peer_result`` → post a synthesis into the root with no live mentions;
      * ``ambient`` / ``thread_ambient`` / ``targeted`` / ``peer_input`` →
        post an ordinary reply into the room's thread root with no live
        mentions.

    Only the *absence* of a company pointer returns ``None`` so the legacy path
    runs byte-for-byte; a session with any company pointer answers into the
    room (the legacy resolution the company delivery path never feeds).
    """
    session_name = os.environ.get("GC_SESSION_NAME", "").strip()
    if not session_name:
        if (getattr(args, "turn_ref", "") or "").strip():
            raise SystemExit(
                "--turn-ref requires GC_SESSION_NAME so the immutable company "
                "turn can be verified against its bound agent session")
        return None
    try:
        import slack_company_outbound as outbound  # type: ignore
    except ImportError:
        return None
    # `--kind room|dm|mpim` overrides which pointer this reply acts on when more
    # than one turn is live; anything else (thread / unset) leaves the
    # newest-by-delivered-at default. A non-company value is ignored here and
    # left to the legacy conversation-kind path.
    kind_override = args.kind if args.kind in ("room", "dm", "mpim") else ""
    origin_ts = (getattr(args, "origin_ts", "") or "").strip()
    turn_ref = (getattr(args, "turn_ref", "") or "").strip()
    try:
        source = outbound.resolve_reply_pointer_source(
            session_name, kind_override=kind_override, turn_ref=turn_ref)
        if source is None:
            return None  # no company pointer — fall through to the legacy path
        body = _load_body(args)
        if source == "dm":
            result = outbound.post_company_dm_reply(
                body=body, origin_ts=origin_ts, session_name=session_name,
                turn_ref=turn_ref)
        elif source == "mpim":
            result = outbound.post_company_mpim_reply(
                body=body, origin_ts=origin_ts, session_name=session_name,
                turn_ref=turn_ref)
        else:
            turn = (outbound.read_turn_ref(session_name, turn_ref)
                    if turn_ref else outbound.read_current_turn(session_name))
            kind = turn.get("kind") if turn is not None else None
            if kind == "peer_delegation":
                result = outbound.post_peer_result(
                    body=body, origin_ts=origin_ts, session_name=session_name,
                    turn_ref=turn_ref)
            elif kind == "peer_result":
                result = outbound.post_peer_synthesis(
                    body=body, origin_ts=origin_ts, session_name=session_name,
                    allow_partial=bool(getattr(args, "allow_partial", False)),
                    turn_ref=turn_ref)
            else:  # ambient / thread_ambient / targeted / peer_input → root reply
                result = outbound.post_company_root_reply(
                    body=body, origin_ts=origin_ts, session_name=session_name,
                    turn_ref=turn_ref)
    except (outbound.OutboundError, outbound.TransientPostError,
            outbound.DefinitivePostError) as exc:
        raise SystemExit(f"company reply failed: {exc}") from exc
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Reply to the latest Slack inbound event seen by the current session",
    )
    parser.add_argument("--session", default="", help="Override session id")
    parser.add_argument("--conversation-id", default="",
                        help="Override Slack channel/DM id")
    parser.add_argument("--kind", default=None,
                        help=("Conversation kind (dm/room/thread). When omitted, "
                              "auto-detected from channel id prefix (C/G=room, "
                              "D=dm; default fallback dm). Company rooms: on a "
                              "session with more than one live current-turn "
                              "pointer, 'room'/'dm'/'mpim' overrides which the "
                              "reply acts on (default: newest by delivered-at)."))
    parser.add_argument("--reply-to", default="",
                        help="Slack message ts to reply to (threaded reply)")
    parser.add_argument(
        "--origin-ts", default="",
        help=("Company rooms: pin a specific turn ts when a newer wake has "
              "overwritten the current-turn pointer (mismatch is a hard error)."))
    parser.add_argument(
        "--turn-ref", default="",
        help=("Company messages: immutable turn reference from the authenticated "
              "Slack reminder. Selects that exact channel/thread even when a "
              "newer message woke the same session."))
    parser.add_argument(
        "--allow-partial", action="store_true",
        help=("Company rooms: on a peer_result (synthesis) turn, synthesize the "
              "currently-materialized compatible delegations even when the "
              "frozen snapshot is not ready (records allow_partial in the "
              "report). Ignored for non-synthesis turns."))
    parser.add_argument(
        "--thread-current",
        action="store_true",
        help=(
            "Thread the reply under the latest inbound message routed to "
            "this session (resolved via gc transcript). Cannot be combined "
            "with --reply-to. If no recent inbound is found, fails fast."
        ),
    )
    parser.add_argument("--idempotency-key", default="",
                        help=("Caller-supplied idempotency key. When omitted, a "
                              "deterministic key is derived from the session, "
                              "conversation, thread anchor and body so a retry of "
                              "the same reply dedupes instead of double-posting."))
    parser.add_argument("--body", default="")
    parser.add_argument("--body-file", default="")
    parser.add_argument(
        "--raw", action="store_true",
        help=("Send the body verbatim, skipping the accidental-mrkdwn guard "
              "(by default, tildes that would pair into unintended Slack "
              "strikethrough are neutralized; deliberate ~word~ wrapping and "
              "code spans always pass through)."))
    parser.add_argument(
        "--via",
        choices=("gc", "adapter"),
        default="gc",
        help=("Publish through gc /extmsg/outbound (default) so peer fanout "
              "+ transcript recording fire, or directly to the local adapter "
              "(adapter-only diagnostics; peers in a bind-room won't see "
              "the message)."),
    )
    args = parser.parse_args(argv)

    company_rc = _maybe_company_reply(args)
    if company_rc is not None:
        return company_rc

    body = _load_body(args)

    session_id = (args.session or "").strip()
    if not session_id:
        try:
            session_id = common.current_session_id()
        except common.GCAPIError as exc:
            raise SystemExit(str(exc)) from exc

    conv = _resolve_conversation(args, session_id)
    if not conv.get("conversation_id"):
        raise SystemExit("could not resolve a target conversation_id")
    if not conv.get("account_id"):
        raise SystemExit("missing slack account_id (SLACK_WORKSPACE_ID env)")

    reply_to = args.reply_to
    if args.thread_current:
        if reply_to:
            raise SystemExit("pass --reply-to OR --thread-current, not both")
        match = common.find_latest_inbound_message_id_for_session(session_id)
        if match is None:
            raise SystemExit(
                "no recent inbound transcript entry for this session; "
                "cannot thread without --reply-to <ts>"
            )
        reply_to = match[0]

    idempotency_key = args.idempotency_key.strip()
    if not idempotency_key:
        idempotency_key = _derive_idempotency_key(
            session_id=session_id,
            conversation_id=conv["conversation_id"],
            reply_to=reply_to,
            body=body,
        )

    publish_kwargs = dict(
        session_id=session_id,
        scope_id=conv["scope_id"],
        provider=conv["provider"],
        account_id=conv["account_id"],
        conversation_id=conv["conversation_id"],
        kind=conv["kind"],
        text=body,
        reply_to_message_id=reply_to,
        idempotency_key=idempotency_key,
    )
    try:
        if args.via == "adapter":
            result = common.publish_via_adapter(**publish_kwargs)
        else:
            result = common.publish_via_gc_outbound(**publish_kwargs)
    except (common.AdapterError, common.GCAPIError) as exc:
        raise SystemExit(str(exc)) from exc

    print(json.dumps({
        "conversation_id": conv["conversation_id"],
        "session_id": session_id,
        "via": args.via,
        "result": result,
    }, indent=2))

    delivered, failure_kind = common.interpret_publish_receipt(result)
    if not delivered:
        print(
            f"slack publish failed: delivered=false failure_kind={failure_kind or 'unknown'}",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
