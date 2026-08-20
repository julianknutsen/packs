#!/usr/bin/env python3
"""Publish a message into the conversation a session is bound to.

Differs from ``reply-current`` in three ways:

  1. The target session is required (defaults to the current session
     if --session is omitted, but no fallback to "any session that
     happens to be running").
  2. We always go through the saved binding — there is no inbound-
     event scan and no event-driven heuristic. The session must have
     an active extmsg binding or this command fails fast.
  3. The intent is explicit: "send X to the channel session SID is
     bound to." Useful for ops pings, cron-driven status posts, and
     cross-session smokes that don't depend on prior inbound traffic.

Use ``reply-current`` when you want to thread under an inbound that
just arrived. Use ``publish`` when you have something to say
unconditionally.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys
from typing import Any

import slack_intake_common as common
import slack_mrkdwn


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


def _resolve_conversation(session_id: str) -> dict[str, str]:
    """Return the conversation envelope for the session's active binding.

    Raises SystemExit if the session has no active binding — fail fast
    rather than silently fall back to "send into the void".
    """
    binding = common.look_up_binding(session_id)
    if not binding:
        raise SystemExit(
            f"session {session_id!r} has no active extmsg binding; "
            f"run `gc slack bind-dm` or `gc slack bind-room` first")
    return {
        "scope_id": binding.get("scope_id", common.gc_city_name()),
        "provider": binding.get("provider", "slack"),
        "account_id": binding.get("account_id",
                                  os.environ.get("SLACK_WORKSPACE_ID", "")),
        "conversation_id": binding.get("conversation_id", ""),
        "kind": binding.get("kind", "dm"),
    }


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Publish a message into the conversation bound to a session",
    )
    parser.add_argument("--session", default="",
                        help="Session id whose binding to publish into. "
                             "Defaults to the current session ($GC_SESSION_ID).")
    parser.add_argument("--reply-to", default="",
                        help="Slack message ts to reply to (threaded reply)")
    parser.add_argument("--idempotency-key", default="",
                        help="Caller-supplied idempotency key")
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

    body = _load_body(args)

    session_id = (args.session or "").strip()
    if not session_id:
        try:
            session_id = common.current_session_id()
        except common.GCAPIError as exc:
            raise SystemExit(str(exc)) from exc

    conv = _resolve_conversation(session_id)
    if not conv.get("conversation_id"):
        raise SystemExit(
            f"session {session_id!r} binding has no conversation_id "
            "(corrupt binding record?)")
    if not conv.get("account_id"):
        raise SystemExit("missing slack account_id (SLACK_WORKSPACE_ID env)")

    publish_kwargs = dict(
        session_id=session_id,
        scope_id=conv["scope_id"],
        provider=conv["provider"],
        account_id=conv["account_id"],
        conversation_id=conv["conversation_id"],
        kind=conv["kind"],
        text=body,
        reply_to_message_id=args.reply_to,
        idempotency_key=args.idempotency_key,
    )
    try:
        if args.via == "adapter":
            result = common.publish_via_adapter(**publish_kwargs)
        else:
            result = common.publish_via_gc_outbound(**publish_kwargs)
    except (common.AdapterError, common.GCAPIError) as exc:
        raise SystemExit(str(exc)) from exc

    print(json.dumps({
        "session_id": session_id,
        "conversation_id": conv["conversation_id"],
        "kind": conv["kind"],
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
