#!/usr/bin/env python3
"""Publish a message into a Slack channel without going through gc binding lookup.

Differs from ``publish`` in that the conversation_id is supplied
explicitly and gc's binding requirement is bypassed — the message goes
straight to the local adapter ``/publish``. Used by mayor / chief-of-staff
to reply into channels they have no binding for, after receiving a
``Slack address-by-handle`` system reminder triggered by `@mayor:` /
`@cos:` keyword routing from any channel.

The session id still flows through so the adapter applies the matching
identity registry override (visible username + avatar).
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys

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
    if not args.raw:
        # gp-o42: tilde pairs render as strikethrough in Slack mrkdwn.
        body = slack_mrkdwn.escape_accidental_mrkdwn(body)
    return body


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Publish into a Slack channel by id, bypassing gc binding lookup",
    )
    parser.add_argument("--conversation-id", required=True,
                        help="Slack channel id (C..., G..., or D...).")
    parser.add_argument("--kind", default="room",
                        choices=("dm", "room", "thread"),
                        help="Conversation kind for the conversation envelope. "
                             "Defaults to 'room' (channels).")
    parser.add_argument("--thread-ts", default="",
                        help="Slack message ts to thread under (optional).")
    parser.add_argument("--session", default="",
                        help="Session id to attribute this publish to "
                             "(applies identity override). Defaults to "
                             "$GC_SESSION_ID.")
    parser.add_argument("--idempotency-key", default="",
                        help="Caller-supplied idempotency key (optional).")
    parser.add_argument("--body", default="")
    parser.add_argument("--body-file", default="")
    parser.add_argument(
        "--raw", action="store_true",
        help=("Send the body verbatim, skipping the accidental-mrkdwn guard "
              "(by default, tildes that would pair into unintended Slack "
              "strikethrough are neutralized; deliberate ~word~ wrapping and "
              "code spans always pass through)."))
    args = parser.parse_args(argv)

    body = _load_body(args)

    session_id = (args.session or "").strip()
    if not session_id:
        try:
            session_id = common.current_session_id()
        except common.GCAPIError as exc:
            raise SystemExit(str(exc)) from exc

    if not os.environ.get("SLACK_WORKSPACE_ID", "").strip():
        raise SystemExit("SLACK_WORKSPACE_ID is not set; cannot construct conversation envelope")

    try:
        result = common.publish_to_channel_via_adapter(
            session_id=session_id,
            conversation_id=args.conversation_id,
            text=body,
            kind=args.kind,
            thread_ts=args.thread_ts,
            idempotency_key=args.idempotency_key,
        )
    except common.AdapterError as exc:
        raise SystemExit(str(exc)) from exc

    print(json.dumps({
        "session_id": session_id,
        "conversation_id": args.conversation_id,
        "thread_ts": args.thread_ts,
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
