#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import pathlib
import sys

import discord_intake_common as common


def _load_body(args: argparse.Namespace) -> str:
    if args.body:
        return args.body
    if args.body_file:
        return pathlib.Path(args.body_file).read_text(encoding="utf-8")
    raise SystemExit("either --body or --body-file is required")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Reply to the latest Discord event seen by the current session")
    parser.add_argument("--session", default="", help="Override the current session selector (defaults to $GC_SESSION_ID or $GC_SESSION_NAME)")
    parser.add_argument("--tail", type=int, default=40, help="How many raw transcript messages to search for the latest Discord event")
    parser.add_argument("--body", default="", help="Inline message body")
    parser.add_argument("--body-file", default="", help="Read the message body from a file")
    args = parser.parse_args(argv)

    body = _load_body(args)
    try:
        context = common.find_latest_discord_reply_context(args.session, tail=max(1, args.tail))
    except common.GCAPIError as exc:
        raise SystemExit(str(exc)) from exc
    binding_id = str(context.get("publish_binding_id", "")).strip()
    if not binding_id:
        raise SystemExit("latest discord event is missing publish_binding_id")
    config = common.load_config()
    binding = common.resolve_chat_binding(config, binding_id)
    if not binding:
        raise SystemExit(f"binding not found: {binding_id}")
    try:
        payload = common.publish_binding_message(
            binding,
            body,
            requested_conversation_id=str(context.get("publish_conversation_id", "")).strip(),
            trigger_id=str(context.get("publish_trigger_id", "")).strip(),
            reply_to_message_id=str(context.get("publish_reply_to_discord_message_id", "")).strip(),
        )
    except (ValueError, common.DiscordAPIError) as exc:
        raise SystemExit(str(exc)) from exc
    payload["reply_context"] = {
        "session_selector": str(args.session).strip() or common.current_session_selector(),
        "binding_id": binding_id,
        "publish_conversation_id": str(context.get("publish_conversation_id", "")).strip(),
        "publish_trigger_id": str(context.get("publish_trigger_id", "")).strip(),
        "publish_reply_to_discord_message_id": str(context.get("publish_reply_to_discord_message_id", "")).strip(),
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
