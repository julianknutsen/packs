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
    parser = argparse.ArgumentParser(description="Publish a Discord-visible message through a saved chat binding")
    parser.add_argument("--binding", required=True, help="Binding id such as room:1234567890")
    parser.add_argument("--conversation-id", default="", help="Discord channel or thread id to publish into")
    parser.add_argument("--trigger", default="", help="Original Discord message id for reply threading")
    parser.add_argument("--reply-to", default="", help="Explicit Discord message id to reply to")
    parser.add_argument("--body", default="", help="Inline message body")
    parser.add_argument("--body-file", default="", help="Read the message body from a file")
    args = parser.parse_args(argv)

    body = _load_body(args)
    config = common.load_config()
    binding = common.resolve_chat_binding(config, args.binding)
    if not binding:
        raise SystemExit(f"binding not found: {args.binding}")
    try:
        payload = common.publish_binding_message(
            binding,
            body,
            requested_conversation_id=args.conversation_id,
            trigger_id=args.trigger,
            reply_to_message_id=args.reply_to,
        )
    except (ValueError, common.DiscordAPIError) as exc:
        raise SystemExit(str(exc)) from exc
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
