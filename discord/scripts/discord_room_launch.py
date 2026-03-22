#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sys

import discord_intake_common as common


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Enable launcher mode for a Discord root room")
    parser.add_argument("--guild-id", required=True, help="Discord guild id for the room")
    parser.add_argument(
        "--response-mode",
        default="mention_only",
        choices=("mention_only", "respond_all"),
        help="How root-room messages are routed",
    )
    parser.add_argument(
        "--default-handle",
        default="",
        help="Qualified rig/alias handle used for respond_all rooms",
    )
    parser.add_argument("conversation_id", help="Discord channel id for the root room")
    args = parser.parse_args(argv)

    default_handle = str(args.default_handle).strip().lower()
    if default_handle:
        qualified_handle, resolve_error = common.resolve_agent_handle(default_handle)
        if resolve_error:
            raise SystemExit(resolve_error)
        default_handle = qualified_handle

    try:
        config = common.set_room_launcher(
            common.load_config(),
            args.guild_id,
            args.conversation_id,
            response_mode=args.response_mode,
            default_qualified_handle=default_handle,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    launcher = common.resolve_room_launcher(config, args.conversation_id)
    print(json.dumps(launcher, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
