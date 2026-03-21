#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sys

import discord_intake_common as common


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Bind a Discord conversation to named sessions")
    parser.add_argument("--kind", required=True, choices=("dm", "room"), help="Binding kind")
    parser.add_argument("--guild-id", default="", help="Discord guild id for room metadata")
    parser.add_argument("conversation_id", help="Discord DM, channel, or thread id")
    parser.add_argument("session_name", nargs="+", help="Exact Gas City session name")
    args = parser.parse_args(argv)

    try:
        config = common.set_chat_binding(
            common.load_config(),
            args.kind,
            args.conversation_id,
            args.session_name,
            guild_id=args.guild_id,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    binding = common.resolve_chat_binding(config, common.chat_binding_id(args.kind, args.conversation_id))
    print(json.dumps(binding, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
