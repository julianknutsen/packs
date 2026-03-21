#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import pathlib
import sys

import discord_intake_common as common


def _read_optional_file(path: str | None) -> str:
    if not path:
        return ""
    return pathlib.Path(path).read_text(encoding="utf-8").strip()


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Import Discord app metadata into the Discord pack")
    parser.add_argument("--application-id", required=True, help="Discord application id")
    parser.add_argument("--public-key", required=True, help="Discord interaction public key (hex)")
    parser.add_argument("--command-name", default=common.COMMAND_NAME_DEFAULT, help="Slash command root name")
    parser.add_argument("--bot-token", default="", help="Discord bot token")
    parser.add_argument("--bot-token-file", default="", help="Read the Discord bot token from a file")
    parser.add_argument("--guild-allowlist", action="append", default=[], help="Optional allowed guild id")
    parser.add_argument("--channel-allowlist", action="append", default=[], help="Optional allowed parent channel id")
    parser.add_argument("--role-allowlist", action="append", default=[], help="Optional allowed Discord role id")
    args = parser.parse_args(argv)

    bot_token = args.bot_token.strip() or _read_optional_file(args.bot_token_file)
    try:
        config = common.import_app_config(
            common.load_config(),
            {
                "application_id": args.application_id,
                "public_key": args.public_key,
                "command_name": args.command_name,
                "guild_allowlist": args.guild_allowlist,
                "channel_allowlist": args.channel_allowlist,
                "role_allowlist": args.role_allowlist,
            },
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    if bot_token:
        common.save_bot_token(bot_token)
    print(json.dumps(common.redact_config(config), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
