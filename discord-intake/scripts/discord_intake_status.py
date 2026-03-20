#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sys

import discord_intake_common as common


def render_text(snapshot: dict[str, object]) -> str:
    config = snapshot.get("config", {})
    requests = snapshot.get("recent_requests", [])
    lines = [
        "Discord Intake",
        f"  interactions_url: {snapshot.get('interactions_url') or '(not published yet)'}",
        f"  admin_url:        {snapshot.get('admin_url') or '(not published yet)'}",
        f"  state_root:       {snapshot.get('state_root') or ''}",
        f"  command_name:     {((config or {}).get('app') or {}).get('command_name', common.COMMAND_NAME_DEFAULT)}",
        f"  bot_token:        {'present' if ((config or {}).get('app') or {}).get('bot_token_present') else 'missing'}",
        f"  channel_mappings: {len(((config or {}).get('channels') or {}))}",
        f"  rig_mappings:     {len(((config or {}).get('rigs') or {}))}",
        "",
        "Recent Requests:",
    ]
    if not requests:
        lines.append("  (none)")
    else:
        for item in requests:
            lines.append(
                "  - {request_id} {status} guild={guild_id} conversation={conversation_id} bead={bead_id}".format(
                    request_id=item.get("request_id", ""),
                    status=item.get("status", ""),
                    guild_id=item.get("guild_id", ""),
                    conversation_id=item.get("conversation_id", ""),
                    bead_id=item.get("bead_id", ""),
                )
            )
    return "\n".join(lines)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Show Discord intake status")
    parser.add_argument("--json", action="store_true", help="Print JSON instead of text")
    parser.add_argument("--limit", type=int, default=20, help="Number of recent requests to show")
    args = parser.parse_args(argv)

    snapshot = common.build_status_snapshot(limit=max(1, args.limit))
    if args.json:
        print(json.dumps(snapshot, indent=2, sort_keys=True))
    else:
        print(render_text(snapshot))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
