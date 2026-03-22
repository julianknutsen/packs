#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sys

import discord_intake_common as common


def render_text(snapshot: dict[str, object]) -> str:
    config = snapshot.get("config", {})
    gateway = snapshot.get("gateway_status", {})
    requests = snapshot.get("recent_requests", [])
    bindings = snapshot.get("chat_bindings", [])
    launchers = snapshot.get("chat_launchers", [])
    ingress = snapshot.get("recent_chat_ingress", [])
    publishes = snapshot.get("recent_chat_publishes", [])
    launches = snapshot.get("recent_room_launches", [])
    lines = [
        "Discord",
        f"  interactions_url: {snapshot.get('interactions_url') or '(not published yet)'}",
        f"  admin_url:        {snapshot.get('admin_url') or '(not published yet)'}",
        f"  command_name:     {((config or {}).get('app') or {}).get('command_name', common.COMMAND_NAME_DEFAULT)}",
        f"  bot_token:        {'present' if ((config or {}).get('app') or {}).get('bot_token_present') else 'missing'}",
        f"  gateway_state:    {((gateway or {}).get('state') or '(unknown)')}",
        f"  gateway_error:    {((gateway or {}).get('last_error') or '-')}",
        f"  channel_mappings: {len(((config or {}).get('channels') or {}))}",
        f"  rig_mappings:     {len(((config or {}).get('rigs') or {}))}",
        f"  chat_bindings:    {len(bindings)}",
        f"  chat_launchers:   {len(launchers)}",
        f"  chat_ingress:     {len(ingress)}",
        f"  chat_publishes:   {len(publishes)}",
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
    lines.append("")
    lines.append("Chat Bindings:")
    if not bindings:
        lines.append("  (none)")
    else:
        for item in bindings:
            lines.append(
                "  - {binding_id} kind={kind} conversation={conversation_id} sessions={sessions}".format(
                    binding_id=item.get("id", ""),
                    kind=item.get("kind", ""),
                    conversation_id=item.get("conversation_id", ""),
                    sessions=",".join(item.get("session_names", [])),
                )
            )
    lines.append("")
    lines.append("Chat Launchers:")
    if not launchers:
        lines.append("  (none)")
    else:
        for item in launchers:
            lines.append(
                "  - {launcher_id} room={conversation_id} mode={mode} default={default}".format(
                    launcher_id=item.get("id", ""),
                    conversation_id=item.get("conversation_id", ""),
                    mode=item.get("response_mode", ""),
                    default=item.get("default_qualified_handle", "") or "-",
                )
            )
    lines.append("")
    lines.append("Recent Chat Ingress:")
    if not ingress:
        lines.append("  (none)")
    else:
        for item in ingress:
            lines.append(
                "  - {ingress_id} binding={binding_id} status={status} from={from_display} preview={preview}".format(
                    ingress_id=item.get("ingress_id", ""),
                    binding_id=item.get("binding_id", ""),
                    status=item.get("status", ""),
                    from_display=item.get("from_display", ""),
                    preview=item.get("body_preview", ""),
                )
            )
    lines.append("")
    lines.append("Recent Chat Publishes:")
    if not publishes:
        lines.append("  (none)")
    else:
        for item in publishes:
            lines.append(
                "  - {publish_id} binding={binding_id} session={session_name} remote={remote_message_id}".format(
                    publish_id=item.get("publish_id", ""),
                    binding_id=item.get("binding_id", ""),
                    session_name=item.get("source_session_name", ""),
                    remote_message_id=item.get("remote_message_id", ""),
                )
            )
    lines.append("")
    lines.append("Recent Room Launches:")
    if not launches:
        lines.append("  (none)")
    else:
        for item in launches:
            lines.append(
                "  - {launch_id} room={conversation_id} handle={qualified_handle} thread={thread_id}".format(
                    launch_id=item.get("launch_id", ""),
                    conversation_id=item.get("conversation_id", ""),
                    qualified_handle=item.get("qualified_handle", ""),
                    thread_id=item.get("thread_id", "") or "(pending)",
                )
            )
    return "\n".join(lines)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Show Discord provider status")
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
