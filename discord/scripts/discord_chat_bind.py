#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

import discord_intake_common as common


def _optional_bool(enabled: bool, disabled: bool, *, enable_flag: str, disable_flag: str) -> bool | None:
    if enabled and disabled:
        raise SystemExit(f"choose only one of {enable_flag} or {disable_flag}")
    if enabled:
        return True
    if disabled:
        return False
    return None


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Bind a Discord conversation to one or more named sessions")
    parser.add_argument("--kind", required=True, choices=("dm", "room"), help="Binding kind")
    parser.add_argument("--app", default="", help="Optional named app identity")
    parser.add_argument("--guild-id", default="", help="Discord guild id")
    parser.add_argument("--enable-ambient-read", action="store_true", help="Accept unmentioned messages in a bound room")
    parser.add_argument("--disable-ambient-read", action="store_true", help="Disable unmentioned room intake")
    parser.add_argument(
        "--allow-untargeted-ambient-delivery",
        action="store_true",
        help="For a single-session ambient room, route untargeted messages to that bound session",
    )
    parser.add_argument(
        "--disallow-untargeted-ambient-delivery",
        action="store_true",
        help="Require explicit @session_name targeting for ambient room messages",
    )
    parser.add_argument("--enable-peer-fanout", action="store_true", help="Enable peer fanout for bound room publishes")
    parser.add_argument("--disable-peer-fanout", action="store_true", help="Disable peer fanout for bound room publishes")
    parser.add_argument(
        "--allow-untargeted-peer-fanout",
        action="store_true",
        help="Allow untargeted peer fanout inside a peer-enabled room",
    )
    parser.add_argument(
        "--disallow-untargeted-peer-fanout",
        action="store_true",
        help="Require explicit peer targets for peer fanout",
    )
    parser.add_argument("--max-peer-triggered-publishes-per-root", type=int, default=None)
    parser.add_argument("--max-total-peer-deliveries-per-root", type=int, default=None)
    parser.add_argument("--max-peer-triggered-publishes-per-session-per-minute", type=int, default=None)
    parser.add_argument(
        "--allow-bot-author",
        action="append",
        default=None,
        metavar="USER_ID",
        help="Accept inbound messages from this bot author in the bound room (repeatable; additive)",
    )
    parser.add_argument(
        "--remove-bot-author",
        action="append",
        default=None,
        metavar="USER_ID",
        help="Remove a previously allowed bot author (repeatable)",
    )
    parser.add_argument("--clear-bot-authors", action="store_true", help="Clear the allowed bot author list")
    parser.add_argument(
        "--enable-guild-bot-authors",
        action="store_true",
        help="Accept messages from ANY bot able to post in this bound channel (trust boundary = guild membership)",
    )
    parser.add_argument(
        "--disable-guild-bot-authors",
        action="store_true",
        help="Disable the guild-wide bot author wildcard",
    )
    parser.add_argument(
        "--max-accepted-bot-author-messages-per-minute",
        type=int,
        default=None,
        help="Anti-loop cap per (binding, bot author); 0 disables acceptance outright",
    )
    parser.add_argument("conversation_id", help="Discord DM, channel, or thread id")
    parser.add_argument("session_name", nargs="+", help="Gas City session name(s)")
    args = parser.parse_args(argv)

    ambient_read = _optional_bool(
        args.enable_ambient_read,
        args.disable_ambient_read,
        enable_flag="--enable-ambient-read",
        disable_flag="--disable-ambient-read",
    )
    untargeted_ambient_delivery = _optional_bool(
        args.allow_untargeted_ambient_delivery,
        args.disallow_untargeted_ambient_delivery,
        enable_flag="--allow-untargeted-ambient-delivery",
        disable_flag="--disallow-untargeted-ambient-delivery",
    )
    peer_fanout = _optional_bool(
        args.enable_peer_fanout,
        args.disable_peer_fanout,
        enable_flag="--enable-peer-fanout",
        disable_flag="--disable-peer-fanout",
    )
    untargeted_peer_fanout = _optional_bool(
        args.allow_untargeted_peer_fanout,
        args.disallow_untargeted_peer_fanout,
        enable_flag="--allow-untargeted-peer-fanout",
        disable_flag="--disallow-untargeted-peer-fanout",
    )
    guild_bot_authors = _optional_bool(
        args.enable_guild_bot_authors,
        args.disable_guild_bot_authors,
        enable_flag="--enable-guild-bot-authors",
        disable_flag="--disable-guild-bot-authors",
    )

    room_policy: dict[str, Any] = {}
    if ambient_read is not None:
        room_policy["ambient_read_enabled"] = ambient_read
    if untargeted_ambient_delivery is not None:
        room_policy["allow_untargeted_ambient_delivery"] = untargeted_ambient_delivery
    if peer_fanout is not None:
        room_policy["peer_fanout_enabled"] = peer_fanout
    if untargeted_peer_fanout is not None:
        room_policy["allow_untargeted_peer_fanout"] = untargeted_peer_fanout
    if args.max_peer_triggered_publishes_per_root is not None:
        room_policy["max_peer_triggered_publishes_per_root"] = args.max_peer_triggered_publishes_per_root
    if args.max_total_peer_deliveries_per_root is not None:
        room_policy["max_total_peer_deliveries_per_root"] = args.max_total_peer_deliveries_per_root
    if args.max_peer_triggered_publishes_per_session_per_minute is not None:
        room_policy["max_peer_triggered_publishes_per_session_per_minute"] = args.max_peer_triggered_publishes_per_session_per_minute
    if guild_bot_authors is not None:
        room_policy["allow_guild_bots"] = guild_bot_authors
    if args.max_accepted_bot_author_messages_per_minute is not None:
        room_policy["max_accepted_bot_author_messages_per_minute"] = args.max_accepted_bot_author_messages_per_minute
    if args.clear_bot_authors or args.allow_bot_author or args.remove_bot_author:
        # additive edit: start from the existing binding's list (set_chat_binding merges
        # dict keys but replaces list values wholesale)
        existing = common.resolve_chat_binding(
            common.load_config(), common.chat_binding_id(args.kind, args.conversation_id)
        )
        existing_policy = existing.get("policy") if isinstance(existing, dict) else None
        current: list[str] = []
        if not args.clear_bot_authors and isinstance(existing_policy, dict):
            current = [str(v).strip() for v in (existing_policy.get("allowed_bot_authors") or []) if str(v).strip()]
        for value in args.allow_bot_author or []:
            normalized = str(value).strip()
            if not normalized.isdigit():
                raise SystemExit(f"--allow-bot-author expects a Discord snowflake, got: {value!r}")
            if normalized not in current:
                current.append(normalized)
        for value in args.remove_bot_author or []:
            normalized = str(value).strip()
            current = [v for v in current if v != normalized]
        room_policy["allowed_bot_authors"] = current

    if args.kind != "room" and room_policy:
        raise SystemExit("room policy flags require --kind room")

    try:
        app_name = common.validate_app_name(args.app)
        loaded_config = common.load_config()
        channel_metadata: dict[str, Any] | None = None
        effective_guild_id = str(args.guild_id).strip()
        if app_name:
            common.resolve_app_config(loaded_config, app_name)
        if args.kind == "room":
            declared_policy_reason = common.outbound_policy_reason(
                loaded_config,
                effective_guild_id,
                args.conversation_id,
                app_name=app_name,
            )
            if declared_policy_reason == "guild_not_allowed" and effective_guild_id:
                display_name = app_name or "default"
                raise ValueError(f"Discord app {display_name!r} policy rejects binding: {declared_policy_reason}")
            app_policy = common.resolve_app_policy(loaded_config, app_name)
            has_outbound_policy = bool(
                app_policy.get("guild_allowlist")
                or app_policy.get("channel_allowlist")
            )
            if has_outbound_policy:
                bot_token = common.load_bot_token(app_name)
                if not bot_token:
                    display_name = app_name or "default"
                    raise ValueError(f"Discord bot token is not configured for app {display_name!r}")
                channel_scope = common.describe_room_channel_scope(
                    args.conversation_id,
                    bot_token=bot_token,
                )
                actual_guild_id = str(channel_scope.get("guild_id", "")).strip()
                if effective_guild_id and actual_guild_id and effective_guild_id != actual_guild_id:
                    raise ValueError(
                        f"--guild-id {effective_guild_id!r} does not match Discord channel guild {actual_guild_id!r}"
                    )
                effective_guild_id = actual_guild_id or effective_guild_id
                channel_metadata = common.normalize_binding_channel_metadata(channel_scope)
                parent_channel_id = (
                    str(channel_scope.get("thread_parent_id", "")).strip()
                    or args.conversation_id
                )
                policy_rejection = common.outbound_policy_reason(
                    loaded_config,
                    effective_guild_id,
                    parent_channel_id,
                    app_name=app_name,
                )
                if policy_rejection:
                    display_name = app_name or "default"
                    raise ValueError(f"Discord app {display_name!r} policy rejects binding: {policy_rejection}")
        config = common.set_chat_binding(
            loaded_config,
            args.kind,
            args.conversation_id,
            args.session_name,
            guild_id=effective_guild_id,
            app_name=app_name,
            policy=room_policy or None,
            channel_metadata=channel_metadata,
        )
    except (ValueError, common.DiscordAPIError) as exc:
        raise SystemExit(str(exc)) from exc

    binding = common.resolve_chat_binding(config, common.chat_binding_id(args.kind, args.conversation_id, app_name))
    print(json.dumps(binding or {}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
