#!/usr/bin/env python3

from __future__ import annotations

import base64
import calendar
import hashlib
import json
import os
import queue
import random
import re
import signal
import socket
import socketserver
import ssl
import struct
import threading
import time
import traceback
import urllib.parse
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler
from typing import Any

import discord_intake_common as common

GATEWAY_INTENTS = (1 << 0) | (1 << 9) | (1 << 12)
ALIAS_PATTERN = re.compile(r"(?<![A-Za-z0-9_<])@([a-z0-9][a-z0-9_-]*)", re.IGNORECASE)
DISCORD_RESERVED_MENTIONS = {"everyone", "here"}
NON_ROUTABLE_SESSION_STATES = {"", "closed", "stopped", "orphaned", "quarantined"}
MAX_STATUS_PREVIEW = 160
GATEWAY_WORKER_THREADS = 8
GATEWAY_MAX_PENDING_MESSAGES = 128
RECONNECT_BASE_DELAY_SECONDS = 5
RECONNECT_MAX_DELAY_SECONDS = 60
PRUNE_INTERVAL_SECONDS = 60
HEALTH_RECONNECT_GRACE_SECONDS = 90
GC_API_HEALTH_TTL_SECONDS = 30
GC_API_HEALTH_PROBE_TIMEOUT_SECONDS = 3.0
CHANNEL_INFO_TTL_SECONDS = 5 * 60
MAX_FRAME_BYTES = 16 * 1024 * 1024
STALE_PROCESSING_RECEIPT_SECONDS = 2 * 60
FAILED_RECEIPT_RETRY_SECONDS = 60
WEBSOCKET_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


class WebSocketClosed(RuntimeError):
    pass


class GatewayFrameTimeout(RuntimeError):
    pass


class ThreadingUnixHTTPServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True


CHANNEL_INFO_CACHE_LOCK = threading.Lock()
CHANNEL_INFO_FETCH_LOCKS_LOCK = threading.Lock()
CHANNEL_INFO_FETCH_LOCKS: dict[str, threading.Lock] = {}
CHANNEL_INFO_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
AMBIENT_ROOM_BINDINGS_CACHE_LOCK = threading.Lock()
AMBIENT_ROOM_BINDINGS_FETCH_LOCK = threading.Lock()
AMBIENT_ROOM_BINDINGS_CACHE: dict[str, Any] = {"config_signature": None, "bindings": {}}
STALE_RECLAIM_LOCKS_LOCK = threading.Lock()
STALE_RECLAIM_LOCKS: dict[str, threading.Lock] = {}
INGRESS_PROCESS_LOCKS_LOCK = threading.Lock()
INGRESS_PROCESS_LOCKS: dict[str, threading.Lock] = {}
GC_API_HEALTH_LOCK = threading.Lock()
GC_API_HEALTH_CACHE = {"checked_at": 0.0, "reachable": True}
WORKER_QUEUE_SENTINEL: tuple[dict[str, Any], str] | None = None


def participant_delivery_selector(participant: dict[str, Any]) -> str:
    for key in ("delivery_selector", "session_alias", "session_name", "session_id"):
        value = str((participant or {}).get(key, "")).strip()
        if value:
            return value
    return ""


def json_response(handler: BaseHTTPRequestHandler, status: int, payload: dict[str, Any]) -> None:
    body = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8") + b"\n"
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def text_response(handler: BaseHTTPRequestHandler, status: int, body: str, content_type: str) -> None:
    payload = body.encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", content_type)
    handler.send_header("Content-Length", str(len(payload)))
    handler.end_headers()
    handler.wfile.write(payload)


def summarize_body(value: str, limit: int = MAX_STATUS_PREVIEW) -> str:
    normalized = " ".join(str(value).split())
    if len(normalized) <= limit:
        return normalized
    return normalized[:limit].rstrip() + "..."


def display_name_from_message(message: dict[str, Any]) -> str:
    member = message.get("member") or {}
    user = message.get("author") or {}
    candidates: list[str] = []
    for raw in (
        member.get("nick"),
        user.get("global_name"),
        user.get("username"),
    ):
        if raw is None:
            continue
        value = str(raw).strip()
        if value:
            candidates.append(value)
    for value in candidates:
        normalized = " ".join(
            value.replace("\r", " ").replace("\n", " ").replace("<", " ").replace(">", " ").split()
        )
        if normalized:
            return normalized
    return "discord-user"


def raw_message_content(message: dict[str, Any]) -> str:
    value = message.get("content", "")
    if isinstance(value, str):
        return value
    if value is None:
        return ""
    return str(value)


def bot_was_mentioned(message: dict[str, Any], bot_user_id: str) -> bool:
    if not bot_user_id:
        return False
    mentions = message.get("mentions") or []
    if not isinstance(mentions, list):
        return False
    return any(str(item.get("id", "")).strip() == bot_user_id for item in mentions if isinstance(item, dict))


def websocket_accept_value(key: str) -> str:
    digest = hashlib.sha1((str(key) + WEBSOCKET_GUID).encode("utf-8")).digest()
    return base64.b64encode(digest).decode("ascii")


def validate_websocket_handshake(header_blob: str, key: str) -> None:
    lines = header_blob.splitlines()
    status_line = lines[0] if lines else ""
    if "101" not in status_line:
        raise RuntimeError(f"websocket handshake failed: {status_line}")
    headers: dict[str, str] = {}
    for line in lines[1:]:
        if ":" not in line:
            continue
        name, value = line.split(":", 1)
        headers[name.strip().lower()] = value.strip()
    if headers.get("upgrade", "").lower() != "websocket":
        raise RuntimeError("websocket handshake missing Upgrade: websocket")
    connection_tokens = {token.strip().lower() for token in headers.get("connection", "").split(",") if token.strip()}
    if "upgrade" not in connection_tokens:
        raise RuntimeError("websocket handshake missing Connection: Upgrade")
    accept_value = headers.get("sec-websocket-accept", "")
    if accept_value != websocket_accept_value(key):
        raise RuntimeError("websocket handshake returned an unexpected Sec-WebSocket-Accept")


def strip_bot_mentions(content: str, bot_user_id: str) -> str:
    if not bot_user_id:
        return " ".join(content.split())
    pattern = re.compile(rf"<@!?{re.escape(bot_user_id)}>\s*", re.IGNORECASE)
    stripped = pattern.sub("", content)
    return " ".join(stripped.split())


def extract_alias_mentions(content: str) -> list[str]:
    seen: set[str] = set()
    aliases: list[str] = []
    for match in ALIAS_PATTERN.finditer(content):
        alias = str(match.group(1) or "").strip().lower()
        if alias and alias not in seen and alias not in DISCORD_RESERVED_MENTIONS:
            seen.add(alias)
            aliases.append(alias)
    return aliases


def referenced_message_id(message: dict[str, Any]) -> str:
    reference = message.get("message_reference")
    if not isinstance(reference, dict):
        return ""
    return str(reference.get("message_id", "")).strip()


def casefold_lookup(values: list[str]) -> tuple[dict[str, str], set[str]]:
    lookup: dict[str, str] = {}
    collisions: set[str] = set()
    for value in values:
        normalized = str(value).strip()
        if not normalized:
            continue
        key = normalized.casefold()
        existing = lookup.get(key)
        if existing and existing != normalized:
            collisions.add(key)
            continue
        lookup[key] = normalized
    return lookup, collisions


def message_ingress_id(message: dict[str, Any]) -> str:
    message_id = str(message.get("id", "")).strip()
    if message_id:
        return f"in-{message_id}"
    return f"in-{int(time.time() * 1000)}"


def conversation_fields(message: dict[str, Any], channel_info: dict[str, Any]) -> tuple[str, str]:
    guild_id = str(message.get("guild_id", "")).strip()
    channel_id = str(message.get("channel_id", "")).strip()
    parent_id = str(channel_info.get("parent_id", "")).strip()
    if not guild_id:
        return f"dm:{channel_id}", f"dm:{channel_id}"
    if parent_id and parent_id != channel_id:
        return (
            f"guild:{guild_id} channel:{parent_id} thread:{channel_id}",
            f"guild:{guild_id}:conversation:{channel_id}",
        )
    return (
        f"guild:{guild_id} channel:{channel_id}",
        f"guild:{guild_id}:conversation:{channel_id}",
    )


def ingress_preview(message: dict[str, Any], bot_user_id: str) -> str:
    return summarize_body(strip_bot_mentions(str(message.get("content", "")), bot_user_id))


def fetch_message_via_rest(channel_id: str, message_id: str) -> dict[str, Any]:
    normalized_channel_id = str(channel_id).strip()
    normalized_message_id = str(message_id).strip()
    if not normalized_channel_id or not normalized_message_id:
        return {}
    quoted_channel = urllib.parse.quote(normalized_channel_id)
    quoted_message = urllib.parse.quote(normalized_message_id)
    try:
        payload = common.discord_api_request("GET", f"/channels/{quoted_channel}/messages/{quoted_message}")
        if isinstance(payload, dict) and str(payload.get("id", "")).strip() == normalized_message_id:
            return payload
    except common.DiscordAPIError as exc:
        if int(getattr(exc, "status_code", 0) or 0) != 404:
            return {}
    except Exception:
        return {}
    try:
        payload = common.discord_api_request(
            "GET",
            f"/channels/{quoted_channel}/messages?around={quoted_message}&limit=3",
        )
    except common.DiscordAPIError:
        return {}
    except Exception:
        return {}
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict) and str(item.get("id", "")).strip() == normalized_message_id:
                return item
    return {}


def recover_message_for_routing(message: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    recovered = dict(message)
    gateway_content = raw_message_content(message)
    debug = {
        "gateway_content_length": len(gateway_content),
        "rest_content_length": 0,
        "content_source": "gateway",
        "rest_fetch_attempted": False,
        "rest_fetch_succeeded": False,
    }
    guild_id = str(message.get("guild_id", "")).strip()
    channel_id = str(message.get("channel_id", "")).strip()
    message_id = str(message.get("id", "")).strip()
    needs_rest = bool(guild_id and channel_id and message_id and not gateway_content.strip())
    if not needs_rest:
        return recovered, debug
    debug["rest_fetch_attempted"] = True
    fetched = fetch_message_via_rest(channel_id, message_id)
    if not isinstance(fetched, dict) or not fetched:
        debug["content_source"] = "gateway_empty_rest_unavailable"
        return recovered, debug
    fetched_content = raw_message_content(fetched)
    debug["rest_content_length"] = len(fetched_content)
    debug["rest_fetch_succeeded"] = True
    if fetched_content:
        recovered["content"] = fetched_content
        debug["content_source"] = "rest_fallback"
    else:
        debug["content_source"] = "gateway_empty_rest_empty"
    for key in ("mentions", "message_reference", "member"):
        current = recovered.get(key)
        if current in (None, "", [], {}):
            fetched_value = fetched.get(key)
            if fetched_value not in (None, "", [], {}):
                recovered[key] = fetched_value
    if display_name_from_message(recovered) == "discord-user":
        fetched_author = fetched.get("author")
        if fetched_author not in (None, "", [], {}):
            recovered["author"] = fetched_author
        fetched_member = fetched.get("member")
        if fetched_member not in (None, "", [], {}):
            recovered["member"] = fetched_member
    return recovered, debug


def empty_body_reason(message: dict[str, Any], message_debug: dict[str, Any] | None = None) -> str:
    raw_content = raw_message_content(message)
    if raw_content.strip():
        return "empty_after_bot_mention_strip"
    if str(message.get("guild_id", "")).strip():
        debug = message_debug or {}
        source = str(debug.get("content_source", "")).strip()
        if source in {"gateway_empty_rest_unavailable", "gateway_empty_rest_empty", "gateway"}:
            return "message_content_unavailable"
    return "empty_message_content"


def utc_age_seconds(value: str) -> float:
    normalized = str(value).strip()
    if not normalized:
        return float("inf")
    try:
        parsed = time.strptime(normalized, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return float("inf")
    return max(time.time() - calendar.timegm(parsed), 0.0)


def normalize_channel_info(info: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(info, dict):
        return {}
    normalized = dict(info)
    has_channel_type = "channel_type" in normalized or "type" in normalized
    channel_type_raw = normalized.get("channel_type", normalized.get("type", 0))
    try:
        channel_type = int(channel_type_raw or 0)
    except (TypeError, ValueError):
        channel_type = 0
    if has_channel_type:
        normalized["type"] = channel_type
    if not has_channel_type:
        return normalized
    if channel_type not in common.THREAD_CHANNEL_TYPES:
        normalized.pop("parent_id", None)
        return normalized
    parent_id = str(normalized.get("parent_id", "")).strip()
    if not parent_id:
        normalized.pop("parent_id", None)
        return normalized
    normalized["parent_id"] = parent_id
    return normalized


def binding_channel_info(binding: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(binding, dict):
        return {}
    channel_type_raw = binding.get("channel_type", 0)
    try:
        channel_type = int(channel_type_raw or 0)
    except (TypeError, ValueError):
        channel_type = 0
    if channel_type not in common.THREAD_CHANNEL_TYPES:
        return {}
    parent_id = str(binding.get("thread_parent_id", "")).strip()
    if not parent_id:
        return {}
    return {"type": channel_type, "parent_id": parent_id}


def persist_binding_channel_metadata(binding: dict[str, Any]) -> None:
    if str(binding.get("kind", "")).strip() != "room":
        return
    channel_metadata = common.normalize_binding_channel_metadata(binding)
    if not channel_metadata:
        return
    conversation_id = str(binding.get("conversation_id", "")).strip()
    if not conversation_id:
        return
    try:
        common.save_channel_metadata_cache(conversation_id, channel_metadata)
    except (ValueError, OSError, json.JSONDecodeError):
        return


def load_channel_info(channel_id: str, bot_token: str) -> dict[str, Any]:
    now = time.monotonic()
    with CHANNEL_INFO_CACHE_LOCK:
        cached = CHANNEL_INFO_CACHE.get(channel_id)
        if cached and cached[0] > now:
            return dict(cached[1])
    # Serialize cache fills so a burst of uncached thread lookups does not fan out
    # into concurrent Discord API reads for the same class of metadata.
    with channel_info_fetch_lock(channel_id):
        now = time.monotonic()
        with CHANNEL_INFO_CACHE_LOCK:
            cached = CHANNEL_INFO_CACHE.get(channel_id)
            if cached and cached[0] > now:
                return dict(cached[1])
        info = common.discord_api_request("GET", f"/channels/{urllib.parse.quote(channel_id)}", bot_token=bot_token)
        if isinstance(info, dict):
            info = normalize_channel_info(info)
            with CHANNEL_INFO_CACHE_LOCK:
                CHANNEL_INFO_CACHE[channel_id] = (now + CHANNEL_INFO_TTL_SECONDS, dict(info))
            return dict(info)
    return {}


def stale_reclaim_lock(ingress_id: str) -> threading.Lock:
    with STALE_RECLAIM_LOCKS_LOCK:
        lock = STALE_RECLAIM_LOCKS.get(ingress_id)
        if lock is None:
            lock = threading.Lock()
            STALE_RECLAIM_LOCKS[ingress_id] = lock
        return lock


def channel_info_fetch_lock(channel_id: str) -> threading.Lock:
    with CHANNEL_INFO_FETCH_LOCKS_LOCK:
        lock = CHANNEL_INFO_FETCH_LOCKS.get(channel_id)
        if lock is None:
            lock = threading.Lock()
            CHANNEL_INFO_FETCH_LOCKS[channel_id] = lock
        return lock


def ingress_process_lock(ingress_id: str) -> threading.Lock:
    with INGRESS_PROCESS_LOCKS_LOCK:
        lock = INGRESS_PROCESS_LOCKS.get(ingress_id)
        if lock is None:
            lock = threading.Lock()
            INGRESS_PROCESS_LOCKS[ingress_id] = lock
        return lock


def prune_channel_info_cache() -> None:
    now = time.monotonic()
    with CHANNEL_INFO_CACHE_LOCK:
        expired = [key for key, (expires_at, _) in CHANNEL_INFO_CACHE.items() if expires_at <= now]
        for key in expired:
            del CHANNEL_INFO_CACHE[key]


def prune_channel_info_fetch_locks() -> None:
    with CHANNEL_INFO_CACHE_LOCK:
        cached_keys = set(CHANNEL_INFO_CACHE.keys())
    with CHANNEL_INFO_FETCH_LOCKS_LOCK:
        expired = [key for key, lock in CHANNEL_INFO_FETCH_LOCKS.items() if not lock.locked() and key not in cached_keys]
        for key in expired:
            del CHANNEL_INFO_FETCH_LOCKS[key]


def prune_stale_reclaim_locks() -> None:
    with STALE_RECLAIM_LOCKS_LOCK:
        expired = [key for key, lock in STALE_RECLAIM_LOCKS.items() if not lock.locked() and common.load_chat_ingress(key) is None]
        for key in expired:
            del STALE_RECLAIM_LOCKS[key]


def prune_ingress_process_locks() -> None:
    with INGRESS_PROCESS_LOCKS_LOCK:
        expired = [key for key, lock in INGRESS_PROCESS_LOCKS.items() if not lock.locked() and common.load_chat_ingress(key) is None]
        for key in expired:
            del INGRESS_PROCESS_LOCKS[key]


def probe_gc_api_health(runtime_state: "GatewayRuntimeState") -> bool:
    now = time.monotonic()
    with GC_API_HEALTH_LOCK:
        checked_at = float(GC_API_HEALTH_CACHE.get("checked_at", 0.0) or 0.0)
        if checked_at and (now - checked_at) < GC_API_HEALTH_TTL_SECONDS:
            return bool(GC_API_HEALTH_CACHE.get("reachable", True))
    try:
        common.gc_api_request(
            "GET",
            "/v0/sessions?limit=1&state=all",
            timeout=GC_API_HEALTH_PROBE_TIMEOUT_SECONDS,
        )
    except common.GCAPIError as exc:
        with GC_API_HEALTH_LOCK:
            GC_API_HEALTH_CACHE["checked_at"] = now
            GC_API_HEALTH_CACHE["reachable"] = False
        runtime_state.patch(last_gc_api_error=str(exc), last_gc_api_error_at=common.utcnow())
        return False
    with GC_API_HEALTH_LOCK:
        GC_API_HEALTH_CACHE["checked_at"] = now
        GC_API_HEALTH_CACHE["reachable"] = True
    runtime_state.patch(last_gc_api_error="", last_gc_api_ok_at=common.utcnow())
    return True


def resolve_binding(config: dict[str, Any], message: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    guild_id = str(message.get("guild_id", "")).strip()
    channel_id = str(message.get("channel_id", "")).strip()
    channel_info: dict[str, Any] = {}
    binding_id = common.chat_binding_id("dm" if not guild_id else "room", channel_id)
    binding = common.resolve_chat_binding(config, binding_id)
    if not guild_id:
        return binding, channel_info
    if binding:
        binding = dict(binding)
        if binding_allows_ambient_read(binding):
            cached_binding = cached_ambient_room_binding(channel_id)
            if cached_binding:
                binding = cached_binding
        channel_info = binding_channel_info(binding)
        if channel_info:
            return binding, channel_info
        cached_channel_metadata = common.load_channel_metadata_cache(channel_id)
        if cached_channel_metadata:
            binding.update(cached_channel_metadata)
            channel_info = binding_channel_info(binding)
            if channel_info:
                return binding, channel_info
        channel_type_raw = binding.get("channel_type", None)
        if channel_type_raw is None:
            bot_token = common.load_bot_token()
            if not bot_token:
                return binding, {}
            try:
                looked_up_channel_info = load_channel_info(channel_id, bot_token)
            except common.DiscordAPIError:
                return binding, {}
            binding.update(common.normalize_binding_channel_metadata(looked_up_channel_info))
            persist_binding_channel_metadata(binding)
            return binding, binding_channel_info(binding)
        try:
            channel_type = int(channel_type_raw or 0)
        except (TypeError, ValueError):
            channel_type = 0
        if channel_type not in common.THREAD_CHANNEL_TYPES:
            return binding, {}
        bot_token = common.load_bot_token()
        if not bot_token:
            return binding, channel_info
        try:
            looked_up_channel_info = load_channel_info(channel_id, bot_token)
        except common.DiscordAPIError:
            return binding, {}
        binding.update(common.normalize_binding_channel_metadata(looked_up_channel_info))
        persist_binding_channel_metadata(binding)
        return binding, binding_channel_info(binding)
    bot_token = common.load_bot_token()
    if not bot_token:
        return None, channel_info
    try:
        channel_info = load_channel_info(channel_id, bot_token)
    except common.DiscordAPIError as exc:
        if exc.status_code == 404:
            return None, {}
        raise
    if not isinstance(channel_info, dict):
        return common.resolve_chat_binding(config, common.chat_binding_id("room", channel_id)), {}
    parent_id = str(channel_info.get("parent_id", "")).strip()
    if parent_id and parent_id != channel_id:
        binding = common.resolve_chat_binding(config, common.chat_binding_id("room", parent_id))
        if binding:
            return binding, channel_info
    return common.resolve_chat_binding(config, common.chat_binding_id("room", channel_id)), channel_info


def resolve_targets(
    binding: dict[str, Any],
    session_index: dict[str, dict[str, Any]],
    mentioned_aliases: list[str],
    *,
    require_targeted_aliases: bool = False,
) -> tuple[list[str], str, str]:
    participants = [str(item).strip() for item in binding.get("session_names", []) if str(item).strip()]
    participant_lookup, participant_collisions = casefold_lookup(participants)
    _, session_collisions = casefold_lookup(list(session_index.keys()))
    if mentioned_aliases:
        for alias in mentioned_aliases:
            key = alias.casefold()
            if key in participant_collisions or key in session_collisions:
                return [], "targeted", f"ambiguous_alias:{alias}"
            participant_name = participant_lookup.get(key)
            if not participant_name:
                return [], "targeted", f"unknown_alias:{alias}"
        targets: list[str] = []
        for alias in mentioned_aliases:
            participant_name = participant_lookup.get(alias.casefold())
            if not participant_name:
                return [], "targeted", f"unknown_alias:{alias}"
            session_payload = session_index.get(participant_name)
            state = str((session_payload or {}).get("state", "")).strip()
            if not session_payload or state in NON_ROUTABLE_SESSION_STATES:
                return [], "targeted", f"unavailable_alias:{alias}"
            targets.append(participant_name)
        return targets, "targeted", ""

    if require_targeted_aliases:
        return [], "targeted", "target_required"

    targets = []
    for alias in participants:
        session_payload = session_index.get(alias)
        state = str((session_payload or {}).get("state", "")).strip()
        if session_payload and state not in NON_ROUTABLE_SESSION_STATES:
            targets.append(alias)
    return targets, "broadcast", ""


def binding_allows_ambient_read(binding: dict[str, Any] | None) -> bool:
    if not isinstance(binding, dict):
        return False
    if str(binding.get("kind", "")).strip() != "room":
        return False
    return bool(common.binding_peer_policy(binding).get("ambient_read_enabled"))


def ambient_bindings_config_signature() -> tuple[int, int, int] | None:
    try:
        stat_result = os.stat(common.config_path())
    except OSError:
        return None
    return (
        int(getattr(stat_result, "st_mtime_ns", 0)),
        int(getattr(stat_result, "st_size", 0)),
        int(getattr(stat_result, "st_ino", 0)),
    )


def cached_ambient_room_binding(channel_id: str) -> dict[str, Any] | None:
    config_signature = ambient_bindings_config_signature()
    with AMBIENT_ROOM_BINDINGS_CACHE_LOCK:
        if AMBIENT_ROOM_BINDINGS_CACHE.get("config_signature") == config_signature:
            bindings = AMBIENT_ROOM_BINDINGS_CACHE.get("bindings", {})
            if isinstance(bindings, dict):
                binding = bindings.get(channel_id)
                return dict(binding) if isinstance(binding, dict) else None

    with AMBIENT_ROOM_BINDINGS_FETCH_LOCK:
        config_signature = ambient_bindings_config_signature()
        with AMBIENT_ROOM_BINDINGS_CACHE_LOCK:
            if AMBIENT_ROOM_BINDINGS_CACHE.get("config_signature") == config_signature:
                bindings = AMBIENT_ROOM_BINDINGS_CACHE.get("bindings", {})
                if isinstance(bindings, dict):
                    binding = bindings.get(channel_id)
                    return dict(binding) if isinstance(binding, dict) else None

        bindings: dict[str, dict[str, Any]] = {}
        try:
            config = common.load_config()
        except (OSError, ValueError, json.JSONDecodeError):
            config = {}
        for binding in common.list_chat_bindings(config):
            if str(binding.get("kind", "")).strip() != "room":
                continue
            if not binding_allows_ambient_read(binding):
                continue
            conversation_id = str(binding.get("conversation_id", "")).strip()
            if conversation_id:
                bindings[conversation_id] = dict(binding)

        with AMBIENT_ROOM_BINDINGS_CACHE_LOCK:
            AMBIENT_ROOM_BINDINGS_CACHE["config_signature"] = config_signature
            AMBIENT_ROOM_BINDINGS_CACHE["bindings"] = bindings
    binding = bindings.get(channel_id)
    return dict(binding) if isinstance(binding, dict) else None


def build_human_envelope(
    *,
    binding: dict[str, Any],
    message: dict[str, Any],
    channel_info: dict[str, Any],
    body: str,
    mentioned_aliases: list[str],
    delivery: str,
    ingress_id: str,
) -> str:
    conversation_value, conversation_key = conversation_fields(message, channel_info)
    lines = [
        "<discord-event>",
        "version: 1",
        "kind: discord_human_message",
        f"binding_id: {str(binding.get('id', '')).strip()}",
        f"ingress_receipt_id: {ingress_id}",
        f"conversation: {conversation_value}",
        f"conversation_key: {conversation_key}",
        f"discord_message_id: {str(message.get('id', '')).strip()}",
        f"from_display: {display_name_from_message(message)}",
        f"from_user_id: {str((message.get('author') or {}).get('id', '')).strip()}",
        f"delivery: {delivery}",
        f"mentioned_aliases_json: {json.dumps(mentioned_aliases)}",
        f"untrusted_body_json: {json.dumps(body)}",
        f"publish_binding_id: {str(binding.get('id', '')).strip()}",
        f"publish_conversation_id: {str(message.get('channel_id', '')).strip()}",
        f"publish_trigger_id: {str(message.get('id', '')).strip()}",
        f"publish_reply_to_discord_message_id: {str(message.get('id', '')).strip()}",
        "normal_output_visibility: internal_only",
        "reply_contract: explicit_publish_required",
        "</discord-event>",
    ]
    return "\n".join(lines)


def build_room_launch_envelope(
    *,
    launcher: dict[str, Any],
    launch: dict[str, Any],
    message: dict[str, Any],
    body: str,
    mentioned_handles: list[str],
    ingress_id: str,
) -> str:
    guild_id = str(message.get("guild_id", "")).strip()
    channel_id = str(message.get("channel_id", "")).strip()
    lines = [
        "<discord-event>",
        "version: 1",
        "kind: discord_human_message",
        f"binding_id: {str(launcher.get('id', '')).strip()}",
        f"ingress_receipt_id: {ingress_id}",
        f"conversation: guild:{guild_id} channel:{channel_id}",
        f"conversation_key: guild:{guild_id}:conversation:{channel_id}",
        f"discord_message_id: {str(message.get('id', '')).strip()}",
        f"from_display: {display_name_from_message(message)}",
        f"from_user_id: {str((message.get('author') or {}).get('id', '')).strip()}",
        "delivery: targeted",
        f"mentioned_handles_json: {json.dumps(mentioned_handles)}",
        f"launch_id: {str(launch.get('launch_id', '')).strip()}",
        "launch_surface_kind: room",
        f"launch_qualified_handle: {str(launch.get('qualified_handle', '')).strip()}",
        f"launch_session_alias: {str(launch.get('session_alias', '')).strip()}",
        f"launch_session_name: {str(launch.get('session_name', '')).strip()}",
        f"thread_participants_json: {json.dumps(common.room_launch_participant_summaries(launch))}",
        f"untrusted_body_json: {json.dumps(body)}",
        f"publish_binding_id: {str(launcher.get('id', '')).strip()}",
        f"publish_conversation_id: {channel_id}",
        f"publish_trigger_id: {str(message.get('id', '')).strip()}",
        f"publish_launch_id: {str(launch.get('launch_id', '')).strip()}",
        "normal_output_visibility: internal_only",
        "reply_contract: explicit_publish_required",
        "</discord-event>",
    ]
    return "\n".join(lines)


def build_room_launch_thread_envelope(
    *,
    launcher: dict[str, Any],
    launch: dict[str, Any],
    target_participant: dict[str, Any],
    message: dict[str, Any],
    body: str,
    mentioned_handles: list[str],
    ingress_id: str,
    routing_mode: str,
    reply_to_id: str,
) -> str:
    guild_id = str(message.get("guild_id", "")).strip()
    channel_id = str(message.get("channel_id", "")).strip()
    parent_id = str(launch.get("conversation_id", "")).strip()
    target_qualified_handle = str(target_participant.get("qualified_handle", "")).strip() or str(launch.get("qualified_handle", "")).strip()
    target_session_alias = str(target_participant.get("session_alias", "")).strip() or str(launch.get("session_alias", "")).strip()
    target_session_name = str(target_participant.get("session_name", "")).strip()
    lines = [
        "<discord-event>",
        "version: 1",
        "kind: discord_human_message",
        f"binding_id: {str(launcher.get('id', '')).strip()}",
        f"ingress_receipt_id: {ingress_id}",
        f"conversation: guild:{guild_id} channel:{parent_id} thread:{channel_id}",
        f"conversation_key: guild:{guild_id}:conversation:{channel_id}",
        f"discord_message_id: {str(message.get('id', '')).strip()}",
        f"from_display: {display_name_from_message(message)}",
        f"from_user_id: {str((message.get('author') or {}).get('id', '')).strip()}",
        "delivery: targeted",
        f"routing_mode: {routing_mode}",
        f"reply_to_discord_message_id: {reply_to_id}",
        f"mentioned_handles_json: {json.dumps(mentioned_handles)}",
        f"launch_id: {str(launch.get('launch_id', '')).strip()}",
        "launch_surface_kind: room",
        f"launch_root_qualified_handle: {str(launch.get('qualified_handle', '')).strip()}",
        f"launch_root_session_alias: {str(launch.get('session_alias', '')).strip()}",
        f"launch_qualified_handle: {target_qualified_handle}",
        f"launch_session_alias: {target_session_alias}",
        f"launch_session_name: {target_session_name}",
        f"thread_participants_json: {json.dumps(common.room_launch_participant_summaries(launch))}",
        f"untrusted_body_json: {json.dumps(body)}",
        f"publish_binding_id: {str(launcher.get('id', '')).strip()}",
        f"publish_conversation_id: {channel_id}",
        f"publish_trigger_id: {str(message.get('id', '')).strip()}",
        f"publish_reply_to_discord_message_id: {str(message.get('id', '')).strip()}",
        f"publish_launch_id: {str(launch.get('launch_id', '')).strip()}",
        "normal_output_visibility: internal_only",
        "reply_contract: explicit_publish_required",
        "</discord-event>",
    ]
    return "\n".join(lines)


def persist_ingress_receipt(payload: dict[str, Any]) -> dict[str, Any]:
    return common.save_chat_ingress(payload)


def save_rejected_ingress_receipt(
    message: dict[str, Any],
    bot_user_id: str,
    *,
    status: str,
    reason: str,
    message_debug: dict[str, Any] | None = None,
) -> tuple[bool, dict[str, Any]]:
    ingress_id = message_ingress_id(message)
    return common.save_chat_ingress_if_absent(
        {
            "ingress_id": ingress_id,
            "discord_message_id": str(message.get("id", "")).strip(),
            "guild_id": str(message.get("guild_id", "")).strip(),
            "conversation_id": str(message.get("channel_id", "")).strip(),
            "binding_id": "",
            "from_user_id": str((message.get("author") or {}).get("id", "")).strip(),
            "from_display": display_name_from_message(message),
            "body_preview": ingress_preview(message, bot_user_id),
            "status": status,
            "reason": reason,
            "message_debug": dict(message_debug or {}),
            "targets": [],
        }
    )


def reject_ingress_before_processing(
    message: dict[str, Any],
    bot_user_id: str,
    *,
    status: str,
    reason: str,
    message_debug: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ingress_id = message_ingress_id(message)
    claimed, receipt = save_rejected_ingress_receipt(
        message,
        bot_user_id,
        status=status,
        reason=reason,
        message_debug=message_debug,
    )
    if claimed:
        return {"status": status, "reason": reason, "ingress_id": ingress_id, "receipt": receipt}
    if str(receipt.get("status", "")).strip() == "claim_conflict_unreadable":
        receipt = persist_ingress_receipt(
            {
                **receipt,
                "ingress_id": ingress_id,
                "discord_message_id": str(message.get("id", "")).strip(),
                "guild_id": str(message.get("guild_id", "")).strip(),
                "conversation_id": str(message.get("channel_id", "")).strip(),
                "binding_id": "",
                "from_user_id": str((message.get("author") or {}).get("id", "")).strip(),
                "from_display": display_name_from_message(message),
                "body_preview": ingress_preview(message, bot_user_id),
                "status": "failed_claim_conflict",
                "reason": str(receipt.get("reason", "")).strip() or "ingress_claim_unreadable",
                "message_debug": dict(message_debug or {}),
                "targets": [],
            }
        )
        return {"status": "failed_claim_conflict", "ingress_id": ingress_id, "receipt": receipt}
    return {"status": "duplicate", "ingress_id": ingress_id, "receipt": receipt}


def process_room_launch_message(
    *,
    base_receipt: dict[str, Any],
    launcher: dict[str, Any],
    message: dict[str, Any],
    bot_user_id: str,
    ingress_id: str,
    message_debug: dict[str, Any] | None = None,
) -> dict[str, Any]:
    body = strip_bot_mentions(str(message.get("content", "")), bot_user_id)
    if not body:
        receipt = persist_ingress_receipt(
            {
                **base_receipt,
                "binding_id": str(launcher.get("id", "")).strip(),
                "status": "ignored_empty",
                "reason": empty_body_reason(message, message_debug),
                "message_debug": dict(message_debug or {}),
                "targets": [],
            }
        )
        return {"status": "ignored_empty", "ingress_id": ingress_id, "receipt": receipt}

    mentioned_handles = common.extract_agent_handles(body)
    response_mode = str(launcher.get("response_mode", "mention_only")).strip() or "mention_only"
    reply_to_id = referenced_message_id(message)
    if len(mentioned_handles) > 1:
        receipt = persist_ingress_receipt(
            {
                **base_receipt,
                "binding_id": str(launcher.get("id", "")).strip(),
                "status": "rejected_targeting",
                "reason": "multiple_handles_not_supported",
                "mentioned_handles": mentioned_handles,
                "targets": [],
            }
        )
        return {"status": "rejected_targeting", "ingress_id": ingress_id, "receipt": receipt}

    requested_handle = mentioned_handles[0] if mentioned_handles else ""
    used_default_handle = False
    if not requested_handle:
        if response_mode != "respond_all":
            receipt = persist_ingress_receipt(
                {
                    **base_receipt,
                    "binding_id": str(launcher.get("id", "")).strip(),
                    "status": "ignored_untargeted",
                    "reason": "launch_handle_required",
                    "targets": [],
                }
            )
            return {"status": "ignored_untargeted", "ingress_id": ingress_id, "receipt": receipt}
        if reply_to_id:
            receipt = persist_ingress_receipt(
                {
                    **base_receipt,
                    "binding_id": str(launcher.get("id", "")).strip(),
                    "status": "ignored_untargeted",
                    "reason": "respond_all_root_reply_requires_handle",
                    "targets": [],
                }
            )
            return {"status": "ignored_untargeted", "ingress_id": ingress_id, "receipt": receipt}
        requested_handle = str(launcher.get("default_qualified_handle", "")).strip()
        if not requested_handle:
            receipt = persist_ingress_receipt(
                {
                    **base_receipt,
                    "binding_id": str(launcher.get("id", "")).strip(),
                    "status": "ignored_untargeted",
                    "reason": "launch_handle_required",
                    "targets": [],
                }
            )
            return {"status": "ignored_untargeted", "ingress_id": ingress_id, "receipt": receipt}
        used_default_handle = True

    if used_default_handle:
        qualified_handle, resolve_error = requested_handle, ""
    else:
        try:
            qualified_handle, resolve_error = common.resolve_agent_handle(requested_handle)
        except common.GCAPIError as exc:
            receipt = persist_ingress_receipt(
                {
                    **base_receipt,
                    "binding_id": str(launcher.get("id", "")).strip(),
                    "status": "failed_lookup",
                    "reason": str(exc),
                    "targets": [],
                }
            )
            return {"status": "failed_lookup", "ingress_id": ingress_id, "receipt": receipt}
    if resolve_error:
        receipt = persist_ingress_receipt(
            {
                **base_receipt,
                "binding_id": str(launcher.get("id", "")).strip(),
                "status": "rejected_targeting",
                "reason": resolve_error,
                "mentioned_handles": mentioned_handles,
                "targets": [],
            }
        )
        return {"status": "rejected_targeting", "ingress_id": ingress_id, "receipt": receipt}

    launch_id = common.room_launch_record_id(str(message.get("id", "")).strip())
    existing_launch = common.load_room_launch(launch_id) or {}
    launch = common.save_room_launch(
        {
            **existing_launch,
            "launch_id": launch_id,
            "state": "pending_thread" if not str(existing_launch.get("thread_id", "")).strip() else str(existing_launch.get("state", "")).strip() or "active",
            "launcher_id": str(launcher.get("id", "")).strip(),
            "guild_id": str(message.get("guild_id", "")).strip(),
            "conversation_id": str(message.get("channel_id", "")).strip(),
            "root_message_id": str(message.get("id", "")).strip(),
            "qualified_handle": qualified_handle,
            "session_alias": str(existing_launch.get("session_alias", "")).strip()
            or common.room_launch_session_alias(
                str(message.get("guild_id", "")).strip(),
                str(message.get("channel_id", "")).strip(),
                str(message.get("id", "")).strip(),
                qualified_handle,
            ),
            "from_user_id": str((message.get("author") or {}).get("id", "")).strip(),
            "from_display": display_name_from_message(message),
            "body_preview": ingress_preview(message, bot_user_id),
        }
    )
    try:
        launch = common.ensure_room_launch_session(launch)
    except (ValueError, common.GCAPIError) as exc:
        receipt = persist_ingress_receipt(
            {
                **base_receipt,
                "binding_id": str(launcher.get("id", "")).strip(),
                "status": "failed_lookup",
                "reason": str(exc),
                "mentioned_handles": mentioned_handles,
                "targets": [],
            }
        )
        return {"status": "failed_lookup", "ingress_id": ingress_id, "receipt": receipt}

    target_selector = participant_delivery_selector(launch)
    receipt = persist_ingress_receipt(
        {
            **base_receipt,
            "binding_id": str(launcher.get("id", "")).strip(),
            "status": "pending",
            "delivery": "targeted",
            "route_kind": "room_launch",
            "launch_id": launch_id,
            "mentioned_handles": mentioned_handles,
            "qualified_handle": qualified_handle,
            "targets": [{"session_name": target_selector, "status": "pending"}],
        }
    )
    envelope = build_room_launch_envelope(
        launcher=launcher,
        launch=launch,
        message=message,
        body=body,
        mentioned_handles=mentioned_handles,
        ingress_id=ingress_id,
    )
    try:
        response = common.deliver_session_message(
            target_selector,
            envelope,
            idempotency_key=f"ingress:{ingress_id}:target:{target_selector}",
        )
    except common.GCAPIError as exc:
        receipt["status"] = "failed"
        receipt["targets"] = [{"session_name": target_selector, "status": "failed", "error": str(exc)}]
        receipt = persist_ingress_receipt(receipt)
        return {"status": "failed", "ingress_id": ingress_id, "receipt": receipt}
    receipt["status"] = "delivered"
    receipt["targets"] = [{"session_name": target_selector, "status": "delivered", "response": response}]
    receipt = persist_ingress_receipt(receipt)
    return {"status": "delivered", "ingress_id": ingress_id, "receipt": receipt}


def process_room_launch_thread_message(
    *,
    base_receipt: dict[str, Any],
    launcher: dict[str, Any],
    launch: dict[str, Any],
    message: dict[str, Any],
    bot_user_id: str,
    ingress_id: str,
    message_debug: dict[str, Any] | None = None,
) -> dict[str, Any]:
    refreshed_launch = common.touch_room_launch(str(launch.get("launch_id", "")).strip())
    if isinstance(refreshed_launch, dict):
        launch = refreshed_launch
    body = strip_bot_mentions(str(message.get("content", "")), bot_user_id)
    if not body:
        receipt = persist_ingress_receipt(
            {
                **base_receipt,
                "binding_id": str(launcher.get("id", "")).strip(),
                "status": "ignored_empty",
                "reason": empty_body_reason(message, message_debug),
                "message_debug": dict(message_debug or {}),
                "targets": [],
            }
        )
        return {"status": "ignored_empty", "ingress_id": ingress_id, "receipt": receipt}
    mentioned_handles = common.extract_agent_handles(body)
    reply_to_id = referenced_message_id(message)
    if len(mentioned_handles) > 1:
        receipt = persist_ingress_receipt(
            {
                **base_receipt,
                "binding_id": str(launcher.get("id", "")).strip(),
                "status": "rejected_targeting",
                "reason": "multiple_handles_not_supported",
                "mentioned_handles": mentioned_handles,
                "targets": [],
            }
        )
        return {"status": "rejected_targeting", "ingress_id": ingress_id, "receipt": receipt}
    target_handle = ""
    routing_mode = ""
    if mentioned_handles:
        try:
            qualified_handle, resolve_error = common.resolve_agent_handle(mentioned_handles[0])
        except common.GCAPIError as exc:
            receipt = persist_ingress_receipt(
                {
                    **base_receipt,
                    "binding_id": str(launcher.get("id", "")).strip(),
                    "status": "failed_lookup",
                    "reason": str(exc),
                    "targets": [],
                }
            )
            return {"status": "failed_lookup", "ingress_id": ingress_id, "receipt": receipt}
        if resolve_error:
            receipt = persist_ingress_receipt(
                {
                    **base_receipt,
                    "binding_id": str(launcher.get("id", "")).strip(),
                    "status": "rejected_targeting",
                    "reason": resolve_error,
                    "mentioned_handles": mentioned_handles,
                    "targets": [],
                }
            )
            return {"status": "rejected_targeting", "ingress_id": ingress_id, "receipt": receipt}
        target_handle = qualified_handle
        routing_mode = "explicit_handle"
    if not target_handle and reply_to_id:
        target_handle = common.room_launch_message_target_handle(launch, reply_to_id)
        if target_handle:
            routing_mode = "reply_to"
    if not target_handle:
        target_handle = str(launch.get("last_addressed_qualified_handle", "")).strip()
        if target_handle:
            routing_mode = "last_addressed"
    if not target_handle:
        target_handle = str(launch.get("qualified_handle", "")).strip()
        if target_handle:
            routing_mode = "launch_default"
    if not target_handle:
        receipt = persist_ingress_receipt(
            {
                **base_receipt,
                "binding_id": str(launcher.get("id", "")).strip(),
                "status": "failed_lookup",
                "reason": "missing_thread_target",
                "targets": [],
            }
        )
        return {"status": "failed_lookup", "ingress_id": ingress_id, "receipt": receipt}
    try:
        launch, target_participant = common.ensure_room_launch_session_for_handle(launch, target_handle)
    except (ValueError, common.GCAPIError) as exc:
        receipt = persist_ingress_receipt(
            {
                **base_receipt,
                "binding_id": str(launcher.get("id", "")).strip(),
                "status": "failed_lookup",
                "reason": str(exc),
                "targets": [],
            }
        )
        return {"status": "failed_lookup", "ingress_id": ingress_id, "receipt": receipt}
    target_selector = participant_delivery_selector(target_participant)

    receipt = persist_ingress_receipt(
        {
            **base_receipt,
            "binding_id": str(launcher.get("id", "")).strip(),
            "status": "pending",
            "delivery": "targeted",
            "route_kind": "room_launch_thread",
            "launch_id": str(launch.get("launch_id", "")).strip(),
            "routing_mode": routing_mode,
            "mentioned_handles": mentioned_handles,
            "qualified_handle": target_handle,
            "targets": [{"session_name": target_selector, "status": "pending"}],
        }
    )
    envelope = build_room_launch_thread_envelope(
        launcher=launcher,
        launch=launch,
        target_participant=target_participant,
        message=message,
        body=body,
        mentioned_handles=mentioned_handles,
        ingress_id=ingress_id,
        routing_mode=routing_mode,
        reply_to_id=reply_to_id,
    )
    try:
        response = common.deliver_session_message(
            target_selector,
            envelope,
            idempotency_key=f"ingress:{ingress_id}:target:{target_selector}",
        )
    except common.GCAPIError as exc:
        receipt["status"] = "failed"
        receipt["targets"] = [{"session_name": target_selector, "status": "failed", "error": str(exc)}]
        receipt = persist_ingress_receipt(receipt)
        return {"status": "failed", "ingress_id": ingress_id, "receipt": receipt}
    updated_launch = common.set_room_launch_last_addressed(str(launch.get("launch_id", "")).strip(), target_handle)
    if isinstance(updated_launch, dict):
        launch = updated_launch
    receipt["status"] = "delivered"
    receipt["targets"] = [{"session_name": target_selector, "status": "delivered", "response": response}]
    receipt = persist_ingress_receipt(receipt)
    return {"status": "delivered", "ingress_id": ingress_id, "receipt": receipt}


def process_inbound_message(message: dict[str, Any], bot_user_id: str) -> dict[str, Any]:
    ingress_id = message_ingress_id(message)
    author = message.get("author") or {}
    if bool(author.get("bot")) or str(author.get("id", "")).strip() == bot_user_id:
        return {"status": "ignored", "reason": "bot_message", "ingress_id": ingress_id}

    guild_id = str(message.get("guild_id", "")).strip()
    channel_id = str(message.get("channel_id", "")).strip()
    if not channel_id:
        return {"status": "ignored", "reason": "missing_channel", "ingress_id": ingress_id}

    message, message_debug = recover_message_for_routing(message)
    author = message.get("author") or {}

    config = common.load_config()
    room_launchers_configured = bool(common.list_room_launchers(config)) if guild_id else False
    mentioned_bot = bot_was_mentioned(message, bot_user_id) if guild_id else False
    preloaded_launcher: dict[str, Any] | None = None
    preloaded_launch: dict[str, Any] | None = None
    preloaded_binding: dict[str, Any] | None = None
    preloaded_channel_info: dict[str, Any] = {}
    preloaded_body: str | None = None
    preloaded_aliases: list[str] | None = None
    if guild_id:
        if room_launchers_configured:
            # Discord message-started threads currently reuse the root message
            # snowflake as the thread channel id. Managed thread follow-ups
            # depend on that contract to reuse the canonical launch record id.
            launch = common.load_room_launch(common.room_launch_record_id(channel_id))
            if launch and str(launch.get("thread_id", "")).strip() == channel_id:
                launcher_id = str(launch.get("launcher_id", "")).strip()
                launcher_conversation_id = launcher_id.removeprefix("launch-room:")
                preloaded_launcher = common.resolve_room_launcher(config, launcher_conversation_id)
                if preloaded_launcher:
                    preloaded_launch = launch
            if preloaded_launch is None:
                preloaded_launcher = common.resolve_room_launcher(config, channel_id)
        if preloaded_launcher is None and not mentioned_bot:
            preloaded_binding = cached_ambient_room_binding(channel_id)
            if not preloaded_binding or not binding_allows_ambient_read(preloaded_binding):
                return {"status": "ignored", "reason": "not_mentioned", "ingress_id": ingress_id}
            preloaded_channel_info = binding_channel_info(preloaded_binding)
            preloaded_body = strip_bot_mentions(str(message.get("content", "")), bot_user_id)
            preloaded_aliases = extract_alias_mentions(preloaded_body)
            if not preloaded_aliases:
                return reject_ingress_before_processing(
                    message,
                    bot_user_id,
                    status="ignored_untargeted",
                    reason="ambient_target_required",
                    message_debug=message_debug,
                )
            participant_names = [str(item).strip() for item in preloaded_binding.get("session_names", []) if str(item).strip()]
            participant_lookup, participant_collisions = casefold_lookup(participant_names)
            has_valid_preloaded_alias = False
            for alias in preloaded_aliases:
                key = alias.casefold()
                if key in participant_collisions:
                    continue
                if participant_lookup.get(key):
                    has_valid_preloaded_alias = True
                    break
            if not has_valid_preloaded_alias:
                return reject_ingress_before_processing(
                    message,
                    bot_user_id,
                    status="ignored_untargeted",
                    reason="ambient_target_required",
                    message_debug=message_debug,
                )
            preloaded_channel_type_raw = preloaded_binding.get("channel_type", 0)
            try:
                preloaded_channel_type = int(preloaded_channel_type_raw or 0)
            except (TypeError, ValueError):
                preloaded_channel_type = 0
            if not preloaded_channel_info and (
                "channel_type" not in preloaded_binding or preloaded_channel_type in common.THREAD_CHANNEL_TYPES
            ):
                preloaded_binding = None

    preview = ingress_preview(message, bot_user_id)
    claimed, base_receipt = common.save_chat_ingress_if_absent(
        {
            "ingress_id": ingress_id,
            "discord_message_id": str(message.get("id", "")).strip(),
            "guild_id": guild_id,
            "conversation_id": channel_id,
            "binding_id": "",
            "from_user_id": str(author.get("id", "")).strip(),
            "from_display": display_name_from_message(message),
            "body_preview": preview,
            "message_debug": dict(message_debug or {}),
            "status": "processing",
            "targets": [],
        }
    )
    if not claimed:
        receipt_status = str(base_receipt.get("status", "")).strip()
        receipt_age = utc_age_seconds(str(base_receipt.get("updated_at", "")).strip())
        if str(base_receipt.get("status", "")).strip() == "claim_conflict_unreadable":
            receipt = persist_ingress_receipt(
                {
                    **base_receipt,
                    "ingress_id": ingress_id,
                    "discord_message_id": str(message.get("id", "")).strip(),
                    "guild_id": guild_id,
                    "conversation_id": channel_id,
                    "binding_id": "",
                    "from_user_id": str(author.get("id", "")).strip(),
                    "from_display": display_name_from_message(message),
                    "body_preview": preview,
                    "message_debug": dict(message_debug or {}),
                    "status": "failed_claim_conflict",
                    "reason": str(base_receipt.get("reason", "")).strip() or "ingress_claim_unreadable",
                    "targets": [],
                }
            )
            return {"status": "failed_claim_conflict", "ingress_id": ingress_id, "receipt": receipt}
        if receipt_status in {"processing", "failed", "partial_failed", "failed_lookup", "failed_claim_conflict", "rejected_shutting_down"} and (
            (receipt_status == "processing" and receipt_age >= STALE_PROCESSING_RECEIPT_SECONDS)
            or (
                receipt_status in {"failed", "partial_failed", "failed_lookup", "failed_claim_conflict"}
                and receipt_age >= FAILED_RECEIPT_RETRY_SECONDS
            )
            or receipt_status == "rejected_shutting_down"
        ):
            reclaim_lock = stale_reclaim_lock(ingress_id)
            if not reclaim_lock.acquire(blocking=False):
                return {"status": "duplicate", "ingress_id": ingress_id, "receipt": base_receipt}
            try:
                latest_receipt = common.load_chat_ingress(ingress_id) or base_receipt
                latest_status = str(latest_receipt.get("status", "")).strip()
                latest_age = utc_age_seconds(str(latest_receipt.get("updated_at", "")).strip())
                if not (
                    (latest_status == "processing" and latest_age >= STALE_PROCESSING_RECEIPT_SECONDS)
                    or (
                        latest_status in {"failed", "partial_failed", "failed_lookup", "failed_claim_conflict"}
                        and latest_age >= FAILED_RECEIPT_RETRY_SECONDS
                    )
                    or latest_status == "rejected_shutting_down"
                ):
                    return {"status": "duplicate", "ingress_id": ingress_id, "receipt": latest_receipt}
                retry_reason = "stale_processing_reclaimed"
                if latest_status in {"failed", "partial_failed", "failed_lookup"}:
                    retry_reason = "retry_after_failed_delivery"
                if latest_status == "failed_lookup":
                    retry_reason = "retry_after_failed_lookup"
                if latest_status == "failed_claim_conflict":
                    retry_reason = "retry_after_failed_claim_conflict"
                if latest_status == "rejected_shutting_down":
                    retry_reason = "retry_after_shutdown"
                base_receipt = persist_ingress_receipt(
                    {
                        **latest_receipt,
                        "ingress_id": ingress_id,
                        "discord_message_id": str(message.get("id", "")).strip(),
                        "guild_id": guild_id,
                        "conversation_id": channel_id,
                        "binding_id": "",
                        "from_user_id": str(author.get("id", "")).strip(),
                        "from_display": display_name_from_message(message),
                        "body_preview": preview,
                        "message_debug": dict(message_debug or {}),
                        "status": "processing",
                        "reason": retry_reason,
                        "targets": [],
                    }
                )
                claimed = True
            finally:
                reclaim_lock.release()
        else:
            return {"status": "duplicate", "ingress_id": ingress_id, "receipt": base_receipt}

    process_lock = ingress_process_lock(ingress_id)
    if not process_lock.acquire(blocking=False):
        return {"status": "duplicate", "ingress_id": ingress_id, "receipt": common.load_chat_ingress(ingress_id) or base_receipt}
    try:
        launcher = preloaded_launcher
        launch = preloaded_launch
        binding = preloaded_binding
        channel_info = dict(preloaded_channel_info)
        if launcher is None and binding is None:
            config = common.load_config()
            try:
                binding, channel_info = resolve_binding(config, message)
            except common.DiscordAPIError as exc:
                receipt = persist_ingress_receipt(
                    {
                        **base_receipt,
                        "status": "failed_lookup",
                        "reason": str(exc),
                        "targets": [],
                    }
                )
                return {"status": "failed_lookup", "ingress_id": ingress_id, "receipt": receipt}
        base_receipt.update(
            {
                "ingress_id": ingress_id,
                "discord_message_id": str(message.get("id", "")).strip(),
                "guild_id": guild_id,
                "conversation_id": channel_id,
                "binding_id": str((launcher or binding or {}).get("id", "")).strip(),
                "from_user_id": str(author.get("id", "")).strip(),
                "from_display": display_name_from_message(message),
                "body_preview": preview,
            }
        )
        if launcher and launch:
            return process_room_launch_thread_message(
                base_receipt=base_receipt,
                launcher=launcher,
                launch=launch,
                message=message,
                bot_user_id=bot_user_id,
                ingress_id=ingress_id,
                message_debug=message_debug,
            )
        if launcher:
            return process_room_launch_message(
                base_receipt=base_receipt,
                launcher=launcher,
                message=message,
                bot_user_id=bot_user_id,
                ingress_id=ingress_id,
                message_debug=message_debug,
            )
        if not binding:
            receipt = persist_ingress_receipt(
                {
                    **base_receipt,
                    "status": "rejected_unbound",
                    "reason": "binding_not_found",
                    "targets": [],
                }
            )
            return {"status": "rejected_unbound", "ingress_id": ingress_id, "receipt": receipt}

        body = preloaded_body if preloaded_body is not None else strip_bot_mentions(str(message.get("content", "")), bot_user_id)
        if not body:
            receipt = persist_ingress_receipt(
                {
                    **base_receipt,
                    "binding_id": str(binding.get("id", "")).strip(),
                    "status": "ignored_empty",
                    "reason": empty_body_reason(message, message_debug),
                    "message_debug": dict(message_debug or {}),
                    "targets": [],
                }
            )
            return {"status": "ignored_empty", "ingress_id": ingress_id, "receipt": receipt}

        mentioned_aliases = preloaded_aliases if preloaded_aliases is not None else extract_alias_mentions(body)
        try:
            session_index = common.session_index_by_name(state="all")
        except common.GCAPIError as exc:
            receipt = persist_ingress_receipt(
                {
                    **base_receipt,
                    "binding_id": str(binding.get("id", "")).strip(),
                    "status": "failed_lookup",
                    "reason": str(exc),
                    "targets": [],
                }
            )
            return {"status": "failed_lookup", "ingress_id": ingress_id, "receipt": receipt}

        targets, delivery, resolve_error = resolve_targets(
            binding,
            session_index,
            mentioned_aliases,
            require_targeted_aliases=bool(binding_allows_ambient_read(binding) and guild_id),
        )
        if resolve_error:
            if resolve_error == "target_required":
                receipt = persist_ingress_receipt(
                    {
                        **base_receipt,
                        "binding_id": str(binding.get("id", "")).strip(),
                        "status": "ignored_untargeted",
                        "reason": "ambient_target_required",
                        "mentioned_aliases": mentioned_aliases,
                        "targets": [],
                    }
                )
                return {"status": "ignored_untargeted", "ingress_id": ingress_id, "receipt": receipt}
            receipt = persist_ingress_receipt(
                {
                    **base_receipt,
                    "binding_id": str(binding.get("id", "")).strip(),
                    "status": "rejected_targeting",
                    "reason": resolve_error,
                    "mentioned_aliases": mentioned_aliases,
                    "targets": [],
                }
            )
            return {"status": "rejected_targeting", "ingress_id": ingress_id, "receipt": receipt}
        if not targets:
            receipt = persist_ingress_receipt(
                {
                    **base_receipt,
                    "binding_id": str(binding.get("id", "")).strip(),
                    "status": "skipped_no_live_targets",
                    "reason": "no_live_targets",
                    "mentioned_aliases": mentioned_aliases,
                    "targets": [],
                }
            )
            return {"status": "skipped_no_live_targets", "ingress_id": ingress_id, "receipt": receipt}

        receipt = persist_ingress_receipt(
            {
                **base_receipt,
                "binding_id": str(binding.get("id", "")).strip(),
                "status": "pending",
                "mentioned_aliases": mentioned_aliases,
                "delivery": delivery,
                "targets": [{"session_name": target, "status": "pending"} for target in targets],
            }
        )
        envelope = build_human_envelope(
            binding=binding,
            message=message,
            channel_info=channel_info,
            body=body,
            mentioned_aliases=mentioned_aliases,
            delivery=delivery,
            ingress_id=ingress_id,
        )
        updated_targets: list[dict[str, Any]] = []
        failures = 0
        for target in targets:
            idempotency_key = f"ingress:{ingress_id}:target:{target}"
            try:
                response = common.deliver_session_message(target, envelope, idempotency_key=idempotency_key)
                updated_targets.append(
                    {
                        "session_name": target,
                        "status": "delivered",
                        "idempotency_key": idempotency_key,
                        "response": response,
                    }
                )
            except common.GCAPIError as exc:
                failures += 1
                updated_targets.append(
                    {
                        "session_name": target,
                        "status": "failed",
                        "idempotency_key": idempotency_key,
                        "error": str(exc),
                    }
                )
        receipt["targets"] = updated_targets
        receipt["status"] = "delivered" if failures == 0 else ("partial_failed" if failures < len(targets) else "failed")
        receipt["delivery"] = delivery
        receipt["mentioned_aliases"] = mentioned_aliases
        receipt = persist_ingress_receipt(receipt)
        return {"status": receipt["status"], "ingress_id": ingress_id, "receipt": receipt}
    finally:
        process_lock.release()


class GatewayRuntimeState:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._last_persist_monotonic = 0.0
        self._status: dict[str, Any] = {
            "service": common.GATEWAY_SERVICE_NAME,
            "connected": False,
            "state": "starting",
            "routed_messages": 0,
            "duplicate_messages": 0,
            "ignored_messages": 0,
            "failed_messages": 0,
            "dropped_messages": 0,
            "message_queue_size": 0,
        }
        self._persist_locked(force=True)

    def _persist_locked(self, force: bool = False) -> None:
        now = time.monotonic()
        if force or (now - self._last_persist_monotonic) >= 1.0:
            common.save_gateway_status(self._status)
            self._last_persist_monotonic = now

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._status)

    def patch(self, **values: Any) -> None:
        with self._lock:
            self._status.update(values)
            force = bool({"state", "connected", "last_error", "last_disconnect_at", "last_ready_at", "last_resumed_at"} & set(values))
            self._persist_locked(force=force)

    def bump(self, field: str, delta: int = 1, **values: Any) -> None:
        with self._lock:
            self._status[field] = int(self._status.get(field, 0) or 0) + delta
            self._status.update(values)
            force = bool({"state", "connected", "last_error", "last_disconnect_at", "last_ready_at", "last_resumed_at"} & set(values))
            self._persist_locked(force=force)


class GatewayWebSocket:
    def __init__(self, url: str) -> None:
        self.url = url
        self._recv_buffer = bytearray()
        self.sock = self._connect(url)
        self._send_lock = threading.Lock()

    def _connect(self, url: str) -> socket.socket:
        parsed = urllib.parse.urlparse(url)
        host = parsed.hostname or ""
        if not host:
            raise RuntimeError(f"gateway URL missing hostname: {url}")
        port = parsed.port or (443 if parsed.scheme == "wss" else 80)
        path = parsed.path or "/"
        if parsed.query:
            path += "?" + parsed.query

        raw_sock = socket.create_connection((host, port), timeout=20)
        if parsed.scheme == "wss":
            context = ssl.create_default_context()
            sock = context.wrap_socket(raw_sock, server_hostname=host)
        else:
            sock = raw_sock
        sock.settimeout(20)

        key = base64.b64encode(os.urandom(16)).decode("ascii")
        request = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {host}:{port}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n"
            "\r\n"
        )
        sock.sendall(request.encode("utf-8"))
        response = b""
        while b"\r\n\r\n" not in response:
            chunk = sock.recv(4096)
            if not chunk:
                raise RuntimeError("websocket handshake closed early")
            response += chunk
        header_bytes, remainder = response.split(b"\r\n\r\n", 1)
        self._recv_buffer.extend(remainder)
        header_blob = header_bytes.decode("utf-8", errors="replace")
        validate_websocket_handshake(header_blob, key)
        return sock

    def close(self) -> None:
        try:
            self.send_frame(0x8, b"")
        except Exception:  # noqa: BLE001
            pass
        try:
            self.sock.close()
        except OSError:
            return

    def read_exact(self, length: int, timeout: float | None = None) -> bytes:
        if timeout is not None:
            self.sock.settimeout(timeout)
        data = bytearray()
        if self._recv_buffer:
            take = min(length, len(self._recv_buffer))
            data.extend(self._recv_buffer[:take])
            del self._recv_buffer[:take]
        while len(data) < length:
            chunk = self.sock.recv(length - len(data))
            if not chunk:
                raise WebSocketClosed("socket closed")
            data.extend(chunk)
        return bytes(data)

    def read_frame(self, timeout: float | None = None) -> tuple[bool, int, bytes]:
        try:
            head = self.read_exact(2, timeout=timeout)
        except TimeoutError as exc:
            raise GatewayFrameTimeout("timed out waiting for gateway frame header") from exc
        fin = bool(head[0] & 0x80)
        opcode = head[0] & 0x0F
        masked = (head[1] & 0x80) != 0
        length = head[1] & 0x7F
        try:
            if length == 126:
                length = struct.unpack("!H", self.read_exact(2, timeout=20.0))[0]
            elif length == 127:
                length = struct.unpack("!Q", self.read_exact(8, timeout=20.0))[0]
            if length > MAX_FRAME_BYTES:
                raise WebSocketClosed(f"gateway frame too large: {length}")
            mask = self.read_exact(4, timeout=20.0) if masked else b""
            payload = self.read_exact(length, timeout=20.0) if length else b""
        except TimeoutError as exc:
            raise WebSocketClosed("timed out while reading gateway frame payload") from exc
        if masked and mask:
            payload = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
        return fin, opcode, payload

    def send_frame(self, opcode: int, payload: bytes) -> None:
        length = len(payload)
        first = 0x80 | (opcode & 0x0F)
        if length < 126:
            header = bytes([first, 0x80 | length])
        elif length < (1 << 16):
            header = bytes([first, 0x80 | 126]) + struct.pack("!H", length)
        else:
            header = bytes([first, 0x80 | 127]) + struct.pack("!Q", length)
        mask = os.urandom(4)
        masked_payload = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
        with self._send_lock:
            self.sock.sendall(header + mask + masked_payload)

    def send_json(self, payload: dict[str, Any]) -> None:
        self.send_frame(0x1, json.dumps(payload, separators=(",", ":")).encode("utf-8"))

    def recv_event(self, timeout: float | None = None) -> dict[str, Any] | None:
        fragments: list[bytes] = []
        while True:
            fin, opcode, payload = self.read_frame(timeout=timeout if not fragments else 20.0)
            if opcode == 0x1:
                if fin:
                    return json.loads(payload.decode("utf-8"))
                fragments = [payload]
                continue
            if opcode == 0x0:
                if not fragments:
                    raise WebSocketClosed("unexpected continuation frame")
                fragments.append(payload)
                if sum(len(part) for part in fragments) > MAX_FRAME_BYTES:
                    raise WebSocketClosed("gateway message too large")
                if fin:
                    return json.loads(b"".join(fragments).decode("utf-8"))
                continue
            if opcode == 0x8:
                raise WebSocketClosed("gateway requested close")
            if opcode == 0x9:
                self.send_frame(0xA, payload)
                continue
            if opcode == 0xA:
                return None
            raise WebSocketClosed(f"unsupported websocket opcode: {opcode}")


class GatewayWorker:
    def __init__(self, runtime_state: GatewayRuntimeState) -> None:
        self.runtime_state = runtime_state
        self.stop_event = threading.Event()
        self._stopped = False
        self._stop_lock = threading.Lock()
        self.message_queue: queue.Queue[tuple[dict[str, Any], str] | None] = queue.Queue(maxsize=GATEWAY_MAX_PENDING_MESSAGES)
        self.worker_threads: list[threading.Thread] = []
        self._current_ws_lock = threading.Lock()
        self._current_ws: GatewayWebSocket | None = None
        for index in range(GATEWAY_WORKER_THREADS):
            thread = threading.Thread(target=self.message_worker_loop, name=f"discord-gateway-worker-{index + 1}")
            thread.start()
            self.worker_threads.append(thread)

    def set_current_ws(self, ws: GatewayWebSocket | None) -> None:
        with self._current_ws_lock:
            self._current_ws = ws

    def close_current_ws(self) -> None:
        with self._current_ws_lock:
            ws = self._current_ws
        if ws is not None:
            ws.close()

    def request_stop(self) -> None:
        self.stop_event.set()
        self.close_current_ws()

    def stop(self) -> None:
        with self._stop_lock:
            if self._stopped:
                return
            self._stopped = True
        self.runtime_state.patch(state="stopping", connected=False)
        self.request_stop()
        for _ in self.worker_threads:
            self.message_queue.put(WORKER_QUEUE_SENTINEL)
        self.message_queue.join()
        for thread in self.worker_threads:
            thread.join()
        self.runtime_state.patch(state="stopped", connected=False, message_queue_size=self.message_queue.qsize())

    def current_bot_user_id(
        self,
        config: dict[str, Any],
        ready_payload: dict[str, Any] | None = None,
        last_known_bot_user_id: str = "",
    ) -> str:
        ready_user = (ready_payload or {}).get("user") or {}
        bot_user_id = str(ready_user.get("id", "")).strip()
        if bot_user_id:
            return bot_user_id
        if last_known_bot_user_id:
            return str(last_known_bot_user_id).strip()
        return str((config.get("app") or {}).get("application_id", "")).strip()

    def gateway_connect_url(self, url: str) -> str:
        if not url:
            raise RuntimeError("Discord gateway URL is missing")
        parsed = urllib.parse.urlparse(url)
        query = dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))
        query.pop("compress", None)
        query["v"] = "10"
        query["encoding"] = "json"
        return urllib.parse.urlunparse(parsed._replace(query=urllib.parse.urlencode(query)))

    def gateway_url(self) -> str:
        payload = common.discord_api_request("GET", "/gateway/bot")
        url = str((payload or {}).get("url", "")).strip()
        if not url:
            raise RuntimeError("Discord gateway URL is missing from /gateway/bot")
        return self.gateway_connect_url(url)

    def identify(self, ws: GatewayWebSocket, token: str) -> None:
        ws.send_json(
            {
                "op": 2,
                "d": {
                    "token": token,
                    "intents": GATEWAY_INTENTS,
                    "properties": {
                        "os": "linux",
                        "browser": "gas-city-discord",
                        "device": "gas-city-discord",
                    },
                },
            }
        )

    def resume(self, ws: GatewayWebSocket, token: str, session_id: str, seq: int) -> None:
        ws.send_json(
            {
                "op": 6,
                "d": {
                    "token": token,
                    "session_id": session_id,
                    "seq": seq,
                },
            }
        )

    def message_worker_loop(self) -> None:
        while True:
            try:
                item = self.message_queue.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                if item is WORKER_QUEUE_SENTINEL:
                    return
                message, bot_user_id = item
                self.handle_gateway_message(message, bot_user_id)
            finally:
                self.message_queue.task_done()
                self.runtime_state.patch(message_queue_size=self.message_queue.qsize())

    def handle_gateway_message(self, message: dict[str, Any], bot_user_id: str) -> None:
        try:
            outcome = process_inbound_message(message, bot_user_id)
            status = str(outcome.get("status", "")).strip()
            preview = summarize_body(str((outcome.get("receipt") or {}).get("body_preview", "")))
            if status == "duplicate":
                self.runtime_state.bump("duplicate_messages", last_message_status=status, last_message_preview=preview, last_event_at=common.utcnow())
                return
            if status.startswith("ignored"):
                self.runtime_state.bump("ignored_messages", last_message_status=status, last_message_preview=preview, last_event_at=common.utcnow())
                return
            if status in {"delivered", "partial_failed"}:
                self.runtime_state.bump("routed_messages", last_message_status=status, last_message_preview=preview, last_event_at=common.utcnow())
                return
            self.runtime_state.bump("failed_messages", last_message_status=status or "failed", last_message_preview=preview, last_event_at=common.utcnow())
        except Exception as exc:  # noqa: BLE001
            preview = ingress_preview(message, bot_user_id)
            self.runtime_state.bump(
                "failed_messages",
                last_message_status="exception",
                last_message_preview=preview,
                last_error=str(exc),
                last_exception=traceback.format_exc(limit=20),
                last_event_at=common.utcnow(),
            )

    def dispatch_gateway_message(self, message: dict[str, Any], bot_user_id: str) -> None:
        if self.stop_event.is_set():
            save_rejected_ingress_receipt(
                message,
                bot_user_id,
                status="rejected_shutting_down",
                reason="service_shutting_down",
            )
            self.runtime_state.bump(
                "dropped_messages",
                last_message_status="shutting_down",
                last_message_preview=ingress_preview(message, bot_user_id),
                last_event_at=common.utcnow(),
                message_queue_size=self.message_queue.qsize(),
            )
            return
        try:
            self.message_queue.put_nowait((message, bot_user_id))
            self.runtime_state.patch(message_queue_size=self.message_queue.qsize())
        except queue.Full:
            ingress_id = message_ingress_id(message)
            save_rejected_ingress_receipt(
                message,
                bot_user_id,
                status="rejected_overloaded",
                reason="message_queue_full",
            )
            print(
                f"[{common.current_service_name() or 'discord-gateway'}] dropping ingress {ingress_id}: message queue full",
                flush=True,
            )
            self.runtime_state.bump(
                "dropped_messages",
                last_message_status="queue_full",
                last_message_preview=ingress_preview(message, bot_user_id),
                last_event_at=common.utcnow(),
                message_queue_size=self.message_queue.qsize(),
            )

    def prune_runtime_data(self) -> None:
        common.prune_requests()
        common.prune_receipts()
        common.prune_pending_modals()
        common.prune_chat_ingress()
        common.prune_chat_publishes()
        common.prune_room_launches()
        prune_channel_info_cache()
        prune_channel_info_fetch_locks()
        prune_stale_reclaim_locks()
        prune_ingress_process_locks()
        self.runtime_state.patch(last_prune_at=common.utcnow())

    def run_forever(self) -> None:
        backoff_seconds = RECONNECT_BASE_DELAY_SECONDS
        next_prune_at = 0.0
        seq: int | None = None
        resume_session_id = ""
        resume_gateway_url = ""
        last_known_bot_user_id = ""
        while not self.stop_event.is_set():
            try:
                now = time.monotonic()
                if now >= next_prune_at:
                    self.prune_runtime_data()
                    next_prune_at = now + PRUNE_INTERVAL_SECONDS
                config = common.load_config()
                bot_token = common.load_bot_token()
                application_id = str((config.get("app") or {}).get("application_id", "")).strip()
                if not bot_token or not application_id:
                    self.runtime_state.patch(
                        connected=False,
                        state="waiting_for_config",
                        last_error="discord app or bot token is not configured",
                    )
                    if self.stop_event.wait(RECONNECT_BASE_DELAY_SECONDS):
                        break
                    continue

                can_resume = bool(resume_session_id and seq is not None)
                connection_url = self.gateway_connect_url(resume_gateway_url) if can_resume and resume_gateway_url else self.gateway_url()
                ws = GatewayWebSocket(connection_url)
                self.set_current_ws(ws)
                ready_payload: dict[str, Any] | None = None
                heartbeat_interval = 0.0
                next_heartbeat_at = 0.0
                awaiting_heartbeat_ack = False
                self.runtime_state.patch(connected=False, state="connecting", last_error="", resume_attempt=can_resume)

                try:
                    hello = ws.recv_event(timeout=20)
                    if not isinstance(hello, dict) or int(hello.get("op", 0) or 0) != 10:
                        raise RuntimeError(f"expected HELLO from Discord gateway, got {hello!r}")
                    heartbeat_interval = max(float((hello.get("d") or {}).get("heartbeat_interval", 45000)) / 1000.0, 1.0)
                    next_heartbeat_at = time.monotonic() + heartbeat_interval * random.uniform(0.2, 0.8)
                    if can_resume and seq is not None:
                        self.resume(ws, bot_token, resume_session_id, seq)
                    else:
                        self.identify(ws, bot_token)

                    while not self.stop_event.is_set():
                        now = time.monotonic()
                        timeout = max(0.1, next_heartbeat_at - now)
                        try:
                            event = ws.recv_event(timeout=timeout)
                        except GatewayFrameTimeout:
                            event = None
                        now = time.monotonic()
                        if now >= next_heartbeat_at:
                            if awaiting_heartbeat_ack:
                                raise RuntimeError("discord gateway missed heartbeat ack")
                            ws.send_json({"op": 1, "d": seq})
                            awaiting_heartbeat_ack = True
                            next_heartbeat_at = now + heartbeat_interval
                            self.runtime_state.patch(last_heartbeat_at=common.utcnow())
                        if now >= next_prune_at:
                            self.prune_runtime_data()
                            next_prune_at = now + PRUNE_INTERVAL_SECONDS
                        if not event:
                            continue

                        op = int(event.get("op", 0) or 0)
                        if event.get("s") is not None:
                            seq = int(event.get("s") or 0)
                            self.runtime_state.patch(last_sequence=seq)
                        if op == 0:
                            event_type = str(event.get("t", "")).strip()
                            data = event.get("d") or {}
                            if event_type == "READY" and isinstance(data, dict):
                                ready_payload = data
                                bot_user_id = self.current_bot_user_id(config, ready_payload, last_known_bot_user_id)
                                last_known_bot_user_id = bot_user_id
                                resume_session_id = str(data.get("session_id", "")).strip()
                                resume_gateway_url = str(data.get("resume_gateway_url", "")).strip()
                                backoff_seconds = RECONNECT_BASE_DELAY_SECONDS
                                self.runtime_state.patch(
                                    connected=True,
                                    state="ready",
                                    bot_user_id=bot_user_id,
                                    last_ready_at=common.utcnow(),
                                    last_ready_epoch=int(time.time()),
                                    last_error="",
                                )
                                continue
                            if event_type == "RESUMED":
                                awaiting_heartbeat_ack = False
                                backoff_seconds = RECONNECT_BASE_DELAY_SECONDS
                                self.runtime_state.patch(
                                    connected=True,
                                    state="ready",
                                    last_resumed_at=common.utcnow(),
                                    last_resumed_epoch=int(time.time()),
                                    last_error="",
                                )
                                continue
                            if event_type == "MESSAGE_CREATE" and isinstance(data, dict):
                                bot_user_id = self.current_bot_user_id(config, ready_payload, last_known_bot_user_id)
                                self.dispatch_gateway_message(data, bot_user_id)
                                continue
                        elif op == 11:
                            awaiting_heartbeat_ack = False
                            self.runtime_state.patch(last_heartbeat_ack_at=common.utcnow())
                            continue
                        elif op == 1:
                            ws.send_json({"op": 1, "d": seq})
                            awaiting_heartbeat_ack = True
                            next_heartbeat_at = time.monotonic() + heartbeat_interval
                        elif op in {7, 9}:
                            if op == 9:
                                resume_session_id = ""
                                resume_gateway_url = ""
                                seq = None
                            raise RuntimeError(f"gateway requested reconnect (op={op})")
                finally:
                    self.set_current_ws(None)
                    ws.close()
            except Exception as exc:  # noqa: BLE001
                if self.stop_event.is_set():
                    break
                sleep_seconds = min(RECONNECT_MAX_DELAY_SECONDS, backoff_seconds * random.uniform(0.8, 1.2))
                self.runtime_state.patch(
                    connected=False,
                    state="reconnecting",
                    last_error=str(exc),
                    last_exception=traceback.format_exc(limit=20),
                    last_disconnect_at=common.utcnow(),
                    next_retry_delay_seconds=round(sleep_seconds, 2),
                )
                if self.stop_event.wait(sleep_seconds):
                    break
                backoff_seconds = min(RECONNECT_MAX_DELAY_SECONDS, max(RECONNECT_BASE_DELAY_SECONDS, backoff_seconds * 2))


class GatewayHandler(BaseHTTPRequestHandler):
    server_version = "DiscordGateway/0.1"

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"[{common.current_service_name() or 'discord-gateway'}] {fmt % args}")

    def do_GET(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/healthz":
            state = get_runtime_state().snapshot()
            gc_api_reachable = True
            if str(state.get("state", "")).strip() in {"ready", "reconnecting"}:
                gc_api_reachable = probe_gc_api_health(get_runtime_state())
            code = gateway_health_status_code(state, gc_api_reachable=gc_api_reachable)
            self.send_response(code)
            self.end_headers()
            return
        if parsed.path in {"", "/"}:
            text_response(self, HTTPStatus.OK, "discord gateway ready\n", "text/plain; charset=utf-8")
            return
        if parsed.path == "/v0/discord/gateway/status":
            json_response(self, HTTPStatus.OK, get_runtime_state().snapshot())
            return
        json_response(self, HTTPStatus.NOT_FOUND, {"error": "not_found"})


RUNTIME_STATE: GatewayRuntimeState | None = None


def get_runtime_state() -> GatewayRuntimeState:
    global RUNTIME_STATE
    if RUNTIME_STATE is None:
        RUNTIME_STATE = GatewayRuntimeState()
    return RUNTIME_STATE


def gateway_health_status_code(state: dict[str, Any], gc_api_reachable: bool = True) -> HTTPStatus:
    status = str(state.get("state", "")).strip()
    if status in {"connecting", "waiting_for_config", "starting"}:
        return HTTPStatus.NO_CONTENT
    if status == "ready":
        return HTTPStatus.NO_CONTENT if gc_api_reachable else HTTPStatus.SERVICE_UNAVAILABLE
    if status == "reconnecting":
        last_ready_epoch = int(state.get("last_ready_epoch", 0) or 0)
        last_resumed_epoch = int(state.get("last_resumed_epoch", 0) or 0)
        fresh_epoch = max(last_ready_epoch, last_resumed_epoch)
        if fresh_epoch and (time.time() - fresh_epoch) <= HEALTH_RECONNECT_GRACE_SECONDS:
            return HTTPStatus.NO_CONTENT if gc_api_reachable else HTTPStatus.SERVICE_UNAVAILABLE
    return HTTPStatus.SERVICE_UNAVAILABLE


def main() -> int:
    common.ensure_layout()
    common.prune_chat_ingress()
    common.prune_chat_publishes()
    common.prune_room_launches()
    socket_path = os.environ.get("GC_SERVICE_SOCKET", "")
    try:
        common.prepare_service_socket(socket_path)
    except RuntimeError as exc:
        raise SystemExit(str(exc)) from exc

    runtime_state = get_runtime_state()
    worker = GatewayWorker(runtime_state)
    thread = threading.Thread(target=worker.run_forever, name="discord-gateway")
    thread.start()

    with ThreadingUnixHTTPServer(socket_path, GatewayHandler) as server:
        def handle_shutdown(signum: int, _frame: Any) -> None:
            runtime_state.patch(last_shutdown_signal=signum, last_shutdown_at=common.utcnow())
            worker.request_stop()
            threading.Thread(target=server.shutdown, daemon=True).start()

        previous_sigint = signal.signal(signal.SIGINT, handle_shutdown)
        previous_sigterm = signal.signal(signal.SIGTERM, handle_shutdown)
        print(f"[{common.current_service_name() or 'discord-gateway'}] listening on {socket_path}")
        try:
            server.serve_forever()
        finally:
            signal.signal(signal.SIGINT, previous_sigint)
            signal.signal(signal.SIGTERM, previous_sigterm)
            worker.stop()
            thread.join()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
