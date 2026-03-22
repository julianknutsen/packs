from __future__ import annotations

import calendar
import base64
import contextlib
import copy
import fcntl
import hashlib
import json
import os
import pathlib
import re
import socket
import subprocess
import tempfile
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

INTERACTIONS_SERVICE_NAME = "discord-interactions"
ADMIN_SERVICE_NAME = "discord-admin"
GATEWAY_SERVICE_NAME = "discord-gateway"
SCHEMA_VERSION = 1
DISCORD_API_BASE = os.environ.get("GC_DISCORD_API_BASE", "https://discord.com/api/v10")
REQUEST_RETENTION_SECONDS = 24 * 60 * 60
PENDING_MODAL_RETENTION_SECONDS = 15 * 60
CHAT_INGRESS_RETENTION_SECONDS = 7 * 24 * 60 * 60
CHAT_PUBLISH_RETENTION_SECONDS = 7 * 24 * 60 * 60
ROOM_LAUNCH_RETENTION_SECONDS = 90 * 24 * 60 * 60
COMMAND_NAME_DEFAULT = "gc"
FIX_FORMULA_DEFAULT = "mol-discord-fix-issue"
ED25519_SPKI_PREFIX = bytes.fromhex("302a300506032b6570032100")
DEFAULT_GC_API_PORT = 9443
DEFAULT_SUPERVISOR_API_BASE = "http://127.0.0.1:8372"
LOCAL_API_BINDS = {"", "0.0.0.0", "::", "[::]", "*"}
DISCORD_RATE_LIMIT_RETRIES = 2
GC_API_REQUEST_TIMEOUT_SECONDS = 20.0
SERVICE_SOCKET_PROBE_TIMEOUT_SECONDS = 0.2
NON_ROUTABLE_SESSION_STATES = {"", "closed", "stopped", "orphaned", "quarantined"}
PEER_DELIVERY_TIMEOUT_SECONDS = 10.0
PEER_IN_PROGRESS_STALE_SECONDS = 30.0
PEER_ROOT_WINDOW_SECONDS = 60.0
CANONICAL_PEER_SESSION_NAME = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
URL_PATTERN = re.compile(r"https?://\S+")
DISCORD_RESERVED_MENTIONS = {"everyone", "here"}
THREAD_CHANNEL_TYPES = {10, 11, 12}
ROOM_LAUNCH_RESPONSE_MODES = {"mention_only", "respond_all"}
ROOM_LAUNCH_ALIAS_SLUG_MAX = 24
AGENT_HANDLE_SEGMENT = re.compile(r"^[a-z][a-z0-9_-]{0,31}$")


class DiscordAPIError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class GCAPIError(RuntimeError):
    pass


def utcnow() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def city_root() -> str:
    return os.environ.get("GC_CITY_ROOT") or os.environ.get("GC_CITY_PATH", "")


def city_name() -> str:
    root = city_root()
    if not root:
        return "workspace"
    return pathlib.Path(root).name


def current_service_name() -> str:
    return os.environ.get("GC_SERVICE_NAME", "")


def state_root() -> str:
    value = os.environ.get("GC_SERVICE_STATE_ROOT")
    if value:
        return value
    root = city_root()
    if not root:
        return ".gc/services/discord"
    return os.path.join(root, ".gc", "services", "discord")


def secrets_dir() -> str:
    value = os.environ.get("GC_SERVICE_SECRETS_DIR")
    if value:
        return value
    return os.path.join(state_root(), "secrets")


def data_dir() -> str:
    return os.path.join(state_root(), "data")


def requests_dir() -> str:
    return os.path.join(data_dir(), "requests")


def receipts_dir() -> str:
    return os.path.join(data_dir(), "receipts")


def workflows_dir() -> str:
    return os.path.join(data_dir(), "workflows")


def pending_modals_dir() -> str:
    return os.path.join(data_dir(), "pending-modals")


def chat_publishes_dir() -> str:
    return os.path.join(data_dir(), "chat-publishes")


def chat_ingress_dir() -> str:
    return os.path.join(data_dir(), "chat-ingress")


def room_launches_dir() -> str:
    return os.path.join(data_dir(), "chat-launches")


def channel_metadata_cache_dir() -> str:
    return os.path.join(data_dir(), "channel-metadata")


def peer_root_budget_dir() -> str:
    return os.path.join(data_dir(), "peer-root-budgets")


def locks_dir() -> str:
    return os.path.join(data_dir(), "locks")


def config_path() -> str:
    return os.path.join(data_dir(), "config.json")


def secret_path(name: str) -> str:
    return os.path.join(secrets_dir(), name)


def published_services_dir() -> str:
    value = os.environ.get("GC_PUBLISHED_SERVICES_DIR")
    if value:
        return value
    root = city_root()
    if not root:
        return ".gc/services/.published"
    return os.path.join(root, ".gc", "services", ".published")


def gateway_status_path() -> str:
    return os.path.join(data_dir(), "gateway-status.json")


def ensure_layout() -> None:
    for path in (
        data_dir(),
        requests_dir(),
        receipts_dir(),
        workflows_dir(),
        pending_modals_dir(),
        chat_publishes_dir(),
        chat_ingress_dir(),
        room_launches_dir(),
        channel_metadata_cache_dir(),
        peer_root_budget_dir(),
        locks_dir(),
        secrets_dir(),
    ):
        os.makedirs(path, exist_ok=True)
    os.chmod(secrets_dir(), 0o700)


def atomic_write_json(path: str, payload: dict[str, Any], mode: int = 0o640) -> None:
    parent = os.path.dirname(path)
    os.makedirs(parent, exist_ok=True)
    data = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8") + b"\n"
    with tempfile.NamedTemporaryFile(dir=parent, delete=False) as handle:
        handle.write(data)
        handle.flush()
        os.fchmod(handle.fileno(), mode)
        temp_path = handle.name
    os.replace(temp_path, path)


def atomic_write_text(path: str, body: str, mode: int = 0o600) -> None:
    parent = os.path.dirname(path)
    os.makedirs(parent, exist_ok=True)
    data = body.encode("utf-8")
    with tempfile.NamedTemporaryFile(dir=parent, delete=False) as handle:
        handle.write(data)
        handle.flush()
        os.fchmod(handle.fileno(), mode)
        temp_path = handle.name
    os.replace(temp_path, path)


def read_json(path: str, default: Any = None, *, allow_invalid: bool = False) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError:
        return default
    except json.JSONDecodeError:
        if allow_invalid:
            return default
        raise


def read_text(path: str, default: str = "") -> str:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return handle.read()
    except FileNotFoundError:
        return default


def default_config() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "app": {
            "command_name": COMMAND_NAME_DEFAULT,
        },
        "policy": {
            "guild_allowlist": [],
            "channel_allowlist": [],
            "role_allowlist": [],
        },
        "channels": {},
        "rigs": {},
        "chat": {
            "bindings": {},
            "launchers": {},
        },
    }


def _normalize_allowlist(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    return [str(value).strip() for value in values if str(value).strip()]


def dedupe_session_names(values: list[str]) -> list[str]:
    seen: set[str] = set()
    session_names: list[str] = []
    for value in values:
        normalized = str(value).strip()
        if not normalized:
            continue
        key = normalized.casefold()
        if key in seen:
            continue
        seen.add(key)
        session_names.append(normalized)
    return session_names


def room_launch_surface_id(conversation_id: str) -> str:
    return f"launch-room:{str(conversation_id).strip()}"


def normalize_room_launch_response_mode(value: Any) -> str:
    normalized = str(value).strip().lower() or "mention_only"
    if normalized not in ROOM_LAUNCH_RESPONSE_MODES:
        return "mention_only"
    return normalized


def normalize_binding_channel_metadata(value: dict[str, Any] | None = None) -> dict[str, Any]:
    raw = value if isinstance(value, dict) else {}
    normalized: dict[str, Any] = {}
    has_channel_type = "channel_type" in raw or "type" in raw
    channel_type_raw = raw.get("channel_type", raw.get("type", 0))
    try:
        channel_type = int(channel_type_raw or 0)
    except (TypeError, ValueError):
        channel_type = 0
    if has_channel_type:
        normalized["channel_type"] = channel_type
    if channel_type in THREAD_CHANNEL_TYPES:
        parent_id = str(raw.get("thread_parent_id", raw.get("parent_id", ""))).strip()
        if parent_id:
            normalized["thread_parent_id"] = parent_id
    return normalized


def normalize_config(raw: dict[str, Any] | None) -> dict[str, Any]:
    config = default_config()
    if not raw:
        return config
    if isinstance(raw.get("app"), dict):
        app = copy.deepcopy(raw["app"])
        command_name = str(app.get("command_name", COMMAND_NAME_DEFAULT)).strip() or COMMAND_NAME_DEFAULT
        config["app"] = {
            "application_id": str(app.get("application_id", "")).strip(),
            "public_key": str(app.get("public_key", "")).strip(),
            "command_name": command_name,
        }
    if isinstance(raw.get("policy"), dict):
        policy = raw["policy"]
        config["policy"] = {
            "guild_allowlist": _normalize_allowlist(policy.get("guild_allowlist")),
            "channel_allowlist": _normalize_allowlist(policy.get("channel_allowlist")),
            "role_allowlist": _normalize_allowlist(policy.get("role_allowlist")),
        }
    chat = raw.get("chat")
    if isinstance(chat, dict):
        normalized_bindings: dict[str, Any] = {}
        normalized_launchers: dict[str, Any] = {}
        bindings = chat.get("bindings")
        if isinstance(bindings, dict):
            for key, value in bindings.items():
                if not isinstance(value, dict):
                    continue
                kind = str(value.get("kind", "")).strip().lower()
                conversation_id = str(value.get("conversation_id", "")).strip()
                if kind not in {"dm", "room"} or not conversation_id:
                    continue
                session_names = value.get("session_names")
                if not isinstance(session_names, list):
                    session_names = []
                normalized_session_names = dedupe_session_names(session_names)
                if kind == "dm" and len(normalized_session_names) != 1:
                    continue
                binding_id = chat_binding_id(kind, conversation_id)
                normalized_bindings[binding_id] = {
                    "id": binding_id,
                    "kind": kind,
                    "conversation_id": conversation_id,
                    "guild_id": str(value.get("guild_id", "")).strip(),
                    "session_names": normalized_session_names,
                }
                channel_metadata = normalize_binding_channel_metadata(value)
                if channel_metadata:
                    normalized_bindings[binding_id].update(channel_metadata)
                if kind == "room":
                    normalized_bindings[binding_id]["policy"] = normalize_room_peer_policy(value.get("policy"))
        launchers = chat.get("launchers")
        if isinstance(launchers, dict):
            for key, value in launchers.items():
                if not isinstance(value, dict):
                    continue
                kind = str(value.get("kind", "")).strip().lower()
                conversation_id = str(value.get("conversation_id", "")).strip()
                guild_id = str(value.get("guild_id", "")).strip()
                if kind != "room" or not conversation_id or not guild_id:
                    continue
                launcher_id = room_launch_surface_id(conversation_id)
                response_mode = normalize_room_launch_response_mode(value.get("response_mode"))
                default_handle = _normalize_agent_handle(value.get("default_qualified_handle", ""))
                if default_handle and "/" not in default_handle:
                    default_handle = ""
                if response_mode == "respond_all" and not default_handle:
                    response_mode = "mention_only"
                normalized_launchers[launcher_id] = {
                    "id": launcher_id,
                    "kind": "room",
                    "conversation_id": conversation_id,
                    "guild_id": guild_id,
                    "response_mode": response_mode,
                    "default_qualified_handle": default_handle,
                }
        config["chat"] = {
            "bindings": normalized_bindings,
            "launchers": normalized_launchers,
        }
    channels = raw.get("channels")
    if isinstance(channels, dict):
        normalized_channels: dict[str, Any] = {}
        for key, value in channels.items():
            if not isinstance(value, dict):
                continue
            guild_id = str(value.get("guild_id", "")).strip()
            channel_id = str(value.get("channel_id", "")).strip()
            target = str(value.get("target", "")).strip()
            if not guild_id or not channel_id or not target:
                continue
            commands = value.get("commands")
            if not isinstance(commands, dict):
                commands = {}
            fix_cfg = commands.get("fix")
            if not isinstance(fix_cfg, dict):
                fix_cfg = {}
            fix_formula = str(fix_cfg.get("formula", FIX_FORMULA_DEFAULT)).strip() or FIX_FORMULA_DEFAULT
            normalized_channels[str(key)] = {
                "guild_id": guild_id,
                "channel_id": channel_id,
                "target": target,
                "commands": {
                    "fix": {
                        "formula": fix_formula,
                    }
                },
            }
        config["channels"] = normalized_channels
    rigs = raw.get("rigs")
    if isinstance(rigs, dict):
        normalized_rigs: dict[str, Any] = {}
        for key, value in rigs.items():
            if not isinstance(value, dict):
                continue
            guild_id = str(value.get("guild_id", "")).strip()
            rig_name = str(value.get("rig_name", "")).strip()
            target = str(value.get("target", "")).strip()
            if not guild_id or not rig_name or not target:
                continue
            commands = value.get("commands")
            if not isinstance(commands, dict):
                commands = {}
            fix_cfg = commands.get("fix")
            if not isinstance(fix_cfg, dict):
                fix_cfg = {}
            fix_formula = str(fix_cfg.get("formula", FIX_FORMULA_DEFAULT)).strip() or FIX_FORMULA_DEFAULT
            normalized_rigs[str(key)] = {
                "guild_id": guild_id,
                "rig_name": rig_name,
                "target": target,
                "commands": {
                    "fix": {
                        "formula": fix_formula,
                    }
                },
            }
        config["rigs"] = normalized_rigs
    config["schema_version"] = SCHEMA_VERSION
    return config


def load_config() -> dict[str, Any]:
    ensure_layout()
    return normalize_config(read_json(config_path(), {}))


def save_config(config: dict[str, Any]) -> dict[str, Any]:
    ensure_layout()
    normalized = normalize_config(config)
    atomic_write_json(config_path(), normalized)
    return normalized


def canonical_peer_session_name(name: str) -> str:
    normalized = str(name).strip()
    if normalized and CANONICAL_PEER_SESSION_NAME.fullmatch(normalized):
        return normalized
    return ""


def _coerce_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on", "enabled"}:
        return True
    if text in {"0", "false", "no", "off", "disabled"}:
        return False
    return default


def default_room_peer_policy() -> dict[str, Any]:
    return {
        "ambient_read_enabled": False,
        "peer_fanout_enabled": False,
        "allow_untargeted_peer_fanout": False,
        "max_peer_triggered_publishes_per_root": 1,
        "max_total_peer_deliveries_per_root": 8,
        "max_peer_triggered_publishes_per_session_per_minute": 5,
    }


def normalize_room_peer_policy(policy: dict[str, Any] | None = None) -> dict[str, Any]:
    raw = policy if isinstance(policy, dict) else {}
    defaults = default_room_peer_policy()
    return {
        "ambient_read_enabled": _coerce_bool(raw.get("ambient_read_enabled"), defaults["ambient_read_enabled"]),
        "peer_fanout_enabled": _coerce_bool(raw.get("peer_fanout_enabled"), defaults["peer_fanout_enabled"]),
        "allow_untargeted_peer_fanout": _coerce_bool(
            raw.get("allow_untargeted_peer_fanout"), defaults["allow_untargeted_peer_fanout"]
        ),
        "max_peer_triggered_publishes_per_root": max(
            0, int(raw.get("max_peer_triggered_publishes_per_root", defaults["max_peer_triggered_publishes_per_root"]) or 0)
        ),
        "max_total_peer_deliveries_per_root": max(
            0, int(raw.get("max_total_peer_deliveries_per_root", defaults["max_total_peer_deliveries_per_root"]) or 0)
        ),
        "max_peer_triggered_publishes_per_session_per_minute": max(
            0,
            int(
                raw.get(
                    "max_peer_triggered_publishes_per_session_per_minute",
                    defaults["max_peer_triggered_publishes_per_session_per_minute"],
                )
                or 0
            ),
        ),
    }


def binding_peer_policy(binding: dict[str, Any]) -> dict[str, Any]:
    if str(binding.get("kind", "")).strip() != "room":
        return default_room_peer_policy()
    return normalize_room_peer_policy(binding.get("policy"))


def redact_config(config: dict[str, Any]) -> dict[str, Any]:
    redacted = normalize_config(config)
    redacted["app"]["bot_token_present"] = bool(load_bot_token())
    return redacted


def validate_application_id(value: str) -> str:
    normalized = str(value).strip()
    if not normalized:
        return ""
    if not normalized.isdigit():
        raise ValueError("application_id must be a Discord snowflake")
    return normalized


def validate_public_key(value: str) -> str:
    normalized = str(value).strip().lower()
    if not normalized:
        return ""
    try:
        raw = bytes.fromhex(normalized)
    except ValueError as exc:
        raise ValueError("public_key must be valid 32-byte hex") from exc
    if len(raw) != 32:
        raise ValueError("public_key must be valid 32-byte hex")
    return normalized


def import_app_config(config: dict[str, Any], app_fields: dict[str, Any]) -> dict[str, Any]:
    cfg = normalize_config(config)
    app = cfg.setdefault("app", {})
    application_id = validate_application_id(app_fields.get("application_id", app_fields.get("app_id", "")))
    public_key = validate_public_key(app_fields.get("public_key", ""))
    if application_id:
        app["application_id"] = application_id
    if public_key:
        app["public_key"] = public_key
    command_name = str(app_fields.get("command_name", app.get("command_name", COMMAND_NAME_DEFAULT))).strip()
    app["command_name"] = command_name or COMMAND_NAME_DEFAULT

    policy = cfg.setdefault("policy", {})
    for key in ("guild_allowlist", "channel_allowlist", "role_allowlist"):
        if key in app_fields:
            policy[key] = _normalize_allowlist(app_fields.get(key))
    return save_config(cfg)


def save_bot_token(token: str) -> None:
    ensure_layout()
    atomic_write_text(secret_path("bot-token.txt"), token.strip() + "\n", mode=0o600)


def load_bot_token() -> str:
    return read_text(secret_path("bot-token.txt")).strip()


def normalize_channel_key(guild_id: str, channel_id: str) -> str:
    return f"{str(guild_id).strip()}/{str(channel_id).strip()}"


def chat_binding_id(kind: str, conversation_id: str) -> str:
    return f"{str(kind).strip().lower()}:{str(conversation_id).strip()}"


def set_chat_binding(
    config: dict[str, Any],
    kind: str,
    conversation_id: str,
    session_names: list[str],
    guild_id: str = "",
    *,
    policy: dict[str, Any] | None = None,
    channel_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_kind = str(kind).strip().lower()
    if normalized_kind not in {"dm", "room"}:
        raise ValueError("kind must be dm or room")
    normalized_conversation = str(conversation_id).strip()
    if not normalized_conversation:
        raise ValueError("conversation_id is required")
    normalized_session_names = dedupe_session_names(session_names)
    if not normalized_session_names:
        raise ValueError("at least one session name is required")
    if normalized_kind == "dm" and len(normalized_session_names) != 1:
        raise ValueError("DM bindings require exactly one session name")

    cfg = normalize_config(config)
    binding_id = chat_binding_id(normalized_kind, normalized_conversation)
    if normalized_kind == "room" and resolve_room_launcher(cfg, normalized_conversation):
        raise ValueError("room launch is already enabled for that conversation")
    existing = resolve_chat_binding(cfg, binding_id) or {}
    raw_room_policy = copy.deepcopy(existing.get("policy")) if isinstance(existing.get("policy"), dict) else {}
    raw_channel_metadata = normalize_binding_channel_metadata(existing)
    if isinstance(channel_metadata, dict):
        raw_channel_metadata.update(normalize_binding_channel_metadata(channel_metadata))
    if isinstance(policy, dict):
        raw_room_policy.update(copy.deepcopy(policy))
    room_policy = normalize_room_peer_policy(raw_room_policy)
    if normalized_kind == "room" and room_policy.get("peer_fanout_enabled"):
        invalid = [name for name in normalized_session_names if canonical_peer_session_name(name) != name]
        if invalid:
            raise ValueError("peer-fanout-enabled room bindings require lowercase canonical session names")
    cfg.setdefault("chat", {})
    cfg["chat"].setdefault("bindings", {})
    binding: dict[str, Any] = {
        "id": binding_id,
        "kind": normalized_kind,
        "conversation_id": normalized_conversation,
        "guild_id": str(guild_id).strip(),
        "session_names": normalized_session_names,
    }
    if normalized_kind == "room":
        if raw_channel_metadata:
            binding.update(raw_channel_metadata)
        binding["policy"] = room_policy
    cfg["chat"]["bindings"][binding_id] = binding
    return save_config(cfg)


def set_room_launcher(
    config: dict[str, Any],
    guild_id: str,
    conversation_id: str,
    *,
    response_mode: str = "mention_only",
    default_qualified_handle: str = "",
) -> dict[str, Any]:
    normalized_conversation = str(conversation_id).strip()
    normalized_guild_id = str(guild_id).strip()
    if not normalized_guild_id:
        raise ValueError("guild_id is required")
    if not normalized_conversation:
        raise ValueError("conversation_id is required")
    normalized_response_mode = normalize_room_launch_response_mode(response_mode)
    normalized_default_handle = _normalize_agent_handle(default_qualified_handle)
    if str(default_qualified_handle).strip() and (not normalized_default_handle or "/" not in normalized_default_handle):
        raise ValueError("default_qualified_handle must use qualified rig/alias syntax")
    if normalized_response_mode == "respond_all" and not normalized_default_handle:
        raise ValueError("respond_all room launchers require --default-handle")

    cfg = normalize_config(config)
    binding_id = chat_binding_id("room", normalized_conversation)
    existing_binding = resolve_chat_binding(cfg, binding_id)
    if existing_binding:
        raise ValueError("room launch cannot be enabled on a directly bound room")
    cfg.setdefault("chat", {})
    cfg["chat"].setdefault("launchers", {})
    launcher_id = room_launch_surface_id(normalized_conversation)
    cfg["chat"]["launchers"][launcher_id] = {
        "id": launcher_id,
        "kind": "room",
        "guild_id": normalized_guild_id,
        "conversation_id": normalized_conversation,
        "response_mode": normalized_response_mode,
        "default_qualified_handle": normalized_default_handle,
    }
    return save_config(cfg)


def resolve_room_launcher(config: dict[str, Any], conversation_id: str) -> dict[str, Any] | None:
    launchers = normalize_config(config).get("chat", {}).get("launchers", {})
    return launchers.get(room_launch_surface_id(conversation_id))


def list_room_launchers(config: dict[str, Any]) -> list[dict[str, Any]]:
    launchers = normalize_config(config).get("chat", {}).get("launchers", {})
    return sorted(launchers.values(), key=lambda item: (str(item.get("kind", "")), str(item.get("conversation_id", ""))))


def describe_room_channel_metadata(conversation_id: str, *, bot_token: str = "") -> dict[str, Any]:
    token = str(bot_token).strip() or load_bot_token()
    if not token:
        return {}
    info = discord_api_request("GET", f"/channels/{urllib.parse.quote(str(conversation_id).strip())}", bot_token=token)
    if not isinstance(info, dict):
        return {}
    return normalize_binding_channel_metadata(info)


def channel_metadata_cache_path(conversation_id: str) -> str:
    return os.path.join(channel_metadata_cache_dir(), f"{safe_storage_id(str(conversation_id).strip(), 'channel-metadata')}.json")


def load_channel_metadata_cache(conversation_id: str) -> dict[str, Any]:
    ensure_layout()
    data = read_json(channel_metadata_cache_path(conversation_id), allow_invalid=True)
    if not isinstance(data, dict):
        return {}
    return normalize_binding_channel_metadata(data)


def save_channel_metadata_cache(conversation_id: str, metadata: dict[str, Any] | None) -> dict[str, Any]:
    ensure_layout()
    normalized = normalize_binding_channel_metadata(metadata)
    if not normalized:
        return {}
    atomic_write_json(channel_metadata_cache_path(conversation_id), normalized)
    return normalized


def resolve_chat_binding(config: dict[str, Any], binding_id: str) -> dict[str, Any] | None:
    bindings = normalize_config(config).get("chat", {}).get("bindings", {})
    return bindings.get(str(binding_id).strip())


def list_chat_bindings(config: dict[str, Any]) -> list[dict[str, Any]]:
    bindings = normalize_config(config).get("chat", {}).get("bindings", {})
    return sorted(bindings.values(), key=lambda item: (str(item.get("kind", "")), str(item.get("conversation_id", ""))))


def resolve_publish_route(config: dict[str, Any], route_id: str) -> dict[str, Any] | None:
    binding = resolve_chat_binding(config, route_id)
    if binding:
        return binding
    route = str(route_id).strip()
    if route.startswith("launch-room:"):
        launcher = resolve_room_launcher(config, route.removeprefix("launch-room:"))
        if launcher:
            payload = dict(launcher)
            payload["publish_route_kind"] = "room_launch"
            payload["kind"] = "room"
            payload.setdefault("session_names", [])
            return payload
    return None


def set_channel_mapping(
    config: dict[str, Any],
    guild_id: str,
    channel_id: str,
    target: str,
    fix_formula: str | None,
) -> dict[str, Any]:
    cfg = normalize_config(config)
    formula = str(fix_formula or FIX_FORMULA_DEFAULT).strip() or FIX_FORMULA_DEFAULT
    normalized_target = validate_fix_dispatch_target(target, formula)
    key = normalize_channel_key(guild_id, channel_id)
    cfg["channels"][key] = {
        "guild_id": str(guild_id).strip(),
        "channel_id": str(channel_id).strip(),
        "target": normalized_target,
        "commands": {
            "fix": {
                "formula": formula,
            }
        },
    }
    return save_config(cfg)


def resolve_channel_mapping(config: dict[str, Any], guild_id: str, channel_id: str) -> dict[str, Any] | None:
    channels = normalize_config(config).get("channels", {})
    return channels.get(normalize_channel_key(guild_id, channel_id))


def normalize_rig_key(guild_id: str, rig_name: str) -> str:
    return f"{str(guild_id).strip()}/{str(rig_name).strip()}"


def set_rig_mapping(
    config: dict[str, Any],
    guild_id: str,
    rig_name: str,
    target: str,
    fix_formula: str | None,
) -> dict[str, Any]:
    cfg = normalize_config(config)
    formula = str(fix_formula or FIX_FORMULA_DEFAULT).strip() or FIX_FORMULA_DEFAULT
    normalized_target = validate_fix_dispatch_target(target, formula)
    key = normalize_rig_key(guild_id, rig_name)
    cfg["rigs"][key] = {
        "guild_id": str(guild_id).strip(),
        "rig_name": str(rig_name).strip(),
        "target": normalized_target,
        "commands": {
            "fix": {
                "formula": formula,
            }
        },
    }
    return save_config(cfg)


def resolve_rig_mapping(config: dict[str, Any], guild_id: str, rig_name: str) -> dict[str, Any] | None:
    rigs = normalize_config(config).get("rigs", {})
    return rigs.get(normalize_rig_key(guild_id, rig_name))


def load_channel_context(
    config: dict[str, Any],
    guild_id: str,
    channel_id: str,
    parent_channel_id_hint: str = "",
) -> dict[str, Any]:
    mapping = resolve_channel_mapping(config, guild_id, channel_id)
    if mapping:
        return {
            "guild_id": str(guild_id),
            "channel_id": str(channel_id),
            "parent_channel_id": str(channel_id),
            "thread_id": "",
            "mapping": mapping,
        }
    parent_channel_id = str(parent_channel_id_hint).strip()
    channel_info = {}
    if parent_channel_id:
        mapping = resolve_channel_mapping(config, guild_id, parent_channel_id)
        if mapping:
            return {
                "guild_id": str(guild_id),
                "channel_id": parent_channel_id,
                "parent_channel_id": parent_channel_id,
                "thread_id": str(channel_id),
                "mapping": mapping,
                "channel_info": channel_info,
            }
    bot_token = load_bot_token()
    if not parent_channel_id and bot_token:
        try:
            info = discord_api_request("GET", f"/channels/{urllib.parse.quote(str(channel_id))}", bot_token=bot_token)
            if isinstance(info, dict):
                channel_info = info
                parent_channel_id = str(info.get("parent_id", "")).strip()
        except DiscordAPIError as exc:
            if exc.status_code == 404:
                parent_channel_id = ""
            else:
                return {
                    "guild_id": str(guild_id),
                    "channel_id": str(channel_id),
                    "parent_channel_id": "",
                    "thread_id": "",
                    "mapping": {},
                    "channel_info": channel_info,
                    "lookup_error": str(exc),
                }
    if parent_channel_id:
        mapping = resolve_channel_mapping(config, guild_id, parent_channel_id)
        if mapping:
            return {
                "guild_id": str(guild_id),
                "channel_id": parent_channel_id,
                "parent_channel_id": parent_channel_id,
                "thread_id": str(channel_id),
                "mapping": mapping,
                "channel_info": channel_info,
            }
        return {
            "guild_id": str(guild_id),
            "channel_id": parent_channel_id,
            "parent_channel_id": parent_channel_id,
            "thread_id": str(channel_id),
            "mapping": None,
            "channel_info": channel_info,
        }
    return {
        "guild_id": str(guild_id),
        "channel_id": str(channel_id),
        "parent_channel_id": str(channel_id),
        "thread_id": "",
        "mapping": None,
        "channel_info": channel_info,
    }


def safe_storage_id(value: str, prefix: str) -> str:
    value = value.strip()
    if value and all(ch.isalnum() or ch in ("-", "_", ":") for ch in value):
        return value
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:24]
    return f"{prefix}-{digest}"


def build_request_id(interaction_id: str, command: str) -> str:
    safe_command = "".join(ch for ch in command.lower() if ch.isalnum() or ch in ("-", "_")) or "command"
    return f"dc-{safe_storage_id(interaction_id, 'interaction')}-{safe_command}"


def build_workflow_key(guild_id: str, conversation_id: str, command: str) -> str:
    safe_command = "".join(ch for ch in command.lower() if ch.isalnum() or ch in ("-", "_")) or "command"
    return f"dc:guild:{guild_id}:conversation:{conversation_id}:{safe_command}"


def request_path(request_id: str) -> str:
    return os.path.join(requests_dir(), f"{safe_storage_id(request_id, 'request')}.json")


def receipt_path(interaction_id: str) -> str:
    return os.path.join(receipts_dir(), f"{safe_storage_id(interaction_id, 'receipt')}.json")


def workflow_path(workflow_key: str) -> str:
    return os.path.join(workflows_dir(), f"{safe_storage_id(workflow_key, 'workflow')}.json")


def pending_modal_path(nonce: str) -> str:
    return os.path.join(pending_modals_dir(), f"{safe_storage_id(nonce, 'modal')}.json")


def chat_publish_path(publish_id: str) -> str:
    return os.path.join(chat_publishes_dir(), f"{safe_storage_id(publish_id, 'chat-publish')}.json")


def chat_ingress_path(ingress_id: str) -> str:
    return os.path.join(chat_ingress_dir(), f"{safe_storage_id(ingress_id, 'chat-ingress')}.json")


def room_launch_path(launch_id: str) -> str:
    return os.path.join(room_launches_dir(), f"{safe_storage_id(launch_id, 'room-launch')}.json")


def room_launch_record_id(root_message_id: str) -> str:
    return f"room-launch:{str(root_message_id).strip()}"


def room_launch_lock_path(launch_id: str) -> str:
    return _safe_lock_name("room-launch", str(launch_id).strip())


def load_request(request_id: str) -> dict[str, Any] | None:
    data = read_json(request_path(request_id), allow_invalid=True)
    if isinstance(data, dict):
        return data
    return None


def save_request(payload: dict[str, Any]) -> dict[str, Any]:
    ensure_layout()
    body = copy.deepcopy(payload)
    body["updated_at"] = utcnow()
    atomic_write_json(request_path(body["request_id"]), body)
    return body


def load_interaction_receipt(interaction_id: str) -> dict[str, Any] | None:
    data = read_json(receipt_path(interaction_id), allow_invalid=True)
    if isinstance(data, dict):
        return data
    return None


def save_interaction_receipt(interaction_id: str, payload: dict[str, Any]) -> bool:
    ensure_layout()
    path = receipt_path(interaction_id)
    body = copy.deepcopy(payload)
    body["interaction_id"] = interaction_id
    body.setdefault("created_at", utcnow())
    data = json.dumps(body, indent=2, sort_keys=True).encode("utf-8") + b"\n"
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o640)
    except FileExistsError:
        return False
    with os.fdopen(fd, "wb") as handle:
        handle.write(data)
    return True


def replace_interaction_receipt(interaction_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    ensure_layout()
    existing = load_interaction_receipt(interaction_id) or {}
    body = copy.deepcopy(payload)
    body["interaction_id"] = interaction_id
    body.setdefault("created_at", str(existing.get("created_at", "")).strip() or utcnow())
    atomic_write_json(receipt_path(interaction_id), body)
    return body


def load_workflow_link(workflow_key: str) -> dict[str, Any] | None:
    data = read_json(workflow_path(workflow_key), allow_invalid=True)
    if isinstance(data, dict):
        return data
    return None


def save_workflow_link(workflow_key: str, request_id: str) -> dict[str, Any]:
    ensure_layout()
    payload = {
        "workflow_key": workflow_key,
        "request_id": request_id,
        "created_at": utcnow(),
    }
    atomic_write_json(workflow_path(workflow_key), payload)
    return payload


def remove_workflow_link(workflow_key: str) -> None:
    try:
        os.remove(workflow_path(workflow_key))
    except FileNotFoundError:
        return


def remove_workflow_link_if_request(workflow_key: str, request_id: str) -> bool:
    current = load_workflow_link(workflow_key)
    if not current:
        return False
    if str(current.get("request_id", "")) != request_id:
        return False
    remove_workflow_link(workflow_key)
    return True


def remove_workflow_links_for_request(request_id: str) -> list[str]:
    released: list[str] = []
    for path in pathlib.Path(workflows_dir()).glob("*.json"):
        payload = read_json(str(path), {}, allow_invalid=True)
        if not isinstance(payload, dict):
            continue
        workflow_key = str(payload.get("workflow_key", "")).strip()
        if not workflow_key or str(payload.get("request_id", "")).strip() != str(request_id).strip():
            continue
        remove_workflow_link(workflow_key)
        released.append(workflow_key)
    return released


def save_pending_modal(payload: dict[str, Any]) -> dict[str, Any]:
    ensure_layout()
    body = copy.deepcopy(payload)
    body.setdefault("created_at", utcnow())
    atomic_write_json(pending_modal_path(str(body["nonce"])), body)
    return body


def load_pending_modal(nonce: str) -> dict[str, Any] | None:
    data = read_json(pending_modal_path(nonce), allow_invalid=True)
    if isinstance(data, dict):
        return data
    return None


def remove_pending_modal(nonce: str) -> None:
    try:
        os.remove(pending_modal_path(nonce))
    except FileNotFoundError:
        return


def _prune_dir(path: str, max_age_seconds: int) -> None:
    now = time.time()
    for entry in pathlib.Path(path).glob("*.json"):
        try:
            age = now - entry.stat().st_mtime
        except FileNotFoundError:
            continue
        if age > max_age_seconds:
            try:
                entry.unlink()
            except FileNotFoundError:
                continue


def prune_receipts() -> None:
    ensure_layout()
    _prune_dir(receipts_dir(), REQUEST_RETENTION_SECONDS)


def active_workflow_request_ids() -> set[str]:
    ensure_layout()
    request_ids: set[str] = set()
    for path in pathlib.Path(workflows_dir()).glob("*.json"):
        payload = read_json(str(path), {}, allow_invalid=True)
        if not isinstance(payload, dict):
            continue
        request_id = str(payload.get("request_id", "")).strip()
        if request_id:
            request_ids.add(request_id)
    return request_ids


def prune_requests() -> None:
    ensure_layout()
    now = time.time()
    active_request_ids = active_workflow_request_ids()
    for entry in pathlib.Path(requests_dir()).glob("*.json"):
        payload = read_json(str(entry), allow_invalid=True)
        if isinstance(payload, dict) and str(payload.get("request_id", "")).strip() in active_request_ids:
            continue
        try:
            age = now - entry.stat().st_mtime
        except FileNotFoundError:
            continue
        if age > REQUEST_RETENTION_SECONDS:
            try:
                entry.unlink()
            except FileNotFoundError:
                continue


def prune_pending_modals() -> None:
    ensure_layout()
    _prune_dir(pending_modals_dir(), PENDING_MODAL_RETENTION_SECONDS)


def list_recent_requests(limit: int = 20) -> list[dict[str, Any]]:
    ensure_layout()
    entries: list[dict[str, Any]] = []
    paths = sorted(
        pathlib.Path(requests_dir()).glob("*.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )[:limit]
    for path in paths:
        data = read_json(str(path), allow_invalid=True)
        if isinstance(data, dict):
            entries.append(data)
    entries.sort(key=lambda item: item.get("created_at", ""), reverse=True)
    return entries[:limit]


def save_gateway_status(payload: dict[str, Any]) -> dict[str, Any]:
    ensure_layout()
    body = copy.deepcopy(payload)
    body["updated_at"] = utcnow()
    atomic_write_json(gateway_status_path(), body)
    return body


def load_gateway_status() -> dict[str, Any]:
    ensure_layout()
    payload = read_json(gateway_status_path(), {}, allow_invalid=True)
    if isinstance(payload, dict):
        return payload
    return {}


def save_chat_publish(payload: dict[str, Any]) -> dict[str, Any]:
    ensure_layout()
    body = copy.deepcopy(payload)
    publish_id = str(body.get("publish_id", "")).strip()
    if not publish_id:
        publish_id = f"discord-publish-{int(time.time() * 1000)}-{hashlib.sha256(os.urandom(16)).hexdigest()[:8]}"
        body["publish_id"] = publish_id
    body.setdefault("created_at", utcnow())
    atomic_write_json(chat_publish_path(publish_id), body)
    _update_peer_root_budget_index(body)
    return body


def load_room_launch(launch_id: str) -> dict[str, Any] | None:
    data = read_json(room_launch_path(launch_id), allow_invalid=True)
    if isinstance(data, dict):
        return data
    return None


def save_room_launch(payload: dict[str, Any]) -> dict[str, Any]:
    ensure_layout()
    body = copy.deepcopy(payload)
    launch_id = str(body.get("launch_id", "")).strip()
    if not launch_id:
        raise ValueError("launch_id is required")
    body.setdefault("created_at", utcnow())
    body["updated_at"] = utcnow()
    atomic_write_json(room_launch_path(launch_id), body)
    return body


def touch_room_launch(launch_id: str, *, activity_at: str = "") -> dict[str, Any] | None:
    normalized_launch_id = str(launch_id).strip()
    if not normalized_launch_id:
        return None
    with advisory_lock(room_launch_lock_path(normalized_launch_id)):
        current = load_room_launch(normalized_launch_id)
        if not isinstance(current, dict):
            return None
        body = copy.deepcopy(current)
        body["last_activity_at"] = str(activity_at).strip() or utcnow()
        return save_room_launch(body)


def list_room_launches(limit: int = 50) -> list[dict[str, Any]]:
    ensure_layout()
    entries: list[dict[str, Any]] = []
    paths = sorted(
        pathlib.Path(room_launches_dir()).glob("*.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )[:limit]
    for path in paths:
        data = read_json(str(path), allow_invalid=True)
        if isinstance(data, dict):
            entries.append(data)
    entries.sort(key=lambda item: item.get("created_at", ""), reverse=True)
    return entries[:limit]


def load_chat_publish(publish_id: str) -> dict[str, Any] | None:
    data = read_json(chat_publish_path(publish_id), allow_invalid=True)
    if isinstance(data, dict):
        return data
    return None


def iter_chat_publishes() -> list[dict[str, Any]]:
    ensure_layout()
    items: list[dict[str, Any]] = []
    for path in pathlib.Path(chat_publishes_dir()).glob("*.json"):
        data = read_json(str(path), allow_invalid=True)
        if isinstance(data, dict):
            items.append(data)
    return items


def list_recent_chat_publishes(limit: int = 20) -> list[dict[str, Any]]:
    ensure_layout()
    entries: list[dict[str, Any]] = []
    paths = sorted(
        pathlib.Path(chat_publishes_dir()).glob("*.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )[:limit]
    for path in paths:
        data = read_json(str(path), allow_invalid=True)
        if isinstance(data, dict):
            entries.append(data)
    entries.sort(key=lambda item: item.get("created_at", ""), reverse=True)
    return entries[:limit]


def iter_chat_publishes_since(since_epoch: float) -> list[dict[str, Any]]:
    ensure_layout()
    items: list[dict[str, Any]] = []
    for path in pathlib.Path(chat_publishes_dir()).glob("*.json"):
        try:
            stat_result = path.stat()
        except OSError:
            continue
        if stat_result.st_mtime < since_epoch:
            continue
        data = read_json(str(path), allow_invalid=True)
        if isinstance(data, dict):
            items.append(data)
    return items


def _safe_lock_name(prefix: str, key: str) -> str:
    digest = hashlib.sha256(str(key).encode("utf-8")).hexdigest()[:16]
    return os.path.join(locks_dir(), f"{prefix}-{digest}.lock")


def peer_root_budget_path(binding_id: str, root_ingress_receipt_id: str) -> str:
    safe_key = safe_storage_id(f"{binding_id}:{root_ingress_receipt_id}", "peer-root-budget")
    return os.path.join(peer_root_budget_dir(), f"{safe_key}.json")


@contextlib.contextmanager
def advisory_lock(path: str):
    ensure_layout()
    handle = open(path, "a+", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        yield handle
    finally:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


def parse_utc_timestamp(value: str) -> float | None:
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = time.strptime(text, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return None
    return float(calendar.timegm(parsed))


def _normalize_peer_root_budget_index(index: dict[str, Any] | None, *, binding_id: str, root_ingress_receipt_id: str) -> dict[str, Any]:
    payload = copy.deepcopy(index) if isinstance(index, dict) else {}
    entries = payload.get("entries")
    normalized_entries: dict[str, dict[str, Any]] = {}
    if isinstance(entries, dict):
        for publish_id, item in entries.items():
            if not isinstance(item, dict):
                continue
            normalized_entries[str(publish_id).strip()] = {
                "publish_id": str(item.get("publish_id", publish_id)).strip(),
                "created_at": str(item.get("created_at", "")).strip(),
                "source_session_name": str(item.get("source_session_name", "")).strip(),
                "source_event_kind": str(item.get("source_event_kind", "")).strip(),
                "frozen_target_count": max(0, int(item.get("frozen_target_count", 0) or 0)),
            }
    payload["binding_id"] = binding_id
    payload["root_ingress_receipt_id"] = root_ingress_receipt_id
    payload["entries"] = normalized_entries
    return payload


def load_peer_root_budget_index(binding_id: str, root_ingress_receipt_id: str) -> dict[str, Any]:
    if not binding_id or not root_ingress_receipt_id:
        return _normalize_peer_root_budget_index({}, binding_id=binding_id, root_ingress_receipt_id=root_ingress_receipt_id)
    payload = read_json(peer_root_budget_path(binding_id, root_ingress_receipt_id), {}, allow_invalid=True)
    if not isinstance(payload, dict):
        payload = {}
    return _normalize_peer_root_budget_index(payload, binding_id=binding_id, root_ingress_receipt_id=root_ingress_receipt_id)


def _prune_peer_root_budget_index(index: dict[str, Any]) -> dict[str, Any]:
    payload = copy.deepcopy(index)
    cutoff = time.time() - CHAT_PUBLISH_RETENTION_SECONDS
    entries = payload.get("entries")
    if not isinstance(entries, dict):
        payload["entries"] = {}
        return payload
    for publish_id, item in list(entries.items()):
        created_at = ""
        if isinstance(item, dict):
            created_at = str(item.get("created_at", "")).strip()
        created_epoch = parse_utc_timestamp(created_at)
        if created_epoch is not None and created_epoch < cutoff:
            entries.pop(publish_id, None)
    payload["entries"] = entries
    return payload


def save_peer_root_budget_index(index: dict[str, Any]) -> dict[str, Any]:
    payload = _prune_peer_root_budget_index(index)
    binding_id = str(payload.get("binding_id", "")).strip()
    root_ingress_receipt_id = str(payload.get("root_ingress_receipt_id", "")).strip()
    if not binding_id or not root_ingress_receipt_id:
        return payload
    atomic_write_json(peer_root_budget_path(binding_id, root_ingress_receipt_id), payload)
    return payload


def _update_peer_root_budget_index(record: dict[str, Any]) -> None:
    binding_id = str(record.get("binding_id", "")).strip()
    root_ingress_receipt_id = str(record.get("root_ingress_receipt_id", "")).strip()
    publish_id = str(record.get("publish_id", "")).strip()
    if not binding_id or not root_ingress_receipt_id or not publish_id:
        return
    lock_path = _safe_lock_name("peer-root-index", f"{binding_id}:{root_ingress_receipt_id}")
    with advisory_lock(lock_path):
        index = load_peer_root_budget_index(binding_id, root_ingress_receipt_id)
        entries = index.setdefault("entries", {})
        peer_delivery = record.get("peer_delivery")
        frozen_targets: list[str] = []
        if isinstance(peer_delivery, dict):
            frozen = peer_delivery.get("frozen_targets")
            if isinstance(frozen, list):
                frozen_targets = [str(item).strip() for item in frozen if str(item).strip()]
        entries[publish_id] = {
            "publish_id": publish_id,
            "created_at": str(record.get("created_at", "")).strip(),
            "source_session_name": str(record.get("source_session_name", "")).strip(),
            "source_event_kind": str(record.get("source_event_kind", "")).strip(),
            "frozen_target_count": len(frozen_targets),
        }
        save_peer_root_budget_index(index)


def load_chat_ingress(ingress_id: str) -> dict[str, Any] | None:
    data = read_json(chat_ingress_path(ingress_id), allow_invalid=True)
    if isinstance(data, dict):
        return data
    return None


def save_chat_ingress(payload: dict[str, Any]) -> dict[str, Any]:
    ensure_layout()
    body = copy.deepcopy(payload)
    ingress_id = str(body.get("ingress_id", "")).strip()
    if not ingress_id:
        ingress_id = f"discord-ingress-{int(time.time() * 1000)}"
        body["ingress_id"] = ingress_id
    body["updated_at"] = utcnow()
    body.setdefault("created_at", body["updated_at"])
    atomic_write_json(chat_ingress_path(ingress_id), body)
    return body


def save_chat_ingress_if_absent(payload: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
    ensure_layout()
    body = copy.deepcopy(payload)
    ingress_id = str(body.get("ingress_id", "")).strip()
    if not ingress_id:
        ingress_id = f"discord-ingress-{int(time.time() * 1000)}"
        body["ingress_id"] = ingress_id
    body["updated_at"] = utcnow()
    body.setdefault("created_at", body["updated_at"])
    path = chat_ingress_path(ingress_id)
    data = json.dumps(body, indent=2, sort_keys=True).encode("utf-8") + b"\n"
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o640)
    except FileExistsError:
        for _ in range(20):
            try:
                existing = load_chat_ingress(ingress_id)
            except json.JSONDecodeError:
                existing = None
            if existing:
                return False, existing
            time.sleep(0.01)
        updated_at = utcnow()
        try:
            stat_result = os.stat(path)
        except OSError:
            pass
        else:
            updated_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(stat_result.st_mtime))
        return False, {
            "ingress_id": ingress_id,
            "status": "claim_conflict_unreadable",
            "reason": "ingress_claim_unreadable",
            "created_at": updated_at,
            "updated_at": updated_at,
        }
    with os.fdopen(fd, "wb") as handle:
        handle.write(data)
    return True, body


def validate_fix_dispatch_target(target: str, fix_formula: str) -> str:
    normalized_target = str(target).strip()
    if not normalized_target:
        raise ValueError("target must be a rig/pool sling target")
    rig, separator, pool = normalized_target.partition("/")
    if not separator or not rig.strip() or not pool.strip():
        raise ValueError("target must be a rig/pool sling target")
    formula = str(fix_formula or FIX_FORMULA_DEFAULT).strip() or FIX_FORMULA_DEFAULT
    if formula == FIX_FORMULA_DEFAULT and pool.strip() != "polecat":
        raise ValueError(f"{FIX_FORMULA_DEFAULT} requires a rig/polecat sling target")
    return f"{rig.strip()}/{pool.strip()}"


def list_recent_chat_ingress(limit: int = 20) -> list[dict[str, Any]]:
    ensure_layout()
    entries: list[dict[str, Any]] = []
    paths = sorted(
        pathlib.Path(chat_ingress_dir()).glob("*.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )[:limit]
    for path in paths:
        data = read_json(str(path), allow_invalid=True)
        if isinstance(data, dict):
            entries.append(data)
    entries.sort(key=lambda item: item.get("created_at", ""), reverse=True)
    return entries[:limit]


def prune_chat_ingress() -> None:
    ensure_layout()
    _prune_dir(chat_ingress_dir(), CHAT_INGRESS_RETENTION_SECONDS)


def prune_chat_publishes() -> None:
    ensure_layout()
    _prune_dir(chat_publishes_dir(), CHAT_PUBLISH_RETENTION_SECONDS)


def prune_room_launches() -> None:
    ensure_layout()
    _prune_dir(room_launches_dir(), ROOM_LAUNCH_RETENTION_SECONDS)


def redact_chat_ingress_record(payload: dict[str, Any]) -> dict[str, Any]:
    body = copy.deepcopy(payload)
    if body.get("from_display"):
        body["from_display"] = "[redacted]"
    if body.get("from_user_id"):
        body["from_user_id"] = "[redacted]"
    if body.get("body_preview"):
        body["body_preview"] = "[redacted]"
    return body


def redact_chat_publish_record(payload: dict[str, Any]) -> dict[str, Any]:
    body = copy.deepcopy(payload)
    if body.get("body"):
        body["body"] = "[redacted]"
    return body


def redact_room_launch_record(payload: dict[str, Any]) -> dict[str, Any]:
    body = copy.deepcopy(payload)
    if body.get("from_display"):
        body["from_display"] = "[redacted]"
    if body.get("from_user_id"):
        body["from_user_id"] = "[redacted]"
    if body.get("body_preview"):
        body["body_preview"] = "[redacted]"
    return body


def redact_request_record(payload: dict[str, Any]) -> dict[str, Any]:
    body = copy.deepcopy(payload)
    for field in (
        "summary",
        "prompt",
        "context_markdown",
        "invoking_user_display_name",
        "invoking_user_id",
        "error_message",
        "traceback",
        "dispatch_stdout",
        "dispatch_stderr",
    ):
        if body.get(field):
            body[field] = "[redacted]"
    return body


def redact_gateway_status(payload: dict[str, Any]) -> dict[str, Any]:
    body = copy.deepcopy(payload)
    if body.get("last_message_preview"):
        body["last_message_preview"] = "[redacted]"
    if body.get("last_error"):
        body["last_error"] = "[redacted]"
    if body.get("last_exception"):
        body["last_exception"] = "[redacted]"
    return body


def published_service_snapshot(service_name: str) -> dict[str, Any]:
    path = os.path.join(published_services_dir(), f"{service_name}.json")
    snapshot = read_json(path, {}, allow_invalid=True)
    if isinstance(snapshot, dict):
        return snapshot
    return {}


def published_service_url(service_name: str) -> str:
    if service_name == current_service_name():
        current_url = os.environ.get("GC_SERVICE_PUBLIC_URL", "")
        if current_url:
            return current_url
    snapshot = published_service_snapshot(service_name)
    current_url = snapshot.get("current_url")
    if isinstance(current_url, str):
        return current_url
    return ""


def admin_url() -> str:
    return published_service_url(ADMIN_SERVICE_NAME)


def interactions_url() -> str:
    return published_service_url(INTERACTIONS_SERVICE_NAME)


def build_status_snapshot(limit: int = 20) -> dict[str, Any]:
    config = load_config()
    return {
        "service_name": current_service_name(),
        "admin_url": admin_url(),
        "interactions_url": interactions_url(),
        "config": redact_config(config),
        "gateway_status": redact_gateway_status(load_gateway_status()),
        "recent_requests": [redact_request_record(item) for item in list_recent_requests(limit=limit)],
        "chat_bindings": list_chat_bindings(config),
        "chat_launchers": list_room_launchers(config),
        "recent_chat_ingress": [redact_chat_ingress_record(item) for item in list_recent_chat_ingress(limit=limit)],
        "recent_chat_publishes": [redact_chat_publish_record(item) for item in list_recent_chat_publishes(limit=limit)],
        "recent_room_launches": [redact_room_launch_record(item) for item in list_room_launches(limit=limit)],
    }


def discord_public_key_pem(public_key_hex: str) -> str:
    raw = bytes.fromhex(public_key_hex.strip())
    der = ED25519_SPKI_PREFIX + raw
    encoded = base64.b64encode(der).decode("ascii")
    lines = [encoded[index : index + 64] for index in range(0, len(encoded), 64)]
    return "-----BEGIN PUBLIC KEY-----\n" + "\n".join(lines) + "\n-----END PUBLIC KEY-----\n"


def verify_discord_signature(public_key_hex: str, timestamp: str, payload: bytes, signature_hex: str) -> bool:
    if not public_key_hex or not timestamp or not signature_hex:
        return False
    try:
        signature = bytes.fromhex(signature_hex)
        message = timestamp.encode("utf-8") + payload
        public_key_pem = discord_public_key_pem(public_key_hex)
    except ValueError:
        return False
    key_path = ""
    message_path = ""
    signature_path = ""
    result: subprocess.CompletedProcess[str] | None = None
    try:
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as key_handle:
            key_handle.write(public_key_pem)
            key_handle.flush()
            os.fchmod(key_handle.fileno(), 0o600)
            key_path = key_handle.name
        with tempfile.NamedTemporaryFile(delete=False) as message_handle:
            message_handle.write(message)
            message_handle.flush()
            message_path = message_handle.name
        with tempfile.NamedTemporaryFile(delete=False) as signature_handle:
            signature_handle.write(signature)
            signature_handle.flush()
            signature_path = signature_handle.name
        result = subprocess.run(
            [
                "openssl",
                "pkeyutl",
                "-verify",
                "-pubin",
                "-inkey",
                key_path,
                "-rawin",
                "-in",
                message_path,
                "-sigfile",
                signature_path,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return False
    finally:
        for path in (key_path, message_path, signature_path):
            if not path:
                continue
            try:
                os.remove(path)
            except FileNotFoundError:
                continue
    return bool(result and result.returncode == 0)


def discord_api_request(
    method: str,
    path: str,
    payload: Any = None,
    bot_token: str | None = None,
) -> Any:
    if path.startswith("http://") or path.startswith("https://"):
        url = path
    else:
        url = urllib.parse.urljoin(DISCORD_API_BASE.rstrip("/") + "/", path.lstrip("/"))
    body = None
    headers = {
        "Accept": "application/json",
        "User-Agent": "gas-city-discord/0.1",
    }
    token = bot_token or load_bot_token()
    if token:
        headers["Authorization"] = f"Bot {token}"
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=body, headers=headers, method=method.upper())
    for attempt in range(DISCORD_RATE_LIMIT_RETRIES + 1):
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                raw = response.read()
            break
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            if exc.code == 429 and attempt < DISCORD_RATE_LIMIT_RETRIES:
                retry_after = discord_retry_after_seconds(exc, raw)
                time.sleep(retry_after)
                continue
            message = raw.decode("utf-8", errors="replace")
            raise DiscordAPIError(f"{method.upper()} {url} failed with {exc.code}: {message}", status_code=exc.code) from exc
        except urllib.error.URLError as exc:
            raise DiscordAPIError(f"{method.upper()} {url} failed: {exc}") from exc
    if not raw:
        return {}
    try:
        return json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise DiscordAPIError(f"{method.upper()} {url} returned invalid JSON") from exc


def discord_retry_after_seconds(exc: urllib.error.HTTPError, raw: bytes) -> float:
    header_value = exc.headers.get("Retry-After") if exc.headers else ""
    try:
        if header_value:
            return max(float(header_value), 0.0)
    except ValueError:
        pass
    if raw:
        try:
            payload = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            payload = {}
        try:
            value = float((payload or {}).get("retry_after", 0) or 0)
        except (TypeError, ValueError):
            value = 0.0
        if value > 0:
            return value
    return 1.0


def city_toml_path() -> str:
    root = city_root()
    if not root:
        return "city.toml"
    return os.path.join(root, "city.toml")


def load_city_toml() -> dict[str, Any]:
    path = city_toml_path()
    try:
        with open(path, "rb") as handle:
            payload = tomllib.load(handle)
    except FileNotFoundError as exc:
        raise GCAPIError(f"city.toml not found at {path}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise GCAPIError(f"invalid city.toml: {exc}") from exc
    if isinstance(payload, dict):
        return payload
    raise GCAPIError("city.toml did not parse to an object")


def normalize_gc_api_bind(value: Any) -> str:
    bind = str(value or "").strip()
    if bind in {"", "0.0.0.0", "*"}:
        return "127.0.0.1"
    if bind in {"::", "[::]", "::1", "[::1]"}:
        return "[::1]"
    return bind or "127.0.0.1"


def discover_supervisor_gc_api_scope(city_cfg: dict[str, Any]) -> str:
    workspace_cfg = city_cfg.get("workspace") or {}
    workspace_name = ""
    if isinstance(workspace_cfg, dict):
        workspace_name = str(workspace_cfg.get("name", "")).strip()
    request = urllib.request.Request(
        DEFAULT_SUPERVISOR_API_BASE + "/v0/cities",
        headers={
            "Accept": "application/json",
            "User-Agent": "gas-city-discord/0.1",
            "X-GC-Request": "true",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=1.0) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError):
        return ""
    items = payload.get("items")
    if not isinstance(items, list):
        return ""
    if workspace_name:
        for item in items:
            if not isinstance(item, dict):
                continue
            if str(item.get("name", "")).strip() != workspace_name:
                continue
            if item.get("running") is False:
                return ""
            return f"/v0/city/{urllib.parse.quote(workspace_name)}"
    if not workspace_name and len(items) == 1 and isinstance(items[0], dict):
        inferred_name = str(items[0].get("name", "")).strip()
        if inferred_name and items[0].get("running") is not False:
            return f"/v0/city/{urllib.parse.quote(inferred_name)}"
    return ""


def gc_api_base_url() -> str:
    override = str(os.environ.get("GC_API_BASE_URL", "")).strip()
    if override:
        return override.rstrip("/")
    city_cfg = load_city_toml()
    if discover_supervisor_gc_api_scope(city_cfg):
        return DEFAULT_SUPERVISOR_API_BASE
    api_cfg = city_cfg.get("api") or {}
    if not isinstance(api_cfg, dict):
        api_cfg = {}
    port_value = api_cfg.get("port", DEFAULT_GC_API_PORT)
    if port_value in (None, ""):
        port_value = DEFAULT_GC_API_PORT
    port = int(port_value)
    if port <= 0:
        raise GCAPIError("gc api is disabled (api.port = 0)")
    bind = normalize_gc_api_bind(api_cfg.get("bind", "127.0.0.1"))
    return f"http://{bind}:{port}"


def gc_api_request(
    method: str,
    path: str,
    payload: Any = None,
    headers: dict[str, str] | None = None,
    timeout: float = GC_API_REQUEST_TIMEOUT_SECONDS,
) -> Any:
    base_url = gc_api_base_url()
    if path.startswith("http://") or path.startswith("https://"):
        url = path
    else:
        city_cfg = load_city_toml()
        scope_prefix = discover_supervisor_gc_api_scope(city_cfg)
        normalized_path = path
        if scope_prefix and path.startswith("/v0/"):
            normalized_path = scope_prefix + "/" + path[len("/v0/") :]
        url = urllib.parse.urljoin(base_url.rstrip("/") + "/", normalized_path.lstrip("/"))
    body = None
    request_headers = {
        "Accept": "application/json",
        "User-Agent": "gas-city-discord/0.1",
        "X-GC-Request": "true",
    }
    if headers:
        request_headers.update(headers)
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        request_headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=body, headers=request_headers, method=method.upper())
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        message = raw.decode("utf-8", errors="replace")
        raise GCAPIError(f"{method.upper()} {url} failed with {exc.code}: {message}") from exc
    except urllib.error.URLError as exc:
        raise GCAPIError(f"{method.upper()} {url} failed: {exc}") from exc
    if not raw:
        return {}
    try:
        return json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise GCAPIError(f"{method.upper()} {url} returned invalid JSON") from exc


def load_session_transcript_raw(session_selector: str, tail: int = 20) -> list[dict[str, Any]]:
    selector = str(session_selector).strip()
    if not selector:
        raise GCAPIError("session selector is required")
    payload = gc_api_request(
        "GET",
        f"/v0/session/{urllib.parse.quote(selector, safe='')}/transcript?format=raw&tail={max(0, int(tail))}",
    )
    messages = payload.get("messages") if isinstance(payload, dict) else None
    if not isinstance(messages, list):
        return []
    return [item for item in messages if isinstance(item, dict)]


def current_session_selector() -> str:
    for key in ("GC_SESSION_ID", "GC_SESSION_NAME", "GC_ALIAS"):
        value = str(os.environ.get(key, "")).strip()
        if value:
            return value
    return ""


def _raw_user_message_text(entry: dict[str, Any]) -> str:
    if str(entry.get("type", "")).strip() != "user":
        return ""
    message = entry.get("message")
    if not isinstance(message, dict):
        return ""
    content = message.get("content")
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    parts: list[str] = []
    for item in content:
        if isinstance(item, str):
            parts.append(item)
            continue
        if not isinstance(item, dict):
            continue
        if str(item.get("type", "")).strip() == "text":
            parts.append(str(item.get("text", "")))
    return "".join(parts)


def _extract_discord_event_fields(text: str) -> dict[str, str]:
    body = str(text)
    start = body.find("<discord-event>")
    end = body.find("</discord-event>")
    if start < 0 or end < 0 or end <= start:
        return {}
    inner = body[start + len("<discord-event>") : end]
    fields: dict[str, str] = {}
    for raw_line in inner.splitlines():
        line = raw_line.strip()
        if not line or ":" not in line:
            continue
        key, value = line.split(":", 1)
        fields[key.strip()] = value.strip()
    return fields


def find_latest_discord_reply_context(session_selector: str = "", tail: int = 40) -> dict[str, str]:
    selector = str(session_selector).strip() or current_session_selector()
    if not selector:
        raise GCAPIError("GC_SESSION_ID or GC_SESSION_NAME is required")
    for entry in reversed(load_session_transcript_raw(selector, tail=tail)):
        fields = _extract_discord_event_fields(_raw_user_message_text(entry))
        if fields.get("publish_binding_id"):
            return fields
    raise GCAPIError(f"no recent discord event with publish metadata found for {selector}")


def list_city_sessions(state: str = "all") -> list[dict[str, Any]]:
    suffix = ""
    normalized_state = str(state).strip()
    if normalized_state:
        suffix = "?state=" + urllib.parse.quote(normalized_state)
    payload = gc_api_request("GET", "/v0/sessions" + suffix)
    items = payload.get("items") if isinstance(payload, dict) else None
    if not isinstance(items, list):
        return []
    return [item for item in items if isinstance(item, dict)]


def list_city_agents() -> list[dict[str, Any]]:
    payload = gc_api_request("GET", "/v0/agents")
    items = payload.get("items") if isinstance(payload, dict) else None
    if not isinstance(items, list):
        return []
    return [item for item in items if isinstance(item, dict)]


def _agent_base_handle(qualified_handle: str) -> str:
    normalized = str(qualified_handle).strip().lower()
    if not normalized:
        return ""
    return normalized.rsplit("/", 1)[-1]


def _normalize_agent_handle(handle: str) -> str:
    normalized = str(handle).strip().lower()
    if not normalized:
        return ""
    parts = normalized.split("/")
    if len(parts) > 2:
        return ""
    if any(not AGENT_HANDLE_SEGMENT.fullmatch(part) for part in parts):
        return ""
    return normalized


def resolve_agent_handle(handle: str) -> tuple[str, str]:
    normalized = _normalize_agent_handle(handle)
    if not normalized:
        return "", "malformed_handle"
    qualified_lookup: dict[str, dict[str, Any]] = {}
    bare_lookup: dict[str, list[str]] = {}
    for item in list_city_agents():
        qualified_name = _normalize_agent_handle(item.get("name", ""))
        if not qualified_name:
            continue
        qualified_lookup[qualified_name] = item
        base_handle = _agent_base_handle(qualified_name)
        bare_lookup.setdefault(base_handle, []).append(qualified_name)
    if "/" in normalized:
        if normalized in qualified_lookup:
            return normalized, ""
        return "", "unknown_handle"
    matches = bare_lookup.get(normalized, [])
    if len(matches) == 1:
        return matches[0], ""
    if len(matches) > 1:
        return "", "ambiguous_handle"
    return "", "unknown_handle"


def service_socket_is_active(socket_path: str, timeout: float = SERVICE_SOCKET_PROBE_TIMEOUT_SECONDS) -> bool:
    path = str(socket_path).strip()
    if not path or not os.path.exists(path):
        return False
    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    probe.settimeout(timeout)
    try:
        probe.connect(path)
    except OSError:
        return False
    finally:
        probe.close()
    return True


def prepare_service_socket(socket_path: str) -> None:
    path = str(socket_path).strip()
    if not path:
        raise RuntimeError("GC_SERVICE_SOCKET is required")
    if service_socket_is_active(path):
        raise RuntimeError(f"refusing to replace active service socket: {path}")
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


def session_index_by_name(state: str = "all") -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for item in list_city_sessions(state=state):
        session_name = str(item.get("session_name", "")).strip()
        if session_name:
            existing = index.get(session_name)
            if existing is None or _session_record_preference(item) > _session_record_preference(existing):
                index[session_name] = item
    return index


def session_index_by_alias(state: str = "all") -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for item in list_city_sessions(state=state):
        alias = str(item.get("alias", "")).strip()
        if alias:
            existing = index.get(alias)
            if existing is None or _session_record_preference(item) > _session_record_preference(existing):
                index[alias] = item
    return index


def resolve_session_identity(session_selector: str) -> dict[str, str]:
    selector = str(session_selector).strip()
    if not selector:
        return {}
    session_by_alias = session_index_by_alias(state="all").get(selector)
    if session_by_alias:
        return {
            "session_name": str(session_by_alias.get("session_name", "")).strip(),
            "session_id": str(session_by_alias.get("id", "")).strip(),
            "alias": str(session_by_alias.get("alias", "")).strip(),
        }
    session_by_name = session_index_by_name(state="all").get(selector)
    if session_by_name:
        return {
            "session_name": str(session_by_name.get("session_name", "")).strip(),
            "session_id": str(session_by_name.get("id", "")).strip(),
            "alias": str(session_by_name.get("alias", "")).strip(),
        }
    for item in list_city_sessions(state="all"):
        session_id = str(item.get("id", "")).strip()
        if session_id != selector:
            continue
        return {
            "session_name": str(item.get("session_name", "")).strip(),
            "session_id": session_id,
            "alias": str(item.get("alias", "")).strip(),
        }
    raise GCAPIError(f"session not found: {selector}")


def _session_record_preference(item: dict[str, Any]) -> tuple[int, int, int, str]:
    state = str(item.get("state", "")).strip()
    return (
        0 if state in NON_ROUTABLE_SESSION_STATES else 1,
        1 if item.get("running") is True else 0,
        1 if item.get("attached") is True else 0,
        str(item.get("created_at", "")).strip(),
    )


def session_record_routable(item: dict[str, Any] | None) -> bool:
    if not isinstance(item, dict):
        return False
    return str(item.get("state", "")).strip() not in NON_ROUTABLE_SESSION_STATES


def _room_launch_alias_slug(qualified_handle: str) -> str:
    normalized = str(qualified_handle).strip().lower().replace("/", "-")
    filtered = "".join(ch for ch in normalized if ch.isalnum() or ch in {"-", "_"})
    filtered = filtered.strip("-_")
    if not filtered:
        filtered = "agent"
    return filtered[:ROOM_LAUNCH_ALIAS_SLUG_MAX]


def room_launch_session_alias(guild_id: str, conversation_id: str, root_message_id: str, qualified_handle: str) -> str:
    digest_input = f"{str(guild_id).strip()}:{str(conversation_id).strip()}:{str(root_message_id).strip()}:{str(qualified_handle).strip().lower()}"
    digest = hashlib.sha256(digest_input.encode("utf-8")).hexdigest()[:12]
    slug = _room_launch_alias_slug(qualified_handle)
    alias = f"dc-{digest}-{slug}" if slug else f"dc-{digest}"
    return alias[:63].rstrip("-_")


def room_launch_thread_name(qualified_handle: str, source_display: str = "") -> str:
    handle_label = str(qualified_handle).strip().rsplit("/", 1)[-1] or "agent"
    display = " ".join(str(source_display).strip().split())
    if display:
        value = f"{handle_label} - {display}"
    else:
        value = handle_label
    return value[:100].strip() or handle_label


def create_agent_session(
    qualified_handle: str,
    *,
    alias: str,
    title: str,
    initial_message: str = "",
    idempotency_key: str = "",
) -> dict[str, Any]:
    headers: dict[str, str] = {}
    if idempotency_key:
        headers["Idempotency-Key"] = idempotency_key
    payload = gc_api_request(
        "POST",
        "/v0/sessions",
        payload={
            "kind": "agent",
            "name": str(qualified_handle).strip(),
            "alias": str(alias).strip(),
            "title": str(title).strip(),
            "message": str(initial_message),
        },
        headers=headers,
    )
    if not isinstance(payload, dict):
        return {}
    return payload


def ensure_room_launch_session(
    launch: dict[str, Any],
    *,
    title: str = "",
    initial_message: str = "",
) -> dict[str, Any]:
    launch_id = str(launch.get("launch_id", "")).strip()
    qualified_handle = str(launch.get("qualified_handle", "")).strip()
    session_alias = str(launch.get("session_alias", "")).strip()
    if not launch_id or not qualified_handle or not session_alias:
        raise ValueError("launch is missing required session fields")
    with advisory_lock(room_launch_lock_path(launch_id)):
        current = load_room_launch(launch_id) or dict(launch)
        current_alias = str(current.get("session_alias", "")).strip() or session_alias
        existing = session_index_by_alias(state="all").get(current_alias)
        if session_record_routable(existing):
            current["session_alias"] = str(existing.get("alias", "")).strip() or current_alias
            current["session_id"] = str(existing.get("id", "")).strip()
            current["session_name"] = str(existing.get("session_name", "")).strip()
            save_room_launch(current)
            return current
        created = create_agent_session(
            qualified_handle,
            alias=current_alias,
            title=title or room_launch_thread_name(qualified_handle, str(current.get("from_display", "")).strip()),
            initial_message=initial_message,
            idempotency_key=f"{launch_id}:create-session",
        )
        current["session_alias"] = str(created.get("alias", "")).strip() or current_alias
        current["session_id"] = str(created.get("id", "")).strip()
        current["session_name"] = str(created.get("session_name", "")).strip()
        save_room_launch(current)
        return current


def create_thread_from_message(parent_channel_id: str, source_message_id: str, name: str) -> dict[str, Any]:
    return discord_api_request(
        "POST",
        f"/channels/{urllib.parse.quote(str(parent_channel_id).strip())}/messages/{urllib.parse.quote(str(source_message_id).strip())}/threads",
        payload={"name": str(name).strip()[:100] or "agent"},
    )


def derive_publish_source_metadata(source_context: dict[str, str] | None = None) -> dict[str, str]:
    fields = source_context if isinstance(source_context, dict) else {}
    source_kind = str(fields.get("kind", "")).strip()
    ingress_id = str(fields.get("ingress_receipt_id", "")).strip()
    root_ingress_id = str(fields.get("root_ingress_receipt_id", "")).strip()
    if source_kind == "discord_human_message" and ingress_id:
        root_ingress_id = ingress_id
    return {
        "source_event_kind": source_kind,
        "ingress_receipt_id": ingress_id,
        "root_ingress_receipt_id": root_ingress_id,
        "publish_binding_id": str(fields.get("publish_binding_id", "")).strip(),
        "publish_trigger_id": str(fields.get("publish_trigger_id", "")).strip(),
        "publish_reply_to_discord_message_id": str(fields.get("publish_reply_to_discord_message_id", "")).strip(),
        "launch_id": str(fields.get("launch_id", "")).strip() or str(fields.get("publish_launch_id", "")).strip(),
    }


def _derive_publish_source_metadata(source_context: dict[str, str] | None = None) -> dict[str, str]:
    return derive_publish_source_metadata(source_context)


def _strip_inline_code(text: str) -> str:
    out: list[str] = []
    i = 0
    while i < len(text):
        ch = text[i]
        if ch == "`":
            j = i
            while j < len(text) and text[j] == "`":
                j += 1
            ticks = text[i:j]
            close = text.find(ticks, j)
            out.extend(" " * len(ticks))
            if close < 0:
                i = j
                continue
            out.extend(" " * (close - j))
            out.extend(" " * len(ticks))
            i = close + len(ticks)
            continue
        out.append(ch)
        i += 1
    return "".join(out)


def _peer_routing_visible_text(body: str) -> str:
    lines: list[str] = []
    in_fence = False
    in_block_quote = False
    for raw_line in str(body).splitlines():
        line = raw_line
        stripped = line.lstrip()
        if stripped.startswith("```"):
            in_fence = not in_fence
            lines.append("")
            continue
        if in_fence:
            lines.append("")
            continue
        if stripped.startswith(">>>"):
            in_block_quote = True
            lines.append("")
            continue
        if in_block_quote:
            lines.append("")
            continue
        if stripped.startswith(">"):
            lines.append("")
            continue
        visible = URL_PATTERN.sub(" ", _strip_inline_code(line))
        lines.append(visible)
    return "\n".join(lines)


def extract_peer_session_mentions(body: str) -> list[str]:
    text = _peer_routing_visible_text(body)
    mentions: list[str] = []
    seen: set[str] = set()
    i = 0
    while i < len(text):
        if text[i] != "@":
            i += 1
            continue
        prev = text[i - 1] if i > 0 else ""
        if prev == "<":
            i += 1
            continue
        if prev and (prev.isalnum() or prev == "_"):
            i += 1
            continue
        j = i + 1
        if j >= len(text) or not text[j].isdigit() and not ("a" <= text[j] <= "z"):
            i += 1
            continue
        while j < len(text) and (text[j].isdigit() or ("a" <= text[j] <= "z") or text[j] in {"_", "-"}):
            j += 1
        token = text[i + 1 : j]
        if token and token not in DISCORD_RESERVED_MENTIONS and canonical_peer_session_name(token) == token and token not in seen:
            seen.add(token)
            mentions.append(token)
        i = j
    return mentions


def extract_agent_handles(body: str) -> list[str]:
    text = _peer_routing_visible_text(body)
    handles: list[str] = []
    seen: set[str] = set()
    i = 0
    while i < len(text):
        if text[i : i + 2] != "@@":
            i += 1
            continue
        prev = text[i - 1] if i > 0 else ""
        if prev and (prev.isalnum() or prev in {"_", "@"}):
            i += 1
            continue
        j = i + 2
        parts: list[str] = []
        while len(parts) < 2:
            start = j
            while j < len(text):
                lower = text[j].lower()
                if ("a" <= lower <= "z") or text[j].isdigit() or text[j] in {"_", "-"}:
                    j += 1
                    continue
                break
            part = text[start:j]
            if not part:
                break
            parts.append(part)
            if j < len(text) and text[j] == "/" and len(parts) == 1:
                j += 1
                continue
            break
        candidate = "/".join(parts)
        normalized = _normalize_agent_handle(candidate)
        if normalized and normalized not in seen:
            seen.add(normalized)
            handles.append(normalized)
        i = max(i + 2, j)
    return handles


def _binding_session_lookup(binding: dict[str, Any]) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for name in binding.get("session_names", []):
        session_name = str(name).strip()
        if session_name:
            lookup[session_name] = session_name
    return lookup


def _peer_delivery_payload(record: dict[str, Any]) -> dict[str, Any]:
    payload = record.get("peer_delivery")
    if isinstance(payload, dict):
        return copy.deepcopy(payload)
    return {
        "phase": "discord_posted",
        "status": "",
        "delivery": "",
        "mentioned_session_names": [],
        "frozen_targets": [],
        "targets": [],
        "budget_snapshot": {},
    }


def _save_chat_publish_record(record: dict[str, Any]) -> dict[str, Any]:
    return save_chat_publish(record)


def _update_peer_target(peer_delivery: dict[str, Any], session_name: str, patch: dict[str, Any]) -> None:
    targets = peer_delivery.setdefault("targets", [])
    for entry in targets:
        if str(entry.get("session_name", "")).strip() == session_name:
            entry.update(copy.deepcopy(patch))
            return
    item = {"session_name": session_name}
    item.update(copy.deepcopy(patch))
    targets.append(item)


def _peer_attempt(session_name: str, status: str, reason: str = "", response: dict[str, Any] | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "at": utcnow(),
        "status": status,
    }
    if reason:
        payload["reason"] = reason
    if response:
        payload["response"] = response
    payload["session_name"] = session_name
    return payload


def _count_matching_publishes(
    *,
    binding_id: str,
    root_ingress_receipt_id: str,
    source_session_name: str = "",
    source_event_kind: str = "",
    since_epoch: float | None = None,
    exclude_publish_id: str = "",
    records: list[dict[str, Any]] | None = None,
) -> int:
    total = 0
    publish_records = records if records is not None else iter_chat_publishes()
    for record in publish_records:
        if exclude_publish_id and str(record.get("publish_id", "")).strip() == exclude_publish_id:
            continue
        if str(record.get("binding_id", "")).strip() != binding_id:
            continue
        if root_ingress_receipt_id and str(record.get("root_ingress_receipt_id", "")).strip() != root_ingress_receipt_id:
            continue
        if source_session_name and str(record.get("source_session_name", "")).strip() != source_session_name:
            continue
        if source_event_kind and str(record.get("source_event_kind", "")).strip() != source_event_kind:
            continue
        if since_epoch is not None:
            created = parse_utc_timestamp(str(record.get("created_at", "")).strip())
            if created is None or created < since_epoch:
                continue
        total += 1
    return total


def _count_root_peer_deliveries(binding_id: str, root_ingress_receipt_id: str, records: list[dict[str, Any]] | None = None) -> int:
    total = 0
    publish_records = records if records is not None else iter_chat_publishes()
    for record in publish_records:
        if str(record.get("binding_id", "")).strip() != binding_id:
            continue
        if str(record.get("root_ingress_receipt_id", "")).strip() != root_ingress_receipt_id:
            continue
        peer_delivery = record.get("peer_delivery")
        if not isinstance(peer_delivery, dict):
            continue
        frozen = peer_delivery.get("frozen_targets")
        if isinstance(frozen, list):
            total += len([item for item in frozen if str(item).strip()])
    return total


def _count_root_peer_triggered_publishes(
    binding_id: str,
    root_ingress_receipt_id: str,
    source_session_name: str,
    *,
    exclude_publish_id: str = "",
) -> int:
    index = load_peer_root_budget_index(binding_id, root_ingress_receipt_id)
    total = 0
    for publish_id, item in index.get("entries", {}).items():
        if exclude_publish_id and publish_id == exclude_publish_id:
            continue
        if not isinstance(item, dict):
            continue
        if str(item.get("source_session_name", "")).strip() != source_session_name:
            continue
        if str(item.get("source_event_kind", "")).strip() != "discord_peer_publication":
            continue
        total += 1
    return total


def _count_root_peer_deliveries_from_index(binding_id: str, root_ingress_receipt_id: str) -> int:
    index = load_peer_root_budget_index(binding_id, root_ingress_receipt_id)
    total = 0
    for item in index.get("entries", {}).values():
        if not isinstance(item, dict):
            continue
        total += max(0, int(item.get("frozen_target_count", 0) or 0))
    return total


def _resolve_peer_targets(
    binding: dict[str, Any],
    *,
    body: str,
    source_session_name: str,
    source_event_kind: str,
) -> tuple[list[str], str, str, list[str]]:
    policy = binding_peer_policy(binding)
    session_lookup = _binding_session_lookup(binding)
    mentions = extract_peer_session_mentions(body)
    if mentions:
        if any(name not in session_lookup for name in mentions):
            return [], "targeted", "failed_targeting_unknown_session", mentions
        targets = [name for name in mentions if name != source_session_name]
        if not targets:
            return [], "targeted", "failed_targeting_self_only", mentions
        return targets, "targeted", "", mentions
    if source_event_kind == "discord_peer_publication":
        return [], "untargeted", "skipped_peer_target_required", []
    if not policy.get("allow_untargeted_peer_fanout"):
        return [], "untargeted", "skipped_policy_untargeted_disabled", []
    targets = [name for name in session_lookup if name != source_session_name]
    if not targets:
        return [], "untargeted", "skipped_no_peer_targets", []
    return targets, "untargeted", "", []


def _build_peer_envelope(
    *,
    binding: dict[str, Any],
    record: dict[str, Any],
    source_session_name: str,
    source_session_id: str,
    target_session_name: str,
    delivery: str,
    mentioned_session_names: list[str],
    root_ingress_receipt_id: str,
    idempotency_key: str,
) -> str:
    lines = [
        "<discord-event>",
        "version: 1",
        "kind: discord_peer_publication",
        f"binding_id: {str(binding.get('id', '')).strip()}",
        f"ingress_receipt_id: peer:{str(record.get('publish_id', '')).strip()}:target:{target_session_name}",
        f"conversation: {'guild:' + str(binding.get('guild_id', '')).strip() + ' channel:' + str(record.get('conversation_id', '')).strip() if str(binding.get('guild_id', '')).strip() else 'dm:' + str(record.get('conversation_id', '')).strip()}",
        f"conversation_key: {('guild:' + str(binding.get('guild_id', '')).strip() + ':conversation:' + str(record.get('conversation_id', '')).strip()) if str(binding.get('guild_id', '')).strip() else ('dm:' + str(record.get('conversation_id', '')).strip())}",
        f"discord_message_id: {str(record.get('remote_message_id', '')).strip()}",
        f"source_session_name: {source_session_name}",
        f"source_session_id: {source_session_id}",
        "source_kind: peer_session",
        f"target_session_name: {target_session_name}",
        f"publish_id: {str(record.get('publish_id', '')).strip()}",
        f"root_ingress_receipt_id: {root_ingress_receipt_id}",
        f"publish_binding_id: {str(binding.get('id', '')).strip()}",
        f"publish_conversation_id: {str(record.get('conversation_id', '')).strip()}",
        f"publish_trigger_id: {str(record.get('remote_message_id', '')).strip()}",
        f"publish_reply_to_discord_message_id: {str(record.get('remote_message_id', '')).strip()}",
        f"delivery_idempotency_key: {idempotency_key}",
        f"delivery: {delivery}",
        f"mentioned_session_names_json: {json.dumps(mentioned_session_names)}",
        f"untrusted_body_json: {json.dumps(str(record.get('body', '')))}",
        f"created_at: {str(record.get('created_at', '')).strip()}",
        "normal_output_visibility: internal_only",
        "reply_contract: explicit_publish_required",
        "hop: 1",
        "max_hop: 1",
        "</discord-event>",
    ]
    return "\n".join(lines)


def _promote_stale_in_progress_targets(record: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    body = copy.deepcopy(record)
    peer_delivery = _peer_delivery_payload(body)
    changed = False
    now = time.time()
    for entry in peer_delivery.get("targets", []):
        if str(entry.get("status", "")).strip() != "in_progress":
            continue
        attempted_at = parse_utc_timestamp(str(entry.get("attempted_at", "")).strip())
        if attempted_at is None or now - attempted_at < PEER_IN_PROGRESS_STALE_SECONDS:
            continue
        entry["status"] = "delivery_unknown"
        entry.setdefault("reason", "stale_in_progress")
        changed = True
    if changed:
        body["peer_delivery"] = peer_delivery
    return body, changed


def ensure_room_launch_thread(binding: dict[str, Any], launch_id: str) -> tuple[dict[str, Any], bool]:
    normalized_launch_id = str(launch_id).strip()
    if not normalized_launch_id:
        raise ValueError("launch_id is required")
    with advisory_lock(room_launch_lock_path(normalized_launch_id)):
        current = load_room_launch(normalized_launch_id)
        if not current:
            raise ValueError(f"launch not found: {normalized_launch_id}")
        thread_id = str(current.get("thread_id", "")).strip()
        if thread_id:
            return current, False
        parent_channel_id = str(binding.get("conversation_id", "")).strip()
        root_message_id = str(current.get("root_message_id", "")).strip()
        if not parent_channel_id or not root_message_id:
            raise ValueError("launch is missing thread creation metadata")
        thread = create_thread_from_message(
            parent_channel_id,
            root_message_id,
            room_launch_thread_name(
                str(current.get("qualified_handle", "")).strip(),
                str(current.get("from_display", "")).strip(),
            ),
        )
        # Discord message-started threads currently reuse the originating
        # message snowflake as the thread channel id. Keep the root message id
        # as a fallback so launch routing still has a stable key if the API
        # omits the new thread id in a degraded response.
        thread_id = str((thread or {}).get("id", "")).strip() or root_message_id
        current["thread_id"] = thread_id
        current["state"] = "active"
        current = save_room_launch(current)
        return current, True


def resolve_publish_conversation_id(binding: dict[str, Any], requested_conversation_id: str) -> str:
    binding_conversation_id = str(binding.get("conversation_id", "")).strip()
    requested = str(requested_conversation_id).strip()
    if not requested or requested == binding_conversation_id:
        return binding_conversation_id
    if str(binding.get("kind", "")).strip() == "dm":
        raise ValueError("--conversation-id cannot override a DM binding")
    try:
        channel_info = discord_api_request("GET", f"/channels/{urllib.parse.quote(requested)}")
    except DiscordAPIError as exc:
        raise ValueError(f"failed to validate --conversation-id: {exc}") from exc
    parent_id = str((channel_info or {}).get("parent_id", "")).strip()
    if parent_id != binding_conversation_id:
        raise ValueError("--conversation-id must be the bound room or a thread within it")
    return requested


def resolve_publish_destination(
    binding: dict[str, Any],
    *,
    requested_conversation_id: str = "",
    trigger_id: str = "",
    reply_to_message_id: str = "",
    source_context: dict[str, str] | None = None,
) -> tuple[str, str, dict[str, Any] | None]:
    reply_target = str(reply_to_message_id).strip() or str(trigger_id).strip()
    source_meta = derive_publish_source_metadata(source_context)
    launch_id = str(source_meta.get("launch_id", "")).strip()
    if str(binding.get("publish_route_kind", "")).strip() != "room_launch" or not launch_id:
        conversation_id = resolve_publish_conversation_id(binding, requested_conversation_id)
        return conversation_id, reply_target, None
    current = load_room_launch(launch_id)
    if not current:
        raise ValueError(f"launch not found: {launch_id}")
    thread_id = str(current.get("thread_id", "")).strip()
    requested = str(requested_conversation_id).strip()
    binding_conversation_id = str(binding.get("conversation_id", "")).strip()
    if thread_id:
        if requested and requested not in {binding_conversation_id, thread_id}:
            raise ValueError("--conversation-id must match the launch thread for this room launch")
        return thread_id, reply_target, current
    if requested and requested != binding_conversation_id:
        raise ValueError("--conversation-id cannot override a pending room launch before the thread exists")
    current, created_thread = ensure_room_launch_thread(binding, launch_id)
    conversation_id = str(current.get("thread_id", "")).strip()
    if not conversation_id:
        raise ValueError("room launch did not produce a thread id")
    if created_thread:
        reply_target = ""
    return conversation_id, reply_target, current


def _peer_delivery_needs_attention(record: dict[str, Any]) -> bool:
    peer_delivery = record.get("peer_delivery")
    if not isinstance(peer_delivery, dict):
        return False
    phase = str(peer_delivery.get("phase", "")).strip()
    status = str(peer_delivery.get("status", "")).strip()
    if phase == "peer_fanout_partial_failure":
        return True
    if status.startswith("failed_"):
        return True
    return any(
        str(entry.get("status", "")).strip() in {"failed_retryable", "failed_permanent", "delivery_unknown"}
        for entry in peer_delivery.get("targets", [])
        if isinstance(entry, dict)
    )


def peer_delivery_exit_code(record: dict[str, Any]) -> int:
    return 2 if _peer_delivery_needs_attention(record) else 0


def _finalize_peer_delivery(record: dict[str, Any]) -> dict[str, Any]:
    body = copy.deepcopy(record)
    peer_delivery = _peer_delivery_payload(body)
    terminal_statuses = {
        str(entry.get("status", "")).strip()
        for entry in peer_delivery.get("targets", [])
        if isinstance(entry, dict)
    }
    status = str(peer_delivery.get("status", "")).strip()
    if {"pending", "in_progress"} & terminal_statuses:
        peer_delivery["phase"] = "peer_fanout_in_progress"
    elif status.startswith("failed_"):
        peer_delivery["phase"] = "peer_fanout_partial_failure"
    elif {"failed_retryable", "failed_permanent", "delivery_unknown"} & terminal_statuses:
        peer_delivery["phase"] = "peer_fanout_partial_failure"
        peer_delivery["status"] = "partial_failure"
    elif terminal_statuses:
        peer_delivery["phase"] = "peer_fanout_complete"
        if "delivered" in terminal_statuses:
            peer_delivery["status"] = "delivered"
        elif not status or status == "partial_failure":
            peer_delivery["status"] = "delivered"
    elif not status:
        peer_delivery["phase"] = "peer_fanout_complete"
        peer_delivery["status"] = "skipped_no_peer_targets"
    body["peer_delivery"] = peer_delivery
    return body


def _update_target_in_progress(
    *,
    publish_id: str,
    fallback_record: dict[str, Any],
    session_name: str,
    idempotency_key: str,
) -> tuple[dict[str, Any], int]:
    with advisory_lock(_safe_lock_name("chat-publish", publish_id)):
        current = load_chat_publish(publish_id) or copy.deepcopy(fallback_record)
        peer_delivery = _peer_delivery_payload(current)
        target_entry = next(
            (item for item in peer_delivery.get("targets", []) if str(item.get("session_name", "")).strip() == session_name),
            None,
        )
        if target_entry and str(target_entry.get("status", "")).strip() == "delivered":
            return current, int(target_entry.get("attempt_count", 0) or 0)
        attempt_count = int((target_entry or {}).get("attempt_count", 0) or 0) + 1
        _update_peer_target(
            peer_delivery,
            session_name,
            {
                "status": "in_progress",
                "idempotency_key": idempotency_key,
                "attempt_count": attempt_count,
                "attempted_at": utcnow(),
            },
        )
        current["peer_delivery"] = peer_delivery
        current = _save_chat_publish_record(current)
        return current, attempt_count


def _update_target_delivery_result(
    *,
    publish_id: str,
    fallback_record: dict[str, Any],
    session_name: str,
    response: dict[str, Any] | None = None,
    error: str = "",
) -> dict[str, Any]:
    with advisory_lock(_safe_lock_name("chat-publish", publish_id)):
        current = load_chat_publish(publish_id) or copy.deepcopy(fallback_record)
        peer_delivery = _peer_delivery_payload(current)
        target_entry = next(
            (item for item in peer_delivery.get("targets", []) if str(item.get("session_name", "")).strip() == session_name),
            None,
        )
        attempts = list((target_entry or {}).get("attempts", []))
        if error:
            attempts.append(_peer_attempt(session_name, "failed_retryable", error))
            _update_peer_target(
                peer_delivery,
                session_name,
                {
                    "status": "failed_retryable",
                    "reason": error,
                    "attempts": attempts,
                },
            )
        else:
            attempts.append(_peer_attempt(session_name, "delivered", response=response or {}))
            _update_peer_target(
                peer_delivery,
                session_name,
                {
                    "status": "delivered",
                    "delivered_at": utcnow(),
                    "response": response or {},
                    "attempts": attempts,
                },
            )
        current["peer_delivery"] = peer_delivery
        return _save_chat_publish_record(current)


def _apply_peer_fanout(
    record: dict[str, Any],
    binding: dict[str, Any],
    *,
    source_context: dict[str, str] | None = None,
) -> dict[str, Any]:
    if str(binding.get("kind", "")).strip() != "room":
        return record

    binding_id = str(binding.get("id", "")).strip()
    publish_id = str(record.get("publish_id", "")).strip()
    source_session_name = str(record.get("source_session_name", "")).strip()
    source_session_id = str(record.get("source_session_id", "")).strip()
    policy = binding_peer_policy(binding)
    source_meta = _derive_publish_source_metadata(source_context)
    root_ingress_receipt_id = source_meta.get("root_ingress_receipt_id", "")
    budget_lock_key = f"{binding_id}:{root_ingress_receipt_id or publish_id}"
    delivery_targets: list[str] = []
    delivery_mode = ""
    delivery_mentions: list[str] = []
    with advisory_lock(_safe_lock_name("peer-budget", budget_lock_key)):
        with advisory_lock(_safe_lock_name("chat-publish", publish_id)):
            current = load_chat_publish(publish_id) or copy.deepcopy(record)
            current, stale_changed = _promote_stale_in_progress_targets(current)
            if stale_changed:
                current = _save_chat_publish_record(current)
            peer_delivery = _peer_delivery_payload(current)
            peer_delivery.setdefault("delivery", "")
            peer_delivery.setdefault("mentioned_session_names", [])
            peer_delivery.setdefault("frozen_targets", [])
            peer_delivery.setdefault("targets", [])
            peer_delivery.setdefault("budget_snapshot", {})
            current["source_event_kind"] = source_meta.get("source_event_kind", "")
            current["root_ingress_receipt_id"] = root_ingress_receipt_id

            if not policy.get("peer_fanout_enabled"):
                peer_delivery["phase"] = "peer_fanout_complete"
                peer_delivery["status"] = "skipped_policy_disabled"
                current["peer_delivery"] = peer_delivery
                return _save_chat_publish_record(current)
            if not root_ingress_receipt_id:
                peer_delivery["phase"] = "peer_fanout_complete"
                peer_delivery["status"] = "skipped_missing_root_context"
                current["peer_delivery"] = peer_delivery
                return _save_chat_publish_record(current)
            if not source_session_name or source_session_name not in _binding_session_lookup(binding):
                peer_delivery["phase"] = "peer_fanout_partial_failure"
                peer_delivery["status"] = "failed_source_not_bound"
                current["peer_delivery"] = peer_delivery
                return _save_chat_publish_record(current)

            now = time.time()
            if source_meta.get("source_event_kind") == "discord_peer_publication":
                per_root_count = _count_root_peer_triggered_publishes(
                    binding_id,
                    root_ingress_receipt_id,
                    source_session_name,
                    exclude_publish_id=publish_id,
                )
                if per_root_count >= int(policy.get("max_peer_triggered_publishes_per_root", 0) or 0):
                    peer_delivery["phase"] = "peer_fanout_complete"
                    peer_delivery["status"] = "skipped_budget_exhausted"
                    peer_delivery["budget_snapshot"] = {"peer_triggered_publishes_per_root": per_root_count}
                    current["peer_delivery"] = peer_delivery
                    return _save_chat_publish_record(current)
                recent_publish_records = iter_chat_publishes_since(now - PEER_ROOT_WINDOW_SECONDS)
                minute_count = _count_matching_publishes(
                    binding_id=binding_id,
                    root_ingress_receipt_id="",
                    source_session_name=source_session_name,
                    source_event_kind="discord_peer_publication",
                    since_epoch=now - PEER_ROOT_WINDOW_SECONDS,
                    exclude_publish_id=publish_id,
                    records=recent_publish_records,
                )
                if minute_count >= int(policy.get("max_peer_triggered_publishes_per_session_per_minute", 0) or 0):
                    peer_delivery["phase"] = "peer_fanout_complete"
                    peer_delivery["status"] = "skipped_rate_limited"
                    peer_delivery["budget_snapshot"] = {"peer_triggered_publishes_per_session_per_minute": minute_count}
                    current["peer_delivery"] = peer_delivery
                    return _save_chat_publish_record(current)

            targets, delivery, resolve_reason, mentions = _resolve_peer_targets(
                binding,
                body=str(current.get("body", "")),
                source_session_name=source_session_name,
                source_event_kind=source_meta.get("source_event_kind", ""),
            )
            peer_delivery["delivery"] = delivery
            peer_delivery["mentioned_session_names"] = mentions
            if resolve_reason:
                peer_delivery["phase"] = "peer_fanout_partial_failure" if resolve_reason.startswith("failed_") else "peer_fanout_complete"
                peer_delivery["status"] = resolve_reason
                current["peer_delivery"] = peer_delivery
                return _save_chat_publish_record(current)

            session_index = session_index_by_name(state="all")
            live_targets: list[str] = []
            targeted_unavailable: list[str] = []
            for session_name in targets:
                session_payload = session_index.get(session_name)
                state = str((session_payload or {}).get("state", "")).strip()
                if not session_payload or state in NON_ROUTABLE_SESSION_STATES:
                    if delivery == "targeted":
                        targeted_unavailable.append(session_name)
                        _update_peer_target(
                            peer_delivery,
                            session_name,
                            {
                                "status": "failed_retryable",
                                "reason": "failed_targeting_unavailable",
                                "attempt_count": 0,
                                "attempts": [_peer_attempt(session_name, "failed_retryable", "failed_targeting_unavailable")],
                            },
                        )
                        continue
                    _update_peer_target(
                        peer_delivery,
                        session_name,
                        {
                            "status": "skipped",
                            "reason": "skipped_unavailable_target",
                            "attempt_count": 0,
                            "attempts": [_peer_attempt(session_name, "skipped", "skipped_unavailable_target")],
                        },
                    )
                    continue
                live_targets.append(session_name)

            if targeted_unavailable:
                for session_name in live_targets:
                    _update_peer_target(
                        peer_delivery,
                        session_name,
                        {
                            "status": "failed_retryable",
                            "reason": "blocked_by_unavailable_explicit_target",
                            "attempt_count": 0,
                            "attempts": [
                                _peer_attempt(session_name, "failed_retryable", "blocked_by_unavailable_explicit_target")
                            ],
                        },
                    )
                peer_delivery["phase"] = "peer_fanout_partial_failure"
                peer_delivery["status"] = "failed_targeting_unavailable"
                current["peer_delivery"] = peer_delivery
                return _save_chat_publish_record(current)

            if not live_targets:
                peer_delivery["phase"] = "peer_fanout_complete"
                peer_delivery["status"] = "skipped_no_live_targets"
                current["peer_delivery"] = peer_delivery
                return _save_chat_publish_record(current)

            total_peer_deliveries = _count_root_peer_deliveries_from_index(binding_id, root_ingress_receipt_id)
            projected = total_peer_deliveries + len(live_targets)
            peer_delivery["budget_snapshot"] = {
                "root_ingress_receipt_id": root_ingress_receipt_id,
                "existing_peer_deliveries_for_root": total_peer_deliveries,
                "projected_peer_deliveries_for_root": projected,
            }
            max_total = int(policy.get("max_total_peer_deliveries_per_root", 0) or 0)
            if max_total and projected > max_total:
                peer_delivery["phase"] = "peer_fanout_complete"
                peer_delivery["status"] = "skipped_budget_exhausted"
                current["peer_delivery"] = peer_delivery
                return _save_chat_publish_record(current)

            peer_delivery["frozen_targets"] = list(live_targets)
            peer_delivery["phase"] = "peer_fanout_in_progress"
            for session_name in live_targets:
                _update_peer_target(
                    peer_delivery,
                    session_name,
                    {
                        "status": "pending",
                        "attempt_count": 0,
                        "attempted_at": "",
                        "attempts": [],
                    },
                )
            current["peer_delivery"] = peer_delivery
            current = _save_chat_publish_record(current)
            delivery_targets = list(live_targets)
            delivery_mode = delivery
            delivery_mentions = list(mentions)

    for session_name in delivery_targets:
        idempotency_key = f"peer_publish:{publish_id}:binding:{binding_id}:target:{session_name}"
        current, _ = _update_target_in_progress(
            publish_id=publish_id,
            fallback_record=current,
            session_name=session_name,
            idempotency_key=idempotency_key,
        )
        envelope = _build_peer_envelope(
            binding=binding,
            record=current,
            source_session_name=source_session_name,
            source_session_id=source_session_id,
            target_session_name=session_name,
            delivery=delivery_mode,
            mentioned_session_names=delivery_mentions,
            root_ingress_receipt_id=root_ingress_receipt_id,
            idempotency_key=idempotency_key,
        )
        try:
            response = deliver_session_message(
                session_name,
                envelope,
                idempotency_key=idempotency_key,
                timeout=PEER_DELIVERY_TIMEOUT_SECONDS,
            )
        except GCAPIError as exc:
            current = _update_target_delivery_result(
                publish_id=publish_id,
                fallback_record=current,
                session_name=session_name,
                error=str(exc),
            )
            continue
        current = _update_target_delivery_result(
            publish_id=publish_id,
            fallback_record=current,
            session_name=session_name,
            response=response,
        )

    with advisory_lock(_safe_lock_name("chat-publish", publish_id)):
        current = load_chat_publish(publish_id) or current
        current, stale_changed = _promote_stale_in_progress_targets(current)
        if stale_changed:
            current = _save_chat_publish_record(current)
        current = _finalize_peer_delivery(current)
        return _save_chat_publish_record(current)


def retry_peer_fanout(
    publish_id: str,
    *,
    include_unknown: bool = False,
    target_session_names: list[str] | None = None,
) -> dict[str, Any]:
    record = load_chat_publish(publish_id)
    if not record:
        raise ValueError(f"publish not found: {publish_id}")
    binding_id = str(record.get("binding_id", "")).strip()
    binding = resolve_publish_route(load_config(), binding_id)
    if not binding:
        raise ValueError(f"binding not found: {binding_id}")
    retry_targets: list[tuple[str, str, str, list[str], str, str, str]] = []
    with advisory_lock(_safe_lock_name("chat-publish", publish_id)):
        current = load_chat_publish(publish_id) or record
        current, changed = _promote_stale_in_progress_targets(current)
        if changed:
            current = _save_chat_publish_record(current)
        peer_delivery = _peer_delivery_payload(current)
        eligible = {"failed_retryable"}
        if include_unknown:
            eligible.add("delivery_unknown")
        selected = {str(item).strip() for item in (target_session_names or []) if str(item).strip()}
        delivery = str(peer_delivery.get("delivery", "")).strip() or "targeted"
        mentioned_session_names = [str(item).strip() for item in peer_delivery.get("mentioned_session_names", []) if str(item).strip()]
        root_ingress_receipt_id = str(current.get("root_ingress_receipt_id", "")).strip()
        source_session_name = str(current.get("source_session_name", "")).strip()
        source_session_id = str(current.get("source_session_id", "")).strip()
        for entry in peer_delivery.get("targets", []):
            if not isinstance(entry, dict):
                continue
            session_name = str(entry.get("session_name", "")).strip()
            if selected and session_name not in selected:
                continue
            if str(entry.get("status", "")).strip() not in eligible:
                continue
            idempotency_key = str(entry.get("idempotency_key", "")).strip()
            if not idempotency_key:
                idempotency_key = f"peer_publish:{publish_id}:binding:{binding_id}:target:{session_name}"
            attempts = list(entry.get("attempts", []))
            attempts.append(_peer_attempt(session_name, "retrying"))
            _update_peer_target(
                peer_delivery,
                session_name,
                {
                    "status": "in_progress",
                    "attempt_count": int(entry.get("attempt_count", 0) or 0) + 1,
                    "attempted_at": utcnow(),
                    "idempotency_key": idempotency_key,
                    "attempts": attempts,
                },
            )
            retry_targets.append(
                (
                    session_name,
                    idempotency_key,
                    delivery,
                    mentioned_session_names,
                    root_ingress_receipt_id,
                    source_session_name,
                    source_session_id,
                )
            )
        current["peer_delivery"] = peer_delivery
        current = _save_chat_publish_record(current)

    for session_name, idempotency_key, delivery, mentioned_session_names, root_ingress_receipt_id, source_session_name, source_session_id in retry_targets:
        envelope = _build_peer_envelope(
            binding=binding,
            record=current,
            source_session_name=source_session_name,
            source_session_id=source_session_id,
            target_session_name=session_name,
            delivery=delivery,
            mentioned_session_names=mentioned_session_names,
            root_ingress_receipt_id=root_ingress_receipt_id,
            idempotency_key=idempotency_key,
        )
        try:
            response = deliver_session_message(
                session_name,
                envelope,
                idempotency_key=idempotency_key,
                timeout=PEER_DELIVERY_TIMEOUT_SECONDS,
            )
        except GCAPIError as exc:
            current = _update_target_delivery_result(
                publish_id=publish_id,
                fallback_record=current,
                session_name=session_name,
                error=str(exc),
            )
            continue
        current = _update_target_delivery_result(
            publish_id=publish_id,
            fallback_record=current,
            session_name=session_name,
            response=response,
        )
    with advisory_lock(_safe_lock_name("chat-publish", publish_id)):
        current = load_chat_publish(publish_id) or current
        current, changed = _promote_stale_in_progress_targets(current)
        if changed:
            current = _save_chat_publish_record(current)
        current = _finalize_peer_delivery(current)
        return _save_chat_publish_record(current)


def publish_binding_message(
    binding: dict[str, Any],
    body: str,
    *,
    requested_conversation_id: str = "",
    trigger_id: str = "",
    reply_to_message_id: str = "",
    source_context: dict[str, str] | None = None,
    source_session_name: str = "",
    source_session_id: str = "",
) -> dict[str, Any]:
    conversation_id, reply_target, launch = resolve_publish_destination(
        binding,
        requested_conversation_id=requested_conversation_id,
        trigger_id=trigger_id,
        reply_to_message_id=reply_to_message_id,
        source_context=source_context,
    )
    if not conversation_id:
        raise ValueError("binding is missing a destination conversation_id")
    response = post_channel_message(
        conversation_id,
        body,
        reply_to_message_id=reply_target,
    )
    remote_message_id = str((response or {}).get("id", "")).strip()
    if not remote_message_id:
        raise DiscordAPIError("discord publish returned no message id")
    source_meta = _derive_publish_source_metadata(source_context)
    resolved_source_identity: dict[str, str] = {}
    source_selector = str(source_session_name).strip() or str(source_session_id).strip() or current_session_selector()
    if source_selector and (not str(source_session_name).strip() or not str(source_session_id).strip()):
        try:
            resolved_source_identity = resolve_session_identity(source_selector)
        except GCAPIError:
            resolved_source_identity = {}
    effective_source_session_name = (
        str(source_session_name).strip()
        or str(resolved_source_identity.get("session_name", "")).strip()
        or str(os.environ.get("GC_SESSION_NAME", "")).strip()
    )
    effective_source_session_id = (
        str(source_session_id).strip()
        or str(resolved_source_identity.get("session_id", "")).strip()
        or str(os.environ.get("GC_SESSION_ID", "")).strip()
    )
    record = save_chat_publish(
        {
            "binding_id": str(binding.get("id", "")).strip(),
            "binding_kind": str(binding.get("kind", "")).strip(),
            "binding_conversation_id": str(binding.get("conversation_id", "")).strip(),
            "conversation_id": conversation_id,
            "guild_id": str(binding.get("guild_id", "")).strip(),
            "trigger_id": str(trigger_id).strip(),
            "reply_to_message_id": reply_target,
            "source_session_id": effective_source_session_id,
            "source_session_name": effective_source_session_name,
            "source_event_kind": source_meta.get("source_event_kind", ""),
            "root_ingress_receipt_id": source_meta.get("root_ingress_receipt_id", ""),
            "launch_id": source_meta.get("launch_id", ""),
            "launch_thread_id": str((launch or {}).get("thread_id", "")).strip(),
            "body": body,
            "remote_message_id": remote_message_id,
        }
    )
    record = _apply_peer_fanout(record, binding, source_context=source_context)
    return {"binding": binding, "record": record, "response": response}


def deliver_session_message(session_name: str, message: str, idempotency_key: str = "", timeout: float = GC_API_REQUEST_TIMEOUT_SECONDS) -> dict[str, Any]:
    headers: dict[str, str] = {}
    key = str(idempotency_key).strip()
    if key:
        headers["Idempotency-Key"] = key
    payload = gc_api_request(
        "POST",
        f"/v0/session/{urllib.parse.quote(str(session_name).strip(), safe='')}/messages",
        payload={"message": message},
        headers=headers,
        timeout=timeout,
    )
    if not isinstance(payload, dict):
        return {}
    return payload


def command_name(config: dict[str, Any]) -> str:
    return str(normalize_config(config).get("app", {}).get("command_name", COMMAND_NAME_DEFAULT)).strip() or COMMAND_NAME_DEFAULT


def build_command_payload(command_name_value: str, scope: str = "guild") -> list[dict[str, Any]]:
    command: dict[str, Any] = {
        "name": command_name_value,
        "type": 1,
        "description": "GC workspace actions",
        "options": [
            {
                "type": 1,
                "name": "fix",
                "description": "Create a GC fix workflow",
                "options": [
                    {
                        "type": 3,
                        "name": "rig",
                        "description": "Target rig for the fix workflow",
                        "required": False,
                    },
                    {
                        "type": 3,
                        "name": "prompt",
                        "description": "Optional fallback when modal collection is disabled",
                        "required": False,
                    }
                ],
            }
        ],
    }
    if scope == "global":
        command["contexts"] = [0]
        command["integration_types"] = [0]
    return [command]


def sync_guild_commands(config: dict[str, Any], guild_id: str) -> Any:
    app_cfg = normalize_config(config).get("app", {})
    application_id = str(app_cfg.get("application_id", "")).strip()
    if not application_id:
        raise DiscordAPIError("Discord application_id is not configured")
    payload = build_command_payload(command_name(config), scope="guild")
    return discord_api_request(
        "PUT",
        f"/applications/{urllib.parse.quote(application_id)}/guilds/{urllib.parse.quote(str(guild_id))}/commands",
        payload=payload,
    )


def post_channel_message(channel_id: str, body: str, reply_to_message_id: str = "") -> Any:
    payload: dict[str, Any] = {
        "content": body,
        "allowed_mentions": {"parse": []},
    }
    reply_to_message_id = str(reply_to_message_id).strip()
    if reply_to_message_id:
        payload["message_reference"] = {
            "channel_id": str(channel_id),
            "message_id": reply_to_message_id,
            "fail_if_not_exists": False,
        }
    return discord_api_request(
        "POST",
        f"/channels/{urllib.parse.quote(str(channel_id))}/messages",
        payload=payload,
    )


def discord_jump_url(guild_id: str, conversation_id: str) -> str:
    if not guild_id or not conversation_id:
        return ""
    if not str(guild_id).isdigit() or not str(conversation_id).isdigit():
        return ""
    return f"https://discord.com/channels/{guild_id}/{conversation_id}"


def policy_reason(config: dict[str, Any], guild_id: str, parent_channel_id: str, role_ids: list[str]) -> str:
    normalized = normalize_config(config)
    policy = normalized.get("policy", {})
    guild_allowlist = set(_normalize_allowlist(policy.get("guild_allowlist")))
    channel_allowlist = set(_normalize_allowlist(policy.get("channel_allowlist")))
    role_allowlist = set(_normalize_allowlist(policy.get("role_allowlist")))
    if guild_allowlist and guild_id not in guild_allowlist:
        return "guild_not_allowed"
    if channel_allowlist and parent_channel_id not in channel_allowlist:
        return "channel_not_allowed"
    if role_allowlist and not role_allowlist.intersection(set(role_ids)):
        return "role_not_allowed"
    return ""
