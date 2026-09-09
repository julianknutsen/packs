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
import threading
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable

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
GC_API_ASYNC_RESULT_TIMEOUT_SECONDS = 4 * 60.0
GC_EVENT_REQUEST_FAILED = "request.failed"
GC_EVENT_SESSION_MESSAGE_SUCCEEDED = "request.result.session.message"
GC_EVENT_SESSION_SUBMIT_SUCCEEDED = "request.result.session.submit"
GC_OPERATION_SESSION_MESSAGE = "session.message"
GC_OPERATION_SESSION_SUBMIT = "session.submit"
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
ROOM_LAUNCH_MESSAGE_TARGET_LIMIT = 64
ROOM_LAUNCH_IDENTITY_RESOLVE_TIMEOUT_SECONDS = 30.0
ROOM_LAUNCH_IDENTITY_RESOLVE_DELAY_SECONDS = 0.25
ROOM_LAUNCH_READY_RESOLVE_TIMEOUT_SECONDS = 90.0
ROOM_LAUNCH_READY_RESOLVE_DELAY_SECONDS = 0.5
ROOM_LAUNCH_PRIMER_VERSION = 1
AGENT_HANDLE_SEGMENT = re.compile(r"^[a-z][a-z0-9_-]{0,31}$")
DISCORD_APP_NAME = re.compile(r"^[a-z][a-z0-9_-]{0,31}$")


class DiscordAPIError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class GCAPIError(RuntimeError):
    pass


class GCAPITransportError(GCAPIError):
    pass


class GCAPIResultUnknown(GCAPIError):
    pass


class GCAPIRequestCancelled(GCAPIResultUnknown):
    pass


class GCAPIRequestFailed(GCAPIError):
    def __init__(self, message: str, payload: dict[str, Any]) -> None:
        super().__init__(message)
        self.payload = copy.deepcopy(payload)


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


def config_mutation_lock_path() -> str:
    return os.path.join(locks_dir(), "config.lock")


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


def gateway_status_path(app_name: str = "") -> str:
    normalized_app_name = validate_app_name(app_name)
    if normalized_app_name:
        return os.path.join(data_dir(), f"gateway-status-{normalized_app_name}.json")
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
        "apps": {},
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
    apps = raw.get("apps")
    if isinstance(apps, dict):
        normalized_apps: dict[str, Any] = {}
        for raw_name, raw_app in apps.items():
            if not isinstance(raw_app, dict):
                continue
            try:
                app_name = validate_app_name(raw_name, allow_default=False)
            except ValueError:
                continue
            app_policy = raw_app.get("policy") if isinstance(raw_app.get("policy"), dict) else {}
            normalized_apps[app_name] = {
                "application_id": str(raw_app.get("application_id", "")).strip(),
                "public_key": str(raw_app.get("public_key", "")).strip(),
                "command_name": str(raw_app.get("command_name", COMMAND_NAME_DEFAULT)).strip()
                or COMMAND_NAME_DEFAULT,
                "policy": {
                    "guild_allowlist": _normalize_allowlist(app_policy.get("guild_allowlist")),
                    "channel_allowlist": _normalize_allowlist(app_policy.get("channel_allowlist")),
                    "role_allowlist": _normalize_allowlist(app_policy.get("role_allowlist")),
                },
            }
        config["apps"] = normalized_apps
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
                try:
                    app_name = validate_app_name(value.get("app", ""))
                except ValueError:
                    continue
                binding_id = chat_binding_id(kind, conversation_id, app_name)
                normalized_bindings[binding_id] = {
                    "id": binding_id,
                    "kind": kind,
                    "conversation_id": conversation_id,
                    "guild_id": str(value.get("guild_id", "")).strip(),
                    "session_names": normalized_session_names,
                }
                if app_name:
                    normalized_bindings[binding_id]["app"] = app_name
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
                if isinstance(value.get("policy"), dict):
                    normalized_launchers[launcher_id]["policy"] = normalize_room_launch_peer_policy(value.get("policy"))
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
        "allow_untargeted_ambient_delivery": False,
        "peer_fanout_enabled": False,
        "allow_untargeted_peer_fanout": False,
        "max_peer_triggered_publishes_per_root": 1,
        "max_total_peer_deliveries_per_root": 8,
        "max_peer_triggered_publishes_per_session_per_minute": 5,
    }


def default_room_launch_peer_policy() -> dict[str, Any]:
    defaults = default_room_peer_policy()
    defaults["peer_fanout_enabled"] = True
    defaults["allow_untargeted_peer_fanout"] = True
    return defaults


def _normalize_peer_policy(policy: dict[str, Any] | None, defaults: dict[str, Any]) -> dict[str, Any]:
    raw = policy if isinstance(policy, dict) else {}
    return {
        "ambient_read_enabled": _coerce_bool(raw.get("ambient_read_enabled"), defaults["ambient_read_enabled"]),
        "allow_untargeted_ambient_delivery": _coerce_bool(
            raw.get("allow_untargeted_ambient_delivery"), defaults["allow_untargeted_ambient_delivery"]
        ),
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


def normalize_room_peer_policy(policy: dict[str, Any] | None = None) -> dict[str, Any]:
    return _normalize_peer_policy(policy, default_room_peer_policy())


def normalize_room_launch_peer_policy(policy: dict[str, Any] | None = None) -> dict[str, Any]:
    return _normalize_peer_policy(policy, default_room_launch_peer_policy())


def binding_peer_policy(binding: dict[str, Any]) -> dict[str, Any]:
    if str(binding.get("kind", "")).strip() != "room":
        return default_room_peer_policy()
    if str(binding.get("publish_route_kind", "")).strip() == "room_launch" or str(binding.get("id", "")).strip().startswith(
        "launch-room:"
    ):
        if not isinstance(binding.get("policy"), dict):
            return default_room_peer_policy()
        return normalize_room_launch_peer_policy(binding.get("policy"))
    return normalize_room_peer_policy(binding.get("policy"))


def redact_config(config: dict[str, Any]) -> dict[str, Any]:
    redacted = normalize_config(config)
    redacted["app"]["bot_token_present"] = bool(load_bot_token())
    for app_name, app in redacted.get("apps", {}).items():
        app["bot_token_present"] = bool(load_bot_token(app_name))
    return redacted


def validate_app_name(value: Any, *, allow_default: bool = True) -> str:
    normalized = str(value or "").strip()
    if not normalized and allow_default:
        return ""
    if normalized == "default":
        raise ValueError("app name 'default' is reserved for the legacy default app")
    if not DISCORD_APP_NAME.fullmatch(normalized):
        raise ValueError("app name must match [a-z][a-z0-9_-]{0,31}")
    return normalized


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


def import_app_config(
    config: dict[str, Any],
    app_fields: dict[str, Any],
    *,
    app_name: str = "",
    bot_token: str | None = None,
) -> dict[str, Any]:
    normalized_app_name = validate_app_name(app_name)
    normalized_bot_token: str | None = None
    if bot_token is not None:
        normalized_bot_token = str(bot_token).strip()
        if not normalized_bot_token:
            raise ValueError("bot token is empty")
    with advisory_lock(config_mutation_lock_path()):
        current = load_config() if os.path.exists(config_path()) else normalize_config(config)
        previous_bot_token = load_bot_token(normalized_app_name) if normalized_bot_token is not None else ""
        existing_app = current.get("apps", {}).get(normalized_app_name) if normalized_app_name else None
        requested_application_id = validate_application_id(
            app_fields.get("application_id", app_fields.get("app_id", ""))
        )
        existing_application_id = (
            str(existing_app.get("application_id", "")).strip()
            if isinstance(existing_app, dict)
            else ""
        )
        existing_named_bot_token = load_bot_token(normalized_app_name) if normalized_app_name else ""
        if existing_named_bot_token and (not isinstance(existing_app, dict) or not existing_application_id):
            raise ValueError(
                f"orphan bot token for Discord app {normalized_app_name!r} has no pinned application_id; "
                "remove it explicitly or import the replacement under a new app name"
            )
        if (
            normalized_app_name
            and existing_application_id
            and requested_application_id
            and requested_application_id != existing_application_id
        ):
            raise ValueError(
                f"Discord app {normalized_app_name!r} cannot change application_id; "
                "import the replacement under a new app name"
            )
        updated = _import_app_config_locked(current, app_fields, app_name=normalized_app_name)
        if normalized_bot_token is None:
            return updated
        try:
            save_bot_token(normalized_bot_token, app_name=normalized_app_name)
        except OSError:
            save_config(current)
            if previous_bot_token:
                save_bot_token(previous_bot_token, app_name=normalized_app_name)
            else:
                try:
                    os.remove(secret_path(bot_token_secret_name(normalized_app_name)))
                except FileNotFoundError:
                    pass
            raise
        return updated


def _import_app_config_locked(
    config: dict[str, Any],
    app_fields: dict[str, Any],
    *,
    app_name: str = "",
) -> dict[str, Any]:
    cfg = normalize_config(config)
    normalized_app_name = validate_app_name(app_name)
    if normalized_app_name:
        app = cfg.setdefault("apps", {}).setdefault(normalized_app_name, {})
        policy = app.setdefault("policy", {})
    else:
        app = cfg.setdefault("app", {})
        policy = cfg.setdefault("policy", {})
    application_id = validate_application_id(app_fields.get("application_id", app_fields.get("app_id", "")))
    public_key = validate_public_key(app_fields.get("public_key", ""))
    if application_id:
        for existing_app_name in list_app_names(cfg):
            if existing_app_name == normalized_app_name:
                continue
            existing_application_id = str(resolve_app_config(cfg, existing_app_name).get("application_id", "")).strip()
            if existing_application_id == application_id:
                display_name = existing_app_name or "default"
                raise ValueError(f"application_id is already configured for app {display_name!r}")
        app["application_id"] = application_id
    if public_key:
        app["public_key"] = public_key
    command_name = str(app_fields.get("command_name", app.get("command_name", COMMAND_NAME_DEFAULT))).strip()
    app["command_name"] = command_name or COMMAND_NAME_DEFAULT

    for key in ("guild_allowlist", "channel_allowlist", "role_allowlist"):
        if key in app_fields:
            policy[key] = _normalize_allowlist(app_fields.get(key))
    return save_config(cfg)


def resolve_app_config(config: dict[str, Any], app_name: str = "") -> dict[str, Any]:
    cfg = normalize_config(config)
    normalized_app_name = validate_app_name(app_name)
    if not normalized_app_name:
        return cfg["app"]
    app = cfg.get("apps", {}).get(normalized_app_name)
    if not isinstance(app, dict):
        raise ValueError(f"unknown Discord app {normalized_app_name!r}")
    return app


def resolve_app_policy(config: dict[str, Any], app_name: str = "") -> dict[str, Any]:
    cfg = normalize_config(config)
    normalized_app_name = validate_app_name(app_name)
    if not normalized_app_name:
        return cfg["policy"]
    app = cfg.get("apps", {}).get(normalized_app_name)
    if not isinstance(app, dict):
        raise ValueError(f"unknown Discord app {normalized_app_name!r}")
    return app["policy"]


def list_app_names(config: dict[str, Any]) -> list[str]:
    cfg = normalize_config(config)
    names = sorted(cfg.get("apps", {}))
    return [""] + names


def bot_token_secret_name(app_name: str = "") -> str:
    normalized_app_name = validate_app_name(app_name)
    if not normalized_app_name:
        return "bot-token.txt"
    return f"bot-token-{normalized_app_name}.txt"


def save_bot_token(token: str, app_name: str = "") -> None:
    ensure_layout()
    atomic_write_text(secret_path(bot_token_secret_name(app_name)), token.strip() + "\n", mode=0o600)


def load_bot_token(app_name: str = "") -> str:
    return read_text(secret_path(bot_token_secret_name(app_name))).strip()


def normalize_channel_key(guild_id: str, channel_id: str) -> str:
    return f"{str(guild_id).strip()}/{str(channel_id).strip()}"


def chat_binding_id(kind: str, conversation_id: str, app_name: str = "") -> str:
    binding_id = f"{str(kind).strip().lower()}:{str(conversation_id).strip()}"
    normalized_app_name = validate_app_name(app_name)
    if normalized_app_name:
        return f"{binding_id}@app:{normalized_app_name}"
    return binding_id


def set_chat_binding(
    config: dict[str, Any],
    kind: str,
    conversation_id: str,
    session_names: list[str],
    guild_id: str = "",
    *,
    app_name: str = "",
    policy: dict[str, Any] | None = None,
    channel_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with advisory_lock(config_mutation_lock_path()):
        current = load_config() if os.path.exists(config_path()) else normalize_config(config)
        return _set_chat_binding_locked(
            current,
            kind,
            conversation_id,
            session_names,
            guild_id,
            app_name=app_name,
            policy=policy,
            channel_metadata=channel_metadata,
        )


def _set_chat_binding_locked(
    config: dict[str, Any],
    kind: str,
    conversation_id: str,
    session_names: list[str],
    guild_id: str = "",
    *,
    app_name: str = "",
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

    normalized_app_name = validate_app_name(app_name)
    cfg = normalize_config(config)
    binding_id = chat_binding_id(normalized_kind, normalized_conversation, normalized_app_name)
    if not normalized_app_name and normalized_kind == "room" and resolve_room_launcher(cfg, normalized_conversation):
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
    if normalized_kind == "room" and room_policy.get("allow_untargeted_ambient_delivery"):
        if not room_policy.get("ambient_read_enabled"):
            raise ValueError("untargeted ambient delivery requires ambient read to be enabled")
        if len(normalized_session_names) != 1:
            raise ValueError("untargeted ambient delivery requires exactly one session name")
    cfg.setdefault("chat", {})
    cfg["chat"].setdefault("bindings", {})
    binding: dict[str, Any] = {
        "id": binding_id,
        "kind": normalized_kind,
        "conversation_id": normalized_conversation,
        "guild_id": str(guild_id).strip(),
        "session_names": normalized_session_names,
    }
    if normalized_app_name:
        binding["app"] = normalized_app_name
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
    policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with advisory_lock(config_mutation_lock_path()):
        current = load_config() if os.path.exists(config_path()) else normalize_config(config)
        return _set_room_launcher_locked(
            current,
            guild_id,
            conversation_id,
            response_mode=response_mode,
            default_qualified_handle=default_qualified_handle,
            policy=policy,
        )


def _set_room_launcher_locked(
    config: dict[str, Any],
    guild_id: str,
    conversation_id: str,
    *,
    response_mode: str = "mention_only",
    default_qualified_handle: str = "",
    policy: dict[str, Any] | None = None,
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
    existing_launcher = resolve_room_launcher(cfg, normalized_conversation) or {}
    existing_has_policy = isinstance(existing_launcher.get("policy"), dict)
    has_policy = existing_has_policy or not existing_launcher
    raw_policy = copy.deepcopy(existing_launcher.get("policy")) if existing_has_policy else {}
    if not existing_launcher:
        raw_policy = default_room_launch_peer_policy()
    if isinstance(policy, dict) and policy:
        has_policy = True
        raw_policy.update(copy.deepcopy(policy))
    launcher = {
        "id": launcher_id,
        "kind": "room",
        "guild_id": normalized_guild_id,
        "conversation_id": normalized_conversation,
        "response_mode": normalized_response_mode,
        "default_qualified_handle": normalized_default_handle,
    }
    if has_policy:
        launcher["policy"] = normalize_room_launch_peer_policy(raw_policy)
    cfg["chat"]["launchers"][launcher_id] = launcher
    return save_config(cfg)


def resolve_room_launcher(config: dict[str, Any], conversation_id: str) -> dict[str, Any] | None:
    launchers = normalize_config(config).get("chat", {}).get("launchers", {})
    return launchers.get(room_launch_surface_id(conversation_id))


def list_room_launchers(config: dict[str, Any]) -> list[dict[str, Any]]:
    launchers = normalize_config(config).get("chat", {}).get("launchers", {})
    return sorted(launchers.values(), key=lambda item: (str(item.get("kind", "")), str(item.get("conversation_id", ""))))


def describe_room_channel_scope(conversation_id: str, *, bot_token: str | None = None) -> dict[str, Any]:
    token = load_bot_token() if bot_token is None else str(bot_token).strip()
    if not token:
        return {}
    info = discord_api_request("GET", f"/channels/{urllib.parse.quote(str(conversation_id).strip())}", bot_token=token)
    if not isinstance(info, dict):
        return {}
    scope = normalize_binding_channel_metadata(info)
    guild_id = str(info.get("guild_id", "")).strip()
    if guild_id:
        scope["guild_id"] = guild_id
    return scope


def describe_room_channel_metadata(conversation_id: str, *, bot_token: str | None = None) -> dict[str, Any]:
    return normalize_binding_channel_metadata(
        describe_room_channel_scope(conversation_id, bot_token=bot_token)
    )


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
    return sorted(
        bindings.values(),
        key=lambda item: (
            str(item.get("kind", "")),
            str(item.get("conversation_id", "")),
            str(item.get("app", "")),
        ),
    )


def resolve_publish_route(
    config: dict[str, Any],
    route_id: str,
    *,
    app_name: str = "",
) -> dict[str, Any] | None:
    route = str(route_id).strip()
    normalized_app_name = validate_app_name(app_name)
    if normalized_app_name:
        resolve_app_config(config, normalized_app_name)
        if "@app:" in route:
            expected_suffix = f"@app:{normalized_app_name}"
            if not route.endswith(expected_suffix):
                raise ValueError("--app does not match the binding app")
        elif route.startswith(("dm:", "room:")):
            route = f"{route}@app:{normalized_app_name}"
        else:
            raise ValueError("named apps support only room and DM bindings")
    binding = resolve_chat_binding(config, route)
    if binding:
        binding_app_name = validate_app_name(binding.get("app", ""))
        if binding_app_name:
            resolve_app_config(config, binding_app_name)
        return binding
    if not normalized_app_name and route.startswith(("dm:", "room:")) and "@app:" not in route:
        candidates = [
            item
            for item in list_chat_bindings(config)
            if str(item.get("id", "")).partition("@app:")[0] == route
        ]
        if len(candidates) == 1:
            candidate_app_name = validate_app_name(candidates[0].get("app", ""))
            if candidate_app_name:
                resolve_app_config(config, candidate_app_name)
            return candidates[0]
        if len(candidates) > 1:
            raise ValueError(f"binding {route!r} is ambiguous; select one with --app")
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
    with advisory_lock(config_mutation_lock_path()):
        current = load_config() if os.path.exists(config_path()) else normalize_config(config)
        return _set_channel_mapping_locked(current, guild_id, channel_id, target, fix_formula)


def _set_channel_mapping_locked(
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
    with advisory_lock(config_mutation_lock_path()):
        current = load_config() if os.path.exists(config_path()) else normalize_config(config)
        return _set_rig_mapping_locked(current, guild_id, rig_name, target, fix_formula)


def _set_rig_mapping_locked(
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


def save_gateway_status(payload: dict[str, Any], app_name: str = "") -> dict[str, Any]:
    ensure_layout()
    body = copy.deepcopy(payload)
    body["updated_at"] = utcnow()
    atomic_write_json(gateway_status_path(app_name), body)
    return body


def load_gateway_status(app_name: str = "") -> dict[str, Any]:
    ensure_layout()
    payload = read_json(gateway_status_path(app_name), {}, allow_invalid=True)
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
        return normalize_room_launch_record(data)
    return None


def normalize_room_launch_record(payload: dict[str, Any]) -> dict[str, Any]:
    body = copy.deepcopy(payload)
    participants: dict[str, dict[str, Any]] = {}
    raw_participants = body.get("participants")
    if isinstance(raw_participants, dict):
        for key, raw in raw_participants.items():
            item = copy.deepcopy(raw) if isinstance(raw, dict) else {}
            qualified_handle = str(item.get("qualified_handle", "")).strip() or str(key).strip()
            if not qualified_handle:
                continue
            participants[qualified_handle] = {
                **item,
                "qualified_handle": qualified_handle,
                "session_alias": str(item.get("session_alias", "")).strip(),
                "session_id": str(item.get("session_id", "")).strip(),
                "session_name": str(item.get("session_name", "")).strip(),
            }
    primary_handle = str(body.get("qualified_handle", "")).strip()
    if primary_handle:
        participant = copy.deepcopy(participants.get(primary_handle, {}))
        participant["qualified_handle"] = primary_handle
        for field in ("session_alias", "session_id", "session_name"):
            top_level = str(body.get(field, "")).strip()
            participant[field] = top_level or str(participant.get(field, "")).strip()
        participants[primary_handle] = participant
    body["participants"] = participants
    message_targets: dict[str, str] = {}
    raw_message_targets = body.get("message_targets")
    if isinstance(raw_message_targets, dict):
        for message_id, qualified_handle in raw_message_targets.items():
            normalized_message_id = str(message_id).strip()
            normalized_handle = str(qualified_handle).strip()
            if normalized_message_id and normalized_handle:
                message_targets[normalized_message_id] = normalized_handle
    root_message_id = str(body.get("root_message_id", "")).strip()
    if primary_handle and root_message_id and root_message_id not in message_targets:
        message_targets[root_message_id] = primary_handle
    if message_targets:
        body["message_targets"] = message_targets
    else:
        body.pop("message_targets", None)
    target_order = [str(item).strip() for item in body.get("message_target_order", []) if str(item).strip()]
    if root_message_id and root_message_id in message_targets and root_message_id not in target_order:
        target_order.insert(0, root_message_id)
    if target_order:
        body["message_target_order"] = target_order
    else:
        body.pop("message_target_order", None)
    if primary_handle and primary_handle in participants:
        participant = participants[primary_handle]
        body["session_alias"] = str(participant.get("session_alias", "")).strip()
        body["session_id"] = str(participant.get("session_id", "")).strip()
        body["session_name"] = str(participant.get("session_name", "")).strip()
    cursor = str(body.get("last_addressed_qualified_handle", "")).strip()
    if cursor:
        body["last_addressed_qualified_handle"] = cursor
    elif primary_handle:
        body["last_addressed_qualified_handle"] = primary_handle
    return body


def room_launch_participants(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    body = normalize_room_launch_record(payload)
    raw = body.get("participants")
    if isinstance(raw, dict):
        return copy.deepcopy(raw)
    return {}


def room_launch_participant(payload: dict[str, Any], qualified_handle: str) -> dict[str, Any]:
    return copy.deepcopy(room_launch_participants(payload).get(str(qualified_handle).strip(), {}))


def room_launch_participant_summaries(payload: dict[str, Any]) -> list[dict[str, str]]:
    summaries: list[dict[str, str]] = []
    for qualified_handle, participant in room_launch_participants(payload).items():
        summaries.append(
            {
                "qualified_handle": qualified_handle,
                "session_alias": str(participant.get("session_alias", "")).strip(),
                "session_name": str(participant.get("session_name", "")).strip(),
            }
        )
    summaries.sort(key=lambda item: item["qualified_handle"])
    return summaries


def room_launch_participant_handle_lookup(payload: dict[str, Any]) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for qualified_handle, participant in room_launch_participants(payload).items():
        selector = room_launch_participant_delivery_selector(participant)
        if qualified_handle and selector:
            lookup[qualified_handle] = selector
    return lookup


def room_launch_participant_session_lookup(payload: dict[str, Any]) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for participant in room_launch_participants(payload).values():
        selector = room_launch_participant_delivery_selector(participant)
        session_name = str(participant.get("session_name", "")).strip()
        if session_name and selector:
            lookup[session_name] = selector
    return lookup


def room_launch_participant_delivery_targets(payload: dict[str, Any]) -> list[str]:
    targets: list[str] = []
    seen: set[str] = set()
    for participant in room_launch_participants(payload).values():
        selector = room_launch_participant_delivery_selector(participant)
        if not selector or selector in seen:
            continue
        seen.add(selector)
        targets.append(selector)
    return targets


def room_launch_participant_handle_for_session(payload: dict[str, Any], *, session_name: str = "", session_id: str = "") -> str:
    for qualified_handle, participant in room_launch_participants(payload).items():
        if _room_launch_participant_matches_session(
            participant,
            session_name=session_name,
            session_id=session_id,
        ):
            return qualified_handle
    return ""


def resolve_room_launch_target_selector(launch: dict[str, Any], target_selector: str) -> tuple[str, dict[str, Any], dict[str, str]]:
    normalized_target = str(target_selector).strip()
    if not normalized_target:
        return "", {}, {}
    participant_handle = room_launch_participant_handle_for_session(
        launch,
        session_name=normalized_target,
        session_id=normalized_target,
    )
    participant = room_launch_participant(launch, participant_handle)
    try:
        sessions = list_city_sessions(state="all")
    except GCAPIError:
        sessions = []
    resolved_selector, identity = resolve_routable_session_candidate_from_sessions(
        sessions,
        participant.get("session_name", ""),
        participant.get("delivery_selector", ""),
        participant.get("session_alias", ""),
        participant.get("session_id", ""),
        normalized_target,
    )
    if resolved_selector:
        return resolved_selector, participant, identity
    local_selector = (
        str(participant.get("session_alias", "")).strip()
        or str(participant.get("delivery_selector", "")).strip()
        or str(participant.get("session_name", "")).strip()
        or str(participant.get("session_id", "")).strip()
        or normalized_target
    )
    return local_selector, participant, {}


def room_launch_message_target_handle(payload: dict[str, Any], remote_message_id: str) -> str:
    body = normalize_room_launch_record(payload)
    message_id = str(remote_message_id).strip()
    if not message_id:
        return ""
    handle = str(body.get("message_targets", {}).get(message_id, "")).strip()
    if not handle:
        return ""
    if handle not in room_launch_participants(body):
        return ""
    return handle


def save_room_launch(payload: dict[str, Any]) -> dict[str, Any]:
    ensure_layout()
    body = normalize_room_launch_record(payload)
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


def set_room_launch_last_addressed(
    launch_id: str,
    qualified_handle: str,
    *,
    delivery_order: str = "",
) -> dict[str, Any] | None:
    normalized_launch_id = str(launch_id).strip()
    normalized_handle = str(qualified_handle).strip()
    normalized_delivery_order = str(delivery_order).strip()
    if not normalized_launch_id or not normalized_handle:
        return None
    with advisory_lock(room_launch_lock_path(normalized_launch_id)):
        current = load_room_launch(normalized_launch_id)
        if not isinstance(current, dict):
            return None
        if normalized_handle not in room_launch_participants(current):
            return current
        current_delivery_order = str(current.get("last_addressed_delivery_order", "")).strip()
        if current_delivery_order and (
            not normalized_delivery_order or normalized_delivery_order <= current_delivery_order
        ):
            return current
        body = copy.deepcopy(current)
        body["last_addressed_qualified_handle"] = normalized_handle
        if normalized_delivery_order:
            body["last_addressed_delivery_order"] = normalized_delivery_order
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


def list_chat_ingress() -> list[dict[str, Any]]:
    ensure_layout()
    entries: list[dict[str, Any]] = []
    for path in pathlib.Path(chat_ingress_dir()).glob("*.json"):
        data = read_json(str(path), allow_invalid=True)
        if isinstance(data, dict):
            entries.append(data)
    entries.sort(key=lambda item: item.get("created_at", ""))
    return entries


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
    gateway_statuses = {
        app_name or "default": redact_gateway_status(load_gateway_status(app_name))
        for app_name in list_app_names(config)
    }
    return {
        "service_name": current_service_name(),
        "admin_url": admin_url(),
        "interactions_url": interactions_url(),
        "config": redact_config(config),
        "gateway_status": redact_gateway_status(load_gateway_status()),
        "gateway_statuses": gateway_statuses,
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
    token = load_bot_token() if bot_token is None else bot_token
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


_supervisor_scope_cache: dict[str, tuple[float, str]] = {}  # workspace_name → (timestamp, scope)
_SCOPE_CACHE_TTL = 60.0  # seconds


def resolved_workspace_name(city_cfg: dict[str, Any]) -> str:
    """Resolve the city's runtime workspace name.

    Precedence mirrors gc itself (applyWorkspaceIdentityBinding): the
    machine-local site binding (.gc/site.toml workspace_name) wins, then the
    declared city.toml workspace.name, then the city directory basename. gc
    registers the city with the supervisor under this resolved name, so scope
    discovery must resolve it identically or a divergent site binding sends
    delivery to the dead default API port.
    """
    root = city_root()
    if root:
        site_path = os.path.join(root, ".gc", "site.toml")
        try:
            with open(site_path, "rb") as handle:
                site_cfg = tomllib.load(handle)
        except (OSError, tomllib.TOMLDecodeError):
            site_cfg = {}
        if isinstance(site_cfg, dict):
            name = str(site_cfg.get("workspace_name", "")).strip()
            if name:
                return name
    workspace_cfg = city_cfg.get("workspace") or {}
    if isinstance(workspace_cfg, dict):
        name = str(workspace_cfg.get("name", "")).strip()
        if name:
            return name
    if not root:
        return ""
    return pathlib.Path(root).name


def discover_supervisor_gc_api_scope(city_cfg: dict[str, Any]) -> str:
    """Discover the supervisor API scope prefix for the city.

    Caches the result for 60 seconds to avoid hitting the supervisor
    on every API call.
    """
    workspace_name = resolved_workspace_name(city_cfg)
    cache_key = workspace_name or "__default__"
    now = time.monotonic()
    if cache_key in _supervisor_scope_cache:
        cached_at, cached_scope = _supervisor_scope_cache[cache_key]
        if now - cached_at < _SCOPE_CACHE_TTL:
            return cached_scope
    scope = _discover_supervisor_gc_api_scope_uncached(city_cfg)
    if scope:  # Only cache successful discovery; retry on next call if empty.
        _supervisor_scope_cache[cache_key] = (now, scope)
    return scope


def _discover_supervisor_gc_api_scope_uncached(city_cfg: dict[str, Any]) -> str:
    workspace_name = resolved_workspace_name(city_cfg)
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


def gc_api_url(path: str) -> str:
    base_url = gc_api_base_url()
    if path.startswith("http://") or path.startswith("https://"):
        return path
    # Skip scope discovery if the caller provided an explicit base URL
    # (it already includes the scope prefix). Strip /v0/ from path
    # since the base URL already contains the scoped root.
    override = str(os.environ.get("GC_API_BASE_URL", "")).strip()
    if override:
        normalized_path = "/" + path[len("/v0/") :] if path.startswith("/v0/") else path
    else:
        city_cfg = load_city_toml()
        scope_prefix = discover_supervisor_gc_api_scope(city_cfg)
        normalized_path = path
        if scope_prefix and path.startswith("/v0/"):
            normalized_path = scope_prefix + "/" + path[len("/v0/") :]
    return urllib.parse.urljoin(base_url.rstrip("/") + "/", normalized_path.lstrip("/"))


def gc_api_request_with_status(
    method: str,
    path: str,
    payload: Any = None,
    headers: dict[str, str] | None = None,
    timeout: float = GC_API_REQUEST_TIMEOUT_SECONDS,
) -> tuple[int, Any]:
    url = gc_api_url(path)
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
            status_code = getattr(response, "status", None)
            if not isinstance(status_code, int):
                getcode = getattr(response, "getcode", None)
                candidate = getcode() if callable(getcode) else None
                status_code = candidate if isinstance(candidate, int) else 200
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        message = raw.decode("utf-8", errors="replace")
        raise GCAPIError(f"{method.upper()} {url} failed with {exc.code}: {message}") from exc
    except urllib.error.URLError as exc:
        raise GCAPITransportError(f"{method.upper()} {url} failed: {exc}") from exc
    except TimeoutError as exc:
        raise GCAPITransportError(f"{method.upper()} {url} timed out") from exc
    except OSError as exc:
        raise GCAPITransportError(f"{method.upper()} {url} failed: {exc}") from exc
    if not raw:
        return status_code, {}
    try:
        return status_code, json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise GCAPIError(f"{method.upper()} {url} returned invalid JSON") from exc


def gc_api_request(
    method: str,
    path: str,
    payload: Any = None,
    headers: dict[str, str] | None = None,
    timeout: float = GC_API_REQUEST_TIMEOUT_SECONDS,
) -> Any:
    _, response_payload = gc_api_request_with_status(
        method,
        path,
        payload=payload,
        headers=headers,
        timeout=timeout,
    )
    return response_payload


def wait_for_gc_request_result(
    request_id: str,
    *,
    event_cursor: str,
    success_type: str,
    failure_operation: str,
    timeout: float = GC_API_ASYNC_RESULT_TIMEOUT_SECONDS,
    cancel_event: threading.Event | None = None,
) -> dict[str, Any]:
    normalized_request_id = str(request_id).strip()
    if not normalized_request_id:
        raise GCAPIError("gc async request did not include request_id")
    cursor = str(event_cursor).strip() or "0"
    query = urllib.parse.urlencode({"after_seq": cursor})
    url = gc_api_url(f"/v0/events/stream?{query}")
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "text/event-stream",
            "User-Agent": "gas-city-discord/0.1",
            "X-GC-Request": "true",
        },
        method="GET",
    )
    data_lines: list[str] = []
    deadline = time.monotonic() + max(float(timeout), 0.0)
    if cancel_event is not None and cancel_event.is_set():
        raise GCAPIRequestCancelled(f"waiting for gc request {normalized_request_id} was cancelled")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            stream_done = threading.Event()
            cancel_watcher: threading.Thread | None = None
            if cancel_event is not None:
                def close_stream_on_cancel() -> None:
                    while not stream_done.wait(0.05):
                        if cancel_event.is_set():
                            close = getattr(response, "close", None)
                            if callable(close):
                                try:
                                    close()
                                except (OSError, ValueError):
                                    pass
                            return

                cancel_watcher = threading.Thread(
                    target=close_stream_on_cancel,
                    name=f"gc-request-cancel-{normalized_request_id}",
                    daemon=True,
                )
                cancel_watcher.start()
            try:
                for raw_line in response:
                    if cancel_event is not None and cancel_event.is_set():
                        raise GCAPIRequestCancelled(f"waiting for gc request {normalized_request_id} was cancelled")
                    if time.monotonic() >= deadline:
                        raise GCAPIResultUnknown(f"GET {url} timed out before request {normalized_request_id} completed")
                    if isinstance(raw_line, bytes):
                        try:
                            line = raw_line.decode("utf-8")
                        except UnicodeDecodeError as exc:
                            raise GCAPIResultUnknown("gc event stream returned invalid UTF-8 before request completed") from exc
                    else:
                        line = str(raw_line)
                    line = line.rstrip("\r\n")
                    if line.startswith("data:"):
                        data_lines.append(line.removeprefix("data:").lstrip())
                        continue
                    if line or not data_lines:
                        continue
                    try:
                        envelope = json.loads("\n".join(data_lines))
                    except json.JSONDecodeError as exc:
                        raise GCAPIResultUnknown("gc event stream returned invalid JSON before request completed") from exc
                    finally:
                        data_lines = []
                    if not isinstance(envelope, dict):
                        continue
                    event_type = str(envelope.get("type", "")).strip()
                    payload = envelope.get("payload")
                    if not isinstance(payload, dict):
                        continue
                    if str(payload.get("request_id", "")).strip() != normalized_request_id:
                        continue
                    if event_type == success_type:
                        return payload
                    if event_type != GC_EVENT_REQUEST_FAILED:
                        continue
                    if str(payload.get("operation", "")).strip() != failure_operation:
                        continue
                    error_code = str(payload.get("error_code", "")).strip() or "request_failed"
                    error_message = str(payload.get("error_message", "")).strip() or "asynchronous request failed"
                    raise GCAPIRequestFailed(
                        f"{failure_operation} failed: {error_code}: {error_message}",
                        payload,
                    )
            finally:
                stream_done.set()
                if cancel_watcher is not None:
                    cancel_watcher.join(timeout=0.2)
    except urllib.error.HTTPError as exc:
        if cancel_event is not None and cancel_event.is_set():
            raise GCAPIRequestCancelled(f"waiting for gc request {normalized_request_id} was cancelled") from exc
        raw = exc.read()
        message = raw.decode("utf-8", errors="replace")
        raise GCAPIResultUnknown(f"GET {url} failed with {exc.code} before request completed: {message}") from exc
    except urllib.error.URLError as exc:
        if cancel_event is not None and cancel_event.is_set():
            raise GCAPIRequestCancelled(f"waiting for gc request {normalized_request_id} was cancelled") from exc
        raise GCAPIResultUnknown(f"GET {url} failed before request completed: {exc}") from exc
    except TimeoutError as exc:
        if cancel_event is not None and cancel_event.is_set():
            raise GCAPIRequestCancelled(f"waiting for gc request {normalized_request_id} was cancelled") from exc
        raise GCAPIResultUnknown(f"GET {url} timed out before request completed") from exc
    except OSError as exc:
        if cancel_event is not None and cancel_event.is_set():
            raise GCAPIRequestCancelled(f"waiting for gc request {normalized_request_id} was cancelled") from exc
        raise GCAPIResultUnknown(f"GET {url} failed before request completed: {exc}") from exc
    except ValueError as exc:
        if cancel_event is not None and cancel_event.is_set():
            raise GCAPIRequestCancelled(f"waiting for gc request {normalized_request_id} was cancelled") from exc
        raise GCAPIResultUnknown(f"GET {url} closed before request completed: {exc}") from exc
    if cancel_event is not None and cancel_event.is_set():
        raise GCAPIRequestCancelled(f"waiting for gc request {normalized_request_id} was cancelled")
    raise GCAPIResultUnknown(f"gc event stream closed before request {normalized_request_id} completed")


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


def _chat_ingress_target_matches_selector(target: dict[str, Any], selector: str) -> bool:
    wanted = str(selector).strip()
    if not wanted:
        return False
    candidates = {
        str(target.get("session_name", "")).strip(),
        str(target.get("session_id", "")).strip(),
        str(target.get("session_alias", "")).strip(),
        str(target.get("id", "")).strip(),
    }
    response = target.get("response")
    if isinstance(response, dict):
        candidates.update(
            {
                str(response.get("id", "")).strip(),
                str(response.get("session_name", "")).strip(),
                str(response.get("session_id", "")).strip(),
                str(response.get("session_alias", "")).strip(),
            }
        )
    terminal_evidence = target.get("terminal_evidence")
    if isinstance(terminal_evidence, dict):
        candidates.update(
            {
                str(terminal_evidence.get("session_name", "")).strip(),
                str(terminal_evidence.get("session_id", "")).strip(),
                str(terminal_evidence.get("session_alias", "")).strip(),
            }
        )
        terminal_payload = terminal_evidence.get("payload")
        if isinstance(terminal_payload, dict):
            candidates.update(
                {
                    str(terminal_payload.get("session_name", "")).strip(),
                    str(terminal_payload.get("session_id", "")).strip(),
                    str(terminal_payload.get("session_alias", "")).strip(),
                }
            )
    return wanted in {candidate for candidate in candidates if candidate}


def _chat_ingress_delivered_to_selector(record: dict[str, Any], selector: str) -> bool:
    targets = record.get("targets")
    if not isinstance(targets, list):
        return False
    for target in targets:
        if not isinstance(target, dict):
            continue
        if str(target.get("status", "")).strip() != "delivered":
            continue
        if _chat_ingress_target_matches_selector(target, selector):
            return True
    return False


def _chat_ingress_reply_context(record: dict[str, Any]) -> dict[str, str]:
    ingress_id = str(record.get("ingress_id", "")).strip()
    binding_id = str(record.get("binding_id", "")).strip()
    conversation_id = str(record.get("conversation_id", "")).strip()
    message_id = str(record.get("discord_message_id", "")).strip()
    if not (ingress_id and binding_id and conversation_id and message_id):
        return {}
    fields = {
        "kind": "discord_human_message",
        "binding_id": binding_id,
        "ingress_receipt_id": ingress_id,
        "discord_message_id": message_id,
        "publish_binding_id": binding_id,
        "publish_conversation_id": conversation_id,
        "publish_trigger_id": message_id,
        "publish_reply_to_discord_message_id": message_id,
    }
    for key in ("guild_id", "delivery", "route_kind", "launch_id", "qualified_handle", "routing_mode"):
        value = str(record.get(key, "")).strip()
        if value:
            fields[key] = value
    if fields.get("launch_id"):
        fields["publish_launch_id"] = fields["launch_id"]
    return fields


def find_latest_delivered_chat_ingress_reply_context(session_selector: str, limit: int = 40) -> dict[str, str]:
    selector = str(session_selector).strip()
    if not selector:
        return {}
    for record in list_recent_chat_ingress(limit=max(1, int(limit))):
        if str(record.get("status", "")).strip() not in {"delivered", "partial_failed"}:
            continue
        if not _chat_ingress_delivered_to_selector(record, selector):
            continue
        fields = _chat_ingress_reply_context(record)
        if fields.get("publish_binding_id"):
            return fields
    return {}


def find_latest_discord_reply_context(session_selector: str = "", tail: int = 40) -> dict[str, str]:
    selector = str(session_selector).strip() or current_session_selector()
    if not selector:
        raise GCAPIError("GC_SESSION_ID or GC_SESSION_NAME is required")
    try:
        transcript_entries = load_session_transcript_raw(selector, tail=tail)
    except GCAPIError as exc:
        fields = find_latest_delivered_chat_ingress_reply_context(selector, limit=tail)
        if fields:
            return fields
        raise exc
    for entry in reversed(transcript_entries):
        fields = _extract_discord_event_fields(_raw_user_message_text(entry))
        if fields.get("publish_binding_id"):
            return fields
    fields = find_latest_delivered_chat_ingress_reply_context(selector, limit=tail)
    if fields:
        return fields
    raise GCAPIError(f"no recent discord event with publish metadata found for {selector}")


def normalize_to_extmsg_message(
    discord_event: dict[str, Any],
    guild_id: str,
    application_id: str,
    participants: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    """Normalize a Discord MESSAGE_CREATE event to an extmsg ExternalInboundMessage."""
    author = discord_event.get("author") or {}
    channel_id = str(discord_event.get("channel_id", ""))
    parent_id = str(discord_event.get("thread_metadata", {}).get("parent_id", "")
                     if "thread_metadata" in discord_event
                     else discord_event.get("parent_id", ""))
    content = str(discord_event.get("content", ""))

    # Determine conversation kind.
    channel_type = discord_event.get("channel_type", discord_event.get("type", 0))
    if channel_type == 1:  # DM
        kind = "dm"
    elif parent_id:  # thread
        kind = "thread"
    else:
        kind = "room"

    # Extract explicit target via natural language matching against participants.
    explicit_target = ""
    if participants:
        explicit_target = _fuzzy_match_handle(content, participants)

    # Extract reply-to from message reference.
    ref = discord_event.get("message_reference") or {}
    reply_to = str(ref.get("message_id", ""))

    return {
        "provider_message_id": str(discord_event.get("id", "")),
        "conversation": {
            "scope_id": guild_id or "global",
            "provider": "discord",
            "account_id": application_id,
            "conversation_id": channel_id,
            "parent_conversation_id": parent_id,
            "kind": kind,
        },
        "actor": {
            "id": str(author.get("id", "")),
            "display_name": str(author.get("global_name", "") or author.get("username", "")),
            "is_bot": bool(author.get("bot", False)),
        },
        "text": content,
        "explicit_target": explicit_target,
        "reply_to_message_id": reply_to,
        "received_at": utcnow(),
    }


def _fuzzy_match_handle(
    text: str,
    participants: list[dict[str, str]],
) -> str:
    """Match participant handles in natural language text.

    Looks for participant names/handles mentioned naturally in the message
    (e.g., "hey worker, can you fix this?" matches handle "worker").
    Returns the first matching handle, or empty string if none found.
    """
    lower_text = text.lower()
    for p in participants:
        handle = str(p.get("handle", "")).strip().lower()
        if not handle:
            continue
        # Match the short name (after the rig/ prefix if present).
        short = handle.rsplit("/", 1)[-1] if "/" in handle else handle
        # Look for the handle as a word boundary match.
        for name in (short, handle):
            if not name:
                continue
            idx = lower_text.find(name)
            if idx < 0:
                continue
            # Check word boundaries.
            before_ok = idx == 0 or not lower_text[idx - 1].isalnum()
            after_idx = idx + len(name)
            after_ok = after_idx >= len(lower_text) or not lower_text[after_idx].isalnum()
            if before_ok and after_ok:
                return handle
    return ""


def deliver_to_extmsg(
    message: dict[str, Any],
    application_id: str,
) -> dict[str, Any]:
    """Post a normalized message to the extmsg inbound API."""
    return gc_api_request("POST", "/v0/extmsg/inbound", {"message": message})


def resolve_at_mentions(text: str) -> list[str]:
    """Extract @mentions from message text.

    Matches @name patterns (e.g., "@sky", "@worker", "@backend/sky").
    Returns list of raw mention strings (without the @ prefix).
    """
    import re
    return re.findall(r"@([a-zA-Z][a-zA-Z0-9_/-]*)", text)


def resolve_nl_agent_mentions(text: str) -> list[str]:
    """Find agent names mentioned naturally in text (without @ prefix).

    Matches known agent names as word boundaries in the message.
    Returns list of matched agent base names.
    """
    lower_text = text.lower()
    agents = list_city_agents()
    found: list[str] = []
    seen: set[str] = set()
    for a in agents:
        qname = str(a.get("name", "")).strip().lower()
        if not qname:
            continue
        base = qname.rsplit("/", 1)[-1] if "/" in qname else qname
        if base in seen or not base:
            continue
        # Word boundary match.
        idx = lower_text.find(base)
        if idx < 0:
            continue
        before_ok = idx == 0 or not lower_text[idx - 1].isalnum()
        after_idx = idx + len(base)
        after_ok = after_idx >= len(lower_text) or not lower_text[after_idx].isalnum()
        if before_ok and after_ok:
            found.append(base)
            seen.add(base)
    return found


def resolve_mention_targets(mentions: list[str]) -> list[dict[str, Any]]:
    """Resolve @mention names to session/template targets.

    Resolution order for each mention:
    1. Managed session with matching alias → {"kind": "session", "session": {...}}
    2. Agent template with matching name → {"kind": "template", "template": str, "agent": {...}}
    3. Unresolved → skipped

    Returns list of resolved targets.
    """
    if not mentions:
        return []
    sessions = list_city_sessions(state="all")
    agents = list_city_agents()

    # Build lookup indices.
    # Match sessions by alias or session name (both are stable identifiers).
    session_by_alias: dict[str, dict[str, Any]] = {}
    for s in sessions:
        alias = str(s.get("alias", "")).strip().lower()
        if alias:
            session_by_alias[alias] = s
        name = str(s.get("session_name", s.get("name", ""))).strip().lower()
        if name:
            session_by_alias[name] = s

    agent_by_name: dict[str, dict[str, Any]] = {}
    for a in agents:
        qname = str(a.get("name", "")).strip().lower()
        if qname:
            agent_by_name[qname] = a
            # Also index by bare name (after rig/).
            base = qname.rsplit("/", 1)[-1] if "/" in qname else qname
            if base not in agent_by_name:
                agent_by_name[base] = a
        template = str(a.get("template", "")).strip().lower()
        if template and template not in agent_by_name:
            agent_by_name[template] = a

    # Collect agent base names so we can detect when a session alias
    # is just the agent template name (those should create new sessions,
    # not reuse the singleton).
    agent_base_names: set[str] = set()
    for a in agents:
        qname = str(a.get("name", "")).strip().lower()
        if qname:
            base = qname.rsplit("/", 1)[-1] if "/" in qname else qname
            agent_base_names.add(base)
            agent_base_names.add(qname)

    targets = []
    seen: set[str] = set()
    for mention in mentions:
        key = mention.strip().lower()
        if key in seen:
            continue
        seen.add(key)
        # 1. Try agent template first — if the mention matches a template
        # name, always create a new session (don't reuse the singleton).
        if key in agent_by_name:
            agent = agent_by_name[key]
            targets.append({"kind": "template", "template": str(agent.get("name", "")), "agent": agent, "mention": mention})
            continue
        # 2. Try managed session by alias/name — only for mentions that
        # don't match any agent template (e.g., explicit session names
        # like "s-gc-33" or custom aliases like "my-worker").
        if key in session_by_alias:
            targets.append({"kind": "session", "session": session_by_alias[key], "mention": mention})
            continue
    return targets


def _create_session_from_template(template_name: str, alias: str, initial_message: str = "") -> str:
    """Create a session from an agent template via the sessions API.

    If initial_message is provided, creates synchronously so the session
    starts with the message in its context. Otherwise creates async.

    Returns the session name.
    """
    payload: dict[str, Any] = {
        "template": template_name,
        "name": template_name,
        "kind": "agent",
        "options": {
            "permission_mode": "unrestricted",
            "effort": "max",
        },
    }
    if initial_message:
        payload["message"] = initial_message
    else:
        payload["async"] = True
    session = gc_api_request("POST", "/v0/sessions", payload, timeout=60.0)
    session_name = str(session.get("session_name", "") or session.get("name", "")).strip()
    if not session_name:
        raise RuntimeError(f"session creation returned no session_name for {template_name}")
    return session_name


def _build_discord_prompt_fragment(handle: str) -> str:
    """Build the Discord conversation prompt fragment for an agent."""
    return f"""<system-context>
You are in a shared Discord thread with humans and other agents.
Your agent handle is "{handle}". When someone mentions "{handle}" (or @{handle}),
they are likely addressing you.

## How this thread works

Everyone in this thread sees every message — humans and agents alike. There is
no private channel. When you reply, your message is visible to all participants.

## When to respond

- **You are named directly** ("{handle}, fix the tests" or "@{handle}"): You
  should definitely respond. This is a strong signal the message is for you.
- **@{handle} in thread**: This is the strongest signal — the human expects a
  response from you specifically.
- **Multiple agents named** ("{handle}, priya, work together"): All named agents
  should respond.
- **No one is named, but you have relevant info**: Respond if you can genuinely
  add value. If someone else already handled it, stay silent. Silence is fine.
- **A peer agent says something relevant to your work**: You may respond, but
  do not pile on.
- **A peer says something not relevant to you**: Read for context, move on.

The key test: **does this message need MY input?** If yes, respond. If maybe,
use judgment. If no, listen.

## Replying to Discord

Normal assistant output stays private to the session. Do not assume a human on
Discord can see it.

If the event includes `reply_contract: explicit_publish_required`, follow that
contract literally: plain assistant output does not go back to Discord.

To send a human-visible reply, write the body to a file and run the command
shown in the `reply_tool` field of the discord-event. Example:
  gc discord reply-current --conversation-id <id> --reply-to <id> --body-file <path>

**Always prefix your Discord messages with your handle in bold** so humans and
other agents know who is speaking. Example: if your handle is "sky", start
every reply with "**sky:** " followed by your message. Other agents should use
@sky to address you directly.

Do not put long replies inline in --body. Use --body-file.
Only claim success after the command returns JSON with a non-empty
record.remote_message_id.

## Addressing others

When addressing a specific peer agent, use @name for clarity (e.g., "@priya can
you look at the test failures?"). This helps the routing layer.

When you need a response from a human, use their Discord mention tag (e.g.,
`<@USER_ID>`) so they get a notification. The discord-event includes
`from_user_id` — use `<@` + the from_user_id value + `>` to tag the person
who messaged you. Do not assume humans are watching the thread.
</system-context>"""


def _build_thread_launch_envelope(
    *,
    discord_message: dict[str, Any],
    thread_id: str,
    channel_id: str,
    guild_id: str,
    handle: str,
    content: str,
) -> str:
    """Build a discord-event envelope with prompt fragment for a thread-launch message."""
    author = discord_message.get("author") or {}
    message_id = str(discord_message.get("id", ""))
    prompt = _build_discord_prompt_fragment(handle)
    lines = [
        prompt,
        "",
        "<discord-event>",
        "version: 1",
        "kind: discord_human_message",
        f"guild_id: {guild_id}",
        f"conversation: discord/{guild_id}/{thread_id}",
        f"conversation_key: {guild_id}:{thread_id}",
        f"discord_message_id: {message_id}",
        f"from_display: {str(author.get('global_name', '') or author.get('username', '')).strip()}",
        f"from_user_id: {str(author.get('id', '')).strip()}",
        "delivery: targeted",
        f"target_handle: {handle}",
        f"untrusted_body_json: {json.dumps(content)}",
        f"publish_binding_id: extmsg:thread:{thread_id}",
        f"publish_conversation_id: {thread_id}",
        f"publish_trigger_id: {message_id}",
        f"publish_reply_to_discord_message_id: {message_id}",
        "normal_output_visibility: internal_only",
        "reply_contract: explicit_publish_required",
        f"reply_tool: gc discord reply-current --conversation-id {thread_id} --body-file <path>",
        "reply_success_signal: record.remote_message_id",
        "reply_turn_requirement: if you intend to answer, do not end the turn without a successful reply-current",
        "</discord-event>",
    ]
    return "\n".join(lines)


def launch_thread_for_mentions(
    discord_message: dict[str, Any],
    targets: list[dict[str, Any]],
    guild_id: str,
    application_id: str,
) -> dict[str, Any] | None:
    """Create a Discord thread from a room message and set up extmsg group.

    Called when a human @mentions agents in a room. Creates the thread,
    creates an extmsg group, and adds each target as a participant.

    Returns the created group record, or None on failure.
    """
    channel_id = str(discord_message.get("channel_id", ""))
    message_id = str(discord_message.get("id", ""))
    content = str(discord_message.get("content", ""))
    if not channel_id or not message_id or not targets:
        return None

    # Create Discord thread from the message.
    thread_name = content[:100] if content else "Thread"
    try:
        thread = discord_api_request(
            "POST",
            f"/channels/{channel_id}/messages/{message_id}/threads",
            {"name": thread_name, "auto_archive_duration": 1440},
        )
    except DiscordAPIError as exc:
        print(f"[extmsg] Discord thread creation failed: {exc}", flush=True)
        return None

    thread_id = str(thread.get("id", ""))
    if not thread_id:
        return None

    # Create extmsg group for the thread.
    thread_conversation = {
        "scope_id": guild_id or "global",
        "provider": "discord",
        "account_id": application_id,
        "conversation_id": thread_id,
        "parent_conversation_id": channel_id,
        "kind": "thread",
    }

    first_handle = ""
    for t in targets:
        if t["kind"] == "session":
            first_handle = str(t["session"].get("alias", "") or t["session"].get("name", ""))
            break
        if t["kind"] == "template":
            first_handle = str(t["agent"].get("name", "")).rsplit("/", 1)[-1]
            break

    print(f"[extmsg] thread created: {thread_id}, creating group...", flush=True)
    try:
        group = gc_api_request("POST", "/v0/extmsg/groups", {
            "root_conversation": thread_conversation,
            "mode": "launcher",
            "default_handle": first_handle,
            "metadata": {"guild_id": guild_id, "source_channel_id": channel_id, "source_message_id": message_id},
        })
    except GCAPIError as exc:
        print(f"[extmsg] group creation failed: {exc}", flush=True)
        return None

    group_id = str(group.get("id", ""))

    # Prepare participant work items.
    def _create_participant(target: dict[str, Any]) -> tuple[str, str] | None:
        """Create a session for a target and return (session_id, handle) or None."""
        if target["kind"] == "session":
            sid = str(target["session"].get("name", ""))
            handle = str(target["session"].get("alias", "") or sid)
            return (sid, handle) if sid else None
        if target["kind"] == "template":
            template_name = str(target["template"])
            handle = template_name.rsplit("/", 1)[-1] if "/" in template_name else template_name
            envelope = _build_thread_launch_envelope(
                discord_message=discord_message,
                thread_id=thread_id,
                channel_id=channel_id,
                guild_id=guild_id,
                handle=handle,
                content=content,
            )
            try:
                sid = _create_session_from_template(template_name, handle, initial_message=envelope)
                print(f"[extmsg] session {sid} created from {template_name}", flush=True)
                return (sid, handle)
            except (GCAPIError, RuntimeError) as exc:
                print(f"[extmsg] session creation failed for {template_name}: {exc}", flush=True)
                return None
        return None

    # Create sessions sequentially to avoid parallel prompt resolution races.
    # TODO: switch back to parallel once the session API prompt race is fixed.
    participants: list[tuple[str, str]] = []
    for t in targets:
        result = _create_participant(t)
        if result:
            participants.append(result)

    # Register participants, bindings, and transcript memberships (fast, sequential).
    for session_id, handle in participants:
        try:
            gc_api_request("POST", "/v0/extmsg/groups/participants", {
                "group_id": group_id,
                "handle": handle,
                "session_id": session_id,
                "public": True,
            })
        except GCAPIError:
            pass
        try:
            gc_api_request("POST", "/v0/extmsg/bindings", {
                "conversation": thread_conversation,
                "session_id": session_id,
            })
        except GCAPIError:
            pass
        # Enroll in transcript so the session receives notifications
        # when other participants post messages.
        try:
            gc_api_request("POST", "/v0/extmsg/transcript/membership", {
                "conversation": thread_conversation,
                "session_id": session_id,
            })
        except GCAPIError:
            pass

    return group


def add_participants_to_thread(
    thread_id: str,
    parent_channel_id: str,
    targets: list[dict[str, Any]],
    guild_id: str,
    application_id: str,
    content: str = "",
) -> None:
    """Add new agent participants to an existing thread.

    For each target, creates a session (if template) or reuses existing,
    adds as group participant, creates binding, and delivers the message
    as a discord-event envelope.
    """
    thread_conversation = {
        "scope_id": guild_id or "global",
        "provider": "discord",
        "account_id": application_id,
        "conversation_id": thread_id,
        "parent_conversation_id": parent_channel_id,
        "kind": "thread",
    }

    # Find or create the group for this thread.
    try:
        group = gc_api_request("GET", f"/v0/extmsg/groups?scope_id={guild_id}&provider=discord&account_id={application_id}&conversation_id={thread_id}&kind=thread")
        group_id = str(group.get("id", ""))
    except GCAPIError:
        # Group doesn't exist yet — create it.
        try:
            group = gc_api_request("POST", "/v0/extmsg/groups", {
                "root_conversation": thread_conversation,
                "mode": "launcher",
                "default_handle": "",
            })
            group_id = str(group.get("id", ""))
        except GCAPIError as exc:
            print(f"[extmsg] group creation failed for thread {thread_id}: {exc}", flush=True)
            return

    # Get existing bindings for this thread to avoid duplicates.
    existing_sessions: set[str] = set()
    try:
        # Check all sessions — find those bound to this thread.
        sessions = list_city_sessions(state="all")
        for s in sessions:
            sid = str(s.get("session_name", s.get("name", "")))
            try:
                bindings = gc_api_request("GET", f"/v0/extmsg/bindings?session_id={sid}")
                for b in (bindings.get("items") or []):
                    if str(b.get("conversation", {}).get("conversation_id", "")) == thread_id:
                        tmpl = str(s.get("template", ""))
                        existing_sessions.add(tmpl)
                        break
            except GCAPIError:
                pass
    except GCAPIError:
        pass

    for target in targets:
        session_id = ""
        handle = ""
        if target["kind"] == "session":
            session_id = str(target["session"].get("session_name", target["session"].get("name", "")))
            handle = str(target["session"].get("alias", "") or session_id)
        elif target["kind"] == "template":
            template_name = str(target["template"])
            handle = template_name.rsplit("/", 1)[-1] if "/" in template_name else template_name
            # Skip if a session from this template is already in the thread.
            if template_name in existing_sessions:
                print(f"[extmsg] {handle} already in thread {thread_id}, skipping", flush=True)
                continue
            envelope = _build_thread_launch_envelope(
                discord_message={"id": "", "content": content, "author": {}, "channel_id": thread_id},
                thread_id=thread_id,
                channel_id=parent_channel_id,
                guild_id=guild_id,
                handle=handle,
                content=content,
            )
            try:
                session_id = _create_session_from_template(
                    template_name, handle, initial_message=envelope,
                )
                print(f"[extmsg] added {handle} to thread {thread_id} as {session_id}", flush=True)
            except (GCAPIError, RuntimeError) as exc:
                print(f"[extmsg] failed to add {handle}: {exc}", flush=True)
                continue

        if not session_id or not handle:
            continue

        # Add as group participant.
        try:
            gc_api_request("POST", "/v0/extmsg/groups/participants", {
                "group_id": group_id,
                "handle": handle,
                "session_id": session_id,
                "public": True,
            })
        except GCAPIError:
            pass

        # Create binding.
        try:
            gc_api_request("POST", "/v0/extmsg/bindings", {
                "conversation": thread_conversation,
                "session_id": session_id,
            })
        except GCAPIError:
            pass

        # Enroll in transcript for notifications.
        try:
            gc_api_request("POST", "/v0/extmsg/transcript/membership", {
                "conversation": thread_conversation,
                "session_id": session_id,
            })
        except GCAPIError:
            pass


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
    # Try /v0/config first — it includes pack-expanded agents.
    # /v0/agents may return empty if the binary predates pack expansion fixes.
    config_payload = gc_api_request("GET", "/v0/config")
    if isinstance(config_payload, dict):
        agents = config_payload.get("agents")
        if isinstance(agents, list) and agents:
            return [item for item in agents if isinstance(item, dict)]
    # Fallback to /v0/agents.
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


def resolve_routable_session_identity(session_selector: str) -> dict[str, str]:
    return resolve_routable_session_identity_from_sessions(list_city_sessions(state="all"), session_selector)


def _session_identity_fields(item: dict[str, Any]) -> dict[str, str]:
    return {
        "session_name": str(item.get("session_name", "")).strip(),
        "session_id": str(item.get("id", "")).strip(),
        "alias": str(item.get("alias", "")).strip(),
    }


def resolve_routable_session_identity_from_sessions(sessions: list[dict[str, Any]], session_selector: str) -> dict[str, str]:
    selector = str(session_selector).strip()
    if not selector:
        return {}
    alias_match: dict[str, Any] | None = None
    name_match: dict[str, Any] | None = None
    id_match: dict[str, Any] | None = None
    for item in sessions:
        if not session_record_routable(item):
            continue
        if str(item.get("alias", "")).strip() == selector and (
            alias_match is None or _session_record_preference(item) > _session_record_preference(alias_match)
        ):
            alias_match = item
        if str(item.get("session_name", "")).strip() == selector and (
            name_match is None or _session_record_preference(item) > _session_record_preference(name_match)
        ):
            name_match = item
        if str(item.get("id", "")).strip() == selector and (
            id_match is None or _session_record_preference(item) > _session_record_preference(id_match)
        ):
            id_match = item
    chosen = alias_match or name_match or id_match
    if not chosen:
        return {}
    return _session_identity_fields(chosen)


def resolve_routable_session_candidate_from_sessions(
    sessions: list[dict[str, Any]], *selectors: str
) -> tuple[str, dict[str, str]]:
    seen: set[str] = set()
    for selector in selectors:
        normalized = str(selector).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        identity = resolve_routable_session_identity_from_sessions(sessions, normalized)
        if identity:
            return normalized, identity
    return "", {}


def resolve_routable_session_identity_eventually(*selectors: str) -> dict[str, str]:
    _matched_selector, identity = resolve_routable_session_candidate_eventually(*selectors)
    return identity


def resolve_routable_session_candidate_eventually(*selectors: str) -> tuple[str, dict[str, str]]:
    candidates: list[str] = []
    seen: set[str] = set()
    for selector in selectors:
        normalized = str(selector).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        candidates.append(normalized)
    if not candidates:
        return "", {}
    deadline = time.monotonic() + ROOM_LAUNCH_IDENTITY_RESOLVE_TIMEOUT_SECONDS
    while True:
        sessions = list_city_sessions(state="all")
        matched_selector, identity = resolve_routable_session_candidate_from_sessions(sessions, *candidates)
        if identity:
            return matched_selector, identity
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return "", {}
        time.sleep(min(ROOM_LAUNCH_IDENTITY_RESOLVE_DELAY_SECONDS, remaining))


def session_record_ready(item: dict[str, Any] | None) -> bool:
    if not session_record_routable(item):
        return False
    if str(item.get("state", "")).strip() != "active":
        return False
    return bool(item.get("running"))


def resolve_ready_session_identity_from_sessions(sessions: list[dict[str, Any]], session_selector: str) -> dict[str, str]:
    selector = str(session_selector).strip()
    if not selector:
        return {}
    alias_match: dict[str, Any] | None = None
    name_match: dict[str, Any] | None = None
    id_match: dict[str, Any] | None = None
    for item in sessions:
        if not session_record_ready(item):
            continue
        if str(item.get("alias", "")).strip() == selector and (
            alias_match is None or _session_record_preference(item) > _session_record_preference(alias_match)
        ):
            alias_match = item
        if str(item.get("session_name", "")).strip() == selector and (
            name_match is None or _session_record_preference(item) > _session_record_preference(name_match)
        ):
            name_match = item
        if str(item.get("id", "")).strip() == selector and (
            id_match is None or _session_record_preference(item) > _session_record_preference(id_match)
        ):
            id_match = item
    chosen = alias_match or name_match or id_match
    if not chosen:
        return {}
    return _session_identity_fields(chosen)


def resolve_ready_session_candidate_from_sessions(
    sessions: list[dict[str, Any]], *selectors: str
) -> tuple[str, dict[str, str]]:
    seen: set[str] = set()
    for selector in selectors:
        normalized = str(selector).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        identity = resolve_ready_session_identity_from_sessions(sessions, normalized)
        if identity:
            return normalized, identity
    return "", {}


def resolve_ready_session_candidate_eventually(*selectors: str) -> tuple[str, dict[str, str]]:
    candidates: list[str] = []
    seen: set[str] = set()
    for selector in selectors:
        normalized = str(selector).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        candidates.append(normalized)
    if not candidates:
        return "", {}
    deadline = time.monotonic() + ROOM_LAUNCH_READY_RESOLVE_TIMEOUT_SECONDS
    while True:
        sessions = list_city_sessions(state="all")
        matched_selector, identity = resolve_ready_session_candidate_from_sessions(sessions, *candidates)
        if identity:
            return matched_selector, identity
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return "", {}
        time.sleep(min(ROOM_LAUNCH_READY_RESOLVE_DELAY_SECONDS, remaining))


def resolve_routable_session_record(session_selector: str) -> dict[str, Any] | None:
    selector = str(session_selector).strip()
    if not selector:
        return None
    by_name = session_index_by_name(state="all").get(selector)
    if session_record_routable(by_name):
        return by_name
    by_alias = session_index_by_alias(state="all").get(selector)
    if session_record_routable(by_alias):
        return by_alias
    for item in list_city_sessions(state="all"):
        if str(item.get("id", "")).strip() == selector and session_record_routable(item):
            return item
    return None


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
    if str(initial_message).strip():
        raise ValueError("initial_message is not supported for async launcher session creation")
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
            "async": True,
        },
        headers=headers,
    )
    if not isinstance(payload, dict):
        return {}
    return payload


def room_launch_participant_delivery_selector(participant: dict[str, Any]) -> str:
    for key in ("session_name", "session_id", "session_alias", "delivery_selector"):
        value = str((participant or {}).get(key, "")).strip()
        if value:
            return value
    return ""


def room_launch_primer_message(
    launch: dict[str, Any],
    participant: dict[str, Any],
    *,
    extra_message: str = "",
    peer_fanout_enabled: bool = True,
) -> str:
    qualified_handle = str(participant.get("qualified_handle", "")).strip() or str(launch.get("qualified_handle", "")).strip()
    session_name = str(participant.get("session_name", "")).strip()
    lines = [
        "<discord-launch-primer>",
        "This is transport guidance for a Discord launcher-backed session.",
        "Do not answer this primer directly.",
        f"Your routed handle for this conversation is: {qualified_handle}",
    ]
    if session_name:
        lines.append(f"Your current session_name is: {session_name}")
    lines.extend(
        [
            "Normal assistant text is private and invisible to Discord humans.",
            "If you want a human-visible reply, you MUST:",
            "1. Write the reply body to a file.",
            "2. Run `gc discord reply-current --body-file <path>`.",
            "3. Only treat the Discord reply as sent after the command returns JSON with a non-empty `record.remote_message_id`.",
            "For launcher-backed root-room turns, the first successful `gc discord reply-current` creates the Discord thread automatically.",
            "Do not end a turn with plain assistant prose if you intend the human to see a reply.",
            "If you decide not to answer, stay silent instead of pretending the reply was sent.",
        ]
    )
    if peer_fanout_enabled:
        lines.extend(
            [
                "In launcher-managed threads, human-visible replies are also forwarded to the other participating launcher sessions as peer input.",
                "Include `@@rig/alias` in the Discord reply if you want to target a specific thread participant; untargeted replies to peer publications do not fan out.",
            ]
        )
    extra = str(extra_message).strip()
    if extra:
        lines.extend(
            [
                "",
                "Additional launch guidance:",
                extra,
            ]
        )
    lines.append("</discord-launch-primer>")
    return "\n".join(lines)


def room_launch_primer_idempotency_key(launch_id: str, qualified_handle: str) -> str:
    return f"{str(launch_id).strip()}:primer:{str(qualified_handle).strip()}:v{ROOM_LAUNCH_PRIMER_VERSION}"


def room_launch_participant_primer_identity(participant: dict[str, Any]) -> str:
    for key in ("session_id", "session_name", "session_alias"):
        value = str((participant or {}).get(key, "")).strip()
        if value:
            return value
    return ""


def room_launch_participant_needs_primer(participant: dict[str, Any]) -> bool:
    try:
        current_version = int(str(participant.get("primer_version", "")).strip() or "0")
    except ValueError:
        current_version = 0
    if current_version < ROOM_LAUNCH_PRIMER_VERSION:
        return True
    current_identity = room_launch_participant_primer_identity(participant)
    if not current_identity:
        return False
    return str(participant.get("primer_identity", "")).strip() != current_identity


def prime_room_launch_participant(
    launch: dict[str, Any],
    participant: dict[str, Any],
    *,
    extra_message: str = "",
) -> dict[str, Any]:
    qualified_handle = str(participant.get("qualified_handle", "")).strip()
    if not qualified_handle:
        raise ValueError("launch participant is missing qualified_handle")
    selector = room_launch_participant_delivery_selector(participant)
    if not selector:
        raise GCAPIError(f"launch participant {qualified_handle!r} is not routable yet")
    launcher = resolve_room_launcher(load_config(), str(launch.get("conversation_id", "")).strip()) or {}
    peer_fanout_enabled = bool(binding_peer_policy(launcher).get("peer_fanout_enabled"))
    deliver_session_message(
        selector,
        room_launch_primer_message(
            launch,
            participant,
            extra_message=extra_message,
            peer_fanout_enabled=peer_fanout_enabled,
        ),
        idempotency_key=room_launch_primer_idempotency_key(str(launch.get("launch_id", "")).strip(), qualified_handle),
    )
    body = copy.deepcopy(participant)
    body["primer_version"] = ROOM_LAUNCH_PRIMER_VERSION
    body["primer_identity"] = room_launch_participant_primer_identity(participant)
    body["primed_at"] = utcnow()
    return body


def _sync_launch_participant(
    current: dict[str, Any],
    normalized_handle: str,
    participant: dict[str, Any],
) -> dict[str, Any]:
    participants = room_launch_participants(current)
    participants[normalized_handle] = participant
    current["participants"] = participants
    if not str(current.get("qualified_handle", "")).strip():
        current["qualified_handle"] = normalized_handle
    if str(current.get("qualified_handle", "")).strip() == normalized_handle:
        current["session_alias"] = str(participant.get("session_alias", "")).strip()
        current["session_id"] = str(participant.get("session_id", "")).strip()
        current["session_name"] = str(participant.get("session_name", "")).strip()
    if not str(current.get("last_addressed_qualified_handle", "")).strip():
        current["last_addressed_qualified_handle"] = normalized_handle
    return current


def _room_launch_participant_matches_session(participant: dict[str, Any], *, session_name: str = "", session_id: str = "") -> bool:
    normalized_session_name = str(session_name).strip()
    normalized_session_id = str(session_id).strip()
    if normalized_session_id and str(participant.get("session_id", "")).strip() == normalized_session_id:
        return True
    if normalized_session_name and normalized_session_name in {
        str(participant.get("session_name", "")).strip(),
        str(participant.get("session_alias", "")).strip(),
        str(participant.get("delivery_selector", "")).strip(),
    }:
        return True
    return False


def ensure_room_launch_session_for_handle(
    launch: dict[str, Any],
    qualified_handle: str,
    *,
    title: str = "",
    initial_message: str = "",
) -> tuple[dict[str, Any], dict[str, Any]]:
    launch_id = str(launch.get("launch_id", "")).strip()
    normalized_handle = str(qualified_handle).strip()
    if not launch_id or not normalized_handle:
        raise ValueError("launch is missing required session fields")
    with advisory_lock(room_launch_lock_path(launch_id)):
        current = load_room_launch(launch_id) or normalize_room_launch_record(dict(launch))
        current = normalize_room_launch_record(current)
        participants = room_launch_participants(current)
        participant = copy.deepcopy(participants.get(normalized_handle, {}))
        created_new = False
        session_alias = (
            str(participant.get("session_alias", "")).strip()
            or room_launch_session_alias(
                str(current.get("guild_id", "")).strip(),
                str(current.get("conversation_id", "")).strip(),
                str(current.get("root_message_id", "")).strip(),
                normalized_handle,
            )
        )
        existing = None
        if str(participant.get("session_name", "")).strip():
            existing_by_name = session_index_by_name(state="all").get(str(participant.get("session_name", "")).strip())
            if session_record_routable(existing_by_name):
                existing = existing_by_name
        if not session_record_routable(existing):
            existing_by_alias = session_index_by_alias(state="all").get(session_alias)
            if session_record_routable(existing_by_alias):
                existing = existing_by_alias
        if session_record_routable(existing):
            participant["session_alias"] = str(existing.get("alias", "")).strip() or session_alias
            participant["session_id"] = str(existing.get("id", "")).strip()
            participant["session_name"] = str(existing.get("session_name", "")).strip()
        else:
            created_new = True
            created = create_agent_session(
                normalized_handle,
                alias=session_alias,
                title=title or room_launch_thread_name(normalized_handle, str(current.get("from_display", "")).strip()),
                initial_message=initial_message,
                idempotency_key=f"{launch_id}:create-session:{normalized_handle}",
            )
            participant["session_alias"] = str(created.get("alias", "")).strip() or session_alias
            participant["session_id"] = str(created.get("id", "")).strip()
            participant["session_name"] = str(created.get("session_name", "")).strip()
        created_identity = {
            "alias": str(participant.get("session_alias", "")).strip(),
            "session_id": str(participant.get("session_id", "")).strip(),
            "session_name": str(participant.get("session_name", "")).strip(),
        }
        selector_snapshot, hydrated = resolve_routable_session_candidate_from_sessions(
            list_city_sessions(state="all"),
            participant.get("session_name", ""),
            participant.get("session_id", ""),
            participant.get("delivery_selector", ""),
            participant.get("session_alias", ""),
        )
        if created_new and not selector_snapshot:
            created_selector = str(created_identity.get("session_name", "")).strip() or str(created_identity.get("session_id", "")).strip()
            if created_selector:
                selector_snapshot = created_selector
                hydrated = created_identity
        if created_new and not selector_snapshot:
            selector_snapshot, hydrated = resolve_routable_session_candidate_eventually(
                participant.get("session_name", ""),
                participant.get("delivery_selector", ""),
                participant.get("session_alias", ""),
                participant.get("session_id", ""),
            )
        if hydrated:
            participant["session_alias"] = str(hydrated.get("alias", "")).strip() or str(participant.get("session_alias", "")).strip()
            participant["session_id"] = str(hydrated.get("session_id", "")).strip() or str(participant.get("session_id", "")).strip()
            participant["session_name"] = str(hydrated.get("session_name", "")).strip() or str(participant.get("session_name", "")).strip()
        if created_new and not selector_snapshot:
            raise GCAPIError(f"created launch session is not routable yet: {participant['session_alias'] or normalized_handle}")
        participant["delivery_selector"] = (
            str(participant.get("session_name", "")).strip()
            or str(participant.get("session_id", "")).strip()
            or str(participant.get("session_alias", "")).strip()
            or selector_snapshot
        )
        participant["qualified_handle"] = normalized_handle
        current = _sync_launch_participant(current, normalized_handle, participant)
        current = save_room_launch(current)
        if created_new:
            _ready_selector, ready_identity = resolve_ready_session_candidate_from_sessions(
                list_city_sessions(state="all"),
                participant.get("session_name", ""),
                participant.get("session_id", ""),
                participant.get("session_alias", ""),
                participant.get("delivery_selector", ""),
            )
            if not ready_identity:
                ready_selector = str(created_identity.get("session_name", "")).strip() or str(created_identity.get("session_id", "")).strip()
                if ready_selector:
                    _ready_selector = ready_selector
                    ready_identity = created_identity
            if not ready_identity:
                _ready_selector, ready_identity = resolve_ready_session_candidate_eventually(
                    participant.get("session_name", ""),
                    participant.get("session_id", ""),
                    participant.get("session_alias", ""),
                    participant.get("delivery_selector", ""),
                )
            if not ready_identity:
                raise GCAPIError(f"created launch session is not ready yet: {participant['session_alias'] or normalized_handle}")
            participant["session_alias"] = str(ready_identity.get("alias", "")).strip() or str(participant.get("session_alias", "")).strip()
            participant["session_id"] = str(ready_identity.get("session_id", "")).strip() or str(participant.get("session_id", "")).strip()
            participant["session_name"] = str(ready_identity.get("session_name", "")).strip() or str(participant.get("session_name", "")).strip()
            participant["delivery_selector"] = (
                str(participant.get("session_name", "")).strip()
                or str(participant.get("session_id", "")).strip()
                or str(participant.get("session_alias", "")).strip()
            )
            current = _sync_launch_participant(current, normalized_handle, participant)
            current = save_room_launch(current)
        if room_launch_participant_needs_primer(participant):
            try:
                participant = prime_room_launch_participant(
                    current,
                    participant,
                    extra_message=initial_message,
                )
            except Exception as exc:  # noqa: BLE001
                participant["primer_error"] = str(exc)
                participant["primer_last_failed_at"] = utcnow()
            else:
                participant.pop("primer_error", None)
                participant.pop("primer_last_failed_at", None)
            current = _sync_launch_participant(current, normalized_handle, participant)
            current = save_room_launch(current)
        return current, copy.deepcopy(participant)


def ensure_room_launch_session(
    launch: dict[str, Any],
    *,
    title: str = "",
    initial_message: str = "",
) -> dict[str, Any]:
    qualified_handle = str(launch.get("qualified_handle", "")).strip()
    current, _participant = ensure_room_launch_session_for_handle(
        launch,
        qualified_handle,
        title=title,
        initial_message=initial_message,
    )
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


def _binding_session_lookup(binding: dict[str, Any], *, launch: dict[str, Any] | None = None) -> dict[str, str]:
    if launch and str(binding.get("publish_route_kind", "")).strip() == "room_launch":
        return room_launch_participant_session_lookup(launch)
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


def _rename_peer_target(peer_delivery: dict[str, Any], old_session_name: str, new_session_name: str) -> None:
    old_name = str(old_session_name).strip()
    new_name = str(new_session_name).strip()
    if not old_name or not new_name or old_name == new_name:
        return
    targets = peer_delivery.setdefault("targets", [])
    old_entry = next((entry for entry in targets if str(entry.get("session_name", "")).strip() == old_name), None)
    new_entry = next((entry for entry in targets if str(entry.get("session_name", "")).strip() == new_name), None)
    if old_entry and not new_entry:
        old_entry["session_name"] = new_name
    elif old_entry and new_entry:
        for key, value in old_entry.items():
            if key == "session_name":
                continue
            new_entry.setdefault(key, value)
        targets.remove(old_entry)
    for key in ("mentioned_session_names", "frozen_targets"):
        values = []
        seen: set[str] = set()
        for item in peer_delivery.get(key, []):
            normalized = new_name if str(item).strip() == old_name else str(item).strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            values.append(normalized)
        peer_delivery[key] = values


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
    source_session_id: str,
    source_event_kind: str,
    launch: dict[str, Any] | None = None,
) -> tuple[list[str], str, str, list[str]]:
    policy = binding_peer_policy(binding)
    session_lookup = _binding_session_lookup(binding, launch=launch)
    handle_lookup = room_launch_participant_handle_lookup(launch) if launch else {}
    delivery_targets = room_launch_participant_delivery_targets(launch) if launch else []
    source_delivery_selector = source_session_name
    if launch:
        source_handle = room_launch_participant_handle_for_session(
            launch,
            session_name=source_session_name,
            session_id=source_session_id,
        )
        if source_handle:
            source_delivery_selector = room_launch_participant_delivery_selector(
                room_launch_participant(launch, source_handle)
            )
    if handle_lookup:
        handles = extract_agent_handles(body)
        if handles:
            if any(handle not in handle_lookup for handle in handles):
                return [], "targeted", "failed_targeting_unknown_handle", handles
            resolved_mentions: list[str] = []
            targets: list[str] = []
            seen: set[str] = set()
            for handle in handles:
                selector = str(handle_lookup.get(handle, "")).strip()
                if not selector:
                    continue
                resolved_mentions.append(selector)
                if selector == source_delivery_selector or selector in seen:
                    continue
                seen.add(selector)
                targets.append(selector)
            if not targets:
                return [], "targeted", "failed_targeting_self_only", resolved_mentions
            return targets, "targeted", "", resolved_mentions
    mentions = extract_peer_session_mentions(body)
    if mentions:
        if any(name not in session_lookup for name in mentions):
            return [], "targeted", "failed_targeting_unknown_session", mentions
        targets = [str(session_lookup.get(name, "")).strip() for name in mentions]
        targets = [name for name in targets if name and name != source_delivery_selector]
        if not targets:
            return [], "targeted", "failed_targeting_self_only", mentions
        return targets, "targeted", "", mentions
    if source_event_kind == "discord_peer_publication":
        return [], "untargeted", "skipped_peer_target_required", []
    if not policy.get("allow_untargeted_peer_fanout"):
        return [], "untargeted", "skipped_policy_untargeted_disabled", []
    if delivery_targets:
        targets = [name for name in delivery_targets if name != source_delivery_selector]
    else:
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
    launch: dict[str, Any] | None = None,
) -> str:
    launch_id = str((launch or {}).get("launch_id", "")).strip()
    target_delivery_selector, target_participant, target_identity = resolve_room_launch_target_selector(
        launch or {},
        target_session_name,
    )
    target_delivery_selector = target_delivery_selector or str(target_session_name).strip()
    source_qualified_handle = room_launch_participant_handle_for_session(
        launch or {},
        session_name=source_session_name,
        session_id=source_session_id,
    )
    target_qualified_handle = str(target_participant.get("qualified_handle", "")).strip()
    if not target_qualified_handle and target_identity:
        target_qualified_handle = room_launch_participant_handle_for_session(
            launch or {},
            session_name=str(target_identity.get("session_name", "")).strip(),
            session_id=str(target_identity.get("session_id", "")).strip(),
        )
    if not target_qualified_handle and target_delivery_selector:
        target_qualified_handle = room_launch_participant_handle_for_session(
            launch or {},
            session_name=target_delivery_selector,
        )
    target_participant = target_participant or room_launch_participant(launch or {}, target_qualified_handle)
    target_session_label = str(target_participant.get("session_name", "")).strip() or target_delivery_selector
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
        f"target_session_name: {target_session_label}",
        f"publish_id: {str(record.get('publish_id', '')).strip()}",
        f"root_ingress_receipt_id: {root_ingress_receipt_id}",
        f"publish_binding_id: {str(binding.get('id', '')).strip()}",
        f"publish_conversation_id: {str(record.get('conversation_id', '')).strip()}",
        f"publish_trigger_id: {str(record.get('remote_message_id', '')).strip()}",
        f"publish_reply_to_discord_message_id: {str(record.get('remote_message_id', '')).strip()}",
        f"publish_launch_id: {launch_id}",
        f"delivery_idempotency_key: {idempotency_key}",
        f"delivery: {delivery}",
        f"mentioned_session_names_json: {json.dumps(mentioned_session_names)}",
        f"untrusted_body_json: {json.dumps(str(record.get('body', '')))}",
        f"created_at: {str(record.get('created_at', '')).strip()}",
        "normal_output_visibility: internal_only",
        "reply_contract: explicit_publish_required",
        "hop: 1",
        "max_hop: 1",
    ]
    if launch_id:
        lines.extend(
            [
                f"launch_id: {launch_id}",
                "launch_surface_kind: room",
                f"launch_root_qualified_handle: {str((launch or {}).get('qualified_handle', '')).strip()}",
                f"launch_root_session_alias: {str((launch or {}).get('session_alias', '')).strip()}",
                f"launch_qualified_handle: {target_qualified_handle}",
                f"launch_session_alias: {str(target_participant.get('session_alias', '')).strip()}",
                f"launch_session_name: {target_session_label}",
                f"source_qualified_handle: {source_qualified_handle}",
                f"thread_participants_json: {json.dumps(room_launch_participant_summaries(launch or {}))}",
            ]
        )
    lines.append("</discord-event>")
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


def record_room_launch_message_target(
    launch_id: str,
    remote_message_id: str,
    *,
    source_session_name: str = "",
    source_session_id: str = "",
) -> dict[str, Any] | None:
    normalized_launch_id = str(launch_id).strip()
    normalized_remote_message_id = str(remote_message_id).strip()
    if not normalized_launch_id or not normalized_remote_message_id:
        return None
    with advisory_lock(room_launch_lock_path(normalized_launch_id)):
        current = load_room_launch(normalized_launch_id)
        if not current:
            return None
        current = normalize_room_launch_record(current)
        matched_handle = ""
        for qualified_handle, participant in room_launch_participants(current).items():
            if _room_launch_participant_matches_session(
                participant,
                session_name=source_session_name,
                session_id=source_session_id,
            ):
                matched_handle = qualified_handle
                break
        if not matched_handle:
            return current
        body = copy.deepcopy(current)
        message_targets = {
            str(message_id).strip(): str(qualified_handle).strip()
            for message_id, qualified_handle in body.get("message_targets", {}).items()
            if str(message_id).strip() and str(qualified_handle).strip()
        }
        target_order = [str(item).strip() for item in body.get("message_target_order", []) if str(item).strip()]
        message_targets[normalized_remote_message_id] = matched_handle
        target_order = [item for item in target_order if item != normalized_remote_message_id]
        target_order.append(normalized_remote_message_id)
        while len(target_order) > ROOM_LAUNCH_MESSAGE_TARGET_LIMIT:
            evicted = target_order.pop(0)
            message_targets.pop(evicted, None)
        body["message_targets"] = {
            message_id: message_targets[message_id]
            for message_id in target_order
            if message_id in message_targets
        }
        body["message_target_order"] = target_order
        return save_room_launch(body)


def resolve_publish_conversation_id(
    binding: dict[str, Any],
    requested_conversation_id: str,
    *,
    bot_token: str | None = None,
) -> str:
    binding_conversation_id = str(binding.get("conversation_id", "")).strip()
    requested = str(requested_conversation_id).strip()
    if not requested or requested == binding_conversation_id:
        return binding_conversation_id
    if str(binding.get("kind", "")).strip() == "dm":
        raise ValueError("--conversation-id cannot override a DM binding")
    try:
        channel_info = discord_api_request(
            "GET",
            f"/channels/{urllib.parse.quote(requested)}",
            bot_token=bot_token,
        )
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
    bot_token: str | None = None,
) -> tuple[str, str, dict[str, Any] | None]:
    reply_target = str(reply_to_message_id).strip() or str(trigger_id).strip()
    source_meta = derive_publish_source_metadata(source_context)
    launch_id = str(source_meta.get("launch_id", "")).strip()
    if str(binding.get("publish_route_kind", "")).strip() != "room_launch" or not launch_id:
        conversation_id = resolve_publish_conversation_id(
            binding,
            requested_conversation_id,
            bot_token=bot_token,
        )
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
    if phase in {"peer_fanout_in_progress", "peer_fanout_partial_failure"}:
        return True
    if status.startswith("failed_"):
        return True
    return any(
        str(entry.get("status", "")).strip()
        in {"pending", "in_progress", "awaiting_result", "failed_retryable", "failed_permanent", "delivery_unknown"}
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
    if {"pending", "in_progress", "awaiting_result"} & terminal_statuses:
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
                "request_id": "",
                "event_cursor": "",
                "response": {},
                "terminal_evidence": {},
                "reason": "",
            },
        )
        current["peer_delivery"] = peer_delivery
        current = _save_chat_publish_record(current)
        return current, attempt_count


def _patch_peer_delivery_target(
    *,
    publish_id: str,
    fallback_record: dict[str, Any],
    session_name: str,
    patch: dict[str, Any],
    expected_request_id: str = "",
) -> dict[str, Any]:
    with advisory_lock(_safe_lock_name("chat-publish", publish_id)):
        current = load_chat_publish(publish_id) or copy.deepcopy(fallback_record)
        peer_delivery = _peer_delivery_payload(current)
        target_entry = next(
            (item for item in peer_delivery.get("targets", []) if str(item.get("session_name", "")).strip() == session_name),
            None,
        )
        if expected_request_id and str((target_entry or {}).get("request_id", "")).strip() != expected_request_id:
            return current
        _update_peer_target(peer_delivery, session_name, patch)
        current["peer_delivery"] = peer_delivery
        return _save_chat_publish_record(current)


def _record_peer_async_acceptance(
    *,
    publish_id: str,
    fallback_record: dict[str, Any],
    session_name: str,
    accepted: dict[str, Any],
) -> dict[str, Any]:
    return _patch_peer_delivery_target(
        publish_id=publish_id,
        fallback_record=fallback_record,
        session_name=session_name,
        patch={
            "status": "awaiting_result",
            "request_id": str(accepted.get("request_id", "")).strip(),
            "event_cursor": str(accepted.get("event_cursor", "")).strip(),
            "intent": str(accepted.get("intent", "default")).strip() or "default",
            "response": accepted.get("response") if isinstance(accepted.get("response"), dict) else {},
        },
    )


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
    launch_record = None
    if str(binding.get("publish_route_kind", "")).strip() == "room_launch":
        launch_id = str(source_meta.get("launch_id", "")).strip() or str(record.get("launch_id", "")).strip()
        if launch_id:
            launch_record = load_room_launch(launch_id) or {}
    source_delivery_selector = source_session_name
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
            session_lookup = _binding_session_lookup(binding, launch=launch_record)
            if str(binding.get("publish_route_kind", "")).strip() == "room_launch" and launch_record:
                source_handle = room_launch_participant_handle_for_session(
                    launch_record,
                    session_name=source_session_name,
                    session_id=source_session_id,
                )
                if not source_handle:
                    current_selector = current_session_selector()
                    if current_selector:
                        source_handle = room_launch_participant_handle_for_session(
                            launch_record,
                            session_name=current_selector,
                            session_id=current_selector,
                        )
                if not source_handle:
                    peer_delivery["phase"] = "peer_fanout_complete"
                    peer_delivery["status"] = "skipped_source_not_thread_participant"
                    current["peer_delivery"] = peer_delivery
                    return _save_chat_publish_record(current)
                source_participant = room_launch_participant(launch_record, source_handle)
                source_delivery_selector = room_launch_participant_delivery_selector(source_participant)
                source_session_name = (
                    source_session_name
                    or str(source_participant.get("session_name", "")).strip()
                    or source_delivery_selector
                )
                source_session_id = source_session_id or str(source_participant.get("session_id", "")).strip()
                current["source_session_name"] = source_session_name
                current["source_session_id"] = source_session_id
                current["source_qualified_handle"] = source_handle
                current = _save_chat_publish_record(current)
            if not source_session_name or (not source_delivery_selector and source_session_name not in session_lookup):
                peer_delivery["phase"] = "peer_fanout_partial_failure"
                peer_delivery["status"] = "failed_source_not_bound"
                current["peer_delivery"] = peer_delivery
                return _save_chat_publish_record(current)
            if source_delivery_selector and not launch_record and source_delivery_selector not in session_lookup.values():
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
                source_session_id=source_session_id,
                source_event_kind=source_meta.get("source_event_kind", ""),
                launch=launch_record,
            )
            peer_delivery["delivery"] = delivery
            peer_delivery["mentioned_session_names"] = mentions
            if resolve_reason:
                peer_delivery["phase"] = "peer_fanout_partial_failure" if resolve_reason.startswith("failed_") else "peer_fanout_complete"
                peer_delivery["status"] = resolve_reason
                current["peer_delivery"] = peer_delivery
                return _save_chat_publish_record(current)

            # Peer delivery also targets the configured selectors directly so
            # sleeping or reserved named sessions still receive the publication.
            resolved_targets: list[tuple[str, str]] = []
            for session_name in targets:
                target_key = str(session_name).strip()
                if not target_key:
                    continue
                delivery_selector = target_key
                if launch_record:
                    resolved_target_selector, participant, _identity = resolve_room_launch_target_selector(launch_record, target_key)
                    delivery_selector = (
                        str(resolved_target_selector).strip()
                        or str((participant or {}).get("delivery_selector", "")).strip()
                        or str((participant or {}).get("session_name", "")).strip()
                        or str((participant or {}).get("session_id", "")).strip()
                        or str((participant or {}).get("session_alias", "")).strip()
                        or target_key
                    )
                resolved_targets.append((target_key, delivery_selector))

            if not resolved_targets:
                peer_delivery["phase"] = "peer_fanout_complete"
                peer_delivery["status"] = "skipped_no_peer_targets"
                current["peer_delivery"] = peer_delivery
                return _save_chat_publish_record(current)

            total_peer_deliveries = _count_root_peer_deliveries_from_index(binding_id, root_ingress_receipt_id)
            projected = total_peer_deliveries + len(resolved_targets)
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

            peer_delivery["frozen_targets"] = [delivery_selector for _target_key, delivery_selector in resolved_targets]
            peer_delivery["phase"] = "peer_fanout_in_progress"
            for target_key, delivery_selector in resolved_targets:
                _update_peer_target(
                    peer_delivery,
                    target_key,
                    {
                        "status": "pending",
                        "attempt_count": 0,
                        "attempted_at": "",
                        "delivery_selector": delivery_selector,
                        "attempts": [],
                    },
                )
            current["peer_delivery"] = peer_delivery
            current = _save_chat_publish_record(current)
            delivery_targets = list(resolved_targets)
            delivery_mode = delivery
            delivery_mentions = list(mentions)

    for target_key, delivery_selector in delivery_targets:
        idempotency_key = f"peer_publish:{publish_id}:binding:{binding_id}:target:{target_key}"
        current, _ = _update_target_in_progress(
            publish_id=publish_id,
            fallback_record=current,
            session_name=target_key,
            idempotency_key=idempotency_key,
        )
        envelope = _build_peer_envelope(
            binding=binding,
            record=current,
            source_session_name=source_session_name,
            source_session_id=source_session_id,
            target_session_name=delivery_selector,
            delivery=delivery_mode,
            mentioned_session_names=delivery_mentions,
            root_ingress_receipt_id=root_ingress_receipt_id,
            idempotency_key=idempotency_key,
            launch=launch_record,
        )

        def record_async_acceptance(accepted: dict[str, Any], session_name: str = target_key) -> None:
            nonlocal current
            current = _record_peer_async_acceptance(
                publish_id=publish_id,
                fallback_record=current,
                session_name=session_name,
                accepted=accepted,
            )

        def record_async_terminal(evidence: dict[str, Any], session_name: str = target_key) -> None:
            nonlocal current
            request_id = str(
                next(
                    (
                        item.get("request_id", "")
                        for item in _peer_delivery_payload(current).get("targets", [])
                        if str(item.get("session_name", "")).strip() == session_name
                    ),
                    "",
                )
            ).strip()
            terminal_status = "delivered" if str(evidence.get("status", "")).strip() == "succeeded" else "failed_retryable"
            current = _patch_peer_delivery_target(
                publish_id=publish_id,
                fallback_record=current,
                session_name=session_name,
                expected_request_id=request_id,
                patch={"status": terminal_status, "terminal_evidence": evidence},
            )

        try:
            response = deliver_session_message(
                delivery_selector,
                envelope,
                idempotency_key=idempotency_key,
                timeout=PEER_DELIVERY_TIMEOUT_SECONDS,
                async_timeout=PEER_DELIVERY_TIMEOUT_SECONDS,
                on_async_accepted=record_async_acceptance,
                on_async_terminal=record_async_terminal,
            )
        except GCAPIResultUnknown as exc:
            peer_delivery = _peer_delivery_payload(current)
            target_entry = next(
                (item for item in peer_delivery.get("targets", []) if str(item.get("session_name", "")).strip() == target_key),
                {},
            )
            target_status = "awaiting_result" if str(target_entry.get("request_id", "")).strip() else "delivery_unknown"
            current = _patch_peer_delivery_target(
                publish_id=publish_id,
                fallback_record=current,
                session_name=target_key,
                patch={"status": target_status, "reason": str(exc)},
            )
            continue
        except GCAPIError as exc:
            current = _update_target_delivery_result(
                publish_id=publish_id,
                fallback_record=current,
                session_name=target_key,
                error=str(exc),
            )
            continue
        current = _update_target_delivery_result(
            publish_id=publish_id,
            fallback_record=current,
            session_name=target_key,
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
    launch_record = None
    launch_id = str(record.get("launch_id", "")).strip()
    if launch_id:
        launch_record = load_room_launch(launch_id)
    retry_targets: list[tuple[str, str, str, str, list[str], str, str, str]] = []
    resume_targets: list[tuple[str, str, str, str]] = []
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
            target_selector = str(entry.get("delivery_selector", "")).strip() or session_name
            if launch_record:
                resolved_name, _participant, _identity = resolve_room_launch_target_selector(launch_record, session_name)
                if resolved_name:
                    target_selector = resolved_name
            if selected and session_name not in selected:
                continue
            entry_status = str(entry.get("status", "")).strip()
            request_id = str(entry.get("request_id", "")).strip()
            event_cursor = str(entry.get("event_cursor", "")).strip()
            intent = str(entry.get("intent", "default")).strip() or "default"
            if entry_status == "awaiting_result" or (
                include_unknown and entry_status == "delivery_unknown" and request_id and event_cursor
            ):
                if request_id and event_cursor:
                    resume_targets.append((session_name, request_id, event_cursor, intent))
                continue
            if entry_status not in eligible:
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
                    "delivery_selector": target_selector,
                    "attempts": attempts,
                    "request_id": "",
                    "event_cursor": "",
                    "response": {},
                    "terminal_evidence": {},
                    "reason": "",
                },
            )
            retry_targets.append(
                (
                    session_name,
                    target_selector,
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

    for session_name, request_id, event_cursor, intent in resume_targets:
        try:
            terminal_payload = resume_session_message_delivery(
                request_id,
                event_cursor,
                intent=intent,
                timeout=PEER_DELIVERY_TIMEOUT_SECONDS,
            )
        except GCAPIResultUnknown as exc:
            current = _patch_peer_delivery_target(
                publish_id=publish_id,
                fallback_record=current,
                session_name=session_name,
                expected_request_id=request_id,
                patch={"status": "awaiting_result", "reason": str(exc)},
            )
        except GCAPIRequestFailed as exc:
            current = _patch_peer_delivery_target(
                publish_id=publish_id,
                fallback_record=current,
                session_name=session_name,
                expected_request_id=request_id,
                patch={
                    "status": "failed_retryable",
                    "reason": str(exc),
                    "terminal_evidence": {"status": "failed", "payload": exc.payload},
                },
            )
        except GCAPIError as exc:
            current = _patch_peer_delivery_target(
                publish_id=publish_id,
                fallback_record=current,
                session_name=session_name,
                expected_request_id=request_id,
                patch={"status": "failed_retryable", "reason": str(exc)},
            )
        else:
            current = _patch_peer_delivery_target(
                publish_id=publish_id,
                fallback_record=current,
                session_name=session_name,
                expected_request_id=request_id,
                patch={
                    "status": "delivered",
                    "delivered_at": utcnow(),
                    "terminal_evidence": {"status": "succeeded", "payload": terminal_payload},
                },
            )

    for session_name, target_selector, idempotency_key, delivery, mentioned_session_names, root_ingress_receipt_id, source_session_name, source_session_id in retry_targets:
        envelope = _build_peer_envelope(
            binding=binding,
            record=current,
            source_session_name=source_session_name,
            source_session_id=source_session_id,
            target_session_name=target_selector,
            delivery=delivery,
            mentioned_session_names=mentioned_session_names,
            root_ingress_receipt_id=root_ingress_receipt_id,
            idempotency_key=idempotency_key,
            launch=launch_record,
        )

        def record_retry_async_acceptance(accepted: dict[str, Any], retry_session_name: str = session_name) -> None:
            nonlocal current
            current = _record_peer_async_acceptance(
                publish_id=publish_id,
                fallback_record=current,
                session_name=retry_session_name,
                accepted=accepted,
            )

        def record_retry_async_terminal(evidence: dict[str, Any], retry_session_name: str = session_name) -> None:
            nonlocal current
            peer_delivery = _peer_delivery_payload(current)
            target_entry = next(
                (
                    item
                    for item in peer_delivery.get("targets", [])
                    if str(item.get("session_name", "")).strip() == retry_session_name
                ),
                {},
            )
            request_id = str(target_entry.get("request_id", "")).strip()
            terminal_status = "delivered" if str(evidence.get("status", "")).strip() == "succeeded" else "failed_retryable"
            current = _patch_peer_delivery_target(
                publish_id=publish_id,
                fallback_record=current,
                session_name=retry_session_name,
                expected_request_id=request_id,
                patch={"status": terminal_status, "terminal_evidence": evidence},
            )

        try:
            response = deliver_session_message(
                target_selector,
                envelope,
                idempotency_key=idempotency_key,
                timeout=PEER_DELIVERY_TIMEOUT_SECONDS,
                async_timeout=PEER_DELIVERY_TIMEOUT_SECONDS,
                on_async_accepted=record_retry_async_acceptance,
                on_async_terminal=record_retry_async_terminal,
            )
        except GCAPIResultUnknown as exc:
            peer_delivery = _peer_delivery_payload(current)
            target_entry = next(
                (item for item in peer_delivery.get("targets", []) if str(item.get("session_name", "")).strip() == session_name),
                {},
            )
            target_status = "awaiting_result" if str(target_entry.get("request_id", "")).strip() else "delivery_unknown"
            current = _patch_peer_delivery_target(
                publish_id=publish_id,
                fallback_record=current,
                session_name=session_name,
                patch={"status": target_status, "reason": str(exc)},
            )
            continue
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
    app_name = validate_app_name(binding.get("app", ""))
    bot_token: str | None = None
    if app_name:
        bot_token = load_bot_token(app_name)
        if not bot_token:
            raise DiscordAPIError(f"Discord bot token is not configured for app {app_name!r}")
    if str(binding.get("kind", "")).strip() == "room":
        config = load_config()
        policy = resolve_app_policy(config, app_name)
        has_outbound_policy = bool(
            _normalize_allowlist(policy.get("guild_allowlist"))
            or _normalize_allowlist(policy.get("channel_allowlist"))
        )
        if has_outbound_policy:
            policy_token = bot_token or load_bot_token()
            if not policy_token:
                display_name = app_name or "default"
                raise DiscordAPIError(f"Discord bot token is not configured for app {display_name!r}")
            binding_conversation_id = str(binding.get("conversation_id", "")).strip()
            channel_scope = describe_room_channel_scope(binding_conversation_id, bot_token=policy_token)
            actual_guild_id = str(channel_scope.get("guild_id", "")).strip()
            parent_channel_id = (
                str(channel_scope.get("thread_parent_id", "")).strip()
                or binding_conversation_id
            )
            policy_rejection = outbound_policy_reason(
                config,
                actual_guild_id,
                parent_channel_id,
                app_name=app_name,
            )
            if policy_rejection:
                display_name = app_name or "default"
                raise ValueError(f"Discord app {display_name!r} policy rejects publish: {policy_rejection}")
    conversation_id, reply_target, launch = resolve_publish_destination(
        binding,
        requested_conversation_id=requested_conversation_id,
        trigger_id=trigger_id,
        reply_to_message_id=reply_to_message_id,
        source_context=source_context,
        bot_token=bot_token,
    )
    if not conversation_id:
        raise ValueError("binding is missing a destination conversation_id")
    if app_name:
        response = post_channel_message(
            conversation_id,
            body,
            reply_to_message_id=reply_target,
            bot_token=bot_token,
        )
    else:
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
    effective_source_qualified_handle = room_launch_participant_handle_for_session(
        launch or {},
        session_name=effective_source_session_name,
        session_id=effective_source_session_id,
    )
    record = save_chat_publish(
        {
            "binding_id": str(binding.get("id", "")).strip(),
            "binding_kind": str(binding.get("kind", "")).strip(),
            "binding_conversation_id": str(binding.get("conversation_id", "")).strip(),
            "app": app_name,
            "conversation_id": conversation_id,
            "guild_id": str(binding.get("guild_id", "")).strip(),
            "trigger_id": str(trigger_id).strip(),
            "reply_to_message_id": reply_target,
            "source_session_id": effective_source_session_id,
            "source_session_name": effective_source_session_name,
            "source_qualified_handle": effective_source_qualified_handle,
            "source_event_kind": source_meta.get("source_event_kind", ""),
            "root_ingress_receipt_id": source_meta.get("root_ingress_receipt_id", ""),
            "launch_id": source_meta.get("launch_id", ""),
            "launch_thread_id": str((launch or {}).get("thread_id", "")).strip(),
            "body": body,
            "remote_message_id": remote_message_id,
        }
    )
    launch_record = launch
    if launch_record is None and str(source_meta.get("launch_id", "")).strip():
        launch_record = load_room_launch(str(source_meta.get("launch_id", "")).strip())
    launch_thread_id = str((launch_record or {}).get("thread_id", "")).strip()
    launch_record_id = str((launch_record or {}).get("launch_id", "")).strip() or str(source_meta.get("launch_id", "")).strip()
    if launch_record_id and launch_thread_id and conversation_id == launch_thread_id:
        record_room_launch_message_target(
            launch_record_id,
            remote_message_id,
            source_session_name=effective_source_session_name,
            source_session_id=effective_source_session_id,
        )
    # Peer notification is now handled by the extmsg outbound orchestrator
    # via transcript membership — no pack-side fanout needed.
    return {"binding": binding, "record": record, "response": response}


def resume_session_message_delivery(
    request_id: str,
    event_cursor: str,
    *,
    intent: str = "default",
    timeout: float = GC_API_ASYNC_RESULT_TIMEOUT_SECONDS,
    cancel_event: threading.Event | None = None,
) -> dict[str, Any]:
    normalized_intent = str(intent or "default").strip() or "default"
    if normalized_intent == "default":
        success_type = GC_EVENT_SESSION_MESSAGE_SUCCEEDED
        failure_operation = GC_OPERATION_SESSION_MESSAGE
    else:
        success_type = GC_EVENT_SESSION_SUBMIT_SUCCEEDED
        failure_operation = GC_OPERATION_SESSION_SUBMIT
    return wait_for_gc_request_result(
        request_id,
        event_cursor=event_cursor,
        success_type=success_type,
        failure_operation=failure_operation,
        timeout=timeout,
        cancel_event=cancel_event,
    )


def deliver_session_message(
    session_name: str,
    message: str,
    idempotency_key: str = "",
    timeout: float = GC_API_REQUEST_TIMEOUT_SECONDS,
    *,
    intent: str = "default",
    async_timeout: float = GC_API_ASYNC_RESULT_TIMEOUT_SECONDS,
    cancel_event: threading.Event | None = None,
    on_async_accepted: Callable[[dict[str, Any]], None] | None = None,
    on_async_terminal: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    if cancel_event is not None and cancel_event.is_set():
        raise GCAPIRequestCancelled("session delivery was cancelled before submission")
    headers: dict[str, str] = {}
    key = str(idempotency_key).strip()
    if key:
        headers["Idempotency-Key"] = key
    normalized_intent = str(intent or "default").strip() or "default"
    path = f"/v0/session/{urllib.parse.quote(str(session_name).strip(), safe='')}/messages"
    payload_body: dict[str, Any] = {"message": message}
    if normalized_intent != "default":
        path = f"/v0/session/{urllib.parse.quote(str(session_name).strip(), safe='')}/submit"
        payload_body["intent"] = normalized_intent
    try:
        status_code, payload = gc_api_request_with_status(
            "POST",
            path,
            payload=payload_body,
            headers=headers,
            timeout=timeout,
        )
    except GCAPITransportError as exc:
        raise GCAPIResultUnknown(f"session delivery outcome is unknown: {exc}") from exc
    if not isinstance(payload, dict):
        if status_code == 202:
            raise GCAPIResultUnknown("gc async HTTP 202 response requires request_id and event_cursor")
        return {}
    if status_code != 202:
        return payload
    request_id = str(payload.get("request_id", "")).strip()
    event_cursor = str(payload.get("event_cursor", "")).strip()
    if not request_id or not event_cursor:
        raise GCAPIResultUnknown("gc async HTTP 202 response requires request_id and event_cursor")
    accepted_request = {
        "http_status": status_code,
        "request_id": request_id,
        "event_cursor": event_cursor,
        "intent": normalized_intent,
        "response": payload,
    }
    if on_async_accepted is not None:
        on_async_accepted(accepted_request)
    try:
        terminal_payload = resume_session_message_delivery(
            request_id,
            event_cursor=event_cursor,
            intent=normalized_intent,
            timeout=async_timeout,
            cancel_event=cancel_event,
        )
    except GCAPIRequestFailed as exc:
        if on_async_terminal is not None:
            on_async_terminal({"status": "failed", "payload": exc.payload})
        raise
    if on_async_terminal is not None:
        on_async_terminal({"status": "succeeded", "payload": terminal_payload})
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


def post_channel_message(
    channel_id: str,
    body: str,
    reply_to_message_id: str = "",
    *,
    bot_token: str | None = None,
) -> Any:
    payload: dict[str, Any] = {
        "content": body,
        "allowed_mentions": {"parse": ["users"]},
    }
    reply_to_message_id = str(reply_to_message_id).strip()
    if reply_to_message_id:
        payload["message_reference"] = {
            "channel_id": str(channel_id),
            "message_id": reply_to_message_id,
            "fail_if_not_exists": False,
        }
    path = f"/channels/{urllib.parse.quote(str(channel_id))}/messages"
    if bot_token is None:
        return discord_api_request("POST", path, payload=payload)
    return discord_api_request("POST", path, payload=payload, bot_token=bot_token)


def discord_jump_url(guild_id: str, conversation_id: str) -> str:
    if not guild_id or not conversation_id:
        return ""
    if not str(guild_id).isdigit() or not str(conversation_id).isdigit():
        return ""
    return f"https://discord.com/channels/{guild_id}/{conversation_id}"


def policy_reason(
    config: dict[str, Any],
    guild_id: str,
    parent_channel_id: str,
    role_ids: list[str],
    *,
    app_name: str = "",
) -> str:
    policy = resolve_app_policy(config, app_name)
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


def outbound_policy_reason(
    config: dict[str, Any],
    guild_id: str,
    parent_channel_id: str,
    *,
    app_name: str = "",
) -> str:
    policy = resolve_app_policy(config, app_name)
    guild_allowlist = set(_normalize_allowlist(policy.get("guild_allowlist")))
    channel_allowlist = set(_normalize_allowlist(policy.get("channel_allowlist")))
    if guild_allowlist and str(guild_id).strip() not in guild_allowlist:
        return "guild_not_allowed"
    if channel_allowlist and str(parent_channel_id).strip() not in channel_allowlist:
        return "channel_not_allowed"
    return ""
