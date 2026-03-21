from __future__ import annotations

import base64
import copy
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
        config["chat"] = {
            "bindings": normalized_bindings,
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
    cfg.setdefault("chat", {})
    cfg["chat"].setdefault("bindings", {})
    cfg["chat"]["bindings"][binding_id] = {
        "id": binding_id,
        "kind": normalized_kind,
        "conversation_id": normalized_conversation,
        "guild_id": str(guild_id).strip(),
        "session_names": normalized_session_names,
    }
    return save_config(cfg)


def resolve_chat_binding(config: dict[str, Any], binding_id: str) -> dict[str, Any] | None:
    bindings = normalize_config(config).get("chat", {}).get("bindings", {})
    return bindings.get(str(binding_id).strip())


def list_chat_bindings(config: dict[str, Any]) -> list[dict[str, Any]]:
    bindings = normalize_config(config).get("chat", {}).get("bindings", {})
    return sorted(bindings.values(), key=lambda item: (str(item.get("kind", "")), str(item.get("conversation_id", ""))))


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
    return body


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
        "recent_chat_ingress": [redact_chat_ingress_record(item) for item in list_recent_chat_ingress(limit=limit)],
        "recent_chat_publishes": [redact_chat_publish_record(item) for item in list_recent_chat_publishes(limit=limit)],
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
    for key in ("GC_SESSION_ID", "GC_SESSION_NAME"):
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


def _session_record_preference(item: dict[str, Any]) -> tuple[int, int, int, str]:
    state = str(item.get("state", "")).strip()
    return (
        0 if state in NON_ROUTABLE_SESSION_STATES else 1,
        1 if item.get("running") is True else 0,
        1 if item.get("attached") is True else 0,
        str(item.get("created_at", "")).strip(),
    )


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


def publish_binding_message(
    binding: dict[str, Any],
    body: str,
    *,
    requested_conversation_id: str = "",
    trigger_id: str = "",
    reply_to_message_id: str = "",
) -> dict[str, Any]:
    conversation_id = resolve_publish_conversation_id(binding, requested_conversation_id)
    if not conversation_id:
        raise ValueError("binding is missing a destination conversation_id")
    reply_target = str(reply_to_message_id).strip() or str(trigger_id).strip()
    response = post_channel_message(
        conversation_id,
        body,
        reply_to_message_id=reply_target,
    )
    remote_message_id = str((response or {}).get("id", "")).strip()
    if not remote_message_id:
        raise DiscordAPIError("discord publish returned no message id")
    record = save_chat_publish(
        {
            "binding_id": str(binding.get("id", "")).strip(),
            "binding_kind": str(binding.get("kind", "")).strip(),
            "binding_conversation_id": str(binding.get("conversation_id", "")).strip(),
            "conversation_id": conversation_id,
            "guild_id": str(binding.get("guild_id", "")).strip(),
            "trigger_id": str(trigger_id).strip(),
            "reply_to_message_id": reply_target,
            "source_session_id": str(os.environ.get("GC_SESSION_ID", "")).strip(),
            "source_session_name": str(os.environ.get("GC_SESSION_NAME", "")).strip(),
            "body": body,
            "remote_message_id": remote_message_id,
        }
    )
    return {"binding": binding, "record": record, "response": response}


def deliver_session_message(session_name: str, message: str, idempotency_key: str = "") -> dict[str, Any]:
    headers: dict[str, str] = {}
    key = str(idempotency_key).strip()
    if key:
        headers["Idempotency-Key"] = key
    payload = gc_api_request(
        "POST",
        f"/v0/session/{urllib.parse.quote(str(session_name).strip(), safe='')}/messages",
        payload={"message": message},
        headers=headers,
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
