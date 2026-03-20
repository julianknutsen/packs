from __future__ import annotations

import base64
import copy
import hashlib
import json
import os
import pathlib
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

INTERACTIONS_SERVICE_NAME = "discord-interactions"
ADMIN_SERVICE_NAME = "discord-admin"
SCHEMA_VERSION = 1
DISCORD_API_BASE = os.environ.get("GC_DISCORD_API_BASE", "https://discord.com/api/v10")
REQUEST_RETENTION_SECONDS = 24 * 60 * 60
PENDING_MODAL_RETENTION_SECONDS = 15 * 60
COMMAND_NAME_DEFAULT = "gc"
FIX_FORMULA_DEFAULT = "mol-discord-fix-issue"
ED25519_SPKI_PREFIX = bytes.fromhex("302a300506032b6570032100")


class DiscordAPIError(RuntimeError):
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
        return ".gc/services/discord-intake"
    return os.path.join(root, ".gc", "services", "discord-intake")


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


def ensure_layout() -> None:
    for path in (data_dir(), requests_dir(), receipts_dir(), workflows_dir(), pending_modals_dir(), secrets_dir()):
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


def read_json(path: str, default: Any = None) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError:
        return default


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
    }


def _normalize_allowlist(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    return [str(value).strip() for value in values if str(value).strip()]


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


def set_channel_mapping(
    config: dict[str, Any],
    guild_id: str,
    channel_id: str,
    target: str,
    fix_formula: str | None,
) -> dict[str, Any]:
    cfg = normalize_config(config)
    key = normalize_channel_key(guild_id, channel_id)
    cfg["channels"][key] = {
        "guild_id": str(guild_id).strip(),
        "channel_id": str(channel_id).strip(),
        "target": str(target).strip(),
        "commands": {
            "fix": {
                "formula": str(fix_formula or FIX_FORMULA_DEFAULT).strip() or FIX_FORMULA_DEFAULT,
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
    key = normalize_rig_key(guild_id, rig_name)
    cfg["rigs"][key] = {
        "guild_id": str(guild_id).strip(),
        "rig_name": str(rig_name).strip(),
        "target": str(target).strip(),
        "commands": {
            "fix": {
                "formula": str(fix_formula or FIX_FORMULA_DEFAULT).strip() or FIX_FORMULA_DEFAULT,
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
        except DiscordAPIError:
            parent_channel_id = ""
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


def load_request(request_id: str) -> dict[str, Any] | None:
    data = read_json(request_path(request_id))
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
    data = read_json(receipt_path(interaction_id))
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
    data = read_json(workflow_path(workflow_key))
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


def save_pending_modal(payload: dict[str, Any]) -> dict[str, Any]:
    ensure_layout()
    body = copy.deepcopy(payload)
    body.setdefault("created_at", utcnow())
    atomic_write_json(pending_modal_path(str(body["nonce"])), body)
    return body


def load_pending_modal(nonce: str) -> dict[str, Any] | None:
    data = read_json(pending_modal_path(nonce))
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
        data = read_json(str(path))
        if isinstance(data, dict):
            entries.append(data)
    entries.sort(key=lambda item: item.get("created_at", ""), reverse=True)
    return entries[:limit]


def published_service_snapshot(service_name: str) -> dict[str, Any]:
    path = os.path.join(published_services_dir(), f"{service_name}.json")
    snapshot = read_json(path, {})
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
        "city_root": city_root(),
        "state_root": state_root(),
        "secrets_dir": secrets_dir(),
        "admin_url": admin_url(),
        "interactions_url": interactions_url(),
        "published_services_dir": published_services_dir(),
        "config": redact_config(config),
        "recent_requests": list_recent_requests(limit=limit),
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
        "User-Agent": "gas-city-discord-intake/0.1",
    }
    token = bot_token or load_bot_token()
    if token:
        headers["Authorization"] = f"Bot {token}"
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=body, headers=headers, method=method.upper())
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            raw = response.read()
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        message = raw.decode("utf-8", errors="replace")
        raise DiscordAPIError(f"{method.upper()} {url} failed with {exc.code}: {message}") from exc
    except urllib.error.URLError as exc:
        raise DiscordAPIError(f"{method.upper()} {url} failed: {exc}") from exc
    if not raw:
        return {}
    try:
        return json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise DiscordAPIError(f"{method.upper()} {url} returned invalid JSON") from exc


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
                        "required": True,
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


def post_channel_message(channel_id: str, body: str) -> Any:
    return discord_api_request(
        "POST",
        f"/channels/{urllib.parse.quote(str(channel_id))}/messages",
        payload={
            "content": body,
            "allowed_mentions": {"parse": []},
        },
    )


def discord_jump_url(guild_id: str, conversation_id: str) -> str:
    if not guild_id or not conversation_id:
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
