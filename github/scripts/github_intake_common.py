from __future__ import annotations

import base64
import copy
import hashlib
import hmac
import json
import os
import pathlib
import re
import shlex
import subprocess
import tempfile
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from typing import Any

WEBHOOK_SERVICE_NAME = "github-webhook"
ADMIN_SERVICE_NAME = "github-admin"
SCHEMA_VERSION = 1
GITHUB_API_BASE = os.environ.get("GC_GITHUB_API_BASE", "https://api.github.com")
GITHUB_API_VERSION = os.environ.get("GC_GITHUB_API_VERSION", "2026-03-10")
GITHUB_APP_IDENTITY_SCHEMA_VERSION = "github-intake.github-app-identity.v1"
GITHUB_APP_IDENTITY_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


class GitHubAPIError(RuntimeError):
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
        return ".gc/services/github"
    return os.path.join(root, ".gc", "services", "github")


def data_dir() -> str:
    return os.path.join(state_root(), "data")


def requests_dir() -> str:
    return os.path.join(data_dir(), "requests")


def deliveries_dir() -> str:
    return os.path.join(data_dir(), "deliveries")


def workflows_dir() -> str:
    return os.path.join(data_dir(), "workflows")


def rule_results_dir() -> str:
    return os.path.join(data_dir(), "rule-results")


def address_results_dir() -> str:
    return os.path.join(data_dir(), "address-results")


def config_path() -> str:
    return os.path.join(data_dir(), "config.json")


def rules_path() -> str:
    override = os.environ.get("GC_GITHUB_INTAKE_RULES_FILE")
    if override:
        return os.path.expanduser(override)
    root = city_root()
    if not root:
        return "config/github-intake/rules.toml"
    return os.path.join(root, "config", "github-intake", "rules.toml")


def published_services_dir() -> str:
    value = os.environ.get("GC_PUBLISHED_SERVICES_DIR")
    if value:
        return value
    root = city_root()
    if not root:
        return ".gc/services/.published"
    return os.path.join(root, ".gc", "services", ".published")


def ensure_layout() -> None:
    for path in (data_dir(), requests_dir(), deliveries_dir(), workflows_dir(), rule_results_dir(), address_results_dir()):
        os.makedirs(path, exist_ok=True)


def atomic_write_json(path: str, payload: dict[str, Any], mode: int = 0o640) -> None:
    parent = os.path.dirname(path)
    os.makedirs(parent, exist_ok=True)
    data = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8") + b"\n"
    with tempfile.NamedTemporaryFile(dir=parent, delete=False) as tmp:
        tmp.write(data)
        tmp.flush()
        os.fchmod(tmp.fileno(), mode)
        tmp_path = tmp.name
    os.replace(tmp_path, path)


def read_json(path: str, default: Any = None) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError:
        return default


def default_config() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "app": {},
        "repositories": {},
    }


def normalize_config(raw: dict[str, Any] | None) -> dict[str, Any]:
    cfg = default_config()
    if not raw:
        return cfg
    if isinstance(raw.get("app"), dict):
        cfg["app"] = copy.deepcopy(raw["app"])
    repositories = raw.get("repositories")
    if isinstance(repositories, dict):
        cfg["repositories"] = copy.deepcopy(repositories)
    cfg["schema_version"] = SCHEMA_VERSION
    return cfg


ENV_APP_FIELDS = {
    "GITHUB_APP_ID": "app_id",
    "GITHUB_APP_CLIENT_ID": "client_id",
    "GITHUB_APP_CLIENT_SECRET": "client_secret",
    "GITHUB_APP_WEBHOOK_SECRET": "webhook_secret",
    "GITHUB_WEBHOOK_SECRET": "webhook_secret",
    "GITHUB_APP_PRIVATE_KEY_PEM": "private_key_pem",
    "GITHUB_APP_PRIVATE_KEY": "private_key_pem",
    "GITHUB_APP_SLUG": "slug",
    "GITHUB_APP_HTML_URL": "html_url",
    "GITHUB_APP_NAME": "name",
    "GITHUB_INSTALLATION_ID": "installation_id",
}


def app_config_from_env() -> dict[str, str]:
    app: dict[str, str] = {}
    for env_key, app_key in ENV_APP_FIELDS.items():
        value = os.environ.get(env_key)
        if value:
            app[app_key] = value
    return app


def effective_config(config: dict[str, Any] | None) -> dict[str, Any]:
    cfg = normalize_config(config)
    env_app = app_config_from_env()
    if env_app:
        app = cfg.setdefault("app", {})
        if isinstance(app, dict):
            app.update(env_app)
    return cfg


def load_config() -> dict[str, Any]:
    ensure_layout()
    return normalize_config(read_json(config_path(), {}))


def load_effective_config() -> dict[str, Any]:
    return effective_config(load_config())


def save_config(config: dict[str, Any]) -> dict[str, Any]:
    ensure_layout()
    normalized = normalize_config(config)
    atomic_write_json(config_path(), normalized)
    return normalized


def redact_config(config: dict[str, Any]) -> dict[str, Any]:
    redacted = normalize_config(config)
    app = redacted.get("app", {})
    if isinstance(app, dict):
        if app.pop("private_key_pem", None):
            app["private_key_pem_present"] = True
        if app.pop("webhook_secret", None):
            app["webhook_secret_present"] = True
        if app.pop("client_secret", None):
            app["client_secret_present"] = True
    return redacted


def import_app_config(config: dict[str, Any], app_fields: dict[str, Any]) -> dict[str, Any]:
    cfg = normalize_config(config)
    app = cfg.setdefault("app", {})
    raw_id = app_fields.get("app_id", app_fields.get("id"))
    if raw_id is not None and raw_id != "":
        app["app_id"] = str(raw_id)
    for key in (
        "client_id",
        "client_secret",
        "webhook_secret",
        "slug",
        "html_url",
        "name",
        "installation_id",
    ):
        value = app_fields.get(key)
        if value:
            app[key] = value
    pem = app_fields.get("private_key_pem", app_fields.get("pem"))
    if pem:
        app["private_key_pem"] = pem
    owner = app_fields.get("owner")
    if owner:
        app["owner"] = owner
    return save_config(cfg)


def normalize_repo_key(value: str) -> str:
    return value.strip().lower()


def validate_github_app_identity(value: str, field: str = "github_app_identity") -> str:
    identity = str(value).strip()
    if not identity:
        return ""
    if not GITHUB_APP_IDENTITY_PATTERN.fullmatch(identity):
        raise ValueError(
            f"{field} must match [A-Za-z0-9][A-Za-z0-9._:-]{{0,127}}; got {identity!r}"
        )
    return identity


def github_app_config_for_identity(identity: str = "") -> dict[str, Any]:
    """Resolve a named App identity, or fall back to the intake App config.

    Delivery commands use this boundary so their broader repository write
    permissions can live on a separate App without widening the intake App.
    """
    identity = validate_github_app_identity(identity)
    if identity:
        # Imported lazily to keep the common module usable by the service that
        # implements the resolver protocol.
        import github_intake_service as service

        return service.load_profile_github_app(identity)
    config = load_effective_config()
    app_cfg = config.get("app", {})
    if isinstance(app_cfg, dict):
        return app_cfg
    return {}


def publish_identity(
    app_fields: dict[str, Any],
    identity: str = "",
    publisher: str = "",
    cwd: str = "",
) -> dict[str, Any]:
    """Push a GitHub App identity to the deployment secret store.

    Write-side mirror of the identity resolver: runs the command configured
    via GITHUB_INTAKE_IDENTITY_PUBLISHER with the identity name as its only
    argument and the identity JSON document on stdin. The publisher is a
    deployment plug — a no-op when unset — and failures are reported in the
    returned status, never raised, so credential capture (manifest callback,
    manual import) cannot be broken by a misbehaving store.
    """
    publisher = publisher.strip() or os.environ.get("GITHUB_INTAKE_IDENTITY_PUBLISHER", "").strip()
    if not publisher:
        return {"status": "skipped", "reason": "no publisher configured"}
    identity = identity.strip() or os.environ.get("GITHUB_INTAKE_APP_IDENTITY", "").strip()
    if not identity:
        return {"status": "skipped", "reason": "GITHUB_INTAKE_APP_IDENTITY is not configured"}
    try:
        identity = validate_github_app_identity(identity, "GITHUB_INTAKE_APP_IDENTITY")
    except ValueError as exc:
        return {"status": "error", "detail": str(exc)}
    command = shlex.split(publisher)
    if not command:
        return {"status": "error", "detail": "GITHUB_INTAKE_IDENTITY_PUBLISHER did not contain a command"}
    payload = {
        str(key): str(value)
        for key, value in app_fields.items()
        if value is not None and str(value) != ""
    }
    payload["schema_version"] = GITHUB_APP_IDENTITY_SCHEMA_VERSION
    try:
        result = subprocess.run(
            [*command, identity],
            input=json.dumps(payload, sort_keys=True),
            capture_output=True,
            text=True,
            check=False,
            cwd=cwd or city_root() or ".",
            timeout=60,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"status": "error", "detail": str(exc)}
    detail = (result.stderr or result.stdout or "").strip()[:1200]
    if result.returncode != 0:
        return {"status": "error", "detail": detail or f"exit {result.returncode}"}
    return {"status": "published", "detail": detail}


def github_repo_rig_name(repository_full_name: str) -> str:
    repo_key = normalize_repo_key(repository_full_name)
    owner, sep, repo = repo_key.partition("/")
    if not sep or not owner or not repo:
        return ""
    slug = re.sub(r"[^a-z0-9_-]+", "-", f"github-{owner}-{repo}")
    slug = re.sub(r"-+", "-", slug).strip("-")
    return slug


def github_repo_dispatch_rig(repository_full_name: str, rules_config: dict[str, Any] | None = None) -> str:
    repo_key = normalize_repo_key(repository_full_name)
    fallback = github_repo_rig_name(repo_key)
    if not fallback:
        return ""
    repos = (rules_config if rules_config is not None else load_rules()).get("repos") or []
    for repo in repos:
        if not isinstance(repo, dict):
            continue
        if normalize_repo_key(str(repo.get("full_name", ""))) != repo_key:
            continue
        rig = str(repo.get("rig", "")).strip()
        if rig:
            return rig
    return fallback


def github_repo_dispatch_route(
    repository_full_name: str,
    rules_config: dict[str, Any] | None = None,
) -> dict[str, str]:
    """Return the exact owning City/Rig pair for a configured repository.

    Cross-City webhook dispatch must never fall back to a derived local Rig:
    an unknown or incomplete topology record fails closed instead.
    """
    repo_key = normalize_repo_key(repository_full_name)
    if not repo_key:
        return {}
    repos = (rules_config if rules_config is not None else load_rules()).get("repos") or []
    for repo in repos:
        if not isinstance(repo, dict):
            continue
        if normalize_repo_key(str(repo.get("full_name", ""))) != repo_key:
            continue
        city = str(repo.get("city", "")).strip()
        rig = str(repo.get("rig", "")).strip()
        if city and rig:
            return {"city": city, "rig": rig}
        return {}
    return {}


def _set_command_formula(commands: dict[str, Any], name: str, formula: str | None) -> dict[str, Any]:
    if formula:
        commands[name] = {"formula": formula}
    return commands


def set_repo_mapping(
    config: dict[str, Any],
    repository: str,
    target: str,
    fix_formula: str | None,
) -> dict[str, Any]:
    cfg = normalize_config(config)
    repo_key = normalize_repo_key(repository)
    mapping: dict[str, Any] = cfg["repositories"].get(repo_key, {})
    mapping["repository"] = repo_key
    mapping["target"] = target
    commands: dict[str, Any] = mapping.get("commands", {})
    commands = _set_command_formula(commands, "fix", fix_formula)
    mapping["commands"] = commands
    cfg["repositories"][repo_key] = mapping
    return save_config(cfg)


def resolve_repo_mapping(
    config: dict[str, Any], repository_full_name: str, repository_id: str | None = None
) -> dict[str, Any] | None:
    repositories = normalize_config(config).get("repositories", {})
    repo_key = normalize_repo_key(repository_full_name)
    if repo_key in repositories:
        return repositories[repo_key]
    if repository_id:
        for mapping in repositories.values():
            if str(mapping.get("repository_id", "")) == str(repository_id):
                return mapping
    return None


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


def workspace_env_value(name: str) -> str:
    """Resolve a config knob from the process env, then city workspace.env.

    Service processes do not inherit [workspace.env], so deployment knobs
    must also be readable straight from city.toml.
    """
    value = os.environ.get(name, "").strip()
    if value:
        return value
    root = city_root()
    if not root:
        return ""
    try:
        with open(os.path.join(root, "city.toml"), "rb") as handle:
            data = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError):
        return ""
    workspace = data.get("workspace") if isinstance(data, dict) else None
    env = workspace.get("env") if isinstance(workspace, dict) else None
    if isinstance(env, dict):
        return str(env.get(name, "")).strip()
    return ""


def admin_url() -> str:
    """Public admin URL: deployment override first, then gc publication.

    Self-hosted cities have no gc publication provider; the override lets
    them run the manifest onboarding flow behind their own edge or tunnel.
    """
    override = workspace_env_value("GITHUB_INTAKE_ADMIN_PUBLIC_URL")
    if override:
        return override
    return published_service_url(ADMIN_SERVICE_NAME)


def webhook_url() -> str:
    """Public webhook base URL: deployment override first, then publication."""
    override = workspace_env_value("GITHUB_INTAKE_WEBHOOK_PUBLIC_URL")
    if override:
        return override
    return published_service_url(WEBHOOK_SERVICE_NAME)


def build_manifest() -> dict[str, Any]:
    admin = admin_url()
    webhook = webhook_url()
    # GITHUB_INTAKE_WEBHOOK_HOOK_URL is the exact delivery URL placed in the
    # manifest, for deployments whose edge uses a different path shape than
    # the service-native /v0/github/webhook suffix.
    hook_url = workspace_env_value("GITHUB_INTAKE_WEBHOOK_HOOK_URL")
    if not admin or not (webhook or hook_url):
        raise ValueError("published admin and webhook URLs are required before building the GitHub App manifest")
    if not hook_url:
        hook_url = webhook.rstrip("/") + "/v0/github/webhook"
    return {
        "name": f"Gas City {city_name()} GitHub Intake",
        "url": admin,
        "hook_attributes": {"url": hook_url, "active": True},
        "redirect_url": admin.rstrip("/") + "/v0/github/app/manifest/callback",
        "callback_urls": [admin.rstrip("/") + "/v0/github/app/manifest/callback"],
        "setup_url": admin,
        "description": "Workspace-hosted GitHub comment and event intake for Gas City",
        "public": False,
        "default_permissions": {
            "issues": "write",
            "metadata": "read",
            "pull_requests": "read",
            "organization_projects": "write",
            "issue_types": "read",
            "issue_fields": "read",
        },
        "default_events": [
            "issue_comment",
            "issues",
            "pull_request",
        ],
    }


def _flatten_match(prefix: str, value: Any, out: dict[str, str]) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            dotted = f"{prefix}.{key}" if prefix else str(key)
            _flatten_match(dotted, child, out)
        return
    out[prefix] = str(value)


def normalize_rule(raw_rule: dict[str, Any], index: int) -> dict[str, Any]:
    rule_id = str(raw_rule.get("id", "")).strip()
    if not rule_id:
        raise ValueError(f"rule[{index}]: id is required")
    event = str(raw_rule.get("event", "")).strip()
    if not event:
        raise ValueError(f"rule {rule_id!r}: event is required")
    raw_match = raw_rule.get("match") or {}
    if not isinstance(raw_match, dict):
        raise ValueError(f"rule {rule_id!r}: match must be a table")
    match: dict[str, str] = {}
    _flatten_match("", raw_match, match)
    raw_actions = raw_rule.get("action") or []
    if not isinstance(raw_actions, list) or not raw_actions:
        raise ValueError(f"rule {rule_id!r}: at least one action is required")
    actions: list[dict[str, Any]] = []
    for action_index, raw_action in enumerate(raw_actions):
        if not isinstance(raw_action, dict):
            raise ValueError(f"rule {rule_id!r}: action[{action_index}] must be a table")
        action_type = str(raw_action.get("type", "")).strip()
        if not action_type:
            raise ValueError(f"rule {rule_id!r}: action[{action_index}].type is required")
        if action_type == "order" and not str(raw_action.get("name", "")).strip():
            raise ValueError(f"rule {rule_id!r}: order action requires name")
        actions.append(dict(raw_action))
    return {
        "id": rule_id,
        "event": event,
        "match": match,
        "allow_self": bool(raw_rule.get("allow_self", False)),
        "action": actions,
    }


def _normalized_unique_strings(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value).strip().lower()
        if normalized and normalized not in seen:
            out.append(normalized)
            seen.add(normalized)
    return out


def normalize_address(raw_address: dict[str, Any], repo_name: str, repo_rig: str, index: int) -> dict[str, Any]:
    address = str(raw_address.get("address", "")).strip().lower()
    if not address:
        raise ValueError(f"repo {repo_name!r}: address[{index}].address is required")
    pool = str(raw_address.get("pool", "")).strip()
    if not pool:
        raise ValueError(f"repo {repo_name!r}: address {address!r}: pool is required")
    if "/" in pool:
        raise ValueError(f"repo {repo_name!r}: address {address!r}: pool must not include a rig")
    formula = str(raw_address.get("formula", "")).strip()
    if not formula:
        raise ValueError(f"repo {repo_name!r}: address {address!r}: formula is required")
    profile = str(raw_address.get("profile", "")).strip() or address.lstrip("@")
    identity = validate_github_app_identity(
        str(raw_address.get("github_app_identity", "")),
        f"repo {repo_name!r}: address {address!r}: github_app_identity",
    )
    return {
        "address": address,
        "pool": pool,
        "target": f"{repo_rig}/{pool}",
        "formula": formula,
        "ack": bool(raw_address.get("ack", True)),
        "profile": profile,
        "github_app_identity": identity,
        "installation_id": str(raw_address.get("installation_id", "")).strip(),
    }


def normalize_repo_addresses(raw_repo: dict[str, Any], index: int) -> dict[str, Any]:
    full_name = normalize_repo_key(str(raw_repo.get("full_name", "")))
    if not full_name:
        raise ValueError(f"repo[{index}]: full_name is required")
    github_rig = github_repo_rig_name(full_name)
    if not github_rig:
        raise ValueError(f"repo[{index}]: full_name must be owner/repo")
    configured_rig = str(raw_repo.get("rig", "")).strip()
    if configured_rig and "/" in configured_rig:
        raise ValueError(f"repo {full_name!r}: rig must be a local rig name, not a target")
    city = str(raw_repo.get("city", "")).strip()
    if city and not configured_rig:
        raise ValueError(f"repo {full_name!r}: city requires an explicit rig")
    rig = configured_rig or github_rig
    raw_addresses = raw_repo.get("address") or []
    if not isinstance(raw_addresses, list):
        raise ValueError(f"repo {full_name!r}: address must be an array of tables")
    return {
        "full_name": full_name,
        "city": city,
        "github_rig": github_rig,
        "rig": rig,
        "authorized_users": _normalized_unique_strings(raw_repo.get("authorized_users")),
        "installation_id": str(raw_repo.get("installation_id", "")).strip(),
        "addresses": [
            normalize_address(raw_address, full_name, rig, address_index)
            for address_index, raw_address in enumerate(raw_addresses)
            if isinstance(raw_address, dict)
        ],
    }


def load_rules() -> dict[str, Any]:
    path = rules_path()
    if not os.path.exists(path):
        return {"version": 1, "path": path, "rules": [], "repos": []}
    with open(path, "rb") as handle:
        data = tomllib.load(handle)
    if not isinstance(data, dict):
        raise ValueError("rules root must be a TOML table")
    if int(data.get("version", 0)) != 1:
        raise ValueError("rules version must be 1")
    raw_rules = data.get("rule") or []
    if not isinstance(raw_rules, list):
        raise ValueError("rule must be an array of tables")
    raw_repos = data.get("repo") or []
    if not isinstance(raw_repos, list):
        raise ValueError("repo must be an array of tables")
    return {
        "version": 1,
        "path": path,
        "rules": [normalize_rule(raw_rule, index) for index, raw_rule in enumerate(raw_rules)],
        "repos": [normalize_repo_addresses(raw_repo, index) for index, raw_repo in enumerate(raw_repos)],
    }


def payload_value(payload: dict[str, Any], dotted_path: str) -> Any:
    current: Any = payload
    for part in dotted_path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def rule_matches(rule: dict[str, Any], event: str, payload: dict[str, Any]) -> bool:
    if str(rule.get("event", "")) != event:
        return False
    match = rule.get("match") or {}
    if not isinstance(match, dict):
        return False
    for dotted_path, expected in match.items():
        actual = payload_value(payload, str(dotted_path))
        if str(actual) != str(expected):
            return False
    return True


def matching_rules(event: str, payload: dict[str, Any], rules_config: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    rules = (rules_config or load_rules()).get("rules") or []
    return [rule for rule in rules if isinstance(rule, dict) and rule_matches(rule, event, payload)]


def rule_result_path(result_id: str) -> str:
    return os.path.join(rule_results_dir(), f"{safe_storage_id(result_id, 'rule-result')}.json")


def save_rule_result(payload: dict[str, Any]) -> dict[str, Any]:
    ensure_layout()
    result = copy.deepcopy(payload)
    result["updated_at"] = utcnow()
    atomic_write_json(rule_result_path(str(result["result_id"])), result)
    return result


def address_result_path(result_id: str) -> str:
    return os.path.join(address_results_dir(), f"{safe_storage_id(result_id, 'address-result')}.json")


def save_address_result(payload: dict[str, Any]) -> dict[str, Any]:
    ensure_layout()
    result = copy.deepcopy(payload)
    result["updated_at"] = utcnow()
    atomic_write_json(address_result_path(str(result["result_id"])), result)
    return result


def load_address_result(result_id: str) -> dict[str, Any] | None:
    data = read_json(address_result_path(result_id))
    if isinstance(data, dict):
        return data
    return None


def list_recent_rule_results(limit: int = 20) -> list[dict[str, Any]]:
    ensure_layout()
    entries: list[dict[str, Any]] = []
    paths = sorted(
        pathlib.Path(rule_results_dir()).glob("*.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )[:limit]
    for path in paths:
        data = read_json(str(path))
        if isinstance(data, dict):
            entries.append(data)
    entries.sort(key=lambda item: item.get("created_at", ""), reverse=True)
    return entries[:limit]


def list_recent_address_results(limit: int = 20) -> list[dict[str, Any]]:
    ensure_layout()
    entries: list[dict[str, Any]] = []
    paths = sorted(
        pathlib.Path(address_results_dir()).glob("*.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )[:limit]
    for path in paths:
        data = read_json(str(path))
        if isinstance(data, dict):
            entries.append(data)
    entries.sort(key=lambda item: item.get("created_at", ""), reverse=True)
    return entries[:limit]


def parse_gc_command(body: str) -> dict[str, Any] | None:
    lines = body.splitlines()
    for index, raw_line in enumerate(lines):
        line = raw_line.strip()
        if not line:
            continue
        parts = line.split(maxsplit=2)
        if len(parts) < 2 or parts[0] != "/gc":
            continue
        command = parts[1].lower()
        if not command or not all(ch.isalnum() or ch in ("-", "_") for ch in command):
            continue
        inline_context = parts[2].strip() if len(parts) == 3 else ""
        trailing_context = "\n".join(lines[index + 1 :]).strip()
        context_parts = [part for part in (inline_context, trailing_context) if part]
        return {
            "command": command,
            "command_line": line,
            "line_index": index,
            "inline_context": inline_context,
            "context": "\n".join(context_parts),
        }
    return None


def build_request_id(repository_id: str, comment_id: str, command: str) -> str:
    safe_command = "".join(ch for ch in command.lower() if ch.isalnum() or ch in ("-", "_")) or "command"
    return f"gh-{repository_id}-{comment_id}-{safe_command}"


def build_workflow_key(repository_id: str, issue_number: str, command: str) -> str:
    safe_command = "".join(ch for ch in command.lower() if ch.isalnum() or ch in ("-", "_")) or "command"
    return f"gh:{repository_id}:issue:{issue_number}:{safe_command}"


def safe_storage_id(value: str, prefix: str) -> str:
    value = value.strip()
    if value and all(ch.isalnum() or ch in ("-", "_") for ch in value):
        return value
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:24]
    return f"{prefix}-{digest}"


def workflow_storage_id(value: str) -> str:
    value = value.strip()
    if value and all(ch.isalnum() or ch in ("-", "_", ":") for ch in value):
        return value
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:24]
    return f"workflow-{digest}"


def address_token_pattern(address: str) -> re.Pattern[str]:
    return re.compile(r"(?<![\w@])" + re.escape(address) + r"(?![\w-])", re.IGNORECASE)


def clean_addressed_body(body: str, addresses: list[str]) -> str:
    cleaned = body
    for address in addresses:
        cleaned = address_token_pattern(address).sub("", cleaned)
    lines = [re.sub(r"[ \t]{2,}", " ", line).strip() for line in cleaned.splitlines()]
    return "\n".join(lines).strip()


def addressed_source_key(repository_id: str, comment_id: str, address: str) -> str:
    return f"github-comment:{repository_id}:{comment_id}:{address.strip().lower()}"


def extract_addressed_comment_requests(
    payload: dict[str, Any], rules_config: dict[str, Any] | None = None
) -> dict[str, Any] | None:
    if payload.get("action") != "created":
        return None
    issue = payload.get("issue") or {}
    comment = payload.get("comment") or {}
    repository = payload.get("repository") or {}
    if not isinstance(issue, dict) or not isinstance(comment, dict) or not isinstance(repository, dict):
        return None
    repo_name = normalize_repo_key(str(repository.get("full_name", "")))
    repository_id = str(repository.get("id", "")).strip()
    comment_id = str(comment.get("id", "")).strip()
    issue_number = str(issue.get("number", "")).strip()
    if not repo_name or not repository_id or not comment_id or not issue_number:
        return None
    repos = (rules_config or load_rules()).get("repos") or []
    repo_cfg = next(
        (
            repo
            for repo in repos
            if isinstance(repo, dict) and normalize_repo_key(str(repo.get("full_name", ""))) == repo_name
        ),
        None,
    )
    if not repo_cfg:
        return None
    body = str(comment.get("body", ""))
    addresses = [address for address in repo_cfg.get("addresses") or [] if isinstance(address, dict)]
    matched: list[dict[str, Any]] = []
    seen_addresses: set[str] = set()
    for address_cfg in addresses:
        address = str(address_cfg.get("address", "")).strip().lower()
        if not address or address in seen_addresses:
            continue
        if address_token_pattern(address).search(body):
            matched.append(address_cfg)
            seen_addresses.add(address)
    if not matched:
        return None

    matched_tokens = [str(address.get("address", "")).strip().lower() for address in matched]
    cleaned_body = clean_addressed_body(body, matched_tokens)
    sender = str((comment.get("user") or {}).get("login", "")).strip()
    sender_key = sender.lower()
    authorized_users = _normalized_unique_strings(repo_cfg.get("authorized_users"))
    owner = repository.get("owner") or {}
    item_kind = "pr" if issue.get("pull_request") else "issue"
    rig = str(repo_cfg.get("rig") or github_repo_rig_name(repo_name))
    base = {
        "repository_id": repository_id,
        "repository_full_name": repo_name,
        "repository_owner": str(owner.get("login", "")),
        "repository_name": str(repository.get("name", "")),
        "repository_default_branch": str(repository.get("default_branch", "")),
        "item_kind": item_kind,
        "item_number": issue_number,
        "item_url": str(issue.get("html_url", "")),
        "issue_id": str(issue.get("id", "")),
        "issue_number": issue_number,
        "issue_title": str(issue.get("title", "")),
        "issue_url": str(issue.get("html_url", "")),
        "comment_id": comment_id,
        "comment_body": body,
        "comment_url": str(comment.get("html_url", "")),
        "comment_author": sender,
        "comment_author_type": str((comment.get("user") or {}).get("type", "")),
        "comment_created_at": str(comment.get("created_at", "")),
        "comment_updated_at": str(comment.get("updated_at", "")),
        "installation_id": str((payload.get("installation") or {}).get("id", "")),
        "cleaned_body": cleaned_body,
    }
    requests: list[dict[str, Any]] = []
    for address_cfg in matched:
        address = str(address_cfg.get("address", "")).strip().lower()
        pool = str(address_cfg.get("pool", "")).strip()
        target = str(address_cfg.get("target") or (f"{rig}/{pool}" if rig and pool else "")).strip()
        profile = str(address_cfg.get("profile", "")).strip() or address.lstrip("@")
        request = dict(base)
        request.update(
            {
                "address": address,
                "rig": rig,
                "pool": pool,
                "target": target,
                "formula": str(address_cfg.get("formula", "")),
                "ack": bool(address_cfg.get("ack", True)),
                "profile": profile,
                "profile_github_app_identity": str(address_cfg.get("github_app_identity", "")),
                "profile_installation_id": str(address_cfg.get("installation_id", "")),
                "source_key": addressed_source_key(repository_id, comment_id, address),
            }
        )
        requests.append(request)
    return {
        "repository": repo_cfg,
        "authorized": bool(sender_key and sender_key in authorized_users),
        "authorized_users": authorized_users,
        "sender": sender,
        "addresses": matched_tokens,
        "cleaned_body": cleaned_body,
        "requests": requests,
    }


def extract_issue_comment_request(payload: dict[str, Any]) -> dict[str, Any] | None:
    if payload.get("action") != "created":
        return None
    issue = payload.get("issue") or {}
    if issue.get("pull_request"):
        return None
    comment = payload.get("comment") or {}
    repository = payload.get("repository") or {}
    owner = repository.get("owner") or {}
    parsed_command = parse_gc_command(str(comment.get("body", "")))
    if not parsed_command:
        return None
    repository_id = str(repository.get("id", ""))
    comment_id = str(comment.get("id", ""))
    issue_number = str(issue.get("number", ""))
    if not repository_id or not comment_id or not issue_number:
        return None
    command = str(parsed_command["command"])
    return {
        "request_id": build_request_id(repository_id, comment_id, command),
        "workflow_key": build_workflow_key(repository_id, issue_number, command),
        "status": "received",
        "command": command,
        "command_line": str(parsed_command.get("command_line", "")),
        "command_context": str(parsed_command.get("context", "")),
        "command_inline_context": str(parsed_command.get("inline_context", "")),
        "created_at": utcnow(),
        "updated_at": utcnow(),
        "repository_id": repository_id,
        "repository_full_name": str(repository.get("full_name", "")).lower(),
        "repository_owner": str(owner.get("login", "")),
        "repository_name": str(repository.get("name", "")),
        "repository_default_branch": str(repository.get("default_branch", "")),
        "issue_id": str(issue.get("id", "")),
        "issue_number": issue_number,
        "issue_title": str(issue.get("title", "")),
        "issue_body": str(issue.get("body", "")),
        "issue_url": str(issue.get("html_url", "")),
        "issue_author": str((issue.get("user") or {}).get("login", "")),
        "comment_id": comment_id,
        "comment_body": str(comment.get("body", "")),
        "comment_url": str(comment.get("html_url", "")),
        "comment_author": str((comment.get("user") or {}).get("login", "")),
        "comment_author_association": str(comment.get("author_association", "")),
        "installation_id": str((payload.get("installation") or {}).get("id", "")),
    }


def request_path(request_id: str) -> str:
    return os.path.join(requests_dir(), f"{request_id}.json")


def delivery_path(delivery_id: str) -> str:
    return os.path.join(deliveries_dir(), f"{safe_storage_id(delivery_id, 'delivery')}.json")


def workflow_path(workflow_key: str) -> str:
    return os.path.join(workflows_dir(), f"{workflow_storage_id(workflow_key)}.json")


def load_request(request_id: str) -> dict[str, Any] | None:
    data = read_json(request_path(request_id))
    if isinstance(data, dict):
        return data
    return None


def save_request(payload: dict[str, Any]) -> dict[str, Any]:
    ensure_layout()
    payload = copy.deepcopy(payload)
    payload["updated_at"] = utcnow()
    atomic_write_json(request_path(payload["request_id"]), payload)
    return payload


def save_delivery(payload: dict[str, Any]) -> None:
    ensure_layout()
    atomic_write_json(delivery_path(payload["delivery_id"]), payload)


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


def find_request(repository_full_name: str, issue_number: str, command: str) -> dict[str, Any] | None:
    ensure_layout()
    repo_key = normalize_repo_key(repository_full_name)
    matches: list[dict[str, Any]] = []
    for path in pathlib.Path(requests_dir()).glob("*.json"):
        data = read_json(str(path))
        if not isinstance(data, dict):
            continue
        if normalize_repo_key(str(data.get("repository_full_name", ""))) != repo_key:
            continue
        if str(data.get("issue_number", "")) != str(issue_number):
            continue
        if str(data.get("command", "")) != command:
            continue
        matches.append(data)
    if not matches:
        return None
    matches.sort(key=lambda item: item.get("created_at", ""), reverse=True)
    return matches[0]


def build_status_snapshot(limit: int = 20) -> dict[str, Any]:
    cfg = load_effective_config()
    rules_error = ""
    try:
        rules = load_rules()
    except Exception as exc:  # noqa: BLE001
        rules = {"version": 1, "path": rules_path(), "rules": []}
        rules_error = str(exc)
    return {
        "service_name": current_service_name(),
        "city_root": city_root(),
        "state_root": state_root(),
        "admin_url": admin_url(),
        "webhook_url": webhook_url(),
        "published_services_dir": published_services_dir(),
        "config": redact_config(cfg),
        "rules": {
            "path": rules.get("path", rules_path()),
            "count": len(rules.get("rules") or []),
            "ids": [str(rule.get("id", "")) for rule in (rules.get("rules") or []) if isinstance(rule, dict)],
            "address_repo_count": len(rules.get("repos") or []),
            "address_count": sum(
                len(repo.get("addresses") or []) for repo in (rules.get("repos") or []) if isinstance(repo, dict)
            ),
            "error": rules_error,
        },
        "recent_requests": list_recent_requests(limit=limit),
        "recent_rule_results": list_recent_rule_results(limit=limit),
        "recent_address_results": list_recent_address_results(limit=limit),
    }


def verify_github_signature(secret: str, payload: bytes, header_value: str) -> bool:
    if not secret or not header_value.startswith("sha256="):
        return False
    expected = hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()
    supplied = header_value.split("=", 1)[1]
    return hmac.compare_digest(expected, supplied)


def _base64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def github_web_base() -> str:
    parsed = urllib.parse.urlparse(GITHUB_API_BASE)
    if parsed.netloc == "api.github.com":
        return "https://github.com"
    host = parsed.netloc or "github.com"
    if host.startswith("api."):
        host = host[4:]
    path = parsed.path.rstrip("/")
    if path.endswith("/api/v3"):
        path = path[:-7]
    return urllib.parse.urlunparse((parsed.scheme or "https", host, path.rstrip("/"), "", "", "")).rstrip("/")


def github_https_origin(value: str) -> str:
    """Validate the work-sync API boundary before signing or sending credentials."""
    message = "GitHub API URL must be an HTTPS origin"
    if (
        not isinstance(value, str)
        or "?" in value or "#" in value
        or any(char.isspace() or ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise ValueError(message)
    try:
        parsed = urllib.parse.urlsplit(value)
        port = parsed.port
        valid = (
            parsed.scheme == "https" and bool(parsed.hostname)
            and parsed.username is None and parsed.password is None
            and parsed.path in {"", "/"} and not parsed.query and not parsed.fragment
            and (port is None or port > 0) and not parsed.netloc.endswith(":")
        )
    except ValueError:
        raise ValueError(message) from None
    if not valid:
        raise ValueError(message)
    return value.rstrip("/")


class _NoGitHubRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        # Even same-origin redirects are not an accepted auth/write destination.
        raise urllib.error.HTTPError(req.full_url, code, "GitHub redirect rejected", headers, fp)


def github_no_redirect_urlopen(request: urllib.request.Request, *, timeout: float):
    return urllib.request.build_opener(_NoGitHubRedirect()).open(request, timeout=timeout)


def github_api_request(
    method: str,
    path: str,
    payload: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    bearer_token: str | None = None,
    *,
    allow_redirects: bool = True,
) -> dict[str, Any]:
    if path.startswith("http://") or path.startswith("https://"):
        url = path
    else:
        url = urllib.parse.urljoin(GITHUB_API_BASE.rstrip("/") + "/", path.lstrip("/"))
    body = None
    request_headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "gas-city-github/0.1",
        "X-GitHub-Api-Version": GITHUB_API_VERSION,
    }
    if headers:
        request_headers.update(headers)
    if bearer_token:
        request_headers["Authorization"] = f"Bearer {bearer_token}"
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        request_headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=body, headers=request_headers, method=method.upper())
    try:
        open_request = urllib.request.urlopen if allow_redirects else github_no_redirect_urlopen
        with open_request(request, timeout=20) as response:
            raw = response.read()
    except urllib.error.HTTPError as exc:
        if not allow_redirects:
            exc.close()
            raise GitHubAPIError("GitHub scoped request failed") from None
        raw = exc.read()
        message = raw.decode("utf-8", errors="replace")
        raise GitHubAPIError(f"{method.upper()} {url} failed with {exc.code}: {message}") from exc
    except urllib.error.URLError as exc:
        if not allow_redirects:
            raise GitHubAPIError("GitHub scoped request failed") from None
        raise GitHubAPIError(f"{method.upper()} {url} failed: {exc}") from exc
    if not raw:
        return {}
    data = json.loads(raw.decode("utf-8"))
    if isinstance(data, dict):
        return data
    raise GitHubAPIError(f"{method.upper()} {url} returned non-object JSON")


def exchange_manifest_code(code: str) -> dict[str, Any]:
    return github_api_request("POST", f"/app-manifests/{urllib.parse.quote(code)}/conversions")


def app_identifier(app_cfg: dict[str, Any]) -> str:
    value = app_cfg.get("app_id")
    if value:
        return str(value)
    raise GitHubAPIError("GitHub App app_id is required for JWT signing")


def build_app_jwt(app_cfg: dict[str, Any]) -> str:
    private_key_pem = app_cfg.get("private_key_pem")
    if not private_key_pem:
        raise GitHubAPIError("GitHub App private key is not configured")
    issued_at = int(time.time()) - 60
    payload = {
        "iat": issued_at,
        "exp": issued_at + 540,
        "iss": app_identifier(app_cfg),
    }
    header_json = json.dumps({"alg": "RS256", "typ": "JWT"}, separators=(",", ":"), sort_keys=True).encode("utf-8")
    payload_json = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    signing_input = f"{_base64url(header_json)}.{_base64url(payload_json)}".encode("ascii")
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as handle:
        handle.write(private_key_pem)
        handle.flush()
        os.fchmod(handle.fileno(), 0o600)
        key_path = handle.name
    try:
        signature = subprocess.run(
            ["openssl", "dgst", "-sha256", "-sign", key_path],
            check=True,
            input=signing_input,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        ).stdout
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr.decode("utf-8", errors="replace")
        raise GitHubAPIError(f"failed to sign GitHub App JWT: {stderr.strip()}") from exc
    finally:
        try:
            os.remove(key_path)
        except FileNotFoundError:
            pass
    return f"{signing_input.decode('ascii')}.{_base64url(signature)}"


def create_installation_token(
    app_cfg: dict[str, Any],
    installation_id: str,
    *,
    repository_full_name: str | None = None,
    permissions: dict[str, str] | None = None,
) -> str:
    """Mint an installation token, proving exact scope when a consumer requests it."""
    scoped = repository_full_name is not None or permissions is not None
    request: dict[str, Any] = {}
    requested_permissions: dict[str, str] = {}
    if scoped:
        github_https_origin(GITHUB_API_BASE)
        if (
            not isinstance(repository_full_name, str)
            or not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository_full_name)
            or not isinstance(permissions, dict)
            or not permissions
            or any(
                not isinstance(key, str)
                or not re.fullmatch(r"[a-z][a-z0-9_]*", key)
                or value not in ("read", "write", "none")
                for key, value in permissions.items()
            )
        ):
            raise GitHubAPIError("GitHub installation token scope is invalid")
        requested_permissions = {key: value for key, value in permissions.items() if value != "none"}
        if not requested_permissions:
            raise GitHubAPIError("GitHub installation token permissions are empty")
        request["payload"] = {
            "repositories": [repository_full_name.split("/", 1)[1]],
            "permissions": requested_permissions,
        }
        request["allow_redirects"] = False
    jwt_token = build_app_jwt(app_cfg)
    response = github_api_request(
        "POST",
        f"/app/installations/{installation_id}/access_tokens",
        bearer_token=jwt_token,
        **request,
    )
    token = response.get("token") if isinstance(response, dict) else None
    if not isinstance(token, str) or not token:
        raise GitHubAPIError("GitHub installation token response did not include a token")
    if scoped:
        effective_permissions = response.get("permissions")
        expected_permissions = {"metadata": "read", **requested_permissions}
        if not isinstance(effective_permissions, dict) or {
            key: value for key, value in effective_permissions.items() if value != "none"
        } != expected_permissions:
            raise GitHubAPIError("GitHub installation token permission readback mismatch")
        try:
            expires = datetime.fromisoformat(response["expires_at"].replace("Z", "+00:00"))
            remaining = expires.timestamp() - time.time() if expires.tzinfo is not None else -1
        except (KeyError, AttributeError, TypeError, ValueError, OverflowError):
            remaining = -1
        if not 0 < remaining <= 3600:
            raise GitHubAPIError("GitHub installation token expiry readback is invalid")
        readback = github_api_request(
            "GET", "/installation/repositories?per_page=100", bearer_token=token,
            allow_redirects=False,
        )
        repositories = readback.get("repositories") if isinstance(readback, dict) else None
        total = readback.get("total_count") if isinstance(readback, dict) else None
        if (
            type(total) is not int
            or total != 1
            or not isinstance(repositories, list)
            or len(repositories) != 1
            or not isinstance(repositories[0], dict)
            or not isinstance(repositories[0].get("full_name"), str)
            or repositories[0]["full_name"].lower() != repository_full_name.lower()
        ):
            raise GitHubAPIError("GitHub installation token repository readback mismatch")
    return token


def repository_permission(
    app_cfg: dict[str, Any],
    installation_id: str,
    owner: str,
    repo: str,
    username: str,
) -> str:
    token = create_installation_token(app_cfg, installation_id)
    try:
        response = github_api_request(
            "GET",
            f"/repos/{urllib.parse.quote(owner)}/{urllib.parse.quote(repo)}/collaborators/{urllib.parse.quote(username)}/permission",
            bearer_token=token,
        )
    except GitHubAPIError as exc:
        if " 404:" in str(exc):
            return "none"
        raise
    return str(response.get("permission", "none")).lower()


def post_issue_comment(
    app_cfg: dict[str, Any],
    installation_id: str,
    owner: str,
    repo: str,
    issue_number: str,
    body: str,
) -> dict[str, Any]:
    token = create_installation_token(app_cfg, installation_id)
    return github_api_request(
        "POST",
        f"/repos/{urllib.parse.quote(owner)}/{urllib.parse.quote(repo)}/issues/{issue_number}/comments",
        payload={"body": body},
        bearer_token=token,
    )


def create_pull_request(
    app_cfg: dict[str, Any],
    installation_id: str,
    owner: str,
    repo: str,
    title: str,
    head: str,
    base: str,
    body: str,
) -> dict[str, Any]:
    token = create_installation_token(app_cfg, installation_id)
    return github_api_request(
        "POST",
        f"/repos/{urllib.parse.quote(owner)}/{urllib.parse.quote(repo)}/pulls",
        payload={
            "title": title,
            "head": head,
            "base": base,
            "body": body,
        },
        bearer_token=token,
    )


def repository_git_url(repository_full_name: str) -> str:
    return f"{github_web_base().rstrip('/')}/{repository_full_name}.git"


def git_push_branch(
    app_cfg: dict[str, Any],
    installation_id: str,
    repository_full_name: str,
    branch: str,
    ref: str = "HEAD",
) -> dict[str, Any]:
    token = create_installation_token(app_cfg, installation_id)
    basic_auth = base64.b64encode(f"x-access-token:{token}".encode("utf-8")).decode("ascii")
    base_url = github_web_base().rstrip("/")
    env = os.environ.copy()
    env["GIT_TERMINAL_PROMPT"] = "0"
    env["GIT_CONFIG_COUNT"] = "1"
    env["GIT_CONFIG_KEY_0"] = f"http.{base_url}/.extraheader"
    env["GIT_CONFIG_VALUE_0"] = f"AUTHORIZATION: basic {basic_auth}"
    result = subprocess.run(
        ["git", "push", repository_git_url(repository_full_name), f"{ref}:refs/heads/{branch}"],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    if result.returncode != 0:
        stderr = result.stderr.strip()
        raise GitHubAPIError(f"git push failed with exit code {result.returncode}: {stderr}")
    return {
        "branch": branch,
        "stdout": result.stdout.strip(),
        "stderr": result.stderr.strip(),
    }


def install_url(app_cfg: dict[str, Any]) -> str:
    slug = app_cfg.get("slug")
    if slug:
        return f"https://github.com/apps/{slug}/installations/new"
    return ""


def app_bot_login(app_cfg: dict[str, Any]) -> str:
    slug = str(app_cfg.get("slug", "")).strip()
    if slug:
        return f"{slug}[bot]"
    return ""
