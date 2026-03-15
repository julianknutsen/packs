from __future__ import annotations

import base64
import copy
import hashlib
import hmac
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

WEBHOOK_SERVICE_NAME = "github-webhook"
ADMIN_SERVICE_NAME = "github-admin"
SCHEMA_VERSION = 1
GITHUB_API_BASE = os.environ.get("GC_GITHUB_API_BASE", "https://api.github.com")
GITHUB_API_VERSION = os.environ.get("GC_GITHUB_API_VERSION", "2026-03-10")


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
        return ".gc/services/github-intake"
    return os.path.join(root, ".gc", "services", "github-intake")


def data_dir() -> str:
    return os.path.join(state_root(), "data")


def requests_dir() -> str:
    return os.path.join(data_dir(), "requests")


def deliveries_dir() -> str:
    return os.path.join(data_dir(), "deliveries")


def workflows_dir() -> str:
    return os.path.join(data_dir(), "workflows")


def config_path() -> str:
    return os.path.join(data_dir(), "config.json")


def published_services_dir() -> str:
    value = os.environ.get("GC_PUBLISHED_SERVICES_DIR")
    if value:
        return value
    root = city_root()
    if not root:
        return ".gc/services/.published"
    return os.path.join(root, ".gc", "services", ".published")


def ensure_layout() -> None:
    for path in (data_dir(), requests_dir(), deliveries_dir(), workflows_dir()):
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
    for key in ("client_id", "client_secret", "webhook_secret", "slug", "html_url", "name"):
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


def admin_url() -> str:
    return published_service_url(ADMIN_SERVICE_NAME)


def webhook_url() -> str:
    return published_service_url(WEBHOOK_SERVICE_NAME)


def build_manifest() -> dict[str, Any]:
    admin = admin_url()
    webhook = webhook_url()
    if not admin or not webhook:
        raise ValueError("published admin and webhook URLs are required before building the GitHub App manifest")
    return {
        "name": f"Gas City {city_name()} GitHub Intake",
        "url": admin,
        "hook_attributes": {"url": webhook.rstrip("/") + "/v0/github/webhook", "active": True},
        "redirect_url": admin.rstrip("/") + "/v0/github/app/manifest/callback",
        "callback_urls": [admin.rstrip("/") + "/v0/github/app/manifest/callback"],
        "setup_url": admin,
        "description": "Workspace-hosted GitHub slash-command intake for Gas City",
        "public": False,
        "default_permissions": {
            "contents": "write",
            "issues": "write",
            "pull_requests": "write",
        },
        "default_events": [
            "issue_comment",
            "installation",
            "installation_repositories",
        ],
    }


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
    cfg = load_config()
    return {
        "service_name": current_service_name(),
        "city_root": city_root(),
        "state_root": state_root(),
        "admin_url": admin_url(),
        "webhook_url": webhook_url(),
        "published_services_dir": published_services_dir(),
        "config": redact_config(cfg),
        "recent_requests": list_recent_requests(limit=limit),
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


def github_api_request(
    method: str,
    path: str,
    payload: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    bearer_token: str | None = None,
) -> dict[str, Any]:
    if path.startswith("http://") or path.startswith("https://"):
        url = path
    else:
        url = urllib.parse.urljoin(GITHUB_API_BASE.rstrip("/") + "/", path.lstrip("/"))
    body = None
    request_headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "gas-city-github-intake/0.1",
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
        with urllib.request.urlopen(request, timeout=20) as response:
            raw = response.read()
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        message = raw.decode("utf-8", errors="replace")
        raise GitHubAPIError(f"{method.upper()} {url} failed with {exc.code}: {message}") from exc
    except urllib.error.URLError as exc:
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


def create_installation_token(app_cfg: dict[str, Any], installation_id: str) -> str:
    jwt_token = build_app_jwt(app_cfg)
    response = github_api_request(
        "POST",
        f"/app/installations/{installation_id}/access_tokens",
        bearer_token=jwt_token,
    )
    token = response.get("token")
    if not token:
        raise GitHubAPIError("GitHub installation token response did not include a token")
    return str(token)


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
