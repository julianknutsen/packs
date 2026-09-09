#!/usr/bin/env python3

from __future__ import annotations

import html
import json
import os
import shlex
import socketserver
import subprocess
import threading
import tomllib
import traceback
import urllib.parse
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler
from typing import Any

import github_intake_common as common

PROCESSING_LOCK = threading.Lock()
ACCEPTANCE_LOCK = threading.Lock()
RULE_PROCESSING_LOCK = threading.Lock()
ADDRESSED_ROUTER_KICK_LOCK = threading.Lock()
PROCESSING_REQUESTS: set[str] = set()
RULE_PROCESSING_DELIVERIES: set[str] = set()
ADDRESSED_ROUTER_KICK_DELIVERIES: set[str] = set()
WRITE_PERMISSION_LEVELS = {"write", "maintain", "admin"}
BUGFLOW_WORKFLOW_FORMULA = "mol-bug-report-flow-v2"
ADDRESSED_MESSAGE_FORMULA = "github-addressed-message"
BUGFLOW_DUPLICATE_MARKERS = (
    "duplicate_open_bead",
    "open bugflow source bead already exists",
)
ADDRESSED_ROUTER_IN_PROGRESS_STATUSES = {"starting", "dispatching"}
ADDRESSED_ROUTER_STALE_AFTER = timedelta(minutes=10)
# Workspace-env keys forwarded to identity resolver/publisher subprocesses.
# Any GITHUB_INTAKE_* key passes through so store-specific resolvers can
# define their own configuration knobs without this pack knowing the store.
PROFILE_IDENTITY_ENV_PREFIX = "GITHUB_INTAKE_"
INTAKE_APP_CONFIG_REQUIRED_FIELDS = ("app_id", "webhook_secret", "private_key_pem")


class ThreadingUnixHTTPServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True


def json_response(handler: BaseHTTPRequestHandler, status: int, payload: dict[str, Any]) -> None:
    body = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8") + b"\n"
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def text_response(handler: BaseHTTPRequestHandler, status: int, body: str, content_type: str) -> None:
    data = body.encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", content_type)
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


def command_behavior(command: str) -> dict[str, Any]:
    if command != "fix":
        return {}
    return {"workflow_scope": "issue"}


def request_summary(request: dict[str, Any]) -> dict[str, Any]:
    return {
        "request_id": request.get("request_id"),
        "workflow_key": request.get("workflow_key", ""),
        "status": request.get("status"),
        "command": request.get("command"),
        "repository_full_name": request.get("repository_full_name"),
        "issue_number": request.get("issue_number"),
        "bead_id": request.get("bead_id", ""),
        "bugflow_source_bead_id": request.get("bugflow_source_bead_id", ""),
        "workflow_root_id": request.get("workflow_root_id", ""),
        "acknowledgement_comment_url": request.get("acknowledgement_comment_url", ""),
        "dispatch_target": request.get("dispatch_target", ""),
        "dispatch_formula": request.get("dispatch_formula", ""),
        "reason": request.get("reason", ""),
    }


def trim_output(value: str, limit: int = 1200) -> str:
    value = value.strip()
    if len(value) <= limit:
        return value
    return value[:limit].rstrip() + "..."


def human_reason(code: str) -> str:
    mapping = {
        "repo_mapping_missing": "no repository mapping exists for this repo",
        "command_not_configured": "this repository does not configure that /gc command",
        "command_not_supported": "this GitHub intake slice only supports /gc fix on issues",
        "gc_not_available": "the gc CLI is not available in this runtime",
        "github_app_not_configured": "the GitHub App is not fully configured in this workspace",
        "comment_author_lacks_write": "the commenter does not have write or admin access to this repository",
        "invalid_dispatch_target": "the repository mapping target is not a rig-scoped sling target",
        "bead_create_failed": "the workflow bead could not be created",
        "bead_update_failed": "the workflow bead could not be initialized",
        "bugflow_router_failed": "the bugflow source bead was created but the router did not start the workflow",
        "github_app_token_failed": "the GitHub App installation token could not be created",
        "issue_url_missing": "the issue URL was missing from the GitHub webhook payload",
        "permission_lookup_failed": "the GitHub App could not verify the commenter's repository permission",
        "pr_comments_not_supported": "this slice only accepts /gc fix commands on GitHub issues, not pull requests",
    }
    return mapping.get(code, code or "unknown_error")


def rig_from_target(target: str) -> str:
    if "/" not in target:
        return ""
    rig, _, _ = target.partition("/")
    return rig.strip()


def gc_bd_command(city_root: str, *args: str, rig: str = "") -> list[str]:
    command = [os.environ.get("GC_BIN", "gc")]
    if city_root not in {"", "."}:
        command.extend(["--city", city_root])
    if rig:
        command.extend(["--rig", rig])
    command.append("bd")
    command.extend(args)
    return command


def rig_workdir(rig: str) -> str:
    """Resolve a rig's working directory from .beads/routes.jsonl."""
    root = common.city_root() or "."
    routes_path = os.path.join(root, ".beads", "routes.jsonl")
    try:
        with open(routes_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                entry = json.loads(line)
                # Match by prefix (e.g. "mc" for mission-control) — the path
                # field is the rig directory relative to city root.
                path = str(entry.get("path", ""))
                if path == rig:
                    resolved = os.path.join(root, path) if not os.path.isabs(path) else path
                    if os.path.isdir(resolved):
                        return resolved
    except (OSError, json.JSONDecodeError):
        pass
    return ""


def extract_json_value(raw: str) -> Any:
    raw = raw.strip()
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass
    for left, right in (("{", "}"), ("[", "]")):
        start = raw.find(left)
        end = raw.rfind(right)
        if start == -1 or end == -1 or end < start:
            continue
        try:
            payload = json.loads(raw[start : end + 1])
        except json.JSONDecodeError:
            continue
        return payload
    return {}


def extract_json_output(raw: str) -> dict[str, Any]:
    payload = extract_json_value(raw)
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, list) and payload and isinstance(payload[0], dict):
        return payload[0]
    return {}


def build_fix_bead_title(request: dict[str, Any]) -> str:
    issue_number = str(request.get("issue_number", "")).strip()
    issue_title = str(request.get("issue_title", "")).strip()
    context = str(request.get("command_inline_context", "")).strip()
    summary = issue_title or context or "GitHub issue follow-up"
    title = f"Fix GitHub issue #{issue_number}: {summary}" if issue_number else f"Fix GitHub issue: {summary}"
    return title[:180]


def build_fix_bead_notes(request: dict[str, Any]) -> str:
    issue_title = str(request.get("issue_title", "")).strip() or "(none)"
    issue_body = str(request.get("issue_body", "")).strip() or "(none)"
    comment_body = str(request.get("comment_body", "")).strip() or "(none)"
    command_context = str(request.get("command_context", "")).strip() or "(none)"
    lines = [
        "## GitHub Source",
        "",
        f"- Repository: {request.get('repository_full_name', '')}",
        f"- Issue: #{request.get('issue_number', '')}",
        f"- Issue URL: {request.get('issue_url', '')}",
        f"- Trigger Comment: {request.get('comment_url', '')}",
        f"- Request ID: {request.get('request_id', '')}",
        f"- Requested By: {request.get('comment_author', '')}",
        "",
        "## Issue Title",
        "",
        issue_title,
        "",
        "## Issue Body",
        "",
        issue_body,
        "",
        "## Trigger Comment Body",
        "",
        comment_body,
        "",
        "## Additional Context From /gc fix",
        "",
        command_context,
    ]
    return "\n".join(lines)


def run_subprocess(command: list[str], cwd: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )


def publish_imported_identity(config: dict[str, Any]) -> dict[str, Any]:
    """Mirror freshly captured app credentials to the deployment secret store.

    Resolves the publisher command and identity name from the process
    environment with the city workspace env as fallback (matching the
    identity-resolver lookup), then delegates to common.publish_identity.
    Always returns a status dict; failures are logged, never raised.
    """
    workspace_env = city_workspace_env()
    publisher = os.environ.get("GITHUB_INTAKE_IDENTITY_PUBLISHER", "").strip()
    if not publisher:
        publisher = workspace_env.get("GITHUB_INTAKE_IDENTITY_PUBLISHER", "").strip()
    identity = os.environ.get("GITHUB_INTAKE_APP_IDENTITY", "").strip()
    if not identity:
        identity = workspace_env.get("GITHUB_INTAKE_APP_IDENTITY", "").strip()
    app = config.get("app", {})
    if not isinstance(app, dict):
        app = {}
    result = common.publish_identity(dict(app), identity=identity, publisher=publisher)
    status = result.get("status", "")
    if status == "error":
        print(
            f"[{common.current_service_name() or 'github-intake'}] "
            f"identity publish failed: {result.get('detail', '')}",
            file=sys.stderr,
        )
    elif status == "published":
        print(f"[{common.current_service_name() or 'github-intake'}] identity published to secret store")
    return result


def city_workspace_env() -> dict[str, str]:
    city_root = common.city_root()
    if not city_root:
        return {}
    city_toml = os.path.join(city_root, "city.toml")
    try:
        with open(city_toml, "rb") as handle:
            config = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError):
        return {}
    workspace = config.get("workspace", {})
    if not isinstance(workspace, dict):
        return {}
    raw_env = workspace.get("env", {})
    if not isinstance(raw_env, dict):
        return {}
    env: dict[str, str] = {}
    for key, value in raw_env.items():
        name = str(key)
        if not name.startswith(PROFILE_IDENTITY_ENV_PREFIX):
            continue
        text = str(value).strip()
        if text:
            env[name] = text
    return env


def create_fix_bead(request: dict[str, Any], target: str) -> dict[str, Any]:
    rig = rig_from_target(target)
    if not rig:
        return {"status": "dispatch_failed", "reason": "invalid_dispatch_target"}
    city_root = common.city_root() or "."
    bd_cwd = rig_workdir(rig) or city_root
    create_command = gc_bd_command(
        city_root,
        "create",
        "--json",
        build_fix_bead_title(request),
        "-t",
        "task",
        rig=rig,
    )
    try:
        create_result = run_subprocess(create_command, bd_cwd)
    except FileNotFoundError:
        return {"status": "dispatch_failed", "reason": "bead_create_failed", "dispatch_stderr": "gc not available"}
    if create_result.returncode != 0:
        return {
            "status": "dispatch_failed",
            "reason": "bead_create_failed",
            "dispatch_stdout": trim_output(create_result.stdout),
            "dispatch_stderr": trim_output(create_result.stderr),
        }
    created = extract_json_output(create_result.stdout)
    bead_id = str(created.get("id", "")).strip()
    if not bead_id:
        return {
            "status": "dispatch_failed",
            "reason": "bead_create_failed",
            "dispatch_stdout": trim_output(create_result.stdout),
            "dispatch_stderr": trim_output(create_result.stderr),
        }

    metadata = {
        "github_repo_full_name": str(request.get("repository_full_name", "")),
        "github_issue_number": str(request.get("issue_number", "")),
        "github_issue_title": str(request.get("issue_title", "")),
        "github_issue_url": str(request.get("issue_url", "")),
        "github_comment_url": str(request.get("comment_url", "")),
        "github_installation_id": str(request.get("installation_id", "")),
        "github_request_id": str(request.get("request_id", "")),
        "github_default_branch": str(request.get("repository_default_branch", "") or "main"),
        "github_comment_author": str(request.get("comment_author", "")),
    }
    update_command = gc_bd_command(city_root, "update", bead_id, "--notes", build_fix_bead_notes(request), rig=rig)
    for key, value in metadata.items():
        if value:
            update_command.extend(["--set-metadata", f"{key}={value}"])
    try:
        update_result = run_subprocess(update_command, bd_cwd)
    except FileNotFoundError:
        return {
            "status": "dispatch_failed",
            "reason": "bead_update_failed",
            "bead_id": bead_id,
            "dispatch_stderr": "gc not available",
        }
    if update_result.returncode != 0:
        return {
            "status": "dispatch_failed",
            "reason": "bead_update_failed",
            "bead_id": bead_id,
            "dispatch_stdout": trim_output(update_result.stdout),
            "dispatch_stderr": trim_output(update_result.stderr),
        }
    return {"bead_id": bead_id}


def build_fix_vars(request: dict[str, Any], bead_id: str) -> dict[str, str]:
    return {
        "issue": bead_id,
        "github_issue_url": str(request.get("issue_url", "")),
        "github_issue_number": str(request.get("issue_number", "")),
        "github_repo_full_name": str(request.get("repository_full_name", "")),
        "github_installation_id": str(request.get("installation_id", "")),
        "github_comment_url": str(request.get("comment_url", "")),
        "github_request_id": str(request.get("request_id", "")),
        "github_default_branch": str(request.get("repository_default_branch", "") or "main"),
        "github_additional_context": str(request.get("command_context", "")),
    }


def close_failed_bead(bead_id: str, reason: str, rig: str = "") -> bool:
    bead_id = bead_id.strip()
    if not bead_id:
        return True
    city_root = common.city_root() or "."
    bd_cwd = (rig_workdir(rig) or city_root) if rig else city_root
    try:
        set_reason = run_subprocess(
            gc_bd_command(
                city_root,
                "update",
                bead_id,
                "--set-metadata",
                f"close_reason=github:{reason or 'dispatch_failed'}",
                rig=rig,
            ),
            bd_cwd,
        )
        if set_reason.returncode != 0:
            return False
        result = run_subprocess(gc_bd_command(city_root, "close", bead_id, rig=rig), bd_cwd)
    except FileNotFoundError:
        return False
    return result.returncode == 0


def bugflow_duplicate_source(result: subprocess.CompletedProcess[str]) -> bool:
    text = f"{result.stdout}\n{result.stderr}"
    return any(marker in text for marker in BUGFLOW_DUPLICATE_MARKERS)


def bugflow_env(app_cfg: dict[str, Any], installation_id: str) -> dict[str, str]:
    env = os.environ.copy()
    env["GH_TOKEN"] = common.create_installation_token(app_cfg, installation_id)
    return env


def add_router_result(outcome: dict[str, Any], result: subprocess.CompletedProcess[str]) -> None:
    outcome["router_exit_code"] = result.returncode
    outcome["router_stdout"] = trim_output(result.stdout)
    outcome["router_stderr"] = trim_output(result.stderr)
    router_payload = extract_json_output(result.stdout)
    started = router_payload.get("started")
    if isinstance(started, list) and started and isinstance(started[0], dict):
        first = started[0]
        workflow_root_id = str(first.get("workflow_root_id", "")).strip()
        rig_launch_bead_id = str(first.get("rig_launch_bead_id", "")).strip()
        if workflow_root_id:
            outcome["workflow_root_id"] = workflow_root_id
        if rig_launch_bead_id:
            outcome["rig_launch_bead_id"] = rig_launch_bead_id


def bugflow_acknowledgement_body(outcome: dict[str, Any]) -> str:
    source_bead_id = str(outcome.get("bugflow_source_bead_id", "") or outcome.get("bead_id", "")).strip()
    workflow_root_id = str(outcome.get("workflow_root_id", "")).strip()
    rig_launch_bead_id = str(outcome.get("rig_launch_bead_id", "")).strip()
    duplicate = outcome.get("reason") == "duplicate_open_bead"

    lines = [
        "GitHub intake received this `/gc fix` request. The `/gc fix` request is queued in bugflow.",
        "",
        f"- Workflow: `{BUGFLOW_WORKFLOW_FORMULA}`",
    ]
    if source_bead_id:
        lines.append(f"- Source bead: `{source_bead_id}`")
    if workflow_root_id:
        lines.append(f"- Workflow root: `{workflow_root_id}`")
    if rig_launch_bead_id:
        lines.append(f"- Launch bead: `{rig_launch_bead_id}`")
    lines.append("")
    if duplicate:
        lines.append("An open bugflow request already exists for this issue, so I refreshed the router scan instead of creating a duplicate.")
    else:
        lines.append("Bugflow will investigate and classify the report before any implementation path begins.")
    return "\n".join(lines)


def post_bugflow_acknowledgement(request: dict[str, Any], app_cfg: dict[str, Any], outcome: dict[str, Any]) -> None:
    installation_id = str(request.get("installation_id", ""))
    owner = str(request.get("repository_owner", ""))
    repo = str(request.get("repository_name", ""))
    issue_number = str(request.get("issue_number", ""))
    if not installation_id or not owner or not repo or not issue_number:
        outcome["acknowledgement_comment_skipped"] = "missing_issue_context"
        return
    try:
        comment = common.post_issue_comment(
            app_cfg,
            installation_id,
            owner,
            repo,
            issue_number,
            bugflow_acknowledgement_body(outcome),
        )
    except Exception as exc:  # noqa: BLE001 - acknowledgement is best-effort after dispatch.
        outcome["acknowledgement_comment_failed"] = trim_output(str(exc))
        return
    comment_id = str(comment.get("id", "")).strip()
    comment_url = str(comment.get("html_url", "")).strip()
    if comment_id:
        outcome["acknowledgement_comment_id"] = comment_id
    if comment_url:
        outcome["acknowledgement_comment_url"] = comment_url


def bead_id(bead: dict[str, Any]) -> str:
    return str(bead.get("id") or bead.get("ID") or "").strip()


def bead_metadata(bead: dict[str, Any]) -> dict[str, str]:
    raw = bead.get("metadata") or {}
    if not isinstance(raw, dict):
        return {}
    return {str(key): str(value) for key, value in raw.items()}


def addressed_result_id(delivery_id: str, suffix: str) -> str:
    return f"{delivery_id or 'unknown-delivery'}-addressed-{suffix or 'result'}"


def addressed_source_result_id(source_key: str, suffix: str) -> str:
    return f"{source_key}:{suffix}"


def format_addresses(addresses: list[str]) -> str:
    quoted = [f"`{address}`" for address in addresses if address]
    if not quoted:
        return "`configured address`"
    if len(quoted) == 1:
        return quoted[0]
    return ", ".join(quoted[:-1]) + f" and {quoted[-1]}"


PROFILE_APP_CACHE: dict[str, dict[str, str]] = {}


def github_app_config_from_secret(secret: dict[str, str]) -> dict[str, str]:
    app: dict[str, str] = {}
    for key in (
        "app_id",
        "client_id",
        "client_secret",
        "webhook_secret",
        "slug",
        "html_url",
        "name",
        "installation_id",
        "owner",
    ):
        value = str(secret.get(key, "")).strip()
        if value:
            app[key] = value
    private_key = str(secret.get("private_key_pem", "")).strip()
    if private_key:
        if "\\n" in private_key and "-----BEGIN" in private_key:
            private_key = private_key.replace("\\n", "\n")
        app["private_key_pem"] = private_key
    return app


def load_profile_github_app(identity: str) -> dict[str, str]:
    try:
        identity = common.validate_github_app_identity(identity)
    except ValueError as exc:
        raise RuntimeError(str(exc)) from exc
    if not identity:
        return {}
    cached = PROFILE_APP_CACHE.get(identity)
    if cached is not None:
        return dict(cached)

    workspace_env = city_workspace_env()
    resolver = os.environ.get("GITHUB_INTAKE_IDENTITY_RESOLVER", "").strip()
    if not resolver:
        resolver = workspace_env.get("GITHUB_INTAKE_IDENTITY_RESOLVER", "").strip()
    if not resolver:
        raise RuntimeError(
            f"GitHub App identity {identity!r} requires GITHUB_INTAKE_IDENTITY_RESOLVER"
        )
    command = shlex.split(resolver)
    if not command:
        raise RuntimeError("GITHUB_INTAKE_IDENTITY_RESOLVER did not contain a command")
    env = os.environ.copy()
    for key, value in workspace_env.items():
        env.setdefault(key, value)
    env["GITHUB_INTAKE_IDENTITY_RESOLVER"] = resolver
    city_root = common.city_root() or "."
    if city_root != ".":
        env.setdefault("GC_CITY_ROOT", city_root)
    result = run_subprocess(
        [*command, identity],
        city_root,
        env=env,
    )
    if result.returncode != 0:
        detail = trim_output(result.stderr or result.stdout) or f"exit {result.returncode}"
        raise RuntimeError(f"GitHub App identity resolver failed for {identity!r}: {detail}")
    secret_payload = extract_json_value(result.stdout)
    if not isinstance(secret_payload, dict):
        raise RuntimeError(f"GitHub App identity resolver for {identity!r} did not return a JSON object")
    schema_version = str(secret_payload.get("schema_version", "")).strip()
    if schema_version != common.GITHUB_APP_IDENTITY_SCHEMA_VERSION:
        raise RuntimeError(
            f"GitHub App identity resolver for {identity!r} must return "
            f"schema_version={common.GITHUB_APP_IDENTITY_SCHEMA_VERSION!r}"
        )
    secret = {str(key): str(value) for key, value in secret_payload.items() if value is not None}
    ready = str(secret.get("ready", "")).strip().lower()
    if ready and ready not in {"1", "true", "yes"}:
        raise RuntimeError(f"GitHub App identity {identity!r} is not ready")
    app = github_app_config_from_secret(secret)
    PROFILE_APP_CACHE[identity] = dict(app)
    return app


def configured_github_app_identity() -> str:
    identity = os.environ.get("GITHUB_INTAKE_APP_IDENTITY", "").strip()
    if not identity:
        identity = city_workspace_env().get("GITHUB_INTAKE_APP_IDENTITY", "").strip()
    try:
        return common.validate_github_app_identity(identity, "GITHUB_INTAKE_APP_IDENTITY")
    except ValueError as exc:
        raise RuntimeError(str(exc)) from exc


def missing_intake_app_config_fields(app_cfg: dict[str, Any]) -> list[str]:
    return [field for field in INTAKE_APP_CONFIG_REQUIRED_FIELDS if not str(app_cfg.get(field, "")).strip()]


def sync_github_app_config_from_identity(identity: str = "") -> dict[str, Any]:
    identity = identity.strip() or configured_github_app_identity()
    if not identity:
        return {"status": "skipped", "reason": "GITHUB_INTAKE_APP_IDENTITY is not configured"}
    app = load_profile_github_app(identity)
    missing = missing_intake_app_config_fields(app)
    if missing:
        raise RuntimeError(
            f"GitHub App identity {identity!r} is missing required app field(s): {', '.join(missing)}"
        )
    config = common.load_config()
    current_app = config.get("app", {}) if isinstance(config.get("app", {}), dict) else {}
    changed = any(str(current_app.get(key, "")) != str(value) for key, value in app.items() if value)
    config = common.import_app_config(config, app)
    return {
        "status": "updated" if changed else "unchanged",
        "identity": identity,
        "config": common.redact_config(config),
    }


def ensure_webhook_app_config(config: dict[str, Any]) -> dict[str, Any]:
    app_cfg = config.get("app", {}) if isinstance(config.get("app", {}), dict) else {}
    if str(app_cfg.get("webhook_secret", "")).strip():
        return config
    identity = configured_github_app_identity()
    if not identity:
        return config
    sync_github_app_config_from_identity(identity)
    return common.load_effective_config()


def sync_configured_github_app_at_startup() -> None:
    identity = configured_github_app_identity()
    if not identity:
        return
    outcome = sync_github_app_config_from_identity(identity)
    if outcome.get("status") != "skipped":
        print(
            f"[{common.current_service_name() or 'github-intake'}] "
            f"GitHub App config {outcome.get('status')} from identity {identity!r}",
            flush=True,
        )


def addressed_reply_app_config(request: dict[str, Any], app_cfg: dict[str, Any]) -> tuple[dict[str, Any], str]:
    identity = str(request.get("profile_github_app_identity", "")).strip()
    if not identity:
        return app_cfg, str(request.get("installation_id", "")).strip()
    profile_app = load_profile_github_app(identity)
    installation_id = str(request.get("profile_installation_id", "") or profile_app.get("installation_id", "")).strip()
    if not installation_id:
        profile = str(request.get("profile", "")).strip() or str(request.get("address", "")).strip()
        raise RuntimeError(f"GitHub App installation_id is missing for addressed profile {profile!r}")
    return profile_app, installation_id


def post_addressed_reply(
    request: dict[str, Any],
    app_cfg: dict[str, Any],
    body: str,
    outcome: dict[str, Any],
) -> None:
    owner = str(request.get("repository_owner", ""))
    repo = str(request.get("repository_name", ""))
    issue_number = str(request.get("issue_number", ""))
    if not owner or not repo or not issue_number:
        outcome["reply_skipped"] = "missing_issue_context"
        return
    try:
        reply_app_cfg, installation_id = addressed_reply_app_config(request, app_cfg)
        comment = common.post_issue_comment(reply_app_cfg, installation_id, owner, repo, issue_number, body)
    except Exception as exc:  # noqa: BLE001 - reply failure must not drop durable work.
        outcome["reply_failed"] = trim_output(str(exc))
        return
    comment_id = str(comment.get("id", "")).strip()
    comment_url = str(comment.get("html_url", "")).strip()
    if comment_id:
        outcome["reply_comment_id"] = comment_id
    if comment_url:
        outcome["reply_comment_url"] = comment_url


def addressed_router_kick_limit() -> int:
    raw = os.environ.get("GITHUB_INTAKE_ADDRESS_ROUTER_KICK_LIMIT", "50").strip()
    try:
        limit = int(raw)
    except ValueError:
        return 50
    return max(1, limit)


def process_queued_addressed_router(delivery_id: str, source_keys: list[str], processing_key: str) -> None:
    try:
        router = run_addressed_router(limit=addressed_router_kick_limit())
        status = str(router.get("status", "failed")) if isinstance(router, dict) else "failed"
        common.save_address_result(
            {
                "result_id": addressed_result_id(delivery_id, "router-kick"),
                "created_at": common.utcnow(),
                "delivery_id": delivery_id,
                "event": "addressed-router-kick",
                "status": status,
                "source_keys": source_keys,
                "router": router,
            }
        )
    except Exception as exc:  # noqa: BLE001 - persist unexpected router failures for operator inspection.
        common.save_address_result(
            {
                "result_id": addressed_result_id(delivery_id, "router-kick"),
                "created_at": common.utcnow(),
                "delivery_id": delivery_id,
                "event": "addressed-router-kick",
                "status": "failed",
                "reason": f"addressed_router_kick_failed: {exc}",
                "source_keys": source_keys,
                "traceback": trim_output(traceback.format_exc(), 4000),
            }
        )
    finally:
        if processing_key:
            with ADDRESSED_ROUTER_KICK_LOCK:
                ADDRESSED_ROUTER_KICK_DELIVERIES.discard(processing_key)


def enqueue_addressed_router(delivery_id: str, source_keys: list[str]) -> str:
    source_keys = [str(source_key).strip() for source_key in source_keys if str(source_key).strip()]
    if not source_keys:
        return "skipped_no_sources"
    processing_key = delivery_id.strip() or ",".join(source_keys)
    if processing_key:
        with ADDRESSED_ROUTER_KICK_LOCK:
            if processing_key in ADDRESSED_ROUTER_KICK_DELIVERIES:
                return "already_processing"
            ADDRESSED_ROUTER_KICK_DELIVERIES.add(processing_key)
    thread = threading.Thread(
        target=process_queued_addressed_router,
        args=(delivery_id, source_keys, processing_key),
        daemon=True,
    )
    thread.start()
    return "queued"


def addressed_bool_metadata(value: Any, default: bool = True) -> str:
    if value is None:
        return "true" if default else "false"
    if isinstance(value, str):
        return "false" if value.strip().lower() in {"0", "false", "no", "off"} else "true"
    return "true" if bool(value) else "false"


def addressed_source_title(request: dict[str, Any]) -> str:
    repo = str(request.get("repository_full_name", ""))
    number = str(request.get("item_number", ""))
    address = str(request.get("address", ""))
    location = f"{repo}#{number}" if repo and number else repo or "GitHub"
    return f"GitHub addressed message {address} in {location}"[:180]


def addressed_source_description(request: dict[str, Any]) -> str:
    body = str(request.get("cleaned_body", "")).strip() or "(empty)"
    lines = [
        "## GitHub Addressed Message",
        "",
        f"- Address: {request.get('address', '')}",
        f"- Repository: {request.get('repository_full_name', '')}",
        f"- Source: {request.get('item_url', '')}",
        f"- Comment: {request.get('comment_url', '')}",
        f"- Requested By: {request.get('comment_author', '')}",
        f"- Source Key: {request.get('source_key', '')}",
        "",
        "## Request",
        "",
        body,
    ]
    return "\n".join(lines)


def addressed_source_metadata(request: dict[str, Any]) -> dict[str, str]:
    rig = str(request.get("rig", ""))
    pool = str(request.get("pool", ""))
    target = str(request.get("target", ""))
    if not target and rig and pool:
        target = f"{rig}/{pool}"
    values = {
        "external.provider": "github",
        "external.kind": "addressed-message",
        "external.source_key": str(request.get("source_key", "")),
        "github.repo": str(request.get("repository_full_name", "")),
        "github.repo_id": str(request.get("repository_id", "")),
        "github.item_kind": str(request.get("item_kind", "")),
        "github.item_number": str(request.get("item_number", "")),
        "github.item_url": str(request.get("item_url", "")),
        "github.issue_number": str(request.get("issue_number", "")),
        "github.comment_id": str(request.get("comment_id", "")),
        "github.comment_url": str(request.get("comment_url", "")),
        "github.sender": str(request.get("comment_author", "")),
        "github.installation_id": str(request.get("installation_id", "")),
        "addressed.address": str(request.get("address", "")),
        "addressed.cleaned_body": str(request.get("cleaned_body", "")),
        "addressed.rig": rig,
        "addressed.pool": pool,
        "addressed.target": target,
        "addressed.profile": str(request.get("profile", "")),
        "addressed.github_app_identity": str(request.get("profile_github_app_identity", "")),
        "addressed.github_app_installation_id": str(request.get("profile_installation_id", "")),
        "addressed.formula": str(request.get("formula", "")),
        "addressed.ack_requested": addressed_bool_metadata(request.get("ack", True)),
        "addressed.status": "source_created",
        "addressed.created_at": common.utcnow(),
        "intake.delivery_id": str(request.get("delivery_id", "")),
        "raw_payload.path": str(request.get("raw_payload_path", "")),
    }
    return {key: value for key, value in values.items() if value}


def addressed_sources_by_key(source_key: str) -> list[dict[str, Any]]:
    city_root = common.city_root() or "."
    result = run_subprocess(
        gc_bd_command(
            city_root,
            "list",
            "--json",
            "--all",
            "--metadata-field",
            f"external.source_key={source_key}",
            "--limit",
            "0",
        ),
        city_root,
    )
    if result.returncode != 0:
        raise RuntimeError(f"gc bd list failed: {trim_output(result.stderr or result.stdout)}")
    payload = extract_json_value(result.stdout)
    if not isinstance(payload, list):
        return []
    return [item for item in payload if isinstance(item, dict)]


def create_addressed_source(request: dict[str, Any]) -> dict[str, Any]:
    source_key = str(request.get("source_key", "")).strip()
    if not source_key:
        return {"status": "failed", "reason": "source_key_missing"}
    try:
        existing = addressed_sources_by_key(source_key)
    except FileNotFoundError:
        return {"status": "failed", "reason": "bd_not_available"}
    except Exception as exc:  # noqa: BLE001
        return {"status": "failed", "reason": "source_lookup_failed", "detail": trim_output(str(exc))}
    if existing:
        existing_id = bead_id(existing[0])
        return {
            "status": "duplicate",
            "reason": "source_key_exists",
            "bead_id": existing_id,
            "source_key": source_key,
            "address": request.get("address", ""),
        }

    city_root = common.city_root() or "."
    metadata = addressed_source_metadata(request)
    command = gc_bd_command(
        city_root,
        "create",
        "--json",
        addressed_source_title(request),
        "-t",
        "task",
        "--description",
        addressed_source_description(request),
        "--labels",
        "github-intake,addressed-message",
        "--external-ref",
        source_key,
        "--metadata",
        json.dumps(metadata, sort_keys=True),
    )
    try:
        result = run_subprocess(command, city_root)
    except FileNotFoundError:
        return {"status": "failed", "reason": "bd_not_available"}
    if result.returncode != 0:
        return {
            "status": "failed",
            "reason": "source_create_failed",
            "source_key": source_key,
            "stdout": trim_output(result.stdout),
            "stderr": trim_output(result.stderr),
        }
    created = extract_json_output(result.stdout)
    created_id = str(created.get("id", "")).strip()
    if not created_id:
        return {
            "status": "failed",
            "reason": "source_create_invalid_json",
            "source_key": source_key,
            "stdout": trim_output(result.stdout),
            "stderr": trim_output(result.stderr),
        }
    return {
        "status": "created",
        "bead_id": created_id,
        "source_key": source_key,
        "address": request.get("address", ""),
    }


def comment_from_bot(payload: dict[str, Any], app_cfg: dict[str, Any]) -> bool:
    comment = payload.get("comment") or {}
    user = comment.get("user") or {}
    login = str(user.get("login", ""))
    user_type = str(user.get("type", ""))
    bot_login = common.app_bot_login(app_cfg)
    if bot_login and login.lower() == bot_login.lower():
        return True
    return user_type.lower() == "bot" or login.lower().endswith("[bot]")


def process_addressed_comment(
    event: str,
    delivery_id: str,
    payload: dict[str, Any],
    app_cfg: dict[str, Any],
) -> dict[str, Any]:
    if event != "issue_comment":
        return {"status": "ignored", "reason": "not_issue_comment"}
    try:
        rules_config = common.load_rules()
    except Exception as exc:  # noqa: BLE001
        result = {
            "result_id": addressed_result_id(delivery_id, "rules-load-failed"),
            "created_at": common.utcnow(),
            "delivery_id": delivery_id,
            "event": event,
            "status": "failed",
            "reason": f"rules_load_failed: {exc}",
        }
        common.save_address_result(result)
        return result
    extracted = common.extract_addressed_comment_requests(payload, rules_config)
    if not extracted:
        return {"status": "ignored", "reason": "no_configured_address_match"}
    if comment_from_bot(payload, app_cfg):
        result = {
            "result_id": addressed_result_id(delivery_id, "comment-from-bot"),
            "created_at": common.utcnow(),
            "delivery_id": delivery_id,
            "event": event,
            "status": "ignored",
            "reason": "comment_from_bot",
            "addresses": extracted.get("addresses", []),
        }
        common.save_address_result(result)
        return result

    first_request = dict((extracted.get("requests") or [{}])[0])
    first_request["delivery_id"] = delivery_id
    first_request["raw_payload_path"] = common.delivery_path(delivery_id or "unknown-delivery")
    addresses = [str(address) for address in extracted.get("addresses", [])]
    first_source_key = str(first_request.get("source_key", ""))
    if not extracted.get("authorized"):
        result_id = addressed_source_result_id(first_source_key, "sender-not-authorized")
        existing = common.load_address_result(result_id) if first_source_key else None
        if existing:
            return {
                "result_id": result_id,
                "created_at": common.utcnow(),
                "delivery_id": delivery_id,
                "event": event,
                "status": "duplicate",
                "reason": "sender_not_authorized_already_reported",
                "addresses": addresses,
            }
        result = {
            "result_id": result_id or addressed_result_id(delivery_id, "unauthorized"),
            "created_at": common.utcnow(),
            "delivery_id": delivery_id,
            "event": event,
            "status": "rejected",
            "reason": "sender_not_authorized",
            "sender": extracted.get("sender", ""),
            "addresses": addresses,
        }
        post_addressed_reply(
            first_request,
            app_cfg,
            f"You are not authorized to use {format_addresses(addresses)} in this repository. No work was created.",
            result,
        )
        common.save_address_result(result)
        return result
    if not str(extracted.get("cleaned_body", "")).strip():
        result_id = addressed_source_result_id(first_source_key, "empty-request")
        existing = common.load_address_result(result_id) if first_source_key else None
        if existing:
            return {
                "result_id": result_id,
                "created_at": common.utcnow(),
                "delivery_id": delivery_id,
                "event": event,
                "status": "duplicate",
                "reason": "empty_request_already_reported",
                "addresses": addresses,
            }
        result = {
            "result_id": result_id or addressed_result_id(delivery_id, "empty-request"),
            "created_at": common.utcnow(),
            "delivery_id": delivery_id,
            "event": event,
            "status": "rejected",
            "reason": "empty_request",
            "addresses": addresses,
        }
        post_addressed_reply(
            first_request,
            app_cfg,
            f"{format_addresses(addresses)} needs a request after the mention. No work was created.",
            result,
        )
        common.save_address_result(result)
        return result

    created: list[dict[str, Any]] = []
    duplicates: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for raw_request in extracted.get("requests") or []:
        request = dict(raw_request)
        request["delivery_id"] = delivery_id
        request["raw_payload_path"] = common.delivery_path(delivery_id or "unknown-delivery")
        outcome = create_addressed_source(request)
        outcome["address"] = request.get("address", "")
        outcome["ack"] = addressed_bool_metadata(request.get("ack", True)) == "true"
        if outcome.get("status") == "created":
            created.append(outcome)
        elif outcome.get("status") == "duplicate":
            duplicates.append(outcome)
        else:
            failures.append(outcome)

    result = {
        "result_id": addressed_result_id(delivery_id, "processed"),
        "created_at": common.utcnow(),
        "delivery_id": delivery_id,
        "event": event,
        "status": "failed" if failures else "accepted",
        "addresses": addresses,
        "created_count": len(created),
        "duplicate_count": len(duplicates),
        "failure_count": len(failures),
        "created": created,
        "duplicates": duplicates,
        "failures": failures,
    }
    source_keys = [str(item.get("source_key", "")).strip() for item in created + duplicates]
    if source_keys:
        result["router_kick"] = enqueue_addressed_router(delivery_id, source_keys)
    if failures:
        result["reason"] = "source_create_failed"
    common.save_address_result(result)
    return result


def run_fix_bugflow_dispatch(request: dict[str, Any], app_cfg: dict[str, Any]) -> dict[str, Any]:
    installation_id = str(request.get("installation_id", ""))
    owner = str(request.get("repository_owner", ""))
    repo = str(request.get("repository_name", ""))
    commenter = str(request.get("comment_author", ""))
    issue_url = str(request.get("issue_url", "")).strip()
    if not app_cfg or not installation_id or not owner or not repo:
        return {"status": "ignored", "reason": "github_app_not_configured"}
    if not issue_url:
        return {"status": "dispatch_failed", "reason": "issue_url_missing"}
    try:
        permission = common.repository_permission(app_cfg, installation_id, owner, repo, commenter)
    except Exception:  # noqa: BLE001
        return {"status": "dispatch_failed", "reason": "permission_lookup_failed"}
    if permission not in WRITE_PERMISSION_LEVELS:
        return {
            "status": "ignored",
            "reason": "comment_author_lacks_write",
            "requester_permission": permission,
        }
    try:
        env = bugflow_env(app_cfg, installation_id)
    except Exception as exc:  # noqa: BLE001
        return {
            "status": "dispatch_failed",
            "reason": "github_app_token_failed",
            "dispatch_stderr": trim_output(str(exc)),
            "requester_permission": permission,
        }

    gc_bin = os.environ.get("GC_BIN", "gc")
    create_command = [gc_bin, "workflows", "bugflow", "create", issue_url]
    try:
        create_result = run_subprocess(create_command, common.city_root() or ".", env=env)
    except FileNotFoundError:
        return {
            "status": "dispatch_failed",
            "reason": "gc_not_available",
            "dispatch_formula": BUGFLOW_WORKFLOW_FORMULA,
            "dispatch_command": create_command,
            "requester_permission": permission,
        }

    create_payload = extract_json_output(create_result.stdout)
    source_bead_id = str(create_payload.get("bead_id", "") or create_payload.get("id", "")).strip()
    outcome = {
        "dispatch_target": "workflows.bugflow-router",
        "dispatch_formula": BUGFLOW_WORKFLOW_FORMULA,
        "dispatch_command": create_command,
        "dispatch_exit_code": create_result.returncode,
        "dispatch_stdout": trim_output(create_result.stdout),
        "dispatch_stderr": trim_output(create_result.stderr),
        "requester_permission": permission,
    }
    if source_bead_id:
        outcome["bead_id"] = source_bead_id
        outcome["bugflow_source_bead_id"] = source_bead_id

    duplicate = bugflow_duplicate_source(create_result)
    if create_result.returncode != 0 and not duplicate:
        outcome["status"] = "dispatch_failed"
        outcome["reason"] = "dispatch_failed"
        return outcome
    if duplicate:
        outcome["reason"] = "duplicate_open_bead"

    router_command = [gc_bin, "workflows", "bugflow", "router-scan"]
    outcome["router_command"] = router_command
    try:
        router_result = run_subprocess(router_command, common.city_root() or ".", env=env)
    except FileNotFoundError:
        outcome["status"] = "dispatch_failed"
        outcome["reason"] = "gc_not_available"
        return outcome
    add_router_result(outcome, router_result)
    if router_result.returncode != 0:
        outcome["status"] = "dispatch_failed"
        outcome["reason"] = "bugflow_router_failed"
        return outcome
    outcome["status"] = "dispatched"
    post_bugflow_acknowledgement(request, app_cfg, outcome)
    return outcome


def run_fix_issue_dispatch(
    request: dict[str, Any],
    mapping: dict[str, Any],
    command_cfg: dict[str, Any],
    app_cfg: dict[str, Any],
) -> dict[str, Any]:
    formula = str(command_cfg.get("formula", ""))
    target = str(mapping.get("target", ""))
    if not formula or not target:
        return {"status": "ignored", "reason": "command_not_configured"}
    installation_id = str(request.get("installation_id", ""))
    owner = str(request.get("repository_owner", ""))
    repo = str(request.get("repository_name", ""))
    commenter = str(request.get("comment_author", ""))
    if not app_cfg or not installation_id or not owner or not repo:
        return {"status": "ignored", "reason": "github_app_not_configured"}
    try:
        permission = common.repository_permission(app_cfg, installation_id, owner, repo, commenter)
    except Exception:  # noqa: BLE001
        return {"status": "dispatch_failed", "reason": "permission_lookup_failed"}
    if permission not in WRITE_PERMISSION_LEVELS:
        return {
            "status": "ignored",
            "reason": "comment_author_lacks_write",
            "requester_permission": permission,
        }

    rig = rig_from_target(target)
    bead_outcome = create_fix_bead(request, target)
    if bead_outcome.get("status") == "dispatch_failed":
        cleanup_ok = close_failed_bead(str(bead_outcome.get("bead_id", "")), str(bead_outcome.get("reason", "")), rig)
        if cleanup_ok:
            bead_outcome["bead_closed"] = True
        else:
            bead_outcome["cleanup_failed"] = True
        return bead_outcome
    if "bead_id" not in bead_outcome:
        return bead_outcome
    bead_id = str(bead_outcome["bead_id"])
    request["bead_id"] = bead_id

    gc_bin = os.environ.get("GC_BIN", "gc")
    command = [gc_bin, "sling", target, bead_id, "--on", formula]
    for key, value in build_fix_vars(request, bead_id).items():
        if value:
            command.extend(["--var", f"{key}={value}"])
    try:
        result = run_subprocess(command, common.city_root() or ".")
    except FileNotFoundError:
        cleanup_ok = close_failed_bead(bead_id, "gc_not_available", rig)
        outcome = {
            "status": "dispatch_failed",
            "reason": "gc_not_available",
            "bead_id": bead_id,
        }
        if cleanup_ok:
            outcome["bead_closed"] = True
        else:
            outcome["cleanup_failed"] = True
        return outcome
    outcome = {
        "bead_id": bead_id,
        "dispatch_target": target,
        "dispatch_formula": formula,
        "dispatch_command": command,
        "dispatch_exit_code": result.returncode,
        "dispatch_stdout": trim_output(result.stdout),
        "dispatch_stderr": trim_output(result.stderr),
        "requester_permission": permission,
    }
    if result.returncode == 0:
        outcome["status"] = "dispatched"
    else:
        outcome["status"] = "dispatch_failed"
        outcome["reason"] = "dispatch_failed"
        if close_failed_bead(bead_id, "dispatch_failed", rig):
            outcome["bead_closed"] = True
        else:
            outcome["cleanup_failed"] = True
    return outcome


def process_request(request_id: str) -> None:
    request: dict[str, Any] | None = None
    workflow_key_hint = ""
    try:
        request = common.load_request(request_id)
        if not request:
            return
        workflow_key_hint = str(request.get("workflow_key", ""))
        config = common.load_effective_config()
        app_cfg = config.get("app", {})
        mapping = common.resolve_repo_mapping(
            config,
            str(request.get("repository_full_name", "")),
            str(request.get("repository_id", "")),
        )
        behavior = command_behavior(str(request.get("command", "")))
        if not behavior:
            request["status"] = "ignored"
            request["reason"] = "command_not_supported"
        elif str(request.get("command", "")) == "fix":
            outcome = run_fix_bugflow_dispatch(request, app_cfg if isinstance(app_cfg, dict) else {})
            request.update(outcome)
        elif not mapping:
            request["status"] = "ignored"
            request["reason"] = "repo_mapping_missing"
        else:
            commands = mapping.get("commands", {})
            command_cfg = commands.get(str(request.get("command", "")), {})
            outcome = run_fix_issue_dispatch(request, mapping, command_cfg, app_cfg if isinstance(app_cfg, dict) else {})
            request.update(outcome)
        common.save_request(request)
    except Exception as exc:  # noqa: BLE001
        payload = request or common.load_request(request_id) or {"request_id": request_id}
        bead_id = str(payload.get("bead_id", ""))
        rig = rig_from_target(str(payload.get("dispatch_target", "")))
        if bead_id and not payload.get("bead_closed"):
            if close_failed_bead(bead_id, "internal_error", rig):
                payload["bead_closed"] = True
            else:
                payload["cleanup_failed"] = True
        payload["status"] = "internal_error"
        payload["reason"] = str(exc)
        payload["traceback"] = traceback.format_exc(limit=20)
        common.save_request(payload)
        request = payload
    finally:
        if request:
            workflow_key = str(request.get("workflow_key", "")) or workflow_key_hint
            if (
                workflow_key
                and request.get("status") in {"ignored", "dispatch_failed", "internal_error"}
                and not request.get("cleanup_failed")
            ):
                common.remove_workflow_link_if_request(workflow_key, request_id)
        with PROCESSING_LOCK:
            PROCESSING_REQUESTS.discard(request_id)


def reserve_request(request: dict[str, Any], behavior: dict[str, Any]) -> dict[str, Any] | None:
    with ACCEPTANCE_LOCK:
        existing = common.load_request(request["request_id"])
        if existing:
            return existing
        workflow_key = str(request.get("workflow_key", ""))
        if behavior.get("workflow_scope") == "issue" and workflow_key:
            workflow_link = common.load_workflow_link(workflow_key)
            if workflow_link:
                existing_request_id = str(workflow_link.get("request_id", ""))
                return common.load_request(existing_request_id) or {
                    "request_id": existing_request_id,
                    "workflow_key": workflow_key,
                    "status": "duplicate",
                    "command": request.get("command", ""),
                    "issue_number": request.get("issue_number", ""),
                    "repository_full_name": request.get("repository_full_name", ""),
                }
        common.save_request(request)
        if behavior.get("workflow_scope") == "issue" and workflow_key:
            common.save_workflow_link(workflow_key, request["request_id"])
    return None


def enqueue_request(request_id: str) -> None:
    with PROCESSING_LOCK:
        if request_id in PROCESSING_REQUESTS:
            return
        PROCESSING_REQUESTS.add(request_id)
    thread = threading.Thread(target=process_request, args=(request_id,), daemon=True)
    thread.start()


def process_queued_event_rules(
    event: str,
    delivery_id: str,
    payload: dict[str, Any],
    app_cfg: dict[str, Any],
    processing_key: str,
) -> None:
    try:
        process_event_rules(event, delivery_id, payload, app_cfg)
    except Exception as exc:  # noqa: BLE001 - persist unexpected rule processor failures for operator inspection.
        common.save_rule_result(
            {
                "result_id": f"{delivery_id or 'unknown-delivery'}-rules-processing-failed",
                "created_at": common.utcnow(),
                "delivery_id": delivery_id,
                "event": event,
                "status": "failed",
                "reason": f"rules_processing_failed: {exc}",
                "traceback": trim_output(traceback.format_exc(), 4000),
                "actions": [],
            }
        )
    finally:
        if processing_key:
            with RULE_PROCESSING_LOCK:
                RULE_PROCESSING_DELIVERIES.discard(processing_key)


def enqueue_event_rules(event: str, delivery_id: str, payload: dict[str, Any], app_cfg: dict[str, Any]) -> str:
    processing_key = delivery_id.strip()
    if processing_key:
        with RULE_PROCESSING_LOCK:
            if processing_key in RULE_PROCESSING_DELIVERIES:
                return "already_processing"
            RULE_PROCESSING_DELIVERIES.add(processing_key)
    thread = threading.Thread(
        target=process_queued_event_rules,
        args=(event, delivery_id, payload, app_cfg, processing_key),
        daemon=True,
    )
    thread.start()
    return "queued"


def render_template(value: str, payload: dict[str, Any]) -> str:
    out = value
    while "{{" in out and "}}" in out:
        start = out.find("{{")
        end = out.find("}}", start + 2)
        if end == -1:
            break
        key = out[start + 2 : end].strip()
        replacement = common.payload_value(payload, key)
        out = out[:start] + ("" if replacement is None else str(replacement)) + out[end + 2 :]
    return out


def github_event_env(event: str, delivery_id: str, payload: dict[str, Any], payload_file: str) -> dict[str, str]:
    repository = payload.get("repository") or {}
    owner = repository.get("owner") or {}
    pull_request = payload.get("pull_request") or {}
    issue = payload.get("issue") or {}
    item_kind = "pr" if pull_request else ("issue" if issue else "")
    item = pull_request or issue
    label = payload.get("label") or {}
    sender = payload.get("sender") or {}
    installation = payload.get("installation") or {}
    values = {
        "GC_GITHUB_EVENT": event,
        "GC_GITHUB_DELIVERY_ID": delivery_id,
        "GC_GITHUB_EVENT_PAYLOAD_FILE": payload_file,
        "GC_GITHUB_ACTION": str(payload.get("action", "")),
        "GC_GITHUB_REPO": str(repository.get("full_name", "")).lower(),
        "GC_GITHUB_REPOSITORY_ID": str(repository.get("id", "")),
        "GC_GITHUB_REPOSITORY_OWNER": str(owner.get("login", "")),
        "GC_GITHUB_REPOSITORY_NAME": str(repository.get("name", "")),
        "GC_GITHUB_PR_NUMBER": str(pull_request.get("number", "")),
        "GC_GITHUB_PR_URL": str(pull_request.get("html_url", "")),
        "GC_GITHUB_PR_STATE": str(pull_request.get("state", "")),
        "GC_GITHUB_ISSUE_NUMBER": str(issue.get("number", "")),
        "GC_GITHUB_ISSUE_URL": str(issue.get("html_url", "")),
        "GC_GITHUB_ISSUE_STATE": str(issue.get("state", "")),
        "GC_GITHUB_ITEM_KIND": item_kind,
        "GC_GITHUB_ITEM_NUMBER": str(item.get("number", "")),
        "GC_GITHUB_ITEM_URL": str(item.get("html_url", "")),
        "GC_GITHUB_ITEM_STATE": str(item.get("state", "")),
        "GC_GITHUB_LABEL_NAME": str(label.get("name", "")),
        "GC_GITHUB_SENDER": str(sender.get("login", "")),
        "GC_GITHUB_INSTALLATION_ID": str(installation.get("id", "")),
    }
    return {key: value for key, value in values.items() if value}


def action_success(action: dict[str, Any], result: subprocess.CompletedProcess[str]) -> bool:
    raw_codes = action.get("success_exit_codes", [0])
    if not isinstance(raw_codes, list):
        raw_codes = [raw_codes]
    success_codes = {int(code) for code in raw_codes}
    if result.returncode in success_codes:
        return True
    for value in action.get("success_stderr_contains") or []:
        if str(value) and str(value) in result.stderr:
            return True
    return False


def action_env(
    action: dict[str, Any],
    event: str,
    delivery_id: str,
    payload: dict[str, Any],
    payload_file: str,
    app_cfg: dict[str, Any],
) -> tuple[dict[str, str], str]:
    env = os.environ.copy()
    env.update(github_event_env(event, delivery_id, payload, payload_file))
    extra_env = action.get("env") or {}
    if isinstance(extra_env, dict):
        for key, value in extra_env.items():
            env[str(key)] = render_template(str(value), payload)
    token_env = str(action.get("github_app_token_env", "")).strip()
    if token_env:
        installation_id = str((payload.get("installation") or {}).get("id", ""))
        if not installation_id:
            raise RuntimeError("github_app_token_env requires payload.installation.id")
        env[token_env] = common.create_installation_token(app_cfg, installation_id)
    return env, token_env


def execute_rule_action(
    rule: dict[str, Any],
    action: dict[str, Any],
    event: str,
    delivery_id: str,
    payload: dict[str, Any],
    payload_file: str,
    app_cfg: dict[str, Any],
    rules_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    started = common.utcnow()
    action_type = str(action.get("type", "")).strip()
    dispatch_route: dict[str, str] = {}
    if action_type == "order":
        order_name = render_template(str(action.get("name", "")), payload)
        if bool(action.get("route_from_repo")):
            repository = str((payload.get("repository") or {}).get("full_name", ""))
            dispatch_route = common.github_repo_dispatch_route(repository, rules_config)
            if not dispatch_route:
                return {
                    "type": action_type,
                    "status": "failed",
                    "reason": "repository_route_not_configured",
                    "started_at": started,
                    "finished_at": common.utcnow(),
                }
            command = [
                "gc",
                "--city",
                dispatch_route["city"],
                "order",
                "run",
                order_name,
                "--rig",
                dispatch_route["rig"],
            ]
        else:
            command = ["gc", "order", "run", order_name]
            rig = str(action.get("rig", "")).strip()
            if rig:
                command.extend(["--rig", render_template(rig, payload)])
    elif action_type == "command":
        raw_command = action.get("command") or []
        if not isinstance(raw_command, list) or not raw_command:
            return {
                "type": action_type,
                "status": "failed",
                "reason": "command_action_requires_command_array",
                "started_at": started,
                "finished_at": common.utcnow(),
            }
        command = [render_template(str(part), payload) for part in raw_command]
    else:
        return {
            "type": action_type,
            "status": "failed",
            "reason": "unsupported_action_type",
            "started_at": started,
            "finished_at": common.utcnow(),
        }

    try:
        env, token_env = action_env(action, event, delivery_id, payload, payload_file, app_cfg)
        if dispatch_route:
            env["GC_GITHUB_DISPATCH_CITY"] = dispatch_route["city"]
            env["GC_GITHUB_DISPATCH_RIG"] = dispatch_route["rig"]
        result = run_subprocess(command, common.city_root() or ".", env=env)
    except Exception as exc:  # noqa: BLE001
        return {
            "type": action_type,
            "status": "failed",
            "reason": str(exc),
            "command": command,
            "started_at": started,
            "finished_at": common.utcnow(),
        }
    status = "success" if action_success(action, result) else "failed"
    outcome = {
        "type": action_type,
        "status": status,
        "command": command,
        "exit_code": result.returncode,
        "stdout": trim_output(result.stdout),
        "stderr": trim_output(result.stderr),
        "started_at": started,
        "finished_at": common.utcnow(),
    }
    if token_env:
        outcome["github_app_token_env"] = token_env
        outcome["github_app_token_injected"] = True
    if status != "success":
        outcome["reason"] = "action_failed"
    return outcome


def process_event_rules(event: str, delivery_id: str, payload: dict[str, Any], app_cfg: dict[str, Any]) -> list[dict[str, Any]]:
    try:
        rules_config = common.load_rules()
    except Exception as exc:  # noqa: BLE001
        result = {
            "result_id": f"{delivery_id or 'unknown-delivery'}-rules-load-failed",
            "created_at": common.utcnow(),
            "delivery_id": delivery_id,
            "event": event,
            "status": "failed",
            "reason": f"rules_load_failed: {exc}",
            "actions": [],
        }
        common.save_rule_result(result)
        return [result]

    payload_file = common.delivery_path(delivery_id or "unknown-delivery")
    summaries: list[dict[str, Any]] = []
    rules = common.matching_rules(event, payload, rules_config)
    bot_login = common.app_bot_login(app_cfg)
    sender = str((payload.get("sender") or {}).get("login", ""))
    if bot_login and sender.lower() == bot_login.lower():
        rules = [rule for rule in rules if bool(rule.get("allow_self"))]

    for rule in rules:
        result = {
            "result_id": f"{delivery_id or 'unknown-delivery'}-{rule.get('id', 'rule')}",
            "created_at": common.utcnow(),
            "delivery_id": delivery_id,
            "event": event,
            "rule_id": rule.get("id", ""),
            "status": "success",
            "actions": [],
        }
        for index, action in enumerate(rule.get("action") or []):
            outcome = execute_rule_action(
                rule,
                action,
                event,
                delivery_id,
                payload,
                payload_file,
                app_cfg,
                rules_config,
            )
            outcome["index"] = index
            result["actions"].append(outcome)
            if outcome.get("status") != "success":
                result["status"] = "failed"
                result["reason"] = outcome.get("reason", "action_failed")
                break
        common.save_rule_result(result)
        summaries.append(
            {
                "result_id": result["result_id"],
                "rule_id": result.get("rule_id", ""),
                "status": result.get("status", ""),
                "reason": result.get("reason", ""),
            }
        )
    return summaries


def list_addressed_router_sources(limit: int) -> list[dict[str, Any]]:
    city_root = common.city_root() or "."
    result = run_subprocess(
        gc_bd_command(
            city_root,
            "list",
            "--json",
            "--status",
            "open",
            "--metadata-field",
            "external.kind=addressed-message",
            "--limit",
            str(limit),
        ),
        city_root,
    )
    if result.returncode != 0:
        raise RuntimeError(f"gc bd list failed: {trim_output(result.stderr or result.stdout)}")
    payload = extract_json_value(result.stdout)
    if not isinstance(payload, list):
        return []
    return [item for item in payload if isinstance(item, dict)]


def update_bead_metadata(bead: str, values: dict[str, str]) -> subprocess.CompletedProcess[str]:
    city_root = common.city_root() or "."
    command = gc_bd_command(city_root, "update", bead)
    for key, value in values.items():
        if value:
            command.extend(["--set-metadata", f"{key}={value}"])
    return run_subprocess(command, city_root)


def close_addressed_source(source_id: str) -> subprocess.CompletedProcess[str]:
    city_root = common.city_root() or "."
    return run_subprocess(
        gc_bd_command(city_root, "close", source_id, "--reason", "github addressed message dispatched"),
        city_root,
    )


def addressed_router_vars(source_id: str, metadata: dict[str, str]) -> dict[str, str]:
    installation_id = metadata.get("addressed.github_app_installation_id", "") or metadata.get("github.installation_id", "")
    return {
        "source_bead_id": source_id,
        "city_root": metadata.get("addressed.city_source_city", "") or common.city_root(),
        "address": metadata.get("addressed.address", ""),
        "request_text": metadata.get("addressed.cleaned_body", ""),
        "github_repo": metadata.get("github.repo", ""),
        "github_issue_number": metadata.get("github.issue_number", ""),
        "github_item_url": metadata.get("github.item_url", ""),
        "github_comment_url": metadata.get("github.comment_url", ""),
        "github_sender": metadata.get("github.sender", ""),
        "github_app_identity": metadata.get("addressed.github_app_identity", ""),
        "github_app_installation_id": installation_id,
        "acknowledgement_requested": metadata.get("addressed.ack_requested", "true") or "true",
        "external_source_key": metadata.get("external.source_key", ""),
        "raw_payload_path": metadata.get("raw_payload.path", ""),
    }


def addressed_route_target(metadata: dict[str, str]) -> str:
    pool = metadata.get("addressed.pool", "").strip()
    try:
        rig = common.github_repo_dispatch_rig(metadata.get("github.repo", ""))
    except Exception:  # noqa: BLE001
        rig = common.github_repo_rig_name(metadata.get("github.repo", ""))
    if rig and pool and "/" not in pool:
        return f"{rig}/{pool}"
    return ""


def addressed_rig_launch_description(source_id: str, metadata: dict[str, str], formula: str) -> str:
    repo = metadata.get("github.repo", "")
    issue_number = metadata.get("github.issue_number", "")
    return "\n".join(
        [
            "GitHub addressed-message rig launch bead.",
            "",
            f"Address: {metadata.get('addressed.address', '')}",
            f"Repository: {metadata.get('github.repo', '')}",
            f"Source: {metadata.get('github.item_url', '')}",
            f"Comment: {metadata.get('github.comment_url', '')}",
            f"Requested By: {metadata.get('github.sender', '')}",
            f"City source bead: {source_id}",
            "",
            f"{formula} is attached to this rig-local bead so the request runs in the same bead database as the target agent.",
            "",
            "## GitHub Response Contract",
            "",
            "The GitHub thread is the user-visible response channel.",
            "Post acknowledgements, dry-run plans, results, and blockers back to the GitHub thread using the addressed profile identity.",
            f"Use: gc github comment-issue {repo} {issue_number} --installation-id <profile-installation-id> --github-app-identity <profile-identity> --body-file <file>",
            "Local session output alone does not complete this request.",
            f"Record response comment IDs or posting failures on the city source bead `{source_id}`.",
        ]
    )


def addressed_rig_launch_metadata(source_id: str, metadata: dict[str, str], target: str, formula: str) -> dict[str, str]:
    launch_metadata = {
        key: value
        for key, value in metadata.items()
        if not key.startswith("addressed.router_")
        and key
        not in {
            "addressed.dispatched_at",
            "addressed.rig_launch_bead",
            "addressed.workflow_root",
            "addressed.workflow_store",
        }
    }
    launch_metadata.update(
        {
            "gc.source_bead_id": source_id,
            "gc.source_store_ref": f"city:{common.city_name()}",
            "addressed.city_source_bead_id": source_id,
            "addressed.city_source_city": common.city_root() or "",
            "addressed.status": "rig_launch_created",
            "addressed.workflow_formula": formula,
            "addressed.workflow_role": "rig-launch",
            "addressed.workflow_target": target,
        }
    )
    return launch_metadata


def create_addressed_rig_launch_bead(
    source_id: str,
    source: dict[str, Any],
    metadata: dict[str, str],
    target: str,
    formula: str,
) -> dict[str, Any]:
    rig = rig_from_target(target)
    if not rig:
        return {"status": "failed", "reason": "missing_rig"}
    gc_bin = os.environ.get("GC_BIN", "gc")
    city_root = common.city_root() or "."
    launch_metadata = addressed_rig_launch_metadata(source_id, metadata, target, formula)
    command = [gc_bin]
    if city_root not in {"", "."}:
        command.extend(["--city", city_root])
    command.extend([
        "--rig", rig, "bd",
        "create",
        str(source.get("title") or addressed_source_title(metadata)),
        "--type",
        "task",
        "--description",
        addressed_rig_launch_description(source_id, metadata, formula),
        "--external-ref",
        metadata.get("external.source_key", ""),
        "--metadata",
        json.dumps(launch_metadata, sort_keys=True),
        "--json",
    ])
    try:
        result = run_subprocess(command, city_root)
    except FileNotFoundError:
        return {"status": "failed", "reason": "gc_not_available"}
    if result.returncode != 0:
        return {
            "status": "failed",
            "reason": "rig_launch_failed",
            "exit_code": result.returncode,
            "stdout": trim_output(result.stdout),
            "stderr": trim_output(result.stderr),
        }
    created = extract_json_output(result.stdout)
    launch_id = str(created.get("id", "")).strip()
    if not launch_id:
        return {
            "status": "failed",
            "reason": "rig_launch_invalid_json",
            "stdout": trim_output(result.stdout),
            "stderr": trim_output(result.stderr),
        }
    return {"status": "created", "bead_id": launch_id, "rig": rig}


def workflow_root_from_sling_output(output: str) -> str:
    payload = extract_json_output(output)
    for key in ("workflow_id", "molecule_id", "bead_id"):
        value = str(payload.get(key, "")).strip()
        if value:
            return value
    return ""


def parse_utc(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def addressed_router_state_stale(metadata: dict[str, str]) -> bool:
    started_at = parse_utc(metadata.get("addressed.router_started_at", ""))
    if not started_at:
        return True
    return datetime.now(timezone.utc) - started_at > ADDRESSED_ROUTER_STALE_AFTER


def route_addressed_source(source: dict[str, Any]) -> dict[str, Any]:
    source_id = bead_id(source)
    metadata = bead_metadata(source)
    if not source_id:
        return {"status": "skipped", "reason": "missing_source_id"}
    if metadata.get("addressed.workflow_root"):
        close = close_addressed_source(source_id)
        if close.returncode != 0:
            return {
                "status": "failed",
                "bead_id": source_id,
                "workflow_root_id": metadata.get("addressed.workflow_root", ""),
                "reason": "source_close_failed",
                "stderr": trim_output(close.stderr),
            }
        return {
            "status": "skipped",
            "bead_id": source_id,
            "workflow_root_id": metadata.get("addressed.workflow_root", ""),
            "reason": "already_dispatched_closed",
        }
    if metadata.get("addressed.router_status") in ADDRESSED_ROUTER_IN_PROGRESS_STATUSES and not addressed_router_state_stale(metadata):
        return {"status": "skipped", "bead_id": source_id, "reason": "router_already_started"}
    pool = metadata.get("addressed.pool", "")
    target = addressed_route_target(metadata)
    formula = metadata.get("addressed.formula", "") or ADDRESSED_MESSAGE_FORMULA
    rig = rig_from_target(target)
    if not target or not formula or not rig:
        outcome = {"status": "failed", "bead_id": source_id, "reason": "missing_route"}
        update_bead_metadata(
            source_id,
            {
                "addressed.router_status": "failed",
                "addressed.router_reason": "missing_route",
                "addressed.router_failed_at": common.utcnow(),
            },
        )
        return outcome

    starting = update_bead_metadata(
        source_id,
        {
            "addressed.router_status": "starting",
            "addressed.router_started_at": common.utcnow(),
        },
    )
    if starting.returncode != 0:
        return {
            "status": "failed",
            "bead_id": source_id,
            "reason": "source_update_failed",
            "stderr": trim_output(starting.stderr),
        }

    gc_bin = os.environ.get("GC_BIN", "gc")
    launch = create_addressed_rig_launch_bead(source_id, source, metadata, target, formula)
    launch_bead_id = str(launch.get("bead_id", ""))
    if launch.get("status") != "created" or not launch_bead_id:
        reason = str(launch.get("reason", "rig_launch_failed"))
        update_bead_metadata(
            source_id,
            {
                "addressed.router_status": "failed",
                "addressed.router_reason": reason,
                "addressed.router_failed_at": common.utcnow(),
                "addressed.router_stderr": str(launch.get("stderr", "")),
            },
        )
        outcome = {
            "status": "failed",
            "bead_id": source_id,
            "reason": reason,
        }
        if launch.get("exit_code") is not None:
            outcome["exit_code"] = launch.get("exit_code")
        if launch.get("stdout"):
            outcome["stdout"] = launch.get("stdout")
        if launch.get("stderr"):
            outcome["stderr"] = launch.get("stderr")
        return outcome

    command = [
        gc_bin,
        "--rig",
        rig,
        "sling",
        "--json",
        target,
        launch_bead_id,
        "--force",
        "--on",
        formula,
        "--title",
        str(source.get("title") or addressed_source_title(metadata)),
    ]
    for key, value in addressed_router_vars(source_id, metadata).items():
        if value:
            command.extend(["--var", f"{key}={value}"])
    try:
        result = run_subprocess(command, common.city_root() or ".")
    except FileNotFoundError:
        update_bead_metadata(
            source_id,
            {
                "addressed.router_status": "failed",
                "addressed.router_reason": "gc_not_available",
                "addressed.router_failed_at": common.utcnow(),
            },
        )
        return {"status": "failed", "bead_id": source_id, "reason": "gc_not_available"}
    workflow_root = workflow_root_from_sling_output(result.stdout)
    if result.returncode != 0 or not workflow_root:
        reason = "sling_failed" if result.returncode != 0 else "missing_workflow_root"
        update_bead_metadata(
            source_id,
            {
                "addressed.router_status": "failed",
                "addressed.router_reason": reason,
                "addressed.router_failed_at": common.utcnow(),
                "addressed.router_stderr": trim_output(result.stderr),
            },
        )
        return {
            "status": "failed",
            "bead_id": source_id,
            "reason": reason,
            "exit_code": result.returncode,
            "stdout": trim_output(result.stdout),
            "stderr": trim_output(result.stderr),
        }

    update = update_bead_metadata(
        source_id,
        {
            "addressed.router_status": "dispatched",
            "addressed.rig_launch_bead": launch_bead_id,
            "addressed.workflow_root": workflow_root,
            "addressed.dispatched_at": common.utcnow(),
            "addressed.workflow_formula": formula,
            "addressed.workflow_pool": pool,
            "addressed.workflow_target": target,
            "addressed.workflow_store": f"rig:{rig}",
        },
    )
    if update.returncode != 0:
        update_bead_metadata(
            source_id,
            {
                "addressed.router_status": "failed",
                "addressed.router_reason": "source_update_failed",
                "addressed.router_failed_at": common.utcnow(),
                "addressed.rig_launch_bead": launch_bead_id,
                "addressed.workflow_root": workflow_root,
            },
        )
        return {
            "status": "failed",
            "bead_id": source_id,
            "rig_launch_bead_id": launch_bead_id,
            "workflow_root_id": workflow_root,
            "reason": "source_update_failed",
            "stderr": trim_output(update.stderr),
        }
    close = close_addressed_source(source_id)
    if close.returncode != 0:
        return {
            "status": "failed",
            "bead_id": source_id,
            "workflow_root_id": workflow_root,
            "reason": "source_close_failed",
            "stderr": trim_output(close.stderr),
        }
    return {
        "status": "started",
        "bead_id": source_id,
        "rig_launch_bead_id": launch_bead_id,
        "workflow_root_id": workflow_root,
        "pool": pool,
        "target": target,
        "formula": formula,
    }


def run_addressed_router(limit: int = 50) -> dict[str, Any]:
    try:
        sources = list_addressed_router_sources(limit)
    except FileNotFoundError:
        return {"status": "failed", "reason": "bd_not_available", "started": [], "skipped": [], "failures": []}
    except Exception as exc:  # noqa: BLE001
        return {"status": "failed", "reason": "source_list_failed", "detail": str(exc), "started": [], "skipped": [], "failures": []}

    started: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for source in sources:
        outcome = route_addressed_source(source)
        status = outcome.get("status")
        if status == "started":
            started.append(outcome)
        elif status == "skipped":
            skipped.append(outcome)
        else:
            failures.append(outcome)
    return {
        "status": "failed" if failures else "ok",
        "started_count": len(started),
        "skipped_count": len(skipped),
        "failure_count": len(failures),
        "started": started,
        "skipped": skipped,
        "failures": failures,
    }


def render_admin_home() -> str:
    snapshot = common.build_status_snapshot(limit=20)
    config = snapshot["config"]
    app_cfg = config.get("app", {})
    manifest_json = ""
    manifest_error = ""
    try:
        manifest_json = json.dumps(common.build_manifest(), indent=2, sort_keys=True)
    except Exception as exc:  # noqa: BLE001
        manifest_error = str(exc)

    install_url = common.install_url(app_cfg) if isinstance(app_cfg, dict) else ""
    register_form = ""
    if manifest_json:
        escaped_manifest = html.escape(manifest_json, quote=True)
        register_form = f"""
<form id="manifest-form" action="https://github.com/settings/apps/new" method="post">
  <input type="hidden" name="manifest" value="{escaped_manifest}">
  <label for="org-name">Organization (leave blank for personal account):</label><br>
  <input type="text" id="org-name" placeholder="my-org" style="margin: 0.5rem 0; padding: 0.3rem; font-family: inherit;">
  <br>
  <button type="submit">Register GitHub App</button>
</form>
<script>
(function() {{
  var orgInput = document.getElementById("org-name");
  var form = document.getElementById("manifest-form");
  orgInput.addEventListener("input", function() {{
    var org = orgInput.value.trim();
    form.action = org
      ? "https://github.com/organizations/" + encodeURIComponent(org) + "/settings/apps/new"
      : "https://github.com/settings/apps/new";
  }});
}})();
</script>
"""

    install_html = ""
    if install_url:
        install_html = f'<p><a href="{html.escape(install_url)}">Install the GitHub App</a></p>'

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>GitHub Intake Admin</title>
  <style>
    body {{ font-family: ui-monospace, SFMono-Regular, Menlo, monospace; margin: 2rem; line-height: 1.45; }}
    pre {{ background: #f5f5f5; padding: 1rem; overflow-x: auto; }}
    code {{ background: #f5f5f5; padding: 0.1rem 0.25rem; }}
    .warning {{ color: #8a3b12; }}
  </style>
</head>
<body>
  <h1>GitHub Intake</h1>
  <p>Admin URL: <code>{html.escape(snapshot['admin_url'] or '(not published yet)')}</code></p>
  <p>Webhook URL: <code>{html.escape(snapshot['webhook_url'] or '(not published yet)')}</code></p>
  <h2>App Setup</h2>
  {register_form or f'<p class="warning">{html.escape(manifest_error or "Manifest unavailable")}</p>'}
  {install_html}
  <details><summary>Raw manifest JSON</summary>
  <pre>{html.escape(manifest_json or manifest_error or "manifest unavailable")}</pre>
  </details>
  <h2>Config</h2>
  <pre>{html.escape(json.dumps(config, indent=2, sort_keys=True))}</pre>
  <h2>Recent Requests</h2>
  <pre>{html.escape(json.dumps(snapshot['recent_requests'], indent=2, sort_keys=True))}</pre>
  <h2>Recent Addressed Messages</h2>
  <pre>{html.escape(json.dumps(snapshot['recent_address_results'], indent=2, sort_keys=True))}</pre>
  <h2>Bugflow Routing</h2>
  <p><code>/gc fix</code> uses the workflows bugflow router. Configure repositories with <code>gc workflows pr-review config set-repo owner/repo --city /abs/city --rig &lt;rig&gt; --base-branch main</code>.</p>
</body>
</html>
"""


class IntakeHandler(BaseHTTPRequestHandler):
    server_version = "GitHubIntake/0.2"

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"[{common.current_service_name() or 'github'}] {fmt % args}")

    def _parsed(self) -> urllib.parse.ParseResult:
        return urllib.parse.urlparse(self.path)

    def _read_json_body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        data = self.rfile.read(length) if length > 0 else b"{}"
        if not data:
            return {}
        parsed = json.loads(data.decode("utf-8"))
        if isinstance(parsed, dict):
            return parsed
        raise ValueError("request body must be a JSON object")

    def do_GET(self) -> None:  # noqa: N802
        parsed = self._parsed()
        service_name = common.current_service_name()
        if parsed.path == "/healthz":
            self.send_response(HTTPStatus.NO_CONTENT)
            self.end_headers()
            return
        if service_name == common.ADMIN_SERVICE_NAME:
            self._do_admin_get(parsed)
            return
        self._do_webhook_get(parsed)

    def do_POST(self) -> None:  # noqa: N802
        parsed = self._parsed()
        service_name = common.current_service_name()
        if service_name == common.ADMIN_SERVICE_NAME:
            self._do_admin_post(parsed)
            return
        self._do_webhook_post(parsed)

    def _do_admin_get(self, parsed: urllib.parse.ParseResult) -> None:
        if parsed.path == "/":
            text_response(self, HTTPStatus.OK, render_admin_home(), "text/html; charset=utf-8")
            return
        if parsed.path == "/v0/github/status":
            json_response(self, HTTPStatus.OK, common.build_status_snapshot(limit=20))
            return
        if parsed.path == "/v0/github/requests":
            json_response(self, HTTPStatus.OK, {"requests": common.list_recent_requests(limit=50)})
            return
        if parsed.path == "/v0/github/app/manifest":
            try:
                manifest = common.build_manifest()
            except Exception as exc:  # noqa: BLE001
                json_response(self, HTTPStatus.SERVICE_UNAVAILABLE, {"error": str(exc)})
                return
            json_response(self, HTTPStatus.OK, manifest)
            return
        if parsed.path == "/v0/github/app/manifest/callback":
            params = urllib.parse.parse_qs(parsed.query)
            code = params.get("code", [""])[0]
            if not code:
                text_response(self, HTTPStatus.BAD_REQUEST, "missing manifest conversion code\n", "text/plain; charset=utf-8")
                return
            try:
                converted = common.exchange_manifest_code(code)
                config = common.import_app_config(common.load_config(), converted)
            except Exception as exc:  # noqa: BLE001
                text_response(
                    self,
                    HTTPStatus.BAD_GATEWAY,
                    f"manifest conversion failed: {exc}\n",
                    "text/plain; charset=utf-8",
                )
                return
            publish_result = publish_imported_identity(config)
            app_cfg = config.get("app", {})
            body = [
                "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><title>GitHub Intake Ready</title></head><body>",
                "<h1>GitHub App Imported</h1>",
                f"<p>App id: <code>{html.escape(str(app_cfg.get('app_id', '')))}</code></p>",
            ]
            publish_status = str(publish_result.get("status", ""))
            if publish_status == "published":
                body.append("<p>Credentials published to the configured secret store.</p>")
            elif publish_status == "error":
                body.append(
                    f'<p class="warning">Secret-store publish failed: '
                    f"<code>{html.escape(str(publish_result.get('detail', '')))}</code>. "
                    "Credentials remain in the local service config.</p>"
                )
            install_url = common.install_url(app_cfg)
            if install_url:
                body.append(f'<p><a href="{html.escape(install_url)}">Install the GitHub App</a></p>')
            body.append("</body></html>")
            text_response(self, HTTPStatus.OK, "".join(body), "text/html; charset=utf-8")
            return
        json_response(self, HTTPStatus.NOT_FOUND, {"error": "not_found"})

    def _do_admin_post(self, parsed: urllib.parse.ParseResult) -> None:
        if parsed.path != "/v0/github/app/import":
            json_response(self, HTTPStatus.NOT_FOUND, {"error": "not_found"})
            return
        try:
            body = self._read_json_body()
        except Exception as exc:  # noqa: BLE001
            json_response(self, HTTPStatus.BAD_REQUEST, {"error": str(exc)})
            return
        config = common.import_app_config(common.load_config(), body)
        publish_result = publish_imported_identity(config)
        json_response(self, HTTPStatus.OK, {"config": common.redact_config(config), "publish": publish_result})

    def _do_webhook_get(self, parsed: urllib.parse.ParseResult) -> None:
        if parsed.path == "/":
            json_response(
                self,
                HTTPStatus.OK,
                {
                    "service": common.current_service_name(),
                    "status": "ok",
                    "webhook_url": common.webhook_url(),
                },
            )
            return
        json_response(self, HTTPStatus.NOT_FOUND, {"error": "not_found"})

    def _do_webhook_post(self, parsed: urllib.parse.ParseResult) -> None:
        if parsed.path != "/v0/github/webhook":
            json_response(self, HTTPStatus.NOT_FOUND, {"error": "not_found"})
            return
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length) if length > 0 else b""
        config = common.load_effective_config()
        try:
            config = ensure_webhook_app_config(config)
        except Exception as exc:  # noqa: BLE001 - report sync failure without dropping the service.
            json_response(
                self,
                HTTPStatus.SERVICE_UNAVAILABLE,
                {
                    "error": "github app webhook secret is not configured",
                    "sync_error": trim_output(str(exc)),
                },
            )
            return
        app_cfg = config.get("app", {})
        secret = str(app_cfg.get("webhook_secret", ""))
        if not secret:
            json_response(self, HTTPStatus.SERVICE_UNAVAILABLE, {"error": "github app webhook secret is not configured"})
            return
        signature = self.headers.get("X-Hub-Signature-256", "")
        if not common.verify_github_signature(secret, body, signature):
            json_response(self, HTTPStatus.UNAUTHORIZED, {"error": "invalid webhook signature"})
            return
        delivery_id = self.headers.get("X-GitHub-Delivery", "")
        event = self.headers.get("X-GitHub-Event", "")
        try:
            payload = json.loads(body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            json_response(self, HTTPStatus.BAD_REQUEST, {"error": f"invalid JSON payload: {exc}"})
            return

        common.save_delivery(
            {
                "delivery_id": delivery_id or "unknown-delivery",
                "received_at": common.utcnow(),
                "event": event,
                "payload": payload,
            }
        )

        rule_processing = enqueue_event_rules(event, delivery_id, payload, app_cfg if isinstance(app_cfg, dict) else {})

        if event != "issue_comment":
            json_response(
                self,
                HTTPStatus.ACCEPTED,
                {
                    "status": "accepted",
                    "event": event,
                    "delivery_id": delivery_id or "unknown-delivery",
                    "rule_processing": rule_processing,
                },
            )
            return
        addressed = process_addressed_comment(event, delivery_id, payload, app_cfg if isinstance(app_cfg, dict) else {})
        if addressed.get("status") != "ignored" or addressed.get("reason") == "comment_from_bot":
            json_response(
                self,
                HTTPStatus.ACCEPTED,
                {
                    "status": addressed.get("status", "accepted"),
                    "addressed_message": {
                        "reason": addressed.get("reason", ""),
                        "created_count": addressed.get("created_count", 0),
                        "duplicate_count": addressed.get("duplicate_count", 0),
                        "failure_count": addressed.get("failure_count", 0),
                    },
                    "command_processing": "skipped_addressed_message",
                    "rule_processing": rule_processing,
                },
            )
            return
        parsed_command = common.parse_gc_command(str((payload.get("comment") or {}).get("body", "")))
        issue = payload.get("issue") or {}
        if issue.get("pull_request") and parsed_command:
            json_response(
                self,
                HTTPStatus.ACCEPTED,
                {
                    "status": "ignored",
                    "reason": "pr_comments_not_supported",
                    "command": str(parsed_command.get("command", "")),
                },
            )
            return
        request = common.extract_issue_comment_request(payload)
        if not request:
            json_response(self, HTTPStatus.ACCEPTED, {"status": "ignored", "reason": "not_an_actionable_issue_comment"})
            return
        bot_login = common.app_bot_login(app_cfg if isinstance(app_cfg, dict) else {})
        if bot_login and str(request.get("comment_author", "")).lower() == bot_login.lower():
            json_response(self, HTTPStatus.ACCEPTED, {"status": "ignored", "reason": "comment_from_app"})
            return
        request["event"] = event
        request["delivery_id"] = delivery_id
        behavior = command_behavior(str(request.get("command", "")))
        if not behavior:
            json_response(
                self,
                HTTPStatus.ACCEPTED,
                {
                    "status": "ignored",
                    "reason": "command_not_supported",
                    "command": str(request.get("command", "")),
                },
            )
            return
        existing = reserve_request(request, behavior)
        if existing:
            json_response(
                self,
                HTTPStatus.ACCEPTED,
                {"status": "duplicate", "request": request_summary(existing)},
            )
            return
        enqueue_request(request["request_id"])
        json_response(self, HTTPStatus.ACCEPTED, {"status": "accepted", "request": request_summary(request)})


def main() -> int:
    common.ensure_layout()
    try:
        sync_configured_github_app_at_startup()
    except Exception as exc:  # noqa: BLE001 - the webhook path still reports config failures precisely.
        print(
            f"[{common.current_service_name() or 'github-intake'}] "
            f"GitHub App config sync failed: {trim_output(str(exc))}",
            flush=True,
        )
    socket_path = os.environ.get("GC_SERVICE_SOCKET")
    if not socket_path:
        raise SystemExit("GC_SERVICE_SOCKET is required")
    try:
        os.remove(socket_path)
    except FileNotFoundError:
        pass
    with ThreadingUnixHTTPServer(socket_path, IntakeHandler) as server:
        print(f"[{common.current_service_name() or 'github'}] listening on {socket_path}")
        server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
