#!/usr/bin/env python3

from __future__ import annotations

import html
import json
import os
import socketserver
import subprocess
import threading
import traceback
import urllib.parse
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler
from typing import Any

import github_intake_common as common

PROCESSING_LOCK = threading.Lock()
ACCEPTANCE_LOCK = threading.Lock()
PROCESSING_REQUESTS: set[str] = set()
WRITE_PERMISSION_LEVELS = {"write", "maintain", "admin"}


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
        "permission_lookup_failed": "the GitHub App could not verify the commenter's repository permission",
        "pr_comments_not_supported": "this slice only accepts /gc fix commands on GitHub issues, not pull requests",
    }
    return mapping.get(code, code or "unknown_error")


def rig_from_target(target: str) -> str:
    if "/" not in target:
        return ""
    rig, _, _ = target.partition("/")
    return rig.strip()


def extract_json_output(raw: str) -> dict[str, Any]:
    raw = raw.strip()
    if not raw:
        return {}
    for left, right in (("{", "}"), ("[", "]")):
        start = raw.find(left)
        end = raw.rfind(right)
        if start == -1 or end == -1 or end < start:
            continue
        try:
            payload = json.loads(raw[start : end + 1])
        except json.JSONDecodeError:
            continue
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


def run_subprocess(command: list[str], cwd: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
    )


def create_fix_bead(request: dict[str, Any], target: str) -> dict[str, Any]:
    rig = rig_from_target(target)
    if not rig:
        return {"status": "dispatch_failed", "reason": "invalid_dispatch_target"}
    city_root = common.city_root() or "."
    bd_bin = os.environ.get("BD_BIN", "bd")
    create_command = [bd_bin, "create", "--json", build_fix_bead_title(request), "--rig", rig, "-t", "task"]
    try:
        create_result = run_subprocess(create_command, city_root)
    except FileNotFoundError:
        return {"status": "dispatch_failed", "reason": "bead_create_failed", "dispatch_stderr": "bd not available"}
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
    update_command = [bd_bin, "update", bead_id, "--notes", build_fix_bead_notes(request)]
    for key, value in metadata.items():
        if value:
            update_command.extend(["--set-metadata", f"{key}={value}"])
    try:
        update_result = run_subprocess(update_command, city_root)
    except FileNotFoundError:
        return {
            "status": "dispatch_failed",
            "reason": "bead_update_failed",
            "bead_id": bead_id,
            "dispatch_stderr": "bd not available",
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


def close_failed_bead(bead_id: str, reason: str) -> bool:
    bead_id = bead_id.strip()
    if not bead_id:
        return True
    bd_bin = os.environ.get("BD_BIN", "bd")
    city_root = common.city_root() or "."
    try:
        set_reason = run_subprocess(
            [bd_bin, "update", bead_id, "--set-metadata", f"close_reason=github-intake:{reason or 'dispatch_failed'}"],
            city_root,
        )
        if set_reason.returncode != 0:
            return False
        result = run_subprocess([bd_bin, "close", bead_id], city_root)
    except FileNotFoundError:
        return False
    return result.returncode == 0


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

    bead_outcome = create_fix_bead(request, target)
    if bead_outcome.get("status") == "dispatch_failed":
        cleanup_ok = close_failed_bead(str(bead_outcome.get("bead_id", "")), str(bead_outcome.get("reason", "")))
        if not cleanup_ok:
            bead_outcome["cleanup_failed"] = True
        return bead_outcome
    if "bead_id" not in bead_outcome:
        return bead_outcome
    bead_id = str(bead_outcome["bead_id"])

    gc_bin = os.environ.get("GC_BIN", "gc")
    command = [gc_bin, "sling", target, bead_id, "--on", formula]
    for key, value in build_fix_vars(request, bead_id).items():
        if value:
            command.extend(["--var", f"{key}={value}"])
    try:
        result = run_subprocess(command, common.city_root() or ".")
    except FileNotFoundError:
        cleanup_ok = close_failed_bead(bead_id, "gc_not_available")
        outcome = {
            "status": "dispatch_failed",
            "reason": "gc_not_available",
            "bead_id": bead_id,
        }
        if not cleanup_ok:
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
        if not close_failed_bead(bead_id, "dispatch_failed"):
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
        config = common.load_config()
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
        payload = common.load_request(request_id) or {"request_id": request_id}
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
<form action="https://github.com/settings/apps/new" method="post">
  <input type="hidden" name="manifest" value="{escaped_manifest}">
  <button type="submit">Register GitHub App</button>
</form>
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
  <p>For organization-owned apps, use the manifest JSON below from the org settings app-registration page.</p>
  <pre>{html.escape(manifest_json or manifest_error or "manifest unavailable")}</pre>
  <h2>Config</h2>
  <pre>{html.escape(json.dumps(config, indent=2, sort_keys=True))}</pre>
  <h2>Recent Requests</h2>
  <pre>{html.escape(json.dumps(snapshot['recent_requests'], indent=2, sort_keys=True))}</pre>
  <h2>Repository Mapping</h2>
  <p>Use <code>gc github-intake map-repo owner/repo rig/polecat --fix-formula mol-github-fix-issue</code> to update repo routing.</p>
</body>
</html>
"""


class IntakeHandler(BaseHTTPRequestHandler):
    server_version = "GitHubIntake/0.2"

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"[{common.current_service_name() or 'github-intake'}] {fmt % args}")

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
            app_cfg = config.get("app", {})
            body = [
                "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><title>GitHub Intake Ready</title></head><body>",
                "<h1>GitHub App Imported</h1>",
                f"<p>App id: <code>{html.escape(str(app_cfg.get('app_id', '')))}</code></p>",
            ]
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
        json_response(self, HTTPStatus.OK, {"config": common.redact_config(config)})

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
        config = common.load_config()
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

        if event != "issue_comment":
            json_response(self, HTTPStatus.ACCEPTED, {"status": "ignored", "event": event})
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
    socket_path = os.environ.get("GC_SERVICE_SOCKET")
    if not socket_path:
        raise SystemExit("GC_SERVICE_SOCKET is required")
    try:
        os.remove(socket_path)
    except FileNotFoundError:
        pass
    with ThreadingUnixHTTPServer(socket_path, IntakeHandler) as server:
        print(f"[{common.current_service_name() or 'github-intake'}] listening on {socket_path}")
        server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
