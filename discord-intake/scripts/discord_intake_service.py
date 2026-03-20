#!/usr/bin/env python3

from __future__ import annotations

import html
import json
import os
import secrets
import socketserver
import subprocess
import threading
import time
import traceback
import urllib.parse
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler
from typing import Any

import discord_intake_common as common

PROCESSING_LOCK = threading.Lock()
ACCEPTANCE_LOCK = threading.Lock()
PROCESSING_REQUESTS: set[str] = set()
MAX_REQUEST_BYTES = 64 * 1024


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
    payload = body.encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", content_type)
    handler.send_header("Content-Length", str(len(payload)))
    handler.end_headers()
    handler.wfile.write(payload)


def interaction_response(handler: BaseHTTPRequestHandler, payload: dict[str, Any]) -> None:
    body = json.dumps(payload).encode("utf-8")
    handler.send_response(HTTPStatus.OK)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def command_behavior(command: str) -> dict[str, Any]:
    if command != "fix":
        return {}
    return {"workflow_scope": "conversation"}


def trim_output(value: str, limit: int = 1200) -> str:
    value = value.strip()
    if len(value) <= limit:
        return value
    return value[:limit].rstrip() + "..."


def human_reason(code: str) -> str:
    mapping = {
        "command_not_supported": "this Discord intake slice only supports /gc fix",
        "channel_mapping_missing": "no channel mapping exists for this conversation",
        "command_not_configured": "this channel does not configure that /gc command",
        "guild_only": "Discord /gc fix is only accepted inside a guild",
        "guild_not_allowed": "this guild is not allowed to dispatch /gc fix",
        "channel_not_allowed": "this channel is not allowed to dispatch /gc fix",
        "role_not_allowed": "you do not have a Discord role that is allowed to dispatch /gc fix",
        "discord_app_not_configured": "the Discord app is not fully configured in this workspace",
        "bead_create_failed": "the workflow bead could not be created",
        "bead_update_failed": "the workflow bead could not be initialized",
        "gc_not_available": "the gc CLI is not available in this runtime",
        "invalid_dispatch_target": "the configured target is not a rig-scoped sling target",
        "modal_expired": "that modal submission has expired; run /gc fix again",
        "bad_modal_context": "that modal submission does not match the original slash command",
        "summary_required": "a short summary is required before the workflow can start",
        "internal_error": "an internal error occurred while starting the workflow",
    }
    return mapping.get(code, code or "unknown_error")


def request_summary(request: dict[str, Any]) -> dict[str, Any]:
    return {
        "request_id": request.get("request_id"),
        "workflow_key": request.get("workflow_key", ""),
        "status": request.get("status"),
        "command": request.get("command"),
        "guild_id": request.get("guild_id"),
        "conversation_id": request.get("conversation_id"),
        "bead_id": request.get("bead_id", ""),
        "dispatch_target": request.get("dispatch_target", ""),
        "dispatch_formula": request.get("dispatch_formula", ""),
        "reason": request.get("reason", ""),
    }


def build_message_response(content: str, ephemeral: bool) -> dict[str, Any]:
    data: dict[str, Any] = {"content": content}
    if ephemeral:
        data["flags"] = 64
    return {"type": 4, "data": data}


def build_modal_response(nonce: str) -> dict[str, Any]:
    return {
        "type": 9,
        "data": {
            "custom_id": f"gc:fix:{nonce}",
            "title": "GC Fix Request",
            "components": [
                {
                    "type": 1,
                    "components": [
                        {
                            "type": 4,
                            "custom_id": "summary",
                            "label": "Short summary",
                            "style": 1,
                            "min_length": 4,
                            "max_length": 120,
                            "required": True,
                        }
                    ],
                },
                {
                    "type": 1,
                    "components": [
                        {
                            "type": 4,
                            "custom_id": "context",
                            "label": "Additional context",
                            "style": 2,
                            "max_length": 4000,
                            "required": False,
                        }
                    ],
                },
            ],
        },
    }


def build_acceptance_response(request: dict[str, Any]) -> dict[str, Any]:
    summary = str(request.get("summary", "")).strip()
    content = "\n".join(
        part
        for part in (
            "Accepted /gc fix for this conversation.",
            f"Request: `{request.get('request_id', '')}`" if request.get("request_id") else "",
            f"Summary: {summary}" if summary else "",
        )
        if part
    )
    return build_message_response(content, ephemeral=False)


def build_duplicate_response(existing: dict[str, Any]) -> dict[str, Any]:
    content = "\n".join(
        part
        for part in (
            "A /gc fix workflow is already active for this conversation.",
            f"Request: `{existing.get('request_id', '')}`" if existing.get("request_id") else "",
            f"Status: `{existing.get('status', '')}`" if existing.get("status") else "",
            f"Bead: `{existing.get('bead_id', '')}`" if existing.get("bead_id") else "",
        )
        if part
    )
    return build_message_response(content or "A workflow is already active for this conversation.", ephemeral=True)


def receipt_payload(response: dict[str, Any], response_kind: str = "", request_id: str = "") -> dict[str, Any]:
    payload: dict[str, Any] = {
        "response": response,
    }
    if response_kind:
        payload["response_kind"] = response_kind
    if request_id:
        payload["request_id"] = request_id
    return payload


def prompt_to_summary_context(prompt: str) -> tuple[str, str]:
    prompt = prompt.strip()
    if not prompt:
        return "", ""
    lines = [line.strip() for line in prompt.splitlines()]
    summary = next((line for line in lines if line), "")[:120]
    return summary, prompt


def parse_application_command(payload: dict[str, Any], command_name_value: str) -> dict[str, Any]:
    data = payload.get("data") or {}
    if str(data.get("name", "")) != command_name_value:
        return {}
    options = data.get("options") or []
    if not isinstance(options, list) or not options:
        return {}
    subcommand = options[0] if isinstance(options[0], dict) else {}
    if int(subcommand.get("type", 0) or 0) != 1:
        return {}
    command = str(subcommand.get("name", "")).strip().lower()
    prompt = ""
    sub_options = subcommand.get("options") or []
    if isinstance(sub_options, list):
        for option in sub_options:
            if not isinstance(option, dict):
                continue
            if str(option.get("name", "")) == "prompt":
                prompt = str(option.get("value", "")).strip()
                break
    return {
        "command": command,
        "prompt": prompt,
    }


def extract_modal_fields(payload: dict[str, Any]) -> dict[str, str]:
    fields: dict[str, str] = {}
    data = payload.get("data") or {}
    stack = list(data.get("components") or [])
    while stack:
        current = stack.pop()
        if not isinstance(current, dict):
            continue
        if int(current.get("type", 0) or 0) == 4:
            custom_id = str(current.get("custom_id", "")).strip()
            value = str(current.get("value", ""))
            if custom_id:
                fields[custom_id] = value
        components = current.get("components") or []
        if isinstance(components, list):
            stack.extend(components)
    return fields


def modal_nonce(payload: dict[str, Any]) -> str:
    custom_id = str((payload.get("data") or {}).get("custom_id", "")).strip()
    prefix = "gc:fix:"
    if not custom_id.startswith(prefix):
        return ""
    return custom_id[len(prefix) :]


def display_name(payload: dict[str, Any]) -> str:
    member = payload.get("member") or {}
    user = member.get("user") or payload.get("user") or {}
    for key in ("global_name", "username"):
        value = str(user.get(key, "")).strip()
        if value:
            return value
    return str(member.get("nick", "")).strip()


def role_ids(payload: dict[str, Any]) -> list[str]:
    member = payload.get("member") or {}
    roles = member.get("roles") or []
    if not isinstance(roles, list):
        return []
    return [str(role).strip() for role in roles if str(role).strip()]


def run_subprocess(command: list[str], cwd: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=cwd, capture_output=True, text=True, check=False)


def rig_from_target(target: str) -> str:
    if "/" not in target:
        return ""
    rig, _, _ = target.partition("/")
    return rig.strip()


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
                path = str(entry.get("path", ""))
                if path == rig:
                    resolved = os.path.join(root, path) if not os.path.isabs(path) else path
                    if os.path.isdir(resolved):
                        return resolved
    except (OSError, json.JSONDecodeError):
        pass
    return ""


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
    summary = str(request.get("summary", "")).strip() or "Discord fix request"
    return f"Fix Discord request: {summary}"[:180]


def build_fix_bead_notes(request: dict[str, Any]) -> str:
    lines = [
        "## Discord Source",
        "",
        f"- Guild: {request.get('guild_id', '')}",
        f"- Channel: {request.get('channel_id', '')}",
        f"- Thread: {request.get('thread_id', '') or '(none)'}",
        f"- Conversation: {request.get('conversation_id', '')}",
        f"- Jump URL: {request.get('jump_url', '')}",
        f"- Request ID: {request.get('request_id', '')}",
        f"- Requested By: {request.get('invoking_user_display_name', '')} ({request.get('invoking_user_id', '')})",
        "",
        "## Summary",
        "",
        str(request.get("summary", "")).strip() or "(none)",
        "",
        "## Additional Context",
        "",
        str(request.get("context_markdown", "")).strip() or "(none)",
    ]
    return "\n".join(lines)


def create_fix_bead(request: dict[str, Any], target: str) -> dict[str, Any]:
    rig = rig_from_target(target)
    if not rig:
        return {"status": "dispatch_failed", "reason": "invalid_dispatch_target"}
    city_root = common.city_root() or "."
    bd_bin = os.environ.get("BD_BIN", "bd")
    bd_cwd = rig_workdir(rig) or city_root
    create_command = [bd_bin, "create", "--json", build_fix_bead_title(request), "-t", "task"]
    try:
        create_result = run_subprocess(create_command, bd_cwd)
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

    update_command = [bd_bin, "update", bead_id, "--notes", build_fix_bead_notes(request)]
    metadata = {
        "discord_request_id": str(request.get("request_id", "")),
        "discord_guild_id": str(request.get("guild_id", "")),
        "discord_channel_id": str(request.get("channel_id", "")),
        "discord_thread_id": str(request.get("thread_id", "")),
        "discord_conversation_id": str(request.get("conversation_id", "")),
        "discord_summary": str(request.get("summary", "")),
    }
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
        "discord_request_id": str(request.get("request_id", "")),
        "discord_guild_id": str(request.get("guild_id", "")),
        "discord_channel_id": str(request.get("channel_id", "")),
        "discord_thread_id": str(request.get("thread_id", "")),
        "discord_conversation_id": str(request.get("conversation_id", "")),
        "discord_jump_url": str(request.get("jump_url", "")),
        "discord_requester": str(request.get("invoking_user_display_name", "")),
        "discord_summary": str(request.get("summary", "")),
        "discord_context": str(request.get("context_markdown", "")),
    }


def close_failed_bead(bead_id: str, reason: str, rig: str = "") -> bool:
    bead_id = bead_id.strip()
    if not bead_id:
        return True
    bd_bin = os.environ.get("BD_BIN", "bd")
    city_root = common.city_root() or "."
    bd_cwd = (rig_workdir(rig) or city_root) if rig else city_root
    try:
        set_reason = run_subprocess(
            [bd_bin, "update", bead_id, "--set-metadata", f"close_reason=discord-intake:{reason or 'dispatch_failed'}"],
            bd_cwd,
        )
        if set_reason.returncode != 0:
            return False
        result = run_subprocess([bd_bin, "close", bead_id], bd_cwd)
    except FileNotFoundError:
        return False
    return result.returncode == 0


def run_fix_dispatch(request: dict[str, Any]) -> dict[str, Any]:
    formula = str(request.get("dispatch_formula", "")).strip()
    target = str(request.get("dispatch_target", "")).strip()
    if not formula or not target:
        return {"status": "ignored", "reason": "command_not_configured"}

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
        outcome = {"status": "dispatch_failed", "reason": "gc_not_available", "bead_id": bead_id}
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
        behavior = command_behavior(str(request.get("command", "")))
        if not behavior:
            request["status"] = "ignored"
            request["reason"] = "command_not_supported"
        else:
            outcome = run_fix_dispatch(request)
            request.update(outcome)
            if request.get("status") in {"dispatch_failed", "internal_error"}:
                maybe_notify_dispatch_failure(request)
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
        payload["reason"] = "internal_error"
        payload["error_message"] = str(exc)
        payload["traceback"] = traceback.format_exc(limit=20)
        maybe_notify_dispatch_failure(payload)
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


def enqueue_request(request_id: str) -> None:
    with PROCESSING_LOCK:
        if request_id in PROCESSING_REQUESTS:
            return
        PROCESSING_REQUESTS.add(request_id)
    thread = threading.Thread(target=process_request, args=(request_id,), daemon=True)
    thread.start()


def reserve_request(request: dict[str, Any], behavior: dict[str, Any], interaction_id: str) -> dict[str, Any] | None:
    with ACCEPTANCE_LOCK:
        existing_receipt = common.load_interaction_receipt(interaction_id)
        if existing_receipt:
            request_id = str(existing_receipt.get("request_id", "")).strip()
            if request_id:
                return common.load_request(request_id) or {"request_id": request_id}
            return {"request_id": "", "status": "duplicate"}
        existing = common.load_request(request["request_id"])
        if existing:
            common.save_interaction_receipt(
                interaction_id,
                {"request_id": str(existing.get("request_id", "")), "response_kind": "duplicate"},
            )
            return existing
        workflow_key = str(request.get("workflow_key", ""))
        if behavior.get("workflow_scope") == "conversation" and workflow_key:
            workflow_link = common.load_workflow_link(workflow_key)
            if workflow_link:
                existing_request_id = str(workflow_link.get("request_id", ""))
                existing_request = common.load_request(existing_request_id) or {
                    "request_id": existing_request_id,
                    "workflow_key": workflow_key,
                    "status": "duplicate",
                    "command": request.get("command", ""),
                    "guild_id": request.get("guild_id", ""),
                    "conversation_id": request.get("conversation_id", ""),
                }
                common.save_interaction_receipt(
                    interaction_id,
                    {"request_id": existing_request_id, "response_kind": "duplicate"},
                )
                return existing_request
        common.save_request(request)
        if behavior.get("workflow_scope") == "conversation" and workflow_key:
            common.save_workflow_link(workflow_key, request["request_id"])
        common.save_interaction_receipt(
            interaction_id,
            {"request_id": request["request_id"], "response_kind": "accepted"},
        )
    return None


def render_admin_home() -> str:
    snapshot = common.build_status_snapshot(limit=20)
    config = snapshot["config"]
    app_cfg = config.get("app", {})
    command_name_value = str(app_cfg.get("command_name", common.COMMAND_NAME_DEFAULT))
    payload_preview = json.dumps(common.build_command_payload(command_name_value), indent=2, sort_keys=True)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Discord Intake Admin</title>
  <style>
    body {{ font-family: ui-monospace, SFMono-Regular, Menlo, monospace; margin: 2rem; line-height: 1.45; }}
    pre {{ background: #f5f5f5; padding: 1rem; overflow-x: auto; }}
    code {{ background: #f5f5f5; padding: 0.1rem 0.25rem; }}
  </style>
</head>
<body>
  <h1>Discord Intake</h1>
  <p>Interactions URL: <code>{html.escape(str(snapshot.get('interactions_url') or '(not published yet)'))}</code></p>
  <p>Admin URL: <code>{html.escape(str(snapshot.get('admin_url') or '(not published yet)'))}</code></p>
  <h2>Setup</h2>
  <p>Import the Discord application id, public key, and bot token with <code>gc discord-intake import-app ...</code>.</p>
  <p>Then point the Discord Interactions Endpoint URL at <code>{html.escape(str(snapshot.get('interactions_url') or '(publish discord-interactions first)'))}/v0/discord/interactions</code>.</p>
  <h2>Command Sync Payload</h2>
  <pre>{html.escape(payload_preview)}</pre>
  <h2>Config</h2>
  <pre>{html.escape(json.dumps(config, indent=2, sort_keys=True))}</pre>
  <h2>Recent Requests</h2>
  <pre>{html.escape(json.dumps(snapshot.get('recent_requests', []), indent=2, sort_keys=True))}</pre>
</body>
</html>
"""


def build_request(
    payload: dict[str, Any],
    summary: str,
    context_markdown: str,
    channel_context: dict[str, Any],
) -> dict[str, Any]:
    guild_id = str(payload.get("guild_id", "")).strip()
    conversation_id = str(payload.get("channel_id", "")).strip()
    interaction_id = str(payload.get("id", "")).strip()
    thread_id = str(channel_context.get("thread_id", "")).strip()
    channel_id = str(channel_context.get("parent_channel_id", "")).strip() or conversation_id
    mapping = channel_context.get("mapping") or {}
    request_id = common.build_request_id(interaction_id, "fix")
    return {
        "request_id": request_id,
        "workflow_key": common.build_workflow_key(guild_id, conversation_id, "fix"),
        "status": "received",
        "command": "fix",
        "created_at": common.utcnow(),
        "updated_at": common.utcnow(),
        "interaction_id": interaction_id,
        "guild_id": guild_id,
        "channel_id": channel_id,
        "thread_id": thread_id,
        "conversation_id": conversation_id,
        "invoking_user_id": str(((payload.get("member") or {}).get("user") or {}).get("id", payload.get("user", {}).get("id", ""))),
        "invoking_user_display_name": display_name(payload),
        "summary": summary.strip(),
        "context_markdown": context_markdown.strip(),
        "jump_url": common.discord_jump_url(guild_id, conversation_id),
        "dispatch_target": str(mapping.get("target", "")),
        "dispatch_formula": str((((mapping.get("commands") or {}).get("fix") or {}).get("formula", common.FIX_FORMULA_DEFAULT))),
    }


def replay_response_from_receipt(receipt: dict[str, Any]) -> dict[str, Any]:
    stored_response = receipt.get("response")
    if isinstance(stored_response, dict):
        return stored_response
    response_kind = str(receipt.get("response_kind", "")).strip()
    if response_kind == "modal":
        return build_modal_response(str(receipt.get("modal_nonce", "")))
    request_id = str(receipt.get("request_id", "")).strip()
    request = common.load_request(request_id) if request_id else {}
    if response_kind == "accepted":
        return build_acceptance_response(request or {"request_id": request_id})
    return build_duplicate_response(request or {"request_id": request_id, "status": "duplicate"})


def build_dispatch_failure_message(request: dict[str, Any]) -> str:
    request_id = str(request.get("request_id", "")).strip()
    bead_id = str(request.get("bead_id", "")).strip()
    status = str(request.get("status", "")).strip()
    reason = str(request.get("reason", "")).strip()
    lines = [
        "Discord `/gc fix` could not be started.",
        f"Request: `{request_id}`" if request_id else "",
        f"Status: `{status}`" if status else "",
        f"Reason: {human_reason(reason)}" if reason else "",
        f"Bead: `{bead_id}`" if bead_id else "",
    ]
    return "\n".join(line for line in lines if line)


def maybe_notify_dispatch_failure(request: dict[str, Any]) -> dict[str, Any]:
    if request.get("failure_notified_at"):
        return request
    target_channel = str(request.get("thread_id", "")).strip() or str(request.get("channel_id", "")).strip()
    if not target_channel:
        return request
    try:
        response = common.post_channel_message(target_channel, build_dispatch_failure_message(request))
    except common.DiscordAPIError as exc:
        request["failure_notification_error"] = str(exc)
        return request
    request["failure_notified_at"] = common.utcnow()
    message_id = str((response or {}).get("id", "")).strip() if isinstance(response, dict) else ""
    if message_id:
        request["failure_message_id"] = message_id
    return request


def finalize_modal_origin_receipt(
    original_interaction_id: str,
    response: dict[str, Any],
    receipt: dict[str, Any],
) -> None:
    original_interaction_id = original_interaction_id.strip()
    if not original_interaction_id:
        return
    payload = dict(receipt)
    payload.setdefault("response", response)
    common.replace_interaction_receipt(original_interaction_id, payload)


def accept_fix_request(
    payload: dict[str, Any],
    summary: str,
    context_markdown: str,
    interaction_id: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    config = common.load_config()
    if not str(payload.get("guild_id", "")).strip():
        response = build_message_response(human_reason("guild_only"), ephemeral=True)
        return response, receipt_payload(response, response_kind="message")
    if not common.load_bot_token():
        response = build_message_response(human_reason("discord_app_not_configured"), ephemeral=True)
        return response, receipt_payload(response, response_kind="message")
    channel_context = common.load_channel_context(
        config,
        str(payload.get("guild_id", "")),
        str(payload.get("channel_id", "")),
        str((payload.get("channel") or {}).get("parent_id", "")),
    )
    mapping = channel_context.get("mapping") or {}
    if not mapping:
        response = build_message_response(human_reason("channel_mapping_missing"), ephemeral=True)
        return response, receipt_payload(response, response_kind="message")
    reason = common.policy_reason(
        config,
        str(payload.get("guild_id", "")),
        str(channel_context.get("parent_channel_id", payload.get("channel_id", ""))),
        role_ids(payload),
    )
    if reason:
        response = build_message_response(human_reason(reason), ephemeral=True)
        return response, receipt_payload(response, response_kind="message")
    summary = summary.strip()
    context_markdown = context_markdown.strip()
    if not summary and context_markdown:
        summary, context_markdown = prompt_to_summary_context(context_markdown)
    if not summary:
        response = build_message_response(human_reason("summary_required"), ephemeral=True)
        return response, receipt_payload(response, response_kind="message")
    request = build_request(payload, summary, context_markdown, channel_context)
    behavior = command_behavior("fix")
    if not behavior:
        response = build_message_response(human_reason("command_not_supported"), ephemeral=True)
        return response, receipt_payload(response, response_kind="message")
    existing = reserve_request(request, behavior, interaction_id)
    if existing:
        response = build_duplicate_response(existing)
        return response, receipt_payload(response, response_kind="duplicate", request_id=str(existing.get("request_id", "")).strip())
    enqueue_request(request["request_id"])
    response = build_acceptance_response(request)
    return response, receipt_payload(response, response_kind="accepted", request_id=request["request_id"])


class IntakeHandler(BaseHTTPRequestHandler):
    server_version = "DiscordIntake/0.1"

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"[{common.current_service_name() or 'discord-intake'}] {fmt % args}")

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
        self._do_interactions_get(parsed)

    def do_POST(self) -> None:  # noqa: N802
        parsed = self._parsed()
        service_name = common.current_service_name()
        if service_name == common.ADMIN_SERVICE_NAME:
            self._do_admin_post(parsed)
            return
        self._do_interactions_post(parsed)

    def _do_admin_get(self, parsed: urllib.parse.ParseResult) -> None:
        if parsed.path == "/":
            text_response(self, HTTPStatus.OK, render_admin_home(), "text/html; charset=utf-8")
            return
        if parsed.path == "/v0/discord/status":
            json_response(self, HTTPStatus.OK, common.build_status_snapshot(limit=20))
            return
        if parsed.path == "/v0/discord/requests":
            json_response(self, HTTPStatus.OK, {"requests": common.list_recent_requests(limit=50)})
            return
        json_response(self, HTTPStatus.NOT_FOUND, {"error": "not_found"})

    def _do_admin_post(self, parsed: urllib.parse.ParseResult) -> None:
        try:
            body = self._read_json_body()
        except Exception as exc:  # noqa: BLE001
            json_response(self, HTTPStatus.BAD_REQUEST, {"error": str(exc)})
            return
        if parsed.path == "/v0/discord/app/import":
            try:
                config = common.import_app_config(common.load_config(), body)
            except ValueError as exc:
                json_response(self, HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return
            json_response(self, HTTPStatus.OK, {"config": common.redact_config(config)})
            return
        if parsed.path == "/v0/discord/bot-token/import":
            token = str(body.get("bot_token", "")).strip()
            if not token:
                json_response(self, HTTPStatus.BAD_REQUEST, {"error": "bot_token is required"})
                return
            common.save_bot_token(token)
            json_response(self, HTTPStatus.OK, {"status": "ok"})
            return
        if parsed.path == "/v0/discord/commands/sync":
            guild_ids = body.get("guild_ids")
            if not isinstance(guild_ids, list):
                guild_id = str(body.get("guild_id", "")).strip()
                guild_ids = [guild_id] if guild_id else []
            if not guild_ids:
                json_response(self, HTTPStatus.BAD_REQUEST, {"error": "guild_id or guild_ids is required"})
                return
            config = common.load_config()
            results: dict[str, Any] = {}
            had_errors = False
            for guild_id in guild_ids:
                try:
                    results[str(guild_id)] = {
                        "status": "ok",
                        "commands": common.sync_guild_commands(config, str(guild_id)),
                    }
                except common.DiscordAPIError as exc:
                    had_errors = True
                    results[str(guild_id)] = {
                        "status": "error",
                        "error": str(exc),
                    }
            json_response(self, HTTPStatus.BAD_GATEWAY if had_errors else HTTPStatus.OK, {"guilds": results})
            return
        json_response(self, HTTPStatus.NOT_FOUND, {"error": "not_found"})

    def _do_interactions_get(self, parsed: urllib.parse.ParseResult) -> None:
        if parsed.path == "/":
            json_response(
                self,
                HTTPStatus.OK,
                {
                    "service": common.current_service_name(),
                    "status": "ok",
                    "interactions_url": common.interactions_url(),
                },
            )
            return
        json_response(self, HTTPStatus.NOT_FOUND, {"error": "not_found"})

    def _do_interactions_post(self, parsed: urllib.parse.ParseResult) -> None:
        if parsed.path != "/v0/discord/interactions":
            json_response(self, HTTPStatus.NOT_FOUND, {"error": "not_found"})
            return
        length = int(self.headers.get("Content-Length", "0"))
        if length > MAX_REQUEST_BYTES:
            json_response(self, HTTPStatus.REQUEST_ENTITY_TOO_LARGE, {"error": "request_too_large"})
            return
        body = self.rfile.read(length) if length > 0 else b""
        config = common.load_config()
        app_cfg = config.get("app", {})
        public_key = str(app_cfg.get("public_key", "")).strip()
        if not public_key:
            json_response(self, HTTPStatus.SERVICE_UNAVAILABLE, {"error": "discord public key is not configured"})
            return
        timestamp = self.headers.get("X-Signature-Timestamp", "")
        signature = self.headers.get("X-Signature-Ed25519", "")
        try:
            skew = abs(int(timestamp) - int(time.time()))
        except ValueError:
            skew = common.REQUEST_RETENTION_SECONDS
        if skew > 10:
            json_response(self, HTTPStatus.UNAUTHORIZED, {"error": "stale_signature_timestamp"})
            return
        if not common.verify_discord_signature(public_key, timestamp, body, signature):
            json_response(self, HTTPStatus.UNAUTHORIZED, {"error": "invalid_signature"})
            return
        try:
            payload = json.loads(body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            json_response(self, HTTPStatus.BAD_REQUEST, {"error": f"invalid JSON payload: {exc}"})
            return
        if not isinstance(payload, dict):
            json_response(self, HTTPStatus.BAD_REQUEST, {"error": "request body must be an object"})
            return

        common.prune_receipts()
        common.prune_pending_modals()

        interaction_type = int(payload.get("type", 0) or 0)
        if interaction_type == 1:
            interaction_response(self, {"type": 1})
            return

        interaction_id = str(payload.get("id", "")).strip()
        if interaction_id:
            existing_receipt = common.load_interaction_receipt(interaction_id)
            if existing_receipt:
                interaction_response(self, replay_response_from_receipt(existing_receipt))
                return

        if interaction_type == 2:
            if not str(payload.get("guild_id", "")).strip():
                interaction_response(self, build_message_response(human_reason("guild_only"), ephemeral=True))
                return
            parsed_command = parse_application_command(payload, common.command_name(config))
            command = str(parsed_command.get("command", "")).strip()
            if command != "fix":
                interaction_response(self, build_message_response(human_reason("command_not_supported"), ephemeral=True))
                return
            prompt = str(parsed_command.get("prompt", "")).strip()
            if prompt:
                summary, context_markdown = prompt_to_summary_context(prompt)
                response, _ = accept_fix_request(payload, summary, context_markdown, interaction_id)
                interaction_response(self, response)
                return
            nonce = secrets.token_hex(12)
            common.save_pending_modal(
                {
                    "nonce": nonce,
                    "guild_id": str(payload.get("guild_id", "")),
                    "channel_id": str(payload.get("channel_id", "")),
                    "user_id": str(((payload.get("member") or {}).get("user") or {}).get("id", "")),
                    "interaction_id": interaction_id,
                    "command": command,
                }
            )
            common.save_interaction_receipt(
                interaction_id,
                {"response_kind": "modal", "modal_nonce": nonce, "command_name": common.command_name(config)},
            )
            interaction_response(self, build_modal_response(nonce))
            return

        if interaction_type == 5:
            nonce = modal_nonce(payload)
            pending = common.load_pending_modal(nonce) if nonce else None
            if not nonce or not pending:
                interaction_response(self, build_message_response(human_reason("modal_expired"), ephemeral=True))
                return
            if str(pending.get("guild_id", "")) != str(payload.get("guild_id", "")) or str(pending.get("channel_id", "")) != str(payload.get("channel_id", "")):
                interaction_response(self, build_message_response(human_reason("bad_modal_context"), ephemeral=True))
                return
            expected_user = str(pending.get("user_id", "")).strip()
            actual_user = str(((payload.get("member") or {}).get("user") or {}).get("id", "")).strip()
            if expected_user and expected_user != actual_user:
                interaction_response(self, build_message_response(human_reason("bad_modal_context"), ephemeral=True))
                return
            fields = extract_modal_fields(payload)
            summary = str(fields.get("summary", "")).strip()
            context_markdown = str(fields.get("context", "")).strip()
            common.remove_pending_modal(nonce)
            response, receipt = accept_fix_request(payload, summary, context_markdown, interaction_id)
            finalize_modal_origin_receipt(str(pending.get("interaction_id", "")), response, receipt)
            interaction_response(self, response)
            return

        interaction_response(self, build_message_response("Unsupported Discord interaction type.", ephemeral=True))


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
        print(f"[{common.current_service_name() or 'discord-intake'}] listening on {socket_path}")
        server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
