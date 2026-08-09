"""Shared helpers for the slack pack scripts.

Mirrors the role of ``discord_intake_common`` in the upstream discord
pack but kept intentionally small for the v0 scaffold. Only the
helpers actually consumed by ``slack_chat_bind`` and
``slack_chat_reply_current`` live here.
"""

from __future__ import annotations

import json
import os
import pathlib
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


CSRF_HEADER = "X-GC-Request"
DEFAULT_GC_API = "http://127.0.0.1:8372"
_LEGACY_ADAPTER_PUBLISH = "http://127.0.0.1:8766/publish"  # legacy nohup mode (pre gc-5rz Phase A)
DEFAULT_ADAPTER_ENV = pathlib.Path.home() / ".config" / "gc-slack-adapter" / "env"


def _maybe_load_adapter_env() -> None:
    """Load SLACK_* keys from the adapter's env file if not in os.environ.

    The adapter's run.sh reads ~/.config/gc-slack-adapter/env. Pack
    commands are invoked from inside agent sessions that don't inherit
    that file, so opportunistically read it here.
    """
    env_path = pathlib.Path(os.environ.get("GC_SLACK_ADAPTER_ENV", str(DEFAULT_ADAPTER_ENV)))
    if not env_path.exists():
        return
    needed = {"SLACK_WORKSPACE_ID", "SLACK_BOT_TOKEN", "SLACK_SIGNING_SECRET",
              "GC_API_BASE_URL"}
    if not needed - os.environ.keys():
        return
    try:
        for raw in env_path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()
            if value and value[0] in ("'", '"') and value[-1] == value[0]:
                value = value[1:-1]
            if key and key not in os.environ:
                os.environ[key] = value
    except OSError:
        return


_maybe_load_adapter_env()


class GCAPIError(RuntimeError):
    """Raised when a gc API call fails."""


class AdapterError(RuntimeError):
    """Raised when the local Slack adapter rejects a publish."""


# --- environment / config -------------------------------------------------

def gc_api_base() -> str:
    return os.environ.get("GC_API_BASE_URL", DEFAULT_GC_API).rstrip("/")


def gc_city_name() -> str:
    name = os.environ.get("GC_CITY_NAME", "").strip()
    if not name:
        raise GCAPIError("GC_CITY_NAME is not set")
    return name


def adapter_publish_url() -> str:
    """Resolve the URL the slack adapter listens on for /publish calls.

    Order of resolution:
      1. ``SLACK_ADAPTER_PUBLISH_URL`` env override (any value, including the
         legacy ``http://127.0.0.1:8766/publish``).
      2. Proxy URL derived from ``GC_API_BASE_URL`` and ``GC_CITY_NAME``:
         ``${GC_API_BASE_URL}/v0/city/${city}/svc/slack/publish``. This is
         the gc-5rz Phase A path: the adapter is supervised as
         proxy_process and listens on a UDS, so adapter-direct TCP is gone.
         Calls to private mutation paths (/publish, /publish-file, /react,
         /identity, /handle-alias) require the ``X-GC-Request`` CSRF header
         on the proxied request — see ``_adapter_csrf_headers``.
      3. Legacy fallback (``http://127.0.0.1:8766/publish``) only when the
         derivation fails (e.g. ``GC_CITY_NAME`` unset). Lets unit-test
         scripts and ad-hoc curls that set ``SLACK_ADAPTER_PUBLISH_URL``
         keep working.
    """
    override = os.environ.get("SLACK_ADAPTER_PUBLISH_URL", "").strip()
    if override:
        return override
    try:
        return f"{gc_api_base()}/v0/city/{gc_city_name()}/svc/slack/publish"
    except GCAPIError:
        return _LEGACY_ADAPTER_PUBLISH


def _adapter_csrf_headers(extra: dict[str, str] | None = None) -> dict[str, str]:
    """Headers to send to the slack adapter via the gc /svc proxy.

    Includes the ``X-GC-Request`` CSRF token gc enforces on private
    service mutation endpoints. Harmless when the override points at a
    legacy ``127.0.0.1:8766`` adapter — that adapter ignores the header.
    """
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        CSRF_HEADER: "1",
    }
    if extra:
        headers.update(extra)
    return headers


def pack_state_dir() -> pathlib.Path:
    """Per-pack state directory inside the active city.

    Falls back to the GC_CITY_PATH-rooted .gc/services/slack/data/ tree.
    """
    base = os.environ.get("GC_CITY_PATH", "").strip()
    if not base:
        raise GCAPIError("GC_CITY_PATH is not set; cannot resolve pack state")
    return pathlib.Path(base) / ".gc" / "services" / "slack" / "data"


def load_pack_config() -> dict[str, Any]:
    path = pack_state_dir() / "config.json"
    if not path.exists():
        return {"version": 1, "bindings": {}}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise GCAPIError(f"corrupt pack state at {path}: {exc}") from exc


def save_pack_config(cfg: dict[str, Any]) -> None:
    path = pack_state_dir() / "config.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(cfg, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


# --- HTTP helpers ---------------------------------------------------------

def _request(method: str, url: str, body: dict[str, Any] | None = None,
             *, csrf: bool = True, timeout: float = 30.0) -> dict[str, Any]:
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    if csrf:
        headers[CSRF_HEADER] = "1"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise GCAPIError(f"{method} {url} -> {exc.code}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise GCAPIError(f"{method} {url} failed: {exc}") from exc
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise GCAPIError(f"{method} {url}: response is not JSON: {raw!r}") from exc


def gc_post(path: str, body: dict[str, Any]) -> dict[str, Any]:
    url = f"{gc_api_base()}/v0/city/{gc_city_name()}{path}"
    return _request("POST", url, body)


def gc_get(path: str) -> dict[str, Any]:
    url = f"{gc_api_base()}/v0/city/{gc_city_name()}{path}"
    return _request("GET", url, csrf=False)


# --- publish --------------------------------------------------------------
#
# Two paths exist:
#
#   * publish_via_gc_outbound — POSTs to gc's /v0/city/{city}/extmsg/outbound.
#     gc resolves the binding, calls the registered adapter (Slack), records
#     transcript + delivery context, and emits ExtMsgOutbound. Peer fanout
#     to other sessions in the same conversation group fires from gc, which
#     is what makes bind-room peer-visibility work end-to-end.
#
#   * publish_via_adapter — POSTs directly to the local adapter's /publish.
#     Bypasses gc entirely. Useful for adapter-only smoke tests, but peers
#     bound to the same room never see the message because gc never observes
#     the publish. Kept for diagnostics; production replies should use the
#     gc path.

def publish_via_gc_outbound(
    *,
    session_id: str,
    scope_id: str,
    provider: str,
    account_id: str,
    conversation_id: str,
    kind: str,
    text: str,
    reply_to_message_id: str = "",
    idempotency_key: str = "",
) -> dict[str, Any]:
    """Publish through gc so peer fanout + transcript recording fire.

    Returns the raw receipt JSON from the adapter. HTTP-200 with
    `delivered: false` is a real failure (slack auth/channel/rate-limit
    rejection); callers MUST pass the result through
    :func:`interpret_publish_receipt` and fail loudly on non-delivery,
    or stamp loop-close on a silently-lost post.
    """
    body: dict[str, Any] = {
        "session_id": session_id,
        "conversation": {
            "scope_id": scope_id,
            "provider": provider,
            "account_id": account_id,
            "conversation_id": conversation_id,
            "kind": kind,
        },
        "text": text,
    }
    if reply_to_message_id:
        body["reply_to_message_id"] = reply_to_message_id
    if idempotency_key:
        body["idempotency_key"] = idempotency_key
    return gc_post("/extmsg/outbound", body)


def publish_via_adapter(
    *,
    session_id: str,
    scope_id: str,
    provider: str,
    account_id: str,
    conversation_id: str,
    kind: str,
    text: str,
    reply_to_message_id: str = "",
    idempotency_key: str = "",
) -> dict[str, Any]:
    """Publish directly to the local adapter (skips gc; peers won't see it).

    Same delivered-field contract as :func:`publish_via_gc_outbound`:
    callers MUST inspect the receipt via :func:`interpret_publish_receipt`.
    HTTP-200 alone is insufficient evidence of slack delivery.
    """
    body: dict[str, Any] = {
        "session_id": session_id,
        "conversation": {
            "scope_id": scope_id,
            "provider": provider,
            "account_id": account_id,
            "conversation_id": conversation_id,
            "kind": kind,
        },
        "text": text,
    }
    if reply_to_message_id:
        body["reply_to_message_id"] = reply_to_message_id
    if idempotency_key:
        body["idempotency_key"] = idempotency_key
    try:
        return _request("POST", adapter_publish_url(), body)
    except GCAPIError as exc:
        raise AdapterError(str(exc)) from exc


def interpret_publish_receipt(result: Any) -> tuple[bool, str]:
    """Inspect a publish-receipt JSON and return (delivered, failure_kind).

    The HTTP layer can succeed (200) while the receipt's `delivered`
    field is false — slack rejected for auth, channel, rate-limit, etc.
    Without this inspection, callers stamp loop_close_posted_at based on
    a 200 alone and the post is silently lost.

    Handles three shapes seen in the wild:
      - Adapter-direct (lowercase keys, per adapter/main.go publishReceipt
        JSON tags): {"delivered": bool, "failure_kind": "...", ...}
      - gc-wrapped, capitalized (Go struct fields without json tags):
        {"Receipt": {"Delivered": bool, "FailureKind": "...", ...}}
      - gc-wrapped, lowercase: {"receipt": {"delivered": bool, ...}}

    Fail-closed semantics: if no recognizable `delivered` key is found,
    returns (False, "schema_mismatch") — better to noisily fail an
    unknown response than silently mask one.
    """
    if not isinstance(result, dict):
        return False, "non_dict_response"
    if "delivered" in result:
        delivered = bool(result.get("delivered"))
        kind = str(result.get("failure_kind", "")) if not delivered else ""
        return delivered, kind
    receipt = result.get("Receipt") if isinstance(result.get("Receipt"), dict) else None
    if receipt is None:
        receipt = result.get("receipt") if isinstance(result.get("receipt"), dict) else None
    if isinstance(receipt, dict):
        if "Delivered" in receipt:
            delivered = bool(receipt.get("Delivered"))
            kind = str(receipt.get("FailureKind", "")) if not delivered else ""
            return delivered, kind
        if "delivered" in receipt:
            delivered = bool(receipt.get("delivered"))
            kind = str(receipt.get("failure_kind", "")) if not delivered else ""
            return delivered, kind
    return False, "schema_mismatch"


# --- session resolution ---------------------------------------------------

def current_session_id() -> str:
    """Best-effort session-id lookup from the calling environment.

    Pack commands are typically invoked from inside a session's tmux
    pane, where gc sets GC_SESSION_ID. Fall back to GC_SESSION_NAME +
    a gc API resolve if needed.
    """
    sid = os.environ.get("GC_SESSION_ID", "").strip()
    if sid:
        return sid
    name = os.environ.get("GC_SESSION_NAME", "").strip()
    if not name:
        raise GCAPIError(
            "neither GC_SESSION_ID nor GC_SESSION_NAME set; pass --session explicitly")
    # Resolve via list (good enough for v0).
    res = gc_get("/sessions")
    for entry in res.get("items", []):
        if entry.get("alias") == name or entry.get("session_name") == name:
            sid = entry.get("id", "")
            if sid:
                return sid
    raise GCAPIError(f"could not resolve session id for name {name!r}")


# --- inbound-event lookup -------------------------------------------------

# Windows tried in order when scanning for a session's latest inbound.
# The events endpoint returns oldest-first with no descending order, and
# `limit` keeps the OLDEST slice of the window — so a wide window on a
# busy city truncates away exactly the newest events we want. Starting
# narrow makes each scan a newest-slice view: the common case (reply
# soon after an inbound) resolves in the first cheap query, and the
# wider fallbacks cover sessions whose latest inbound predates the
# narrow window (e.g. an agent idle overnight) without paying the
# truncation risk up front.
_INBOUND_SCAN_WINDOWS = ("10m", "2h", "48h")


def find_latest_inbound_for_session(session_id: str) -> dict[str, Any] | None:
    """Find the most recent extmsg.inbound event targeting session_id.

    Queries the gc events stream (HTTP, not SSE — single shot snapshot)
    over escalating `since` windows (see _INBOUND_SCAN_WINDOWS).
    Returns the parsed event dict, or None if no window matched.

    Without the time bound, `gc slack reply-current --thread-current`
    bails with 'no inbound event...' on busy cities (oldest-first +
    limit returns only ancient history) and the agent falls back to a
    non-threaded publish-to-channel. limit=500 is a defensive cap; when
    the server reports more matching events than it returned
    (total > items), the scan was incomplete and a warning is emitted
    so truncation is never silent.
    """
    for window in _INBOUND_SCAN_WINDOWS:
        url = (
            f"{gc_api_base()}/v0/city/{gc_city_name()}/events"
            f"?type=extmsg.inbound&since={window}&limit=500"
        )
        res = _request("GET", url, csrf=False)
        raw = res.get("items", [])
        total = res.get("total", len(raw))
        if total > len(raw):
            print(
                f"warning: inbound event scan ({window} window) truncated to "
                f"{len(raw)}/{total} oldest events; the latest inbound may be missed",
                file=sys.stderr,
            )
        matches = [e for e in raw if (e.get("payload") or {}).get("target_session") == session_id]
        if matches:
            return matches[-1]  # events are in chronological order
    return None


def find_latest_inbound_message_id_for_session(
    session_id: str,
) -> tuple[str, dict[str, str]] | None:
    """Find the latest inbound transcript entry routed to this session.

    Returns (provider_message_id, conversation_dict) on hit, or None if no
    inbound has reached this session yet. Thin wrapper over
    find_latest_inbound_thread_for_session for callers that only need the
    message id (react/upload anchoring).
    """
    match = find_latest_inbound_thread_for_session(session_id)
    if match is None:
        return None
    mid, _thread_root, conv = match
    return mid, conv


def find_latest_inbound_thread_for_session(
    session_id: str,
) -> tuple[str, str, dict[str, str]] | None:
    """Find the latest inbound transcript entry routed to this session.

    Returns (provider_message_id, thread_root, conversation_dict) on hit,
    or None if no inbound has reached this session yet. thread_root is the
    entry's ReplyToMessageID — the Slack thread_ts the adapter stamps on a
    thread-reply inbound — or "" when the inbound was a plain channel/DM
    message (gp-i62).

    The lookup chain:
      1. Find the latest extmsg.inbound event targeting this session.
      2. Read its conversation_id from the event payload.
      3. Query the transcript for that conversation newest-first
         (order=desc, gascity#3128), take the first entry whose
         Kind=="inbound", and return its ProviderMessageID +
         ReplyToMessageID.

    Two queries (event + transcript) because the inbound event payload
    intentionally does NOT carry message_id — that field lives in the
    transcript and is the canonical source. See engdocs/architecture/
    api-control-plane.md for the typed-wire rationale.

    The transcript query MUST be newest-first: the endpoint's oldest-first
    default with a bounded limit hides the most recent inbound on busy
    conversations, threading the reply under a stale message.
    """
    event = find_latest_inbound_for_session(session_id)
    if event is None:
        return None
    payload = event.get("payload") or {}
    conv_id = (payload.get("conversation_id") or "").strip()
    provider = (payload.get("provider") or "").strip()
    if not conv_id or not provider:
        return None
    workspace = os.environ.get("SLACK_WORKSPACE_ID", "").strip()
    # `kind` is mandatory on the transcript GET (extmsg ConversationRef
    # validates it). We try room first since rig channels are the
    # primary use case; fall through to dm if no rows come back.
    items: list[dict[str, Any]] = []
    for kind in ("room", "dm"):
        qs = (
            f"scope_id={gc_city_name()}&provider={provider}"
            f"&conversation_id={conv_id}&kind={kind}&order=desc&limit=500"
        )
        if workspace:
            qs += f"&account_id={workspace}"
        try:
            res = gc_get(f"/extmsg/transcript?{qs}")
        except GCAPIError:
            continue
        items = res.get("items") or []
        if items:
            break
    # items are newest-first (order=desc): the first inbound is the latest.
    for entry in items:
        if (entry.get("Kind") or entry.get("kind")) == "inbound":
            mid = (entry.get("ProviderMessageID") or entry.get("provider_message_id") or "").strip()
            if mid:
                thread_root = (
                    entry.get("ReplyToMessageID") or entry.get("reply_to_message_id") or ""
                ).strip()
                conv = entry.get("Conversation") or entry.get("conversation") or {}
                return mid, thread_root, {
                    "scope_id": conv.get("ScopeID") or conv.get("scope_id") or gc_city_name(),
                    "provider": conv.get("Provider") or conv.get("provider") or provider,
                    "account_id": conv.get("AccountID") or conv.get("account_id") or workspace,
                    "conversation_id": conv.get("ConversationID") or conv.get("conversation_id") or conv_id,
                    "kind": conv.get("Kind") or conv.get("kind") or "room",
                }
    return None


def react_via_adapter(
    *,
    conversation_id: str,
    message_id: str,
    emoji: str,
) -> dict[str, Any]:
    """POST a reaction directly to the local adapter /react endpoint.

    Reactions are not part of the gc extmsg API — they go straight to
    the adapter, which calls Slack reactions.add. The adapter listens
    on the legacy internal TCP listener.
    """
    base = adapter_publish_url()
    # /publish -> /react: same host:port, different path.
    react_url = base.rsplit("/", 1)[0] + "/react" if base.endswith("/publish") else base.rstrip("/") + "/react"
    body = {
        "conversation": {"conversation_id": conversation_id},
        "message_id": message_id,
        "emoji": emoji,
    }
    headers = _adapter_csrf_headers()
    req = urllib.request.Request(
        react_url,
        data=json.dumps(body).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise AdapterError(f"POST {react_url} -> {exc.code}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise AdapterError(f"POST {react_url} failed: {exc}") from exc
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise AdapterError(f"POST {react_url}: response is not JSON: {raw!r}") from exc


def register_identity_via_adapter(
    *,
    session_id: str,
    username: str = "",
    icon_url: str = "",
    icon_emoji: str = "",
) -> dict[str, Any]:
    """POST a per-session Slack identity override to the adapter /identity endpoint.

    The adapter persists the override to disk and applies it to every
    subsequent /publish call for the same session_id (chat:write.customize
    username/icon_url/icon_emoji on chat.postMessage). Empty fields mean
    "do not override" — pass at least one of username/icon_url/icon_emoji
    or the override is a no-op.
    """
    base = adapter_publish_url()
    identity_url = (
        base.rsplit("/", 1)[0] + "/identity"
        if base.endswith("/publish")
        else base.rstrip("/") + "/identity"
    )
    body: dict[str, Any] = {"session_id": session_id}
    if username:
        body["username"] = username
    if icon_url:
        body["icon_url"] = icon_url
    if icon_emoji:
        body["icon_emoji"] = icon_emoji
    headers = _adapter_csrf_headers()
    req = urllib.request.Request(
        identity_url,
        data=json.dumps(body).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise AdapterError(f"POST {identity_url} -> {exc.code}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise AdapterError(f"POST {identity_url} failed: {exc}") from exc
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise AdapterError(f"POST {identity_url}: response is not JSON: {raw!r}") from exc


def remove_identity_via_adapter(*, session_id: str) -> dict[str, Any]:
    """Remove a per-session Slack identity override from the adapter.

    Calls DELETE /identity?session_id=... on the adapter. Idempotent —
    deleting a missing session id returns existed=false without error.
    """
    base = adapter_publish_url()
    identity_url = (
        base.rsplit("/", 1)[0] + "/identity"
        if base.endswith("/publish")
        else base.rstrip("/") + "/identity"
    )
    delete_url = identity_url + "?session_id=" + urllib.parse.quote(session_id, safe="")
    headers = _adapter_csrf_headers()
    req = urllib.request.Request(delete_url, headers=headers, method="DELETE")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise AdapterError(f"DELETE {delete_url} -> {exc.code}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise AdapterError(f"DELETE {delete_url} failed: {exc}") from exc
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise AdapterError(f"DELETE {delete_url}: response is not JSON: {raw!r}") from exc


def register_handle_alias_via_adapter(
    *,
    handle: str,
    session_id: str,
) -> dict[str, Any]:
    """Register a handle -> session mapping with the adapter /handle-alias endpoint.

    Used for cross-channel address-by-handle: when a Slack inbound parses
    `@<handle>:` and the handle matches an alias, the adapter delivers
    the message directly to the aliased session via gc's session-message
    API regardless of channel binding. Empty session_id removes the alias.
    """
    base = adapter_publish_url()
    alias_url = (
        base.rsplit("/", 1)[0] + "/handle-alias"
        if base.endswith("/publish")
        else base.rstrip("/") + "/handle-alias"
    )
    body = {"handle": handle, "session_id": session_id}
    headers = _adapter_csrf_headers()
    req = urllib.request.Request(
        alias_url,
        data=json.dumps(body).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise AdapterError(f"POST {alias_url} -> {exc.code}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise AdapterError(f"POST {alias_url} failed: {exc}") from exc
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise AdapterError(f"POST {alias_url}: response is not JSON: {raw!r}") from exc


def remove_handle_alias_via_adapter(*, handle: str) -> dict[str, Any]:
    """Remove a handle -> session alias via the adapter DELETE endpoint.

    Idempotent — deleting a missing handle returns existed=false. Unlike
    ``register_handle_alias_via_adapter`` with empty session_id, the
    DELETE verb makes intent unambiguous in adapter logs.
    """
    base = adapter_publish_url()
    alias_url = (
        base.rsplit("/", 1)[0] + "/handle-alias"
        if base.endswith("/publish")
        else base.rstrip("/") + "/handle-alias"
    )
    delete_url = alias_url + "?handle=" + urllib.parse.quote(handle, safe="")
    headers = _adapter_csrf_headers()
    req = urllib.request.Request(delete_url, headers=headers, method="DELETE")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise AdapterError(f"DELETE {delete_url} -> {exc.code}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise AdapterError(f"DELETE {delete_url} failed: {exc}") from exc
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise AdapterError(f"DELETE {delete_url}: response is not JSON: {raw!r}") from exc


def publish_to_channel_via_adapter(
    *,
    session_id: str,
    conversation_id: str,
    text: str,
    kind: str = "room",
    thread_ts: str = "",
    idempotency_key: str = "",
) -> dict[str, Any]:
    """POST a publish directly to the adapter with explicit conversation override.

    Bypasses gc's binding lookup — used by mayor/cos to reply into channels
    they have no binding for, after receiving a `Slack address-by-handle`
    system reminder. session_id flows through so the adapter applies the
    matching identity registry override.

    Same delivered-field contract as :func:`publish_via_gc_outbound`:
    callers MUST inspect the receipt via :func:`interpret_publish_receipt`.
    """
    publish_url = adapter_publish_url()
    body: dict[str, Any] = {
        "session_id": session_id,
        "conversation": {
            "scope_id": gc_city_name(),
            "provider": "slack",
            "account_id": os.environ.get("SLACK_WORKSPACE_ID", ""),
            "conversation_id": conversation_id,
            "kind": kind,
        },
        "text": text,
    }
    if thread_ts:
        body["reply_to_message_id"] = thread_ts
    if idempotency_key:
        body["idempotency_key"] = idempotency_key
    headers = _adapter_csrf_headers()
    req = urllib.request.Request(
        publish_url,
        data=json.dumps(body).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise AdapterError(f"POST {publish_url} -> {exc.code}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise AdapterError(f"POST {publish_url} failed: {exc}") from exc
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise AdapterError(f"POST {publish_url}: response is not JSON: {raw!r}") from exc


def upload_via_gc_outbound_file(
    *,
    session_id: str,
    scope_id: str,
    provider: str,
    account_id: str,
    conversation_id: str,
    kind: str,
    file_path: str,
    filename: str = "",
    initial_comment: str = "",
    thread_ts: str = "",
    title: str = "",
    idempotency_key: str = "",
) -> dict[str, Any]:
    """Upload a file through gc's /extmsg/outbound-file endpoint.

    Matches ``publish_via_gc_outbound`` but for file payloads. gc resolves
    the binding, hands off to the adapter via the FileTransportAdapter
    interface, records the outbound transcript entry, and fans out to
    other sessions bound to the same conversation. The file body is not
    streamed through gc — ``file_path`` is interpreted on the adapter
    side (gc and the adapter share a filesystem).
    """
    body: dict[str, Any] = {
        "session_id": session_id,
        "conversation": {
            "scope_id": scope_id,
            "provider": provider,
            "account_id": account_id,
            "conversation_id": conversation_id,
            "kind": kind,
        },
        "file_path": file_path,
    }
    if filename:
        body["filename"] = filename
    if initial_comment:
        body["initial_comment"] = initial_comment
    if thread_ts:
        body["reply_to_message_id"] = thread_ts
    if title:
        body["title"] = title
    if idempotency_key:
        body["idempotency_key"] = idempotency_key
    return gc_post("/extmsg/outbound-file", body)


def upload_via_adapter(
    *,
    session_id: str,
    conversation_id: str,
    file_path: str,
    kind: str = "room",
    filename: str = "",
    initial_comment: str = "",
    thread_ts: str = "",
    title: str = "",
    idempotency_key: str = "",
) -> dict[str, Any]:
    """POST a file upload directly to the adapter /publish-file endpoint.

    Mirrors ``publish_to_channel_via_adapter`` but for files. The adapter
    handles Slack's three-step upload protocol (getUploadURLExternal →
    PUT bytes → completeUploadExternal). session_id flows through for
    log parity with /publish; Slack's file-upload API does not honor
    chat:write.customize identity overrides on the file post itself.

    Bot scope ``files:write`` is required. When missing, the adapter
    returns ``{delivered: false, failure_kind: "auth", error: "missing_scope"}``.
    """
    base = adapter_publish_url()
    upload_url = (
        base.rsplit("/", 1)[0] + "/publish-file"
        if base.endswith("/publish")
        else base.rstrip("/") + "/publish-file"
    )
    body: dict[str, Any] = {
        "session_id": session_id,
        "conversation": {
            "scope_id": gc_city_name(),
            "provider": "slack",
            "account_id": os.environ.get("SLACK_WORKSPACE_ID", ""),
            "conversation_id": conversation_id,
            "kind": kind,
        },
        "file_path": file_path,
    }
    if filename:
        body["filename"] = filename
    if initial_comment:
        body["initial_comment"] = initial_comment
    if thread_ts:
        body["reply_to_message_id"] = thread_ts
    if title:
        body["title"] = title
    if idempotency_key:
        body["idempotency_key"] = idempotency_key
    try:
        return _request("POST", upload_url, body, timeout=60.0)
    except GCAPIError as exc:
        raise AdapterError(str(exc)) from exc


def look_up_binding(session_id: str) -> dict[str, Any] | None:
    """Resolve a session's most recent active extmsg binding."""
    res = gc_get(f"/extmsg/bindings?session_id={session_id}")
    items = res.get("items", [])
    for entry in reversed(items):
        if entry.get("Status") == "active":
            return entry.get("Conversation") or {}
    return None
