"""In-process mock of gc's HTTP API surface for slack-pack E2E tests.

Implements the minimum subset slack-pack scripts touch:

  * ``GET  /v0/city/<city>/extmsg/bindings?session_id=<sid>``
  * ``POST /v0/city/<city>/extmsg/outbound``

For ``/extmsg/outbound``, the mock optionally forwards the request to a
configured *adapter callback URL* with the ``X-GC-Request: true`` header
that gc's real ``http_adapter`` sets after gastownhall/gascity#1818.
This lets a test wire ``GcMock`` and ``SlackMock`` together to exercise
the full ``script -> gc -> adapter -> Slack`` round-trip without running
the real gc binary.

Tests assert against ``GcMock.calls()`` for the gc-side leg and
``SlackMock.calls()`` for the adapter-side leg.
"""

from __future__ import annotations

import http.server
import json
import socketserver
import threading
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any


_GO_DURATION_UNIT_SECONDS = {"s": 1.0, "m": 60.0, "h": 3600.0}


def _parse_go_duration_seconds(value: str) -> float | None:
    """Parse the single-unit Go duration forms the pack emits (10m, 2h, 30s).

    Returns seconds, or None for empty/unparseable input (the real
    endpoint 400s on garbage; the mock treats it as 'no since filter'
    since the pack only ever sends well-formed values).
    """
    value = value.strip()
    if not value:
        return None
    unit = value[-1]
    if unit not in _GO_DURATION_UNIT_SECONDS:
        return None
    try:
        magnitude = float(value[:-1])
    except ValueError:
        return None
    return magnitude * _GO_DURATION_UNIT_SECONDS[unit]


@dataclass
class GcCall:
    """A single gc API call captured at the mock."""

    method: str
    path: str
    query: dict[str, str]
    body: Any
    headers: dict[str, str]
    at: float


class GcMock:
    """HTTP server that stands in for gc's API surface in tests."""

    def __init__(
        self,
        city_name: str = "test-city",
        *,
        enforce_authorization: bool = False,
    ) -> None:
        self.city_name = city_name
        # When True, /extmsg/outbound checks that the (session_id,
        # conversation_id) pair is authorized via a direct binding OR a
        # group membership. When False (default), all outbound requests
        # succeed unconditionally — useful for tests that don't care
        # about the auth path.
        self.enforce_authorization = enforce_authorization
        self._calls: list[GcCall] = []
        self._lock = threading.Lock()
        self._bindings: dict[str, list[dict[str, Any]]] = {}
        self._inbound_events: list[dict[str, Any]] = []
        # transcripts[(provider, conversation_id, kind)] = list of entries
        # in chronological order. Each entry mirrors gc's transcript shape:
        # {Kind, ProviderMessageID, Conversation: {ScopeID, Provider, ...}}.
        self._transcripts: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
        # group_memberships[session_id] = set of conversation_ids the
        # session can publish into via group membership. This models the
        # post-gastownhall/gascity#1831 fallback path where a session
        # without a direct binding can still authorize through an
        # extmsg-group it participates in.
        self._group_memberships: dict[str, set[str]] = {}
        self._adapter_callback_url: str | None = None
        self._msg_counter = 0
        self._server = self._build_server()
        self._thread = threading.Thread(
            target=self._server.serve_forever, daemon=True, name="gc-mock"
        )
        self._thread.start()

    def _build_server(self) -> socketserver.TCPServer:
        outer = self

        class _Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                outer._handle(self, "GET")

            def do_POST(self) -> None:  # noqa: N802
                outer._handle(self, "POST")

            def log_message(self, *args: Any, **kwargs: Any) -> None:
                return

        return socketserver.TCPServer(("127.0.0.1", 0), _Handler)

    @property
    def url(self) -> str:
        """Base URL of the mock — point ``GC_API_BASE_URL`` here."""
        host, port = self._server.server_address
        return f"http://{host}:{port}"

    def calls(self) -> list[GcCall]:
        with self._lock:
            return list(self._calls)

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()

    def register_binding(
        self,
        session_id: str,
        *,
        conversation_id: str,
        kind: str = "dm",
        provider: str = "slack",
        account_id: str = "T0TESTWS",
    ) -> None:
        """Register an active binding so ``look_up_binding(session_id)`` returns this conversation."""
        scope_id = self.city_name
        entry = {
            "Status": "active",
            "Conversation": {
                "scope_id": scope_id,
                "provider": provider,
                "account_id": account_id,
                "conversation_id": conversation_id,
                "kind": kind,
            },
        }
        with self._lock:
            self._bindings.setdefault(session_id, []).append(entry)

    def register_inbound_event(
        self,
        *,
        target_session: str,
        conversation_id: str,
        provider: str = "slack",
        kind: str = "dm",
        message_id: str = "",
        age_seconds: float = 0.0,
    ) -> None:
        """Seed an extmsg.inbound event so reply-current's lookup path resolves.

        Mirrors the payload shape produced by gc when a real Slack event_callback
        arrives at the slack-pack adapter and gets forwarded to /extmsg/inbound.

        ``age_seconds`` backdates the event so tests can exercise the
        client's escalating ``since`` windows; it is mock-internal and
        never serialized into the response.
        """
        event = {
            "type": "extmsg.inbound",
            "payload": {
                "target_session": target_session,
                "conversation_id": conversation_id,
                "provider": provider,
                "kind": kind,
                "message_id": message_id,
            },
            "_age_seconds": age_seconds,
        }
        with self._lock:
            self._inbound_events.append(event)

    def register_transcript_entry(
        self,
        *,
        conversation_id: str,
        provider: str = "slack",
        kind: str = "room",
        provider_message_id: str,
        message_kind: str = "inbound",
        account_id: str = "T0TESTWS",
        reply_to_message_id: str = "",
    ) -> None:
        """Seed a transcript entry for ``GET /extmsg/transcript`` lookups.

        ``message_kind`` is the transcript-row Kind ("inbound" / "outbound").
        ``--thread-current`` resolves the latest *inbound* entry for the
        session's conversation. ``reply_to_message_id`` mirrors the thread
        root ts the adapter stamps on a thread-reply inbound (gp-i62);
        empty means a plain channel/DM message.
        """
        key = (provider, conversation_id, kind)
        entry = {
            "Kind": message_kind,
            "ProviderMessageID": provider_message_id,
            "ReplyToMessageID": reply_to_message_id,
            "Conversation": {
                "ScopeID": self.city_name,
                "Provider": provider,
                "AccountID": account_id,
                "ConversationID": conversation_id,
                "Kind": kind,
            },
        }
        with self._lock:
            self._transcripts.setdefault(key, []).append(entry)

    def register_group_membership(
        self, session_id: str, conversation_id: str
    ) -> None:
        """Mark session_id as a participant in the extmsg-group bound to conversation_id.

        With ``enforce_authorization=True``, this lets the session publish to
        the conversation without a direct binding — the post-#1831 group-
        fallback path. Pre-#1831 behavior would 403 the publish.
        """
        with self._lock:
            self._group_memberships.setdefault(session_id, set()).add(conversation_id)

    def set_adapter_callback(self, url: str) -> None:
        """Make ``/extmsg/outbound`` forward to this URL with X-GC-Request: true.

        Mirrors gc's ``http_adapter`` behavior post-#1818.
        """
        self._adapter_callback_url = url

    def _next_message_id(self) -> str:
        with self._lock:
            self._msg_counter += 1
            n = self._msg_counter
        return f"170000000{n}.00010{n}"

    def _handle(
        self, req: http.server.BaseHTTPRequestHandler, method: str
    ) -> None:
        parsed = urllib.parse.urlparse(req.path)
        path = parsed.path
        query = dict(urllib.parse.parse_qsl(parsed.query))

        body: Any = None
        length = int(req.headers.get("Content-Length", "0"))
        raw = req.rfile.read(length) if length else b""
        if raw:
            try:
                body = json.loads(raw)
            except json.JSONDecodeError as exc:
                req.send_response(400)
                req.end_headers()
                req.wfile.write(f"invalid json: {exc}".encode())
                return

        call = GcCall(
            method=method,
            path=path,
            query=query,
            body=body,
            headers=dict(req.headers),
            at=time.time(),
        )
        with self._lock:
            self._calls.append(call)

        prefix = f"/v0/city/{self.city_name}"
        if not path.startswith(prefix):
            req.send_response(404)
            req.end_headers()
            req.wfile.write(b"unknown city")
            return

        suffix = path[len(prefix):]

        if method == "GET" and suffix == "/extmsg/bindings":
            self._handle_bindings_lookup(req, query)
            return

        if method == "POST" and suffix == "/extmsg/outbound":
            self._handle_outbound(req, body)
            return

        if method == "GET" and suffix == "/events":
            self._handle_events_query(req, query)
            return

        if method == "GET" and suffix == "/extmsg/transcript":
            self._handle_transcript_query(req, query)
            return

        req.send_response(404)
        req.end_headers()
        req.wfile.write(f"unhandled: {method} {suffix}".encode())

    def _handle_bindings_lookup(
        self,
        req: http.server.BaseHTTPRequestHandler,
        query: dict[str, str],
    ) -> None:
        session_id = query.get("session_id", "")
        with self._lock:
            entries = list(self._bindings.get(session_id, []))
        resp = json.dumps({"items": entries}).encode()
        req.send_response(200)
        req.send_header("Content-Type", "application/json")
        req.send_header("Content-Length", str(len(resp)))
        req.end_headers()
        req.wfile.write(resp)

    def _handle_events_query(
        self,
        req: http.server.BaseHTTPRequestHandler,
        query: dict[str, str],
    ) -> None:
        """GET /events — scoped event-stream snapshot. Used for inbound-event lookup.

        Mirrors the real endpoint's contract (huma_handlers_events.go):
        events are returned OLDEST-first, ``since`` (Go duration string)
        filters by age, ``limit`` keeps the oldest slice (the newest
        events are what truncation drops), and ``total`` reports the
        pre-truncation match count so clients can detect incompleteness.
        """
        wanted_type = query.get("type", "")
        with self._lock:
            if wanted_type == "extmsg.inbound":
                items = list(self._inbound_events)
            else:
                items = []
        since = _parse_go_duration_seconds(query.get("since", ""))
        if since is not None:
            items = [e for e in items if float(e.get("_age_seconds", 0.0)) <= since]
        items = [{k: v for k, v in e.items() if k != "_age_seconds"} for e in items]
        try:
            limit = int(query.get("limit", "100"))
        except ValueError:
            limit = 100
        total = len(items)
        items = items[:limit]
        resp = json.dumps({"items": items, "total": total}).encode()
        req.send_response(200)
        req.send_header("Content-Type", "application/json")
        req.send_header("Content-Length", str(len(resp)))
        req.end_headers()
        req.wfile.write(resp)

    def _handle_transcript_query(
        self,
        req: http.server.BaseHTTPRequestHandler,
        query: dict[str, str],
    ) -> None:
        """GET /extmsg/transcript?scope_id=&provider=&conversation_id=&kind=[&account_id=][&order=][&limit=]

        Mirrors the real endpoint's pagination contract (gascity#3128):
        oldest-first default, ``order=desc``, ``limit`` default 100 /
        max 500. Faithful truncation is what makes the stale-thread-root
        regression reproducible in tests.
        """
        provider = query.get("provider", "")
        conversation_id = query.get("conversation_id", "")
        kind = query.get("kind", "")
        key = (provider, conversation_id, kind)
        with self._lock:
            items = list(self._transcripts.get(key, []))
        if query.get("order", "asc") == "desc":
            items.reverse()
        try:
            limit = int(query.get("limit", "100"))
        except ValueError:
            limit = 100
        items = items[: min(limit, 500)]
        resp = json.dumps({"items": items}).encode()
        req.send_response(200)
        req.send_header("Content-Type", "application/json")
        req.send_header("Content-Length", str(len(resp)))
        req.end_headers()
        req.wfile.write(resp)

    def _resolve_authorization(
        self, session_id: str, conversation_id: str
    ) -> tuple[bool, str]:
        """Mirror gc's /extmsg/outbound auth chain (post-#1831):

          1. direct binding for (session, conversation) → "binding"
          2. else group membership for (session, conversation) → "group_membership"
          3. else unauthorized
        """
        with self._lock:
            for entry in self._bindings.get(session_id, []):
                conv = entry.get("Conversation") or {}
                if (
                    entry.get("Status") == "active"
                    and conv.get("conversation_id") == conversation_id
                ):
                    return True, "binding"
            if conversation_id in self._group_memberships.get(session_id, set()):
                return True, "group_membership"
        return False, ""

    def _handle_outbound(
        self,
        req: http.server.BaseHTTPRequestHandler,
        body: Any,
    ) -> None:
        if not isinstance(body, dict):
            req.send_response(400)
            req.end_headers()
            req.wfile.write(b"outbound body must be a JSON object")
            return

        if self.enforce_authorization:
            session_id = body.get("session_id", "")
            conv = body.get("conversation") or {}
            conversation_id = conv.get("conversation_id", "")
            authorized, auth_via = self._resolve_authorization(
                session_id, conversation_id
            )
            if not authorized:
                # Mirror gc's response shape on auth failure.
                resp = json.dumps({
                    "Receipt": {
                        "Delivered": False,
                        "MessageID": "",
                        "Conversation": conv,
                        "FailureKind": "auth",
                        "FailureMessage": (
                            f"session {session_id!r} not authorized for "
                            f"conversation {conversation_id!r}: no active "
                            "binding and no group membership"
                        ),
                    }
                }).encode()
                req.send_response(200)  # gc returns 200 with FailureKind, not 403
                req.send_header("Content-Type", "application/json")
                req.send_header("Content-Length", str(len(resp)))
                req.end_headers()
                req.wfile.write(resp)
                return
        else:
            auth_via = "unenforced"

        message_id = self._next_message_id()
        delivered = True
        forward_error: str | None = None

        if self._adapter_callback_url:
            # Model gc's http_adapter forwarding the publish to the
            # registered adapter callback. Setting X-GC-Request: true
            # is the post-#1818 behavior — a regression that drops it
            # makes the SlackMock's CSRF gate 403 here.
            conv = body.get("conversation") or {}
            forward_payload = {
                "channel": conv.get("conversation_id", ""),
                "text": body.get("text", ""),
                "thread_ts": body.get("reply_to_message_id", ""),
                "idempotency_key": body.get("idempotency_key", ""),
            }
            try:
                fwd_req = urllib.request.Request(
                    self._adapter_callback_url + "/api/chat.postMessage",
                    data=json.dumps(forward_payload).encode(),
                    method="POST",
                    headers={
                        "Content-Type": "application/json",
                        "X-GC-Request": "true",
                    },
                )
                with urllib.request.urlopen(fwd_req, timeout=2) as fwd_resp:
                    fwd_data = json.loads(fwd_resp.read() or b"{}")
                    message_id = fwd_data.get("ts") or message_id
            except Exception as exc:  # urllib HTTPError / URLError / ...
                delivered = False
                forward_error = str(exc)

        receipt: dict[str, Any] = {
            "Receipt": {
                "Delivered": delivered,
                "MessageID": message_id,
                "Conversation": body.get("conversation"),
            }
        }
        if self.enforce_authorization:
            # Surface which auth path was taken so tests can assert on
            # the binding-vs-group-fallback distinction.
            receipt["Receipt"]["AuthVia"] = auth_via
        if forward_error:
            receipt["Receipt"]["FailureKind"] = "adapter"
            receipt["Receipt"]["FailureMessage"] = forward_error

        resp = json.dumps(receipt).encode()
        req.send_response(200)
        req.send_header("Content-Type", "application/json")
        req.send_header("Content-Length", str(len(resp)))
        req.end_headers()
        req.wfile.write(resp)
