"""Tests for ``gc slack publish``.

The behavior under test:

  * a target session's binding is the single source of truth — no
    inbound-event scan, no fallback chain;
  * absence of a binding fails fast with a useful message rather than
    silently falling through to "send into the void";
  * the publish defaults to gc /extmsg/outbound so peer fanout and
    transcript recording fire; --via adapter is the explicit opt-out.
"""

from __future__ import annotations

import json
import pathlib
import sys
from typing import Any

import pytest

PACK_DIR = pathlib.Path(__file__).resolve().parent.parent
SCRIPTS_DIR = PACK_DIR / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))


@pytest.fixture(autouse=True)
def _isolate_env(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    monkeypatch.setenv("GC_CITY_NAME", "test-city")
    monkeypatch.setenv("GC_CITY_PATH", str(tmp_path))
    monkeypatch.setenv("GC_API_BASE_URL", "http://127.0.0.1:8372")
    monkeypatch.setenv("SLACK_WORKSPACE_ID", "T0TESTWS")
    monkeypatch.setenv("GC_SESSION_ID", "gc-default-session")
    monkeypatch.delenv("GC_SLACK_ADAPTER_ENV", raising=False)


def _import_modules():
    for name in ("slack_chat_publish", "slack_intake_common"):
        sys.modules.pop(name, None)
    import slack_intake_common  # type: ignore
    import slack_chat_publish  # type: ignore
    return slack_chat_publish, slack_intake_common


def _binding_room() -> dict[str, Any]:
    return {
        "scope_id": "test-city",
        "provider": "slack",
        "account_id": "T0TESTWS",
        "conversation_id": "C0ROOM01",
        "kind": "room",
    }


def _binding_dm() -> dict[str, Any]:
    return {
        "scope_id": "test-city",
        "provider": "slack",
        "account_id": "T0TESTWS",
        "conversation_id": "D0DM0001",
        "kind": "dm",
    }


def test_publish_routes_through_gc_outbound_by_default(
        monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    pub, common = _import_modules()
    captured: dict[str, Any] = {}

    def fake_request(method: str, url: str, body=None, *, csrf: bool = True,
                     timeout: float = 30.0):
        captured["method"] = method
        captured["url"] = url
        captured["body"] = body
        captured["csrf"] = csrf
        return {"Receipt": {"Delivered": True, "MessageID": "1700.001"}}

    monkeypatch.setattr(common, "_request", fake_request)
    monkeypatch.setattr(common, "look_up_binding", lambda _sid: _binding_room())

    rc = pub.main(["--session", "gc-82783", "--body", "*hello*"])
    assert rc == 0
    assert captured["method"] == "POST"
    assert captured["url"] == "http://127.0.0.1:8372/v0/city/test-city/extmsg/outbound"
    assert captured["csrf"] is True
    assert captured["body"]["session_id"] == "gc-82783"
    assert captured["body"]["conversation"]["conversation_id"] == "C0ROOM01"
    assert captured["body"]["conversation"]["kind"] == "room"
    assert captured["body"]["text"] == "*hello*"

    out = json.loads(capsys.readouterr().out)
    assert out["session_id"] == "gc-82783"
    assert out["conversation_id"] == "C0ROOM01"
    assert out["via"] == "gc"


def test_publish_via_adapter_keeps_direct_path(
        monkeypatch: pytest.MonkeyPatch) -> None:
    pub, common = _import_modules()
    captured: dict[str, Any] = {}

    def fake_request(method: str, url: str, body=None, *, csrf: bool = True,
                     timeout: float = 30.0):
        captured["url"] = url
        captured["csrf"] = csrf
        return {"delivered": True, "message_id": "1700.002"}

    monkeypatch.setattr(common, "_request", fake_request)
    monkeypatch.setattr(common, "look_up_binding", lambda _sid: _binding_dm())

    rc = pub.main([
        "--session", "gc-83347",
        "--body", "diag",
        "--via", "adapter",
    ])
    assert rc == 0
    assert captured["url"].endswith("/publish")
    # gc-5rz Phase A: the supervised adapter is reached via the gc /svc
    # proxy, which requires X-GC-Request on private mutation endpoints
    # — so even the adapter-direct path carries csrf=True.
    assert captured["csrf"] is True
    assert "/extmsg/" not in captured["url"]


def test_publish_fails_fast_when_session_has_no_binding(
        monkeypatch: pytest.MonkeyPatch) -> None:
    pub, common = _import_modules()

    def boom(*_a, **_kw):
        raise AssertionError("publish should not call HTTP without a binding")

    monkeypatch.setattr(common, "_request", boom)
    monkeypatch.setattr(common, "look_up_binding", lambda _sid: None)

    with pytest.raises(SystemExit) as exc:
        pub.main(["--session", "gc-no-bind", "--body", "x"])
    msg = str(exc.value)
    assert "no active extmsg binding" in msg
    assert "gc-no-bind" in msg


def test_publish_omits_session_uses_current_session_id(
        monkeypatch: pytest.MonkeyPatch) -> None:
    pub, common = _import_modules()
    seen_sid: dict[str, str] = {}

    def fake_lookup(sid: str):
        seen_sid["sid"] = sid
        return _binding_dm()

    def fake_request(method, url, body=None, *, csrf=True, timeout=30.0):
        return {"Receipt": {"Delivered": True}}

    monkeypatch.setattr(common, "look_up_binding", fake_lookup)
    monkeypatch.setattr(common, "_request", fake_request)

    rc = pub.main(["--body", "ping"])
    assert rc == 0
    assert seen_sid["sid"] == "gc-default-session"


def test_publish_propagates_idempotency_and_reply_to(
        monkeypatch: pytest.MonkeyPatch) -> None:
    pub, common = _import_modules()
    captured: dict[str, Any] = {}

    def fake_request(method, url, body=None, *, csrf=True, timeout=30.0):
        captured["body"] = body
        return {"Receipt": {"Delivered": True}}

    monkeypatch.setattr(common, "_request", fake_request)
    monkeypatch.setattr(common, "look_up_binding", lambda _sid: _binding_room())

    rc = pub.main([
        "--session", "gc-82783",
        "--body", "x",
        "--reply-to", "1700.000",
        "--idempotency-key", "cron-2026-05-02",
    ])
    assert rc == 0
    assert captured["body"]["reply_to_message_id"] == "1700.000"
    assert captured["body"]["idempotency_key"] == "cron-2026-05-02"


def test_publish_body_file_is_loaded(
        monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    pub, common = _import_modules()
    body_path = tmp_path / "msg.md"
    body_path.write_text("*from file:* hello", encoding="utf-8")
    captured: dict[str, Any] = {}

    def fake_request(method, url, body=None, *, csrf=True, timeout=30.0):
        captured["text"] = body["text"]
        return {"Receipt": {"Delivered": True}}

    monkeypatch.setattr(common, "_request", fake_request)
    monkeypatch.setattr(common, "look_up_binding", lambda _sid: _binding_room())

    rc = pub.main([
        "--session", "gc-82783",
        "--body-file", str(body_path),
    ])
    assert rc == 0
    assert captured["text"] == "*from file:* hello"


def test_publish_rejects_both_body_and_body_file() -> None:
    pub, _ = _import_modules()
    with pytest.raises(SystemExit) as exc:
        pub.main(["--session", "gc-1", "--body", "a", "--body-file", "/dev/null"])
    assert "OR" in str(exc.value)


def test_publish_requires_a_body() -> None:
    pub, _ = _import_modules()
    with pytest.raises(SystemExit) as exc:
        pub.main(["--session", "gc-1"])
    assert "--body" in str(exc.value)


def test_publish_exits_nonzero_when_adapter_receipt_delivered_false(
        monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    """HTTP-200 with delivered=false must NOT be reported as success.

    Mirrors the latent defect tracked by gpk-5sk: PL stamps
    loop_close_posted_at on the bead based on the CLI's exit code. Any
    slack rejection (auth, channel renamed, scope change) that returns
    a 200 with delivered=false must surface as a non-zero exit so the
    upstream loop doesn't mark a lost post as delivered.
    """
    pub, common = _import_modules()

    def fake_request(method, url, body=None, *, csrf=True, timeout=30.0):
        return {"delivered": False, "failure_kind": "auth"}

    monkeypatch.setattr(common, "_request", fake_request)
    monkeypatch.setattr(common, "look_up_binding", lambda _sid: _binding_room())

    rc = pub.main([
        "--session", "gc-82783",
        "--body", "rejected",
        "--via", "adapter",
    ])
    assert rc == 1
    err = capsys.readouterr().err
    # Stderr must be parseable and include the failing reason.
    assert "delivered=false" in err
    assert "failure_kind=auth" in err


def test_publish_exits_nonzero_when_gc_receipt_delivered_false(
        monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    """gc-routed publishes wrap the receipt under "Receipt"; same gate."""
    pub, common = _import_modules()

    def fake_request(method, url, body=None, *, csrf=True, timeout=30.0):
        return {"Receipt": {"Delivered": False, "FailureKind": "not_found"}}

    monkeypatch.setattr(common, "_request", fake_request)
    monkeypatch.setattr(common, "look_up_binding", lambda _sid: _binding_room())

    rc = pub.main(["--session", "gc-82783", "--body", "x"])
    assert rc == 1
    err = capsys.readouterr().err
    assert "delivered=false" in err
    assert "failure_kind=not_found" in err


def test_publish_exits_nonzero_when_receipt_schema_unknown(
        monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    """No recognizable delivered key → fail closed (don't mask a silent loss)."""
    pub, common = _import_modules()

    def fake_request(method, url, body=None, *, csrf=True, timeout=30.0):
        return {"some_unrelated_field": "value"}

    monkeypatch.setattr(common, "_request", fake_request)
    monkeypatch.setattr(common, "look_up_binding", lambda _sid: _binding_room())

    rc = pub.main(["--session", "gc-82783", "--body", "x"])
    assert rc == 1
    err = capsys.readouterr().err
    assert "delivered=false" in err
    assert "schema_mismatch" in err


def test_publish_still_exits_zero_on_delivered_true(
        monkeypatch: pytest.MonkeyPatch) -> None:
    """Regression check: success path must still return 0."""
    pub, common = _import_modules()

    def fake_request(method, url, body=None, *, csrf=True, timeout=30.0):
        return {"delivered": True, "message_id": "1700.005"}

    monkeypatch.setattr(common, "_request", fake_request)
    monkeypatch.setattr(common, "look_up_binding", lambda _sid: _binding_room())

    rc = pub.main([
        "--session", "gc-82783",
        "--body", "ok",
        "--via", "adapter",
    ])
    assert rc == 0


def test_interpret_publish_receipt_shapes() -> None:
    """Helper handles adapter-direct, gc-wrapped capitalized, and lowercase
    nested shapes, plus fail-closed semantics on unknown shapes."""
    _, common = _import_modules()
    assert common.interpret_publish_receipt({"delivered": True}) == (True, "")
    assert common.interpret_publish_receipt(
        {"delivered": False, "failure_kind": "auth"}) == (False, "auth")
    assert common.interpret_publish_receipt(
        {"Receipt": {"Delivered": True, "MessageID": "x"}}) == (True, "")
    assert common.interpret_publish_receipt(
        {"Receipt": {"Delivered": False, "FailureKind": "rate_limited"}}
    ) == (False, "rate_limited")
    assert common.interpret_publish_receipt(
        {"receipt": {"delivered": False, "failure_kind": "permanent"}}
    ) == (False, "permanent")
    # Fail-closed on unknown shapes.
    delivered, kind = common.interpret_publish_receipt({})
    assert delivered is False
    assert kind == "schema_mismatch"
    delivered, kind = common.interpret_publish_receipt("not a dict")
    assert delivered is False
    assert kind == "non_dict_response"


# --------------------------------------------------------------------------
# Accidental-mrkdwn guard (gp-o42) — tilde pairs must not strike through.
# --------------------------------------------------------------------------

def test_publish_guards_tildes_by_default(monkeypatch) -> None:
    pub, common = _import_modules()
    import slack_mrkdwn
    captured = {}

    def fake_publish(**kwargs):
        captured.update(kwargs)
        return {"Receipt": {"Delivered": True, "MessageID": "1700.001"}}

    monkeypatch.setattr(common, "publish_via_gc_outbound", fake_publish)
    monkeypatch.setattr(
        common, "look_up_binding",
        lambda _sid: {"conversation_id": "C0CHAN01", "kind": "room",
                      "scope_id": "test-city", "provider": "slack",
                      "account_id": "T0TESTWS"})

    rc = pub.main(["--session", "gc-1", "--body", "~$58.5k out, ~$16.5k left"])
    assert rc == 0
    assert captured["text"] == \
        "~$58.5k out, ~$16.5k left".replace("~", slack_mrkdwn.TILDE_SUBSTITUTE)


def test_publish_raw_flag_skips_guard(monkeypatch) -> None:
    pub, common = _import_modules()
    captured = {}

    def fake_publish(**kwargs):
        captured.update(kwargs)
        return {"Receipt": {"Delivered": True, "MessageID": "1700.001"}}

    monkeypatch.setattr(common, "publish_via_gc_outbound", fake_publish)
    monkeypatch.setattr(
        common, "look_up_binding",
        lambda _sid: {"conversation_id": "C0CHAN01", "kind": "room",
                      "scope_id": "test-city", "provider": "slack",
                      "account_id": "T0TESTWS"})

    rc = pub.main(["--session", "gc-1", "--raw",
                   "--body", "~$58.5k out, ~$16.5k left"])
    assert rc == 0
    assert captured["text"] == "~$58.5k out, ~$16.5k left"
