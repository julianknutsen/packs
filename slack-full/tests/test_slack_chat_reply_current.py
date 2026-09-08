"""Tests for slack reply-current's gc-vs-adapter publish path.

The behavior under test: by default, replies should route through gc's
``/extmsg/outbound`` so peer fanout + transcript recording fire. Only the
explicit ``--via adapter`` opt-in skips gc and hits the local adapter.
"""

from __future__ import annotations

import argparse
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
    monkeypatch.setenv("GC_SESSION_ID", "gc-test-session")
    monkeypatch.delenv("GC_SLACK_ADAPTER_ENV", raising=False)


def _import_modules():
    for name in ("slack_chat_reply_current", "slack_intake_common"):
        sys.modules.pop(name, None)
    import slack_intake_common  # type: ignore
    import slack_chat_reply_current  # type: ignore
    return slack_chat_reply_current, slack_intake_common


def test_default_via_routes_through_gc_outbound(monkeypatch: pytest.MonkeyPatch) -> None:
    rc, common = _import_modules()
    captured: dict[str, Any] = {}

    def fake_request(method: str, url: str, body: dict[str, Any] | None = None,
                     *, csrf: bool = True, timeout: float = 30.0) -> dict[str, Any]:
        captured["method"] = method
        captured["url"] = url
        captured["body"] = body
        captured["csrf"] = csrf
        return {"Receipt": {"Delivered": True, "MessageID": "1700000.000100"}}

    monkeypatch.setattr(common, "_request", fake_request)
    monkeypatch.setattr(common, "find_latest_inbound_for_session", lambda _sid: None)
    monkeypatch.setattr(common, "look_up_binding", lambda _sid: None)

    exit_code = rc.main([
        "--session", "gc-test-session",
        "--conversation-id", "D0123ROOM",
        "--body", "*hello*",
    ])
    assert exit_code == 0
    assert captured["method"] == "POST"
    assert captured["url"] == "http://127.0.0.1:8372/v0/city/test-city/extmsg/outbound"
    assert captured["csrf"] is True
    assert captured["body"]["session_id"] == "gc-test-session"
    assert captured["body"]["conversation"] == {
        "scope_id": "test-city",
        "provider": "slack",
        "account_id": "T0TESTWS",
        "conversation_id": "D0123ROOM",
        "kind": "dm",
    }
    assert captured["body"]["text"] == "*hello*"


def test_via_adapter_keeps_direct_adapter_path(monkeypatch: pytest.MonkeyPatch) -> None:
    rc, common = _import_modules()
    captured: dict[str, Any] = {}

    def fake_request(method: str, url: str, body: dict[str, Any] | None = None,
                     *, csrf: bool = True, timeout: float = 30.0) -> dict[str, Any]:
        captured["method"] = method
        captured["url"] = url
        captured["body"] = body
        captured["csrf"] = csrf
        return {"delivered": True, "message_id": "1700000.000200"}

    monkeypatch.setattr(common, "_request", fake_request)
    monkeypatch.setattr(common, "find_latest_inbound_for_session", lambda _sid: None)
    monkeypatch.setattr(common, "look_up_binding", lambda _sid: None)

    exit_code = rc.main([
        "--session", "gc-test-session",
        "--conversation-id", "D0123ROOM",
        "--body", "diag",
        "--via", "adapter",
    ])
    assert exit_code == 0
    assert captured["url"].endswith("/publish")
    # gc-5rz Phase A: the supervised adapter is reached via the gc /svc
    # proxy, which requires X-GC-Request on private mutation endpoints
    # — so even the adapter-direct path carries csrf=True.
    assert captured["csrf"] is True
    assert "/extmsg/" not in captured["url"]


def test_idempotency_and_reply_to_propagate(monkeypatch: pytest.MonkeyPatch) -> None:
    rc, common = _import_modules()
    captured: dict[str, Any] = {}

    def fake_request(method: str, url: str, body: dict[str, Any] | None = None,
                     *, csrf: bool = True, timeout: float = 30.0) -> dict[str, Any]:
        captured["body"] = body
        return {"Receipt": {"Delivered": True}}

    monkeypatch.setattr(common, "_request", fake_request)
    monkeypatch.setattr(common, "find_latest_inbound_for_session", lambda _sid: None)
    monkeypatch.setattr(common, "look_up_binding", lambda _sid: None)

    rc.main([
        "--session", "gc-test-session",
        "--conversation-id", "D0123ROOM",
        "--body", "x",
        "--reply-to", "1700000.000100",
        "--idempotency-key", "key-42",
    ])
    assert captured["body"]["reply_to_message_id"] == "1700000.000100"
    assert captured["body"]["idempotency_key"] == "key-42"


def test_auto_derives_stable_idempotency_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """gpk-lbhl: with no --idempotency-key, the key is derived and stable.

    Two identical invocations (the shape of a retry after a delivered-but-
    timed-out POST) must send the SAME idempotency_key so the adapter
    dedupes the second post instead of duplicating the reply.
    """
    rc, common = _import_modules()
    keys: list[str] = []

    def fake_request(method: str, url: str, body: dict[str, Any] | None = None,
                     *, csrf: bool = True, timeout: float = 30.0) -> dict[str, Any]:
        keys.append((body or {}).get("idempotency_key", ""))
        return {"Receipt": {"Delivered": True}}

    monkeypatch.setattr(common, "_request", fake_request)
    monkeypatch.setattr(common, "find_latest_inbound_for_session", lambda _sid: None)
    monkeypatch.setattr(common, "look_up_binding", lambda _sid: None)

    argv = [
        "--session", "gc-test-session",
        "--conversation-id", "D0123ROOM",
        "--body", "x",
        "--reply-to", "1700000.000100",
    ]
    assert rc.main(argv) == 0
    assert rc.main(argv) == 0
    assert keys[0] != ""
    assert keys[0] == keys[1]
    assert keys[0].startswith("reply-current:")


def test_derived_key_varies_with_body(monkeypatch: pytest.MonkeyPatch) -> None:
    """A different body yields a different fingerprint (no cross-collapse)."""
    rc, common = _import_modules()
    keys: list[str] = []

    def fake_request(method: str, url: str, body: dict[str, Any] | None = None,
                     *, csrf: bool = True, timeout: float = 30.0) -> dict[str, Any]:
        keys.append((body or {}).get("idempotency_key", ""))
        return {"Receipt": {"Delivered": True}}

    monkeypatch.setattr(common, "_request", fake_request)
    monkeypatch.setattr(common, "find_latest_inbound_for_session", lambda _sid: None)
    monkeypatch.setattr(common, "look_up_binding", lambda _sid: None)

    base = ["--session", "gc-test-session", "--conversation-id", "D0123ROOM",
            "--reply-to", "1700000.000100"]
    assert rc.main(base + ["--body", "first"]) == 0
    assert rc.main(base + ["--body", "second"]) == 0
    assert keys[0] != keys[1]


def test_reply_current_exits_nonzero_on_adapter_delivered_false(
        monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    """Mirror gpk-5sk's gate for the reply-current CLI on the adapter route.

    Added in response to Copilot review on PR #14 — the prior commit landed
    the delivered-false gate without a regression test for this CLI.
    """
    rc, common = _import_modules()

    def fake_request(method: str, url: str, body: dict[str, Any] | None = None,
                     *, csrf: bool = True, timeout: float = 30.0) -> dict[str, Any]:
        return {"delivered": False, "failure_kind": "rate_limited"}

    monkeypatch.setattr(common, "_request", fake_request)
    monkeypatch.setattr(common, "find_latest_inbound_for_session", lambda _sid: None)
    monkeypatch.setattr(common, "look_up_binding", lambda _sid: None)

    exit_code = rc.main([
        "--session", "gc-test-session",
        "--conversation-id", "D0123ROOM",
        "--body", "rejected",
        "--via", "adapter",
    ])
    assert exit_code == 1
    err = capsys.readouterr().err
    assert "delivered=false" in err
    assert "failure_kind=rate_limited" in err


def test_reply_current_exits_nonzero_on_gc_outbound_delivered_false(
        monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    """Same gate via the default gc /extmsg/outbound route (capitalized shape)."""
    rc, common = _import_modules()

    def fake_request(method: str, url: str, body: dict[str, Any] | None = None,
                     *, csrf: bool = True, timeout: float = 30.0) -> dict[str, Any]:
        return {"Receipt": {"Delivered": False, "FailureKind": "not_found"}}

    monkeypatch.setattr(common, "_request", fake_request)
    monkeypatch.setattr(common, "find_latest_inbound_for_session", lambda _sid: None)
    monkeypatch.setattr(common, "look_up_binding", lambda _sid: None)

    exit_code = rc.main([
        "--session", "gc-test-session",
        "--conversation-id", "D0123ROOM",
        "--body", "x",
    ])
    assert exit_code == 1
    err = capsys.readouterr().err
    assert "delivered=false" in err
    assert "failure_kind=not_found" in err


# --------------------------------------------------------------------------
# Company-context awareness (company rooms 2b) — additive to the legacy path.
# --------------------------------------------------------------------------

import os as _os  # noqa: E402

_DIRECTORY = {
    "schema_version": 1,
    "agents": [
        {"name": "ollie", "app_id": "A0AAAAAA1", "bot_user_id": "U0AAAAAA1"},
        {"name": "riley", "app_id": "A0AAAAAA2", "bot_user_id": "U0AAAAAA2"},
    ],
    "rooms": [{
        "name": "orchestrator-team", "team_id": "T0AAAAAAA", "channel_id": "C0AAAAAAA",
        "members": ["ollie", "riley"], "ambient_wake": ["ollie"],
        "mention_wake": ["ollie", "riley"],
    }],
}
_BINDINGS = {
    "schema_version": 1,
    "bindings": [
        {"room": "orchestrator-team", "agent": "ollie", "session": "ollie-main"},
        {"room": "orchestrator-team", "agent": "riley", "session": "riley-main"},
    ],
}


def _import_outbound():
    sys.modules.pop("slack_company_outbound", None)
    sys.modules.pop("slack_company_directory", None)
    import slack_company_outbound  # type: ignore
    return slack_company_outbound


def _setup_company(outbound, tmp_path: pathlib.Path) -> None:
    slackdir = tmp_path / ".gc" / "slack"
    slackdir.mkdir(parents=True, exist_ok=True)
    (slackdir / "company_directory.json").write_text(json.dumps(_DIRECTORY))
    (slackdir / "company_bindings.json").write_text(json.dumps(_BINDINGS))
    for agent in ("ollie", "riley"):
        sdir = outbound.secrets_dir()
        sdir.mkdir(parents=True, exist_ok=True)
        _os.chmod(sdir, 0o700)
        p = sdir / f"bot-token-{agent}.txt"
        p.write_text(f"xoxb-{agent}")
        _os.chmod(p, 0o600)


def _write_delegation(outbound, *, ts: str, nonce: str) -> str:
    record = {
        "schema_version": 1, "generation": 1, "nonce": nonce,
        "room": "orchestrator-team", "team_id": "T0AAAAAAA", "channel_id": "C0AAAAAAA",
        "ts": ts, "thread_root_ts": "1700000000.000100",
        "requester_agent": "ollie", "requester_bot_user_id": "U0AAAAAA1",
        "requester_session": "ollie-main",
        "expected_responder_agent": "riley", "expected_responder_bot_user_id": "U0AAAAAA2",
        "created_at": outbound._rfc3339(outbound._now()), "ttl_seconds": 86400,
        "status": "pending", "result_ts": "", "result_claimed_at": "",
    }
    key = outbound.delegation_filename("T0AAAAAAA", "C0AAAAAAA", ts)
    ddir = outbound.delegations_dir()
    ddir.mkdir(parents=True, exist_ok=True)
    (ddir / key).write_text(json.dumps(record))
    return key


def _write_turn(outbound, *, session: str, kind: str, agent: str,
                ts: str, delegation_key: str = "", **overrides) -> dict:
    tdir = outbound.turns_dir()
    tdir.mkdir(parents=True, exist_ok=True)
    turn = {
        "schema_version": 1, "session": session, "receipt_id": "in-x",
        "team_id": "T0AAAAAAA", "channel_id": "C0AAAAAAA", "ts": ts,
        "room": "orchestrator-team", "kind": kind,
        "thread_root_ts": "1700000000.000100", "agent": agent,
        "delegation_key": delegation_key, "delivered_at": "2026-07-17T12:00:00Z",
    }
    turn.update(overrides)
    (tdir / f"{session}.json").write_text(json.dumps(turn))
    return turn


def test_company_peer_delegation_posts_result(
        monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    rc, _common = _import_modules()
    outbound = _import_outbound()
    _setup_company(outbound, tmp_path)
    key = _write_delegation(outbound, ts="1700000000.000500", nonce="gcs-result00000000000")
    _write_turn(outbound, session="riley-main", kind="peer_delegation", agent="riley",
                ts="1700000000.000500", delegation_key=key)
    monkeypatch.setenv("GC_SESSION_NAME", "riley-main")

    captured: list = []

    def fake_post(method, token, payload, *, api_base, timeout):
        captured.append({"token": token, "payload": payload})
        return 200, {}, {"ok": True, "ts": "1700000000.000700"}
    monkeypatch.setattr(outbound, "_slack_web_post", fake_post)

    rc_code = rc.main(["--body", "the answer is 42"])
    assert rc_code == 0
    assert len(captured) == 1
    p = captured[0]["payload"]
    # Acting agent's own token, requester the only live mention, into the root.
    assert captured[0]["token"] == "xoxb-riley"
    assert p["text"].startswith("<@U0AAAAAA1> ")
    assert p["thread_ts"] == "1700000000.000100"
    assert p["metadata"]["event_type"] == "gc_delegation_result"
    assert p["metadata"]["event_payload"]["nonce"] == "gcs-result00000000000"
    assert p["metadata"]["event_payload"]["delegation_ts"] == "1700000000.000500"


def test_company_peer_result_posts_synthesis_no_mentions(
        monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    rc, _common = _import_modules()
    outbound = _import_outbound()
    _setup_company(outbound, tmp_path)
    key = _write_delegation(outbound, ts="1700000000.000500", nonce="gcs-synth000000000000")
    _write_turn(outbound, session="ollie-main", kind="peer_result", agent="ollie",
                ts="1700000000.000700", delegation_key=key)
    monkeypatch.setenv("GC_SESSION_NAME", "ollie-main")

    captured: list = []

    def fake_post(method, token, payload, *, api_base, timeout):
        captured.append({"token": token, "payload": payload})
        return 200, {}, {"ok": True, "ts": "1700000000.000900"}
    monkeypatch.setattr(outbound, "_slack_web_post", fake_post)

    rc_code = rc.main(["--body", "riley says <b>42</b> & @here"])
    assert rc_code == 0
    p = captured[0]["payload"]
    assert captured[0]["token"] == "xoxb-ollie"
    assert p["thread_ts"] == "1700000000.000100"
    assert "<@" not in p["text"]  # no live agent mentions
    assert "&amp;" in p["text"] and "&lt;b&gt;" in p["text"]
    # Synthesis is now durable-intent-backed: it carries a reconcile-only
    # metadata nonce (inert to the router — no live mention wakes anyone).
    assert p["metadata"]["event_type"] == "gc_delegation_synthesis"
    assert p["metadata"]["event_payload"]["nonce"].startswith("gcs-")


def test_company_origin_ts_mismatch_is_hard_error(
        monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    rc, _common = _import_modules()
    outbound = _import_outbound()
    _setup_company(outbound, tmp_path)
    key = _write_delegation(outbound, ts="1700000000.000500", nonce="gcs-result11111111111")
    _write_turn(outbound, session="riley-main", kind="peer_delegation", agent="riley",
                ts="1700000000.000500", delegation_key=key)
    monkeypatch.setenv("GC_SESSION_NAME", "riley-main")
    monkeypatch.setattr(outbound, "_slack_web_post",
                        lambda *a, **k: (200, {}, {"ok": True, "ts": "x"}))

    with pytest.raises(SystemExit) as exc:
        rc.main(["--body", "x", "--origin-ts", "1700000000.999999"])
    assert "origin-ts" in str(exc.value)


def test_no_company_pointer_falls_through_to_legacy(
        monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    """GC_SESSION_NAME set but no pointer → legacy gc /extmsg/outbound path."""
    rc, common = _import_modules()
    monkeypatch.setenv("GC_SESSION_NAME", "riley-main")
    captured: dict = {}

    def fake_request(method, url, body=None, *, csrf=True, timeout=30.0):
        captured["url"] = url
        return {"Receipt": {"Delivered": True}}
    monkeypatch.setattr(common, "_request", fake_request)
    monkeypatch.setattr(common, "find_latest_inbound_for_session", lambda _sid: None)
    monkeypatch.setattr(common, "look_up_binding", lambda _sid: None)

    rc_code = rc.main(["--session", "gc-test-session",
                       "--conversation-id", "D0123ROOM", "--body", "legacy"])
    assert rc_code == 0
    assert captured["url"].endswith("/extmsg/outbound")


def test_company_peer_input_posts_root_reply_no_mentions(
        monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    """P-A: a keyless peer_input wake replies into the thread root with the
    acting token and no live mentions — no delegation record involved."""
    rc, _common = _import_modules()
    outbound = _import_outbound()
    _setup_company(outbound, tmp_path)
    # peer_input carries NO delegation_key (the keyless pointer must parse).
    _write_turn(outbound, session="riley-main", kind="peer_input", agent="riley",
                ts="1700000000.000500")
    monkeypatch.setenv("GC_SESSION_NAME", "riley-main")

    captured: list = []

    def fake_post(method, token, payload, *, api_base, timeout):
        captured.append({"token": token, "payload": payload})
        return 200, {}, {"ok": True, "ts": "1700000000.000800"}
    monkeypatch.setattr(outbound, "_slack_web_post", fake_post)

    rc_code = rc.main(["--body", "on it <b> & @here"])
    assert rc_code == 0
    assert len(captured) == 1
    p = captured[0]["payload"]
    assert captured[0]["token"] == "xoxb-riley"
    assert p["thread_ts"] == "1700000000.000100"
    assert "<@" not in p["text"]  # no live agent mentions
    assert "&lt;b&gt;" in p["text"] and "&amp;" in p["text"]
    assert "metadata" not in p  # ordinary reply carries no gc metadata


def test_keyless_peer_input_pointer_parses(
        monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    """P-A: a peer_input pointer written without delegation_key parses (the Go
    keyless-pointer schema round-trips) rather than raising OutboundError."""
    outbound = _import_outbound()
    _setup_company(outbound, tmp_path)
    tdir = outbound.turns_dir()
    tdir.mkdir(parents=True, exist_ok=True)
    turn = {
        "schema_version": 1, "session": "riley-main", "receipt_id": "in-x",
        "team_id": "T0AAAAAAA", "channel_id": "C0AAAAAAA", "ts": "1700000000.000500",
        "room": "orchestrator-team", "kind": "peer_input",
        "thread_root_ts": "1700000000.000100", "agent": "riley",
        "delivered_at": "2026-07-17T12:00:00Z",
    }  # NOTE: no "delegation_key" key at all.
    (tdir / "riley-main.json").write_text(json.dumps(turn))
    parsed = outbound.read_current_turn("riley-main")
    assert parsed is not None and parsed["kind"] == "peer_input"


def _install_claimed_fixture(outbound, fixture_name: str) -> str:
    """Copy a golden claimed record into the delegations dir under its own key.

    The pruner evicts terminal records keyed on ``result_claimed_at`` (falling
    back to ``created_at``); the golden fixtures freeze both to their authoring
    date, which ages past the retention floor. Re-stamp both to now so the
    record survives the prune inside post_peer_synthesis and reaches the gate.
    """
    fixtures = pathlib.Path(__file__).resolve().parent / "fixtures" / "company"
    data = json.loads((fixtures / fixture_name).read_text())
    _fresh = outbound._rfc3339(outbound._now())
    data["created_at"] = _fresh
    if data.get("result_claimed_at"):
        data["result_claimed_at"] = _fresh
    key = outbound.delegation_filename(data["team_id"], data["channel_id"], data["ts"])
    ddir = outbound.delegations_dir()
    ddir.mkdir(parents=True, exist_ok=True)
    (ddir / key).write_text(json.dumps(data))
    return key


def test_company_synthesis_gate_refuses_then_allow_partial_passes(
        monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
        capsys: pytest.CaptureFixture) -> None:
    """D2: reply-current refuses a not-ready synthesis and forwards
    --allow-partial through to post_peer_synthesis (which records the flag)."""
    rc, _common = _import_modules()
    outbound = _import_outbound()
    _setup_company(outbound, tmp_path)
    monkeypatch.setattr(outbound, "_sleep", lambda *_a, **_k: None)
    key = _install_claimed_fixture(outbound, "claimed_delegation_not_ready.json")
    _write_turn(outbound, session="ollie-main", kind="peer_result", agent="ollie",
                ts="1700000000.000700", delegation_key=key)
    monkeypatch.setenv("GC_SESSION_NAME", "ollie-main")

    captured: list = []

    def fake_post(method, token, payload, *, api_base, timeout):
        captured.append(payload)
        return 200, {}, {"ok": True, "ts": "1700000000.000900"}
    monkeypatch.setattr(outbound, "_slack_web_post", fake_post)

    # Without --allow-partial the not-ready snapshot hard-errors (exit 1).
    with pytest.raises(SystemExit) as exc:
        rc.main(["--body", "too early"])
    assert "not ready" in str(exc.value)
    assert captured == []
    capsys.readouterr()  # drain

    # With --allow-partial it posts and the report carries allow_partial.
    assert rc.main(["--body", "partial", "--allow-partial"]) == 0
    assert len(captured) == 1
    printed = json.loads(capsys.readouterr().out)
    assert printed["allow_partial"] is True


@pytest.mark.parametrize("kind", ["ambient", "thread_ambient", "targeted"])
def test_company_ambient_targeted_posts_root_reply(
        monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path, kind: str) -> None:
    """Human-authored room turns answer into the room thread root with the
    acting token, instead of falling through to legacy resolution."""
    rc, common = _import_modules()
    outbound = _import_outbound()
    _setup_company(outbound, tmp_path)
    _write_turn(outbound, session="ollie-main", kind=kind, agent="ollie",
                ts="1700000000.000500")
    monkeypatch.setenv("GC_SESSION_NAME", "ollie-main")

    captured: list = []

    def fake_post(method, token, payload, *, api_base, timeout):
        captured.append({"token": token, "payload": payload})
        return 200, {}, {"ok": True, "ts": "1700000000.000800"}
    monkeypatch.setattr(outbound, "_slack_web_post", fake_post)

    # Legacy path must NOT be reached (it would call common._request).
    def boom_request(*_a, **_k):
        raise AssertionError("legacy resolution must not run for a company turn")
    monkeypatch.setattr(common, "_request", boom_request)

    rc_code = rc.main(["--body", "answering the room"])
    assert rc_code == 0
    assert len(captured) == 1
    p = captured[0]["payload"]
    assert captured[0]["token"] == "xoxb-ollie"
    assert p["thread_ts"] == "1700000000.000100"
    assert "<@" not in p["text"]
    assert "metadata" not in p


def test_company_turn_ref_keeps_concurrent_room_reply_in_origin_thread(
        monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    """A later #it wake must not redirect a reply to an earlier alerts turn.

    Both rooms deliberately bind the same agent session, reproducing the
    production failure mode where the mutable room pointer is overwritten
    while the first turn is still running.  The immutable turn reference from
    the first reminder must continue to select its exact channel and thread.
    """
    rc, _common = _import_modules()
    outbound = _import_outbound()
    _setup_company(outbound, tmp_path)

    slackdir = tmp_path / ".gc" / "slack"
    directory = json.loads(json.dumps(_DIRECTORY))
    directory["rooms"] = [
        {
            "name": "pd-alerts-internal", "team_id": "T0AAAAAAA",
            "channel_id": "C0ALERTS00", "members": ["riley"],
            "ambient_wake": ["riley"], "mention_wake": ["riley"],
        },
        {
            "name": "it", "team_id": "T0AAAAAAA",
            "channel_id": "C0ITROOM000", "members": ["riley"],
            "ambient_wake": ["riley"], "mention_wake": ["riley"],
        },
    ]
    bindings = {
        "schema_version": 1,
        "bindings": [
            {"room": "pd-alerts-internal", "agent": "riley", "session": "riley-main"},
            {"room": "it", "agent": "riley", "session": "riley-main"},
        ],
    }
    (slackdir / "company_directory.json").write_text(json.dumps(directory))
    (slackdir / "company_bindings.json").write_text(json.dumps(bindings))

    alerts_ref = "gct-aaaaaaaaaaaaaaaaaaaa"
    alerts_turn = _write_turn(
        outbound, session="riley-main", kind="targeted", agent="riley",
        ts="1700000000.000500")
    alerts_turn.update({
        "turn_ref": alerts_ref,
        "receipt_id": "in-alerts",
        "channel_id": "C0ALERTS00",
        "room": "pd-alerts-internal",
        "thread_root_ts": "1700000000.000100",
    })
    ref_dir = outbound.turns_dir() / "by-ref"
    ref_dir.mkdir(parents=True, exist_ok=True)
    (ref_dir / f"{alerts_ref}.json").write_text(json.dumps(alerts_turn))

    # A newer #it delivery overwrites the legacy mutable pointer before Riley
    # finishes composing the alerts response.
    _write_turn(
        outbound, session="riley-main", kind="targeted", agent="riley",
        ts="1700000001.000500", turn_ref="gct-bbbbbbbbbbbbbbbbbbbb",
        receipt_id="in-it", channel_id="C0ITROOM000", room="it",
        thread_root_ts="1700000001.000100",
        delivered_at="2026-07-17T12:00:01Z")
    monkeypatch.setenv("GC_SESSION_NAME", "riley-main")

    captured: list = []

    def fake_post(method, token, payload, *, api_base, timeout):
        captured.append({"token": token, "payload": payload})
        return 200, {}, {"ok": True, "ts": "1700000002.000800"}
    monkeypatch.setattr(outbound, "_slack_web_post", fake_post)

    assert rc.main([
        "--turn-ref", alerts_ref, "--body", "the alert is resolved",
    ]) == 0
    assert len(captured) == 1
    assert captured[0]["token"] == "xoxb-riley"
    assert captured[0]["payload"]["channel"] == "C0ALERTS00"
    assert captured[0]["payload"]["thread_ts"] == "1700000000.000100"


def test_company_post_rollout_pointer_without_turn_ref_fails_closed(
        monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    rc, _common = _import_modules()
    outbound = _import_outbound()
    _setup_company(outbound, tmp_path)
    turn_ref = "gct-cccccccccccccccccccc"
    _write_turn(
        outbound, session="riley-main", kind="targeted", agent="riley",
        ts="1700000000.000500", turn_ref=turn_ref)
    monkeypatch.setenv("GC_SESSION_NAME", "riley-main")
    captured: list = []
    monkeypatch.setattr(
        outbound, "_slack_web_post",
        lambda *args, **kwargs: captured.append((args, kwargs)))

    with pytest.raises(SystemExit) as exc:
        rc.main(["--body", "must not guess"])
    assert "--turn-ref" in str(exc.value)
    assert captured == []


def test_company_turn_ref_rejects_cross_session_and_tampered_route(
        monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    rc, _common = _import_modules()
    outbound = _import_outbound()
    _setup_company(outbound, tmp_path)
    turn_ref = "gct-dddddddddddddddddddd"
    turn = _write_turn(
        outbound, session="riley-main", kind="targeted", agent="riley",
        ts="1700000000.000500", turn_ref=turn_ref)
    ref_dir = outbound.turns_dir() / "by-ref"
    ref_dir.mkdir(parents=True, exist_ok=True)
    (ref_dir / f"{turn_ref}.json").write_text(json.dumps(turn))
    captured: list = []
    monkeypatch.setattr(
        outbound, "_slack_web_post",
        lambda *args, **kwargs: captured.append((args, kwargs)))

    monkeypatch.setenv("GC_SESSION_NAME", "ollie-main")
    with pytest.raises(SystemExit) as exc:
        rc.main(["--turn-ref", turn_ref, "--body", "cross-session"])
    assert "spoof guard" in str(exc.value)

    monkeypatch.setenv("GC_SESSION_NAME", "riley-main")
    turn["channel_id"] = "C0TAMPERED0"
    (ref_dir / f"{turn_ref}.json").write_text(json.dumps(turn))
    with pytest.raises(SystemExit) as exc:
        rc.main(["--turn-ref", turn_ref, "--body", "tampered"])
    assert "directory route" in str(exc.value)
    assert captured == []


def test_company_turn_ref_requires_bound_session_env(
        monkeypatch: pytest.MonkeyPatch) -> None:
    rc, _common = _import_modules()
    monkeypatch.delenv("GC_SESSION_NAME", raising=False)
    with pytest.raises(SystemExit) as exc:
        rc.main([
            "--turn-ref", "gct-eeeeeeeeeeeeeeeeeeee", "--body", "x",
        ])
    assert "GC_SESSION_NAME" in str(exc.value)


# --------------------------------------------------------------------------
# Per-agent DM reply path (Phase 4) — reply-current diverts to the DM pointer.
# --------------------------------------------------------------------------

def _write_dm_bindings(outbound, *, session: str = "ollie-main", agent: str = "ollie") -> None:
    slackdir = pathlib.Path(_os.environ["GC_CITY_PATH"]) / ".gc" / "slack"
    slackdir.mkdir(parents=True, exist_ok=True)
    (slackdir / "dm_bindings.json").write_text(json.dumps({
        "schema_version": 1, "dm_bindings": [{"agent": agent, "session": session}]}))


def _write_dm_turn(outbound, *, session: str, agent: str = "ollie",
                   ts: str = "1700000000.000900",
                   delivered_at: str = "2026-07-18T12:00:05Z") -> None:
    tdir = outbound.turns_dir()
    tdir.mkdir(parents=True, exist_ok=True)
    turn = {
        "schema_version": 1, "session": session, "receipt_id": "in-dm",
        "team_id": "T0AAAAAAA", "channel_id": "D0HUMANOLLIE", "ts": ts,
        "room": "", "kind": "dm", "thread_root_ts": ts, "agent": agent,
        "owner_app_id": "A0AAAAAA1", "delivered_at": delivered_at,
    }
    dm_dir = tdir / "dm"
    dm_dir.mkdir(parents=True, exist_ok=True)
    (dm_dir / f"{session}.json").write_text(json.dumps(turn))


def test_company_dm_pointer_posts_dm_reply(
        monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    rc, _common = _import_modules()
    outbound = _import_outbound()
    _setup_company(outbound, tmp_path)
    _write_dm_bindings(outbound, session="ollie-main")
    _write_dm_turn(outbound, session="ollie-main")
    monkeypatch.setenv("GC_SESSION_NAME", "ollie-main")

    captured: list = []

    def fake_post(method, token, payload, *, api_base, timeout):
        captured.append({"token": token, "payload": payload})
        return 200, {}, {"ok": True, "ts": "1700000000.001000"}
    monkeypatch.setattr(outbound, "_slack_web_post", fake_post)

    rc_code = rc.main(["--body", "hello human"])
    assert rc_code == 0
    assert len(captured) == 1
    p = captured[0]["payload"]
    assert captured[0]["token"] == "xoxb-ollie"  # owner agent token
    assert p["channel"] == "D0HUMANOLLIE"
    assert p["text"] == "hello human"
    assert "<@" not in p["text"]
    assert p["metadata"]["event_type"] == "gc_dm_reply"


def test_company_kind_override_selects_room_over_newer_dm(
        monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    rc, _common = _import_modules()
    outbound = _import_outbound()
    _setup_company(outbound, tmp_path)
    _write_dm_bindings(outbound, session="ollie-main")
    # Room pointer (older) + DM pointer (newer). Newest would pick DM.
    _write_turn(outbound, session="ollie-main", kind="targeted", agent="ollie",
                ts="1700000000.000500")
    _write_dm_turn(outbound, session="ollie-main", delivered_at="2026-07-18T13:00:00Z")
    monkeypatch.setenv("GC_SESSION_NAME", "ollie-main")

    captured: list = []

    def fake_post(method, token, payload, *, api_base, timeout):
        captured.append({"token": token, "payload": payload})
        return 200, {}, {"ok": True, "ts": "1700000000.001000"}
    monkeypatch.setattr(outbound, "_slack_web_post", fake_post)

    # --kind room forces the room pointer despite the newer DM.
    assert rc.main(["--body", "to the room", "--kind", "room"]) == 0
    p = captured[0]["payload"]
    assert p["channel"] == "C0AAAAAAA"  # room channel, not the DM channel
    assert "metadata" not in p  # ambient/targeted room reply carries no metadata


def test_company_newest_dm_wins_without_override(
        monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    rc, _common = _import_modules()
    outbound = _import_outbound()
    _setup_company(outbound, tmp_path)
    _write_dm_bindings(outbound, session="ollie-main")
    _write_turn(outbound, session="ollie-main", kind="targeted", agent="ollie",
                ts="1700000000.000500")  # delivered 2026-07-17T12:00:00Z
    _write_dm_turn(outbound, session="ollie-main", delivered_at="2026-07-18T13:00:00Z")
    monkeypatch.setenv("GC_SESSION_NAME", "ollie-main")

    captured: list = []

    def fake_post(method, token, payload, *, api_base, timeout):
        captured.append({"token": token, "payload": payload})
        return 200, {}, {"ok": True, "ts": "1700000000.001000"}
    monkeypatch.setattr(outbound, "_slack_web_post", fake_post)

    assert rc.main(["--body", "auto"]) == 0
    assert captured[0]["payload"]["channel"] == "D0HUMANOLLIE"  # DM wins (newer)


def test_kind_room_does_not_hijack_non_company_session(
        monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    """A non-company session passing --kind room must reach the legacy path,
    not error on a missing company room pointer (regression guard)."""
    rc, common = _import_modules()
    monkeypatch.setenv("GC_SESSION_NAME", "not-a-company-session")
    monkeypatch.setenv("SLACK_WORKSPACE_ID", "T0TESTWS")

    seen = {}

    def fake_publish(**kwargs):
        seen.update(kwargs)
        return {"delivered": True}
    monkeypatch.setattr(common, "publish_via_gc_outbound", fake_publish)
    monkeypatch.setattr(common, "current_session_id", lambda: "sess-id")

    rc_code = rc.main(["--conversation-id", "C0LEGACY", "--kind", "room", "--body", "hi"])
    assert rc_code == 0
    assert seen["kind"] == "room"  # honored as the legacy conversation kind


# --------------------------------------------------------------------------
# Accidental-mrkdwn guard (gp-o42) — tilde pairs must not strike through.
# --------------------------------------------------------------------------

_RUNWAY_LINE = "• Total out: ~$58.5k → *~$16.5k left on Sep 30* from a $75k start."


def test_body_tildes_are_guarded_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    rc, common = _import_modules()
    import slack_mrkdwn
    captured: dict[str, Any] = {}

    def fake_request(method, url, body=None, *, csrf=True, timeout=30.0):
        captured["body"] = body
        return {"Receipt": {"Delivered": True, "MessageID": "1700000.000100"}}

    monkeypatch.setattr(common, "_request", fake_request)
    monkeypatch.setattr(common, "find_latest_inbound_for_session", lambda _sid: None)
    monkeypatch.setattr(common, "find_latest_inbound_thread_for_session",
                        lambda _sid: None, raising=False)
    monkeypatch.setattr(common, "look_up_binding", lambda _sid: None)

    exit_code = rc.main([
        "--session", "gc-test-session",
        "--conversation-id", "C0BEZ3CQK5X",
        "--body", _RUNWAY_LINE,
    ])
    assert exit_code == 0
    text = captured["body"]["text"]
    assert "~" not in text  # no pairable ASCII tildes reach Slack
    assert text == _RUNWAY_LINE.replace("~", slack_mrkdwn.TILDE_SUBSTITUTE)


def test_raw_flag_skips_the_mrkdwn_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    rc, common = _import_modules()
    captured: dict[str, Any] = {}

    def fake_request(method, url, body=None, *, csrf=True, timeout=30.0):
        captured["body"] = body
        return {"Receipt": {"Delivered": True, "MessageID": "1700000.000100"}}

    monkeypatch.setattr(common, "_request", fake_request)
    monkeypatch.setattr(common, "find_latest_inbound_for_session", lambda _sid: None)
    monkeypatch.setattr(common, "find_latest_inbound_thread_for_session",
                        lambda _sid: None, raising=False)
    monkeypatch.setattr(common, "look_up_binding", lambda _sid: None)

    exit_code = rc.main([
        "--session", "gc-test-session",
        "--conversation-id", "C0BEZ3CQK5X",
        "--body", _RUNWAY_LINE,
        "--raw",
    ])
    assert exit_code == 0
    assert captured["body"]["text"] == _RUNWAY_LINE


def test_company_path_guards_tildes_too(
        monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    # Acceptance case: the mayor's reply-current diverts into the company
    # path; the exact runway body must land with zero pairable tildes and
    # intentional *bold* intact — no caller changes.
    rc, _common = _import_modules()
    import slack_mrkdwn
    outbound = _import_outbound()
    _setup_company(outbound, tmp_path)
    _write_turn(outbound, session="ollie-main", kind="ambient", agent="ollie",
                ts="1700000000.000300")
    monkeypatch.setenv("GC_SESSION_NAME", "ollie-main")

    captured: list = []

    def fake_post(method, token, payload, *, api_base, timeout):
        captured.append({"token": token, "payload": payload})
        return 200, {}, {"ok": True, "ts": "1700000000.000900"}
    monkeypatch.setattr(outbound, "_slack_web_post", fake_post)

    rc_code = rc.main(["--body", _RUNWAY_LINE])
    assert rc_code == 0
    text = captured[0]["payload"]["text"]
    assert "~" not in text
    assert "*" in text  # bold delimiters untouched
    assert slack_mrkdwn.TILDE_SUBSTITUTE in text


def test_company_path_raw_flag_passes_tildes(
        monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    rc, _common = _import_modules()
    outbound = _import_outbound()
    _setup_company(outbound, tmp_path)
    _write_turn(outbound, session="ollie-main", kind="ambient", agent="ollie",
                ts="1700000000.000300")
    monkeypatch.setenv("GC_SESSION_NAME", "ollie-main")

    captured: list = []

    def fake_post(method, token, payload, *, api_base, timeout):
        captured.append({"token": token, "payload": payload})
        return 200, {}, {"ok": True, "ts": "1700000000.000900"}
    monkeypatch.setattr(outbound, "_slack_web_post", fake_post)

    rc_code = rc.main(["--body", _RUNWAY_LINE, "--raw"])
    assert rc_code == 0
    assert captured[0]["payload"]["text"] == _RUNWAY_LINE
