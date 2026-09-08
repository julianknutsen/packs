"""Tests for the slack-pack company-rooms outbound surface (company rooms 2b).

Hermetic: no network, no real Slack. The single provider seam
(``_slack_web_post``) is monkeypatched, receipt/intent/delegation state lives
under ``tmp_path``, and the retry sleep is stubbed. Covers the sanitizer
fixture parity, the record parsers against the golden fixtures, the token
loader's permission/symlink refusal, the intent lifecycle + receipt-based
reconciliation, delegation-record create-once and one-pending (including a
real two-process flock race), composition/escaping, postMessage retry
policy, the ``delegate``/``--cancel`` verbs, and pruning.
"""

from __future__ import annotations

import hashlib
import json
import multiprocessing
import os
import pathlib
import sys
from datetime import datetime, timezone

import pytest

PACK_DIR = pathlib.Path(__file__).resolve().parent.parent
SCRIPTS_DIR = PACK_DIR / "scripts"
FIXTURES = PACK_DIR / "tests" / "fixtures" / "company"
INTEROP = FIXTURES / "interop"
sys.path.insert(0, str(SCRIPTS_DIR))


@pytest.fixture(autouse=True)
def _isolate_env(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    monkeypatch.setenv("GC_CITY_NAME", "test-city")
    monkeypatch.setenv("GC_CITY_PATH", str(tmp_path))
    monkeypatch.setenv("SLACK_WORKSPACE_ID", "T0AAAAAAA")
    # Never resolve a real adapter env / token.
    monkeypatch.setenv("GC_SLACK_ADAPTER_ENV", str(tmp_path / "no-such-env"))
    for var in (
        "SLACK_COMPANY_SECRETS_DIR", "SLACK_COMPANY_INTENTS_DIR",
        "SLACK_COMPANY_DELEGATIONS_DIR", "SLACK_COMPANY_TURNS_DIR",
        "SLACK_COMPANY_LOCKS_DIR", "SLACK_COMPANY_INGRESS_DIR",
        "SLACK_COMPANY_DIRECTORY_PATH", "SLACK_COMPANY_BINDINGS_PATH",
    ):
        monkeypatch.delenv(var, raising=False)


def _mod():
    sys.modules.pop("slack_company_outbound", None)
    import slack_company_outbound  # type: ignore
    return slack_company_outbound


# --------------------------------------------------------------------------
# 1. Sanitizer fixture parity (byte-for-byte).
# --------------------------------------------------------------------------

def test_sanitizer_matches_golden_fixtures() -> None:
    mod = _mod()
    cases = json.loads((FIXTURES / "sanitizer.json").read_text())["cases"]
    assert cases
    for case in cases:
        got = mod.delegation_filename(case["team_id"], case["channel_id"], case["ts"])
        assert got == case["expected_filename"], case


def test_component_safe_edges() -> None:
    mod = _mod()
    assert mod.component_safe("T0AAAAAAA")
    assert mod.component_safe("1700000000.000100")
    assert mod.component_safe("")  # empty passes through (matches Go)
    assert not mod.component_safe("..")
    assert not mod.component_safe(".hidden")
    assert not mod.component_safe("has/slash")
    assert not mod.component_safe("x" * 65)
    assert mod.component_safe("x" * 64)
    assert not mod.component_safe("café")  # non-ascii hashed


def test_lock_and_nonce_are_deterministic() -> None:
    mod = _mod()
    a = mod.dtuple_lock_name("T", "C", "1.0", "Uresp", "Ureq")
    b = mod.dtuple_lock_name("T", "C", "1.0", "Uresp", "Ureq")
    assert a == b and a.startswith("dtuple-") and a.endswith(".lock")
    assert mod.intent_lock_name("gcs-x").startswith("intent-")
    kw = dict(
        source_app_id="A1", source_bot_user_id="U1", target_agent="riley",
        target_bot_user_id="U2", team_id="T", channel_id="C",
        human_root_ts="1.0", body_sha256="deadbeef",
    )
    n0 = mod.compute_nonce(retry_seq=0, **kw)
    n1 = mod.compute_nonce(retry_seq=1, **kw)
    assert n0.startswith("gcs-") and len(n0) == len("gcs-") + 20
    assert n0 != n1  # retry_seq mints a fresh nonce
    assert n0 == mod.compute_nonce(retry_seq=0, **kw)  # stable


# --------------------------------------------------------------------------
# 1b. Cross-language interop fixtures generated via the REAL code path.
# --------------------------------------------------------------------------

_INTEROP_PINNED = datetime(2026, 7, 17, 12, 0, 0, tzinfo=timezone.utc)


def _build_interop_records(mod):
    """Regenerate the interop intent + delegation via the real record-creation
    code path (clock pinned by the caller) — same bytes a Go consumption test
    reads from the committed fixtures."""
    body_hex = mod.body_sha256("please review PR 42")
    nonce = mod.compute_nonce(
        source_app_id="A0AAAAAA1", source_bot_user_id="U0AAAAAA1", target_agent="riley",
        target_bot_user_id="U0AAAAAA2", team_id="T0AAAAAAA", channel_id="C0AAAAAAA",
        human_root_ts="1700000000.000100", body_sha256=body_hex, retry_seq=0)
    intent = mod.build_intent(
        nonce=nonce, retry_seq=0, source_agent="ollie", source_app_id="A0AAAAAA1",
        source_bot_user_id="U0AAAAAA1", target_agent="riley", target_bot_user_id="U0AAAAAA2",
        team_id="T0AAAAAAA", channel_id="C0AAAAAAA", room="orchestrator-team",
        human_root_ts="1700000000.000100", requester_session="ollie-main", body_hex=body_hex)
    posted = dict(intent, status="published", posted_ts="1700000000.000500")
    delegation = mod.build_delegation(posted)
    return intent, delegation


def test_interop_intent_fixture_regenerates_byte_for_byte(monkeypatch) -> None:
    mod = _mod()
    monkeypatch.setattr(mod, "_now", lambda: _INTEROP_PINNED)
    intent, _ = _build_interop_records(mod)
    regenerated = mod._dumps(intent)
    assert regenerated == (INTEROP / "intent_python.json").read_bytes()
    # The pinned bytes are a valid, fail-closed record.
    assert mod.parse_intent(json.loads(regenerated))["nonce"] == intent["nonce"]


def test_interop_delegation_fixture_regenerates_byte_for_byte(monkeypatch) -> None:
    mod = _mod()
    monkeypatch.setattr(mod, "_now", lambda: _INTEROP_PINNED)
    _, delegation = _build_interop_records(mod)
    regenerated = mod._dumps(delegation)
    assert regenerated == (INTEROP / "delegation_python.json").read_bytes()
    parsed = mod.parse_delegation(json.loads(regenerated))
    assert parsed["status"] == "pending" and parsed["generation"] == 1


# --------------------------------------------------------------------------
# 2. Record parsers validate the golden fixtures.
# --------------------------------------------------------------------------

def test_parsers_accept_golden_fixtures() -> None:
    mod = _mod()
    intent = json.loads((FIXTURES / "intent.json").read_text())
    delegation = json.loads((FIXTURES / "delegation.json").read_text())
    turn = json.loads((FIXTURES / "current_turn.json").read_text())
    assert mod.parse_intent(intent)["nonce"].startswith("gcs-")
    assert mod.parse_delegation(delegation)["status"] == "pending"
    parsed_turn = mod.parse_current_turn(turn)
    assert parsed_turn["kind"] == "peer_delegation"
    # The pointer's delegation_key must re-derive from the sanitizer.
    assert parsed_turn["delegation_key"] == mod.delegation_filename(
        turn["team_id"], turn["channel_id"], turn["ts"])


@pytest.mark.parametrize("turn_ref", [
    "../current", "gct-short", "gct-AAAAAAAAAAAAAAAAAAAA",
    "gct-0000000000000000000g",
])
def test_read_turn_ref_rejects_noncanonical_input(turn_ref: str) -> None:
    mod = _mod()
    with pytest.raises(mod.OutboundError) as exc:
        mod.read_turn_ref("riley-main", turn_ref)
    assert "--turn-ref" in str(exc.value)


@pytest.mark.parametrize("mutate,err", [
    (lambda d: d.update(schema_version=2), "schema_version"),
    (lambda d: d.update(status="bogus"), "status"),
    (lambda d: d.update(nonce="nope"), "nonce"),
    (lambda d: d.pop("body_sha256"), "body_sha256"),
])
def test_parse_intent_rejects_bad(mutate, err) -> None:
    mod = _mod()
    data = json.loads((FIXTURES / "intent.json").read_text())
    mutate(data)
    with pytest.raises(mod.OutboundError) as exc:
        mod.parse_intent(data)
    assert err in str(exc.value)


# --------------------------------------------------------------------------
# Shared fixtures/helpers for the stateful tests.
# --------------------------------------------------------------------------

DIRECTORY = {
    "schema_version": 1,
    "agents": [
        {"name": "ollie", "app_id": "A0AAAAAA1", "bot_user_id": "U0AAAAAA1"},
        {"name": "riley", "app_id": "A0AAAAAA2", "bot_user_id": "U0AAAAAA2"},
    ],
    "rooms": [{
        "name": "orchestrator-team",
        "team_id": "T0AAAAAAA",
        "channel_id": "C0AAAAAAA",
        "members": ["ollie", "riley"],
        "ambient_wake": ["ollie"],
        "mention_wake": ["ollie", "riley"],
    }],
}
BINDINGS = {
    "schema_version": 1,
    "bindings": [
        {"room": "orchestrator-team", "agent": "ollie", "session": "ollie-main"},
        {"room": "orchestrator-team", "agent": "riley", "session": "riley-main"},
    ],
}


def _write_registries(tmp_path: pathlib.Path) -> None:
    slackdir = tmp_path / ".gc" / "slack"
    slackdir.mkdir(parents=True, exist_ok=True)
    (slackdir / "company_directory.json").write_text(json.dumps(DIRECTORY))
    (slackdir / "company_bindings.json").write_text(json.dumps(BINDINGS))


def _write_secret(mod, agent: str, token: str = "xoxb-test-token") -> pathlib.Path:
    sdir = mod.secrets_dir()
    sdir.mkdir(parents=True, exist_ok=True)
    os.chmod(sdir, 0o700)
    path = sdir / f"bot-token-{agent}.txt"
    path.write_text(token)
    os.chmod(path, 0o600)
    return path


def _write_turn(mod, session: str, **overrides) -> dict:
    tdir = mod.turns_dir()
    tdir.mkdir(parents=True, exist_ok=True)
    turn = {
        "schema_version": 1,
        "session": session,
        "receipt_id": "in-example",
        "team_id": "T0AAAAAAA",
        "channel_id": "C0AAAAAAA",
        "ts": "1700000000.000100",
        "room": "orchestrator-team",
        "kind": "targeted",
        "thread_root_ts": "1700000000.000100",
        "agent": "ollie",
        "delegation_key": "",
        "delivered_at": "2026-07-17T12:00:00Z",
    }
    turn.update(overrides)
    (tdir / f"{session}.json").write_text(json.dumps(turn))
    return turn


def _mock_ok(captured: list, ts_values=None):
    seq = iter(ts_values) if ts_values else None

    def fn(method, token, payload, *, api_base, timeout):
        captured.append({"method": method, "token": token, "payload": payload})
        ts = next(seq) if seq else "1700000000.000900"
        return 200, {}, {"ok": True, "ts": ts}
    return fn


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path):
    mod = _mod()
    _write_registries(tmp_path)
    _write_secret(mod, "ollie")
    _write_secret(mod, "riley")
    monkeypatch.setattr(mod, "_sleep", lambda *_a, **_k: None)
    return mod


# --------------------------------------------------------------------------
# 3. Token loader — permission / symlink refusal.
# --------------------------------------------------------------------------

def test_token_loader_reads_valid(env) -> None:
    assert env.load_bot_token("ollie") == "xoxb-test-token"


def test_token_loader_refuses_loose_file_mode(env) -> None:
    path = env.secrets_dir() / "bot-token-ollie.txt"
    os.chmod(path, 0o644)
    with pytest.raises(env.OutboundError) as exc:
        env.load_bot_token("ollie")
    assert "0600" in str(exc.value)


def test_token_loader_refuses_loose_dir_mode(env) -> None:
    os.chmod(env.secrets_dir(), 0o755)
    with pytest.raises(env.OutboundError) as exc:
        env.load_bot_token("ollie")
    assert "0700" in str(exc.value)


def test_token_loader_refuses_symlink_file(env, tmp_path) -> None:
    real = tmp_path / "elsewhere-token.txt"
    real.write_text("xoxb-evil")
    os.chmod(real, 0o600)
    link = env.secrets_dir() / "bot-token-mallory.txt"
    os.symlink(real, link)
    with pytest.raises(env.OutboundError) as exc:
        env.load_bot_token("mallory")
    assert "symlink" in str(exc.value)


def test_token_loader_missing_agent(env) -> None:
    with pytest.raises(env.OutboundError):
        env.load_bot_token("ghost")


# --------------------------------------------------------------------------
# 4. postMessage retry policy + composition.
# --------------------------------------------------------------------------

def test_escaping_only_target_mention_live(env) -> None:
    text = env.compose_delegation_text("U0AAAAAA2", "@channel @here #general <!channel> & <b>")
    assert text.startswith("<@U0AAAAAA2> ")
    assert "<!channel>" not in text  # escaped
    assert "&lt;!channel&gt;" in text
    assert "&amp;" in text
    assert "&lt;b&gt;" in text
    # Inert address tokens survive as literal text (no parsing enabled).
    assert "@channel @here #general" in text
    # Exactly one live entity: the service-built mention.
    assert text.count("<@") == 1


def test_post_message_success(env) -> None:
    captured: list = []
    env._slack_web_post = _mock_ok(captured)
    ts = env.post_message("tok", channel="C0AAAAAAA", text="hi", thread_ts="1.0",
                          metadata={"event_type": "gc_delegation"})
    assert ts == "1700000000.000900"
    p = captured[0]["payload"]
    assert p["channel"] == "C0AAAAAAA" and p["thread_ts"] == "1.0"
    assert "blocks" not in p and "link_names" not in p and "reply_broadcast" not in p
    assert "parse" not in p
    assert p["metadata"] == {"event_type": "gc_delegation"}


def test_post_message_honors_retry_after_429(env, monkeypatch) -> None:
    slept: list = []
    monkeypatch.setattr(env, "_sleep", lambda s: slept.append(s))
    calls = {"n": 0}

    def fn(method, token, payload, *, api_base, timeout):
        calls["n"] += 1
        if calls["n"] == 1:
            return 429, {"Retry-After": "3"}, {"ok": False, "error": "ratelimited"}
        return 200, {}, {"ok": True, "ts": "1700000000.000901"}
    env._slack_web_post = fn
    ts = env.post_message("tok", channel="C", text="x", max_attempts=3)
    assert ts == "1700000000.000901"
    assert slept == [3.0]


def test_post_message_definitive_4xx_raises(env) -> None:
    env._slack_web_post = lambda *a, **k: (200, {}, {"ok": False, "error": "channel_not_found"})
    with pytest.raises(env.DefinitivePostError):
        env.post_message("tok", channel="C", text="x")


def test_post_message_5xx_is_transient(env) -> None:
    env._slack_web_post = lambda *a, **k: (503, {}, {})
    with pytest.raises(env.TransientPostError):
        env.post_message("tok", channel="C", text="x")


# --------------------------------------------------------------------------
# 5. Root derivation.
# --------------------------------------------------------------------------

def test_root_derivation(env) -> None:
    assert env.derive_human_root_ts("", "1700000000.000100") == "1700000000.000100"
    assert env.derive_human_root_ts("1700000000.000050", "1700000000.000100") == "1700000000.000050"


# --------------------------------------------------------------------------
# 6. delegate end-to-end + intent/record lifecycle.
# --------------------------------------------------------------------------

def test_delegate_publishes_and_materializes(env, monkeypatch) -> None:
    monkeypatch.setenv("GC_SESSION_NAME", "ollie-main")
    _write_turn(env, "ollie-main")
    captured: list = []
    env._slack_web_post = _mock_ok(captured, ["1700000000.000500"])

    result = env.run_delegate(to="riley", body="please review", origin_ts="", session_name="ollie-main")
    assert result["status"] == "published"
    assert result["posted_ts"] == "1700000000.000500"
    # Posted as riley's mention into the human root thread.
    payload = captured[0]["payload"]
    assert payload["text"].startswith("<@U0AAAAAA2> ")
    assert payload["thread_ts"] == "1700000000.000100"
    assert payload["metadata"]["event_type"] == "gc_delegation"
    assert payload["metadata"]["event_payload"]["nonce"] == result["nonce"]
    # Intent published + record materialized pending.
    intents = env.list_intents()
    assert len(intents) == 1 and intents[0]["status"] == "published"
    dels = env.list_delegations()
    assert len(dels) == 1 and dels[0][1]["status"] == "pending"
    assert dels[0][1]["expected_responder_bot_user_id"] == "U0AAAAAA2"
    assert result["delegation_key"] == env.delegation_filename(
        "T0AAAAAAA", "C0AAAAAAA", "1700000000.000500")


def test_delegate_threaded_pointer_posts_into_root(env) -> None:
    _write_turn(env, "ollie-main", ts="1700000000.000200",
                thread_root_ts="1700000000.000050")
    captured: list = []
    env._slack_web_post = _mock_ok(captured, ["1700000000.000600"])
    env.run_delegate(to="riley", body="threaded", origin_ts="", session_name="ollie-main")
    assert captured[0]["payload"]["thread_ts"] == "1700000000.000050"


def test_delegate_thread_ambient_is_human_rooted(env) -> None:
    _write_turn(env, "ollie-main", kind="thread_ambient",
                ts="1700000000.000200",
                thread_root_ts="1700000000.000050")
    captured: list = []
    env._slack_web_post = _mock_ok(captured, ["1700000000.000600"])

    result = env.run_delegate(
        to="riley", body="please take this", origin_ts="",
        session_name="ollie-main")

    assert result["status"] == "published"
    assert captured[0]["payload"]["thread_ts"] == "1700000000.000050"


def test_delegate_one_pending_rejected(env) -> None:
    _write_turn(env, "ollie-main")
    env._slack_web_post = _mock_ok([], ["1700000000.000500"])
    env.run_delegate(to="riley", body="first", origin_ts="", session_name="ollie-main")
    with pytest.raises(env.OutboundError) as exc:
        env.run_delegate(to="riley", body="second", origin_ts="", session_name="ollie-main")
    assert "pending delegation" in str(exc.value)


def test_delegate_self_target_rejected(env) -> None:
    _write_turn(env, "ollie-main")
    with pytest.raises(env.OutboundError) as exc:
        env.run_delegate(to="ollie", body="x", origin_ts="", session_name="ollie-main")
    assert "self-targeting" in str(exc.value)


def test_delegate_origin_ts_mismatch(env) -> None:
    _write_turn(env, "ollie-main", ts="1700000000.000100")
    with pytest.raises(env.OutboundError) as exc:
        env.run_delegate(to="riley", body="x", origin_ts="1700000000.999999",
                         session_name="ollie-main")
    assert "origin-ts" in str(exc.value)


def test_delegate_no_session_name(env) -> None:
    _write_turn(env, "ollie-main")
    with pytest.raises(env.OutboundError) as exc:
        env.run_delegate(to="riley", body="x", origin_ts="", session_name="")
    assert "GC_SESSION_NAME" in str(exc.value)


def test_delegate_definitive_failure_marks_intent_failed(env) -> None:
    _write_turn(env, "ollie-main")
    env._slack_web_post = lambda *a, **k: (200, {}, {"ok": False, "error": "channel_not_found"})
    with pytest.raises(env.OutboundError):
        env.run_delegate(to="riley", body="x", origin_ts="", session_name="ollie-main")
    intents = env.list_intents()
    assert len(intents) == 1 and intents[0]["status"] == "failed"
    assert env.list_delegations() == []  # never materialized


def test_delegate_transient_parks_without_repost(env) -> None:
    _write_turn(env, "ollie-main")
    env._slack_web_post = lambda *a, **k: (503, {}, {})
    result = env.run_delegate(to="riley", body="x", origin_ts="", session_name="ollie-main")
    assert result["status"] == "parked"
    intents = env.list_intents()
    assert len(intents) == 1 and intents[0]["status"] == "posting"
    assert env.list_delegations() == []


def test_retry_seq_freshness_after_terminal(env) -> None:
    _write_turn(env, "ollie-main")
    env._slack_web_post = _mock_ok([], ["1700000000.000500"])
    first = env.run_delegate(to="riley", body="same body", origin_ts="", session_name="ollie-main")
    # Cancel so the record is no longer pending, then re-delegate the same body.
    env.run_cancel(to="riley", origin_ts="", session_name="ollie-main")
    env._slack_web_post = _mock_ok([], ["1700000000.000700"])
    second = env.run_delegate(to="riley", body="same body", origin_ts="", session_name="ollie-main")
    assert second["status"] == "published"
    assert first["nonce"] != second["nonce"]  # retry_seq advanced
    assert len(env.list_intents()) == 2


# --------------------------------------------------------------------------
# 7. Receipt-based reconciliation.
# --------------------------------------------------------------------------

def _drop_receipt(mod, nonce, ts, *, team="T0AAAAAAA", channel="C0AAAAAAA", bot=True,
                  app_id="A0AAAAAA1", event_type="gc_delegation", name="in-recon") -> None:
    """Drop a switchboard echo receipt. Defaults author to Ollie's app_id
    (A0AAAAAA1) — the source of the reconcile-test intents — so the reconciler's
    author gate is satisfied on the happy path."""
    rdir = mod.ingress_dir()
    rdir.mkdir(parents=True, exist_ok=True)
    event = {
        "type": "message",
        "metadata": {"event_type": event_type, "event_payload": {"nonce": nonce}},
    }
    if bot:
        event["bot_id"] = "B0OLLIE"
        event["app_id"] = app_id
    receipt = {
        "id": name,
        "origin": {"team_id": team, "channel_id": channel, "ts": ts},
        "event": event,
    }
    (rdir / f"{name}.json").write_text(json.dumps(receipt))


def test_reconcile_adopts_receipt_and_materializes(env) -> None:
    intent = env.build_intent(
        nonce="gcs-recon0000000000000", retry_seq=0, source_agent="ollie",
        source_app_id="A0AAAAAA1", source_bot_user_id="U0AAAAAA1", target_agent="riley",
        target_bot_user_id="U0AAAAAA2", team_id="T0AAAAAAA", channel_id="C0AAAAAAA",
        room="orchestrator-team", human_root_ts="1700000000.000100",
        requester_session="ollie-main", body_hex="ab" * 32)
    intent["status"] = "posting"
    env.write_intent(intent)
    _drop_receipt(env, "gcs-recon0000000000000", "1700000000.000800")

    resolved = env.reconcile_intent(intent)
    assert resolved is not None and resolved["status"] == "published"
    assert resolved["posted_ts"] == "1700000000.000800"
    dels = env.list_delegations()
    assert len(dels) == 1 and dels[0][1]["ts"] == "1700000000.000800"


def test_reconcile_absent_receipt_stays_parked(env) -> None:
    intent = env.build_intent(
        nonce="gcs-recon1111111111111", retry_seq=0, source_agent="ollie",
        source_app_id="A0AAAAAA1", source_bot_user_id="U0AAAAAA1", target_agent="riley",
        target_bot_user_id="U0AAAAAA2", team_id="T0AAAAAAA", channel_id="C0AAAAAAA",
        room="orchestrator-team", human_root_ts="1700000000.000100",
        requester_session="ollie-main", body_hex="cd" * 32)
    intent["status"] = "posting"
    env.write_intent(intent)
    assert env.reconcile_intent(intent) is None
    assert env.list_intents()[0]["status"] == "posting"
    assert env.list_delegations() == []


def test_reconcile_ignores_human_authored_receipt(env) -> None:
    intent = env.build_intent(
        nonce="gcs-recon2222222222222", retry_seq=0, source_agent="ollie",
        source_app_id="A0AAAAAA1", source_bot_user_id="U0AAAAAA1", target_agent="riley",
        target_bot_user_id="U0AAAAAA2", team_id="T0AAAAAAA", channel_id="C0AAAAAAA",
        room="orchestrator-team", human_root_ts="1700000000.000100",
        requester_session="ollie-main", body_hex="ef" * 32)
    intent["status"] = "posting"
    env.write_intent(intent)
    _drop_receipt(env, "gcs-recon2222222222222", "1700000000.000800", bot=False)
    assert env.reconcile_intent(intent) is None


def test_reconcile_rejects_cross_app_receipt(env) -> None:
    """A same-nonce echo authored by a DIFFERENT app is never adopted (the
    nonce is workspace-visible, so author identity is the correlation gate)."""
    intent = env.build_intent(
        nonce="gcs-recon3333333333333", retry_seq=0, source_agent="ollie",
        source_app_id="A0AAAAAA1", source_bot_user_id="U0AAAAAA1", target_agent="riley",
        target_bot_user_id="U0AAAAAA2", team_id="T0AAAAAAA", channel_id="C0AAAAAAA",
        room="orchestrator-team", human_root_ts="1700000000.000100",
        requester_session="ollie-main", body_hex="1a" * 32)
    intent["status"] = "posting"
    env.write_intent(intent)
    # Spoofer bot (Riley's app A0AAAAAA2) posts the same nonce.
    _drop_receipt(env, "gcs-recon3333333333333", "1700000000.000800", app_id="A0AAAAAA2")
    assert env.reconcile_intent(intent) is None
    assert env.list_intents()[0]["status"] == "posting"
    assert env.list_delegations() == []


def test_reconcile_rejects_wrong_event_type(env) -> None:
    """A same-nonce receipt carrying a different event_type (e.g. the result
    echo of the same nonce) is not adopted for a delegation intent."""
    intent = env.build_intent(
        nonce="gcs-recon4444444444444", retry_seq=0, source_agent="ollie",
        source_app_id="A0AAAAAA1", source_bot_user_id="U0AAAAAA1", target_agent="riley",
        target_bot_user_id="U0AAAAAA2", team_id="T0AAAAAAA", channel_id="C0AAAAAAA",
        room="orchestrator-team", human_root_ts="1700000000.000100",
        requester_session="ollie-main", body_hex="2b" * 32)
    intent["status"] = "posting"
    env.write_intent(intent)
    _drop_receipt(env, "gcs-recon4444444444444", "1700000000.000800",
                  event_type="gc_delegation_result")
    assert env.reconcile_intent(intent) is None


# --------------------------------------------------------------------------
# 8. Delegation record create-once (EEXIST adopts).
# --------------------------------------------------------------------------

def test_materialize_create_once_adopts(env) -> None:
    intent = env.build_intent(
        nonce="gcs-once00000000000000", retry_seq=0, source_agent="ollie",
        source_app_id="A0AAAAAA1", source_bot_user_id="U0AAAAAA1", target_agent="riley",
        target_bot_user_id="U0AAAAAA2", team_id="T0AAAAAAA", channel_id="C0AAAAAAA",
        room="orchestrator-team", human_root_ts="1700000000.000100",
        requester_session="ollie-main", body_hex="11" * 32)
    intent["posted_ts"] = "1700000000.000500"
    rec1, created1 = env.materialize_delegation(intent)
    assert created1 is True
    # Simulate Go writing a claim transition, then re-materialize: must adopt.
    path = env._delegation_path("T0AAAAAAA", "C0AAAAAAA", "1700000000.000500")
    claimed = dict(rec1, status="result_claimed", result_ts="1700000000.000600", generation=2)
    env._atomic_write_json(path, claimed)
    rec2, created2 = env.materialize_delegation(intent)
    assert created2 is False
    assert rec2["status"] == "result_claimed"  # never overwritten
    assert rec2["generation"] == 2


# --------------------------------------------------------------------------
# 9. --cancel expires the caller's own pending record.
# --------------------------------------------------------------------------

def test_cancel_expires_pending(env) -> None:
    _write_turn(env, "ollie-main")
    env._slack_web_post = _mock_ok([], ["1700000000.000500"])
    env.run_delegate(to="riley", body="cancel me", origin_ts="", session_name="ollie-main")
    result = env.run_cancel(to="riley", origin_ts="", session_name="ollie-main")
    assert result["status"] == "cancelled"
    dels = env.list_delegations()
    assert len(dels) == 1 and dels[0][1]["status"] == "expired"
    assert dels[0][1]["generation"] == 2


def test_cancel_no_pending(env) -> None:
    _write_turn(env, "ollie-main")
    result = env.run_cancel(to="riley", origin_ts="", session_name="ollie-main")
    assert result["status"] == "no_pending"


# --------------------------------------------------------------------------
# 10. TTL-expired pending is not-pending (delegate proceeds).
# --------------------------------------------------------------------------

def test_ttl_expired_pending_treated_as_not_pending(env) -> None:
    _write_turn(env, "ollie-main")
    # Materialize a pending record whose created_at is well past its TTL.
    intent = env.build_intent(
        nonce="gcs-ttl000000000000000", retry_seq=0, source_agent="ollie",
        source_app_id="A0AAAAAA1", source_bot_user_id="U0AAAAAA1", target_agent="riley",
        target_bot_user_id="U0AAAAAA2", team_id="T0AAAAAAA", channel_id="C0AAAAAAA",
        room="orchestrator-team", human_root_ts="1700000000.000100",
        requester_session="ollie-main", body_hex="22" * 32)
    intent["posted_ts"] = "1700000000.000400"
    rec, _ = env.materialize_delegation(intent)
    path = env._delegation_path("T0AAAAAAA", "C0AAAAAAA", "1700000000.000400")
    env._atomic_write_json(path, dict(rec, created_at="2000-01-01T00:00:00Z", ttl_seconds=1))
    env._slack_web_post = _mock_ok([], ["1700000000.000500"])
    result = env.run_delegate(to="riley", body="fresh", origin_ts="", session_name="ollie-main")
    assert result["status"] == "published"
    statuses = sorted(r["status"] for _p, r in env.list_delegations())
    assert statuses == ["expired", "pending"]


# --------------------------------------------------------------------------
# 11. Two-process flock race — exactly one pending record wins.
# --------------------------------------------------------------------------

def test_two_process_flock_race(env, tmp_path, monkeypatch) -> None:
    _write_turn(env, "ollie-main")
    # Deterministic post seam inherited by both forked children.
    env._slack_web_post = lambda *a, **k: (200, {}, {"ok": True, "ts": "1700000000.000555"})
    results_dir = tmp_path / "race"
    results_dir.mkdir()
    barrier = multiprocessing.Barrier(2)

    def child(idx: int) -> None:
        try:
            barrier.wait(timeout=10)
            res = env.run_delegate(to="riley", body="race", origin_ts="", session_name="ollie-main")
            out = {"ok": True, "result": res}
        except env.OutboundError as exc:
            out = {"ok": False, "error": str(exc)}
        except BaseException as exc:  # noqa: BLE001
            out = {"ok": False, "error": f"unexpected: {exc!r}"}
        (results_dir / f"{idx}.json").write_text(json.dumps(out))
        os._exit(0)

    pids = []
    for idx in (0, 1):
        pid = os.fork()
        if pid == 0:
            child(idx)
        pids.append(pid)
    for pid in pids:
        os.waitpid(pid, 0)

    outs = [json.loads((results_dir / f"{i}.json").read_text()) for i in (0, 1)]
    oks = [o for o in outs if o["ok"]]
    errs = [o for o in outs if not o["ok"]]
    assert len(oks) == 1, outs
    assert len(errs) == 1, outs
    assert "pending delegation" in errs[0]["error"], errs
    # Exactly one delegation record materialized.
    assert len([d for _p, d in env.list_delegations()]) == 1


# --------------------------------------------------------------------------
# 12. Pruning (terminal intents + terminal records past retention).
# --------------------------------------------------------------------------

def test_prune_removes_old_terminal(env) -> None:
    intent = env.build_intent(
        nonce="gcs-prune000000000000", retry_seq=0, source_agent="ollie",
        source_app_id="A0AAAAAA1", source_bot_user_id="U0AAAAAA1", target_agent="riley",
        target_bot_user_id="U0AAAAAA2", team_id="T0AAAAAAA", channel_id="C0AAAAAAA",
        room="orchestrator-team", human_root_ts="1700000000.000100",
        requester_session="ollie-main", body_hex="33" * 32)
    intent["status"] = "failed"
    intent["updated_at"] = "2000-01-01T00:00:00Z"
    env.write_intent(intent)
    # A fresh terminal intent must survive.
    fresh = dict(intent, nonce="gcs-prune111111111111", updated_at=env._rfc3339(env._now()))
    env.write_intent(fresh)

    removed = env.prune()
    assert removed["intents"] == 1
    remaining = {i["nonce"] for i in env.list_intents()}
    assert remaining == {"gcs-prune111111111111"}


def test_prune_retention_floor_clamped(env) -> None:
    intent = env.build_intent(
        nonce="gcs-floor00000000000", retry_seq=0, source_agent="ollie",
        source_app_id="A0AAAAAA1", source_bot_user_id="U0AAAAAA1", target_agent="riley",
        target_bot_user_id="U0AAAAAA2", team_id="T0AAAAAAA", channel_id="C0AAAAAAA",
        room="orchestrator-team", human_root_ts="1700000000.000100",
        requester_session="ollie-main", body_hex="44" * 32)
    intent["status"] = "failed"
    # ~2h old: below the 24h floor, so even prune(0) must keep it.
    from datetime import timedelta
    intent["updated_at"] = env._rfc3339(env._now() - timedelta(hours=2))
    env.write_intent(intent)
    removed = env.prune(retention_seconds=0)
    assert removed["intents"] == 0
    assert len(env.list_intents()) == 1


# --------------------------------------------------------------------------
# 13. P-C: a prepared intent (crash before mark_posting) is adopted & resumed.
# --------------------------------------------------------------------------

def test_delegate_adopts_prepared_intent_exactly_one_post(env) -> None:
    _write_turn(env, "ollie-main")
    body = "resume me"
    body_hex = env.body_sha256(body)
    nonce = env.compute_nonce(
        source_app_id="A0AAAAAA1", source_bot_user_id="U0AAAAAA1", target_agent="riley",
        target_bot_user_id="U0AAAAAA2", team_id="T0AAAAAAA", channel_id="C0AAAAAAA",
        human_root_ts="1700000000.000100", body_sha256=body_hex, retry_seq=0)
    prepared = env.build_intent(
        nonce=nonce, retry_seq=0, source_agent="ollie", source_app_id="A0AAAAAA1",
        source_bot_user_id="U0AAAAAA1", target_agent="riley", target_bot_user_id="U0AAAAAA2",
        team_id="T0AAAAAAA", channel_id="C0AAAAAAA", room="orchestrator-team",
        human_root_ts="1700000000.000100", requester_session="ollie-main", body_hex=body_hex)
    env.write_intent(prepared)  # status 'prepared', nothing ever posted
    assert env.list_intents()[0]["status"] == "prepared"

    captured: list = []
    env._slack_web_post = _mock_ok(captured, ["1700000000.000500"])
    result = env.run_delegate(to="riley", body=body, origin_ts="", session_name="ollie-main")
    assert result["status"] == "published"
    assert result["nonce"] == nonce  # adopted the prepared intent, not a fresh one
    assert len(captured) == 1  # exactly one post
    assert len(env.list_intents()) == 1  # no orphaned second intent
    assert env.list_intents()[0]["status"] == "published"


# --------------------------------------------------------------------------
# 14. P-G4: prune retains the per-tuple retry_seq watermark; re-mint is fresh.
# --------------------------------------------------------------------------

def test_prune_retains_watermark_and_redelegate_is_fresh(env) -> None:
    from datetime import timedelta
    _write_turn(env, "ollie-main")
    env._slack_web_post = _mock_ok([], ["1700000000.000500"])
    first = env.run_delegate(to="riley", body="same body", origin_ts="", session_name="ollie-main")
    env.run_cancel(to="riley", origin_ts="", session_name="ollie-main")  # record expired

    intents = env.list_intents()
    assert len(intents) == 1
    old = intents[0]
    old["updated_at"] = env._rfc3339(env._now() - timedelta(days=30))
    env.write_intent(old)

    removed = env.prune()
    # The highest-seq intent per tuple is retained even when terminal + old, so
    # next_retry_seq stays monotonic and a re-mint cannot reuse the old nonce.
    assert removed["intents"] == 0
    assert len(env.list_intents()) == 1

    env._slack_web_post = _mock_ok([], ["1700000000.000700"])
    second = env.run_delegate(to="riley", body="same body", origin_ts="", session_name="ollie-main")
    assert second["status"] == "published"
    assert second["nonce"] != first["nonce"]  # fresh nonce, no reuse
    assert len(env.list_intents()) == 2


# --------------------------------------------------------------------------
# 15. P-G7: hostile bot_user_ids are rejected before mention interpolation.
# --------------------------------------------------------------------------

def test_compose_rejects_hostile_bot_user_id(env) -> None:
    with pytest.raises(env.OutboundError) as exc:
        env.compose_delegation_text("U0> <!channel", "hi")
    assert "member id" in str(exc.value)
    with pytest.raises(env.OutboundError):
        env.compose_result_text("U0><https://evil.example|click", "hi")
    # A well-formed id still composes to exactly one live mention.
    text = env.compose_delegation_text("U0AAAAAA2", "hi")
    assert text.startswith("<@U0AAAAAA2> ") and text.count("<@") == 1


def test_delegate_rejects_hostile_directory_bot_user_id(env, tmp_path) -> None:
    slackdir = tmp_path / ".gc" / "slack"
    poisoned = json.loads((slackdir / "company_directory.json").read_text())
    for a in poisoned["agents"]:
        if a["name"] == "riley":
            a["bot_user_id"] = "U0> <!channel"
    (slackdir / "company_directory.json").write_text(json.dumps(poisoned))
    _write_turn(env, "ollie-main")
    env._slack_web_post = _mock_ok([], ["1700000000.000500"])
    with pytest.raises(env.OutboundError) as exc:
        env.run_delegate(to="riley", body="x", origin_ts="", session_name="ollie-main")
    assert "member id" in str(exc.value)
    assert env.list_intents() == []  # rejected before any intent was created


# --------------------------------------------------------------------------
# 16. P-B: result posts are durable — a timeout reconciles, never double-posts.
# --------------------------------------------------------------------------

def _write_peer_delegation(env, session: str, *, ts: str, nonce: str) -> str:
    record = {
        "schema_version": 1, "generation": 1, "nonce": nonce,
        "room": "orchestrator-team", "team_id": "T0AAAAAAA", "channel_id": "C0AAAAAAA",
        "ts": ts, "thread_root_ts": "1700000000.000100",
        "requester_agent": "ollie", "requester_bot_user_id": "U0AAAAAA1",
        "requester_session": "ollie-main",
        "expected_responder_agent": "riley", "expected_responder_bot_user_id": "U0AAAAAA2",
        "created_at": env._rfc3339(env._now()), "ttl_seconds": 86400,
        "status": "pending", "result_ts": "", "result_claimed_at": "",
    }
    key = env.delegation_filename("T0AAAAAAA", "C0AAAAAAA", ts)
    ddir = env.delegations_dir()
    ddir.mkdir(parents=True, exist_ok=True)
    (ddir / key).write_text(json.dumps(record))
    _write_turn(env, session, kind="peer_delegation", agent="riley", ts=ts,
                delegation_key=key)
    return key


def test_result_timeout_then_reconcile_never_double_posts(env) -> None:
    key = _write_peer_delegation(env, "riley-main", ts="1700000000.000500",
                                 nonce="gcs-presult0000000000")
    posts: list = []

    # First attempt: chat.postMessage times out AFTER Slack accepted it.
    def timeout_post(method, token, payload, *, api_base, timeout):
        posts.append(payload)
        raise env.TransientPostError("simulated timeout")
    env._slack_web_post = timeout_post

    first = env.post_peer_result(body="the answer is 42", origin_ts="",
                                 session_name="riley-main")
    assert first["status"] == "parked"
    assert len(posts) == 1
    intents = env.list_intents()
    assert len(intents) == 1 and intents[0]["status"] == "posting"
    assert intents[0]["op"] == "result"
    # No delegation record was materialized for a result intent.
    assert all(r["status"] == "pending" for _p, r in env.list_delegations())

    # The switchboard echo of the accepted result now lands as a receipt.
    result_intent = intents[0]
    rdir = env.ingress_dir()
    rdir.mkdir(parents=True, exist_ok=True)
    echo = {
        "id": "in-result-echo",
        "origin": {"team_id": "T0AAAAAAA", "channel_id": "C0AAAAAAA",
                   "ts": "1700000000.000700"},
        "event": {
            "type": "message", "app_id": "A0AAAAAA2", "bot_id": "B0RILEY",
            "metadata": {"event_type": "gc_delegation_result",
                         "event_payload": {"v": 1, "nonce": "gcs-presult0000000000",
                                           "delegation_ts": "1700000000.000500"}},
        },
    }
    (rdir / "in-result-echo.json").write_text(json.dumps(echo))

    # Second attempt must reconcile (adopt the echo) and NOT post again.
    def forbid_post(method, token, payload, *, api_base, timeout):
        raise AssertionError("must not repost after reconciliation")
    env._slack_web_post = forbid_post

    second = env.post_peer_result(body="the answer is 42", origin_ts="",
                                  session_name="riley-main")
    assert second["status"] == "posted"
    assert second["posted_ts"] == "1700000000.000700"
    assert len(posts) == 1  # still exactly one provider POST
    assert env.list_intents()[0]["status"] == "published"


def test_session_name_aliases_dot_dunder():
    """gc sanitizes configured session names dot->dunder; both spellings
    identify one session for pointer lookup and spoof-guard comparison."""
    mod = _mod()
    assert mod.session_name_aliases("teams__it") == ["teams__it", "teams.it"]
    assert mod.session_name_aliases("teams.it") == ["teams.it", "teams__it"]
    assert mod.session_name_aliases("plain") == ["plain"]
    assert mod.session_name_aliases("") == []


# --------------------------------------------------------------------------
# 12. Per-agent DMs (Phase 4): pointer contract, resolution, spoof guard,
#     durable DM posting + reconciliation, delegation refusal.
# --------------------------------------------------------------------------

DM_BINDINGS = {
    "schema_version": 1,
    "dm_bindings": [
        {"agent": "ollie", "session": "ollie-dm"},
        {"agent": "riley", "session": "riley-dm"},
    ],
}


def _write_dm_bindings(tmp_path: pathlib.Path, obj=None) -> None:
    slackdir = tmp_path / ".gc" / "slack"
    slackdir.mkdir(parents=True, exist_ok=True)
    (slackdir / "dm_bindings.json").write_text(json.dumps(obj or DM_BINDINGS))


def _write_dm_turn(mod, session: str, **overrides) -> dict:
    tdir = mod.turns_dir()
    tdir.mkdir(parents=True, exist_ok=True)
    turn = {
        "schema_version": 1,
        "session": session,
        "receipt_id": "in-dm",
        "team_id": "T0AAAAAAA",
        "channel_id": "D0HUMANOLLIE",
        "ts": "1700000000.000900",
        "room": "",
        "kind": "dm",
        "thread_root_ts": "1700000000.000900",
        "agent": "ollie",
        "owner_app_id": "A0AAAAAA1",
        "delivered_at": "2026-07-18T12:00:00Z",
    }
    turn.update(overrides)
    dm_dir = tdir / "dm"
    dm_dir.mkdir(parents=True, exist_ok=True)
    (dm_dir / f"{session}.json").write_text(json.dumps(turn))
    return turn


def test_dm_pointer_parses_with_empty_room(env) -> None:
    turn = _write_dm_turn(env, "ollie-dm")
    parsed = env.read_current_turn_dm("ollie-dm")
    assert parsed is not None
    assert parsed["kind"] == "dm" and parsed["room"] == ""
    assert parsed["owner_app_id"] == "A0AAAAAA1"


def test_dm_pointer_requires_owner_app_id(env) -> None:
    _write_dm_turn(env, "ollie-dm", owner_app_id="")
    with pytest.raises(env.OutboundError) as exc:
        env.read_current_turn_dm("ollie-dm")
    assert "owner_app_id" in str(exc.value)


def test_non_dm_pointer_still_requires_room(env) -> None:
    # A room-kind pointer with an empty room stays invalid (relaxation is
    # dm-only).
    tdir = env.turns_dir(); tdir.mkdir(parents=True, exist_ok=True)
    turn = _write_turn(env, "ollie-main")
    turn["room"] = ""
    (tdir / "ollie-main.json").write_text(json.dumps(turn))
    with pytest.raises(env.OutboundError):
        env.read_current_turn("ollie-main")


def test_room_pointer_without_owner_app_id_is_byte_compatible(env) -> None:
    # A pre-Phase-4 room pointer (no owner_app_id key) parses unchanged.
    _write_turn(env, "ollie-main", kind="targeted")
    on_disk = json.loads((env.turns_dir() / "ollie-main.json").read_text())
    assert "owner_app_id" not in on_disk
    parsed = env.read_current_turn("ollie-main")
    assert parsed is not None and parsed["kind"] == "targeted"


def test_resolve_source_none_without_pointers(env) -> None:
    assert env.resolve_reply_pointer_source("ollie-main") is None


def test_resolve_source_room_only_and_dm_only(env) -> None:
    _write_turn(env, "ollie-main", kind="targeted")
    assert env.resolve_reply_pointer_source("ollie-main") == "room"
    _write_dm_turn(env, "ollie-dm")
    assert env.resolve_reply_pointer_source("ollie-dm") == "dm"


def test_resolve_source_newest_delivered_at_wins(env) -> None:
    # Same session has both a room and a DM pointer; newest delivered_at wins.
    _write_turn(env, "sess", kind="targeted", delivered_at="2026-07-18T12:00:00Z")
    _write_dm_turn(env, "sess", delivered_at="2026-07-18T12:00:05Z")
    assert env.resolve_reply_pointer_source("sess") == "dm"
    # Move the room pointer newer.
    _write_turn(env, "sess", kind="targeted", delivered_at="2026-07-18T12:00:09Z")
    assert env.resolve_reply_pointer_source("sess") == "room"


def test_resolve_source_tie_prefers_dm(env) -> None:
    _write_turn(env, "sess", kind="targeted", delivered_at="2026-07-18T12:00:00Z")
    _write_dm_turn(env, "sess", delivered_at="2026-07-18T12:00:00Z")
    assert env.resolve_reply_pointer_source("sess") == "dm"


def test_resolve_source_kind_override(env) -> None:
    _write_turn(env, "sess", kind="targeted", delivered_at="2026-07-18T12:00:09Z")
    _write_dm_turn(env, "sess", delivered_at="2026-07-18T12:00:00Z")
    # Room is newer, but --kind dm forces the DM pointer.
    assert env.resolve_reply_pointer_source("sess", kind_override="dm") == "dm"
    assert env.resolve_reply_pointer_source("sess", kind_override="room") == "room"


def test_resolve_source_kind_override_missing_pointer_errors(env) -> None:
    _write_dm_turn(env, "ollie-dm")
    with pytest.raises(env.OutboundError):
        env.resolve_reply_pointer_source("ollie-dm", kind_override="room")


def test_dm_reply_posts_with_owner_token(env, tmp_path) -> None:
    _write_dm_bindings(tmp_path)
    _write_dm_turn(env, "ollie-dm")
    captured: list = []
    env._slack_web_post = _mock_ok(captured, ["1700000000.001000"])
    result = env.post_company_dm_reply(body="hi human", origin_ts="", session_name="ollie-dm")
    assert result["status"] == "posted" and result["kind"] == "dm"
    p = captured[0]["payload"]
    assert captured[0]["token"] == "xoxb-test-token"  # ollie's own token
    assert p["channel"] == "D0HUMANOLLIE"
    # Top-level DM message (root == ts): the reply posts flat, in-channel.
    assert "thread_ts" not in p
    assert p["text"] == "hi human"  # escaped, no live mention
    assert "<@" not in p["text"]
    assert p["metadata"]["event_type"] == "gc_dm_reply"
    assert p["metadata"]["event_payload"]["nonce"] == result["nonce"]


def test_dm_reply_threads_only_when_human_threaded(env, tmp_path) -> None:
    _write_dm_bindings(tmp_path)
    # The human replied INSIDE a thread: the turn's own ts differs from its
    # root, so the agent reply stays in that thread.
    _write_dm_turn(env, "ollie-dm", ts="1700000000.000950",
                   thread_root_ts="1700000000.000900")
    captured: list = []
    env._slack_web_post = _mock_ok(captured, ["1700000000.001000"])
    result = env.post_company_dm_reply(body="in thread", origin_ts="", session_name="ollie-dm")
    assert result["status"] == "posted"
    assert captured[0]["payload"]["thread_ts"] == "1700000000.000900"


def test_dm_reply_escapes_body(env, tmp_path) -> None:
    _write_dm_bindings(tmp_path)
    _write_dm_turn(env, "ollie-dm")
    captured: list = []
    env._slack_web_post = _mock_ok(captured, ["1700000000.001000"])
    env.post_company_dm_reply(body="<@U0AAAAAA2> & <b>", origin_ts="", session_name="ollie-dm")
    text = captured[0]["payload"]["text"]
    assert "<@" not in text and "&amp;" in text and "&lt;b&gt;" in text


def test_dm_reply_spoof_guard_unbound_session(env, tmp_path) -> None:
    # ollie's DM pointer, but the session is not ollie's dm-bound session.
    _write_dm_bindings(tmp_path, {
        "schema_version": 1, "dm_bindings": [{"agent": "ollie", "session": "someone-else"}]})
    _write_dm_turn(env, "ollie-dm")
    with pytest.raises(env.OutboundError) as exc:
        env.post_company_dm_reply(body="x", origin_ts="", session_name="ollie-dm")
    assert "spoof guard" in str(exc.value)


def test_dm_reply_session_alias_accepted(env, tmp_path) -> None:
    # dm_binding uses the dotted form; the session runs with the dunder form.
    _write_dm_bindings(tmp_path, {
        "schema_version": 1, "dm_bindings": [{"agent": "ollie", "session": "teams.pm"}]})
    _write_dm_turn(env, "teams__pm")
    captured: list = []
    env._slack_web_post = _mock_ok(captured, ["1700000000.001000"])
    result = env.post_company_dm_reply(body="ok", origin_ts="", session_name="teams__pm")
    assert result["status"] == "posted"


def test_dm_reply_owner_app_id_mismatch_rejected(env, tmp_path) -> None:
    _write_dm_bindings(tmp_path)
    _write_dm_turn(env, "ollie-dm", owner_app_id="A0WRONGAPP")
    with pytest.raises(env.OutboundError) as exc:
        env.post_company_dm_reply(body="x", origin_ts="", session_name="ollie-dm")
    assert "owner_app_id" in str(exc.value)


def test_dm_reply_origin_ts_mismatch(env, tmp_path) -> None:
    _write_dm_bindings(tmp_path)
    _write_dm_turn(env, "ollie-dm", ts="1700000000.000900")
    with pytest.raises(env.OutboundError) as exc:
        env.post_company_dm_reply(body="x", origin_ts="1700000000.999999",
                                  session_name="ollie-dm")
    assert "origin-ts" in str(exc.value)


def test_dm_reply_transient_parks_then_reconciles(env, tmp_path) -> None:
    _write_dm_bindings(tmp_path)
    _write_dm_turn(env, "ollie-dm")
    # First POST times out -> parked (no receipt yet).
    env._slack_web_post = lambda *a, **k: (_ for _ in ()).throw(
        env.TransientPostError("timeout"))
    parked = env.post_company_dm_reply(body="deferred", origin_ts="", session_name="ollie-dm")
    assert parked["status"] == "parked"
    nonce = parked["nonce"]
    intents = env.list_intents()
    assert len(intents) == 1 and intents[0]["status"] == "posting"
    assert intents[0]["op"] == "dm"
    # The owner app's self-echo lands as a dm receipt carrying our nonce.
    _drop_receipt(env, nonce, "1700000000.001100", channel="D0HUMANOLLIE",
                  app_id="A0AAAAAA1", event_type="gc_dm_reply", name="in-dm-echo")
    assert env.reconcile_posting_intents() == 1
    resolved = env.list_intents()[0]
    assert resolved["status"] == "published"
    assert resolved["posted_ts"] == "1700000000.001100"
    # A DM reply owns NO delegation record.
    assert env.list_delegations() == []


def test_dm_reply_no_company_pointer_needs_dm_pointer(env, tmp_path) -> None:
    _write_dm_bindings(tmp_path)
    # Only a room pointer exists; the DM verb refuses (no dm pointer).
    _write_turn(env, "ollie-dm", kind="targeted")
    with pytest.raises(env.OutboundError) as exc:
        env.post_company_dm_reply(body="x", origin_ts="", session_name="ollie-dm")
    assert "no DM current-turn pointer" in str(exc.value)


def test_delegate_refused_from_dm_root(env, tmp_path) -> None:
    # A DM turn can only reach delegate if it were the room pointer; the gate
    # refuses a dm kind explicitly regardless.
    _write_dm_bindings(tmp_path)
    tdir = env.turns_dir(); tdir.mkdir(parents=True, exist_ok=True)
    dm_as_room = {
        "schema_version": 1, "session": "ollie-main", "receipt_id": "in-x",
        "team_id": "T0AAAAAAA", "channel_id": "D0HUMANOLLIE", "ts": "1700000000.000900",
        "room": "", "kind": "dm", "thread_root_ts": "1700000000.000900",
        "agent": "ollie", "owner_app_id": "A0AAAAAA1",
        "delivered_at": "2026-07-18T12:00:00Z",
    }
    (tdir / "ollie-main.json").write_text(json.dumps(dm_as_room))
    with pytest.raises(env.OutboundError) as exc:
        env.run_delegate(to="riley", body="x", origin_ts="", session_name="ollie-main")
    assert "DM-rooted turn" in str(exc.value)


# --------------------------------------------------------------------------
# 13. Cross-language golden/interop fixtures for the DM surface (skip-if-absent;
#     the Go/adapter side owns creating tests/fixtures/company/).
# --------------------------------------------------------------------------

def test_dm_bindings_golden_fixture_parses_if_present(env) -> None:
    fx = FIXTURES / "dm_bindings.json"
    if not fx.exists():
        pytest.skip("dm_bindings.json golden fixture not present yet (adapter side)")
    data = json.loads(fx.read_text())
    assert data.get("schema_version") == 1
    assert isinstance(data.get("dm_bindings"), list)
    seen_agents = set()
    for entry in data["dm_bindings"]:
        assert entry["agent"] not in seen_agents  # singleton per agent
        seen_agents.add(entry["agent"])
        assert entry.get("session")


def test_dm_pointer_golden_fixture_parses_if_present(env) -> None:
    fx = FIXTURES / "dm_current_turn.json"
    if not fx.exists():
        pytest.skip("dm_current_turn.json golden fixture not present yet (adapter side)")
    parsed = env.parse_current_turn(json.loads(fx.read_text()))
    assert parsed["kind"] == "dm"
    assert parsed["room"] == ""  # empty room permitted for kind dm
    assert parsed["owner_app_id"]
    assert "delegation_key" not in json.loads(fx.read_text())  # DM turns are keyless


def test_self_echo_fixture_metadata_matches_dm_metadata(env) -> None:
    """C4/m3: the committed self-echo wire fixture pins the EXACT metadata block
    ``dm_metadata`` produces ({"event_type":"gc_dm_reply","event_payload":
    {"v":1,"nonce":...}}), so a drift in either breaks a test."""
    envelope = json.loads((FIXTURES / "message_im_self_echo.json").read_text())
    inner_meta = envelope["event"]["metadata"]
    nonce = inner_meta["event_payload"]["nonce"]
    assert inner_meta == env.dm_metadata(nonce)


def test_self_echo_fixture_reconciles_dm_intent(env) -> None:
    """C4/m12: a stuck posting op=dm intent reconciles against the committed
    self-echo fixture's nonce end-to-end through _scan_receipt_for_nonce — the
    interop pin the fixture exists for (spec test-plan items 4 + 12)."""
    envelope = json.loads((FIXTURES / "message_im_self_echo.json").read_text())
    inner = envelope["event"]
    nonce = inner["metadata"]["event_payload"]["nonce"]

    rdir = env.ingress_dir()
    rdir.mkdir(parents=True, exist_ok=True)
    receipt = {
        "id": "in-selfecho",
        "origin": {
            "team_id": envelope["team_id"],
            "channel_id": inner["channel"],
            "ts": inner["ts"],
        },
        "event": inner,  # the exact wire event Slack echoed back
    }
    (rdir / "in-selfecho.json").write_text(json.dumps(receipt))

    intent = {
        "op": "dm",
        "nonce": nonce,
        "team_id": envelope["team_id"],
        "channel_id": inner["channel"],
        "source_app_id": inner["app_id"],
        "source_bot_user_id": inner["user"],
    }
    assert env._scan_receipt_for_nonce(intent) == inner["ts"]


# --------------------------------------------------------------------------
# Phase 5 body-store split — the receipt_body accessor (both shapes forever).
# --------------------------------------------------------------------------

def _write_body_split_receipt(env, receipt_id, origin, inner) -> dict:
    """Write a body-split receipt: the raw event in bodies/<id>.body.json and a
    receipt carrying body_ref + event_digest (no embedded event), as Go's Admit
    produces."""
    rdir = env.ingress_dir()
    bdir = env.bodies_dir()
    rdir.mkdir(parents=True, exist_ok=True)
    bdir.mkdir(parents=True, exist_ok=True)
    body_bytes = json.dumps(inner).encode("utf-8")
    digest = hashlib.sha256(body_bytes).hexdigest()
    (bdir / f"{receipt_id}.body.json").write_bytes(body_bytes)
    receipt = {
        "id": receipt_id,
        "schema_version": 1,
        "origin": origin,
        "body_ref": receipt_id,
        "event_digest": digest,
    }
    (rdir / f"{receipt_id}.json").write_text(json.dumps(receipt))
    return receipt


def test_receipt_body_resolves_body_split(env) -> None:
    inner = {"type": "message", "text": "hi", "bot_id": "B1"}
    receipt = _write_body_split_receipt(
        env, "in-bodysplit-aaaa",
        {"team_id": "T1", "channel_id": "C1", "ts": "1.1"}, inner)
    assert env.receipt_body(receipt) == inner


def test_receipt_body_legacy_embedded(env) -> None:
    inner = {"type": "message", "text": "legacy"}
    receipt = {"id": "in-legacy", "origin": {"team_id": "T1", "channel_id": "C1", "ts": "1.2"},
               "event": inner}
    assert env.receipt_body(receipt) == inner


def test_receipt_body_redacted_is_none(env) -> None:
    receipt = _write_body_split_receipt(
        env, "in-redact-bbbb",
        {"team_id": "T1", "channel_id": "C1", "ts": "1.3"},
        {"type": "message", "text": "secret"})
    # Redact: truncate the body to the tombstone the Go verb writes.
    bpath = env.bodies_dir() / "in-redact-bbbb.body.json"
    bpath.write_text(json.dumps({"redacted": True, "event_digest": receipt["event_digest"]}))
    assert env.receipt_body(receipt) is None


def test_receipt_body_missing_is_none(env) -> None:
    receipt = _write_body_split_receipt(
        env, "in-missing-cccc",
        {"team_id": "T1", "channel_id": "C1", "ts": "1.4"},
        {"type": "message"})
    (env.bodies_dir() / "in-missing-cccc.body.json").unlink()
    assert env.receipt_body(receipt) is None


def test_receipt_body_digest_mismatch_is_none(env) -> None:
    receipt = _write_body_split_receipt(
        env, "in-mismatch-dddd",
        {"team_id": "T1", "channel_id": "C1", "ts": "1.5"},
        {"type": "message", "text": "orig"})
    # Tamper the body without updating the receipt's immutable digest.
    (env.bodies_dir() / "in-mismatch-dddd.body.json").write_text(
        json.dumps({"type": "message", "text": "tampered"}))
    assert env.receipt_body(receipt) is None


def test_body_split_receipt_reconciles_intent(env) -> None:
    """A body-split self-echo receipt reconciles through _scan_receipt_for_nonce
    exactly as the legacy embedded shape does — both shapes forever."""
    nonce = "gcs-bodysplit-nonce"
    inner = {
        "type": "message", "bot_id": "B0OLLIE", "app_id": "A0OLLIE", "user": "U0OLLIE",
        "metadata": {"event_type": "gc_dm_reply", "event_payload": {"nonce": nonce}},
    }
    origin = {"team_id": "T0AAAAAAA", "channel_id": "C0DM", "ts": "1700000000.123456"}
    _write_body_split_receipt(env, "in-bodyecho-eeee", origin, inner)
    intent = {
        "op": "dm", "nonce": nonce,
        "team_id": origin["team_id"], "channel_id": origin["channel_id"],
        "source_app_id": inner["app_id"], "source_bot_user_id": inner["user"],
    }
    assert env._scan_receipt_for_nonce(intent) == origin["ts"]


def test_redacted_body_split_receipt_does_not_reconcile(env) -> None:
    """A redacted body reads as a no-match: reconciliation never adopts it."""
    nonce = "gcs-redacted-nonce"
    inner = {
        "type": "message", "bot_id": "B0OLLIE", "app_id": "A0OLLIE", "user": "U0OLLIE",
        "metadata": {"event_type": "gc_dm_reply", "event_payload": {"nonce": nonce}},
    }
    origin = {"team_id": "T0AAAAAAA", "channel_id": "C0DM", "ts": "1700000000.654321"}
    receipt = _write_body_split_receipt(env, "in-redecho-ffff", origin, inner)
    (env.bodies_dir() / "in-redecho-ffff.body.json").write_text(
        json.dumps({"redacted": True, "event_digest": receipt["event_digest"]}))
    intent = {
        "op": "dm", "nonce": nonce,
        "team_id": origin["team_id"], "channel_id": origin["channel_id"],
        "source_app_id": inner["app_id"], "source_bot_user_id": inner["user"],
    }
    assert env._scan_receipt_for_nonce(intent) is None


def test_interop_body_split_receipt_golden(env) -> None:
    """m4: the body-split receipt + sidecar pair generated by the REAL Go Admit
    writer (tests/fixtures/company/interop/receipt_body_split_go{,.body}.json) reads
    byte-for-byte through the Python accessor and reconciles end to end. A Go-side
    drift in the filename suffix, digest computation, or field spelling that passed
    the Go suite AND the Python suite's own synthetic shape would break HERE."""
    receipt = json.loads((INTEROP / "receipt_body_split_go.json").read_text())
    body_bytes = (INTEROP / "receipt_body_split_go.body.json").read_bytes()

    # Byte-parity: the sidecar bytes hash to the receipt's immutable event_digest
    # exactly as the Go writer recorded it.
    assert hashlib.sha256(body_bytes).hexdigest() == receipt["event_digest"]

    # Lay the pair into the store exactly where Go wrote it, then resolve through
    # the Python accessor: it returns the exact inner event the sidecar holds.
    rdir = env.ingress_dir()
    bdir = env.bodies_dir()
    rdir.mkdir(parents=True, exist_ok=True)
    bdir.mkdir(parents=True, exist_ok=True)
    (rdir / f"{receipt['id']}.json").write_text(json.dumps(receipt))
    (bdir / f"{receipt['body_ref']}.body.json").write_bytes(body_bytes)

    inner = env.receipt_body(receipt)
    assert inner is not None
    assert inner == json.loads(body_bytes.decode("utf-8"))

    # And it reconciles a stuck posting intent through _scan_receipt_for_nonce.
    nonce = inner["metadata"]["event_payload"]["nonce"]
    intent = {
        "op": "dm", "nonce": nonce,
        "team_id": receipt["origin"]["team_id"],
        "channel_id": receipt["origin"]["channel_id"],
        "source_app_id": inner["app_id"], "source_bot_user_id": inner["user"],
    }
    assert env._scan_receipt_for_nonce(intent) == receipt["origin"]["ts"]


# --------------------------------------------------------------------------
# 14. Group DMs (Phase 4b): mpim pointer contract, three-way resolution,
#     reply spoof guard + owner_app_id, delegation refusal, golden fixture.
# --------------------------------------------------------------------------

def _write_mpim_turn(mod, session: str, **overrides) -> dict:
    tdir = mod.turns_dir()
    tdir.mkdir(parents=True, exist_ok=True)
    turn = {
        "schema_version": 1,
        "session": session,
        "receipt_id": "in-mpim",
        "team_id": "T0AAAAAAA",
        "channel_id": "G0GROUPDM01",
        "ts": "1700000000.000900",
        "room": "",
        "kind": "mpim",
        "thread_root_ts": "1700000000.000900",
        "agent": "ollie",
        "owner_app_id": "A0AAAAAA1",
        "delivered_at": "2026-07-18T12:00:00Z",
    }
    turn.update(overrides)
    mpim_dir = tdir / "mpim"
    mpim_dir.mkdir(parents=True, exist_ok=True)
    (mpim_dir / f"{session}.json").write_text(json.dumps(turn))
    return turn


def test_mpim_pointer_parses_with_empty_room(env) -> None:
    _write_mpim_turn(env, "ollie-dm")
    parsed = env.read_current_turn_mpim("ollie-dm")
    assert parsed is not None
    assert parsed["kind"] == "mpim" and parsed["room"] == ""
    assert parsed["owner_app_id"] == "A0AAAAAA1"


def test_mpim_pointer_requires_owner_app_id(env) -> None:
    _write_mpim_turn(env, "ollie-dm", owner_app_id="")
    with pytest.raises(env.OutboundError) as exc:
        env.read_current_turn_mpim("ollie-dm")
    assert "owner_app_id" in str(exc.value)


def test_resolve_source_mpim_only(env) -> None:
    _write_mpim_turn(env, "ollie-dm")
    assert env.resolve_reply_pointer_source("ollie-dm") == "mpim"


def test_resolve_source_three_way_newest_wins(env) -> None:
    # A dm turn then an mpim turn for the same session: BOTH live, newest wins.
    _write_dm_turn(env, "sess", delivered_at="2026-07-18T12:00:00Z")
    _write_mpim_turn(env, "sess", delivered_at="2026-07-18T12:00:05Z")
    assert env.resolve_reply_pointer_source("sess") == "mpim"
    # --kind dm recovers the older 1:1 turn (pointer isolation: both still live).
    assert env.resolve_reply_pointer_source("sess", kind_override="dm") == "dm"
    # Move the dm turn newer; it wins by delivered_at.
    _write_dm_turn(env, "sess", delivered_at="2026-07-18T12:00:09Z")
    assert env.resolve_reply_pointer_source("sess") == "dm"


def test_resolve_source_mpim_kind_override_and_missing(env) -> None:
    _write_turn(env, "sess", kind="targeted", delivered_at="2026-07-18T12:00:09Z")
    _write_mpim_turn(env, "sess", delivered_at="2026-07-18T12:00:00Z")
    # Room is newer, but --kind mpim forces the mpim pointer.
    assert env.resolve_reply_pointer_source("sess", kind_override="mpim") == "mpim"
    # --kind mpim on a company session with a pointer but no mpim one errors
    # (a session with NO company pointer at all returns None instead — legacy).
    _write_turn(env, "roomonly", kind="targeted")
    with pytest.raises(env.OutboundError):
        env.resolve_reply_pointer_source("roomonly", kind_override="mpim")


def test_resolve_source_tie_prefers_dm_over_mpim_over_room(env) -> None:
    # Equal delivered_at across all three: the most-private surface wins.
    _write_turn(env, "sess", kind="targeted", delivered_at="2026-07-18T12:00:00Z")
    _write_mpim_turn(env, "sess", delivered_at="2026-07-18T12:00:00Z")
    assert env.resolve_reply_pointer_source("sess") == "mpim"  # mpim beats room
    _write_dm_turn(env, "sess", delivered_at="2026-07-18T12:00:00Z")
    assert env.resolve_reply_pointer_source("sess") == "dm"    # dm beats both


def test_resolve_source_tolerates_poison_mpim_pointer(env) -> None:
    # m4 defense-in-depth: a session with a healthy room pointer AND a POISONED
    # mpim pointer (empty owner_app_id, which parse_current_turn refuses). One
    # corrupt pointer must not brick reply-current on the session's OTHER live
    # surfaces — auto-resolution treats the corrupt kind as absent and returns the
    # room pointer. An explicit --kind targeting the corrupt pointer STILL errors.
    _write_turn(env, "sess", kind="targeted", delivered_at="2026-07-18T12:00:00Z")
    _write_mpim_turn(env, "sess", owner_app_id="", delivered_at="2026-07-18T12:00:05Z")
    assert env.resolve_reply_pointer_source("sess") == "room"
    with pytest.raises(env.OutboundError) as exc:
        env.resolve_reply_pointer_source("sess", kind_override="mpim")
    assert "owner_app_id" in str(exc.value)


def test_mpim_reply_posts_with_woken_token_flat(env, tmp_path) -> None:
    _write_dm_bindings(tmp_path)
    _write_mpim_turn(env, "ollie-dm")
    captured: list = []
    env._slack_web_post = _mock_ok(captured, ["1700000000.001000"])
    result = env.post_company_mpim_reply(body="on it", origin_ts="", session_name="ollie-dm")
    assert result["status"] == "posted" and result["kind"] == "mpim"
    p = captured[0]["payload"]
    assert captured[0]["token"] == "xoxb-test-token"  # ollie's OWN token
    assert p["channel"] == "G0GROUPDM01"
    assert "thread_ts" not in p  # top-level group message → flat
    assert "<@" not in p["text"]  # escaped, no live mention
    # Reconciliation reuses the DM op/event_type unchanged.
    assert p["metadata"]["event_type"] == "gc_dm_reply"
    assert result["nonce"] == p["metadata"]["event_payload"]["nonce"]


def test_mpim_reply_threads_only_when_human_threaded(env, tmp_path) -> None:
    _write_dm_bindings(tmp_path)
    _write_mpim_turn(env, "ollie-dm", ts="1700000000.000950",
                     thread_root_ts="1700000000.000900")
    captured: list = []
    env._slack_web_post = _mock_ok(captured, ["1700000000.001000"])
    env.post_company_mpim_reply(body="in thread", origin_ts="", session_name="ollie-dm")
    assert captured[0]["payload"]["thread_ts"] == "1700000000.000900"


def test_mpim_reply_non_winner_agent_passes_own_guard(env, tmp_path) -> None:
    # C11 regression: an mpim pointer for the WOKEN agent riley (its own app id),
    # even though riley may not be the admission owner, passes riley's own guard.
    _write_dm_bindings(tmp_path)
    _write_mpim_turn(env, "riley-dm", agent="riley", owner_app_id="A0AAAAAA2")
    captured: list = []
    env._slack_web_post = _mock_ok(captured, ["1700000000.001000"])
    result = env.post_company_mpim_reply(body="ack", origin_ts="", session_name="riley-dm")
    assert result["status"] == "posted"
    assert captured[0]["token"] == "xoxb-test-token"  # riley's own token in this env


def test_mpim_reply_spoof_guard_unbound_session(env, tmp_path) -> None:
    _write_dm_bindings(tmp_path, {
        "schema_version": 1, "dm_bindings": [{"agent": "ollie", "session": "someone-else"}]})
    _write_mpim_turn(env, "ollie-dm")
    with pytest.raises(env.OutboundError) as exc:
        env.post_company_mpim_reply(body="x", origin_ts="", session_name="ollie-dm")
    assert "spoof guard" in str(exc.value)


def test_mpim_reply_owner_app_id_mismatch_rejected(env, tmp_path) -> None:
    _write_dm_bindings(tmp_path)
    _write_mpim_turn(env, "ollie-dm", owner_app_id="A0WRONGAPP")
    with pytest.raises(env.OutboundError) as exc:
        env.post_company_mpim_reply(body="x", origin_ts="", session_name="ollie-dm")
    assert "owner_app_id" in str(exc.value)


def test_mpim_reply_origin_ts_mismatch(env, tmp_path) -> None:
    _write_dm_bindings(tmp_path)
    _write_mpim_turn(env, "ollie-dm", ts="1700000000.000900")
    with pytest.raises(env.OutboundError) as exc:
        env.post_company_mpim_reply(body="x", origin_ts="1700000000.999999",
                                    session_name="ollie-dm")
    assert "origin-ts" in str(exc.value)


def test_mpim_reply_needs_mpim_pointer(env, tmp_path) -> None:
    _write_dm_bindings(tmp_path)
    # Only a room pointer exists; the mpim verb refuses (no mpim pointer).
    _write_turn(env, "ollie-dm", kind="targeted")
    with pytest.raises(env.OutboundError) as exc:
        env.post_company_mpim_reply(body="x", origin_ts="", session_name="ollie-dm")
    assert "no mpim current-turn pointer" in str(exc.value)


def test_mpim_reply_transient_parks_then_reconciles(env, tmp_path) -> None:
    _write_dm_bindings(tmp_path)
    _write_mpim_turn(env, "ollie-dm")
    env._slack_web_post = lambda *a, **k: (_ for _ in ()).throw(
        env.TransientPostError("timeout"))
    parked = env.post_company_mpim_reply(body="deferred", origin_ts="", session_name="ollie-dm")
    assert parked["status"] == "parked"
    nonce = parked["nonce"]
    intents = env.list_intents()
    assert len(intents) == 1 and intents[0]["op"] == "dm"  # op unchanged for mpim
    # The owner app's mpim self-echo lands as an mpim receipt carrying our nonce
    # (channel is the group DM); it reconciles the op=dm intent by nonce.
    _drop_receipt(env, nonce, "1700000000.001100", channel="G0GROUPDM01",
                  app_id="A0AAAAAA1", event_type="gc_dm_reply", name="in-mpim-echo")
    assert env.reconcile_posting_intents() == 1
    resolved = env.list_intents()[0]
    assert resolved["status"] == "published"
    assert resolved["posted_ts"] == "1700000000.001100"
    assert env.list_delegations() == []  # a dm-family reply owns no record


def test_delegate_refused_from_mpim_root(env, tmp_path) -> None:
    # An mpim kind reaching the room-pointer delegate gate is refused (dm-family).
    _write_dm_bindings(tmp_path)
    tdir = env.turns_dir(); tdir.mkdir(parents=True, exist_ok=True)
    mpim_as_room = {
        "schema_version": 1, "session": "ollie-main", "receipt_id": "in-x",
        "team_id": "T0AAAAAAA", "channel_id": "G0GROUPDM01", "ts": "1700000000.000900",
        "room": "", "kind": "mpim", "thread_root_ts": "1700000000.000900",
        "agent": "ollie", "owner_app_id": "A0AAAAAA1",
        "delivered_at": "2026-07-18T12:00:00Z",
    }
    (tdir / "ollie-main.json").write_text(json.dumps(mpim_as_room))
    with pytest.raises(env.OutboundError) as exc:
        env.run_delegate(to="riley", body="x", origin_ts="", session_name="ollie-main")
    assert "MPIM-rooted turn" in str(exc.value)


def test_mpim_pointer_golden_fixture_parses_if_present(env) -> None:
    fx = FIXTURES / "mpim_current_turn.json"
    if not fx.exists():
        pytest.skip("mpim_current_turn.json golden fixture not present yet")
    parsed = env.parse_current_turn(json.loads(fx.read_text()))
    assert parsed["kind"] == "mpim"
    assert parsed["room"] == ""  # empty room permitted for kind mpim
    assert parsed["owner_app_id"]
    assert "delegation_key" not in json.loads(fx.read_text())  # mpim turns are keyless


# --------------------------------------------------------------------------
# 17. Accidental-mrkdwn guard (gp-o42) on the delegate verb — the body is
#     posted as mrkdwn, so tilde pairs would strike it through.
# --------------------------------------------------------------------------

_RUNWAY_LINE = "• Total out: ~$58.5k → *~$16.5k left on Sep 30* from a $75k start."


def _capture_delegate(env, monkeypatch: pytest.MonkeyPatch) -> dict:
    """Fake ``run_delegate`` so the test pins exactly the CLI plumbing:
    argv → body load → guard → the body the delegation actually posts."""
    captured: dict = {}

    def fake_run_delegate(**kwargs):
        captured.update(kwargs)
        return {"status": "posted", "posted_ts": "1700000000.000900"}

    monkeypatch.setattr(env, "run_delegate", fake_run_delegate)
    monkeypatch.setenv("GC_SESSION_NAME", "ollie-main")
    return captured


def test_delegate_body_is_guarded_by_default(
        env, monkeypatch: pytest.MonkeyPatch) -> None:
    import slack_mrkdwn  # type: ignore
    captured = _capture_delegate(env, monkeypatch)

    assert env.cmd_delegate(["--to", "riley", "--body", _RUNWAY_LINE]) == 0
    assert "~" not in captured["body"]  # no pairable ASCII tilde reaches Slack
    assert captured["body"] == \
        _RUNWAY_LINE.replace("~", slack_mrkdwn.TILDE_SUBSTITUTE)


def test_delegate_raw_flag_skips_the_guard(
        env, monkeypatch: pytest.MonkeyPatch) -> None:
    captured = _capture_delegate(env, monkeypatch)

    assert env.cmd_delegate(
        ["--to", "riley", "--body", _RUNWAY_LINE, "--raw"]) == 0
    assert captured["body"] == _RUNWAY_LINE


def test_delegate_body_file_is_guarded_too(
        env, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    # --body-file is the documented shape in the agent prompt fragment, and
    # it loads through the same _load_body_arg the guard sits after.
    import slack_mrkdwn  # type: ignore
    captured = _capture_delegate(env, monkeypatch)
    body_file = tmp_path / "delegation.md"
    body_file.write_text(_RUNWAY_LINE, encoding="utf-8")

    assert env.cmd_delegate(
        ["--to", "riley", "--body-file", str(body_file)]) == 0
    assert captured["body"] == \
        _RUNWAY_LINE.replace("~", slack_mrkdwn.TILDE_SUBSTITUTE)
