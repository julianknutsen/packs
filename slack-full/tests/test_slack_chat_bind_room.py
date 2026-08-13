"""Tests for the slack pack's bind-room builders.

Only the pure functions are exercised here — the HTTP path is verified
end-to-end via gc events in the slack-pack README's verification recipe.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys

import pytest

PACK_DIR = pathlib.Path(__file__).resolve().parent.parent
SCRIPTS_DIR = PACK_DIR / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))


@pytest.fixture(autouse=True)
def _isolate_env(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    monkeypatch.setenv("GC_CITY_NAME", "test-city")
    monkeypatch.setenv("GC_CITY_PATH", str(tmp_path))
    monkeypatch.setenv("SLACK_WORKSPACE_ID", "T0TESTWS")
    monkeypatch.delenv("GC_SLACK_ADAPTER_ENV", raising=False)


def _import_module():
    if "slack_chat_bind_room" in sys.modules:
        del sys.modules["slack_chat_bind_room"]
    import slack_chat_bind_room  # type: ignore
    return slack_chat_bind_room


def _make_args(**overrides) -> argparse.Namespace:
    base = dict(
        enable_peer_fanout=False,
        allow_untargeted_publication=False,
        max_peer_triggered_publishes=0,
        max_total_peer_deliveries=0,
    )
    base.update(overrides)
    return argparse.Namespace(**base)


def test_default_handle_for_session_with_path_and_dot():
    mod = _import_module()
    assert mod._default_handle_for_session("geo/oversight-rig.project-lead") == "geo-project-lead"


def test_default_handle_for_session_dot_only():
    mod = _import_module()
    assert mod._default_handle_for_session("oversight-rig.mayor") == "mayor"


def test_default_handle_for_session_raw_id():
    mod = _import_module()
    assert mod._default_handle_for_session("gc-83347") == "gc-83347"


def test_parse_handle_overrides():
    mod = _import_module()
    out = mod._parse_handle_overrides(["mayor=oversight-rig.mayor", "geo-pl=geo/oversight-rig.project-lead"])
    assert out == {
        "oversight-rig.mayor": "mayor",
        "geo/oversight-rig.project-lead": "geo-pl",
    }


def test_parse_handle_overrides_rejects_malformed():
    mod = _import_module()
    with pytest.raises(SystemExit):
        mod._parse_handle_overrides(["mayor"])
    with pytest.raises(SystemExit):
        mod._parse_handle_overrides(["=oversight-rig.mayor"])
    with pytest.raises(SystemExit):
        mod._parse_handle_overrides(["mayor="])


def test_parse_handle_overrides_rejects_dup_session():
    mod = _import_module()
    with pytest.raises(SystemExit):
        mod._parse_handle_overrides(["m=s.x", "n=s.x"])


def test_build_fanout_policy_none_when_no_flags_set():
    mod = _import_module()
    assert mod.build_fanout_policy(_make_args()) is None


def test_build_fanout_policy_includes_all_fields_when_any_flag_set():
    mod = _import_module()
    policy = mod.build_fanout_policy(_make_args(
        enable_peer_fanout=True,
        allow_untargeted_publication=True,
        max_peer_triggered_publishes=5,
        max_total_peer_deliveries=12,
    ))
    assert policy == {
        "enabled": True,
        "allow_untargeted_publication": True,
        "max_peer_triggered_publishes": 5,
        "max_total_peer_deliveries": 12,
    }


def test_build_fanout_policy_only_caps_set_still_emits_full_struct():
    mod = _import_module()
    policy = mod.build_fanout_policy(_make_args(max_total_peer_deliveries=24))
    assert policy is not None
    assert policy["enabled"] is False
    assert policy["max_total_peer_deliveries"] == 24
    assert policy["max_peer_triggered_publishes"] == 0


def test_build_participants_uses_default_handle_for_each_session():
    mod = _import_module()
    out = mod.build_participants(
        ["oversight-rig.mayor", "geo/oversight-rig.project-lead"],
        overrides={},
        default_handle="",
    )
    assert out == [
        ("mayor", "oversight-rig.mayor"),
        ("geo-project-lead", "geo/oversight-rig.project-lead"),
    ]


def test_build_participants_overrides_win():
    mod = _import_module()
    out = mod.build_participants(
        ["oversight-rig.mayor", "geo/oversight-rig.project-lead"],
        overrides={"geo/oversight-rig.project-lead": "geo-pl"},
        default_handle="",
    )
    assert out[1] == ("geo-pl", "geo/oversight-rig.project-lead")


def test_build_participants_rejects_duplicate_handles():
    mod = _import_module()
    with pytest.raises(SystemExit):
        mod.build_participants(
            ["a.mayor", "b.mayor"],  # both derive to "mayor"
            overrides={},
            default_handle="",
        )


def test_build_participants_default_handle_must_match_a_participant():
    mod = _import_module()
    with pytest.raises(SystemExit):
        mod.build_participants(
            ["oversight-rig.mayor"],
            overrides={},
            default_handle="ghost",
        )


def test_build_conversation_ref_is_room_kind_and_full_scope():
    mod = _import_module()
    ref = mod.build_conversation_ref(
        conversation_id="C0123ROOM01",
        kind="room",
        workspace_id="T0TESTWS",
        scope_id="my-city",
    )
    assert ref == {
        "scope_id": "my-city",
        "provider": "slack",
        "account_id": "T0TESTWS",
        "conversation_id": "C0123ROOM01",
        "kind": "room",
    }


def test_main_round_trip_with_fake_gc(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture):
    """Mock ``common.gc_post``, verify the script issues the expected calls and writes pack config."""
    mod = _import_module()
    common = sys.modules["slack_intake_common"]

    calls: list[tuple[str, dict]] = []

    def fake_post(path: str, body: dict):
        calls.append((path, body))
        if path == "/extmsg/groups":
            return {"ID": "group-xyz", "FanoutPolicy": body.get("fanout_policy") or {}}
        if path == "/extmsg/participants":
            return {"ID": "p-" + body["handle"], "Handle": body["handle"], "SessionID": body["session_id"]}
        if path == "/extmsg/unbind":
            return {"unbound": [{"ID": "binding-old", "SessionID": "gc-stale"}]}
        raise AssertionError(f"unexpected path {path}")

    monkeypatch.setattr(common, "gc_post", fake_post)

    rc = mod.main([
        "C0123ROOM01",
        "oversight-rig.mayor", "geo/oversight-rig.project-lead",
        "--enable-peer-fanout",
        "--max-peer-triggered-publishes", "5",
    ])
    assert rc == 0
    paths = [c[0] for c in calls]
    assert paths == [
        "/extmsg/groups",
        "/extmsg/participants",
        "/extmsg/participants",
        "/extmsg/unbind",
    ]
    assert calls[-1][1] == {"conversation": {
        "scope_id": "test-city",
        "provider": "slack",
        "account_id": "T0TESTWS",
        "conversation_id": "C0123ROOM01",
        "kind": "room",
    }}

    group_body = calls[0][1]
    assert group_body["root_conversation"]["kind"] == "room"
    assert group_body["root_conversation"]["conversation_id"] == "C0123ROOM01"
    assert group_body["mode"] == "launcher"
    assert group_body["default_handle"] == "mayor"
    assert group_body["fanout_policy"] == {
        "enabled": True,
        "allow_untargeted_publication": False,
        "max_peer_triggered_publishes": 5,
        "max_total_peer_deliveries": 0,
    }

    p1, p2 = calls[1][1], calls[2][1]
    assert p1["group_id"] == "group-xyz"
    assert p1["handle"] == "mayor"
    assert p1["session_id"] == "oversight-rig.mayor"
    assert p2["handle"] == "geo-project-lead"
    assert p2["session_id"] == "geo/oversight-rig.project-lead"

    cfg_path = pathlib.Path(os.environ["GC_CITY_PATH"]) / ".gc/services/slack/data/config.json"
    assert cfg_path.exists()
    saved = json.loads(cfg_path.read_text())
    binding = saved["bindings"]["room:C0123ROOM01"]
    assert binding["group_id"] == "group-xyz"
    assert binding["default_handle"] == "mayor"
    assert binding["fanout_policy"]["enabled"] is True
    assert [p["session_name"] for p in binding["participants"]] == [
        "oversight-rig.mayor", "geo/oversight-rig.project-lead",
    ]

    out = capsys.readouterr().out
    result = json.loads(out)
    assert result["binding_key"] == "room:C0123ROOM01"
    assert result["group_id"] == "group-xyz"
    assert result["binding_record"] == {
        "unbound": [{"ID": "binding-old", "SessionID": "gc-stale"}],
    }


def test_main_with_binding_owner_emits_extmsg_bind(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture):
    """``--binding-owner SESSION`` adds a fourth POST to /extmsg/bind for that session."""
    mod = _import_module()
    common = sys.modules["slack_intake_common"]

    calls: list[tuple[str, dict]] = []

    def fake_post(path: str, body: dict):
        calls.append((path, body))
        if path == "/extmsg/groups":
            return {"ID": "group-xyz"}
        if path == "/extmsg/participants":
            return {"ID": "p-" + body["handle"]}
        if path == "/extmsg/bind":
            return {"ID": "binding-1", "SessionID": body["session_id"]}
        raise AssertionError(f"unexpected path {path}")

    monkeypatch.setattr(common, "gc_post", fake_post)

    rc = mod.main([
        "C0123ROOM01",
        "gc-77139", "gc-83347",
        "--handle", "geo-pl=gc-77139",
        "--handle", "cos=gc-83347",
        "--default-handle", "geo-pl",
        "--binding-owner", "gc-77139",
    ])
    assert rc == 0
    paths = [c[0] for c in calls]
    assert paths == [
        "/extmsg/groups",
        "/extmsg/participants",
        "/extmsg/participants",
        "/extmsg/bind",
    ]
    bind_body = calls[-1][1]
    assert bind_body["session_id"] == "gc-77139"
    assert bind_body["replace"] is True
    assert bind_body["conversation"] == {
        "scope_id": "test-city",
        "provider": "slack",
        "account_id": "T0TESTWS",
        "conversation_id": "C0123ROOM01",
        "kind": "room",
    }

    saved = json.loads(
        (pathlib.Path(os.environ["GC_CITY_PATH"]) / ".gc/services/slack/data/config.json").read_text()
    )
    binding = saved["bindings"]["room:C0123ROOM01"]
    assert binding["binding_owner"] == "gc-77139"
    assert binding["binding_record"] == "binding-1"


def test_binding_owner_can_be_separate_gcid_when_participants_are_aliases(monkeypatch: pytest.MonkeyPatch):
    """``--binding-owner`` accepts a gc-id even when participants are passed as aliases.

    This is the canonical room-binding shape used by oversight-rig: participants
    are passed as aliases (e.g. ``geo/oversight-rig.project-lead``) so handles
    derive cleanly, but the binding owner is the gc-id of the project-lead so
    that ``resolve_rig_channel.py`` (which queries bindings by gc-id from the
    sessions list) finds the binding.
    """
    mod = _import_module()
    common = sys.modules["slack_intake_common"]

    calls: list[tuple[str, dict]] = []

    def fake_post(path: str, body: dict):
        calls.append((path, body))
        if path == "/extmsg/groups":
            return {"ID": "group-xyz"}
        if path == "/extmsg/participants":
            return {"ID": "p-" + body["handle"]}
        if path == "/extmsg/bind":
            return {"ID": "binding-1", "SessionID": body["session_id"]}
        raise AssertionError(f"unexpected path {path}")

    monkeypatch.setattr(common, "gc_post", fake_post)

    rc = mod.main([
        "C0123ROOM01",
        "oversight-rig.cos", "geo/oversight-rig.project-lead",
        "--binding-owner", "gc-77139",  # gc-id, not in participant alias set
    ])
    assert rc == 0
    bind_call = next(c for c in calls if c[0] == "/extmsg/bind")
    assert bind_call[1]["session_id"] == "gc-77139"


@pytest.mark.parametrize("binding_owner", ["", "gc-77139"])
def test_binding_reconciliation_failure_does_not_persist_or_report_success(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
    binding_owner: str,
):
    mod = _import_module()
    common = sys.modules["slack_intake_common"]

    def fake_post(path: str, body: dict):
        if path == "/extmsg/groups":
            return {"ID": "group-xyz"}
        if path == "/extmsg/participants":
            return {"ID": "p-" + body["handle"]}
        if path in {"/extmsg/bind", "/extmsg/unbind"}:
            raise common.GCAPIError("authoritative binding store unavailable")
        raise AssertionError(f"unexpected path {path}")

    monkeypatch.setattr(common, "gc_post", fake_post)
    args = ["C0123ROOM01", "gc-77139", "--no-protocol-nudge"]
    if binding_owner:
        args.extend(["--binding-owner", binding_owner])

    with pytest.raises(SystemExit, match="authoritative binding store unavailable"):
        mod.main(args)

    cfg_path = pathlib.Path(os.environ["GC_CITY_PATH"]) / ".gc/services/slack/data/config.json"
    assert not cfg_path.exists()
    assert capsys.readouterr().out == ""
