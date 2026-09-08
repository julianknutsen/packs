"""Tests for slack upload's gc-vs-adapter routing path.

Mirror of ``test_slack_chat_reply_current``: by default ``gc slack
upload`` routes through gc's ``/extmsg/outbound-file`` so transcript
recording + peer fanout fire. ``--via adapter`` is preserved as the
diagnostic path that hits the local adapter directly.
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
    monkeypatch.setenv("GC_SESSION_ID", "gc-test-session")
    monkeypatch.delenv("GC_SLACK_ADAPTER_ENV", raising=False)


def _import_modules():
    for name in ("slack_chat_upload", "slack_intake_common"):
        sys.modules.pop(name, None)
    import slack_intake_common  # type: ignore
    import slack_chat_upload  # type: ignore
    return slack_chat_upload, slack_intake_common


def _fake_binding() -> dict[str, str]:
    return {
        "scope_id": "test-city",
        "provider": "slack",
        "account_id": "T0TESTWS",
        "conversation_id": "C0123ROOM",
        "kind": "room",
    }


def _make_file(tmp_path: pathlib.Path) -> pathlib.Path:
    p = tmp_path / "report.txt"
    p.write_text("payload bytes", encoding="utf-8")
    return p


def test_default_via_routes_through_gc_outbound_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    upload, common = _import_modules()
    captured: dict[str, Any] = {}

    def fake_request(method: str, url: str, body: dict[str, Any] | None = None,
                     *, csrf: bool = True, timeout: float = 30.0) -> dict[str, Any]:
        captured["method"] = method
        captured["url"] = url
        captured["body"] = body
        captured["csrf"] = csrf
        return {"Receipt": {"Delivered": True, "FileID": "F123"}}

    monkeypatch.setattr(common, "_request", fake_request)
    monkeypatch.setattr(common, "look_up_binding", lambda _sid: _fake_binding())

    file_path = _make_file(tmp_path)
    exit_code = upload.main([
        "--file", str(file_path),
        "--session", "gc-test-session",
        "--initial-comment", "hi peers",
    ])
    assert exit_code == 0
    assert captured["method"] == "POST"
    assert captured["url"] == (
        "http://127.0.0.1:8372/v0/city/test-city/extmsg/outbound-file"
    )
    assert captured["csrf"] is True
    body = captured["body"]
    assert body["session_id"] == "gc-test-session"
    assert body["conversation"]["conversation_id"] == "C0123ROOM"
    assert body["file_path"] == str(file_path.resolve())
    assert body["initial_comment"] == "hi peers"

    out = json.loads(capsys.readouterr().out)
    assert out["via"] == "gc"


def test_via_adapter_keeps_direct_adapter_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    upload, common = _import_modules()
    captured: dict[str, Any] = {}

    def fake_request(method: str, url: str, body: dict[str, Any] | None = None,
                     *, csrf: bool = True, timeout: float = 30.0) -> dict[str, Any]:
        captured["method"] = method
        captured["url"] = url
        captured["body"] = body
        captured["csrf"] = csrf
        return {"delivered": True, "file_id": "F999"}

    monkeypatch.setattr(common, "_request", fake_request)
    monkeypatch.setattr(common, "look_up_binding", lambda _sid: _fake_binding())

    file_path = _make_file(tmp_path)
    exit_code = upload.main([
        "--file", str(file_path),
        "--session", "gc-test-session",
        "--via", "adapter",
    ])
    assert exit_code == 0
    assert captured["url"].endswith("/publish-file")
    # No /extmsg/ prefix — adapter-direct path bypasses gc entirely.
    assert "/extmsg/" not in captured["url"]

    out = json.loads(capsys.readouterr().out)
    assert out["via"] == "adapter"


def test_thread_ts_and_idempotency_propagate_through_gc_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
) -> None:
    upload, common = _import_modules()
    captured: dict[str, Any] = {}

    def fake_request(method: str, url: str, body: dict[str, Any] | None = None,
                     *, csrf: bool = True, timeout: float = 30.0) -> dict[str, Any]:
        captured["body"] = body
        return {"Receipt": {"Delivered": True, "FileID": "F1"}}

    monkeypatch.setattr(common, "_request", fake_request)
    monkeypatch.setattr(common, "look_up_binding", lambda _sid: _fake_binding())

    file_path = _make_file(tmp_path)
    upload.main([
        "--file", str(file_path),
        "--session", "gc-test-session",
        "--thread-ts", "1700000.000100",
        "--idempotency-key", "up-42",
        "--title", "Run 7",
        "--filename", "out.txt",
    ])
    body = captured["body"]
    assert body["reply_to_message_id"] == "1700000.000100"
    assert body["idempotency_key"] == "up-42"
    assert body["title"] == "Run 7"
    assert body["filename"] == "out.txt"


def test_thread_current_unwraps_helper_tuple(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
) -> None:
    """Regression for the gc-j8h live-smoke bug.

    `find_latest_inbound_message_id_for_session` returns
    `tuple[str, dict]`; the upload script must extract `match[0]`
    rather than passing the whole tuple as `thread_ts`. Without
    the unpack, gc rejects the request with a 422 because
    `reply_to_message_id` arrives on the wire as a 2-element list
    instead of a string.
    """
    upload, common = _import_modules()
    captured: dict[str, Any] = {}

    def fake_request(method: str, url: str, body: dict[str, Any] | None = None,
                     *, csrf: bool = True, timeout: float = 30.0) -> dict[str, Any]:
        captured["body"] = body
        return {"Receipt": {"Delivered": True, "FileID": "F2"}}

    monkeypatch.setattr(common, "_request", fake_request)
    monkeypatch.setattr(common, "look_up_binding", lambda _sid: _fake_binding())
    monkeypatch.setattr(
        common,
        "find_latest_inbound_message_id_for_session",
        lambda _sid: ("1777779766.848799", _fake_binding()),
    )

    file_path = _make_file(tmp_path)
    upload.main([
        "--file", str(file_path),
        "--session", "gc-test-session",
        "--thread-current",
    ])
    body = captured["body"]
    # Critical: a plain string, NOT the (msg_id, conversation) tuple.
    assert body["reply_to_message_id"] == "1777779766.848799"
    assert isinstance(body["reply_to_message_id"], str)


# --------------------------------------------------------------------------
# Accidental-mrkdwn guard (gp-o42) — --initial-comment renders as mrkdwn
# beside the file, so tilde pairs would strike it through.
# --------------------------------------------------------------------------

_RUNWAY_LINE = "• Total out: ~$58.5k → *~$16.5k left on Sep 30* from a $75k start."


def _capture_upload(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    upload, common = _import_modules()
    captured: dict[str, Any] = {}

    def fake_request(method: str, url: str, body: dict[str, Any] | None = None,
                     *, csrf: bool = True, timeout: float = 30.0) -> dict[str, Any]:
        captured["body"] = body
        return {"Receipt": {"Delivered": True, "FileID": "F7"}}

    monkeypatch.setattr(common, "_request", fake_request)
    monkeypatch.setattr(common, "look_up_binding", lambda _sid: _fake_binding())
    captured["upload"] = upload
    return captured


def test_initial_comment_is_guarded_by_default(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
) -> None:
    import slack_mrkdwn  # type: ignore
    captured = _capture_upload(monkeypatch)

    exit_code = captured["upload"].main([
        "--file", str(_make_file(tmp_path)),
        "--session", "gc-test-session",
        "--initial-comment", _RUNWAY_LINE,
    ])
    assert exit_code == 0
    comment = captured["body"]["initial_comment"]
    assert "~" not in comment  # no pairable ASCII tilde reaches Slack
    assert comment == _RUNWAY_LINE.replace("~", slack_mrkdwn.TILDE_SUBSTITUTE)


def test_initial_comment_raw_flag_skips_the_guard(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
) -> None:
    captured = _capture_upload(monkeypatch)

    exit_code = captured["upload"].main([
        "--file", str(_make_file(tmp_path)),
        "--session", "gc-test-session",
        "--initial-comment", _RUNWAY_LINE,
        "--raw",
    ])
    assert exit_code == 0
    assert captured["body"]["initial_comment"] == _RUNWAY_LINE


def test_via_adapter_branch_is_guarded_too(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
) -> None:
    # Upload guards inline in main() rather than in a shared body loader,
    # and both --via branches read the one guarded variable. Pin the
    # adapter branch as well so a future refactor cannot guard only the
    # default path while the suite stays green.
    import slack_mrkdwn  # type: ignore
    captured = _capture_upload(monkeypatch)

    exit_code = captured["upload"].main([
        "--file", str(_make_file(tmp_path)),
        "--session", "gc-test-session",
        "--via", "adapter",
        "--initial-comment", _RUNWAY_LINE,
    ])
    assert exit_code == 0
    assert captured["body"]["initial_comment"] == \
        _RUNWAY_LINE.replace("~", slack_mrkdwn.TILDE_SUBSTITUTE)
