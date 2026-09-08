"""Tests for ``gc slack publish-to-channel``.

Covers exit-code behavior on the adapter receipt's ``delivered`` field,
plus argument plumbing. Added in response to Copilot review on PR #14
(gpk-bf3 iteration) — the prior commit landed the delivered-false gate
without a regression test for this specific CLI.
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
    for name in ("slack_chat_publish_to_channel", "slack_intake_common"):
        sys.modules.pop(name, None)
    import slack_intake_common  # type: ignore
    import slack_chat_publish_to_channel  # type: ignore
    return slack_chat_publish_to_channel, slack_intake_common


def test_publish_to_channel_success_returns_zero(
        monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    pub, common = _import_modules()
    captured: dict[str, Any] = {}

    def fake_publish(**kwargs):
        captured.update(kwargs)
        return {"delivered": True, "message_id": "1700.001"}

    monkeypatch.setattr(common, "publish_to_channel_via_adapter", fake_publish)

    rc = pub.main([
        "--conversation-id", "C0CHAN01",
        "--session", "gc-1",
        "--body", "ok",
    ])
    assert rc == 0
    assert captured["conversation_id"] == "C0CHAN01"
    assert captured["text"] == "ok"
    out = json.loads(capsys.readouterr().out)
    assert out["conversation_id"] == "C0CHAN01"


def test_publish_to_channel_exits_nonzero_on_delivered_false(
        monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    """Adapter HTTP-200 with delivered=false must surface as exit 1.

    Regression test for the latent defect tracked by gpk-5sk and the
    follow-up Copilot review on PR #14. Without this gate, the PL
    stamps loop_close_posted_at on the bead even when slack rejected
    the post (auth, channel renamed, scope change).
    """
    pub, common = _import_modules()

    def fake_publish(**_kwargs):
        return {"delivered": False, "failure_kind": "auth"}

    monkeypatch.setattr(common, "publish_to_channel_via_adapter", fake_publish)

    rc = pub.main([
        "--conversation-id", "C0CHAN01",
        "--session", "gc-1",
        "--body", "rejected",
    ])
    assert rc == 1
    err = capsys.readouterr().err
    assert "delivered=false" in err
    assert "failure_kind=auth" in err


def test_publish_to_channel_exits_nonzero_on_schema_mismatch(
        monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    """Unknown response shape must fail closed (not silently treated as success)."""
    pub, common = _import_modules()

    def fake_publish(**_kwargs):
        return {"some_other_field": "value"}

    monkeypatch.setattr(common, "publish_to_channel_via_adapter", fake_publish)

    rc = pub.main([
        "--conversation-id", "C0CHAN01",
        "--session", "gc-1",
        "--body", "x",
    ])
    assert rc == 1
    err = capsys.readouterr().err
    assert "schema_mismatch" in err


def test_publish_to_channel_requires_workspace_id(
        monkeypatch: pytest.MonkeyPatch) -> None:
    pub, _ = _import_modules()
    monkeypatch.setenv("SLACK_WORKSPACE_ID", "")
    with pytest.raises(SystemExit) as exc:
        pub.main([
            "--conversation-id", "C0CHAN01",
            "--session", "gc-1",
            "--body", "x",
        ])
    assert "SLACK_WORKSPACE_ID" in str(exc.value)


def test_publish_to_channel_rejects_both_body_and_body_file() -> None:
    pub, _ = _import_modules()
    with pytest.raises(SystemExit) as exc:
        pub.main([
            "--conversation-id", "C0CHAN01",
            "--session", "gc-1",
            "--body", "a",
            "--body-file", "/dev/null",
        ])
    assert "OR" in str(exc.value)


# --------------------------------------------------------------------------
# Accidental-mrkdwn guard (gp-o42) — tilde pairs must not strike through.
# --------------------------------------------------------------------------

def test_publish_to_channel_guards_tildes_by_default(
        monkeypatch: pytest.MonkeyPatch) -> None:
    pub, common = _import_modules()
    import slack_mrkdwn
    captured: dict[str, Any] = {}

    def fake_publish(**kwargs):
        captured.update(kwargs)
        return {"delivered": True, "message_id": "1700.001"}

    monkeypatch.setattr(common, "publish_to_channel_via_adapter", fake_publish)

    rc = pub.main([
        "--conversation-id", "C0CHAN01",
        "--session", "gc-1",
        "--body", "~$58.5k out, ~$16.5k left",
    ])
    assert rc == 0
    assert captured["text"] == \
        "~$58.5k out, ~$16.5k left".replace("~", slack_mrkdwn.TILDE_SUBSTITUTE)


def test_publish_to_channel_raw_flag_skips_guard(
        monkeypatch: pytest.MonkeyPatch) -> None:
    pub, common = _import_modules()
    captured: dict[str, Any] = {}

    def fake_publish(**kwargs):
        captured.update(kwargs)
        return {"delivered": True, "message_id": "1700.001"}

    monkeypatch.setattr(common, "publish_to_channel_via_adapter", fake_publish)

    rc = pub.main([
        "--conversation-id", "C0CHAN01",
        "--session", "gc-1",
        "--body", "~$58.5k out, ~$16.5k left",
        "--raw",
    ])
    assert rc == 0
    assert captured["text"] == "~$58.5k out, ~$16.5k left"
