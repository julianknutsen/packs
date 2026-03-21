from __future__ import annotations

import io
import pathlib
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest import mock

import os
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "scripts"))

import discord_chat_bind as bind_script
import discord_chat_publish as publish_script
import discord_chat_reply_current as reply_current_script
import discord_intake_common as common


class DiscordChatScriptTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self._old_environ = os.environ.copy()
        os.environ["GC_CITY_ROOT"] = self.tempdir.name

    def tearDown(self) -> None:
        os.environ.clear()
        os.environ.update(self._old_environ)

    def test_publish_uses_binding_target_and_saves_record(self) -> None:
        common.set_chat_binding(common.load_config(), "room", "22", ["sky"], guild_id="1")

        with mock.patch.object(common, "post_channel_message", return_value={"id": "msg-1"}) as post_channel_message:
            with redirect_stdout(io.StringIO()):
                code = publish_script.main(["--binding", "room:22", "--trigger", "orig-9", "--body", "hello humans"])

        self.assertEqual(code, 0)
        post_channel_message.assert_called_once_with("22", "hello humans", reply_to_message_id="orig-9")
        recent = common.list_recent_chat_publishes(limit=5)
        self.assertEqual(len(recent), 1)
        self.assertEqual(recent[0]["binding_id"], "room:22")
        self.assertEqual(recent[0]["remote_message_id"], "msg-1")

    def test_publish_allows_conversation_override_for_thread_replies(self) -> None:
        common.set_chat_binding(common.load_config(), "room", "22", ["sky"], guild_id="1")

        with mock.patch.object(common, "discord_api_request", return_value={"id": "222", "parent_id": "22"}), mock.patch.object(
            common, "post_channel_message", return_value={"id": "msg-2"}
        ) as post_channel_message:
            with redirect_stdout(io.StringIO()):
                code = publish_script.main(
                    [
                        "--binding",
                        "room:22",
                        "--conversation-id",
                        "222",
                        "--trigger",
                        "orig-10",
                        "--body",
                        "thread reply",
                    ]
                )

        self.assertEqual(code, 0)
        post_channel_message.assert_called_once_with("222", "thread reply", reply_to_message_id="orig-10")
        recent = common.list_recent_chat_publishes(limit=5)
        self.assertEqual(recent[0]["binding_conversation_id"], "22")
        self.assertEqual(recent[0]["conversation_id"], "222")

    def test_publish_rejects_cross_channel_override(self) -> None:
        common.set_chat_binding(common.load_config(), "room", "22", ["sky"], guild_id="1")

        with mock.patch.object(common, "discord_api_request", return_value={"id": "999", "parent_id": "77"}):
            with self.assertRaises(SystemExit) as exc:
                publish_script.main(["--binding", "room:22", "--conversation-id", "999", "--body", "nope"])

        self.assertEqual(str(exc.exception), "--conversation-id must be the bound room or a thread within it")

    def test_publish_rejects_missing_remote_message_id(self) -> None:
        common.set_chat_binding(common.load_config(), "room", "22", ["sky"], guild_id="1")

        with mock.patch.object(common, "post_channel_message", return_value={}):
            with self.assertRaises(SystemExit) as exc:
                publish_script.main(["--binding", "room:22", "--trigger", "orig-9", "--body", "hello humans"])

        self.assertEqual(str(exc.exception), "discord publish returned no message id")

    def test_reply_current_uses_latest_discord_context(self) -> None:
        common.set_chat_binding(common.load_config(), "dm", "22", ["sky"])
        os.environ["GC_SESSION_NAME"] = "sky"
        body_file = pathlib.Path(self.tempdir.name) / "reply.txt"
        body_file.write_text("safe reply", encoding="utf-8")

        with mock.patch.object(
            common,
            "find_latest_discord_reply_context",
            return_value={
                "publish_binding_id": "dm:22",
                "publish_conversation_id": "22",
                "publish_trigger_id": "orig-22",
                "publish_reply_to_discord_message_id": "orig-22",
            },
        ), mock.patch.object(common, "post_channel_message", return_value={"id": "msg-22"}) as post_channel_message:
            with redirect_stdout(io.StringIO()):
                code = reply_current_script.main(["--body-file", str(body_file)])

        self.assertEqual(code, 0)
        post_channel_message.assert_called_once_with("22", "safe reply", reply_to_message_id="orig-22")
        recent = common.list_recent_chat_publishes(limit=5)
        self.assertEqual(recent[0]["binding_id"], "dm:22")
        self.assertEqual(recent[0]["remote_message_id"], "msg-22")

    def test_bind_script_creates_room_binding(self) -> None:
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            code = bind_script.main(["--kind", "room", "--guild-id", "1", "22", "sky", "lawrence"])

        self.assertEqual(code, 0)
        binding = common.resolve_chat_binding(common.load_config(), "room:22")
        self.assertIsNotNone(binding)
        assert binding is not None
        self.assertEqual(binding["session_names"], ["sky", "lawrence"])

    def test_bind_script_rejects_invalid_dm_fanout_cleanly(self) -> None:
        with self.assertRaises(SystemExit) as exc:
            bind_script.main(["--kind", "dm", "55", "sky", "lawrence"])

        self.assertEqual(str(exc.exception), "DM bindings require exactly one session name")


if __name__ == "__main__":
    unittest.main()
