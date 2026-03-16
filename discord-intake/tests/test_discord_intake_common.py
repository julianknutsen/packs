from __future__ import annotations

import pathlib
import tempfile
import unittest
from unittest import mock

import os
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "scripts"))

import discord_intake_common as common


class DiscordIntakeCommonTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self._old_environ = os.environ.copy()
        os.environ["GC_CITY_ROOT"] = self.tempdir.name

    def tearDown(self) -> None:
        os.environ.clear()
        os.environ.update(self._old_environ)

    def test_build_command_payload_registers_gc_fix(self) -> None:
        payload = common.build_command_payload("gc")

        self.assertEqual(payload[0]["name"], "gc")
        self.assertNotIn("contexts", payload[0])
        self.assertNotIn("integration_types", payload[0])
        self.assertNotIn("default_member_permissions", payload[0])
        self.assertEqual(payload[0]["options"][0]["name"], "fix")
        self.assertEqual(payload[0]["options"][0]["options"][0]["name"], "prompt")

    def test_build_global_command_payload_adds_global_only_fields(self) -> None:
        payload = common.build_command_payload("gc", scope="global")

        self.assertEqual(payload[0]["contexts"], [0])
        self.assertEqual(payload[0]["integration_types"], [0])

    def test_import_app_config_redacts_bot_token_presence(self) -> None:
        config = common.import_app_config(
            common.load_config(),
            {
                "application_id": "123",
                "public_key": "ab" * 32,
                "command_name": "gc",
                "guild_allowlist": ["1"],
            },
        )
        common.save_bot_token("discord-bot-token")

        redacted = common.redact_config(config)

        self.assertEqual(redacted["app"]["application_id"], "123")
        self.assertEqual(redacted["app"]["public_key"], "ab" * 32)
        self.assertTrue(redacted["app"]["bot_token_present"])
        self.assertEqual(redacted["policy"]["guild_allowlist"], ["1"])

    def test_set_channel_mapping_persists_fix_formula(self) -> None:
        config = common.set_channel_mapping(common.load_config(), "1", "2", "product/polecat", "mol-discord-fix-issue")

        mapping = common.resolve_channel_mapping(config, "1", "2")

        self.assertIsNotNone(mapping)
        assert mapping is not None
        self.assertEqual(mapping["target"], "product/polecat")
        self.assertEqual(mapping["commands"]["fix"]["formula"], "mol-discord-fix-issue")

    def test_load_channel_context_uses_parent_mapping_for_threads(self) -> None:
        config = common.set_channel_mapping(common.load_config(), "1", "22", "product/polecat", "mol-discord-fix-issue")

        with mock.patch.object(common, "load_bot_token", return_value="bot-token"), mock.patch.object(
            common,
            "discord_api_request",
            return_value={"id": "33", "parent_id": "22", "type": 11},
        ):
            context = common.load_channel_context(config, "1", "33")

        self.assertEqual(context["parent_channel_id"], "22")
        self.assertEqual(context["thread_id"], "33")
        self.assertEqual(context["mapping"]["target"], "product/polecat")

    def test_load_channel_context_prefers_parent_hint_without_discord_lookup(self) -> None:
        config = common.set_channel_mapping(common.load_config(), "1", "22", "product/polecat", "mol-discord-fix-issue")

        with mock.patch.object(common, "discord_api_request") as discord_api_request:
            context = common.load_channel_context(config, "1", "33", "22")

        self.assertEqual(context["parent_channel_id"], "22")
        self.assertEqual(context["thread_id"], "33")
        self.assertEqual(context["mapping"]["target"], "product/polecat")
        discord_api_request.assert_not_called()

    def test_sync_guild_commands_omits_global_only_fields(self) -> None:
        config = common.import_app_config(common.load_config(), {"application_id": "123", "public_key": "ab" * 32})

        with mock.patch.object(common, "discord_api_request", return_value={"ok": True}) as discord_api_request:
            common.sync_guild_commands(config, "55")

        payload = discord_api_request.call_args.kwargs["payload"]
        self.assertEqual(payload[0]["name"], "gc")
        self.assertNotIn("contexts", payload[0])
        self.assertNotIn("integration_types", payload[0])
        self.assertNotIn("default_member_permissions", payload[0])

    def test_save_interaction_receipt_is_unique(self) -> None:
        first = common.save_interaction_receipt("abc", {"response_kind": "accepted", "request_id": "dc-1"})
        second = common.save_interaction_receipt("abc", {"response_kind": "accepted", "request_id": "dc-1"})

        self.assertTrue(first)
        self.assertFalse(second)
        self.assertEqual(common.load_interaction_receipt("abc")["request_id"], "dc-1")

    def test_verify_discord_signature_returns_true_when_openssl_verifies(self) -> None:
        with mock.patch.object(common.subprocess, "run", return_value=mock.Mock(returncode=0)):
            verified = common.verify_discord_signature("ab" * 32, "1700000000", b"{}", "cd" * 64)

        self.assertTrue(verified)

    def test_verify_discord_signature_rejects_invalid_hex(self) -> None:
        verified = common.verify_discord_signature("not-hex", "1700000000", b"{}", "cd" * 64)

        self.assertFalse(verified)


if __name__ == "__main__":
    unittest.main()
