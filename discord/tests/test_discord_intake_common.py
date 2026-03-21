from __future__ import annotations

import io
import pathlib
import socket
import tempfile
import threading
import time
import urllib.error
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
        self.assertEqual(payload[0]["options"][0]["options"][0]["name"], "rig")
        self.assertEqual(payload[0]["options"][0]["options"][1]["name"], "prompt")

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

    def test_import_app_config_rejects_invalid_public_key(self) -> None:
        with self.assertRaisesRegex(ValueError, "public_key must be valid 32-byte hex"):
            common.import_app_config(
                common.load_config(),
                {
                    "application_id": "123",
                    "public_key": "not-hex",
                },
            )

    def test_set_channel_mapping_persists_fix_formula(self) -> None:
        config = common.set_channel_mapping(common.load_config(), "1", "2", "product/polecat", "mol-discord-fix-issue")

        mapping = common.resolve_channel_mapping(config, "1", "2")

        self.assertIsNotNone(mapping)
        assert mapping is not None
        self.assertEqual(mapping["target"], "product/polecat")
        self.assertEqual(mapping["commands"]["fix"]["formula"], "mol-discord-fix-issue")

    def test_set_channel_mapping_rejects_non_polecat_target_for_default_formula(self) -> None:
        with self.assertRaisesRegex(ValueError, "requires a rig/polecat sling target"):
            common.set_channel_mapping(common.load_config(), "1", "2", "product/witness", "mol-discord-fix-issue")

    def test_set_channel_mapping_allows_non_polecat_target_for_custom_formula(self) -> None:
        config = common.set_channel_mapping(common.load_config(), "1", "2", "product/witness", "custom-fix-formula")

        mapping = common.resolve_channel_mapping(config, "1", "2")

        self.assertIsNotNone(mapping)
        assert mapping is not None
        self.assertEqual(mapping["target"], "product/witness")
        self.assertEqual(mapping["commands"]["fix"]["formula"], "custom-fix-formula")

    def test_set_chat_binding_persists_room_binding(self) -> None:
        config = common.set_chat_binding(common.load_config(), "room", "22", ["sky", "lawrence"], guild_id="1")

        binding = common.resolve_chat_binding(config, "room:22")

        self.assertIsNotNone(binding)
        assert binding is not None
        self.assertEqual(binding["guild_id"], "1")
        self.assertEqual(binding["session_names"], ["sky", "lawrence"])

    def test_set_chat_binding_deduplicates_participants_case_insensitively(self) -> None:
        config = common.set_chat_binding(common.load_config(), "room", "22", ["sky", "Sky", "lawrence"], guild_id="1")

        binding = common.resolve_chat_binding(config, "room:22")

        self.assertIsNotNone(binding)
        assert binding is not None
        self.assertEqual(binding["session_names"], ["sky", "lawrence"])

    def test_set_chat_binding_rejects_dm_fanout(self) -> None:
        with self.assertRaisesRegex(ValueError, "exactly one session name"):
            common.set_chat_binding(common.load_config(), "dm", "22", ["sky", "lawrence"])

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

    def test_load_channel_context_surfaces_non_404_lookup_errors(self) -> None:
        config = common.set_channel_mapping(common.load_config(), "1", "22", "product/polecat", "mol-discord-fix-issue")

        with mock.patch.object(common, "load_bot_token", return_value="bot-token"), mock.patch.object(
            common,
            "discord_api_request",
            side_effect=common.DiscordAPIError("GET failed", status_code=500),
        ):
            context = common.load_channel_context(config, "1", "33")

        self.assertEqual(context["lookup_error"], "GET failed")

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

    def test_replace_interaction_receipt_overwrites_existing_payload(self) -> None:
        common.save_interaction_receipt("abc", {"response_kind": "modal", "modal_nonce": "nonce-1"})

        common.replace_interaction_receipt("abc", {"response_kind": "accepted", "request_id": "dc-1"})

        receipt = common.load_interaction_receipt("abc")
        self.assertEqual(receipt["response_kind"], "accepted")
        self.assertEqual(receipt["request_id"], "dc-1")

    def test_load_interaction_receipt_ignores_invalid_json(self) -> None:
        pathlib.Path(common.receipt_path("broken")).parent.mkdir(parents=True, exist_ok=True)
        pathlib.Path(common.receipt_path("broken")).write_text("{", encoding="utf-8")

        self.assertIsNone(common.load_interaction_receipt("broken"))

    def test_load_request_ignores_invalid_json(self) -> None:
        pathlib.Path(common.request_path("broken")).parent.mkdir(parents=True, exist_ok=True)
        pathlib.Path(common.request_path("broken")).write_text("{", encoding="utf-8")

        self.assertIsNone(common.load_request("broken"))

    def test_set_rig_mapping_persists_fix_formula(self) -> None:
        config = common.set_rig_mapping(common.load_config(), "1", "mission-control", "mission-control/polecat", "mol-discord-fix-issue")

        mapping = common.resolve_rig_mapping(config, "1", "mission-control")

        self.assertIsNotNone(mapping)
        assert mapping is not None
        self.assertEqual(mapping["target"], "mission-control/polecat")
        self.assertEqual(mapping["rig_name"], "mission-control")
        self.assertEqual(mapping["commands"]["fix"]["formula"], "mol-discord-fix-issue")

    def test_normalize_config_preserves_distinct_mixed_case_rig_entries(self) -> None:
        config = common.normalize_config(
            {
                "rigs": {
                    "1/Mission-Control": {
                        "guild_id": "1",
                        "rig_name": "Mission-Control",
                        "target": "mission-control/polecat",
                        "commands": {"fix": {"formula": "mol-discord-fix-issue"}},
                    },
                    "1/mission-control": {
                        "guild_id": "1",
                        "rig_name": "mission-control",
                        "target": "product/polecat",
                        "commands": {"fix": {"formula": "mol-discord-fix-issue"}},
                    }
                }
            }
        )

        self.assertIn("1/Mission-Control", config["rigs"])
        self.assertIn("1/mission-control", config["rigs"])
        self.assertEqual(config["rigs"]["1/Mission-Control"]["target"], "mission-control/polecat")
        self.assertEqual(config["rigs"]["1/mission-control"]["target"], "product/polecat")

    def test_build_command_payload_includes_rig_option(self) -> None:
        payload = common.build_command_payload("gc")

        fix_options = payload[0]["options"][0]["options"]
        rig_opt = next((o for o in fix_options if o["name"] == "rig"), None)
        self.assertIsNotNone(rig_opt)
        self.assertFalse(rig_opt["required"])
        self.assertEqual(rig_opt["type"], 3)

    def test_verify_discord_signature_returns_true_when_openssl_verifies(self) -> None:
        with mock.patch.object(common.subprocess, "run", return_value=mock.Mock(returncode=0)):
            verified = common.verify_discord_signature("ab" * 32, "1700000000", b"{}", "cd" * 64)

        self.assertTrue(verified)

    def test_verify_discord_signature_rejects_invalid_hex(self) -> None:
        verified = common.verify_discord_signature("not-hex", "1700000000", b"{}", "cd" * 64)

        self.assertFalse(verified)

    def test_post_channel_message_adds_reply_reference(self) -> None:
        with mock.patch.object(common, "discord_api_request", return_value={"id": "msg-1"}) as discord_api_request:
            response = common.post_channel_message("22", "hello", reply_to_message_id="99")

        self.assertEqual(response["id"], "msg-1")
        payload = discord_api_request.call_args.kwargs["payload"]
        self.assertEqual(payload["message_reference"]["message_id"], "99")
        self.assertFalse(payload["message_reference"]["fail_if_not_exists"])
        self.assertEqual(payload["allowed_mentions"]["parse"], [])

    def test_discord_jump_url_rejects_non_numeric_ids(self) -> None:
        self.assertEqual(common.discord_jump_url("guild", "22"), "")
        self.assertEqual(common.discord_jump_url("1", "thread"), "")
        self.assertEqual(common.discord_jump_url("1", "22"), "https://discord.com/channels/1/22")

    def test_gc_api_base_url_uses_city_toml_bind_and_port(self) -> None:
        pathlib.Path(self.tempdir.name, "city.toml").write_text('[api]\nbind = "0.0.0.0"\nport = 9555\n', encoding="utf-8")

        self.assertEqual(common.gc_api_base_url(), "http://127.0.0.1:9555")

    def test_gc_api_base_url_uses_ipv6_loopback_for_unspecified_ipv6_bind(self) -> None:
        pathlib.Path(self.tempdir.name, "city.toml").write_text('[api]\nbind = "::"\nport = 9555\n', encoding="utf-8")

        self.assertEqual(common.gc_api_base_url(), "http://[::1]:9555")

    def test_gc_api_base_url_honors_env_override(self) -> None:
        pathlib.Path(self.tempdir.name, "city.toml").write_text('[api]\nbind = "0.0.0.0"\nport = 9555\n', encoding="utf-8")
        os.environ["GC_API_BASE_URL"] = "http://override.test:1234/"

        self.assertEqual(common.gc_api_base_url(), "http://override.test:1234")

    def test_gc_api_base_url_rejects_disabled_port(self) -> None:
        pathlib.Path(self.tempdir.name, "city.toml").write_text('[api]\nport = 0\n', encoding="utf-8")

        with self.assertRaisesRegex(common.GCAPIError, "gc api is disabled"):
            common.gc_api_base_url()

    def test_prepare_service_socket_rejects_active_listener(self) -> None:
        socket_path = pathlib.Path(self.tempdir.name, "discord.sock")
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(socket_path))
        listener.listen(1)
        self.addCleanup(listener.close)
        self.addCleanup(lambda: socket_path.exists() and socket_path.unlink())

        with self.assertRaisesRegex(RuntimeError, "refusing to replace active service socket"):
            common.prepare_service_socket(str(socket_path))

    def test_prepare_service_socket_removes_stale_socket_file(self) -> None:
        socket_path = pathlib.Path(self.tempdir.name, "discord-stale.sock")
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(socket_path))
        listener.listen(1)
        listener.close()
        self.addCleanup(lambda: socket_path.exists() and socket_path.unlink())

        common.prepare_service_socket(str(socket_path))

        self.assertFalse(socket_path.exists())

    def test_save_chat_publish_lists_recent_records(self) -> None:
        common.save_chat_publish({"publish_id": "pub-1", "binding_id": "room:22"})

        recent = common.list_recent_chat_publishes(limit=5)

        self.assertEqual(len(recent), 1)
        self.assertEqual(recent[0]["publish_id"], "pub-1")

    def test_prune_chat_publishes_removes_expired_records(self) -> None:
        common.save_chat_publish({"publish_id": "pub-old", "binding_id": "room:22"})
        path = common.chat_publish_path("pub-old")
        expired = time.time() - common.CHAT_PUBLISH_RETENTION_SECONDS - 10
        os.utime(path, (expired, expired))

        common.prune_chat_publishes()

        recent = common.list_recent_chat_publishes(limit=5)
        self.assertEqual(recent, [])

    def test_save_chat_ingress_if_absent_only_claims_once(self) -> None:
        payload = {"ingress_id": "in-claim", "status": "processing"}
        barrier = threading.Barrier(2)
        results: list[tuple[bool, dict[str, object]]] = []

        def claim() -> None:
            barrier.wait()
            results.append(common.save_chat_ingress_if_absent(payload))

        threads = [threading.Thread(target=claim), threading.Thread(target=claim)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(sum(1 for created, _ in results if created), 1)
        self.assertEqual(sum(1 for created, _ in results if not created), 1)

    def test_save_chat_ingress_if_absent_marks_unreadable_claim_conflict(self) -> None:
        path = common.chat_ingress_path("in-broken")
        pathlib.Path(path).parent.mkdir(parents=True, exist_ok=True)
        pathlib.Path(path).write_text("", encoding="utf-8")

        created, receipt = common.save_chat_ingress_if_absent({"ingress_id": "in-broken", "status": "processing"})

        self.assertFalse(created)
        self.assertEqual(receipt["status"], "claim_conflict_unreadable")
        self.assertEqual(receipt["reason"], "ingress_claim_unreadable")

    def test_build_status_snapshot_redacts_chat_content(self) -> None:
        common.save_request(
            {
                "request_id": "dc-1",
                "summary": "secret bug",
                "context_markdown": "trace here",
                "invoking_user_display_name": "alice",
                "error_message": "boom",
                "traceback": "stack",
            }
        )
        common.save_gateway_status({"last_message_preview": "peek", "last_error": "boom"})
        common.save_chat_ingress(
            {
                "ingress_id": "in-1",
                "from_display": "alice",
                "from_user_id": "u-1",
                "body_preview": "super secret body",
                "status": "delivered",
            }
        )
        common.save_chat_publish(
            {
                "publish_id": "pub-1",
                "binding_id": "room:22",
                "body": "internal reply",
            }
        )

        snapshot = common.build_status_snapshot(limit=5)

        self.assertEqual(snapshot["recent_requests"][0]["summary"], "[redacted]")
        self.assertEqual(snapshot["recent_requests"][0]["context_markdown"], "[redacted]")
        self.assertEqual(snapshot["recent_requests"][0]["invoking_user_display_name"], "[redacted]")
        self.assertEqual(snapshot["recent_requests"][0]["error_message"], "[redacted]")
        self.assertEqual(snapshot["recent_requests"][0]["traceback"], "[redacted]")
        self.assertEqual(snapshot["gateway_status"]["last_message_preview"], "[redacted]")
        self.assertEqual(snapshot["gateway_status"]["last_error"], "[redacted]")
        self.assertEqual(snapshot["recent_chat_ingress"][0]["from_display"], "[redacted]")
        self.assertEqual(snapshot["recent_chat_ingress"][0]["from_user_id"], "[redacted]")
        self.assertEqual(snapshot["recent_chat_ingress"][0]["body_preview"], "[redacted]")
        self.assertEqual(snapshot["recent_chat_publishes"][0]["body"], "[redacted]")

    def test_list_recent_requests_skips_invalid_json_files(self) -> None:
        common.save_request({"request_id": "dc-valid"})
        pathlib.Path(common.request_path("dc-bad")).write_text("{", encoding="utf-8")

        requests = common.list_recent_requests(limit=5)

        self.assertEqual([item["request_id"] for item in requests], ["dc-valid"])

    def test_prune_requests_removes_expired_records(self) -> None:
        common.save_request({"request_id": "dc-old"})
        path = common.request_path("dc-old")
        expired = time.time() - common.REQUEST_RETENTION_SECONDS - 10
        os.utime(path, (expired, expired))

        common.prune_requests()

        self.assertEqual(common.list_recent_requests(limit=5), [])

    def test_prune_requests_keeps_records_with_active_workflow_links(self) -> None:
        common.save_request({"request_id": "dc-active"})
        common.save_workflow_link("dc:guild:1:conversation:22:fix", "dc-active")
        path = common.request_path("dc-active")
        expired = time.time() - common.REQUEST_RETENTION_SECONDS - 10
        os.utime(path, (expired, expired))

        common.prune_requests()

        self.assertIsNotNone(common.load_request("dc-active"))

    def test_discord_api_request_retries_after_rate_limit(self) -> None:
        rate_limited = urllib.error.HTTPError(
            "https://discord.test/api",
            429,
            "Too Many Requests",
            {"Retry-After": "0"},
            io.BytesIO(b'{"retry_after": 0}'),
        )
        success = mock.Mock()
        success.__enter__ = mock.Mock(return_value=mock.Mock(read=mock.Mock(return_value=b'{"ok": true}')))
        success.__exit__ = mock.Mock(return_value=False)

        with mock.patch.object(common.urllib.request, "urlopen", side_effect=[rate_limited, success]) as urlopen, mock.patch.object(
            common.time,
            "sleep",
        ) as sleep:
            payload = common.discord_api_request("GET", "/channels/1", bot_token="token")

        self.assertEqual(payload, {"ok": True})
        self.assertEqual(urlopen.call_count, 2)
        sleep.assert_called_once_with(0.0)


if __name__ == "__main__":
    unittest.main()
