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
        self.assertEqual(binding["policy"], common.default_room_peer_policy())

    def test_set_chat_binding_deduplicates_participants_case_insensitively(self) -> None:
        config = common.set_chat_binding(common.load_config(), "room", "22", ["sky", "Sky", "lawrence"], guild_id="1")

        binding = common.resolve_chat_binding(config, "room:22")

        self.assertIsNotNone(binding)
        assert binding is not None
        self.assertEqual(binding["session_names"], ["sky", "lawrence"])

    def test_set_chat_binding_rejects_dm_fanout(self) -> None:
        with self.assertRaisesRegex(ValueError, "exactly one session name"):
            common.set_chat_binding(common.load_config(), "dm", "22", ["sky", "lawrence"])

    def test_set_room_launcher_persists_room_launcher(self) -> None:
        config = common.set_room_launcher(common.load_config(), "1", "22")

        launcher = common.resolve_room_launcher(config, "22")

        self.assertIsNotNone(launcher)
        assert launcher is not None
        self.assertEqual(launcher["id"], "launch-room:22")
        self.assertEqual(launcher["response_mode"], "mention_only")

    def test_set_chat_binding_rejects_room_with_launcher(self) -> None:
        config = common.set_room_launcher(common.load_config(), "1", "22")

        with self.assertRaisesRegex(ValueError, "room launch is already enabled"):
            common.set_chat_binding(config, "room", "22", ["sky"], guild_id="1")

    def test_set_room_launcher_rejects_direct_binding_conflict(self) -> None:
        config = common.set_chat_binding(common.load_config(), "room", "22", ["sky"], guild_id="1")

        with self.assertRaisesRegex(ValueError, "directly bound room"):
            common.set_room_launcher(config, "1", "22")

    def test_set_room_launcher_rejects_unqualified_default_handle(self) -> None:
        with self.assertRaisesRegex(ValueError, "qualified rig/alias syntax"):
            common.set_room_launcher(common.load_config(), "1", "22", response_mode="respond_all", default_qualified_handle="sky")

    def test_set_chat_binding_persists_room_peer_policy(self) -> None:
        config = common.set_chat_binding(
            common.load_config(),
            "room",
            "22",
            ["corp--sky", "corp--priya"],
            guild_id="1",
            policy={
                "ambient_read_enabled": True,
                "peer_fanout_enabled": True,
                "allow_untargeted_peer_fanout": True,
                "max_peer_triggered_publishes_per_root": 2,
                "max_total_peer_deliveries_per_root": 9,
                "max_peer_triggered_publishes_per_session_per_minute": 7,
            },
        )

        binding = common.resolve_chat_binding(config, "room:22")

        assert binding is not None
        self.assertTrue(binding["policy"]["ambient_read_enabled"])
        self.assertTrue(binding["policy"]["peer_fanout_enabled"])
        self.assertTrue(binding["policy"]["allow_untargeted_peer_fanout"])
        self.assertEqual(binding["policy"]["max_peer_triggered_publishes_per_root"], 2)
        self.assertEqual(binding["policy"]["max_total_peer_deliveries_per_root"], 9)
        self.assertEqual(binding["policy"]["max_peer_triggered_publishes_per_session_per_minute"], 7)

    def test_set_chat_binding_persists_room_channel_metadata(self) -> None:
        config = common.set_chat_binding(
            common.load_config(),
            "room",
            "222",
            ["sky"],
            guild_id="1",
            channel_metadata={"channel_type": 11, "thread_parent_id": "22"},
        )

        binding = common.resolve_chat_binding(config, "room:222")

        assert binding is not None
        self.assertEqual(binding["channel_type"], 11)
        self.assertEqual(binding["thread_parent_id"], "22")

    def test_set_chat_binding_merges_existing_room_peer_policy(self) -> None:
        config = common.set_chat_binding(
            common.load_config(),
            "room",
            "22",
            ["corp--sky", "corp--priya"],
            guild_id="1",
            policy={"ambient_read_enabled": True, "peer_fanout_enabled": True, "allow_untargeted_peer_fanout": True},
        )
        config = common.set_chat_binding(
            config,
            "room",
            "22",
            ["corp--sky", "corp--priya"],
            guild_id="1",
            policy={"allow_untargeted_peer_fanout": False},
        )

        binding = common.resolve_chat_binding(config, "room:22")

        assert binding is not None
        self.assertTrue(binding["policy"]["ambient_read_enabled"])
        self.assertTrue(binding["policy"]["peer_fanout_enabled"])
        self.assertFalse(binding["policy"]["allow_untargeted_peer_fanout"])

    def test_set_chat_binding_rejects_noncanonical_names_when_peer_fanout_enabled(self) -> None:
        with self.assertRaisesRegex(ValueError, "lowercase canonical session names"):
            common.set_chat_binding(
                common.load_config(),
                "room",
                "22",
                ["Corp--Sky", "corp--priya"],
                guild_id="1",
                policy={"peer_fanout_enabled": True},
            )

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

    def test_describe_room_channel_metadata_normalizes_threads(self) -> None:
        with mock.patch.object(common, "discord_api_request", return_value={"id": "33", "parent_id": "22", "type": 11}):
            metadata = common.describe_room_channel_metadata("33", bot_token="bot-token")

        self.assertEqual(metadata, {"channel_type": 11, "thread_parent_id": "22"})

    def test_describe_room_channel_metadata_strips_parent_for_non_threads(self) -> None:
        with mock.patch.object(common, "discord_api_request", return_value={"id": "22", "parent_id": "77", "type": 0}):
            metadata = common.describe_room_channel_metadata("22", bot_token="bot-token")

        self.assertEqual(metadata, {"channel_type": 0})

    def test_save_channel_metadata_cache_round_trips_normalized_metadata(self) -> None:
        metadata = common.save_channel_metadata_cache("22", {"type": 11, "parent_id": "7"})

        self.assertEqual(metadata, {"channel_type": 11, "thread_parent_id": "7"})
        self.assertEqual(common.load_channel_metadata_cache("22"), {"channel_type": 11, "thread_parent_id": "7"})

    def test_load_channel_metadata_cache_ignores_invalid_payload(self) -> None:
        common.ensure_layout()
        pathlib.Path(common.channel_metadata_cache_path("22")).write_text("{not valid json", encoding="utf-8")

        self.assertEqual(common.load_channel_metadata_cache("22"), {})

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

    def test_extract_agent_handles_finds_bare_and_qualified_handles(self) -> None:
        handles = common.extract_agent_handles("hello @@sky and @@corp/priya")

        self.assertEqual(handles, ["sky", "corp/priya"])

    def test_extract_agent_handles_is_case_insensitive(self) -> None:
        handles = common.extract_agent_handles("hello @@Sky and @@Corp/Priya")

        self.assertEqual(handles, ["sky", "corp/priya"])

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

    def test_gc_api_base_url_prefers_supervisor_api_when_available(self) -> None:
        pathlib.Path(self.tempdir.name, "city.toml").write_text(
            '[workspace]\nname = "gc"\n[api]\nbind = "0.0.0.0"\nport = 9555\n',
            encoding="utf-8",
        )
        response = mock.Mock()
        response.__enter__ = mock.Mock(
            return_value=mock.Mock(read=mock.Mock(return_value=b'{"items":[{"name":"gc","running":true}]}'))
        )
        response.__exit__ = mock.Mock(return_value=False)

        with mock.patch.object(common.urllib.request, "urlopen", return_value=response) as urlopen:
            self.assertEqual(common.gc_api_base_url(), "http://127.0.0.1:8372")

        self.assertEqual(urlopen.call_count, 1)

    def test_gc_api_base_url_falls_back_when_supervisor_city_missing(self) -> None:
        pathlib.Path(self.tempdir.name, "city.toml").write_text(
            '[workspace]\nname = "gc"\n[api]\nbind = "0.0.0.0"\nport = 9555\n',
            encoding="utf-8",
        )
        response = mock.Mock()
        response.__enter__ = mock.Mock(
            return_value=mock.Mock(read=mock.Mock(return_value=b'{"items":[{"name":"other-city","running":true}]}'))
        )
        response.__exit__ = mock.Mock(return_value=False)

        with mock.patch.object(common.urllib.request, "urlopen", return_value=response):
            self.assertEqual(common.gc_api_base_url(), "http://127.0.0.1:9555")

    def test_gc_api_request_routes_through_supervisor_city_scope(self) -> None:
        pathlib.Path(self.tempdir.name, "city.toml").write_text(
            '[workspace]\nname = "gc"\n[api]\nbind = "0.0.0.0"\nport = 9555\n',
            encoding="utf-8",
        )
        cities = mock.Mock()
        cities.__enter__ = mock.Mock(
            return_value=mock.Mock(read=mock.Mock(return_value=b'{"items":[{"name":"gc","running":true}]}'))
        )
        cities.__exit__ = mock.Mock(return_value=False)
        sessions = mock.Mock()
        sessions.__enter__ = mock.Mock(return_value=mock.Mock(read=mock.Mock(return_value=b'{"items": []}')))
        sessions.__exit__ = mock.Mock(return_value=False)

        with mock.patch.object(common.urllib.request, "urlopen", side_effect=[cities, cities, sessions]) as urlopen:
            payload = common.gc_api_request("GET", "/v0/sessions")

        self.assertEqual(payload, {"items": []})
        self.assertEqual(urlopen.call_args_list[-1].args[0].full_url, "http://127.0.0.1:8372/v0/city/gc/sessions")

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

    def test_session_index_by_name_prefers_routable_duplicate(self) -> None:
        with mock.patch.object(
            common,
            "list_city_sessions",
            return_value=[
                {"session_name": "corp--sky", "state": "awake", "running": False, "created_at": "2026-03-18T07:55:10Z"},
                {"session_name": "corp--sky", "state": "", "running": False, "created_at": "2026-03-17T05:10:53Z"},
            ],
        ):
            index = common.session_index_by_name()

        self.assertEqual(index["corp--sky"]["state"], "awake")
        self.assertEqual(index["corp--sky"]["created_at"], "2026-03-18T07:55:10Z")

    def test_session_index_by_name_prefers_running_duplicate(self) -> None:
        with mock.patch.object(
            common,
            "list_city_sessions",
            return_value=[
                {"session_name": "corp--sky", "state": "awake", "running": False, "created_at": "2026-03-18T07:55:10Z"},
                {"session_name": "corp--sky", "state": "active", "running": True, "created_at": "2026-03-18T07:55:10Z"},
            ],
        ):
            index = common.session_index_by_name()

        self.assertEqual(index["corp--sky"]["state"], "active")
        self.assertTrue(index["corp--sky"]["running"])

    def test_find_latest_discord_reply_context_uses_latest_event(self) -> None:
        with mock.patch.object(
            common,
            "gc_api_request",
            return_value={
                "messages": [
                    {
                        "type": "user",
                        "message": {
                            "content": "<discord-event>\npublish_binding_id: dm:1\npublish_trigger_id: older\n</discord-event>"
                        },
                    },
                    {
                        "type": "user",
                        "message": {
                            "content": "<discord-event>\npublish_binding_id: dm:2\npublish_conversation_id: 22\npublish_trigger_id: newer\npublish_reply_to_discord_message_id: newer\n</discord-event>"
                        },
                    },
                ]
            },
        ) as gc_api_request:
            fields = common.find_latest_discord_reply_context("corp--sky", tail=5)

        gc_api_request.assert_called_once()
        self.assertEqual(fields["publish_binding_id"], "dm:2")
        self.assertEqual(fields["publish_conversation_id"], "22")
        self.assertEqual(fields["publish_trigger_id"], "newer")

    def test_extract_peer_session_mentions_ignores_urls_and_code(self) -> None:
        mentions = common.extract_peer_session_mentions(
            "\n".join(
                [
                    "Talk to @corp--priya please",
                    "Ignore https://example.test/@corp--eve here",
                    "`@corp--lawrence` stays code",
                    "> @corp--eve is quoted",
                    "@everyone should not route",
                ]
            )
        )

        self.assertEqual(mentions, ["corp--priya"])

    def test_extract_peer_session_mentions_ignores_double_backtick_code_spans(self) -> None:
        mentions = common.extract_peer_session_mentions("Talk to ``@corp--priya`` later")

        self.assertEqual(mentions, [])

    def test_extract_peer_session_mentions_ignores_discord_multiline_quotes(self) -> None:
        mentions = common.extract_peer_session_mentions(">>> quoted preface\n@corp--priya should stay quoted")

        self.assertEqual(mentions, [])

    def test_extract_peer_session_mentions_ignores_raw_discord_user_mentions(self) -> None:
        mentions = common.extract_peer_session_mentions("<@123456789012345678> @corp--priya")

        self.assertEqual(mentions, ["corp--priya"])

    def test_publish_binding_message_requires_remote_message_id(self) -> None:
        common.set_chat_binding(common.load_config(), "dm", "22", ["sky"])
        binding = common.resolve_chat_binding(common.load_config(), "dm:22")
        assert binding is not None

        with mock.patch.object(common, "post_channel_message", return_value={}):
            with self.assertRaisesRegex(common.DiscordAPIError, "returned no message id"):
                common.publish_binding_message(binding, "hello humans", trigger_id="orig-9")

    def test_publish_binding_message_room_launch_creates_thread_on_first_publish(self) -> None:
        config = common.set_room_launcher(common.load_config(), "1", "22")
        route = common.resolve_publish_route(config, "launch-room:22")
        assert route is not None
        common.save_room_launch(
            {
                "launch_id": "room-launch:orig-9",
                "launcher_id": "launch-room:22",
                "guild_id": "1",
                "conversation_id": "22",
                "root_message_id": "orig-9",
                "qualified_handle": "corp/sky",
                "session_alias": "dc-123-sky",
                "thread_id": "",
            }
        )

        with mock.patch.object(common, "create_thread_from_message", return_value={"id": "333"}) as create_thread_from_message, mock.patch.object(
            common, "post_channel_message", return_value={"id": "msg-1"}
        ) as post_channel_message:
            payload = common.publish_binding_message(
                route,
                "hello humans",
                trigger_id="orig-9",
                source_context={"kind": "discord_human_message", "publish_launch_id": "room-launch:orig-9"},
            )

        create_thread_from_message.assert_called_once_with("22", "orig-9", "sky")
        post_channel_message.assert_called_once_with("333", "hello humans", reply_to_message_id="")
        self.assertEqual(payload["record"]["conversation_id"], "333")
        self.assertEqual(payload["record"]["launch_id"], "room-launch:orig-9")
        self.assertEqual(common.load_room_launch("room-launch:orig-9")["thread_id"], "333")

    def test_publish_binding_message_records_room_launch_message_target(self) -> None:
        config = common.set_room_launcher(common.load_config(), "1", "22")
        route = common.resolve_publish_route(config, "launch-room:22")
        assert route is not None
        common.save_room_launch(
            {
                "launch_id": "room-launch:orig-11",
                "launcher_id": "launch-room:22",
                "guild_id": "1",
                "conversation_id": "22",
                "root_message_id": "orig-11",
                "qualified_handle": "corp/sky",
                "session_alias": "dc-123-sky",
                "session_name": "dc-sky",
                "participants": {
                    "corp/sky": {
                        "qualified_handle": "corp/sky",
                        "session_alias": "dc-123-sky",
                        "session_name": "dc-sky",
                        "session_id": "gc-sky",
                    }
                },
                "thread_id": "333",
            }
        )

        with mock.patch.object(common, "post_channel_message", return_value={"id": "msg-launch-11"}):
            payload = common.publish_binding_message(
                route,
                "hello humans",
                source_context={"kind": "discord_human_message", "publish_launch_id": "room-launch:orig-11"},
                source_session_name="dc-sky",
                source_session_id="gc-sky",
            )

        self.assertEqual(payload["record"]["remote_message_id"], "msg-launch-11")
        launch = common.load_room_launch("room-launch:orig-11")
        assert launch is not None
        self.assertEqual(launch["message_targets"]["msg-launch-11"], "corp/sky")

    def test_publish_binding_message_does_not_record_non_thread_launch_publish_target(self) -> None:
        common.set_chat_binding(common.load_config(), "room", "22", ["corp--sky"], guild_id="1")
        binding = common.resolve_chat_binding(common.load_config(), "room:22")
        assert binding is not None
        common.save_room_launch(
            {
                "launch_id": "room-launch:orig-12",
                "launcher_id": "launch-room:22",
                "guild_id": "1",
                "conversation_id": "22",
                "root_message_id": "orig-12",
                "qualified_handle": "corp/sky",
                "session_alias": "dc-123-sky",
                "session_name": "dc-sky",
                "thread_id": "333",
                "participants": {
                    "corp/sky": {
                        "qualified_handle": "corp/sky",
                        "session_alias": "dc-123-sky",
                        "session_name": "dc-sky",
                        "session_id": "gc-sky",
                    }
                },
            }
        )

        with mock.patch.object(common, "post_channel_message", return_value={"id": "msg-root-12"}):
            common.publish_binding_message(
                binding,
                "root-room note",
                source_context={"kind": "discord_human_message", "publish_launch_id": "room-launch:orig-12"},
                source_session_name="dc-sky",
                source_session_id="gc-sky",
            )

        launch = common.load_room_launch("room-launch:orig-12")
        assert launch is not None
        self.assertEqual(launch["message_targets"], {"orig-12": "corp/sky"})

    def test_touch_room_launch_sets_last_activity_at(self) -> None:
        common.save_room_launch(
            {
                "launch_id": "room-launch:activity",
                "launcher_id": "launch-room:22",
                "guild_id": "1",
                "conversation_id": "22",
                "root_message_id": "activity",
                "qualified_handle": "corp/sky",
                "session_alias": "dc-123-sky",
                "thread_id": "222",
            }
        )

        touched = common.touch_room_launch("room-launch:activity")

        assert touched is not None
        self.assertTrue(str(touched.get("last_activity_at", "")).strip())

    def test_prune_room_launches_keeps_recent_thread_routes(self) -> None:
        common.save_room_launch(
            {
                "launch_id": "room-launch:recent",
                "launcher_id": "launch-room:22",
                "guild_id": "1",
                "conversation_id": "22",
                "root_message_id": "recent",
                "qualified_handle": "corp/sky",
                "session_alias": "dc-123-sky",
                "thread_id": "222",
            }
        )
        path = common.room_launch_path("room-launch:recent")
        aged_but_recent = time.time() - common.CHAT_INGRESS_RETENTION_SECONDS - 10
        os.utime(path, (aged_but_recent, aged_but_recent))

        common.prune_room_launches()

        self.assertIsNotNone(common.load_room_launch("room-launch:recent"))

    def test_ensure_room_launch_session_recreates_non_routable_alias_match(self) -> None:
        launch = {
            "launch_id": "room-launch:revive",
            "qualified_handle": "corp/sky",
            "session_alias": "dc-123-sky",
            "from_display": "alice",
        }

        sessions_first = [
            {
                "id": "gc-old",
                "alias": "dc-123-sky",
                "session_name": "dc-old-sky",
                "state": "closed",
                "running": False,
                "created_at": "2026-03-20T00:00:00Z",
            }
        ]
        sessions_second = [
            {
                "id": "gc-new",
                "alias": "dc-123-sky",
                "session_name": "dc-new-sky",
                "state": "active",
                "running": True,
                "created_at": "2026-03-22T00:00:00Z",
            }
        ]
        calls = {"count": 0}

        def list_sessions(*, state: str = "all") -> list[dict[str, object]]:
            self.assertEqual(state, "all")
            calls["count"] += 1
            if calls["count"] == 1:
                return sessions_first
            return sessions_second

        with mock.patch.object(
            common,
            "list_city_sessions",
            side_effect=list_sessions,
        ), mock.patch.object(
            common,
            "create_agent_session",
            return_value={"id": "gc-new", "session_name": "dc-new-sky", "alias": "dc-123-sky"},
        ) as create_agent_session, mock.patch.object(common.time, "sleep"):
            current = common.ensure_room_launch_session(launch)

        create_agent_session.assert_called_once()
        self.assertEqual(calls["count"], 2)
        self.assertEqual(current["session_id"], "gc-new")
        self.assertEqual(current["session_name"], "dc-new-sky")

    def test_ensure_room_launch_session_hydrates_routable_identity_after_create(self) -> None:
        launch = {
            "launch_id": "room-launch:hydrate",
            "qualified_handle": "corp/sky",
            "session_alias": "dc-123-sky",
            "from_display": "alice",
        }

        sessions_first = [
            {
                "id": "gc-old",
                "alias": "dc-123-sky",
                "session_name": "dc-old-sky",
                "state": "closed",
                "running": False,
                "created_at": "2026-03-20T00:00:00Z",
            }
        ]
        sessions_second = [
            {
                "id": "gc-new",
                "alias": "dc-123-sky",
                "session_name": "dc-new-sky",
                "state": "active",
                "running": True,
                "created_at": "2026-03-22T00:00:00Z",
            }
        ]
        calls = {"count": 0}

        def list_sessions(*, state: str = "all") -> list[dict[str, object]]:
            self.assertEqual(state, "all")
            calls["count"] += 1
            if calls["count"] < 4:
                return sessions_first
            return sessions_second

        with mock.patch.object(
            common,
            "list_city_sessions",
            side_effect=list_sessions,
        ), mock.patch.object(
            common,
            "create_agent_session",
            return_value={"alias": "dc-123-sky"},
        ) as create_agent_session, mock.patch.object(common.time, "sleep"):
            current = common.ensure_room_launch_session(launch)

        create_agent_session.assert_called_once()
        self.assertEqual(calls["count"], 4)
        self.assertEqual(current["session_id"], "gc-new")
        self.assertEqual(current["session_name"], "dc-new-sky")

    def test_ensure_room_launch_session_hydrates_routable_identity_when_create_omits_alias(self) -> None:
        launch = {
            "launch_id": "room-launch:hydrate-no-alias",
            "qualified_handle": "corp/sky",
            "session_alias": "dc-123-sky",
            "from_display": "alice",
        }

        sessions_first: list[dict[str, object]] = []
        sessions_second = [
            {
                "id": "gc-new",
                "alias": "dc-123-sky",
                "session_name": "dc-new-sky",
                "state": "active",
                "running": True,
                "created_at": "2026-03-22T00:00:00Z",
            }
        ]
        calls = {"count": 0}

        def list_sessions(*, state: str = "all") -> list[dict[str, object]]:
            self.assertEqual(state, "all")
            calls["count"] += 1
            if calls["count"] < 5:
                return sessions_first
            return sessions_second

        with mock.patch.object(
            common,
            "list_city_sessions",
            side_effect=list_sessions,
        ), mock.patch.object(
            common,
            "create_agent_session",
            return_value={},
        ) as create_agent_session, mock.patch.object(common.time, "sleep"):
            current = common.ensure_room_launch_session(launch)

        create_agent_session.assert_called_once()
        self.assertEqual(calls["count"], 5)
        self.assertEqual(current["session_alias"], "dc-123-sky")
        self.assertEqual(current["session_id"], "gc-new")
        self.assertEqual(current["session_name"], "dc-new-sky")
        self.assertEqual(current["delivery_selector"], "dc-123-sky")

    def test_ensure_room_launch_session_waits_for_routable_identity_when_create_returns_name(self) -> None:
        launch = {
            "launch_id": "room-launch:hydrate-name-only",
            "qualified_handle": "corp/sky",
            "session_alias": "dc-123-sky",
            "from_display": "alice",
        }

        sessions_first: list[dict[str, object]] = []
        sessions_second = [
            {
                "id": "gc-new",
                "alias": "dc-123-sky",
                "session_name": "dc-new-sky",
                "state": "active",
                "running": True,
                "created_at": "2026-03-22T00:00:00Z",
            }
        ]
        calls = {"count": 0}

        def list_sessions(*, state: str = "all") -> list[dict[str, object]]:
            self.assertEqual(state, "all")
            calls["count"] += 1
            if calls["count"] < 5:
                return sessions_first
            return sessions_second

        with mock.patch.object(
            common,
            "list_city_sessions",
            side_effect=list_sessions,
        ), mock.patch.object(
            common,
            "create_agent_session",
            return_value={"id": "gc-new", "session_name": "dc-new-sky"},
        ) as create_agent_session, mock.patch.object(common.time, "sleep"):
            current = common.ensure_room_launch_session(launch)

        create_agent_session.assert_called_once()
        self.assertEqual(calls["count"], 5)
        self.assertEqual(current["session_alias"], "dc-123-sky")
        self.assertEqual(current["session_id"], "gc-new")
        self.assertEqual(current["session_name"], "dc-new-sky")
        self.assertEqual(current["delivery_selector"], "dc-123-sky")

    def test_ensure_room_launch_session_raises_when_created_identity_never_becomes_routable(self) -> None:
        launch = {
            "launch_id": "room-launch:stuck",
            "qualified_handle": "corp/sky",
            "session_alias": "dc-123-sky",
            "from_display": "alice",
        }

        with mock.patch.object(
            common,
            "list_city_sessions",
            return_value=[],
        ), mock.patch.object(
            common,
            "create_agent_session",
            return_value={"alias": "dc-123-sky"},
        ), mock.patch.object(common.time, "sleep"):
            with self.assertRaisesRegex(common.GCAPIError, "created launch session is not routable yet"):
                common.ensure_room_launch_session(launch)

    def test_ensure_room_launch_session_raises_when_create_returns_name_but_never_becomes_routable(self) -> None:
        launch = {
            "launch_id": "room-launch:stuck-name-only",
            "qualified_handle": "corp/sky",
            "session_alias": "dc-123-sky",
            "from_display": "alice",
        }

        with mock.patch.object(
            common,
            "list_city_sessions",
            return_value=[],
        ), mock.patch.object(
            common,
            "create_agent_session",
            return_value={"id": "gc-new", "session_name": "dc-new-sky"},
        ), mock.patch.object(common.time, "sleep"):
            with self.assertRaisesRegex(common.GCAPIError, "created launch session is not routable yet"):
                common.ensure_room_launch_session(launch)

    def test_ensure_room_launch_session_for_handle_creates_secondary_participant(self) -> None:
        common.save_room_launch(
            {
                "launch_id": "room-launch:thread-join",
                "launcher_id": "launch-room:22",
                "guild_id": "1",
                "conversation_id": "22",
                "root_message_id": "thread-join",
                "qualified_handle": "corp/sky",
                "session_alias": "dc-123-sky",
                "session_name": "dc-sky",
            }
        )

        sessions_first: list[dict[str, object]] = []
        sessions_second = [
            {
                "id": "gc-alex",
                "alias": "dc-456-alex",
                "session_name": "dc-alex",
                "state": "active",
                "running": True,
                "created_at": "2026-03-22T00:00:00Z",
            }
        ]
        calls = {"count": 0}

        def list_sessions(*, state: str = "all") -> list[dict[str, object]]:
            self.assertEqual(state, "all")
            calls["count"] += 1
            if calls["count"] == 1:
                return sessions_first
            return sessions_second

        with mock.patch.object(
            common,
            "list_city_sessions",
            side_effect=list_sessions,
        ), mock.patch.object(
            common,
            "create_agent_session",
            return_value={"id": "gc-alex", "session_name": "dc-alex", "alias": "dc-456-alex"},
        ) as create_agent_session, mock.patch.object(common.time, "sleep"):
            current, participant = common.ensure_room_launch_session_for_handle(
                common.load_room_launch("room-launch:thread-join") or {},
                "corp/alex",
            )

        create_agent_session.assert_called_once()
        self.assertEqual(calls["count"], 2)
        self.assertEqual(participant["session_name"], "dc-alex")
        self.assertEqual(current["participants"]["corp/alex"]["session_alias"], "dc-456-alex")

    def test_publish_binding_message_peer_fanout_delivers_targeted_peer_event(self) -> None:
        common.set_chat_binding(
            common.load_config(),
            "room",
            "22",
            ["corp--sky", "corp--priya"],
            guild_id="1",
            policy={"peer_fanout_enabled": True},
        )
        binding = common.resolve_chat_binding(common.load_config(), "room:22")
        assert binding is not None
        os.environ["GC_SESSION_NAME"] = "corp--sky"
        os.environ["GC_SESSION_ID"] = "gc-sky"

        with mock.patch.object(common, "post_channel_message", return_value={"id": "msg-1"}), mock.patch.object(
            common,
            "deliver_session_message",
            return_value={"status": "accepted", "id": "gc-priya"},
        ) as deliver_session_message, mock.patch.object(
            common,
            "list_city_sessions",
            return_value=[
                {"session_name": "corp--sky", "state": "active", "running": True, "created_at": "2026-03-21T00:00:00Z"},
                {"session_name": "corp--priya", "state": "active", "running": True, "created_at": "2026-03-21T00:00:00Z"},
            ],
        ):
            payload = common.publish_binding_message(
                binding,
                "@corp--priya hello",
                trigger_id="orig-9",
                source_context={
                    "kind": "discord_human_message",
                    "ingress_receipt_id": "in-9",
                    "publish_binding_id": "room:22",
                    "publish_conversation_id": "22",
                    "publish_trigger_id": "orig-9",
                    "publish_reply_to_discord_message_id": "orig-9",
                },
            )

        record = payload["record"]
        self.assertEqual(record["source_event_kind"], "discord_human_message")
        self.assertEqual(record["root_ingress_receipt_id"], "in-9")
        self.assertEqual(record["peer_delivery"]["status"], "delivered")
        self.assertEqual(record["peer_delivery"]["delivery"], "targeted")
        self.assertEqual(record["peer_delivery"]["mentioned_session_names"], ["corp--priya"])
        self.assertEqual(record["peer_delivery"]["frozen_targets"], ["corp--priya"])
        deliver_session_message.assert_called_once()
        self.assertIn("publish_conversation_id: 22", deliver_session_message.call_args.args[1])
        self.assertIn("kind: discord_peer_publication", deliver_session_message.call_args.args[1])

    def test_publish_binding_message_resolves_source_name_from_id_only_env(self) -> None:
        common.set_chat_binding(
            common.load_config(),
            "room",
            "22",
            ["corp--sky", "corp--priya"],
            guild_id="1",
            policy={"peer_fanout_enabled": True},
        )
        binding = common.resolve_chat_binding(common.load_config(), "room:22")
        assert binding is not None
        os.environ.pop("GC_SESSION_NAME", None)
        os.environ["GC_SESSION_ID"] = "gc-sky"

        with mock.patch.object(common, "post_channel_message", return_value={"id": "msg-1"}), mock.patch.object(
            common,
            "deliver_session_message",
            return_value={"status": "accepted", "id": "gc-priya"},
        ), mock.patch.object(
            common,
            "list_city_sessions",
            return_value=[
                {"id": "gc-sky", "session_name": "corp--sky", "state": "active", "running": True, "created_at": "2026-03-21T00:00:00Z"},
                {"id": "gc-priya", "session_name": "corp--priya", "state": "active", "running": True, "created_at": "2026-03-21T00:00:00Z"},
            ],
        ):
            payload = common.publish_binding_message(
                binding,
                "@corp--priya hello",
                trigger_id="orig-9",
                source_context={
                    "kind": "discord_human_message",
                    "ingress_receipt_id": "in-9",
                },
            )

        self.assertEqual(payload["record"]["source_session_name"], "corp--sky")
        self.assertEqual(payload["record"]["source_session_id"], "gc-sky")

    def test_publish_binding_message_peer_fanout_skips_without_root_context(self) -> None:
        common.set_chat_binding(
            common.load_config(),
            "room",
            "22",
            ["corp--sky", "corp--priya"],
            guild_id="1",
            policy={"peer_fanout_enabled": True},
        )
        binding = common.resolve_chat_binding(common.load_config(), "room:22")
        assert binding is not None
        os.environ["GC_SESSION_NAME"] = "corp--sky"
        os.environ["GC_SESSION_ID"] = "gc-sky"

        with mock.patch.object(common, "post_channel_message", return_value={"id": "msg-1"}), mock.patch.object(
            common,
            "deliver_session_message",
        ) as deliver_session_message:
            payload = common.publish_binding_message(binding, "@corp--priya hello", trigger_id="orig-9")

        self.assertEqual(payload["record"]["peer_delivery"]["status"], "skipped_missing_root_context")
        deliver_session_message.assert_not_called()

    def test_publish_binding_message_targeted_unavailable_records_retryable_targets(self) -> None:
        common.set_chat_binding(
            common.load_config(),
            "room",
            "22",
            ["corp--sky", "corp--priya", "corp--eve"],
            guild_id="1",
            policy={"peer_fanout_enabled": True},
        )
        binding = common.resolve_chat_binding(common.load_config(), "room:22")
        assert binding is not None
        os.environ["GC_SESSION_NAME"] = "corp--sky"
        os.environ["GC_SESSION_ID"] = "gc-sky"

        with mock.patch.object(common, "post_channel_message", return_value={"id": "msg-1"}), mock.patch.object(
            common,
            "deliver_session_message",
        ) as deliver_session_message, mock.patch.object(
            common,
            "list_city_sessions",
            return_value=[
                {"id": "gc-sky", "session_name": "corp--sky", "state": "active", "running": True, "created_at": "2026-03-21T00:00:00Z"},
                {"id": "gc-priya", "session_name": "corp--priya", "state": "active", "running": True, "created_at": "2026-03-21T00:00:00Z"},
                {"id": "gc-eve", "session_name": "corp--eve", "state": "closed", "running": False, "created_at": "2026-03-21T00:00:00Z"},
            ],
        ):
            payload = common.publish_binding_message(
                binding,
                "@corp--priya @corp--eve hello",
                trigger_id="orig-9",
                source_context={
                    "kind": "discord_human_message",
                    "ingress_receipt_id": "in-9",
                },
            )

        self.assertEqual(payload["record"]["peer_delivery"]["status"], "failed_targeting_unavailable")
        targets = {entry["session_name"]: entry for entry in payload["record"]["peer_delivery"]["targets"]}
        self.assertEqual(targets["corp--priya"]["status"], "failed_retryable")
        self.assertEqual(targets["corp--eve"]["status"], "failed_retryable")
        deliver_session_message.assert_not_called()

    def test_resolve_session_identity_prefers_routable_named_session(self) -> None:
        with mock.patch.object(
            common,
            "list_city_sessions",
            return_value=[
                {"id": "gc-old", "session_name": "corp--sky", "state": "", "running": False, "created_at": "2026-03-18T00:00:00Z"},
                {"id": "gc-new", "session_name": "corp--sky", "state": "active", "running": True, "created_at": "2026-03-19T00:00:00Z"},
            ],
        ):
            identity = common.resolve_session_identity("corp--sky")

        self.assertEqual(identity["session_name"], "corp--sky")
        self.assertEqual(identity["session_id"], "gc-new")

    def test_current_session_selector_falls_back_to_gc_alias(self) -> None:
        os.environ.pop("GC_SESSION_ID", None)
        os.environ.pop("GC_SESSION_NAME", None)
        os.environ["GC_ALIAS"] = "dc-123-sky"

        self.assertEqual(common.current_session_selector(), "dc-123-sky")

    def test_peer_root_budget_index_tracks_root_counts(self) -> None:
        common.save_chat_publish(
            {
                "publish_id": "discord-publish-1",
                "binding_id": "room:22",
                "root_ingress_receipt_id": "in-1",
                "source_session_name": "corp--sky",
                "source_event_kind": "discord_peer_publication",
                "created_at": "2026-03-21T00:00:00Z",
                "peer_delivery": {"frozen_targets": ["corp--priya", "corp--eve"]},
            }
        )
        common.save_chat_publish(
            {
                "publish_id": "discord-publish-2",
                "binding_id": "room:22",
                "root_ingress_receipt_id": "in-1",
                "source_session_name": "corp--sky",
                "source_event_kind": "discord_peer_publication",
                "created_at": "2026-03-21T00:01:00Z",
                "peer_delivery": {"frozen_targets": ["corp--lawrence"]},
            }
        )

        self.assertEqual(common._count_root_peer_triggered_publishes("room:22", "in-1", "corp--sky"), 2)
        self.assertEqual(common._count_root_peer_deliveries_from_index("room:22", "in-1"), 3)

    def test_retry_peer_fanout_redrives_failed_target_without_reposting(self) -> None:
        common.set_chat_binding(
            common.load_config(),
            "room",
            "22",
            ["corp--sky", "corp--priya"],
            guild_id="1",
            policy={"peer_fanout_enabled": True},
        )
        common.save_chat_publish(
            {
                "publish_id": "discord-publish-1",
                "binding_id": "room:22",
                "binding_kind": "room",
                "binding_conversation_id": "22",
                "conversation_id": "22",
                "guild_id": "1",
                "source_session_name": "corp--sky",
                "source_session_id": "gc-sky",
                "source_event_kind": "discord_human_message",
                "root_ingress_receipt_id": "in-9",
                "body": "@corp--priya hello",
                "remote_message_id": "msg-1",
                "peer_delivery": {
                    "phase": "peer_fanout_partial_failure",
                    "status": "partial_failure",
                    "delivery": "targeted",
                    "mentioned_session_names": ["corp--priya"],
                    "frozen_targets": ["corp--priya"],
                    "targets": [
                        {
                            "session_name": "corp--priya",
                            "status": "failed_retryable",
                            "attempt_count": 1,
                            "idempotency_key": "peer_publish:discord-publish-1:binding:room:22:target:corp--priya",
                            "attempts": [],
                        }
                    ],
                    "budget_snapshot": {},
                },
            }
        )

        with mock.patch.object(
            common,
            "deliver_session_message",
            return_value={"status": "accepted", "id": "gc-priya"},
        ) as deliver_session_message, mock.patch.object(common, "post_channel_message") as post_channel_message:
            record = common.retry_peer_fanout("discord-publish-1")

        self.assertEqual(record["peer_delivery"]["status"], "delivered")
        post_channel_message.assert_not_called()
        deliver_session_message.assert_called_once()

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
