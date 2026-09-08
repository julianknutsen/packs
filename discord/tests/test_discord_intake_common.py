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

# GC_BIN and GC_API_BASE_URL: the assertions here expect the defaults the
# scripts fall back to (`gc`, and the supervisor's own base URL). A Gas City
# seat exports both, so nine of these tests fail for anyone running the suite
# from inside a city, and pass in CI only because CI sets neither. Each setUp
# pins the fallback rather than depending on the variables' absence.
SEAT_ENV_OVERRIDES = ("GC_BIN", "GC_API_BASE_URL")


class DiscordIntakeCommonTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self._old_environ = os.environ.copy()
        os.environ["GC_CITY_ROOT"] = self.tempdir.name
        for name in SEAT_ENV_OVERRIDES:
            os.environ.pop(name, None)

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

    def test_named_app_import_preserves_default_app_and_isolates_policy(self) -> None:
        config = common.import_app_config(
            common.load_config(),
            {
                "application_id": "123",
                "public_key": "ab" * 32,
                "guild_allowlist": ["default-guild"],
            },
        )

        config = common.import_app_config(
            config,
            {
                "application_id": "456",
                "public_key": "cd" * 32,
                "guild_allowlist": ["ollie-guild"],
                "channel_allowlist": ["ollie-channel"],
            },
            app_name="ollie",
        )

        self.assertEqual(config["app"]["application_id"], "123")
        self.assertEqual(config["policy"]["guild_allowlist"], ["default-guild"])
        self.assertEqual(config["apps"]["ollie"]["application_id"], "456")
        self.assertEqual(config["apps"]["ollie"]["policy"]["guild_allowlist"], ["ollie-guild"])
        self.assertEqual(config["apps"]["ollie"]["policy"]["channel_allowlist"], ["ollie-channel"])

    def test_import_app_config_rejects_named_id_change_even_with_a_new_token(self) -> None:
        config = common.import_app_config(
            common.load_config(),
            {"application_id": "456", "public_key": "cd" * 32},
            app_name="ollie",
        )
        common.save_bot_token("existing-credential", app_name="ollie")

        with self.assertRaisesRegex(ValueError, "cannot change.*application_id"):
            common.import_app_config(
                config,
                {"application_id": "789", "public_key": "ef" * 32},
                app_name="ollie",
                bot_token="replacement-credential",
            )

        self.assertEqual(common.resolve_app_config(common.load_config(), "ollie")["application_id"], "456")
        self.assertEqual(common.load_bot_token("ollie"), "existing-credential")

    def test_import_app_config_rejects_empty_id_partial_app_with_an_existing_token(self) -> None:
        common.save_config(
            {
                "apps": {
                    "ollie": {
                        "public_key": "cd" * 32,
                    }
                }
            }
        )
        common.save_bot_token("orphan-credential", app_name="ollie")

        with self.assertRaisesRegex(ValueError, "orphan bot token.*new app name"):
            common.import_app_config(
                common.load_config(),
                {"application_id": "456", "public_key": "cd" * 32},
                app_name="ollie",
            )

        self.assertEqual(common.resolve_app_config(common.load_config(), "ollie")["application_id"], "")
        self.assertEqual(common.load_bot_token("ollie"), "orphan-credential")

    def test_new_named_identity_rejects_orphan_token_even_when_a_token_is_supplied(self) -> None:
        common.save_bot_token("orphan-credential", app_name="ollie")

        with self.assertRaisesRegex(ValueError, "orphan bot token.*new app name"):
            common.import_app_config(
                common.load_config(),
                {"application_id": "456", "public_key": "cd" * 32},
                app_name="ollie",
                bot_token="replacement-credential",
            )

        self.assertEqual(common.load_config()["apps"], {})
        self.assertEqual(common.load_bot_token("ollie"), "orphan-credential")

    def test_named_app_import_rejects_unsafe_app_name(self) -> None:
        with self.assertRaisesRegex(ValueError, "app name"):
            common.import_app_config(
                common.load_config(),
                {
                    "application_id": "456",
                    "public_key": "cd" * 32,
                },
                app_name="../ollie",
            )

    def test_named_app_import_rejects_reserved_default_name(self) -> None:
        with self.assertRaisesRegex(ValueError, "reserved"):
            common.import_app_config(
                common.load_config(),
                {
                    "application_id": "456",
                    "public_key": "cd" * 32,
                },
                app_name="default",
            )

    def test_config_normalization_drops_empty_named_app_key(self) -> None:
        config = common.normalize_config(
            {
                "apps": {
                    "": {
                        "application_id": "456",
                        "public_key": "cd" * 32,
                    }
                }
            }
        )

        self.assertEqual(config["apps"], {})

    def test_named_app_import_rejects_duplicate_application_id(self) -> None:
        config = common.import_app_config(
            common.load_config(),
            {
                "application_id": "456",
                "public_key": "ab" * 32,
            },
        )

        with self.assertRaisesRegex(ValueError, "application_id.*already configured"):
            common.import_app_config(
                config,
                {
                    "application_id": "456",
                    "public_key": "cd" * 32,
                },
                app_name="ollie",
            )

    def test_concurrent_named_app_imports_preserve_both_apps(self) -> None:
        stale_config = common.load_config()
        start = threading.Barrier(2)
        errors: list[BaseException] = []

        def import_named(app_name: str, application_id: str, public_key_byte: str) -> None:
            try:
                start.wait(timeout=2)
                common.import_app_config(
                    stale_config,
                    {
                        "application_id": application_id,
                        "public_key": public_key_byte * 32,
                    },
                    app_name=app_name,
                )
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [
            threading.Thread(target=import_named, args=("ollie", "456", "ab")),
            threading.Thread(target=import_named, args=("olivia", "789", "cd")),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=3)

        self.assertEqual(errors, [])
        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual(set(common.load_config()["apps"]), {"ollie", "olivia"})

    def test_concurrent_metadata_import_cannot_split_a_rotated_named_identity(self) -> None:
        stale_config = common.import_app_config(
            common.load_config(),
            {"application_id": "456", "public_key": "ab" * 32},
            app_name="ollie",
        )
        common.save_bot_token("old-credential", app_name="ollie")
        rotation_holds_lock = threading.Event()
        release_rotation = threading.Event()
        errors: list[BaseException] = []
        original_save_bot_token = common.save_bot_token

        def blocking_save_bot_token(token: str, app_name: str = "") -> None:
            if token == "new-credential":
                rotation_holds_lock.set()
                if not release_rotation.wait(timeout=2):
                    raise TimeoutError("timed out waiting to release credential rotation")
            original_save_bot_token(token, app_name=app_name)

        def rotate_identity() -> None:
            try:
                common.import_app_config(
                    stale_config,
                    {"application_id": "456", "public_key": "cd" * 32},
                    app_name="ollie",
                    bot_token="new-credential",
                )
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        def write_stale_metadata() -> None:
            try:
                common.import_app_config(
                    stale_config,
                    {"application_id": "456", "public_key": "ef" * 32},
                    app_name="ollie",
                )
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        with mock.patch.object(common, "save_bot_token", side_effect=blocking_save_bot_token):
            rotation_thread = threading.Thread(target=rotate_identity)
            rotation_thread.start()
            self.assertTrue(rotation_holds_lock.wait(timeout=2))
            metadata_thread = threading.Thread(target=write_stale_metadata)
            metadata_thread.start()
            release_rotation.set()
            rotation_thread.join(timeout=3)
            metadata_thread.join(timeout=3)

        self.assertFalse(rotation_thread.is_alive())
        self.assertFalse(metadata_thread.is_alive())
        self.assertEqual(errors, [])
        app = common.resolve_app_config(common.load_config(), "ollie")
        self.assertEqual(app["application_id"], "456")
        self.assertEqual(app["public_key"], "ef" * 32)
        self.assertEqual(common.load_bot_token("ollie"), "new-credential")

    def test_concurrent_named_bindings_preserve_both_bindings(self) -> None:
        config = common.import_app_config(
            common.load_config(),
            {"application_id": "456", "public_key": "ab" * 32},
            app_name="ollie",
        )
        common.import_app_config(
            config,
            {"application_id": "789", "public_key": "cd" * 32},
            app_name="olivia",
        )
        stale_config = common.load_config()
        start = threading.Barrier(2)
        errors: list[BaseException] = []

        def bind_named(app_name: str, session_name: str) -> None:
            try:
                start.wait(timeout=2)
                common.set_chat_binding(
                    stale_config,
                    "room",
                    "22",
                    [session_name],
                    guild_id="1",
                    app_name=app_name,
                )
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [
            threading.Thread(target=bind_named, args=("ollie", "teams.lead")),
            threading.Thread(target=bind_named, args=("olivia", "teams.pm")),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=3)

        self.assertEqual(errors, [])
        self.assertTrue(all(not thread.is_alive() for thread in threads))
        binding_ids = {item["id"] for item in common.list_chat_bindings(common.load_config())}
        self.assertEqual(binding_ids, {"room:22@app:ollie", "room:22@app:olivia"})

    def test_room_launcher_mutation_reloads_config_and_preserves_a_concurrent_app_import(self) -> None:
        stale_config = common.load_config()
        common.import_app_config(
            stale_config,
            {"application_id": "456", "public_key": "ab" * 32},
            app_name="ollie",
        )

        common.set_room_launcher(stale_config, "1", "22")

        config = common.load_config()
        self.assertIn("ollie", config["apps"])
        self.assertIsNotNone(common.resolve_room_launcher(config, "22"))

    def test_channel_mapping_mutation_reloads_config_and_preserves_a_concurrent_app_import(self) -> None:
        stale_config = common.load_config()
        common.import_app_config(
            stale_config,
            {"application_id": "456", "public_key": "ab" * 32},
            app_name="ollie",
        )

        common.set_channel_mapping(stale_config, "1", "22", "product/polecat", None)

        config = common.load_config()
        self.assertIn("ollie", config["apps"])
        self.assertIn("1/22", config["channels"])

    def test_rig_mapping_mutation_reloads_config_and_preserves_a_concurrent_app_import(self) -> None:
        stale_config = common.load_config()
        common.import_app_config(
            stale_config,
            {"application_id": "456", "public_key": "ab" * 32},
            app_name="ollie",
        )

        common.set_rig_mapping(stale_config, "1", "product", "product/polecat", None)

        config = common.load_config()
        self.assertIn("ollie", config["apps"])
        self.assertIn("1/product", config["rigs"])

    def test_named_app_tokens_are_isolated_and_mode_0600(self) -> None:
        common.save_bot_token("default-token")
        common.save_bot_token("ollie-token", app_name="ollie")
        common.save_bot_token("olivia-token", app_name="olivia")

        self.assertEqual(common.load_bot_token(), "default-token")
        self.assertEqual(common.load_bot_token("ollie"), "ollie-token")
        self.assertEqual(common.load_bot_token("olivia"), "olivia-token")
        self.assertEqual(
            pathlib.Path(common.secret_path("bot-token-ollie.txt")).stat().st_mode & 0o777,
            0o600,
        )
        self.assertEqual(
            pathlib.Path(common.secret_path("bot-token-olivia.txt")).stat().st_mode & 0o777,
            0o600,
        )

    def test_redacted_config_reports_named_token_presence_without_token_value(self) -> None:
        config = common.import_app_config(
            common.load_config(),
            {
                "application_id": "456",
                "public_key": "cd" * 32,
            },
            app_name="ollie",
        )
        common.save_bot_token("ollie-token", app_name="ollie")

        redacted = common.redact_config(config)

        self.assertTrue(redacted["apps"]["ollie"]["bot_token_present"])
        self.assertNotIn("bot_token", redacted["apps"]["ollie"])

    def test_app_config_and_policy_resolution_fail_closed_for_unknown_name(self) -> None:
        config = common.import_app_config(
            common.load_config(),
            {
                "application_id": "456",
                "public_key": "cd" * 32,
                "guild_allowlist": ["1"],
                "channel_allowlist": ["22"],
            },
            app_name="ollie",
        )

        self.assertEqual(common.resolve_app_config(config, "ollie")["application_id"], "456")
        self.assertEqual(common.resolve_app_policy(config, "ollie")["channel_allowlist"], ["22"])
        with self.assertRaisesRegex(ValueError, "unknown Discord app"):
            common.resolve_app_config(config, "olivia")
        with self.assertRaisesRegex(ValueError, "unknown Discord app"):
            common.resolve_app_policy(config, "olivia")

    def test_gateway_status_is_isolated_per_named_app_and_keeps_legacy_projection(self) -> None:
        config = common.import_app_config(
            common.load_config(),
            {
                "application_id": "123",
                "public_key": "ab" * 32,
            },
        )
        common.import_app_config(
            config,
            {
                "application_id": "456",
                "public_key": "cd" * 32,
            },
            app_name="ollie",
        )
        common.save_gateway_status({"state": "ready", "bot_user_id": "123"})
        common.save_gateway_status({"state": "waiting_for_config"}, app_name="ollie")

        snapshot = common.build_status_snapshot(limit=1)

        self.assertEqual(snapshot["gateway_status"]["bot_user_id"], "123")
        self.assertEqual(snapshot["gateway_statuses"]["default"]["bot_user_id"], "123")
        self.assertEqual(snapshot["gateway_statuses"]["ollie"]["state"], "waiting_for_config")

    def test_shared_discord_prompt_requires_bold_speaker_prefix(self) -> None:
        fragment = (
            pathlib.Path(__file__).resolve().parents[1] / "template-fragments" / "discord-v0.template.md"
        ).read_text(encoding="utf-8")

        self.assertIn("Always prefix your Discord messages with your handle in bold", fragment)
        self.assertIn("**randy:**", fragment)

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

    def test_named_apps_can_bind_the_same_room_independently(self) -> None:
        config = common.set_chat_binding(
            common.load_config(),
            "room",
            "22",
            ["teams.lead"],
            guild_id="1",
            app_name="ollie",
        )
        config = common.set_chat_binding(
            config,
            "room",
            "22",
            ["teams.pm"],
            guild_id="1",
            app_name="olivia",
        )

        lead = common.resolve_chat_binding(config, "room:22@app:ollie")
        product = common.resolve_chat_binding(config, "room:22@app:olivia")

        self.assertEqual(lead["app"], "ollie")
        self.assertEqual(lead["session_names"], ["teams.lead"])
        self.assertEqual(product["app"], "olivia")
        self.assertEqual(product["session_names"], ["teams.pm"])
        self.assertEqual(len(common.list_chat_bindings(config)), 2)

    def test_legacy_and_named_binding_can_share_a_room(self) -> None:
        config = common.set_chat_binding(
            common.load_config(),
            "room",
            "22",
            ["legacy"],
            guild_id="1",
        )
        config = common.set_chat_binding(
            config,
            "room",
            "22",
            ["teams.lead"],
            guild_id="1",
            app_name="ollie",
        )

        legacy = common.resolve_chat_binding(config, "room:22")
        named = common.resolve_chat_binding(config, "room:22@app:ollie")

        self.assertNotIn("app", legacy)
        self.assertEqual(named["app"], "ollie")

    def test_publish_route_rejects_binding_for_unknown_named_app(self) -> None:
        config = common.set_chat_binding(
            common.load_config(),
            "room",
            "22",
            ["teams.lead"],
            guild_id="1",
            app_name="ghost",
        )

        with self.assertRaisesRegex(ValueError, "unknown Discord app"):
            common.resolve_publish_route(config, "room:22@app:ghost")

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
        self.assertTrue(launcher["policy"]["peer_fanout_enabled"])
        self.assertTrue(launcher["policy"]["allow_untargeted_peer_fanout"])

    def test_set_room_launcher_can_disable_peer_fanout_policy(self) -> None:
        config = common.set_room_launcher(
            common.load_config(),
            "1",
            "22",
            policy={"peer_fanout_enabled": False, "allow_untargeted_peer_fanout": False},
        )

        launcher = common.resolve_room_launcher(config, "22")

        self.assertIsNotNone(launcher)
        assert launcher is not None
        self.assertFalse(launcher["policy"]["peer_fanout_enabled"])
        self.assertFalse(launcher["policy"]["allow_untargeted_peer_fanout"])

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

    def test_set_chat_binding_persists_untargeted_ambient_delivery_policy(self) -> None:
        config = common.set_chat_binding(
            common.load_config(),
            "room",
            "22",
            ["randy"],
            guild_id="1",
            policy={"ambient_read_enabled": True, "allow_untargeted_ambient_delivery": True},
        )

        binding = common.resolve_chat_binding(config, "room:22")

        assert binding is not None
        self.assertTrue(binding["policy"]["ambient_read_enabled"])
        self.assertTrue(binding["policy"]["allow_untargeted_ambient_delivery"])

    def test_set_chat_binding_rejects_untargeted_ambient_delivery_without_ambient_read(self) -> None:
        with self.assertRaisesRegex(ValueError, "requires ambient read to be enabled"):
            common.set_chat_binding(
                common.load_config(),
                "room",
                "22",
                ["randy"],
                guild_id="1",
                policy={"allow_untargeted_ambient_delivery": True},
            )

    def test_set_chat_binding_rejects_untargeted_ambient_delivery_for_multi_session_room(self) -> None:
        with self.assertRaisesRegex(ValueError, "requires exactly one session name"):
            common.set_chat_binding(
                common.load_config(),
                "room",
                "22",
                ["randy", "wendy"],
                guild_id="1",
                policy={"ambient_read_enabled": True, "allow_untargeted_ambient_delivery": True},
            )

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

    def test_describe_room_channel_scope_does_not_fall_back_from_an_explicit_empty_token(self) -> None:
        common.save_bot_token("default-token")

        with mock.patch.object(common, "discord_api_request") as discord_api_request:
            scope = common.describe_room_channel_scope("22", bot_token="")

        self.assertEqual(scope, {})
        discord_api_request.assert_not_called()

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
        self.assertEqual(payload["allowed_mentions"]["parse"], ["users"])

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

    def test_gc_api_base_url_uses_site_workspace_name_when_city_toml_omits_name(self) -> None:
        pathlib.Path(self.tempdir.name, "city.toml").write_text(
            "[workspace]\nmax_active_sessions = 5\n",
            encoding="utf-8",
        )
        site_dir = pathlib.Path(self.tempdir.name, ".gc")
        site_dir.mkdir()
        (site_dir / "site.toml").write_text('workspace_name = "gc"\n', encoding="utf-8")
        common._supervisor_scope_cache.clear()
        response = mock.Mock()
        response.__enter__ = mock.Mock(
            return_value=mock.Mock(read=mock.Mock(return_value=b'{"items":[{"name":"gc","running":true}]}'))
        )
        response.__exit__ = mock.Mock(return_value=False)

        with mock.patch.object(common.urllib.request, "urlopen", return_value=response):
            self.assertEqual(common.gc_api_base_url(), "http://127.0.0.1:8372")

    def test_gc_api_base_url_falls_back_to_city_dir_basename_when_no_declared_name(self) -> None:
        pathlib.Path(self.tempdir.name, "city.toml").write_text("", encoding="utf-8")
        common._supervisor_scope_cache.clear()
        basename = pathlib.Path(self.tempdir.name).name
        body = ('{"items":[{"name":"%s","running":true}]}' % basename).encode("utf-8")
        response = mock.Mock()
        response.__enter__ = mock.Mock(return_value=mock.Mock(read=mock.Mock(return_value=body)))
        response.__exit__ = mock.Mock(return_value=False)

        with mock.patch.object(common.urllib.request, "urlopen", return_value=response):
            self.assertEqual(common.gc_api_base_url(), "http://127.0.0.1:8372")

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
        common._supervisor_scope_cache.clear()
        cities = mock.Mock()
        cities.__enter__ = mock.Mock(
            return_value=mock.Mock(read=mock.Mock(return_value=b'{"items":[{"name":"gc","running":true}]}'))
        )
        cities.__exit__ = mock.Mock(return_value=False)
        sessions = mock.Mock()
        sessions.__enter__ = mock.Mock(return_value=mock.Mock(read=mock.Mock(return_value=b'{"items": []}')))
        sessions.__exit__ = mock.Mock(return_value=False)

        with mock.patch.object(common.urllib.request, "urlopen", side_effect=[cities, sessions]) as urlopen:
            payload = common.gc_api_request("GET", "/v0/sessions")

        self.assertEqual(payload, {"items": []})
        self.assertEqual(urlopen.call_args_list[-1].args[0].full_url, "http://127.0.0.1:8372/v0/city/gc/sessions")

    def test_deliver_session_message_uses_messages_endpoint_for_default_intent(self) -> None:
        with mock.patch.object(common, "gc_api_request_with_status", return_value=(200, {"status": "accepted"})) as gc_api_request:
            payload = common.deliver_session_message("corp--sky", "hello", idempotency_key="ingress:1")

        self.assertEqual(payload, {"status": "accepted"})
        gc_api_request.assert_called_once_with(
            "POST",
            "/v0/session/corp--sky/messages",
            payload={"message": "hello"},
            headers={"Idempotency-Key": "ingress:1"},
            timeout=common.GC_API_REQUEST_TIMEOUT_SECONDS,
        )

    def test_deliver_session_message_uses_submit_endpoint_for_follow_up_intent(self) -> None:
        with mock.patch.object(common, "gc_api_request_with_status", return_value=(200, {"status": "accepted"})) as gc_api_request:
            payload = common.deliver_session_message(
                "corp--sky",
                "hello again",
                idempotency_key="ingress:2",
                intent="follow_up",
            )

        self.assertEqual(payload, {"status": "accepted"})
        gc_api_request.assert_called_once_with(
            "POST",
            "/v0/session/corp--sky/submit",
            payload={"message": "hello again", "intent": "follow_up"},
            headers={"Idempotency-Key": "ingress:2"},
            timeout=common.GC_API_REQUEST_TIMEOUT_SECONDS,
        )

    def test_deliver_session_message_preserves_accepted_payload_after_async_success(self) -> None:
        accepted = {"status": "accepted", "request_id": "req-submit", "event_cursor": "42"}
        accepted_response = mock.MagicMock()
        accepted_response.status = 202
        accepted_response.__enter__.return_value = mock.Mock(
            read=mock.Mock(
                return_value=b'{"status":"accepted","request_id":"req-submit","event_cursor":"42"}'
            )
        )
        accepted_response.__enter__.return_value.status = 202
        accepted_response.__exit__.return_value = False
        succeeded_stream = mock.MagicMock()
        succeeded_stream.__enter__.return_value = iter(
            [
                b"event: request.result.session.submit\n",
                b'data: {"type":"request.result.session.submit","payload":{"request_id":"req-submit","session_id":"session-1","queued":true,"intent":"follow_up"}}\n',
                b"\n",
            ]
        )
        succeeded_stream.__exit__.return_value = False

        with mock.patch.dict(
            os.environ,
            {"GC_API_BASE_URL": "http://gc.test/v0/city/test"},
        ), mock.patch.object(
            common.urllib.request,
            "urlopen",
            side_effect=[accepted_response, succeeded_stream],
        ) as urlopen:
            payload = common.deliver_session_message(
                "corp--sky",
                "hello again",
                idempotency_key="ingress:3",
                intent="follow_up",
            )

        self.assertEqual(payload, accepted)
        self.assertEqual(common.GC_API_ASYNC_RESULT_TIMEOUT_SECONDS, 4 * 60)
        self.assertEqual(len(urlopen.call_args_list), 2)
        self.assertEqual(
            urlopen.call_args_list[1].args[0].full_url,
            "http://gc.test/v0/city/test/events/stream?after_seq=42",
        )
        self.assertEqual(
            urlopen.call_args_list[0].kwargs["timeout"],
            common.GC_API_REQUEST_TIMEOUT_SECONDS,
        )
        self.assertEqual(
            urlopen.call_args_list[1].kwargs["timeout"],
            common.GC_API_ASYNC_RESULT_TIMEOUT_SECONDS,
        )

    def test_deliver_session_message_rejects_malformed_async_accepted_response(self) -> None:
        accepted_response = mock.MagicMock()
        accepted_response.status = 202
        accepted_response.__enter__.return_value = mock.Mock(
            read=mock.Mock(return_value=b'{"status":"accepted"}')
        )
        accepted_response.__enter__.return_value.status = 202
        accepted_response.__exit__.return_value = False

        with mock.patch.dict(
            os.environ,
            {"GC_API_BASE_URL": "http://gc.test/v0/city/test"},
        ), mock.patch.object(
            common.urllib.request,
            "urlopen",
            return_value=accepted_response,
        ):
            on_async_accepted = mock.Mock()
            with self.assertRaisesRegex(common.GCAPIError, "request_id.*event_cursor"):
                common.deliver_session_message(
                    "corp--sky",
                    "hello again",
                    idempotency_key="ingress:malformed",
                    intent="follow_up",
                    on_async_accepted=on_async_accepted,
                )

        on_async_accepted.assert_not_called()

    def test_deliver_session_message_rejects_async_response_without_event_cursor(self) -> None:
        on_async_accepted = mock.Mock()
        with mock.patch.object(
            common,
            "gc_api_request_with_status",
            return_value=(202, {"status": "accepted", "request_id": "req-no-cursor"}),
        ), mock.patch.object(common, "resume_session_message_delivery") as resume_session_message_delivery:
            with self.assertRaisesRegex(common.GCAPIError, "request_id.*event_cursor"):
                common.deliver_session_message(
                    "corp--sky",
                    "hello again",
                    intent="follow_up",
                    on_async_accepted=on_async_accepted,
                )

        on_async_accepted.assert_not_called()
        resume_session_message_delivery.assert_not_called()

    def test_deliver_session_message_accepts_zero_event_cursor(self) -> None:
        accepted = {"status": "accepted", "request_id": "req-zero", "event_cursor": "0"}
        with mock.patch.object(
            common,
            "gc_api_request_with_status",
            return_value=(202, accepted),
        ), mock.patch.object(
            common,
            "resume_session_message_delivery",
            return_value={"request_id": "req-zero", "session_id": "session-zero"},
        ) as resume_session_message_delivery:
            payload = common.deliver_session_message("corp--sky", "hello again", intent="follow_up")

        self.assertEqual(payload, accepted)
        resume_session_message_delivery.assert_called_once_with(
            "req-zero",
            event_cursor="0",
            intent="follow_up",
            timeout=common.GC_API_ASYNC_RESULT_TIMEOUT_SECONDS,
            cancel_event=None,
        )

    def test_deliver_session_message_allows_legacy_synchronous_response_without_async_metadata(self) -> None:
        accepted_response = mock.MagicMock()
        accepted_response.status = 200
        accepted_response.__enter__.return_value = mock.Mock(
            read=mock.Mock(return_value=b'{"status":"accepted","request_id":"legacy-request"}')
        )
        accepted_response.__enter__.return_value.status = 200
        accepted_response.__exit__.return_value = False

        with mock.patch.dict(
            os.environ,
            {"GC_API_BASE_URL": "http://gc.test/v0/city/test"},
        ), mock.patch.object(
            common.urllib.request,
            "urlopen",
            return_value=accepted_response,
        ) as urlopen, mock.patch.object(common, "resume_session_message_delivery") as resume_session_message_delivery:
            payload = common.deliver_session_message(
                "corp--sky",
                "hello again",
                idempotency_key="ingress:legacy",
                intent="follow_up",
            )

        self.assertEqual(payload, {"status": "accepted", "request_id": "legacy-request"})
        self.assertEqual(len(urlopen.call_args_list), 1)
        resume_session_message_delivery.assert_not_called()

    def test_deliver_session_message_persists_async_acceptance_before_waiting(self) -> None:
        accepted_response = mock.MagicMock()
        accepted_response.status = 202
        accepted_response.__enter__.return_value = mock.Mock(
            read=mock.Mock(
                return_value=b'{"status":"accepted","request_id":"req-submit","event_cursor":"42"}'
            )
        )
        accepted_response.__enter__.return_value.status = 202
        accepted_response.__exit__.return_value = False
        persisted: list[dict[str, object]] = []

        def assert_persisted_before_wait(*args: object, **kwargs: object) -> dict[str, object]:
            self.assertEqual(persisted[0]["request_id"], "req-submit")
            self.assertEqual(persisted[0]["event_cursor"], "42")
            return {"request_id": "req-submit", "session_id": "session-1"}

        with mock.patch.dict(
            os.environ,
            {"GC_API_BASE_URL": "http://gc.test/v0/city/test"},
        ), mock.patch.object(
            common.urllib.request,
            "urlopen",
            return_value=accepted_response,
        ), mock.patch.object(
            common,
            "wait_for_gc_request_result",
            side_effect=assert_persisted_before_wait,
        ):
            common.deliver_session_message(
                "corp--sky",
                "hello again",
                idempotency_key="ingress:persist-first",
                intent="follow_up",
                on_async_accepted=persisted.append,
            )

        self.assertEqual(len(persisted), 1)

    def test_deliver_session_message_does_not_open_sse_when_acceptance_persistence_fails(self) -> None:
        accepted = {"status": "accepted", "request_id": "req-submit", "event_cursor": "42"}

        with mock.patch.object(
            common,
            "gc_api_request_with_status",
            return_value=(202, accepted),
        ), mock.patch.object(
            common,
            "resume_session_message_delivery",
        ) as resume_session_message_delivery:
            with self.assertRaisesRegex(OSError, "receipt write failed"):
                common.deliver_session_message(
                    "corp--sky",
                    "hello again",
                    intent="follow_up",
                    on_async_accepted=mock.Mock(side_effect=OSError("receipt write failed")),
                )

        resume_session_message_delivery.assert_not_called()

    def test_deliver_session_message_reports_terminal_success_evidence(self) -> None:
        accepted = {"status": "accepted", "request_id": "req-submit", "event_cursor": "42"}
        terminal = {"request_id": "req-submit", "session_id": "session-1", "queued": True}
        evidence: list[dict[str, object]] = []

        with mock.patch.object(
            common,
            "gc_api_request_with_status",
            return_value=(202, accepted),
        ), mock.patch.object(
            common,
            "resume_session_message_delivery",
            return_value=terminal,
        ):
            payload = common.deliver_session_message(
                "corp--sky",
                "hello again",
                idempotency_key="ingress:terminal",
                intent="follow_up",
                on_async_terminal=evidence.append,
            )

        self.assertEqual(payload, accepted)
        self.assertEqual(evidence, [{"status": "succeeded", "payload": terminal}])

    def test_resume_session_message_delivery_waits_without_resubmitting(self) -> None:
        cancel_event = threading.Event()
        terminal = {"request_id": "req-submit", "session_id": "session-1", "queued": True}

        with mock.patch.object(
            common,
            "wait_for_gc_request_result",
            return_value=terminal,
        ) as wait_for_gc_request_result, mock.patch.object(
            common,
            "gc_api_request_with_status",
        ) as gc_api_request_with_status:
            result = common.resume_session_message_delivery(
                "req-submit",
                "42",
                intent="follow_up",
                timeout=12,
                cancel_event=cancel_event,
            )

        self.assertEqual(result, terminal)
        gc_api_request_with_status.assert_not_called()
        wait_for_gc_request_result.assert_called_once_with(
            "req-submit",
            event_cursor="42",
            success_type=common.GC_EVENT_SESSION_SUBMIT_SUCCEEDED,
            failure_operation=common.GC_OPERATION_SESSION_SUBMIT,
            timeout=12,
            cancel_event=cancel_event,
        )

    def test_wait_for_gc_request_result_cancellation_closes_open_stream(self) -> None:
        entered = threading.Event()
        closed = threading.Event()
        cancel_event = threading.Event()
        errors: list[BaseException] = []

        class BlockingStream:
            def __enter__(self) -> "BlockingStream":
                return self

            def __exit__(self, *args: object) -> None:
                self.close()

            def __iter__(self) -> "BlockingStream":
                return self

            def __next__(self) -> bytes:
                entered.set()
                closed.wait(timeout=2)
                raise StopIteration

            def close(self) -> None:
                closed.set()

        def wait_for_result() -> None:
            try:
                common.wait_for_gc_request_result(
                    "req-submit",
                    event_cursor="42",
                    success_type=common.GC_EVENT_SESSION_SUBMIT_SUCCEEDED,
                    failure_operation=common.GC_OPERATION_SESSION_SUBMIT,
                    timeout=60,
                    cancel_event=cancel_event,
                )
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        with mock.patch.dict(
            os.environ,
            {"GC_API_BASE_URL": "http://gc.test/v0/city/test"},
        ), mock.patch.object(common.urllib.request, "urlopen", return_value=BlockingStream()):
            waiter = threading.Thread(target=wait_for_result)
            waiter.start()
            self.assertTrue(entered.wait(timeout=1))
            cancel_event.set()
            waiter.join(timeout=1)

        self.assertFalse(waiter.is_alive())
        self.assertTrue(closed.is_set())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], common.GCAPIRequestCancelled)

    def test_wait_for_gc_request_result_eof_is_recoverable_uncertainty(self) -> None:
        closed_stream = mock.MagicMock()
        closed_stream.__enter__.return_value = iter([])
        closed_stream.__exit__.return_value = False

        with mock.patch.dict(
            os.environ,
            {"GC_API_BASE_URL": "http://gc.test/v0/city/test"},
        ), mock.patch.object(common.urllib.request, "urlopen", return_value=closed_stream):
            with self.assertRaisesRegex(common.GCAPIResultUnknown, "closed before request"):
                common.wait_for_gc_request_result(
                    "req-submit",
                    event_cursor="42",
                    success_type=common.GC_EVENT_SESSION_SUBMIT_SUCCEEDED,
                    failure_operation=common.GC_OPERATION_SESSION_SUBMIT,
                )

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

    def test_find_latest_discord_reply_context_falls_back_to_delivered_ingress(self) -> None:
        common.save_chat_ingress(
            {
                "ingress_id": "in-older",
                "binding_id": "room:old",
                "conversation_id": "old",
                "discord_message_id": "old-msg",
                "created_at": "2026-04-20T22:35:00Z",
                "status": "delivered",
                "targets": [
                    {
                        "session_name": "wendy__wendy",
                        "status": "delivered",
                        "response": {"id": "mc-ayq6xi"},
                    }
                ],
            }
        )
        common.save_chat_ingress(
            {
                "ingress_id": "in-newer",
                "binding_id": "room:22",
                "conversation_id": "22",
                "discord_message_id": "new-msg",
                "guild_id": "1",
                "created_at": "2026-04-20T22:36:00Z",
                "status": "delivered",
                "targets": [
                    {
                        "session_name": "wendy__wendy",
                        "status": "delivered",
                        "response": {"id": "mc-ayq6xi"},
                    }
                ],
            }
        )

        with mock.patch.object(common, "gc_api_request", return_value={"messages": []}):
            fields = common.find_latest_discord_reply_context("mc-ayq6xi", tail=5)

        self.assertEqual(fields["kind"], "discord_human_message")
        self.assertEqual(fields["ingress_receipt_id"], "in-newer")
        self.assertEqual(fields["publish_binding_id"], "room:22")
        self.assertEqual(fields["publish_conversation_id"], "22")
        self.assertEqual(fields["publish_trigger_id"], "new-msg")
        self.assertEqual(fields["publish_reply_to_discord_message_id"], "new-msg")

    def test_find_latest_discord_reply_context_matches_async_terminal_session_id(self) -> None:
        common.save_chat_ingress(
            {
                "ingress_id": "in-async",
                "binding_id": "room:22@app:riley",
                "conversation_id": "22",
                "discord_message_id": "async-msg",
                "created_at": "2026-07-15T02:24:01Z",
                "status": "delivered",
                "targets": [
                    {
                        "session_name": "employees.corp--riley",
                        "status": "delivered",
                        "response": {
                            "status": "accepted",
                            "request_id": "req-async",
                        },
                        "terminal_evidence": {
                            "status": "succeeded",
                            "payload": {
                                "request_id": "req-async",
                                "session_id": "mc-wisp-n2val",
                                "queued": True,
                            },
                        },
                    }
                ],
            }
        )

        with mock.patch.object(common, "gc_api_request", return_value={"messages": []}):
            fields = common.find_latest_discord_reply_context("mc-wisp-n2val", tail=5)

        self.assertEqual(fields["ingress_receipt_id"], "in-async")
        self.assertEqual(fields["publish_binding_id"], "room:22@app:riley")
        self.assertEqual(fields["publish_reply_to_discord_message_id"], "async-msg")

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

    def test_set_room_launch_last_addressed_ignores_older_delivery_order(self) -> None:
        common.save_room_launch(
            {
                "launch_id": "room-launch:ordered",
                "launcher_id": "launch-room:22",
                "guild_id": "1",
                "conversation_id": "22",
                "root_message_id": "ordered",
                "qualified_handle": "corp/sky",
                "session_alias": "dc-123-sky",
                "thread_id": "222",
                "participants": {
                    "corp/priya": {
                        "qualified_handle": "corp/priya",
                        "session_alias": "dc-123-priya",
                    }
                },
            }
        )

        common.set_room_launch_last_addressed(
            "room-launch:ordered",
            "corp/priya",
            delivery_order="snowflake:00000000000000000200",
        )
        common.set_room_launch_last_addressed(
            "room-launch:ordered",
            "corp/sky",
            delivery_order="snowflake:00000000000000000100",
        )

        launch = common.load_room_launch("room-launch:ordered")
        assert launch is not None
        self.assertEqual(launch["last_addressed_qualified_handle"], "corp/priya")
        self.assertEqual(launch["last_addressed_delivery_order"], "snowflake:00000000000000000200")

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

        with mock.patch.object(
            common,
            "list_city_sessions",
            return_value=[
                {
                    "id": "gc-old",
                    "alias": "dc-123-sky",
                    "session_name": "dc-old-sky",
                    "state": "closed",
                    "running": False,
                    "created_at": "2026-03-20T00:00:00Z",
                }
            ],
        ), mock.patch.object(
            common,
            "create_agent_session",
            return_value={"id": "gc-new", "session_name": "dc-new-sky", "alias": "dc-123-sky"},
        ) as create_agent_session, mock.patch.object(
            common,
            "deliver_session_message",
            return_value={"status": "accepted", "id": "gc-new"},
        ):
            current = common.ensure_room_launch_session(launch)

        create_agent_session.assert_called_once()
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
        ) as create_agent_session, mock.patch.object(
            common,
            "deliver_session_message",
            return_value={"status": "accepted", "id": "gc-new"},
        ), mock.patch.object(common.time, "sleep"):
            current = common.ensure_room_launch_session(launch)

        create_agent_session.assert_called_once()
        self.assertEqual(current["session_id"], "gc-new")
        self.assertEqual(current["session_name"], "dc-new-sky")

    def test_ensure_room_launch_session_hydrates_routable_identity_after_longer_async_delay(self) -> None:
        launch = {
            "launch_id": "room-launch:slow-hydrate",
            "qualified_handle": "corp/maya",
            "session_alias": "dc-123-maya",
            "from_display": "alice",
        }

        calls = {"count": 0}

        def list_sessions(*, state: str = "all") -> list[dict[str, object]]:
            self.assertEqual(state, "all")
            calls["count"] += 1
            if calls["count"] < 25:
                return []
            return [
                {
                    "id": "gc-maya",
                    "alias": "dc-123-maya",
                    "session_name": "s-gc-maya",
                    "state": "active",
                    "running": True,
                    "created_at": "2026-03-23T00:00:00Z",
                }
            ]

        with mock.patch.object(
            common,
            "list_city_sessions",
            side_effect=list_sessions,
        ), mock.patch.object(
            common,
            "create_agent_session",
            return_value={"alias": "dc-123-maya"},
        ) as create_agent_session, mock.patch.object(
            common,
            "deliver_session_message",
            return_value={"status": "accepted", "id": "gc-maya"},
        ), mock.patch.object(common.time, "sleep"):
            current = common.ensure_room_launch_session(launch)

        create_agent_session.assert_called_once()
        self.assertGreaterEqual(calls["count"], 25)
        self.assertEqual(current["session_id"], "gc-maya")
        self.assertEqual(current["session_name"], "s-gc-maya")

    def test_ensure_room_launch_session_primes_new_session_before_first_human_turn(self) -> None:
        launch = {
            "launch_id": "room-launch:prime",
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
            return_value={"id": "gc-new", "session_name": "dc-new-sky", "alias": "dc-123-sky"},
        ), mock.patch.object(
            common,
            "deliver_session_message",
            return_value={"status": "accepted", "id": "gc-new"},
        ) as deliver_session_message:
            current = common.ensure_room_launch_session(launch)

        deliver_session_message.assert_called_once()
        self.assertEqual(deliver_session_message.call_args.args[0], "dc-new-sky")
        primer_message = deliver_session_message.call_args.args[1]
        self.assertIn("<discord-launch-primer>", primer_message)
        self.assertIn("gc discord reply-current --body-file <path>", primer_message)
        self.assertEqual(
            deliver_session_message.call_args.kwargs["idempotency_key"],
            "room-launch:prime:primer:corp/sky:v1",
        )
        participant = current["participants"]["corp/sky"]
        self.assertEqual(participant["primer_version"], common.ROOM_LAUNCH_PRIMER_VERSION)
        self.assertTrue(str(participant.get("primed_at", "")).strip())

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

        with mock.patch.object(
            common,
            "list_city_sessions",
            return_value=[],
        ), mock.patch.object(
            common,
            "create_agent_session",
            return_value={"id": "gc-alex", "session_name": "dc-alex", "alias": "dc-456-alex"},
        ) as create_agent_session, mock.patch.object(
            common,
            "deliver_session_message",
            return_value={"status": "accepted", "id": "gc-alex"},
        ):
            current, participant = common.ensure_room_launch_session_for_handle(
                common.load_room_launch("room-launch:thread-join") or {},
                "corp/alex",
            )

        create_agent_session.assert_called_once()
        self.assertEqual(participant["session_name"], "dc-alex")
        self.assertEqual(current["participants"]["corp/alex"]["session_alias"], "dc-456-alex")

    def test_ensure_room_launch_session_for_handle_does_not_reprime_current_participant(self) -> None:
        common.save_room_launch(
            {
                "launch_id": "room-launch:no-reprime",
                "launcher_id": "launch-room:22",
                "guild_id": "1",
                "conversation_id": "22",
                "root_message_id": "no-reprime",
                "qualified_handle": "corp/sky",
                "participants": {
                    "corp/sky": {
                        "qualified_handle": "corp/sky",
                        "session_alias": "dc-123-sky",
                        "session_name": "dc-sky",
                        "session_id": "gc-sky",
                        "primer_version": common.ROOM_LAUNCH_PRIMER_VERSION,
                        "primer_identity": "gc-sky",
                        "primed_at": "2026-03-22T00:00:00Z",
                    }
                },
                "session_alias": "dc-123-sky",
                "session_name": "dc-sky",
                "session_id": "gc-sky",
            }
        )

        with mock.patch.object(
            common,
            "list_city_sessions",
            return_value=[
                {
                    "id": "gc-sky",
                    "alias": "dc-123-sky",
                    "session_name": "dc-sky",
                    "state": "active",
                    "running": True,
                    "created_at": "2026-03-22T00:00:00Z",
                }
            ],
        ), mock.patch.object(common, "deliver_session_message") as deliver_session_message:
            current, participant = common.ensure_room_launch_session_for_handle(
                common.load_room_launch("room-launch:no-reprime") or {},
                "corp/sky",
            )

        deliver_session_message.assert_not_called()
        self.assertEqual(participant["primer_version"], common.ROOM_LAUNCH_PRIMER_VERSION)
        self.assertEqual(current["participants"]["corp/sky"]["primed_at"], "2026-03-22T00:00:00Z")

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
        self.assertEqual(record["source_session_name"], "corp--sky")
        self.assertEqual(record["source_session_id"], "gc-sky")
        self.assertNotIn("peer_delivery", record)
        deliver_session_message.assert_not_called()

    def test_publish_binding_message_room_launch_peer_fanout_delivers_other_thread_participants(self) -> None:
        common.set_room_launcher(common.load_config(), "1", "22")
        common.save_room_launch(
            {
                "launch_id": "room-launch:thread-44",
                "guild_id": "1",
                "conversation_id": "22",
                "root_message_id": "root-44",
                "thread_id": "thread-44",
                "qualified_handle": "corp/sky",
                "session_alias": "dc-thread-corp-sky",
                "session_id": "gc-sky",
                "session_name": "s-gc-sky",
                "participants": {
                    "corp/sky": {
                        "qualified_handle": "corp/sky",
                        "session_alias": "dc-thread-corp-sky",
                        "session_id": "gc-sky",
                        "session_name": "s-gc-sky",
                    },
                    "corp/priya": {
                        "qualified_handle": "corp/priya",
                        "session_alias": "dc-thread-corp-priya",
                        "session_id": "gc-priya",
                        "session_name": "s-gc-priya",
                    },
                },
                "state": "active",
            }
        )
        binding = common.resolve_publish_route(common.load_config(), "launch-room:22")
        assert binding is not None
        os.environ["GC_SESSION_NAME"] = "s-gc-priya"
        os.environ["GC_SESSION_ID"] = "gc-priya"

        with mock.patch.object(common, "post_channel_message", return_value={"id": "msg-44"}), mock.patch.object(
            common,
            "deliver_session_message",
            return_value={"status": "accepted", "id": "gc-sky"},
        ) as deliver_session_message, mock.patch.object(
            common,
            "list_city_sessions",
            return_value=[
                {"id": "gc-sky", "session_name": "s-gc-sky", "state": "active", "running": True, "created_at": "2026-03-22T00:00:00Z"},
                {"id": "gc-priya", "session_name": "s-gc-priya", "state": "active", "running": True, "created_at": "2026-03-22T00:00:00Z"},
            ],
        ):
            payload = common.publish_binding_message(
                binding,
                "Here is my take.",
                requested_conversation_id="thread-44",
                trigger_id="human-44",
                source_context={
                    "kind": "discord_human_message",
                    "ingress_receipt_id": "in-44",
                    "publish_binding_id": "launch-room:22",
                    "publish_conversation_id": "thread-44",
                    "publish_trigger_id": "human-44",
                    "publish_reply_to_discord_message_id": "human-44",
                    "publish_launch_id": "room-launch:thread-44",
                },
            )

        record = payload["record"]
        self.assertEqual(record["source_session_name"], "s-gc-priya")
        self.assertEqual(record["source_qualified_handle"], "corp/priya")
        self.assertEqual(record["launch_id"], "room-launch:thread-44")
        self.assertEqual(record["conversation_id"], "thread-44")
        self.assertNotIn("peer_delivery", record)
        deliver_session_message.assert_not_called()
        updated_launch = common.load_room_launch("room-launch:thread-44")
        assert updated_launch is not None
        self.assertEqual(updated_launch["message_targets"]["msg-44"], "corp/priya")

    def test_publish_binding_message_room_launch_peer_fanout_targets_explicit_handle(self) -> None:
        common.set_room_launcher(common.load_config(), "1", "22")
        common.save_room_launch(
            {
                "launch_id": "room-launch:thread-55",
                "guild_id": "1",
                "conversation_id": "22",
                "root_message_id": "root-55",
                "thread_id": "thread-55",
                "qualified_handle": "corp/sky",
                "session_alias": "dc-thread-corp-sky",
                "session_id": "gc-sky",
                "session_name": "s-gc-sky",
                "participants": {
                    "corp/sky": {
                        "qualified_handle": "corp/sky",
                        "session_alias": "dc-thread-corp-sky",
                        "session_id": "gc-sky",
                        "session_name": "s-gc-sky",
                    },
                    "corp/priya": {
                        "qualified_handle": "corp/priya",
                        "session_alias": "dc-thread-corp-priya",
                        "session_id": "gc-priya",
                        "session_name": "s-gc-priya",
                    },
                },
                "state": "active",
            }
        )
        binding = common.resolve_publish_route(common.load_config(), "launch-room:22")
        assert binding is not None
        os.environ["GC_SESSION_NAME"] = "s-gc-sky"
        os.environ["GC_SESSION_ID"] = "gc-sky"

        with mock.patch.object(common, "post_channel_message", return_value={"id": "msg-55"}), mock.patch.object(
            common,
            "deliver_session_message",
            return_value={"status": "accepted", "id": "gc-priya"},
        ) as deliver_session_message, mock.patch.object(
            common,
            "list_city_sessions",
            return_value=[
                {"id": "gc-sky", "session_name": "s-gc-sky", "state": "active", "running": True, "created_at": "2026-03-22T00:00:00Z"},
                {"id": "gc-priya", "session_name": "s-gc-priya", "state": "active", "running": True, "created_at": "2026-03-22T00:00:00Z"},
            ],
        ):
            payload = common.publish_binding_message(
                binding,
                "@@corp/priya take a look at this",
                requested_conversation_id="thread-55",
                trigger_id="peer-55",
                source_context={
                    "kind": "discord_peer_publication",
                    "root_ingress_receipt_id": "in-55",
                    "publish_binding_id": "launch-room:22",
                    "publish_conversation_id": "thread-55",
                    "publish_trigger_id": "peer-55",
                    "publish_reply_to_discord_message_id": "peer-55",
                    "publish_launch_id": "room-launch:thread-55",
                },
            )

        record = payload["record"]
        self.assertEqual(record["launch_id"], "room-launch:thread-55")
        self.assertEqual(record["source_qualified_handle"], "corp/sky")
        self.assertNotIn("peer_delivery", record)
        deliver_session_message.assert_not_called()
        updated_launch = common.load_room_launch("room-launch:thread-55")
        assert updated_launch is not None
        self.assertEqual(updated_launch["message_targets"]["msg-55"], "corp/sky")

    def test_publish_binding_message_room_launch_peer_fanout_matches_source_by_session_id_when_name_missing(self) -> None:
        common.set_room_launcher(common.load_config(), "1", "22")
        common.save_room_launch(
            {
                "launch_id": "room-launch:thread-66",
                "guild_id": "1",
                "conversation_id": "22",
                "root_message_id": "root-66",
                "thread_id": "thread-66",
                "qualified_handle": "corp/sky",
                "session_alias": "dc-thread-corp-sky",
                "session_id": "gc-sky",
                "session_name": "",
                "participants": {
                    "corp/sky": {
                        "qualified_handle": "corp/sky",
                        "session_alias": "dc-thread-corp-sky",
                        "session_id": "gc-sky",
                        "session_name": "",
                    },
                    "corp/priya": {
                        "qualified_handle": "corp/priya",
                        "session_alias": "dc-thread-corp-priya",
                        "session_id": "gc-priya",
                        "session_name": "s-gc-priya",
                    },
                },
                "state": "active",
            }
        )
        binding = common.resolve_publish_route(common.load_config(), "launch-room:22")
        assert binding is not None
        os.environ.pop("GC_SESSION_NAME", None)
        os.environ["GC_SESSION_ID"] = "gc-sky"

        with mock.patch.object(common, "post_channel_message", return_value={"id": "msg-66"}), mock.patch.object(
            common,
            "deliver_session_message",
            return_value={"status": "accepted", "id": "gc-priya"},
        ) as deliver_session_message, mock.patch.object(
            common,
            "list_city_sessions",
            return_value=[
                {"id": "gc-sky", "alias": "dc-thread-corp-sky", "session_name": "", "state": "active", "running": True, "created_at": "2026-03-22T00:00:00Z"},
                {"id": "gc-priya", "alias": "dc-thread-corp-priya", "session_name": "s-gc-priya", "state": "active", "running": True, "created_at": "2026-03-22T00:00:00Z"},
            ],
        ):
            payload = common.publish_binding_message(
                binding,
                "@@corp/priya take a look at this",
                requested_conversation_id="thread-66",
                trigger_id="peer-66",
                source_context={
                    "kind": "discord_human_message",
                    "ingress_receipt_id": "in-66",
                    "publish_binding_id": "launch-room:22",
                    "publish_conversation_id": "thread-66",
                    "publish_trigger_id": "peer-66",
                    "publish_reply_to_discord_message_id": "peer-66",
                    "publish_launch_id": "room-launch:thread-66",
                },
            )

        record = payload["record"]
        self.assertEqual(record["source_qualified_handle"], "corp/sky")
        self.assertEqual(record["source_session_id"], "gc-sky")
        self.assertEqual(record["launch_id"], "room-launch:thread-66")
        self.assertNotIn("peer_delivery", record)
        deliver_session_message.assert_not_called()
        updated_launch = common.load_room_launch("room-launch:thread-66")
        assert updated_launch is not None
        self.assertEqual(updated_launch["message_targets"]["msg-66"], "corp/sky")

    def test_publish_binding_message_room_launch_peer_fanout_targets_handle_when_target_name_missing(self) -> None:
        common.set_room_launcher(common.load_config(), "1", "22")
        common.save_room_launch(
            {
                "launch_id": "room-launch:thread-67",
                "guild_id": "1",
                "conversation_id": "22",
                "root_message_id": "root-67",
                "thread_id": "thread-67",
                "qualified_handle": "corp/sky",
                "session_alias": "dc-thread-corp-sky",
                "session_id": "gc-sky",
                "session_name": "s-gc-sky",
                "participants": {
                    "corp/sky": {
                        "qualified_handle": "corp/sky",
                        "session_alias": "dc-thread-corp-sky",
                        "session_id": "gc-sky",
                        "session_name": "s-gc-sky",
                    },
                    "corp/priya": {
                        "qualified_handle": "corp/priya",
                        "session_alias": "dc-thread-corp-priya",
                        "session_id": "gc-priya",
                        "session_name": "",
                    },
                },
                "state": "active",
            }
        )
        binding = common.resolve_publish_route(common.load_config(), "launch-room:22")
        assert binding is not None
        os.environ["GC_SESSION_NAME"] = "s-gc-sky"
        os.environ["GC_SESSION_ID"] = "gc-sky"

        with mock.patch.object(common, "post_channel_message", return_value={"id": "msg-67"}), mock.patch.object(
            common,
            "deliver_session_message",
            return_value={"status": "accepted", "id": "gc-priya"},
        ) as deliver_session_message, mock.patch.object(
            common,
            "list_city_sessions",
            return_value=[
                {"id": "gc-sky", "alias": "dc-thread-corp-sky", "session_name": "s-gc-sky", "state": "active", "running": True, "created_at": "2026-03-22T00:00:00Z"},
                {"id": "gc-priya", "alias": "dc-thread-corp-priya", "session_name": "", "state": "active", "running": True, "created_at": "2026-03-22T00:00:00Z"},
            ],
        ):
            payload = common.publish_binding_message(
                binding,
                "@@corp/priya take a look at this",
                requested_conversation_id="thread-67",
                trigger_id="peer-67",
                source_context={
                    "kind": "discord_human_message",
                    "ingress_receipt_id": "in-67",
                    "publish_binding_id": "launch-room:22",
                    "publish_conversation_id": "thread-67",
                    "publish_trigger_id": "peer-67",
                    "publish_reply_to_discord_message_id": "peer-67",
                    "publish_launch_id": "room-launch:thread-67",
                },
            )

        record = payload["record"]
        self.assertEqual(record["source_qualified_handle"], "corp/sky")
        self.assertEqual(record["launch_id"], "room-launch:thread-67")
        self.assertNotIn("peer_delivery", record)
        deliver_session_message.assert_not_called()
        updated_launch = common.load_room_launch("room-launch:thread-67")
        assert updated_launch is not None
        self.assertEqual(updated_launch["message_targets"]["msg-67"], "corp/sky")

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

        self.assertNotIn("peer_delivery", payload["record"])
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
        now = common.utcnow()
        common.save_chat_publish(
            {
                "publish_id": "discord-publish-1",
                "binding_id": "room:22",
                "root_ingress_receipt_id": "in-1",
                "source_session_name": "corp--sky",
                "source_event_kind": "discord_peer_publication",
                "created_at": now,
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
                "created_at": now,
                "peer_delivery": {"frozen_targets": ["corp--lawrence"]},
            }
        )

        self.assertEqual(common._count_root_peer_triggered_publishes("room:22", "in-1", "corp--sky"), 2)
        self.assertEqual(common._count_root_peer_deliveries_from_index("room:22", "in-1"), 3)

    def _save_peer_retry_record(self, publish_id: str, target: dict[str, object]) -> None:
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
                "publish_id": publish_id,
                "binding_id": "room:22",
                "binding_kind": "room",
                "binding_conversation_id": "22",
                "conversation_id": "22",
                "guild_id": "1",
                "source_session_name": "corp--sky",
                "source_session_id": "gc-sky",
                "source_event_kind": "discord_human_message",
                "root_ingress_receipt_id": f"in-{publish_id}",
                "body": "@corp--priya hello",
                "remote_message_id": f"msg-{publish_id}",
                "peer_delivery": {
                    "phase": "peer_fanout_partial_failure",
                    "status": "partial_failure",
                    "delivery": "targeted",
                    "mentioned_session_names": ["corp--priya"],
                    "frozen_targets": ["corp--priya"],
                    "targets": [target],
                    "budget_snapshot": {},
                },
            }
        )

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
        self.assertEqual(
            deliver_session_message.call_args.kwargs["async_timeout"],
            common.PEER_DELIVERY_TIMEOUT_SECONDS,
        )

    def test_retry_peer_fanout_preserves_async_correlation_when_result_is_unknown(self) -> None:
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
                "publish_id": "discord-publish-unknown",
                "binding_id": "room:22",
                "binding_kind": "room",
                "binding_conversation_id": "22",
                "conversation_id": "22",
                "guild_id": "1",
                "source_session_name": "corp--sky",
                "source_session_id": "gc-sky",
                "source_event_kind": "discord_human_message",
                "root_ingress_receipt_id": "in-unknown",
                "body": "@corp--priya hello",
                "remote_message_id": "msg-unknown",
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
                            "attempts": [],
                        }
                    ],
                    "budget_snapshot": {},
                },
            }
        )

        def unknown_after_acceptance(*args: object, **kwargs: object) -> dict[str, object]:
            kwargs["on_async_accepted"](
                {
                    "http_status": 202,
                    "request_id": "req-peer-unknown",
                    "event_cursor": "71",
                    "intent": "default",
                    "response": {
                        "status": "accepted",
                        "request_id": "req-peer-unknown",
                        "event_cursor": "71",
                    },
                }
            )
            raise common.GCAPIResultUnknown("peer result stream closed")

        with mock.patch.object(
            common,
            "deliver_session_message",
            side_effect=unknown_after_acceptance,
        ) as deliver_session_message:
            record = common.retry_peer_fanout("discord-publish-unknown")

        target = record["peer_delivery"]["targets"][0]
        self.assertEqual(record["peer_delivery"]["phase"], "peer_fanout_in_progress")
        self.assertEqual(target["status"], "awaiting_result")
        self.assertEqual(target["request_id"], "req-peer-unknown")
        self.assertEqual(target["event_cursor"], "71")
        self.assertEqual(
            deliver_session_message.call_args.kwargs["async_timeout"],
            common.PEER_DELIVERY_TIMEOUT_SECONDS,
        )

    def test_retry_peer_fanout_explicitly_redrives_uncorrelated_unknown(self) -> None:
        self._save_peer_retry_record(
            "discord-publish-explicit-unknown",
            {
                "session_name": "corp--priya",
                "delivery_selector": "corp--priya",
                "status": "delivery_unknown",
                "attempt_count": 1,
                "attempts": [],
            },
        )

        with mock.patch.object(
            common,
            "deliver_session_message",
            return_value={"status": "accepted"},
        ) as deliver_session_message:
            record = common.retry_peer_fanout(
                "discord-publish-explicit-unknown",
                include_unknown=True,
            )

        deliver_session_message.assert_called_once()
        self.assertEqual(record["peer_delivery"]["targets"][0]["status"], "delivered")

    def test_retry_peer_fanout_clears_stale_async_ref_before_new_post(self) -> None:
        self._save_peer_retry_record(
            "discord-publish-stale-ref",
            {
                "session_name": "corp--priya",
                "delivery_selector": "corp--priya",
                "status": "failed_retryable",
                "attempt_count": 1,
                "request_id": "req-old",
                "event_cursor": "81",
                "response": {"status": "accepted", "request_id": "req-old", "event_cursor": "81"},
                "terminal_evidence": {"status": "failed"},
                "attempts": [],
            },
        )

        with mock.patch.object(
            common,
            "deliver_session_message",
            side_effect=common.GCAPIResultUnknown("new POST outcome unknown"),
        ) as deliver_session_message, mock.patch.object(
            common,
            "resume_session_message_delivery",
        ) as resume_session_message_delivery:
            record = common.retry_peer_fanout("discord-publish-stale-ref")

        deliver_session_message.assert_called_once()
        resume_session_message_delivery.assert_not_called()
        target = record["peer_delivery"]["targets"][0]
        self.assertEqual(target["status"], "delivery_unknown")
        self.assertEqual(target["request_id"], "")
        self.assertEqual(target["event_cursor"], "")
        self.assertEqual(target["terminal_evidence"], {})

    def test_peer_in_progress_staleness_exceeds_post_and_result_waits(self) -> None:
        self.assertGreater(
            common.PEER_IN_PROGRESS_STALE_SECONDS,
            common.PEER_DELIVERY_TIMEOUT_SECONDS * 2,
        )

    def test_retry_peer_fanout_resumes_awaiting_result_without_resubmitting(self) -> None:
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
                "publish_id": "discord-publish-awaiting",
                "binding_id": "room:22",
                "binding_kind": "room",
                "binding_conversation_id": "22",
                "conversation_id": "22",
                "guild_id": "1",
                "source_session_name": "corp--sky",
                "source_session_id": "gc-sky",
                "source_event_kind": "discord_human_message",
                "root_ingress_receipt_id": "in-awaiting",
                "body": "@corp--priya hello",
                "remote_message_id": "msg-awaiting",
                "peer_delivery": {
                    "phase": "peer_fanout_in_progress",
                    "status": "",
                    "delivery": "targeted",
                    "mentioned_session_names": ["corp--priya"],
                    "frozen_targets": ["corp--priya"],
                    "targets": [
                        {
                            "session_name": "corp--priya",
                            "delivery_selector": "corp--priya",
                            "status": "awaiting_result",
                            "attempt_count": 1,
                            "request_id": "req-peer",
                            "event_cursor": "61",
                            "intent": "default",
                            "response": {"status": "accepted", "request_id": "req-peer", "event_cursor": "61"},
                            "attempts": [],
                        }
                    ],
                    "budget_snapshot": {},
                },
            }
        )
        terminal = {"request_id": "req-peer", "session_id": "gc-priya"}

        with mock.patch.object(
            common,
            "resume_session_message_delivery",
            return_value=terminal,
        ) as resume_session_message_delivery, mock.patch.object(
            common,
            "deliver_session_message",
        ) as deliver_session_message:
            record = common.retry_peer_fanout("discord-publish-awaiting")

        deliver_session_message.assert_not_called()
        resume_session_message_delivery.assert_called_once_with(
            "req-peer",
            "61",
            intent="default",
            timeout=common.PEER_DELIVERY_TIMEOUT_SECONDS,
        )
        target = record["peer_delivery"]["targets"][0]
        self.assertEqual(target["status"], "delivered")
        self.assertEqual(target["terminal_evidence"], {"status": "succeeded", "payload": terminal})

    def test_retry_peer_fanout_room_launch_preserves_launch_context(self) -> None:
        common.set_room_launcher(common.load_config(), "1", "22")
        common.save_room_launch(
            {
                "launch_id": "room-launch:thread-77",
                "guild_id": "1",
                "conversation_id": "22",
                "root_message_id": "root-77",
                "thread_id": "thread-77",
                "qualified_handle": "corp/sky",
                "session_alias": "dc-thread-corp-sky",
                "session_id": "gc-sky",
                "session_name": "",
                "participants": {
                    "corp/sky": {
                        "qualified_handle": "corp/sky",
                        "session_alias": "dc-thread-corp-sky",
                        "session_id": "gc-sky",
                        "session_name": "",
                    },
                    "corp/priya": {
                        "qualified_handle": "corp/priya",
                        "session_alias": "dc-thread-corp-priya",
                        "session_id": "gc-priya",
                        "session_name": "s-gc-priya",
                    },
                },
                "state": "active",
            }
        )
        common.save_chat_publish(
            {
                "publish_id": "discord-publish-launch",
                "binding_id": "launch-room:22",
                "binding_kind": "room",
                "binding_conversation_id": "22",
                "conversation_id": "thread-77",
                "guild_id": "1",
                "source_session_name": "",
                "source_session_id": "gc-sky",
                "source_event_kind": "discord_human_message",
                "root_ingress_receipt_id": "in-77",
                "launch_id": "room-launch:thread-77",
                "body": "@@corp/priya hello",
                "remote_message_id": "msg-77",
                "peer_delivery": {
                    "phase": "peer_fanout_partial_failure",
                    "status": "partial_failure",
                    "delivery": "targeted",
                    "mentioned_session_names": ["s-gc-priya"],
                    "frozen_targets": ["s-gc-priya"],
                    "targets": [
                        {
                            "session_name": "s-gc-priya",
                            "status": "failed_retryable",
                            "attempt_count": 1,
                            "idempotency_key": "peer_publish:discord-publish-launch:binding:launch-room:22:target:s-gc-priya",
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
            record = common.retry_peer_fanout("discord-publish-launch")

        self.assertEqual(record["peer_delivery"]["status"], "delivered")
        post_channel_message.assert_not_called()
        deliver_session_message.assert_called_once()
        envelope = deliver_session_message.call_args.args[1]
        self.assertIn("publish_launch_id: room-launch:thread-77", envelope)
        self.assertIn("launch_id: room-launch:thread-77", envelope)
        self.assertIn("launch_qualified_handle: corp/priya", envelope)
        self.assertIn("thread_participants_json:", envelope)

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

    def test_discord_api_request_explicit_empty_token_never_falls_back_to_default(self) -> None:
        common.save_bot_token("default-test-token")
        success = mock.Mock()
        success.__enter__ = mock.Mock(return_value=mock.Mock(read=mock.Mock(return_value=b'{}')))
        success.__exit__ = mock.Mock(return_value=False)

        with mock.patch.object(common.urllib.request, "urlopen", return_value=success) as urlopen:
            common.discord_api_request("GET", "/channels/1", bot_token="")

        request = urlopen.call_args.args[0]
        self.assertNotIn("Authorization", request.headers)


if __name__ == "__main__":
    unittest.main()
