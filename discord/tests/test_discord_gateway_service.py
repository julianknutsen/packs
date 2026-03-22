from __future__ import annotations

import pathlib
import struct
import tempfile
import threading
import time
import unittest
from unittest import mock

import os
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "scripts"))

import discord_gateway_service as gateway_service
import discord_intake_common as common


class DiscordGatewayServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self._old_environ = os.environ.copy()
        os.environ["GC_CITY_ROOT"] = self.tempdir.name
        gateway_service.CHANNEL_INFO_CACHE.clear()
        gateway_service.CHANNEL_INFO_FETCH_LOCKS.clear()
        gateway_service.STALE_RECLAIM_LOCKS.clear()
        gateway_service.INGRESS_PROCESS_LOCKS.clear()
        gateway_service.GC_API_HEALTH_CACHE["checked_at"] = 0.0
        gateway_service.GC_API_HEALTH_CACHE["reachable"] = True
        gateway_service.AMBIENT_ROOM_BINDINGS_CACHE["config_signature"] = None
        gateway_service.AMBIENT_ROOM_BINDINGS_CACHE["bindings"] = {}

    def tearDown(self) -> None:
        os.environ.clear()
        os.environ.update(self._old_environ)

    def test_gateway_does_not_require_message_content_intent(self) -> None:
        self.assertFalse(gateway_service.GATEWAY_INTENTS & (1 << 15))

    def test_display_name_from_message_skips_none_strings(self) -> None:
        message = {
            "author": {"username": None, "global_name": None},
            "member": {"nick": None},
        }
        self.assertEqual(gateway_service.display_name_from_message(message), "discord-user")

    def test_process_inbound_dm_routes_to_bound_session(self) -> None:
        common.set_chat_binding(common.load_config(), "dm", "55", ["sky"])
        message = {
            "id": "101",
            "channel_id": "55",
            "content": "hello from discord",
            "author": {"id": "u-1", "username": "alice"},
        }

        with mock.patch.object(common, "session_index_by_name", return_value={"sky": {"session_name": "sky", "state": "suspended"}}), mock.patch.object(
            common,
            "deliver_session_message",
            return_value={"status": "accepted", "id": "gc-1"},
        ) as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "delivered")
        deliver_session_message.assert_called_once()
        self.assertEqual(deliver_session_message.call_args.args[0], "sky")
        envelope = deliver_session_message.call_args.args[1]
        self.assertIn("kind: discord_human_message", envelope)
        self.assertIn('untrusted_body_json: "hello from discord"', envelope)
        self.assertEqual(common.load_chat_ingress("in-101")["status"], "delivered")

    def test_process_inbound_room_message_targets_only_named_alias(self) -> None:
        common.set_chat_binding(common.load_config(), "room", "22", ["sky", "lawrence"], guild_id="1")
        message = {
            "id": "202",
            "guild_id": "1",
            "channel_id": "22",
            "content": "<@999> @Sky please check the shard",
            "mentions": [{"id": "999"}],
            "author": {"id": "u-2", "username": "alice"},
            "member": {"nick": "alice"},
        }

        with mock.patch.object(
            common,
            "session_index_by_name",
            return_value={
                "sky": {"session_name": "sky", "state": "active"},
                "lawrence": {"session_name": "lawrence", "state": "active"},
            },
        ), mock.patch.object(common, "deliver_session_message", return_value={"status": "accepted"}) as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "delivered")
        deliver_session_message.assert_called_once()
        self.assertEqual(deliver_session_message.call_args.args[0], "sky")
        receipt = common.load_chat_ingress("in-202")
        self.assertEqual(receipt["delivery"], "targeted")
        self.assertEqual(receipt["mentioned_aliases"], ["sky"])

    def test_process_inbound_room_message_matches_session_names_case_insensitively(self) -> None:
        common.set_chat_binding(common.load_config(), "room", "22", ["Sky"], guild_id="1")
        message = {
            "id": "207",
            "guild_id": "1",
            "channel_id": "22",
            "content": "<@999> @sky please check the shard",
            "mentions": [{"id": "999"}],
            "author": {"id": "u-2", "username": "alice"},
        }

        with mock.patch.object(
            common,
            "session_index_by_name",
            return_value={"Sky": {"session_name": "Sky", "state": "active"}},
        ), mock.patch.object(common, "deliver_session_message", return_value={"status": "accepted"}) as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "delivered")
        deliver_session_message.assert_called_once()
        self.assertEqual(deliver_session_message.call_args.args[0], "Sky")

    def test_process_inbound_room_launch_routes_handle_without_bot_mention(self) -> None:
        common.set_room_launcher(common.load_config(), "1", "22")
        message = {
            "id": "208",
            "guild_id": "1",
            "channel_id": "22",
            "content": "@@sky please help",
            "author": {"id": "u-208", "username": "alice"},
        }

        with mock.patch.object(common, "resolve_agent_handle", return_value=("corp/sky", "")), mock.patch.object(
            common,
            "ensure_room_launch_session",
            return_value={
                "launch_id": "room-launch:208",
                "qualified_handle": "corp/sky",
                "delivery_selector": "s-gc-123",
                "session_alias": "dc-123-sky",
                "session_name": "s-gc-123",
                "session_id": "gc-123",
            },
        ), mock.patch.object(common, "deliver_session_message", return_value={"status": "accepted"}) as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "delivered")
        deliver_session_message.assert_called_once()
        self.assertEqual(deliver_session_message.call_args.args[0], "s-gc-123")
        envelope = deliver_session_message.call_args.args[1]
        self.assertIn("binding_id: launch-room:22", envelope)
        self.assertIn("launch_id: room-launch:208", envelope)
        self.assertIn("launch_session_alias: dc-123-sky", envelope)
        receipt = common.load_chat_ingress("in-208")
        assert receipt is not None
        self.assertEqual(receipt["launch_id"], "room-launch:208")

    def test_process_inbound_room_launch_recovers_empty_guild_content_via_rest(self) -> None:
        common.set_room_launcher(common.load_config(), "1", "22")
        message = {
            "id": "208b",
            "guild_id": "1",
            "channel_id": "22",
            "content": "",
            "author": {"id": "u-208b", "username": None},
        }

        with mock.patch.object(
            common,
            "discord_api_request",
            return_value={
                "id": "208b",
                "channel_id": "22",
                "content": "@@corp/sky please help",
                "author": {"id": "u-208b", "username": "alice"},
                "mentions": [],
            },
        ), mock.patch.object(common, "resolve_agent_handle", return_value=("corp/sky", "")), mock.patch.object(
            common,
            "ensure_room_launch_session",
            return_value={
                "launch_id": "room-launch:208b",
                "qualified_handle": "corp/sky",
                "delivery_selector": "s-gc-123",
                "session_alias": "dc-123-sky",
                "session_name": "s-gc-123",
                "session_id": "gc-123",
            },
        ), mock.patch.object(common, "deliver_session_message", return_value={"status": "accepted"}) as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "delivered")
        deliver_session_message.assert_called_once()
        self.assertEqual(deliver_session_message.call_args.args[0], "s-gc-123")
        envelope = deliver_session_message.call_args.args[1]
        self.assertIn('untrusted_body_json: "@@corp/sky please help"', envelope)
        receipt = common.load_chat_ingress("in-208b")
        assert receipt is not None
        self.assertEqual(receipt["body_preview"], "@@corp/sky please help")
        self.assertEqual(receipt["from_display"], "alice")
        self.assertEqual((receipt.get("message_debug") or {}).get("content_source"), "rest_fallback")

    def test_process_inbound_room_launch_marks_guild_empty_content_unavailable(self) -> None:
        common.set_room_launcher(common.load_config(), "1", "22")
        message = {
            "id": "208c",
            "guild_id": "1",
            "channel_id": "22",
            "content": "",
            "author": {"id": "u-208c", "username": "alice"},
        }

        with mock.patch.object(common, "discord_api_request", return_value=[]), mock.patch.object(
            common, "deliver_session_message"
        ) as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "ignored_empty")
        deliver_session_message.assert_not_called()
        receipt = common.load_chat_ingress("in-208c")
        assert receipt is not None
        self.assertEqual(receipt["reason"], "message_content_unavailable")
        self.assertEqual((receipt.get("message_debug") or {}).get("content_source"), "gateway_empty_rest_unavailable")

    def test_process_inbound_room_launch_ignores_unexpected_rest_recovery_failure(self) -> None:
        common.set_room_launcher(common.load_config(), "1", "22")
        message = {
            "id": "208d",
            "guild_id": "1",
            "channel_id": "22",
            "content": "",
            "author": {"id": "u-208d", "username": "alice"},
        }

        with mock.patch.object(common, "discord_api_request", side_effect=RuntimeError("boom")), mock.patch.object(
            common, "deliver_session_message"
        ) as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "ignored_empty")
        deliver_session_message.assert_not_called()
        receipt = common.load_chat_ingress("in-208d")
        assert receipt is not None
        self.assertEqual(receipt["reason"], "message_content_unavailable")
        self.assertEqual((receipt.get("message_debug") or {}).get("content_source"), "gateway_empty_rest_unavailable")

    def test_process_inbound_room_launch_thread_routes_follow_up_without_bot_mention(self) -> None:
        common.set_room_launcher(common.load_config(), "1", "22")
        common.save_room_launch(
            {
                "launch_id": "room-launch:222",
                "launcher_id": "launch-room:22",
                "guild_id": "1",
                "conversation_id": "22",
                "root_message_id": "222",
                "qualified_handle": "corp/sky",
                "session_alias": "dc-123-sky",
                "thread_id": "222",
                "state": "active",
            }
        )
        message = {
            "id": "209",
            "guild_id": "1",
            "channel_id": "222",
            "content": "follow up",
            "author": {"id": "u-209", "username": "alice"},
        }

        with mock.patch.object(
            common,
            "list_city_sessions",
            return_value=[
                {
                    "id": "gc-123",
                    "alias": "dc-123-sky",
                    "session_name": "dc-123-sky",
                    "state": "active",
                    "running": True,
                    "created_at": "2026-03-22T00:00:00Z",
                }
            ],
        ), mock.patch.object(common, "deliver_session_message", return_value={"status": "accepted"}) as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "delivered")
        deliver_session_message.assert_called_once()
        self.assertEqual(deliver_session_message.call_args.args[0], "dc-123-sky")
        envelope = deliver_session_message.call_args.args[1]
        self.assertIn("conversation: guild:1 channel:22 thread:222", envelope)
        self.assertIn("publish_conversation_id: 222", envelope)
        self.assertTrue(str(common.load_room_launch("room-launch:222").get("last_activity_at", "")).strip())

    def test_process_inbound_room_launch_rejects_ambiguous_handle(self) -> None:
        common.set_room_launcher(common.load_config(), "1", "22")
        message = {
            "id": "210",
            "guild_id": "1",
            "channel_id": "22",
            "content": "@@sky please help",
            "author": {"id": "u-210", "username": "alice"},
        }

        with mock.patch.object(common, "resolve_agent_handle", return_value=("", "ambiguous_handle")), mock.patch.object(
            common, "deliver_session_message"
        ) as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "rejected_targeting")
        deliver_session_message.assert_not_called()
        receipt = common.load_chat_ingress("in-210")
        assert receipt is not None
        self.assertEqual(receipt["reason"], "ambiguous_handle")

    def test_process_inbound_room_launch_respond_all_uses_default_handle(self) -> None:
        common.set_room_launcher(common.load_config(), "1", "22", response_mode="respond_all", default_qualified_handle="corp/sky")
        message = {
            "id": "210b",
            "guild_id": "1",
            "channel_id": "22",
            "content": "please help",
            "author": {"id": "u-210b", "username": "alice"},
        }

        with mock.patch.object(
            common,
            "ensure_room_launch_session",
            return_value={
                "launch_id": "room-launch:210b",
                "qualified_handle": "corp/sky",
                "session_alias": "dc-123-sky",
            },
        ), mock.patch.object(common, "deliver_session_message", return_value={"status": "accepted"}) as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "delivered")
        deliver_session_message.assert_called_once()
        envelope = deliver_session_message.call_args.args[1]
        self.assertIn("launch_qualified_handle: corp/sky", envelope)

    def test_process_inbound_room_launch_respond_all_honors_mixed_case_explicit_handle(self) -> None:
        common.set_room_launcher(common.load_config(), "1", "22", response_mode="respond_all", default_qualified_handle="corp/default")
        message = {
            "id": "210c",
            "guild_id": "1",
            "channel_id": "22",
            "content": "@@Sky please help",
            "author": {"id": "u-210c", "username": "alice"},
        }

        with mock.patch.object(common, "resolve_agent_handle", return_value=("corp/sky", "")), mock.patch.object(
            common,
            "ensure_room_launch_session",
            return_value={
                "launch_id": "room-launch:210c",
                "qualified_handle": "corp/sky",
                "session_alias": "dc-123-sky",
            },
        ), mock.patch.object(common, "deliver_session_message", return_value={"status": "accepted"}) as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "delivered")
        deliver_session_message.assert_called_once()
        envelope = deliver_session_message.call_args.args[1]
        self.assertIn("launch_qualified_handle: corp/sky", envelope)
        self.assertNotIn("launch_qualified_handle: corp/default", envelope)

    def test_process_inbound_room_launch_thread_retargets_to_new_handle(self) -> None:
        common.set_room_launcher(common.load_config(), "1", "22")
        common.save_room_launch(
            {
                "launch_id": "room-launch:222",
                "launcher_id": "launch-room:22",
                "guild_id": "1",
                "conversation_id": "22",
                "root_message_id": "222",
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
                "thread_id": "222",
                "state": "active",
            }
        )
        message = {
            "id": "211",
            "guild_id": "1",
            "channel_id": "222",
            "content": "@@alex please join",
            "author": {"id": "u-211", "username": "alice"},
        }

        with mock.patch.object(common, "resolve_agent_handle", return_value=("corp/alex", "")), mock.patch.object(
            common,
            "ensure_room_launch_session_for_handle",
            return_value=(
                {
                    **(common.load_room_launch("room-launch:222") or {}),
                    "participants": {
                        "corp/sky": {
                            "qualified_handle": "corp/sky",
                            "session_alias": "dc-123-sky",
                            "session_name": "dc-sky",
                            "session_id": "gc-sky",
                        },
                        "corp/alex": {
                            "delivery_selector": "dc-alex",
                            "qualified_handle": "corp/alex",
                            "session_alias": "dc-456-alex",
                            "session_name": "dc-alex",
                            "session_id": "gc-alex",
                        },
                    },
                },
                {
                    "delivery_selector": "dc-alex",
                    "qualified_handle": "corp/alex",
                    "session_alias": "dc-456-alex",
                    "session_name": "dc-alex",
                    "session_id": "gc-alex",
                },
            ),
        ), mock.patch.object(common, "set_room_launch_last_addressed"), mock.patch.object(
            common,
            "deliver_session_message",
            return_value={"status": "accepted"},
        ) as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "delivered")
        deliver_session_message.assert_called_once()
        self.assertEqual(deliver_session_message.call_args.args[0], "dc-alex")
        envelope = deliver_session_message.call_args.args[1]
        self.assertIn("launch_qualified_handle: corp/alex", envelope)
        self.assertIn('thread_participants_json: [{"qualified_handle": "corp/alex"', envelope)
        receipt = common.load_chat_ingress("in-211")
        assert receipt is not None
        self.assertEqual(receipt["routing_mode"], "explicit_handle")
        self.assertEqual(receipt["qualified_handle"], "corp/alex")

    def test_process_inbound_room_launch_thread_reply_targets_matching_agent_publish(self) -> None:
        common.set_room_launcher(common.load_config(), "1", "22")
        common.save_room_launch(
            {
                "launch_id": "room-launch:222",
                "launcher_id": "launch-room:22",
                "guild_id": "1",
                "conversation_id": "22",
                "root_message_id": "222",
                "qualified_handle": "corp/sky",
                "session_alias": "dc-123-sky",
                "session_name": "dc-sky",
                "participants": {
                    "corp/sky": {
                        "qualified_handle": "corp/sky",
                        "session_alias": "dc-123-sky",
                        "session_name": "dc-sky",
                        "session_id": "gc-sky",
                    },
                    "corp/alex": {
                        "qualified_handle": "corp/alex",
                        "session_alias": "dc-456-alex",
                        "session_name": "dc-alex",
                        "session_id": "gc-alex",
                    },
                },
                "message_targets": {"msg-agent-1": "corp/alex"},
                "thread_id": "222",
                "state": "active",
            }
        )
        message = {
            "id": "212a",
            "guild_id": "1",
            "channel_id": "222",
            "content": "what do you think?",
            "message_reference": {"message_id": "msg-agent-1"},
            "author": {"id": "u-212a", "username": "alice"},
        }

        with mock.patch.object(
            common,
            "ensure_room_launch_session_for_handle",
            return_value=(
                common.load_room_launch("room-launch:222") or {},
                {
                    "delivery_selector": "dc-alex",
                    "qualified_handle": "corp/alex",
                    "session_alias": "dc-456-alex",
                    "session_name": "dc-alex",
                    "session_id": "gc-alex",
                },
            ),
        ), mock.patch.object(common, "set_room_launch_last_addressed"), mock.patch.object(
            common,
            "deliver_session_message",
            return_value={"status": "accepted"},
        ) as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "delivered")
        self.assertEqual(deliver_session_message.call_args.args[0], "dc-alex")
        receipt = common.load_chat_ingress("in-212a")
        assert receipt is not None
        self.assertEqual(receipt["routing_mode"], "reply_to")

    def test_process_inbound_room_launch_thread_uses_last_addressed_fallback(self) -> None:
        common.set_room_launcher(common.load_config(), "1", "22")
        common.save_room_launch(
            {
                "launch_id": "room-launch:222",
                "launcher_id": "launch-room:22",
                "guild_id": "1",
                "conversation_id": "22",
                "root_message_id": "222",
                "qualified_handle": "corp/sky",
                "session_alias": "dc-123-sky",
                "session_name": "dc-sky",
                "last_addressed_qualified_handle": "corp/alex",
                "participants": {
                    "corp/sky": {
                        "qualified_handle": "corp/sky",
                        "session_alias": "dc-123-sky",
                        "session_name": "dc-sky",
                        "session_id": "gc-sky",
                    },
                    "corp/alex": {
                        "qualified_handle": "corp/alex",
                        "session_alias": "dc-456-alex",
                        "session_name": "dc-alex",
                        "session_id": "gc-alex",
                    },
                },
                "thread_id": "222",
                "state": "active",
            }
        )
        message = {
            "id": "212b",
            "guild_id": "1",
            "channel_id": "222",
            "content": "keep going",
            "author": {"id": "u-212b", "username": "alice"},
        }

        with mock.patch.object(
            common,
            "ensure_room_launch_session_for_handle",
            return_value=(
                common.load_room_launch("room-launch:222") or {},
                {
                    "delivery_selector": "dc-alex",
                    "qualified_handle": "corp/alex",
                    "session_alias": "dc-456-alex",
                    "session_name": "dc-alex",
                    "session_id": "gc-alex",
                },
            ),
        ), mock.patch.object(common, "set_room_launch_last_addressed"), mock.patch.object(
            common,
            "deliver_session_message",
            return_value={"status": "accepted"},
        ) as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "delivered")
        self.assertEqual(deliver_session_message.call_args.args[0], "dc-alex")
        receipt = common.load_chat_ingress("in-212b")
        assert receipt is not None
        self.assertEqual(receipt["routing_mode"], "last_addressed")

    def test_process_inbound_thread_message_inherits_parent_room_binding(self) -> None:
        common.set_chat_binding(common.load_config(), "room", "22", ["sky"], guild_id="1")
        common.save_bot_token("bot-token")
        message = {
            "id": "212",
            "guild_id": "1",
            "channel_id": "222",
            "content": "<@999> can you take a look?",
            "mentions": [{"id": "999"}],
            "author": {"id": "u-22", "username": "alice"},
        }

        with mock.patch.object(common, "discord_api_request", return_value={"id": "222", "parent_id": "22", "type": 11}), mock.patch.object(
            common,
            "session_index_by_name",
            return_value={"sky": {"session_name": "sky", "state": "active"}},
        ), mock.patch.object(common, "deliver_session_message", return_value={"status": "accepted"}) as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "delivered")
        deliver_session_message.assert_called_once()
        self.assertEqual(deliver_session_message.call_args.args[0], "sky")
        envelope = deliver_session_message.call_args.args[1]
        self.assertIn("binding_id: room:22", envelope)
        self.assertIn("conversation: guild:1 channel:22 thread:222", envelope)
        self.assertIn("publish_conversation_id: 222", envelope)
        self.assertIn("publish_trigger_id: 212", envelope)

    def test_process_inbound_thread_message_inherits_parent_room_binding_when_lookup_omits_type(self) -> None:
        common.set_chat_binding(common.load_config(), "room", "22", ["sky"], guild_id="1")
        common.save_bot_token("bot-token")
        message = {
            "id": "212b",
            "guild_id": "1",
            "channel_id": "222",
            "content": "<@999> can you take a look?",
            "mentions": [{"id": "999"}],
            "author": {"id": "u-22b", "username": "alice"},
        }

        with mock.patch.object(common, "discord_api_request", return_value={"id": "222", "parent_id": "22"}), mock.patch.object(
            common,
            "session_index_by_name",
            return_value={"sky": {"session_name": "sky", "state": "active"}},
        ), mock.patch.object(common, "deliver_session_message", return_value={"status": "accepted"}) as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "delivered")
        deliver_session_message.assert_called_once()
        envelope = deliver_session_message.call_args.args[1]
        self.assertIn("binding_id: room:22", envelope)
        self.assertIn("conversation: guild:1 channel:22 thread:222", envelope)

    def test_process_inbound_thread_message_marks_lookup_failure_as_retryable(self) -> None:
        common.save_bot_token("bot-token")
        message = {
            "id": "213",
            "guild_id": "1",
            "channel_id": "222",
            "content": "<@999> can you take a look?",
            "mentions": [{"id": "999"}],
            "author": {"id": "u-23", "username": "alice"},
        }

        with mock.patch.object(common, "discord_api_request", side_effect=common.DiscordAPIError("GET channel failed", status_code=500)), mock.patch.object(
            common,
            "deliver_session_message",
        ) as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "failed_lookup")
        deliver_session_message.assert_not_called()
        receipt = common.load_chat_ingress("in-213")
        assert receipt is not None
        self.assertEqual(receipt["status"], "failed_lookup")

    def test_process_inbound_room_message_skips_closed_participants(self) -> None:
        common.set_chat_binding(common.load_config(), "room", "22", ["sky", "lawrence"], guild_id="1")
        message = {
            "id": "214",
            "guild_id": "1",
            "channel_id": "22",
            "content": "<@999> please investigate",
            "mentions": [{"id": "999"}],
            "author": {"id": "u-24", "username": "alice"},
        }

        with mock.patch.object(
            common,
            "session_index_by_name",
            return_value={
                "sky": {"session_name": "sky", "state": "active"},
                "lawrence": {"session_name": "lawrence", "state": "closed"},
            },
        ), mock.patch.object(common, "deliver_session_message", return_value={"status": "accepted"}) as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "delivered")
        deliver_session_message.assert_called_once()
        self.assertEqual(deliver_session_message.call_args.args[0], "sky")

    def test_process_inbound_room_message_rejects_unknown_alias(self) -> None:
        common.set_chat_binding(common.load_config(), "room", "22", ["sky", "lawrence"], guild_id="1")
        message = {
            "id": "303",
            "guild_id": "1",
            "channel_id": "22",
            "content": "<@999> @ghost please check the shard",
            "mentions": [{"id": "999"}],
            "author": {"id": "u-3", "username": "alice"},
        }

        with mock.patch.object(
            common,
            "session_index_by_name",
            return_value={
                "sky": {"session_name": "sky", "state": "active"},
                "lawrence": {"session_name": "lawrence", "state": "active"},
            },
        ), mock.patch.object(common, "deliver_session_message") as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "rejected_targeting")
        deliver_session_message.assert_not_called()
        receipt = common.load_chat_ingress("in-303")
        self.assertEqual(receipt["reason"], "unknown_alias:ghost")

    def test_process_inbound_message_dedupes_existing_ingress(self) -> None:
        common.save_chat_ingress({"ingress_id": "in-404", "status": "delivered"})
        message = {
            "id": "404",
            "channel_id": "55",
            "content": "hello from discord",
            "author": {"id": "u-4", "username": "alice"},
        }

        with mock.patch.object(common, "deliver_session_message") as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "duplicate")
        deliver_session_message.assert_not_called()

    def test_process_inbound_room_message_ignores_non_mentions(self) -> None:
        common.set_chat_binding(common.load_config(), "room", "22", ["sky", "lawrence"], guild_id="1")
        message = {
            "id": "505",
            "guild_id": "1",
            "channel_id": "22",
            "content": "just chatting here",
            "mentions": [],
            "author": {"id": "u-5", "username": "alice"},
        }

        with mock.patch.object(common, "deliver_session_message") as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "ignored")
        self.assertEqual(outcome["reason"], "not_mentioned")
        deliver_session_message.assert_not_called()

    def test_process_inbound_ambient_room_message_routes_targeted_alias_without_bot_mention(self) -> None:
        common.set_chat_binding(
            common.load_config(),
            "room",
            "22",
            ["sky", "lawrence"],
            guild_id="1",
            policy={"ambient_read_enabled": True},
        )
        message = {
            "id": "506",
            "guild_id": "1",
            "channel_id": "22",
            "content": "@Sky please check the shard",
            "mentions": [],
            "author": {"id": "u-5a", "username": "alice"},
        }

        with mock.patch.object(
            common,
            "session_index_by_name",
            return_value={
                "sky": {"session_name": "sky", "state": "active"},
                "lawrence": {"session_name": "lawrence", "state": "active"},
            },
        ), mock.patch.object(common, "deliver_session_message", return_value={"status": "accepted"}) as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "delivered")
        deliver_session_message.assert_called_once()
        self.assertEqual(deliver_session_message.call_args.args[0], "sky")
        receipt = common.load_chat_ingress("in-506")
        assert receipt is not None
        self.assertEqual(receipt["delivery"], "targeted")
        self.assertEqual(receipt["mentioned_aliases"], ["sky"])

    def test_process_inbound_ambient_thread_message_hydrates_thread_context_on_delivery(self) -> None:
        common.set_chat_binding(
            common.load_config(),
            "room",
            "222",
            ["sky"],
            guild_id="1",
            policy={"ambient_read_enabled": True},
            channel_metadata={"channel_type": 11, "thread_parent_id": "22"},
        )
        message = {
            "id": "506b",
            "guild_id": "1",
            "channel_id": "222",
            "content": "@sky please check the shard",
            "mentions": [],
            "author": {"id": "u-5aa", "username": "alice"},
        }

        with mock.patch.object(common, "discord_api_request") as discord_api_request, mock.patch.object(
            common,
            "session_index_by_name",
            return_value={"sky": {"session_name": "sky", "state": "active"}},
        ), mock.patch.object(common, "deliver_session_message", return_value={"status": "accepted"}) as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "delivered")
        discord_api_request.assert_not_called()
        deliver_session_message.assert_called_once()
        envelope = deliver_session_message.call_args.args[1]
        self.assertIn("conversation: guild:1 channel:22 thread:222", envelope)

    def test_process_inbound_ambient_thread_missing_parent_metadata_falls_back_to_lookup(self) -> None:
        common.set_chat_binding(
            common.load_config(),
            "room",
            "222",
            ["sky"],
            guild_id="1",
            policy={"ambient_read_enabled": True},
            channel_metadata={"channel_type": 11},
        )
        common.save_bot_token("bot-token")
        message = {
            "id": "506bb",
            "guild_id": "1",
            "channel_id": "222",
            "content": "@sky please check the shard",
            "mentions": [],
            "author": {"id": "u-5aab", "username": "alice"},
        }

        with mock.patch.object(common, "discord_api_request", return_value={"id": "222", "parent_id": "22", "type": 11}) as discord_api_request, mock.patch.object(
            common,
            "session_index_by_name",
            return_value={"sky": {"session_name": "sky", "state": "active"}},
        ), mock.patch.object(common, "deliver_session_message", return_value={"status": "accepted"}) as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "delivered")
        discord_api_request.assert_called_once()
        deliver_session_message.assert_called_once()
        envelope = deliver_session_message.call_args.args[1]
        self.assertIn("conversation: guild:1 channel:22 thread:222", envelope)

    def test_process_inbound_bot_mentioned_ambient_thread_uses_binding_thread_metadata(self) -> None:
        common.set_chat_binding(
            common.load_config(),
            "room",
            "222",
            ["sky"],
            guild_id="1",
            policy={"ambient_read_enabled": True},
            channel_metadata={"channel_type": 11, "thread_parent_id": "22"},
        )
        message = {
            "id": "506c",
            "guild_id": "1",
            "channel_id": "222",
            "content": "<@999> @sky please check the shard",
            "mentions": [{"id": "999"}],
            "author": {"id": "u-5ab", "username": "alice"},
        }

        with mock.patch.object(common, "discord_api_request") as discord_api_request, mock.patch.object(
            common,
            "session_index_by_name",
            return_value={"sky": {"session_name": "sky", "state": "active"}},
        ), mock.patch.object(common, "deliver_session_message", return_value={"status": "accepted"}) as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "delivered")
        discord_api_request.assert_not_called()
        deliver_session_message.assert_called_once()
        envelope = deliver_session_message.call_args.args[1]
        self.assertIn("conversation: guild:1 channel:22 thread:222", envelope)

    def test_process_inbound_direct_thread_binding_missing_parent_metadata_falls_back_to_lookup(self) -> None:
        common.set_chat_binding(
            common.load_config(),
            "room",
            "222",
            ["sky"],
            guild_id="1",
            channel_metadata={"channel_type": 11},
        )
        common.save_bot_token("bot-token")
        message = {
            "id": "506d",
            "guild_id": "1",
            "channel_id": "222",
            "content": "<@999> @sky please check the shard",
            "mentions": [{"id": "999"}],
            "author": {"id": "u-5ac", "username": "alice"},
        }

        with mock.patch.object(common, "discord_api_request", return_value={"id": "222", "parent_id": "22", "type": 11}) as discord_api_request, mock.patch.object(
            common,
            "session_index_by_name",
            return_value={"sky": {"session_name": "sky", "state": "active"}},
        ), mock.patch.object(common, "deliver_session_message", return_value={"status": "accepted"}) as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "delivered")
        discord_api_request.assert_called_once()
        deliver_session_message.assert_called_once()
        envelope = deliver_session_message.call_args.args[1]
        self.assertIn("conversation: guild:1 channel:22 thread:222", envelope)

    def test_process_inbound_ambient_room_message_ignores_untargeted_body(self) -> None:
        common.set_chat_binding(
            common.load_config(),
            "room",
            "22",
            ["sky", "lawrence"],
            guild_id="1",
            policy={"ambient_read_enabled": True},
        )
        message = {
            "id": "507",
            "guild_id": "1",
            "channel_id": "22",
            "content": "just chatting here",
            "mentions": [],
            "author": {"id": "u-5b", "username": "alice"},
        }

        with mock.patch.object(common, "session_index_by_name") as session_index_by_name, mock.patch.object(
            common, "deliver_session_message"
        ) as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "ignored_untargeted")
        session_index_by_name.assert_not_called()
        deliver_session_message.assert_not_called()
        receipt = common.load_chat_ingress("in-507")
        assert receipt is not None
        self.assertEqual(receipt["status"], "ignored_untargeted")
        self.assertEqual(receipt["reason"], "ambient_target_required")

    def test_process_inbound_ambient_room_message_ignores_unknown_alias_with_receipt(self) -> None:
        common.set_chat_binding(
            common.load_config(),
            "room",
            "22",
            ["sky", "lawrence"],
            guild_id="1",
            policy={"ambient_read_enabled": True},
        )
        message = {
            "id": "507a",
            "guild_id": "1",
            "channel_id": "22",
            "content": "@ghost please check the shard",
            "mentions": [],
            "author": {"id": "u-5ba", "username": "alice"},
        }

        with mock.patch.object(common, "session_index_by_name") as session_index_by_name, mock.patch.object(
            common, "deliver_session_message"
        ) as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "ignored_untargeted")
        session_index_by_name.assert_not_called()
        deliver_session_message.assert_not_called()
        receipt = common.load_chat_ingress("in-507a")
        assert receipt is not None
        self.assertEqual(receipt["status"], "ignored_untargeted")
        self.assertEqual(receipt["reason"], "ambient_target_required")

    def test_process_inbound_ambient_room_message_dedupes_replayed_unknown_alias_after_binding_changes(self) -> None:
        common.set_chat_binding(
            common.load_config(),
            "room",
            "22",
            ["sky", "lawrence"],
            guild_id="1",
            policy={"ambient_read_enabled": True},
        )
        message = {
            "id": "507aa",
            "guild_id": "1",
            "channel_id": "22",
            "content": "@ghost please check the shard",
            "mentions": [],
            "author": {"id": "u-5baa", "username": "alice"},
        }

        with mock.patch.object(common, "session_index_by_name") as session_index_by_name, mock.patch.object(
            common, "deliver_session_message"
        ) as deliver_session_message:
            first_outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(first_outcome["status"], "ignored_untargeted")
        session_index_by_name.assert_not_called()
        deliver_session_message.assert_not_called()
        common.set_chat_binding(
            common.load_config(),
            "room",
            "22",
            ["sky", "lawrence", "ghost"],
            guild_id="1",
            policy={"ambient_read_enabled": True},
        )

        with mock.patch.object(common, "session_index_by_name") as session_index_by_name, mock.patch.object(
            common, "deliver_session_message"
        ) as deliver_session_message:
            replay_outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(replay_outcome["status"], "duplicate")
        session_index_by_name.assert_not_called()
        deliver_session_message.assert_not_called()

    def test_process_inbound_ambient_room_message_preserves_unknown_alias_rejection_when_mixed(self) -> None:
        common.set_chat_binding(
            common.load_config(),
            "room",
            "22",
            ["sky", "lawrence"],
            guild_id="1",
            policy={"ambient_read_enabled": True},
        )
        message = {
            "id": "507b",
            "guild_id": "1",
            "channel_id": "22",
            "content": "@sky @ghost please check the shard",
            "mentions": [],
            "author": {"id": "u-5bb", "username": "alice"},
        }

        with mock.patch.object(
            common,
            "session_index_by_name",
            return_value={
                "sky": {"session_name": "sky", "state": "active"},
                "lawrence": {"session_name": "lawrence", "state": "active"},
            },
        ), mock.patch.object(common, "deliver_session_message") as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "rejected_targeting")
        deliver_session_message.assert_not_called()
        receipt = common.load_chat_ingress("in-507b")
        assert receipt is not None
        self.assertEqual(receipt["reason"], "unknown_alias:ghost")

    def test_process_inbound_bound_room_message_does_not_fetch_channel_info_for_main_room_delivery(self) -> None:
        common.set_chat_binding(
            common.load_config(),
            "room",
            "22",
            ["sky"],
            guild_id="1",
            channel_metadata={"channel_type": 0},
        )
        common.save_bot_token("bot-token")
        message = {
            "id": "507b",
            "guild_id": "1",
            "channel_id": "22",
            "content": "<@999> @sky please check the shard",
            "mentions": [{"id": "999"}],
            "author": {"id": "u-5bb", "username": "alice"},
        }

        with mock.patch.object(common, "discord_api_request") as discord_api_request, mock.patch.object(
            common,
            "session_index_by_name",
            return_value={"sky": {"session_name": "sky", "state": "active"}},
        ), mock.patch.object(common, "deliver_session_message", return_value={"status": "accepted"}) as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "delivered")
        discord_api_request.assert_not_called()
        deliver_session_message.assert_called_once()

    def test_process_inbound_legacy_main_room_binding_survives_lookup_failure(self) -> None:
        common.set_chat_binding(common.load_config(), "room", "22", ["sky"], guild_id="1")
        common.save_bot_token("bot-token")
        message = {
            "id": "507c",
            "guild_id": "1",
            "channel_id": "22",
            "content": "<@999> @sky please check the shard",
            "mentions": [{"id": "999"}],
            "author": {"id": "u-5bc", "username": "alice"},
        }

        with mock.patch.object(
            common, "discord_api_request", side_effect=common.DiscordAPIError("GET failed", status_code=500)
        ), mock.patch.object(
            common,
            "session_index_by_name",
            return_value={"sky": {"session_name": "sky", "state": "active"}},
        ), mock.patch.object(common, "deliver_session_message", return_value={"status": "accepted"}) as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "delivered")
        deliver_session_message.assert_called_once()

    def test_process_inbound_legacy_main_room_binding_caches_metadata_after_successful_lookup(self) -> None:
        common.set_chat_binding(common.load_config(), "room", "22", ["sky"], guild_id="1")
        common.save_bot_token("bot-token")
        first_message = {
            "id": "507d",
            "guild_id": "1",
            "channel_id": "22",
            "content": "<@999> @sky please check the shard",
            "mentions": [{"id": "999"}],
            "author": {"id": "u-5bd", "username": "alice"},
        }
        second_message = {
            "id": "507e",
            "guild_id": "1",
            "channel_id": "22",
            "content": "<@999> @sky please check the shard again",
            "mentions": [{"id": "999"}],
            "author": {"id": "u-5be", "username": "alice"},
        }

        with mock.patch.object(common, "discord_api_request", return_value={"id": "22", "type": 0}) as discord_api_request, mock.patch.object(
            common,
            "session_index_by_name",
            return_value={"sky": {"session_name": "sky", "state": "active"}},
        ), mock.patch.object(common, "deliver_session_message", return_value={"status": "accepted"}) as deliver_session_message:
            first_outcome = gateway_service.process_inbound_message(first_message, bot_user_id="999")

        self.assertEqual(first_outcome["status"], "delivered")
        discord_api_request.assert_called_once()
        deliver_session_message.assert_called_once()
        binding = common.resolve_chat_binding(common.load_config(), common.chat_binding_id("room", "22"))
        assert binding is not None
        self.assertNotIn("channel_type", binding)
        self.assertEqual(common.load_channel_metadata_cache("22"), {"channel_type": 0})

        gateway_service.CHANNEL_INFO_CACHE.clear()

        with mock.patch.object(common, "discord_api_request") as discord_api_request, mock.patch.object(
            common,
            "session_index_by_name",
            return_value={"sky": {"session_name": "sky", "state": "active"}},
        ), mock.patch.object(common, "deliver_session_message", return_value={"status": "accepted"}) as deliver_session_message:
            second_outcome = gateway_service.process_inbound_message(second_message, bot_user_id="999")

        self.assertEqual(second_outcome["status"], "delivered")
        discord_api_request.assert_not_called()
        deliver_session_message.assert_called_once()

    def test_persist_binding_channel_metadata_writes_runtime_cache_without_rewriting_binding(self) -> None:
        common.set_chat_binding(
            common.load_config(),
            "room",
            "22",
            ["sky"],
            guild_id="1",
            policy={"ambient_read_enabled": True},
        )
        stale_binding = common.resolve_chat_binding(common.load_config(), common.chat_binding_id("room", "22"))
        assert stale_binding is not None
        common.set_chat_binding(
            common.load_config(),
            "room",
            "22",
            ["sky", "lawrence"],
            guild_id="1",
            policy={"ambient_read_enabled": True, "peer_fanout_enabled": True},
        )

        gateway_service.persist_binding_channel_metadata({**stale_binding, "channel_type": 0})

        binding = common.resolve_chat_binding(common.load_config(), common.chat_binding_id("room", "22"))
        assert binding is not None
        self.assertEqual(binding["session_names"], ["sky", "lawrence"])
        self.assertNotIn("channel_type", binding)
        self.assertTrue(common.binding_peer_policy(binding)["peer_fanout_enabled"])
        self.assertEqual(common.load_channel_metadata_cache("22"), {"channel_type": 0})

    def test_process_inbound_ambient_room_message_still_requires_target_when_bot_mentioned(self) -> None:
        common.set_chat_binding(
            common.load_config(),
            "room",
            "22",
            ["sky", "lawrence"],
            guild_id="1",
            policy={"ambient_read_enabled": True},
        )
        message = {
            "id": "508",
            "guild_id": "1",
            "channel_id": "22",
            "content": "<@999> please check the shard",
            "mentions": [{"id": "999"}],
            "author": {"id": "u-5c", "username": "alice"},
        }

        with mock.patch.object(
            common,
            "session_index_by_name",
            return_value={
                "sky": {"session_name": "sky", "state": "active"},
                "lawrence": {"session_name": "lawrence", "state": "active"},
            },
        ), mock.patch.object(common, "deliver_session_message") as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "ignored_untargeted")
        deliver_session_message.assert_not_called()
        receipt = common.load_chat_ingress("in-508")
        assert receipt is not None
        self.assertEqual(receipt["reason"], "ambient_target_required")

    def test_process_inbound_bot_mentioned_thread_with_ambient_parent_still_requires_target(self) -> None:
        common.set_chat_binding(
            common.load_config(),
            "room",
            "22",
            ["sky", "lawrence"],
            guild_id="1",
            policy={"ambient_read_enabled": True},
        )
        common.save_bot_token("bot-token")
        message = {
            "id": "508b",
            "guild_id": "1",
            "channel_id": "222",
            "content": "<@999> please check the shard",
            "mentions": [{"id": "999"}],
            "author": {"id": "u-5cc", "username": "alice"},
        }

        with mock.patch.object(common, "discord_api_request", return_value={"id": "222", "parent_id": "22", "type": 11}), mock.patch.object(
            common,
            "session_index_by_name",
            return_value={
                "sky": {"session_name": "sky", "state": "active"},
                "lawrence": {"session_name": "lawrence", "state": "active"},
            },
        ), mock.patch.object(common, "deliver_session_message") as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "ignored_untargeted")
        deliver_session_message.assert_not_called()
        receipt = common.load_chat_ingress("in-508b")
        assert receipt is not None
        self.assertEqual(receipt["reason"], "ambient_target_required")

    def test_process_inbound_unmentioned_thread_does_not_probe_parent_binding_for_ambient_read(self) -> None:
        common.set_chat_binding(
            common.load_config(),
            "room",
            "22",
            ["sky", "lawrence"],
            guild_id="1",
            policy={"ambient_read_enabled": True},
        )
        message = {
            "id": "509",
            "guild_id": "1",
            "channel_id": "222",
            "content": "@sky please check the shard",
            "mentions": [],
            "author": {"id": "u-5d", "username": "alice"},
        }

        with mock.patch.object(common, "discord_api_request") as discord_api_request, mock.patch.object(
            common,
            "deliver_session_message",
        ) as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "ignored")
        self.assertEqual(outcome["reason"], "not_mentioned")
        discord_api_request.assert_not_called()
        deliver_session_message.assert_not_called()

    def test_cached_ambient_room_binding_reuses_cached_config_between_messages(self) -> None:
        common.set_chat_binding(
            common.load_config(),
            "room",
            "22",
            ["sky"],
            guild_id="1",
            policy={"ambient_read_enabled": True},
        )

        with mock.patch.object(common, "load_config", wraps=common.load_config) as load_config:
            first = gateway_service.cached_ambient_room_binding("22")
            second = gateway_service.cached_ambient_room_binding("22")

        self.assertIsNotNone(first)
        self.assertEqual(first, second)
        self.assertEqual(load_config.call_count, 1)

    def test_cached_ambient_room_binding_serializes_cache_refill(self) -> None:
        common.set_chat_binding(
            common.load_config(),
            "room",
            "22",
            ["sky"],
            guild_id="1",
            policy={"ambient_read_enabled": True},
        )

        release = threading.Event()
        started = threading.Event()
        load_count = 0
        real_load_config = common.load_config

        def blocking_load_config() -> dict[str, object]:
            nonlocal load_count
            load_count += 1
            started.set()
            release.wait(timeout=2)
            return real_load_config()

        def worker() -> None:
            gateway_service.cached_ambient_room_binding("22")

        with mock.patch.object(common, "load_config", side_effect=blocking_load_config):
            thread_a = threading.Thread(target=worker)
            thread_b = threading.Thread(target=worker)
            thread_a.start()
            thread_b.start()
            started.wait(timeout=2)
            time.sleep(0.05)
            release.set()
            thread_a.join()
            thread_b.join()

        self.assertEqual(load_count, 1)

    def test_cached_ambient_room_binding_refreshes_on_signature_change(self) -> None:
        config_one = {
            "chat": {
                "bindings": {
                    "room:22": {
                        "id": "room:22",
                        "kind": "room",
                        "conversation_id": "22",
                        "guild_id": "1",
                        "session_names": ["sky"],
                        "policy": {"ambient_read_enabled": True},
                    }
                }
            }
        }
        config_two = {
            "chat": {
                "bindings": {
                    "room:22": {
                        "id": "room:22",
                        "kind": "room",
                        "conversation_id": "22",
                        "guild_id": "1",
                        "session_names": ["lawrence"],
                        "policy": {"ambient_read_enabled": True},
                    }
                }
            }
        }

        stat_one = mock.Mock(st_mtime_ns=100, st_size=1000, st_ino=1)
        stat_two = mock.Mock(st_mtime_ns=100, st_size=1000, st_ino=2)

        with mock.patch.object(gateway_service.os, "stat", side_effect=[stat_one, stat_one, stat_two, stat_two]), mock.patch.object(
            common,
            "load_config",
            side_effect=[common.normalize_config(config_one), common.normalize_config(config_two)],
        ) as load_config:
            first = gateway_service.cached_ambient_room_binding("22")
            second = gateway_service.cached_ambient_room_binding("22")

        assert first is not None
        assert second is not None
        self.assertEqual(first["session_names"], ["sky"])
        self.assertEqual(second["session_names"], ["lawrence"])
        self.assertEqual(load_config.call_count, 2)

    def test_cached_ambient_room_binding_skips_lookup_for_persisted_main_room_metadata(self) -> None:
        common.set_chat_binding(
            common.load_config(),
            "room",
            "22",
            ["sky"],
            guild_id="1",
            policy={"ambient_read_enabled": True},
            channel_metadata={"channel_type": 0},
        )

        with mock.patch.object(common, "discord_api_request") as discord_api_request:
            binding = gateway_service.cached_ambient_room_binding("22")

        self.assertIsNotNone(binding)
        assert binding is not None
        self.assertEqual(binding["channel_type"], 0)
        discord_api_request.assert_not_called()

    def test_cached_ambient_room_binding_ignores_invalid_config(self) -> None:
        config_path = common.config_path()
        common.ensure_layout()
        pathlib.Path(config_path).write_text("{not valid json", encoding="utf-8")

        binding = gateway_service.cached_ambient_room_binding("22")

        self.assertIsNone(binding)

    def test_process_inbound_room_message_ignores_native_user_mentions_for_aliases(self) -> None:
        common.set_chat_binding(common.load_config(), "room", "22", ["sky", "lawrence"], guild_id="1")
        message = {
            "id": "606",
            "guild_id": "1",
            "channel_id": "22",
            "content": "<@999> talk to <@123456789> about it",
            "mentions": [{"id": "999"}, {"id": "123456789"}],
            "author": {"id": "u-6", "username": "alice"},
        }

        with mock.patch.object(
            common,
            "session_index_by_name",
            return_value={
                "sky": {"session_name": "sky", "state": "active"},
                "lawrence": {"session_name": "lawrence", "state": "active"},
            },
        ), mock.patch.object(common, "deliver_session_message", return_value={"status": "accepted"}) as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "delivered")
        self.assertEqual(deliver_session_message.call_count, 2)
        receipt = common.load_chat_ingress("in-606")
        self.assertEqual(receipt["delivery"], "broadcast")
        self.assertEqual(receipt["mentioned_aliases"], [])

    def test_process_inbound_room_message_treats_reserved_mentions_as_broadcast(self) -> None:
        common.set_chat_binding(common.load_config(), "room", "22", ["sky", "lawrence"], guild_id="1")
        message = {
            "id": "607",
            "guild_id": "1",
            "channel_id": "22",
            "content": "<@999> @everyone please look at this",
            "mentions": [{"id": "999"}],
            "author": {"id": "u-7", "username": "alice"},
        }

        with mock.patch.object(
            common,
            "session_index_by_name",
            return_value={
                "sky": {"session_name": "sky", "state": "active"},
                "lawrence": {"session_name": "lawrence", "state": "active"},
            },
        ), mock.patch.object(common, "deliver_session_message", return_value={"status": "accepted"}) as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "delivered")
        self.assertEqual(deliver_session_message.call_count, 2)
        receipt = common.load_chat_ingress("in-607")
        self.assertEqual(receipt["delivery"], "broadcast")
        self.assertEqual(receipt["mentioned_aliases"], [])

    def test_process_inbound_room_message_records_partial_failed_delivery(self) -> None:
        common.set_chat_binding(common.load_config(), "room", "22", ["sky", "lawrence"], guild_id="1")
        message = {
            "id": "707",
            "guild_id": "1",
            "channel_id": "22",
            "content": "<@999> please investigate",
            "mentions": [{"id": "999"}],
            "author": {"id": "u-7", "username": "alice"},
        }

        with mock.patch.object(
            common,
            "session_index_by_name",
            return_value={
                "sky": {"session_name": "sky", "state": "active"},
                "lawrence": {"session_name": "lawrence", "state": "active"},
            },
        ), mock.patch.object(
            common,
            "deliver_session_message",
            side_effect=[{"status": "accepted"}, common.GCAPIError("boom")],
        ):
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "partial_failed")
        receipt = common.load_chat_ingress("in-707")
        self.assertEqual(receipt["status"], "partial_failed")
        self.assertEqual(receipt["targets"][0]["status"], "delivered")
        self.assertEqual(receipt["targets"][1]["status"], "failed")

    def test_process_inbound_message_reclaims_stale_processing_receipt(self) -> None:
        common.set_chat_binding(common.load_config(), "dm", "55", ["sky"])
        common.atomic_write_json(
            common.chat_ingress_path("in-909"),
            {
                "ingress_id": "in-909",
                "status": "processing",
                "created_at": "2000-01-01T00:00:00Z",
                "updated_at": "2000-01-01T00:00:00Z",
            },
        )
        message = {
            "id": "909",
            "channel_id": "55",
            "content": "hello from discord",
            "author": {"id": "u-9", "username": "alice"},
        }

        with mock.patch.object(common, "session_index_by_name", return_value={"sky": {"session_name": "sky", "state": "active"}}), mock.patch.object(
            common,
            "deliver_session_message",
            return_value={"status": "accepted", "id": "gc-9"},
        ) as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "delivered")
        deliver_session_message.assert_called_once()

    def test_process_inbound_message_records_unreadable_claim_conflict(self) -> None:
        path = common.chat_ingress_path("in-910")
        pathlib.Path(path).parent.mkdir(parents=True, exist_ok=True)
        pathlib.Path(path).write_text("", encoding="utf-8")
        message = {
            "id": "910",
            "channel_id": "55",
            "content": "hello from discord",
            "author": {"id": "u-10", "username": "alice"},
        }

        with mock.patch.object(common, "deliver_session_message") as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "failed_claim_conflict")
        deliver_session_message.assert_not_called()
        receipt = common.load_chat_ingress("in-910")
        assert receipt is not None
        self.assertEqual(receipt["status"], "failed_claim_conflict")
        self.assertEqual(receipt["reason"], "ingress_claim_unreadable")

    def test_failed_claim_conflict_receipt_retries_after_backoff(self) -> None:
        common.set_chat_binding(common.load_config(), "dm", "55", ["sky"])
        common.atomic_write_json(
            common.chat_ingress_path("in-915"),
            {
                "ingress_id": "in-915",
                "status": "failed_claim_conflict",
                "created_at": "2000-01-01T00:00:00Z",
                "updated_at": "2000-01-01T00:00:00Z",
            },
        )
        message = {
            "id": "915",
            "channel_id": "55",
            "content": "hello from discord",
            "author": {"id": "u-15", "username": "alice"},
        }

        with mock.patch.object(common, "session_index_by_name", return_value={"sky": {"session_name": "sky", "state": "active"}}), mock.patch.object(
            common,
            "deliver_session_message",
            return_value={"status": "accepted", "id": "gc-15"},
        ) as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "delivered")
        deliver_session_message.assert_called_once()
        receipt = common.load_chat_ingress("in-915")
        assert receipt is not None
        self.assertEqual(receipt["reason"], "retry_after_failed_claim_conflict")

    def test_rejected_shutting_down_receipt_retries_immediately(self) -> None:
        common.set_chat_binding(common.load_config(), "dm", "55", ["sky"])
        common.atomic_write_json(
            common.chat_ingress_path("in-916"),
            {
                "ingress_id": "in-916",
                "status": "rejected_shutting_down",
                "created_at": "2000-01-01T00:00:00Z",
                "updated_at": "2000-01-01T00:00:00Z",
            },
        )
        message = {
            "id": "916",
            "channel_id": "55",
            "content": "hello from discord",
            "author": {"id": "u-16", "username": "alice"},
        }

        with mock.patch.object(common, "session_index_by_name", return_value={"sky": {"session_name": "sky", "state": "active"}}), mock.patch.object(
            common,
            "deliver_session_message",
            return_value={"status": "accepted", "id": "gc-16"},
        ) as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "delivered")
        deliver_session_message.assert_called_once()
        receipt = common.load_chat_ingress("in-916")
        assert receipt is not None
        self.assertEqual(receipt["reason"], "retry_after_shutdown")

    def test_stale_reclaim_lock_allows_only_one_delivery(self) -> None:
        common.set_chat_binding(common.load_config(), "dm", "55", ["sky"])
        common.atomic_write_json(
            common.chat_ingress_path("in-911"),
            {
                "ingress_id": "in-911",
                "status": "processing",
                "created_at": "2000-01-01T00:00:00Z",
                "updated_at": "2000-01-01T00:00:00Z",
            },
        )
        message = {
            "id": "911",
            "channel_id": "55",
            "content": "hello from discord",
            "author": {"id": "u-11", "username": "alice"},
        }
        barrier = threading.Barrier(2)
        release = threading.Event()
        started = threading.Event()
        outcomes: list[str] = []

        def fake_deliver(*args: object, **kwargs: object) -> dict[str, object]:
            started.set()
            release.wait(timeout=1)
            return {"status": "accepted", "id": "gc-11"}

        def worker() -> None:
            barrier.wait()
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")
            outcomes.append(str(outcome.get("status", "")))

        with mock.patch.object(common, "session_index_by_name", return_value={"sky": {"session_name": "sky", "state": "active"}}), mock.patch.object(
            common,
            "deliver_session_message",
            side_effect=fake_deliver,
        ) as deliver_session_message:
            thread_a = threading.Thread(target=worker)
            thread_b = threading.Thread(target=worker)
            thread_a.start()
            thread_b.start()
            self.assertTrue(started.wait(timeout=1))
            release.set()
            thread_a.join()
            thread_b.join()

        self.assertEqual(deliver_session_message.call_count, 1)
        self.assertEqual(sorted(outcomes), ["delivered", "duplicate"])

    def test_stale_reclaim_defers_when_original_processor_lock_is_held(self) -> None:
        common.set_chat_binding(common.load_config(), "dm", "55", ["sky"])
        common.atomic_write_json(
            common.chat_ingress_path("in-912"),
            {
                "ingress_id": "in-912",
                "status": "processing",
                "created_at": "2000-01-01T00:00:00Z",
                "updated_at": "2000-01-01T00:00:00Z",
            },
        )
        message = {
            "id": "912",
            "channel_id": "55",
            "content": "hello from discord",
            "author": {"id": "u-12", "username": "alice"},
        }
        process_lock = gateway_service.ingress_process_lock("in-912")
        process_lock.acquire()
        self.addCleanup(lambda: process_lock.locked() and process_lock.release())

        with mock.patch.object(common, "session_index_by_name", return_value={"sky": {"session_name": "sky", "state": "active"}}), mock.patch.object(
            common,
            "deliver_session_message",
            return_value={"status": "accepted", "id": "gc-12"},
        ) as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "duplicate")
        deliver_session_message.assert_not_called()

    def test_failed_receipt_retries_after_backoff(self) -> None:
        common.set_chat_binding(common.load_config(), "dm", "55", ["sky"])
        common.atomic_write_json(
            common.chat_ingress_path("in-913"),
            {
                "ingress_id": "in-913",
                "status": "failed",
                "created_at": "2000-01-01T00:00:00Z",
                "updated_at": "2000-01-01T00:00:00Z",
            },
        )
        message = {
            "id": "913",
            "channel_id": "55",
            "content": "hello from discord",
            "author": {"id": "u-13", "username": "alice"},
        }

        with mock.patch.object(common, "session_index_by_name", return_value={"sky": {"session_name": "sky", "state": "active"}}), mock.patch.object(
            common,
            "deliver_session_message",
            return_value={"status": "accepted", "id": "gc-13"},
        ) as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "delivered")
        deliver_session_message.assert_called_once()

    def test_failed_lookup_receipt_retries_after_backoff(self) -> None:
        common.set_chat_binding(common.load_config(), "dm", "55", ["sky"])
        common.atomic_write_json(
            common.chat_ingress_path("in-914"),
            {
                "ingress_id": "in-914",
                "status": "failed_lookup",
                "created_at": "2000-01-01T00:00:00Z",
                "updated_at": "2000-01-01T00:00:00Z",
            },
        )
        message = {
            "id": "914",
            "channel_id": "55",
            "content": "hello from discord",
            "author": {"id": "u-14", "username": "alice"},
        }

        with mock.patch.object(common, "session_index_by_name", return_value={"sky": {"session_name": "sky", "state": "active"}}), mock.patch.object(
            common,
            "deliver_session_message",
            return_value={"status": "accepted", "id": "gc-14"},
        ) as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "delivered")
        deliver_session_message.assert_called_once()
        receipt = common.load_chat_ingress("in-914")
        assert receipt is not None
        self.assertEqual(receipt["reason"], "retry_after_failed_lookup")

    def test_process_inbound_thread_messages_cache_parent_lookup(self) -> None:
        common.set_chat_binding(common.load_config(), "room", "22", ["sky"], guild_id="1")
        common.save_bot_token("bot-token")
        base_message = {
            "guild_id": "1",
            "channel_id": "222",
            "content": "<@999> please check",
            "mentions": [{"id": "999"}],
            "author": {"id": "u-22", "username": "alice"},
        }

        with mock.patch.object(common, "discord_api_request", return_value={"id": "222", "parent_id": "22", "type": 11}) as discord_api_request, mock.patch.object(
            common,
            "session_index_by_name",
            return_value={"sky": {"session_name": "sky", "state": "active"}},
        ), mock.patch.object(common, "deliver_session_message", return_value={"status": "accepted"}):
            outcome_1 = gateway_service.process_inbound_message({**base_message, "id": "801"}, bot_user_id="999")
            outcome_2 = gateway_service.process_inbound_message({**base_message, "id": "802"}, bot_user_id="999")

        self.assertEqual(outcome_1["status"], "delivered")
        self.assertEqual(outcome_2["status"], "delivered")
        discord_api_request.assert_called_once()

    def test_load_channel_info_serializes_cache_fill(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        calls: list[str] = []
        results: list[dict[str, object]] = []

        def fake_request(method: str, path: str, bot_token: str = "") -> dict[str, object]:
            calls.append(path)
            entered.set()
            release.wait(timeout=1)
            return {"id": "222", "parent_id": "22", "type": 11}

        def worker() -> None:
            results.append(gateway_service.load_channel_info("222", "bot-token"))

        with mock.patch.object(common, "discord_api_request", side_effect=fake_request):
            thread_a = threading.Thread(target=worker)
            thread_b = threading.Thread(target=worker)
            thread_a.start()
            thread_b.start()
            self.assertTrue(entered.wait(timeout=1))
            release.set()
            thread_a.join()
            thread_b.join()

        self.assertEqual(calls, ["/channels/222"])
        self.assertEqual(
            results,
            [{"id": "222", "parent_id": "22", "type": 11}, {"id": "222", "parent_id": "22", "type": 11}],
        )

    def test_load_channel_info_strips_parent_id_for_non_thread_channels(self) -> None:
        with mock.patch.object(common, "discord_api_request", return_value={"id": "22", "parent_id": "88", "type": 0}):
            info = gateway_service.load_channel_info("22", "bot-token")

        self.assertEqual(info, {"id": "22", "type": 0})

    def test_channel_info_fetch_lock_is_scoped_per_channel(self) -> None:
        lock_a = gateway_service.channel_info_fetch_lock("222")
        lock_b = gateway_service.channel_info_fetch_lock("223")

        self.assertIs(lock_a, gateway_service.channel_info_fetch_lock("222"))
        self.assertIsNot(lock_a, lock_b)

    def test_worker_stop_drains_queued_messages_before_exit(self) -> None:
        runtime_state = gateway_service.GatewayRuntimeState()
        worker = gateway_service.GatewayWorker(runtime_state)
        self.addCleanup(lambda: worker.stop() if not worker.stop_event.is_set() else None)

        handled: list[str] = []
        with mock.patch.object(worker, "handle_gateway_message", side_effect=lambda message, bot_user_id: handled.append(str(message.get("id", "")))):
            worker.dispatch_gateway_message({"id": "1001", "channel_id": "55", "author": {"id": "u-1001"}}, "999")
            worker.stop()

        self.assertEqual(handled, ["1001"])
        self.assertTrue(worker.stop_event.is_set())
        self.assertTrue(all(not thread.is_alive() for thread in worker.worker_threads))

    def test_dispatch_gateway_message_persists_shutting_down_receipt(self) -> None:
        runtime_state = gateway_service.GatewayRuntimeState()
        worker = gateway_service.GatewayWorker(runtime_state)
        self.addCleanup(worker.stop)
        worker.request_stop()

        worker.dispatch_gateway_message({"id": "1002", "channel_id": "55", "author": {"id": "u-1002"}}, "999")

        receipt = common.load_chat_ingress("in-1002")
        assert receipt is not None
        self.assertEqual(receipt["status"], "rejected_shutting_down")
        self.assertEqual(receipt["reason"], "service_shutting_down")

    def test_worker_stop_returns_when_worker_pool_is_idle(self) -> None:
        runtime_state = gateway_service.GatewayRuntimeState()
        worker = gateway_service.GatewayWorker(runtime_state)

        stop_thread = threading.Thread(target=worker.stop)
        stop_thread.start()
        stop_thread.join(timeout=2)

        self.assertFalse(stop_thread.is_alive())
        self.assertTrue(worker.stop_event.is_set())
        self.assertTrue(all(not thread.is_alive() for thread in worker.worker_threads))

    def test_utc_age_seconds_uses_utc_epoch_conversion(self) -> None:
        if not hasattr(time, "tzset"):
            self.skipTest("tzset not available on this platform")
        previous_tz = os.environ.get("TZ")
        try:
            os.environ["TZ"] = "Etc/GMT+8"
            time.tzset()
            stamp = time.strftime(
                "%Y-%m-%dT%H:%M:%SZ",
                time.gmtime(time.time() - gateway_service.STALE_PROCESSING_RECEIPT_SECONDS - 5),
            )
            age = gateway_service.utc_age_seconds(stamp)
        finally:
            if previous_tz is None:
                os.environ.pop("TZ", None)
            else:
                os.environ["TZ"] = previous_tz
            time.tzset()

        self.assertGreaterEqual(age, gateway_service.STALE_PROCESSING_RECEIPT_SECONDS)
        self.assertLess(age, gateway_service.STALE_PROCESSING_RECEIPT_SECONDS + 30)

    def test_gateway_connect_url_preserves_resume_host_and_adds_required_query_params(self) -> None:
        worker = object.__new__(gateway_service.GatewayWorker)

        url = gateway_service.GatewayWorker.gateway_connect_url(worker, "wss://gateway.discord.gg/?compress=zlib-stream")

        self.assertIn("v=10", url)
        self.assertIn("encoding=json", url)
        self.assertNotIn("compress=", url)

    def test_probe_gc_api_health_caches_recent_result(self) -> None:
        runtime_state = gateway_service.GatewayRuntimeState()

        with mock.patch.object(common, "gc_api_request", return_value={"items": []}) as gc_api_request:
            self.assertTrue(gateway_service.probe_gc_api_health(runtime_state))
            self.assertTrue(gateway_service.probe_gc_api_health(runtime_state))

        gc_api_request.assert_called_once()
        self.assertEqual(
            gc_api_request.call_args.kwargs["timeout"],
            gateway_service.GC_API_HEALTH_PROBE_TIMEOUT_SECONDS,
        )

    def test_current_bot_user_id_prefers_last_known_id_after_resume(self) -> None:
        worker = object.__new__(gateway_service.GatewayWorker)

        bot_user_id = gateway_service.GatewayWorker.current_bot_user_id(
            worker,
            {"app": {"application_id": "app-1"}},
            None,
            "bot-9",
        )

        self.assertEqual(bot_user_id, "bot-9")

    def test_gateway_health_status_code_requires_gc_api_when_ready(self) -> None:
        self.assertEqual(
            gateway_service.gateway_health_status_code({"state": "ready"}, gc_api_reachable=False),
            gateway_service.HTTPStatus.SERVICE_UNAVAILABLE,
        )
        self.assertEqual(
            gateway_service.gateway_health_status_code({"state": "ready"}, gc_api_reachable=True),
            gateway_service.HTTPStatus.NO_CONTENT,
        )

    def test_gateway_health_status_code_honors_reconnect_grace_window(self) -> None:
        state = {"state": "reconnecting", "last_ready_epoch": int(time.time())}

        self.assertEqual(
            gateway_service.gateway_health_status_code(state, gc_api_reachable=True),
            gateway_service.HTTPStatus.NO_CONTENT,
        )

    def test_gateway_health_status_code_honors_resume_grace_window(self) -> None:
        state = {
            "state": "reconnecting",
            "last_ready_epoch": 1,
            "last_resumed_epoch": int(time.time()),
        }

        self.assertEqual(
            gateway_service.gateway_health_status_code(state, gc_api_reachable=True),
            gateway_service.HTTPStatus.NO_CONTENT,
        )

    def test_gateway_websocket_recv_event_reassembles_fragmented_text_frames(self) -> None:
        ws = object.__new__(gateway_service.GatewayWebSocket)
        frames = iter(
            [
                (False, 0x1, b'{"op":0,'),
                (True, 0x0, b'"d":{"ok":true}}'),
            ]
        )
        ws.read_frame = lambda timeout=None: next(frames)  # type: ignore[attr-defined]
        ws.send_frame = mock.Mock()

        event = gateway_service.GatewayWebSocket.recv_event(ws, timeout=1.0)

        self.assertEqual(event, {"op": 0, "d": {"ok": True}})

    def test_gateway_websocket_read_frame_rejects_oversized_payloads(self) -> None:
        ws = object.__new__(gateway_service.GatewayWebSocket)
        parts = iter(
            [
                bytes([0x81, 0x7F]),
                struct.pack("!Q", gateway_service.MAX_FRAME_BYTES + 1),
            ]
        )
        ws.read_exact = lambda length, timeout=None: next(parts)  # type: ignore[attr-defined]

        with self.assertRaises(gateway_service.WebSocketClosed):
            gateway_service.GatewayWebSocket.read_frame(ws)

    def test_validate_websocket_handshake_rejects_bad_accept_header(self) -> None:
        key = "dGhlIHNhbXBsZSBub25jZQ=="
        header_blob = "\r\n".join(
            [
                "HTTP/1.1 101 Switching Protocols",
                "Upgrade: websocket",
                "Connection: Upgrade",
                "Sec-WebSocket-Accept: bad-value",
            ]
        )

        with self.assertRaisesRegex(RuntimeError, "Sec-WebSocket-Accept"):
            gateway_service.validate_websocket_handshake(header_blob, key)


if __name__ == "__main__":
    unittest.main()
