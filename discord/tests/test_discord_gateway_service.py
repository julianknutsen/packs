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
        self.addCleanup(self._restore_environment)
        os.environ["GC_CITY_ROOT"] = self.tempdir.name
        gateway_service.CHANNEL_INFO_CACHE.clear()
        gateway_service.CHANNEL_INFO_FETCH_LOCKS.clear()
        gateway_service.STALE_RECLAIM_LOCKS.clear()
        gateway_service.INGRESS_PROCESS_LOCKS.clear()
        gateway_service.GC_API_HEALTH_CACHE["checked_at"] = 0.0
        gateway_service.GC_API_HEALTH_CACHE["reachable"] = True
        gateway_service.AMBIENT_ROOM_BINDINGS_CACHE["config_signature"] = None
        gateway_service.AMBIENT_ROOM_BINDINGS_CACHE["bindings"] = {}

    def _restore_environment(self) -> None:
        os.environ.clear()
        os.environ.update(self._old_environ)

    def _new_gateway_worker(self) -> gateway_service.GatewayWorker:
        runtime_state = gateway_service.GatewayRuntimeState()
        with mock.patch.object(gateway_service, "GATEWAY_WORKER_THREADS", 0):
            worker = gateway_service.GatewayWorker(runtime_state)
        self.addCleanup(worker.stop)
        return worker

    def _configure_discord_app(self) -> None:
        common.import_app_config(common.load_config(), {"application_id": "1484616391729483786"})

    def test_gateway_requests_message_content_intent(self) -> None:
        self.assertTrue(gateway_service.GATEWAY_INTENTS & (1 << 15))

    def test_build_gateway_workers_creates_one_worker_per_configured_app(self) -> None:
        config = common.import_app_config(
            common.load_config(),
            {"application_id": "111", "public_key": "ab" * 32},
        )
        config = common.import_app_config(
            config,
            {"application_id": "222", "public_key": "cd" * 32},
            app_name="ollie",
        )
        config = common.import_app_config(
            config,
            {"application_id": "333", "public_key": "ef" * 32},
            app_name="olivia",
        )

        with mock.patch.object(gateway_service, "GATEWAY_WORKER_THREADS", 0):
            workers = gateway_service.build_gateway_workers(config)
        self.addCleanup(lambda: [worker.stop() for worker in workers])

        self.assertEqual(workers[0].app_name, "")
        self.assertEqual({worker.app_name for worker in workers[1:]}, {"ollie", "olivia"})
        bot_ids = {worker.app_name: worker.current_bot_user_id(config) for worker in workers}
        self.assertEqual(bot_ids, {"": "111", "ollie": "222", "olivia": "333"})
        delays = [worker.initial_connect_delay_seconds for worker in workers]
        self.assertEqual(delays[0], 0)
        self.assertEqual(delays[1:], [
            gateway_service.GATEWAY_IDENTIFY_STAGGER_SECONDS,
            gateway_service.GATEWAY_IDENTIFY_STAGGER_SECONDS * 2,
        ])

    def test_build_gateway_workers_rejects_duplicate_application_ids_from_config(self) -> None:
        config = common.normalize_config(
            {
                "app": {"application_id": "111", "public_key": "ab" * 32},
                "apps": {
                    "ollie": {
                        "application_id": "111",
                        "public_key": "cd" * 32,
                    }
                },
            }
        )

        with self.assertRaisesRegex(ValueError, "application_id.*more than once"):
            gateway_service.build_gateway_workers(config)

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
        self.assertIn("reply_tool: gc discord reply-current --conversation-id 55 --reply-to 101 --body-file <path>", envelope)
        receipt = common.load_chat_ingress("in-101")
        self.assertEqual(receipt["status"], "delivered")
        self.assertEqual(receipt["app"], "")

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

    def test_named_apps_route_the_same_discord_message_independently(self) -> None:
        config = common.import_app_config(
            common.load_config(),
            {"application_id": "999", "public_key": "ab" * 32},
            app_name="ollie",
        )
        config = common.import_app_config(
            config,
            {"application_id": "998", "public_key": "cd" * 32},
            app_name="olivia",
        )
        config = common.set_chat_binding(
            config,
            "room",
            "22",
            ["teams.lead"],
            guild_id="1",
            app_name="ollie",
            channel_metadata={"channel_type": 0},
        )
        common.set_chat_binding(
            config,
            "room",
            "22",
            ["teams.pm"],
            guild_id="1",
            app_name="olivia",
            channel_metadata={"channel_type": 0},
        )
        message = {
            "id": "multi-202",
            "guild_id": "1",
            "channel_id": "22",
            "content": "<@999> <@998> please coordinate",
            "mentions": [{"id": "999"}, {"id": "998"}],
            "author": {"id": "u-2", "username": "alice"},
        }

        with mock.patch.object(
            common,
            "session_index_by_name",
            return_value={
                "teams.lead": {"session_name": "teams.lead", "state": "active"},
                "teams.pm": {"session_name": "teams.pm", "state": "active"},
            },
        ), mock.patch.object(common, "deliver_session_message", return_value={"status": "accepted"}) as deliver:
            lead_outcome = gateway_service.process_inbound_message(message, bot_user_id="999", app_name="ollie")
            pm_outcome = gateway_service.process_inbound_message(message, bot_user_id="998", app_name="olivia")

        self.assertEqual(lead_outcome["status"], "delivered")
        self.assertEqual(pm_outcome["status"], "delivered")
        self.assertEqual([call.args[0] for call in deliver.call_args_list], ["teams.lead", "teams.pm"])
        lead_receipt = common.load_chat_ingress("in-multi-202-app-ollie")
        pm_receipt = common.load_chat_ingress("in-multi-202-app-olivia")
        self.assertEqual(lead_receipt["app"], "ollie")
        self.assertEqual(pm_receipt["app"], "olivia")
        self.assertEqual(lead_receipt["binding_id"], "room:22@app:ollie")
        self.assertEqual(pm_receipt["binding_id"], "room:22@app:olivia")

    def test_named_app_async_submit_failure_cannot_leave_delivered_receipt(self) -> None:
        config = common.import_app_config(
            common.load_config(),
            {"application_id": "999", "public_key": "ab" * 32},
            app_name="ollie",
        )
        common.set_chat_binding(
            config,
            "room",
            "22",
            ["teams.lead"],
            guild_id="1",
            app_name="ollie",
            channel_metadata={"channel_type": 0},
        )
        message = {
            "id": "async-failure-203",
            "guild_id": "1",
            "channel_id": "22",
            "content": "<@999> please investigate",
            "mentions": [{"id": "999"}],
            "author": {"id": "u-2", "username": "alice"},
        }
        accepted_response = mock.MagicMock()
        accepted_response.__enter__.return_value = mock.Mock(
            read=mock.Mock(
                return_value=b'{"status":"accepted","request_id":"req-submit","event_cursor":"41"}'
            )
        )
        accepted_response.__enter__.return_value.status = 202
        accepted_response.__exit__.return_value = False
        failed_stream = mock.MagicMock()
        failed_stream.__enter__.return_value = iter(
            [
                b"event: request.failed\n",
                b'data: {"type":"request.failed","payload":{"request_id":"req-other-app","operation":"session.submit","error_code":"submit_failed","error_message":"other app failed"}}\n',
                b"\n",
                b"event: request.failed\n",
                b'data: {"type":"request.failed","payload":{"request_id":"req-submit","operation":"session.submit","error_code":"submit_failed","error_message":"session disappeared"}}\n',
                b"\n",
            ]
        )
        failed_stream.__exit__.return_value = False

        with mock.patch.dict(
            os.environ,
            {"GC_API_BASE_URL": "http://gc.test/v0/city/test"},
        ), mock.patch.object(
            common,
            "session_index_by_name",
            return_value={"teams.lead": {"session_name": "teams.lead", "state": "active"}},
        ), mock.patch.object(
            common.urllib.request,
            "urlopen",
            side_effect=[accepted_response, failed_stream],
        ) as urlopen:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999", app_name="ollie")

        self.assertEqual(outcome["status"], "failed")
        self.assertEqual(len(urlopen.call_args_list), 2)
        self.assertEqual(
            urlopen.call_args_list[1].args[0].full_url,
            "http://gc.test/v0/city/test/events/stream?after_seq=41",
        )
        receipt = common.load_chat_ingress("in-async-failure-203-app-ollie")
        self.assertEqual(receipt["app"], "ollie")
        self.assertEqual(receipt["status"], "failed")
        self.assertEqual(receipt["targets"][0]["status"], "failed")
        self.assertIn("session.submit failed: submit_failed: session disappeared", receipt["targets"][0]["error"])
        self.assertEqual(receipt["targets"][0]["terminal_evidence"]["status"], "failed")
        self.assertEqual(receipt["targets"][0]["terminal_evidence"]["payload"]["error_code"], "submit_failed")

    def test_named_app_gateway_policy_is_evaluated_independently(self) -> None:
        config = common.import_app_config(
            common.load_config(),
            {
                "application_id": "999",
                "public_key": "ab" * 32,
                "guild_allowlist": ["allowed-guild"],
                "channel_allowlist": ["22"],
            },
            app_name="ollie",
        )
        common.set_chat_binding(
            config,
            "room",
            "22",
            ["teams.lead"],
            guild_id="blocked-guild",
            app_name="ollie",
            channel_metadata={"channel_type": 0},
        )
        message = {
            "id": "policy-203",
            "guild_id": "blocked-guild",
            "channel_id": "22",
            "content": "<@999> hello",
            "mentions": [{"id": "999"}],
            "author": {"id": "u-2", "username": "alice"},
        }

        with mock.patch.object(common, "deliver_session_message") as deliver:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999", app_name="ollie")

        self.assertEqual(outcome["status"], "rejected_policy")
        self.assertEqual(outcome["reason"], "guild_not_allowed")
        self.assertEqual(common.load_chat_ingress("in-policy-203-app-ollie")["app"], "ollie")
        deliver.assert_not_called()

    def test_default_app_gateway_chat_uses_the_top_level_policy(self) -> None:
        config = common.import_app_config(
            common.load_config(),
            {
                "application_id": "999",
                "public_key": "ab" * 32,
                "guild_allowlist": ["allowed-guild"],
                "channel_allowlist": ["22"],
            },
        )
        common.set_chat_binding(
            config,
            "room",
            "22",
            ["teams.lead"],
            guild_id="blocked-guild",
            channel_metadata={"channel_type": 0},
        )
        message = {
            "id": "default-policy-203",
            "guild_id": "blocked-guild",
            "channel_id": "22",
            "content": "<@999> hello",
            "mentions": [{"id": "999"}],
            "author": {"id": "u-2", "username": "alice"},
        }

        with mock.patch.object(common, "deliver_session_message") as deliver:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "rejected_policy")
        self.assertEqual(outcome["reason"], "guild_not_allowed")
        self.assertEqual(common.load_chat_ingress("in-default-policy-203")["app"], "")
        deliver.assert_not_called()

    def test_named_app_rest_recovery_uses_its_own_token(self) -> None:
        named_token = "ollie-test-token"
        config = common.import_app_config(
            common.load_config(),
            {"application_id": "999", "public_key": "ab" * 32},
            app_name="ollie",
            bot_token=named_token,
        )
        common.set_chat_binding(
            config,
            "room",
            "22",
            ["teams.lead"],
            guild_id="1",
            app_name="ollie",
            channel_metadata={"channel_type": 0},
        )
        message = {
            "id": "recovery-204",
            "guild_id": "1",
            "channel_id": "22",
            "content": "",
            "mentions": [{"id": "999"}],
            "author": {"id": "u-2", "username": "alice"},
        }

        with mock.patch.object(
            common,
            "discord_api_request",
            return_value={**message, "content": "<@999> recovered body"},
        ) as discord_api_request, mock.patch.object(
            common,
            "session_index_by_name",
            return_value={"teams.lead": {"session_name": "teams.lead", "state": "active"}},
        ), mock.patch.object(common, "deliver_session_message", return_value={"status": "accepted"}):
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999", app_name="ollie")

        self.assertEqual(outcome["status"], "delivered")
        self.assertEqual(discord_api_request.call_args.kwargs["bot_token"], named_token)

    def test_named_app_binding_takes_precedence_over_default_room_launcher(self) -> None:
        config = common.set_room_launcher(common.load_config(), "1", "22")
        config = common.import_app_config(
            config,
            {"application_id": "999", "public_key": "ab" * 32},
            app_name="ollie",
        )
        common.set_chat_binding(
            config,
            "room",
            "22",
            ["teams.lead"],
            guild_id="1",
            app_name="ollie",
            channel_metadata={"channel_type": 0},
        )
        message = {
            "id": "launcher-isolation-205",
            "guild_id": "1",
            "channel_id": "22",
            "content": "<@999> use the direct binding",
            "mentions": [{"id": "999"}],
            "author": {"id": "u-2", "username": "alice"},
        }

        with mock.patch.object(
            common,
            "session_index_by_name",
            return_value={"teams.lead": {"session_name": "teams.lead", "state": "active"}},
        ), mock.patch.object(common, "deliver_session_message", return_value={"status": "accepted"}) as deliver:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999", app_name="ollie")

        self.assertEqual(outcome["status"], "delivered")
        self.assertEqual(deliver.call_args.args[0], "teams.lead")
        self.assertEqual(outcome["receipt"]["binding_id"], "room:22@app:ollie")

    def test_named_app_channel_policy_accepts_thread_under_allowed_parent(self) -> None:
        config = common.import_app_config(
            common.load_config(),
            {
                "application_id": "999",
                "public_key": "ab" * 32,
                "guild_allowlist": ["1"],
                "channel_allowlist": ["22"],
            },
            app_name="ollie",
            bot_token="ollie-test-token",
        )
        common.set_chat_binding(
            config,
            "room",
            "22",
            ["teams.lead"],
            guild_id="1",
            app_name="ollie",
            channel_metadata={"channel_type": 0},
        )
        message = {
            "id": "thread-policy-206",
            "guild_id": "1",
            "channel_id": "222",
            "content": "<@999> thread follow-up",
            "mentions": [{"id": "999"}],
            "author": {"id": "u-2", "username": "alice"},
        }

        with mock.patch.object(
            common,
            "discord_api_request",
            return_value={"id": "222", "parent_id": "22", "type": 11},
        ), mock.patch.object(
            common,
            "session_index_by_name",
            return_value={"teams.lead": {"session_name": "teams.lead", "state": "active"}},
        ), mock.patch.object(common, "deliver_session_message", return_value={"status": "accepted"}):
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999", app_name="ollie")

        self.assertEqual(outcome["status"], "delivered")
        self.assertEqual(outcome["receipt"]["binding_id"], "room:22@app:ollie")

    def test_named_app_channel_policy_accepts_a_direct_thread_binding_under_allowed_parent(self) -> None:
        config = common.import_app_config(
            common.load_config(),
            {
                "application_id": "999",
                "public_key": "ab" * 32,
                "guild_allowlist": ["1"],
                "channel_allowlist": ["22"],
            },
            app_name="ollie",
        )
        common.set_chat_binding(
            config,
            "room",
            "222",
            ["teams.lead"],
            guild_id="1",
            app_name="ollie",
            channel_metadata={"channel_type": 11, "thread_parent_id": "22"},
        )
        message = {
            "id": "direct-thread-policy-206",
            "guild_id": "1",
            "channel_id": "222",
            "content": "<@999> direct thread follow-up",
            "mentions": [{"id": "999"}],
            "author": {"id": "u-2", "username": "alice"},
        }

        with mock.patch.object(
            common,
            "session_index_by_name",
            return_value={"teams.lead": {"session_name": "teams.lead", "state": "active"}},
        ), mock.patch.object(common, "deliver_session_message", return_value={"status": "accepted"}):
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999", app_name="ollie")

        self.assertEqual(outcome["status"], "delivered")
        self.assertEqual(outcome["receipt"]["binding_id"], "room:222@app:ollie")

    def test_default_gateway_policy_rejects_before_extmsg_routing(self) -> None:
        common.import_app_config(
            common.load_config(),
            {
                "application_id": "999",
                "public_key": "ab" * 32,
                "guild_allowlist": ["1"],
                "channel_allowlist": ["22"],
            },
        )
        runtime_state = gateway_service.GatewayRuntimeState()
        with mock.patch.object(gateway_service, "GATEWAY_WORKER_THREADS", 0):
            worker = gateway_service.GatewayWorker(runtime_state)
        self.addCleanup(worker.stop)
        message = {
            "id": "extmsg-policy-207",
            "guild_id": "1",
            "channel_id": "33",
            "content": "@sky hello",
            "author": {"id": "u-2", "username": "alice"},
        }

        with mock.patch.object(gateway_service, "_resolve_thread_parent", return_value=""), mock.patch.object(
            common,
            "resolve_at_mentions",
            return_value=["sky"],
        ) as resolve_at_mentions, mock.patch.object(
            common,
            "resolve_mention_targets",
            return_value=[{"mention": "sky"}],
        ), mock.patch.object(common, "launch_thread_for_mentions") as launch_thread_for_mentions:
            worker.handle_gateway_message(message, bot_user_id="999")

        resolve_at_mentions.assert_called_once_with("@sky hello")
        launch_thread_for_mentions.assert_not_called()
        receipt = common.load_chat_ingress("in-extmsg-policy-207")
        self.assertEqual(receipt["status"], "rejected_policy")
        self.assertEqual(receipt["reason"], "channel_not_allowed")
        self.assertEqual(receipt["app"], "")

    def test_default_gateway_policy_does_not_turn_bot_messages_into_failures(self) -> None:
        common.import_app_config(
            common.load_config(),
            {
                "application_id": "999",
                "public_key": "ab" * 32,
                "guild_allowlist": ["1"],
                "channel_allowlist": ["22"],
            },
        )
        worker = self._new_gateway_worker()
        message = {
            "id": "extmsg-bot-policy-208",
            "guild_id": "1",
            "channel_id": "33",
            "content": "automated",
            "author": {"id": "other-bot", "username": "robot", "bot": True},
        }

        worker.handle_gateway_message(message, bot_user_id="999")

        self.assertIsNone(common.load_chat_ingress("in-extmsg-bot-policy-208"))
        self.assertEqual(worker.runtime_state.snapshot()["ignored_messages"], 1)

    def test_default_gateway_policy_does_not_persist_irrelevant_unmentioned_messages(self) -> None:
        common.import_app_config(
            common.load_config(),
            {
                "application_id": "999",
                "public_key": "ab" * 32,
                "guild_allowlist": ["1"],
                "channel_allowlist": ["22"],
            },
        )
        worker = self._new_gateway_worker()
        message = {
            "id": "extmsg-unmentioned-policy-209",
            "guild_id": "1",
            "channel_id": "33",
            "content": "ordinary chatter",
            "author": {"id": "u-2", "username": "alice"},
        }

        with mock.patch.object(gateway_service, "_resolve_thread_parent", return_value=""):
            worker.handle_gateway_message(message, bot_user_id="999")

        self.assertIsNone(common.load_chat_ingress("in-extmsg-unmentioned-policy-209"))
        self.assertEqual(worker.runtime_state.snapshot()["ignored_messages"], 1)

    def test_extmsg_prefers_a_direct_bindings_stored_thread_parent(self) -> None:
        self._configure_discord_app()
        common.set_chat_binding(
            common.load_config(),
            "room",
            "222",
            ["randy"],
            guild_id="1",
            channel_metadata={"channel_type": 11, "thread_parent_id": "22"},
        )
        worker = self._new_gateway_worker()
        message = {
            "id": "stored-thread-parent-210",
            "guild_id": "1",
            "channel_id": "222",
            "content": "@randy still there?",
            "author": {"id": "u-2", "username": "alice"},
        }

        with mock.patch.object(gateway_service, "_resolve_thread_parent") as resolve_thread_parent:
            handled = worker._record_extmsg_inbound(message, bot_user_id="999")

        self.assertFalse(handled)
        resolve_thread_parent.assert_not_called()

    def test_default_extmsg_ignores_a_thread_message_aimed_only_at_a_named_bot(self) -> None:
        config = common.import_app_config(
            common.load_config(),
            {"application_id": "111", "public_key": "ab" * 32},
        )
        common.import_app_config(
            config,
            {"application_id": "222", "public_key": "cd" * 32},
            app_name="ollie",
        )
        worker = self._new_gateway_worker()
        message = {
            "id": "named-only-thread-211",
            "guild_id": "1",
            "channel_id": "2222",
            "content": "<@222> can you handle this?",
            "mentions": [{"id": "222"}],
            "author": {"id": "u-2", "username": "alice"},
        }

        with mock.patch.object(gateway_service, "_resolve_thread_parent", return_value="22"), mock.patch.object(
            common,
            "resolve_at_mentions",
            return_value=[],
        ), mock.patch.object(common, "resolve_nl_agent_mentions", return_value=[]), mock.patch.object(
            common,
            "normalize_to_extmsg_message",
            return_value={"id": "normalized"},
        ), mock.patch.object(common, "deliver_to_extmsg") as deliver_to_extmsg:
            handled = worker._record_extmsg_inbound(message, bot_user_id="111")

        self.assertFalse(handled)
        deliver_to_extmsg.assert_not_called()

    def test_named_ambient_binding_cache_cannot_substitute_default_binding(self) -> None:
        config = common.set_chat_binding(
            common.load_config(),
            "room",
            "22",
            ["legacy.session"],
            guild_id="1",
            policy={"ambient_read_enabled": True, "allow_untargeted_ambient_delivery": True},
            channel_metadata={"channel_type": 0},
        )
        config = common.import_app_config(
            config,
            {"application_id": "999", "public_key": "ab" * 32},
            app_name="ollie",
        )
        common.set_chat_binding(
            config,
            "room",
            "22",
            ["teams.lead"],
            guild_id="1",
            app_name="ollie",
            policy={"ambient_read_enabled": True, "allow_untargeted_ambient_delivery": True},
            channel_metadata={"channel_type": 0},
        )
        message = {
            "id": "ambient-cache-207",
            "guild_id": "1",
            "channel_id": "22",
            "content": "<@999> route to Ollie",
            "mentions": [{"id": "999"}],
            "author": {"id": "u-2", "username": "alice"},
        }

        with mock.patch.object(
            common,
            "session_index_by_name",
            return_value={
                "legacy.session": {"session_name": "legacy.session", "state": "active"},
                "teams.lead": {"session_name": "teams.lead", "state": "active"},
            },
        ), mock.patch.object(common, "deliver_session_message", return_value={"status": "accepted"}) as deliver:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999", app_name="ollie")

        self.assertEqual(outcome["status"], "delivered")
        self.assertEqual(deliver.call_args.args[0], "teams.lead")
        self.assertEqual(outcome["receipt"]["binding_id"], "room:22@app:ollie")

    def test_sticky_named_binding_ignores_message_for_another_configured_bot(self) -> None:
        config = common.import_app_config(
            common.load_config(),
            {"application_id": "999", "public_key": "ab" * 32},
            app_name="ollie",
        )
        config = common.import_app_config(
            config,
            {"application_id": "998", "public_key": "cd" * 32},
            app_name="olivia",
        )
        config = common.set_chat_binding(
            config,
            "room",
            "22",
            ["teams.lead"],
            guild_id="1",
            app_name="ollie",
            policy={"ambient_read_enabled": True, "allow_untargeted_ambient_delivery": True},
            channel_metadata={"channel_type": 0},
        )
        common.set_chat_binding(
            config,
            "room",
            "22",
            ["teams.pm"],
            guild_id="1",
            app_name="olivia",
            policy={"ambient_read_enabled": True, "allow_untargeted_ambient_delivery": True},
            channel_metadata={"channel_type": 0},
        )
        message = {
            "id": "other-bot-208",
            "guild_id": "1",
            "channel_id": "22",
            "content": "<@999> Ollie only",
            "mentions": [{"id": "999"}],
            "author": {"id": "u-2", "username": "alice"},
        }

        with mock.patch.object(
            common,
            "session_index_by_name",
            return_value={
                "teams.lead": {"session_name": "teams.lead", "state": "active"},
                "teams.pm": {"session_name": "teams.pm", "state": "active"},
            },
        ), mock.patch.object(common, "deliver_session_message", return_value={"status": "accepted"}) as deliver:
            lead_outcome = gateway_service.process_inbound_message(message, bot_user_id="999", app_name="ollie")
            pm_outcome = gateway_service.process_inbound_message(message, bot_user_id="998", app_name="olivia")

        self.assertEqual(lead_outcome["status"], "delivered")
        self.assertEqual(pm_outcome, {
            "status": "ignored",
            "reason": "different_configured_bot_mentioned",
            "ingress_id": "in-other-bot-208-app-olivia",
        })
        self.assertEqual([call.args[0] for call in deliver.call_args_list], ["teams.lead"])

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

    def test_record_extmsg_inbound_skips_bound_room_mentions(self) -> None:
        self._configure_discord_app()
        common.set_chat_binding(
            common.load_config(),
            "room",
            "22",
            ["randy"],
            guild_id="1",
            policy={"ambient_read_enabled": True, "allow_untargeted_ambient_delivery": True},
        )
        worker = self._new_gateway_worker()
        message = {
            "id": "207a",
            "guild_id": "1",
            "channel_id": "22",
            "content": "@randy what changed?",
            "author": {"id": "u-207a", "username": "alice"},
        }

        with mock.patch.object(common, "resolve_at_mentions", return_value=["randy"]) as resolve_at_mentions, mock.patch.object(
            common, "resolve_mention_targets"
        ) as resolve_mention_targets, mock.patch.object(common, "launch_thread_for_mentions") as launch_thread_for_mentions:
            handled = worker._record_extmsg_inbound(message, bot_user_id="999")

        self.assertFalse(handled)
        resolve_at_mentions.assert_not_called()
        resolve_mention_targets.assert_not_called()
        launch_thread_for_mentions.assert_not_called()

    def test_handle_gateway_message_prefers_bound_room_over_extmsg_thread_launch(self) -> None:
        self._configure_discord_app()
        common.set_chat_binding(
            common.load_config(),
            "room",
            "22",
            ["randy"],
            guild_id="1",
            policy={"ambient_read_enabled": True, "allow_untargeted_ambient_delivery": True},
        )
        worker = self._new_gateway_worker()
        message = {
            "id": "207aa",
            "guild_id": "1",
            "channel_id": "22",
            "content": "@randy what changed?",
            "author": {"id": "u-207aa", "username": "alice"},
        }

        with mock.patch.object(common, "launch_thread_for_mentions") as launch_thread_for_mentions, mock.patch.object(
            common,
            "session_index_by_name",
            return_value={"randy": {"session_name": "randy", "state": "active"}},
        ), mock.patch.object(
            common,
            "deliver_session_message",
            return_value={"status": "accepted"},
        ) as deliver_session_message:
            worker.handle_gateway_message(message, bot_user_id="999")

        launch_thread_for_mentions.assert_not_called()
        deliver_session_message.assert_called_once()
        self.assertEqual(deliver_session_message.call_args.args[0], "randy")
        receipt = common.load_chat_ingress("in-207aa")
        assert receipt is not None
        self.assertEqual(receipt["binding_id"], "room:22")
        self.assertEqual(receipt["status"], "delivered")

    def test_record_extmsg_inbound_skips_thread_when_parent_room_is_bound(self) -> None:
        self._configure_discord_app()
        common.set_chat_binding(common.load_config(), "room", "22", ["randy"], guild_id="1")
        worker = self._new_gateway_worker()
        message = {
            "id": "207b",
            "guild_id": "1",
            "channel_id": "222",
            "content": "@randy still there?",
            "author": {"id": "u-207b", "username": "alice"},
        }

        with mock.patch.object(gateway_service, "_resolve_thread_parent", return_value="22"), mock.patch.object(
            common, "deliver_to_extmsg"
        ) as deliver_to_extmsg, mock.patch.object(common, "normalize_to_extmsg_message") as normalize_to_extmsg_message:
            handled = worker._record_extmsg_inbound(message, bot_user_id="999")

        self.assertFalse(handled)
        normalize_to_extmsg_message.assert_not_called()
        deliver_to_extmsg.assert_not_called()

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
        self.assertIn("reply_tool: gc discord reply-current --body-file <path>", envelope)
        self.assertIn("reply_turn_requirement: if you intend to answer, do not end the turn without a successful reply-current", envelope)
        self.assertIn(
            "peer_targeting_rule: include @@rig/alias in the Discord reply if you want another launcher participant to receive it as peer input",
            envelope,
        )
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
                "participants": {
                    "corp/sky": {
                        "qualified_handle": "corp/sky",
                        "session_alias": "dc-123-sky",
                        "session_name": "dc-123-sky",
                        "primer_version": common.ROOM_LAUNCH_PRIMER_VERSION,
                        "primed_at": "2026-03-22T00:00:00Z",
                    }
                },
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
            "ensure_room_launch_session_for_handle",
            return_value=(
                common.load_room_launch("room-launch:222") or {},
                {
                    "qualified_handle": "corp/sky",
                    "session_alias": "dc-123-sky",
                    "session_name": "dc-123-sky",
                    "session_id": "gc-sky",
                },
            ),
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
                            "qualified_handle": "corp/alex",
                            "session_alias": "dc-456-alex",
                            "session_name": "dc-alex",
                            "session_id": "gc-alex",
                        },
                    },
                },
                {
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
        self.assertIn("reply_success_signal: record.remote_message_id", envelope)
        self.assertIn(
            "peer_targeting_rule: include @@rig/alias in the Discord reply if you want another launcher participant to receive it as peer input",
            envelope,
        )
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

    def test_process_inbound_room_message_broadcasts_to_all_bound_selectors(self) -> None:
        common.set_chat_binding(common.load_config(), "room", "22", ["sky", "lawrence"], guild_id="1")
        message = {
            "id": "214",
            "guild_id": "1",
            "channel_id": "22",
            "content": "<@999> please investigate",
            "mentions": [{"id": "999"}],
            "author": {"id": "u-24", "username": "alice"},
        }

        with mock.patch.object(common, "deliver_session_message", return_value={"status": "accepted"}) as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "delivered")
        self.assertEqual(deliver_session_message.call_count, 2)
        self.assertEqual(deliver_session_message.call_args_list[0].args[0], "sky")
        self.assertEqual(deliver_session_message.call_args_list[1].args[0], "lawrence")

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

    def test_process_inbound_ambient_room_message_routes_untargeted_single_session_when_enabled(self) -> None:
        common.set_chat_binding(
            common.load_config(),
            "room",
            "22",
            ["randy"],
            guild_id="1",
            policy={"ambient_read_enabled": True, "allow_untargeted_ambient_delivery": True},
        )
        message = {
            "id": "507-single",
            "guild_id": "1",
            "channel_id": "22",
            "content": "what changed since yesterday?",
            "mentions": [],
            "author": {"id": "u-5b1", "username": "alice"},
        }

        with mock.patch.object(common, "deliver_session_message", return_value={"status": "accepted"}) as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "delivered")
        deliver_session_message.assert_called_once()
        self.assertEqual(deliver_session_message.call_args.args[0], "randy")
        receipt = common.load_chat_ingress("in-507-single")
        assert receipt is not None
        self.assertEqual(receipt["status"], "delivered")
        self.assertEqual(receipt["delivery"], "broadcast")

    def test_process_inbound_ambient_room_message_routes_unknown_alias_to_sticky_single_session(self) -> None:
        common.set_chat_binding(
            common.load_config(),
            "room",
            "22",
            ["deacon__deacon"],
            guild_id="1",
            policy={"ambient_read_enabled": True, "allow_untargeted_ambient_delivery": True},
        )
        message = {
            "id": "507-sticky-alias",
            "guild_id": "1",
            "channel_id": "22",
            "content": "@deacon: can you check the dashboard deploy?",
            "mentions": [],
            "author": {"id": "u-5b2", "username": "alice"},
        }

        with mock.patch.object(common, "deliver_session_message", return_value={"status": "accepted"}) as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "delivered")
        deliver_session_message.assert_called_once()
        self.assertEqual(deliver_session_message.call_args.args[0], "deacon__deacon")
        receipt = common.load_chat_ingress("in-507-sticky-alias")
        assert receipt is not None
        self.assertEqual(receipt["status"], "delivered")
        self.assertEqual(receipt["delivery"], "broadcast")
        self.assertEqual(receipt["mentioned_aliases"], ["deacon"])

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

    def test_process_inbound_bot_mentioned_ambient_room_routes_untargeted_single_session_when_enabled(self) -> None:
        common.set_chat_binding(
            common.load_config(),
            "room",
            "22",
            ["randy"],
            guild_id="1",
            policy={"ambient_read_enabled": True, "allow_untargeted_ambient_delivery": True},
        )
        message = {
            "id": "508-single",
            "guild_id": "1",
            "channel_id": "22",
            "content": "<@999> what changed since yesterday?",
            "mentions": [{"id": "999"}],
            "author": {"id": "u-5c1", "username": "alice"},
        }

        with mock.patch.object(
            common,
            "session_index_by_name",
            return_value={"randy": {"session_name": "randy", "state": "active"}},
        ), mock.patch.object(common, "deliver_session_message", return_value={"status": "accepted"}) as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "delivered")
        deliver_session_message.assert_called_once()
        self.assertEqual(deliver_session_message.call_args.args[0], "randy")
        receipt = common.load_chat_ingress("in-508-single")
        assert receipt is not None
        self.assertEqual(receipt["status"], "delivered")

    def test_process_inbound_bot_mentioned_ambient_room_delivers_to_unmaterialized_named_selector(self) -> None:
        common.set_chat_binding(
            common.load_config(),
            "room",
            "22",
            ["employees.corp--alex"],
            guild_id="1",
            policy={"ambient_read_enabled": True, "allow_untargeted_ambient_delivery": True},
        )
        message = {
            "id": "508-unmaterialized",
            "guild_id": "1",
            "channel_id": "22",
            "content": "<@999> are you there?",
            "mentions": [{"id": "999"}],
            "author": {"id": "u-5c2", "username": "alice"},
        }

        with mock.patch.object(common, "deliver_session_message", return_value={"status": "accepted"}) as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "delivered")
        deliver_session_message.assert_called_once()
        self.assertEqual(deliver_session_message.call_args.args[0], "employees.corp--alex")
        receipt = common.load_chat_ingress("in-508-unmaterialized")
        assert receipt is not None
        self.assertEqual(receipt["status"], "delivered")

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

    def test_process_inbound_message_persists_async_correlation_when_result_is_unknown(self) -> None:
        common.set_chat_binding(common.load_config(), "dm", "55", ["sky"])
        message = {
            "id": "917",
            "channel_id": "55",
            "content": "hello from discord",
            "author": {"id": "u-17", "username": "alice"},
        }

        def unknown_after_acceptance(*args: object, **kwargs: object) -> dict[str, object]:
            kwargs["on_async_accepted"](
                {
                    "http_status": 202,
                    "request_id": "req-17",
                    "event_cursor": "42",
                    "intent": "follow_up",
                    "response": {"status": "accepted", "request_id": "req-17", "event_cursor": "42"},
                }
            )
            raise common.GCAPIResultUnknown("event stream closed")

        with mock.patch.object(common, "session_index_by_name", return_value={"sky": {"session_name": "sky", "state": "active"}}), mock.patch.object(
            common,
            "deliver_session_message",
            side_effect=unknown_after_acceptance,
        ):
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "pending")
        receipt = common.load_chat_ingress("in-917")
        assert receipt is not None
        self.assertEqual(receipt["delivery_protocol_version"], 2)
        self.assertEqual(receipt["targets"][0]["status"], "awaiting_result")
        self.assertEqual(receipt["targets"][0]["request_id"], "req-17")
        self.assertEqual(receipt["targets"][0]["event_cursor"], "42")

    def test_recover_pending_ingress_resumes_original_async_request_without_posting(self) -> None:
        common.atomic_write_json(
            common.chat_ingress_path("in-919-app-ollie"),
            {
                "ingress_id": "in-919-app-ollie",
                "app": "ollie",
                "status": "pending",
                "delivery_protocol_version": 2,
                "targets": [
                    {
                        "session_name": "teams.lead",
                        "status": "awaiting_result",
                        "request_id": "req-19",
                        "event_cursor": "51",
                        "intent": "follow_up",
                        "response": {"status": "accepted", "request_id": "req-19", "event_cursor": "51"},
                    }
                ],
                "created_at": "2000-01-01T00:00:00Z",
                "updated_at": "2000-01-01T00:00:00Z",
            },
        )
        terminal = {"request_id": "req-19", "session_id": "session-19", "queued": True}

        with mock.patch.object(
            common,
            "resume_session_message_delivery",
            return_value=terminal,
        ) as resume_session_message_delivery, mock.patch.object(
            common,
            "deliver_session_message",
        ) as deliver_session_message:
            recovered = gateway_service.recover_pending_ingress_receipts(
                bot_user_id="999",
                app_name="ollie",
                cancel_event=threading.Event(),
            )

        self.assertEqual(recovered, ["in-919-app-ollie"])
        deliver_session_message.assert_not_called()
        resume_session_message_delivery.assert_called_once_with(
            "req-19",
            "51",
            intent="follow_up",
            timeout=common.GC_API_ASYNC_RESULT_TIMEOUT_SECONDS,
            cancel_event=mock.ANY,
        )
        receipt = common.load_chat_ingress("in-919-app-ollie")
        assert receipt is not None
        self.assertEqual(receipt["status"], "delivered")
        self.assertEqual(receipt["targets"][0]["status"], "delivered")
        self.assertEqual(receipt["targets"][0]["terminal_evidence"], {"status": "succeeded", "payload": terminal})

    def test_recover_pending_ingress_does_not_retain_or_repost_pre_submission_body(self) -> None:
        common.atomic_write_json(
            common.chat_ingress_path("in-921-app-ollie"),
            {
                "ingress_id": "in-921-app-ollie",
                "app": "ollie",
                "status": "pending",
                "delivery_protocol_version": 2,
                "targets": [
                    {
                        "session_name": "teams.lead",
                        "status": "pending",
                        "intent": "follow_up",
                        "idempotency_key": "ingress:in-921-app-ollie:target:teams.lead",
                    }
                ],
                "created_at": "2000-01-01T00:00:00Z",
                "updated_at": "2000-01-01T00:00:00Z",
            },
        )

        with mock.patch.object(
            common,
            "deliver_session_message",
            return_value={"status": "accepted"},
        ) as deliver_session_message:
            recovered = gateway_service.recover_pending_ingress_receipts(
                bot_user_id="999",
                app_name="ollie",
                cancel_event=threading.Event(),
            )

        self.assertEqual(recovered, ["in-921-app-ollie"])
        deliver_session_message.assert_not_called()
        receipt = common.load_chat_ingress("in-921-app-ollie")
        assert receipt is not None
        self.assertEqual(receipt["status"], "delivery_unknown")
        self.assertEqual(receipt["targets"][0]["status"], "delivery_unknown")
        self.assertEqual(receipt["targets"][0]["reason"], "delivery_payload_not_retained")
        self.assertNotIn("envelope", receipt["targets"][0])

    def test_recover_pending_ingress_quarantines_legacy_receipt_without_async_correlation(self) -> None:
        common.atomic_write_json(
            common.chat_ingress_path("in-920-app-ollie"),
            {
                "ingress_id": "in-920-app-ollie",
                "app": "ollie",
                "status": "pending",
                "targets": [{"session_name": "teams.lead", "status": "pending"}],
                "created_at": "2000-01-01T00:00:00Z",
                "updated_at": "2000-01-01T00:00:00Z",
            },
        )

        with mock.patch.object(common, "resume_session_message_delivery") as resume_session_message_delivery, mock.patch.object(
            common,
            "deliver_session_message",
        ) as deliver_session_message:
            recovered = gateway_service.recover_pending_ingress_receipts(
                bot_user_id="999",
                app_name="ollie",
                cancel_event=threading.Event(),
            )

        self.assertEqual(recovered, ["in-920-app-ollie"])
        resume_session_message_delivery.assert_not_called()
        deliver_session_message.assert_not_called()
        receipt = common.load_chat_ingress("in-920-app-ollie")
        assert receipt is not None
        self.assertEqual(receipt["status"], "delivery_unknown")
        self.assertEqual(receipt["targets"][0]["status"], "delivery_unknown")
        self.assertEqual(receipt["targets"][0]["reason"], "missing_async_correlation")

    def test_recovery_applies_missing_room_launch_thread_routing_side_effect(self) -> None:
        common.atomic_write_json(
            common.chat_ingress_path("in-922-app-ollie"),
            {
                "ingress_id": "in-922-app-ollie",
                "app": "ollie",
                "status": "delivered",
                "delivery_protocol_version": 2,
                "route_kind": "room_launch_thread",
                "launch_id": "room-launch:thread-922",
                "qualified_handle": "corp/priya",
                "targets": [{"session_name": "teams.priya", "status": "delivered"}],
                "created_at": "2000-01-01T00:00:00Z",
                "updated_at": "2000-01-01T00:00:00Z",
            },
        )

        with mock.patch.object(
            common,
            "set_room_launch_last_addressed",
            return_value={"launch_id": "room-launch:thread-922"},
        ) as set_room_launch_last_addressed:
            recovered = gateway_service.recover_pending_ingress_receipts(
                bot_user_id="999",
                app_name="ollie",
                cancel_event=threading.Event(),
            )

        self.assertEqual(recovered, ["in-922-app-ollie"])
        set_room_launch_last_addressed.assert_called_once_with(
            "room-launch:thread-922",
            "corp/priya",
            delivery_order=mock.ANY,
        )
        receipt = common.load_chat_ingress("in-922-app-ollie")
        assert receipt is not None
        self.assertTrue(receipt["routing_state_applied_at"])

    def test_recovery_does_not_replay_routing_state_for_legacy_delivered_receipt(self) -> None:
        common.atomic_write_json(
            common.chat_ingress_path("in-923-app-ollie"),
            {
                "ingress_id": "in-923-app-ollie",
                "app": "ollie",
                "status": "delivered",
                "route_kind": "room_launch_thread",
                "launch_id": "room-launch:thread-923",
                "qualified_handle": "corp/priya",
                "targets": [{"session_name": "teams.priya", "status": "delivered"}],
                "created_at": "2000-01-01T00:00:00Z",
                "updated_at": "2000-01-01T00:00:00Z",
            },
        )

        with mock.patch.object(common, "set_room_launch_last_addressed") as set_room_launch_last_addressed:
            recovered = gateway_service.recover_pending_ingress_receipts(
                bot_user_id="999",
                app_name="ollie",
                cancel_event=threading.Event(),
            )

        self.assertEqual(recovered, [])
        set_room_launch_last_addressed.assert_not_called()

    def test_process_inbound_message_keeps_fresh_pending_receipt_as_duplicate(self) -> None:
        common.set_chat_binding(common.load_config(), "dm", "55", ["sky"])
        now = common.utcnow()
        common.atomic_write_json(
            common.chat_ingress_path("in-918"),
            {
                "ingress_id": "in-918",
                "status": "pending",
                "created_at": now,
                "updated_at": now,
            },
        )
        message = {
            "id": "918",
            "channel_id": "55",
            "content": "hello from discord",
            "author": {"id": "u-18", "username": "alice"},
        }

        with mock.patch.object(common, "deliver_session_message") as deliver_session_message:
            outcome = gateway_service.process_inbound_message(message, bot_user_id="999")

        self.assertEqual(outcome["status"], "duplicate")
        deliver_session_message.assert_not_called()
        receipt = common.load_chat_ingress("in-918")
        assert receipt is not None
        self.assertEqual(receipt["status"], "pending")

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
        self.assertEqual(receipt["app"], "")

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
        self.assertEqual(receipt["app"], "")

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

    def test_worker_stop_rejects_queued_messages_before_exit(self) -> None:
        runtime_state = gateway_service.GatewayRuntimeState()
        with mock.patch.object(gateway_service, "GATEWAY_WORKER_THREADS", 1):
            worker = gateway_service.GatewayWorker(runtime_state)
        self.addCleanup(lambda: worker.stop() if not worker.stop_event.is_set() else None)

        handled: list[str] = []
        active_started = threading.Event()

        def handle_until_stopped(message: dict[str, object], bot_user_id: str) -> None:
            del bot_user_id
            handled.append(str(message.get("id", "")))
            active_started.set()
            worker.stop_event.wait(timeout=1)

        with mock.patch.object(worker, "handle_gateway_message", side_effect=handle_until_stopped):
            worker.dispatch_gateway_message({"id": "1000", "channel_id": "55", "author": {"id": "u-1000"}}, "999")
            self.assertTrue(active_started.wait(timeout=1))
            worker.dispatch_gateway_message({"id": "1001", "channel_id": "55", "author": {"id": "u-1001"}}, "999")
            worker.stop()

        self.assertEqual(handled, ["1000"])
        self.assertTrue(worker.stop_event.is_set())
        self.assertTrue(all(not thread.is_alive() for thread in worker.worker_threads))
        receipt = common.load_chat_ingress("in-1001")
        assert receipt is not None
        self.assertEqual(receipt["status"], "rejected_shutting_down")
        self.assertEqual(receipt["reason"], "service_shutting_down")

    def test_named_worker_starts_app_scoped_pending_recovery(self) -> None:
        runtime_state = gateway_service.GatewayRuntimeState("ollie")
        with mock.patch.object(gateway_service, "GATEWAY_NAMED_WORKER_THREADS", 0):
            worker = gateway_service.GatewayWorker(runtime_state, "ollie")
        self.addCleanup(worker.stop)
        recovered = threading.Event()

        def recover_once(**kwargs: object) -> list[str]:
            recovered.set()
            worker.stop_event.set()
            return []

        with mock.patch.object(
            gateway_service,
            "recover_pending_ingress_receipts",
            side_effect=recover_once,
        ) as recover_pending_ingress_receipts:
            worker.start_pending_recovery("999")
            self.assertTrue(recovered.wait(timeout=1))
            assert worker.recovery_thread is not None
            worker.recovery_thread.join(timeout=1)

        recover_pending_ingress_receipts.assert_called_once_with(
            bot_user_id="999",
            app_name="ollie",
            cancel_event=worker.stop_event,
        )

    def test_handle_gateway_message_passes_worker_stop_event_to_delivery(self) -> None:
        runtime_state = gateway_service.GatewayRuntimeState("ollie")
        with mock.patch.object(gateway_service, "GATEWAY_NAMED_WORKER_THREADS", 0):
            worker = gateway_service.GatewayWorker(runtime_state, "ollie")
        self.addCleanup(worker.stop)
        message = {"id": "1003", "channel_id": "55", "author": {"id": "u-1003"}}

        with mock.patch.object(worker, "_record_extmsg_inbound", return_value=False), mock.patch.object(
            gateway_service,
            "process_inbound_message",
            return_value={"status": "duplicate", "receipt": {}},
        ) as process_inbound_message:
            worker.handle_gateway_message(message, "999")

        process_inbound_message.assert_called_once_with(
            message,
            "999",
            "ollie",
            cancel_event=worker.stop_event,
        )

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

    def test_worker_stop_is_bounded_when_consumer_ignores_cancellation(self) -> None:
        runtime_state = gateway_service.GatewayRuntimeState()
        with mock.patch.object(gateway_service, "GATEWAY_WORKER_THREADS", 1):
            worker = gateway_service.GatewayWorker(runtime_state)
        entered = threading.Event()
        release = threading.Event()

        def block_delivery(message: dict[str, object], bot_user_id: str) -> None:
            del message, bot_user_id
            entered.set()
            release.wait(timeout=2)

        with mock.patch.object(worker, "handle_gateway_message", side_effect=block_delivery), mock.patch.object(
            gateway_service,
            "GATEWAY_WORKER_STOP_TIMEOUT_SECONDS",
            0.05,
        ):
            worker.dispatch_gateway_message({"id": "1004", "channel_id": "55", "author": {"id": "u-1004"}}, "999")
            self.assertTrue(entered.wait(timeout=1))
            stop_thread = threading.Thread(target=worker.stop)
            stop_thread.start()
            stop_thread.join(timeout=0.25)
            returned_before_release = not stop_thread.is_alive()
            release.set()
            stop_thread.join(timeout=1)

        self.assertTrue(returned_before_release)
        self.assertEqual(runtime_state.snapshot()["state"], "stop_timeout")
        self.assertTrue(all(thread.daemon for thread in worker.worker_threads))
        for thread in worker.worker_threads:
            thread.join(timeout=1)

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

    def test_processing_receipt_staleness_exceeds_max_async_delivery_wait(self) -> None:
        max_delivery_wait = (
            common.GC_API_REQUEST_TIMEOUT_SECONDS
            + common.GC_API_ASYNC_RESULT_TIMEOUT_SECONDS
        )

        self.assertGreaterEqual(
            gateway_service.STALE_PROCESSING_RECEIPT_SECONDS - max_delivery_wait,
            60,
        )

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
        worker.app_name = ""

        bot_user_id = gateway_service.GatewayWorker.current_bot_user_id(
            worker,
            {"app": {"application_id": "bot-9"}},
            None,
            "bot-9",
        )

        self.assertEqual(bot_user_id, "bot-9")

    def test_named_gateway_rejects_resumed_identity_mismatched_to_configured_app(self) -> None:
        config = common.import_app_config(
            common.load_config(),
            {"application_id": "222", "public_key": "ab" * 32},
            app_name="ollie",
        )
        worker = object.__new__(gateway_service.GatewayWorker)
        worker.app_name = "ollie"

        with self.assertRaisesRegex(RuntimeError, "authenticated as.*configured application_id"):
            gateway_service.GatewayWorker.current_bot_user_id(
                worker,
                config,
                None,
                "999",
            )

    def test_named_gateway_rejects_ready_identity_mismatched_to_configured_app(self) -> None:
        config = common.import_app_config(
            common.load_config(),
            {"application_id": "222", "public_key": "ab" * 32},
            app_name="ollie",
        )
        worker = object.__new__(gateway_service.GatewayWorker)
        worker.app_name = "ollie"

        with self.assertRaisesRegex(RuntimeError, "authenticated as.*configured application_id"):
            gateway_service.GatewayWorker.current_bot_user_id(
                worker,
                config,
                {"user": {"id": "999"}},
            )

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

    def test_gateway_status_payload_preserves_legacy_default_fields_and_exposes_all_apps(self) -> None:
        states = {
            "default": {"state": "stopped", "routed_messages": 4},
            "ollie": {"state": "ready", "routed_messages": 7},
        }

        payload = gateway_service.gateway_status_payload(
            states,
            configured_app_names={"default", "ollie"},
            gc_api_reachable=True,
        )

        self.assertEqual(payload["state"], "stopped")
        self.assertEqual(payload["routed_messages"], 4)
        self.assertEqual(payload["gateway_statuses"], states)
        self.assertEqual(payload["aggregate"]["state"], "degraded")
        self.assertEqual(payload["aggregate"]["configured_apps"], 2)
        self.assertEqual(payload["aggregate"]["ready_apps"], 1)

    def test_aggregate_health_is_available_when_default_fails_but_named_app_is_ready(self) -> None:
        states = {
            "default": {"state": "stopped"},
            "ollie": {"state": "ready"},
        }

        self.assertEqual(
            gateway_service.aggregate_gateway_health_status_code(
                states,
                configured_app_names={"default", "ollie"},
                gc_api_reachable=True,
            ),
            gateway_service.HTTPStatus.NO_CONTENT,
        )

    def test_aggregate_health_is_available_when_named_app_fails_but_default_is_ready(self) -> None:
        states = {
            "default": {"state": "ready"},
            "ollie": {"state": "stopped"},
        }

        self.assertEqual(
            gateway_service.aggregate_gateway_health_status_code(
                states,
                configured_app_names={"default", "ollie"},
                gc_api_reachable=True,
            ),
            gateway_service.HTTPStatus.NO_CONTENT,
        )

    def test_reconnecting_named_worker_does_not_stop_ready_default_worker(self) -> None:
        config = common.import_app_config(
            common.load_config(),
            {"application_id": "111"},
            bot_token="default-test-token",
        )
        config = common.import_app_config(
            config,
            {"application_id": "222"},
            app_name="ollie",
            bot_token="ollie-test-token",
        )

        with mock.patch.object(gateway_service, "GATEWAY_WORKER_THREADS", 0), mock.patch.object(
            gateway_service,
            "GATEWAY_NAMED_WORKER_THREADS",
            0,
        ), mock.patch.object(gateway_service, "GATEWAY_IDENTIFY_STAGGER_SECONDS", 0):
            workers = gateway_service.build_gateway_workers(config)
        workers_by_app = {worker.app_name: worker for worker in workers}
        default_worker = workers_by_app[""]
        named_worker = workers_by_app["ollie"]

        ready_frames = iter(
            [
                {"op": 10, "d": {"heartbeat_interval": 60_000}},
                {
                    "op": 0,
                    "t": "READY",
                    "s": 1,
                    "d": {"user": {"id": "111"}, "session_id": "default-session"},
                },
            ]
        )
        ready_closed = threading.Event()

        def receive_ready_frame(timeout: float | None = None) -> dict[str, object]:
            del timeout
            try:
                return next(ready_frames)
            except StopIteration:
                ready_closed.wait()
                raise gateway_service.WebSocketClosed("websocket closed")

        ready_websocket = mock.Mock()
        ready_websocket.recv_event.side_effect = receive_ready_frame
        ready_websocket.close.side_effect = ready_closed.set

        named_attempts = 0
        named_retried = threading.Event()

        def fail_named_gateway_url(_bot_token: str = "") -> str:
            nonlocal named_attempts
            named_attempts += 1
            if named_attempts >= 2:
                named_retried.set()
            raise RuntimeError("named gateway unavailable")

        top_level_threads = [
            threading.Thread(target=default_worker.run_forever, name="discord-gateway-test-default"),
            threading.Thread(target=named_worker.run_forever, name="discord-gateway-test-ollie"),
        ]
        with mock.patch.object(default_worker, "gateway_url", return_value="wss://ready.example"), mock.patch.object(
            named_worker,
            "gateway_url",
            side_effect=fail_named_gateway_url,
        ), mock.patch.object(default_worker, "prune_runtime_data"), mock.patch.object(
            gateway_service,
            "GatewayWebSocket",
            return_value=ready_websocket,
        ), mock.patch.object(gateway_service, "RECONNECT_BASE_DELAY_SECONDS", 0.01), mock.patch.object(
            gateway_service,
            "RECONNECT_MAX_DELAY_SECONDS",
            0.01,
        ), mock.patch.object(gateway_service.random, "uniform", return_value=1.0):
            try:
                for thread in top_level_threads:
                    thread.start()

                self.assertTrue(named_retried.wait(2.0), "named gateway did not retry")
                deadline = time.monotonic() + 2.0
                while True:
                    default_state = default_worker.runtime_state.snapshot()
                    named_state = named_worker.runtime_state.snapshot()
                    if default_state["state"] == "ready" and named_state["state"] == "reconnecting":
                        break
                    if time.monotonic() >= deadline:
                        self.fail(f"gateway states did not converge: default={default_state}, named={named_state}")
                    time.sleep(0.01)

                self.assertTrue(top_level_threads[0].is_alive())
                self.assertTrue(top_level_threads[1].is_alive())
                self.assertFalse(default_worker.stop_event.is_set())
                self.assertFalse(named_worker.stop_event.is_set())
                self.assertTrue(default_state["connected"])
                self.assertIn("named gateway unavailable", named_state["last_error"])
            finally:
                for worker in workers:
                    worker.request_stop()
                for thread in top_level_threads:
                    if thread.ident is not None:
                        thread.join(timeout=2.0)
                for worker in workers:
                    worker.stop()

        self.assertTrue(all(not thread.is_alive() for thread in top_level_threads))

    def test_aggregate_health_fails_when_all_configured_apps_are_stale(self) -> None:
        states = {
            "default": {"state": "reconnecting", "last_ready_epoch": 1},
            "ollie": {"state": "stopped"},
        }

        self.assertEqual(
            gateway_service.aggregate_gateway_health_status_code(
                states,
                configured_app_names={"default", "ollie"},
                gc_api_reachable=True,
            ),
            gateway_service.HTTPStatus.SERVICE_UNAVAILABLE,
        )

    def test_aggregate_health_requires_shared_gc_api(self) -> None:
        states = {
            "default": {"state": "ready"},
            "ollie": {"state": "ready"},
        }

        self.assertEqual(
            gateway_service.aggregate_gateway_health_status_code(
                states,
                configured_app_names={"default", "ollie"},
                gc_api_reachable=False,
            ),
            gateway_service.HTTPStatus.SERVICE_UNAVAILABLE,
        )

    def test_aggregate_status_ignores_an_unconfigured_default_worker(self) -> None:
        states = {
            "default": {"state": "waiting_for_config"},
            "ollie": {"state": "ready"},
        }

        aggregate = gateway_service.aggregate_gateway_status(
            states,
            configured_app_names={"ollie"},
            gc_api_reachable=True,
        )

        self.assertEqual(aggregate["state"], "ready")
        self.assertEqual(aggregate["configured_apps"], 1)
        self.assertEqual(aggregate["ready_apps"], 1)

    def test_aggregate_health_allows_all_apps_to_provision(self) -> None:
        states = {
            "default": {"state": "starting"},
            "ollie": {"state": "waiting_for_config"},
        }

        self.assertEqual(
            gateway_service.aggregate_gateway_health_status_code(
                states,
                configured_app_names={"default", "ollie"},
                gc_api_reachable=True,
            ),
            gateway_service.HTTPStatus.NO_CONTENT,
        )

    def test_named_gateway_uses_one_consumer_while_default_keeps_legacy_pool(self) -> None:
        default_state = gateway_service.GatewayRuntimeState()
        named_state = gateway_service.GatewayRuntimeState("ollie")
        with mock.patch.object(gateway_service, "GATEWAY_WORKER_THREADS", 3), mock.patch.object(
            gateway_service,
            "GATEWAY_NAMED_WORKER_THREADS",
            1,
        ):
            default_worker = gateway_service.GatewayWorker(default_state)
            named_worker = gateway_service.GatewayWorker(named_state, "ollie")
        self.addCleanup(default_worker.stop)
        self.addCleanup(named_worker.stop)

        self.assertEqual(len(default_worker.worker_threads), 3)
        self.assertEqual(len(named_worker.worker_threads), 1)

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


class BotAuthorOptInTests(unittest.TestCase):
    """Opt-in acceptance of bot-authored inbound messages (default: dropped)."""

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
        gateway_service._bot_author_accept_times.clear()

    def tearDown(self) -> None:
        os.environ.clear()
        os.environ.update(self._old_environ)

    def _bind_room(self, extra_policy: dict | None = None) -> None:
        policy = {"ambient_read_enabled": True}
        policy.update(extra_policy or {})
        common.set_chat_binding(
            common.load_config(), "room", "22", ["sky"], guild_id="1", policy=policy
        )

    def _bot_message(self, message_id: str = "801", author_id: str = "77") -> dict:
        return {
            "id": message_id,
            "guild_id": "1",
            "channel_id": "22",
            "content": "@sky status ping",
            "mentions": [],
            "author": {"id": author_id, "username": "peer-mayor", "bot": True},
        }

    def _process(self, message: dict):
        with mock.patch.object(
            common,
            "session_index_by_name",
            return_value={"sky": {"session_name": "sky", "state": "active"}},
        ), mock.patch.object(
            common, "deliver_session_message", return_value={"status": "accepted"}
        ):
            return gateway_service.process_inbound_message(message, bot_user_id="999")

    def test_bot_message_dropped_by_default(self) -> None:
        self._bind_room()
        outcome = self._process(self._bot_message())
        self.assertEqual(outcome["status"], "ignored")
        self.assertEqual(outcome["reason"], "bot_message")

    def test_explicitly_allowed_bot_author_routes(self) -> None:
        self._bind_room({"allowed_bot_authors": ["77"]})
        outcome = self._process(self._bot_message())
        self.assertEqual(outcome["status"], "delivered")

    def test_guild_bot_wildcard_routes_any_bot_author(self) -> None:
        self._bind_room({"allow_guild_bots": True})
        outcome = self._process(self._bot_message(author_id="4242"))
        self.assertEqual(outcome["status"], "delivered")

    def test_own_bot_id_never_accepted_even_with_wildcard(self) -> None:
        self._bind_room({"allow_guild_bots": True})
        outcome = self._process(self._bot_message(author_id="999"))
        self.assertEqual(outcome["status"], "ignored")
        self.assertEqual(outcome["reason"], "bot_message")

    def test_unlisted_bot_author_still_dropped(self) -> None:
        self._bind_room({"allowed_bot_authors": ["4242"]})
        outcome = self._process(self._bot_message(author_id="77"))
        self.assertEqual(outcome["status"], "ignored")
        self.assertEqual(outcome["reason"], "bot_message")

    def test_bot_message_in_unbound_channel_dropped(self) -> None:
        self._bind_room({"allow_guild_bots": True})
        message = self._bot_message()
        message["channel_id"] = "33"  # no binding here; wildcard on "22" must not leak
        with mock.patch.object(gateway_service, "_resolve_thread_parent", return_value=""):
            outcome = self._process(message)
        self.assertEqual(outcome["status"], "ignored")
        self.assertEqual(outcome["reason"], "bot_message")

    def test_bot_dm_always_dropped(self) -> None:
        common.set_chat_binding(common.load_config(), "dm", "55", ["sky"])
        message = {
            "id": "802",
            "channel_id": "55",
            "content": "dm ping",
            "author": {"id": "77", "username": "peer-mayor", "bot": True},
        }
        outcome = self._process(message)
        self.assertEqual(outcome["status"], "ignored")
        self.assertEqual(outcome["reason"], "bot_message")

    def test_rate_limit_caps_accepted_bot_messages(self) -> None:
        self._bind_room(
            {"allowed_bot_authors": ["77"], "max_accepted_bot_author_messages_per_minute": 2}
        )
        self.assertEqual(self._process(self._bot_message("810")).get("status"), "delivered")
        self.assertEqual(self._process(self._bot_message("811")).get("status"), "delivered")
        outcome = self._process(self._bot_message("812"))
        self.assertEqual(outcome["status"], "ignored")
        self.assertEqual(outcome["reason"], "bot_author_rate_limited")

    def test_rate_charge_idempotent_per_message_id(self) -> None:
        self._bind_room(
            {"allowed_bot_authors": ["77"], "max_accepted_bot_author_messages_per_minute": 1}
        )
        self.assertEqual(self._process(self._bot_message("820")).get("status"), "delivered")
        # same message consulted again (extmsg + legacy both gate): not double-charged
        accepted, reason = gateway_service.bot_author_acceptance(
            common.load_config(), self._bot_message("820"), "999"
        )
        self.assertTrue(accepted)
        self.assertEqual(reason, "")

    def test_zero_rate_disables_acceptance(self) -> None:
        self._bind_room(
            {"allowed_bot_authors": ["77"], "max_accepted_bot_author_messages_per_minute": 0}
        )
        outcome = self._process(self._bot_message("830"))
        self.assertEqual(outcome["status"], "ignored")
        self.assertEqual(outcome["reason"], "bot_author_rate_limited")

    def test_human_messages_unaffected(self) -> None:
        self._bind_room()
        message = self._bot_message("840")
        message["author"] = {"id": "u-1", "username": "alice"}
        outcome = self._process(message)
        self.assertEqual(outcome["status"], "delivered")
