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

    def tearDown(self) -> None:
        os.environ.clear()
        os.environ.update(self._old_environ)

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

        with mock.patch.object(common, "discord_api_request", return_value={"id": "222", "parent_id": "22"}), mock.patch.object(
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
        self.assertIn("publish_conversation_id: 222", envelope)
        self.assertIn("publish_trigger_id: 212", envelope)

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

        with mock.patch.object(common, "discord_api_request", return_value={"id": "222", "parent_id": "22"}) as discord_api_request, mock.patch.object(
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
            return {"id": "222", "parent_id": "22"}

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
        self.assertEqual(results, [{"id": "222", "parent_id": "22"}, {"id": "222", "parent_id": "22"}])

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
