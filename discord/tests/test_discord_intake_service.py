from __future__ import annotations

import base64
import pathlib
import tempfile
import unittest
from unittest import mock

import os
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "scripts"))

import discord_intake_common as common
import discord_intake_service as service


class DiscordIntakeServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self._old_environ = os.environ.copy()
        os.environ["GC_CITY_ROOT"] = self.tempdir.name
        service.LAST_REQUEST_PRUNE_AT = 0.0

    def tearDown(self) -> None:
        os.environ.clear()
        os.environ.update(self._old_environ)

    def write_rig_route(self, rig: str) -> None:
        beads_dir = pathlib.Path(self.tempdir.name, ".beads")
        beads_dir.mkdir(parents=True, exist_ok=True)
        pathlib.Path(self.tempdir.name, rig).mkdir(parents=True, exist_ok=True)
        pathlib.Path(beads_dir, "routes.jsonl").write_text(f'{{"path":"{rig}"}}\n', encoding="utf-8")

    def test_fix_command_behavior(self) -> None:
        behavior = service.command_behavior("fix")

        self.assertEqual(behavior["workflow_scope"], "conversation")

    def test_parse_application_command_reads_prompt_option(self) -> None:
        payload = {
            "data": {
                "name": "gc",
                "options": [
                    {
                        "type": 1,
                        "name": "fix",
                        "options": [{"type": 3, "name": "prompt", "value": "crash on startup\nwhen x is unset"}],
                    }
                ],
            }
        }

        parsed = service.parse_application_command(payload, "gc")

        self.assertEqual(parsed["command"], "fix")
        self.assertIn("crash on startup", parsed["prompt"])

    def test_extract_modal_fields_reads_summary_and_context(self) -> None:
        payload = {
            "data": {
                "custom_id": "gc:fix:abc",
                "components": [
                    {
                        "type": 1,
                        "components": [
                            {"type": 4, "custom_id": "summary", "value": "Crash on boot"},
                            {"type": 4, "custom_id": "context", "value": "unset env X"},
                        ],
                    }
                ],
            }
        }

        fields = service.extract_modal_fields(payload)

        self.assertEqual(fields["summary"], "Crash on boot")
        self.assertEqual(fields["context"], "unset env X")

    def test_build_fix_bead_notes_includes_discord_context(self) -> None:
        request = {
            "guild_id": "1",
            "channel_id": "2",
            "thread_id": "3",
            "conversation_id": "3",
            "jump_url": "https://discord.com/channels/1/3",
            "request_id": "dc-1-fix",
            "invoking_user_display_name": "alice",
            "invoking_user_id": "99",
            "summary": "Crash on startup",
            "context_markdown": "repro: unset X",
        }

        notes = service.build_fix_bead_notes(request)

        self.assertIn("## Discord Source", notes)
        self.assertIn("Crash on startup", notes)
        self.assertIn("repro: unset X", notes)
        self.assertIn("https://discord.com/channels/1/3", notes)

    def test_build_fix_vars_encodes_prompt_sensitive_fields(self) -> None:
        request = {
            "request_id": "dc-1-fix",
            "guild_id": "1",
            "channel_id": "22",
            "thread_id": "44",
            "conversation_id": "44",
            "jump_url": "https://discord.com/channels/1/44",
            "invoking_user_display_name": "alice `review this`",
            "summary": "Crash on startup",
            "context_markdown": "unset env X",
        }

        variables = service.build_fix_vars(request, "bd-1")

        self.assertNotIn("discord_requester", variables)
        self.assertNotIn("discord_jump_url", variables)
        self.assertNotIn("discord_summary", variables)
        self.assertNotIn("discord_context", variables)
        self.assertEqual(
            base64.b64decode(variables["discord_requester_b64"]).decode("utf-8"),
            "alice `review this`",
        )
        self.assertEqual(
            base64.b64decode(variables["discord_jump_url_b64"]).decode("utf-8"),
            "https://discord.com/channels/1/44",
        )
        self.assertEqual(
            base64.b64decode(variables["discord_summary_b64"]).decode("utf-8"),
            "Crash on startup",
        )
        self.assertEqual(
            base64.b64decode(variables["discord_context_b64"]).decode("utf-8"),
            "unset env X",
        )

    def test_reserve_request_deduplicates_conversation_workflow(self) -> None:
        behavior = service.command_behavior("fix")
        first = {
            "request_id": "dc-1-fix",
            "workflow_key": "dc:guild:1:conversation:2:fix",
            "command": "fix",
            "guild_id": "1",
            "conversation_id": "2",
        }
        second = {
            "request_id": "dc-2-fix",
            "workflow_key": "dc:guild:1:conversation:2:fix",
            "command": "fix",
            "guild_id": "1",
            "conversation_id": "2",
        }

        self.assertIsNone(service.reserve_request(first, behavior, "interaction-1"))
        duplicate = service.reserve_request(second, behavior, "interaction-2")

        self.assertIsNotNone(duplicate)
        assert duplicate is not None
        self.assertEqual(duplicate["request_id"], "dc-1-fix")

    def test_accept_fix_request_saves_and_enqueues_new_request(self) -> None:
        common.import_app_config(common.load_config(), {"application_id": "1", "public_key": "ab" * 32})
        common.set_channel_mapping(common.load_config(), "1", "22", "product/polecat", "mol-discord-fix-issue")
        common.save_bot_token("bot-token")
        payload = {
            "id": "interaction-1",
            "guild_id": "1",
            "channel_id": "22",
            "member": {"user": {"id": "99", "username": "alice"}, "roles": []},
        }

        with mock.patch.object(service, "enqueue_request") as enqueue_request:
            response, receipt = service.accept_fix_request(payload, "Crash on startup", "unset env X", "interaction-1")

        self.assertEqual(response["type"], 4)
        self.assertIn("Accepted /gc fix", response["data"]["content"])
        self.assertEqual(receipt["response_kind"], "accepted")
        request = common.list_recent_requests(limit=1)[0]
        self.assertEqual(request["summary"], "Crash on startup")
        self.assertEqual(request["dispatch_target"], "product/polecat")
        enqueue_request.assert_called_once()

    def test_accept_fix_request_rejects_when_bot_token_missing(self) -> None:
        common.import_app_config(common.load_config(), {"application_id": "1", "public_key": "ab" * 32})
        common.set_channel_mapping(common.load_config(), "1", "22", "product/polecat", "mol-discord-fix-issue")
        payload = {
            "id": "interaction-1",
            "guild_id": "1",
            "channel_id": "22",
            "member": {"user": {"id": "99", "username": "alice"}, "roles": []},
        }

        response, receipt = service.accept_fix_request(payload, "Crash on startup", "unset env X", "interaction-1")

        self.assertEqual(response["type"], 4)
        self.assertIn("not fully configured", response["data"]["content"])
        self.assertEqual(response["data"]["flags"], 64)
        self.assertEqual(receipt["response_kind"], "message")
        self.assertEqual(common.list_recent_requests(limit=20), [])

    def test_create_fix_bead_parses_json_after_cli_noise(self) -> None:
        self.write_rig_route("product")
        request = {
            "summary": "Crash on startup",
            "dispatch_target": "product/polecat",
            "guild_id": "1",
            "channel_id": "22",
            "thread_id": "",
            "conversation_id": "22",
            "jump_url": "https://discord.com/channels/1/22",
            "request_id": "dc-1-fix",
            "invoking_user_display_name": "alice",
            "invoking_user_id": "99",
            "context_markdown": "unset env X",
        }

        with mock.patch.object(
            service,
            "run_subprocess",
            side_effect=[
                mock.Mock(returncode=0, stdout="warning: something\n{\"id\":\"bd-1\"}\n", stderr=""),
                mock.Mock(returncode=0, stdout="", stderr=""),
            ],
        ):
            outcome = service.create_fix_bead(request, "product/polecat")

        self.assertEqual(outcome["bead_id"], "bd-1")

    def test_run_fix_dispatch_returns_bead_init_failure_without_slinging(self) -> None:
        self.write_rig_route("product")
        request = {
            "summary": "Crash on startup",
            "dispatch_target": "product/polecat",
            "dispatch_formula": "mol-discord-fix-issue",
        }

        with mock.patch.object(
            service,
            "create_fix_bead",
            return_value={"status": "dispatch_failed", "reason": "bead_update_failed", "bead_id": "bd-1"},
        ), mock.patch.object(
            service,
            "run_subprocess",
            side_effect=[mock.Mock(returncode=0), mock.Mock(returncode=0), mock.Mock(returncode=0)],
        ) as run_subprocess:
            outcome = service.run_fix_dispatch(request)

        self.assertEqual(outcome["status"], "dispatch_failed")
        self.assertEqual(outcome["bead_id"], "bd-1")
        self.assertTrue(outcome["bead_closed"])
        commands = [call.args[0] for call in run_subprocess.call_args_list]
        self.assertEqual(commands[0], ["bd", "update", "bd-1", "--set-metadata", "close_reason=discord:bead_update_failed"])
        self.assertEqual(commands[1], ["bd", "ready", "bd-1"])
        self.assertEqual(commands[2], ["bd", "close", "bd-1"])
        self.assertNotIn("gc", [command[0] for command in commands])

    def test_process_request_releases_workflow_link_after_dispatch_failure(self) -> None:
        request = {
            "request_id": "dc-3-fix",
            "workflow_key": "dc:guild:1:conversation:3:fix",
            "command": "fix",
            "summary": "Crash on startup",
            "dispatch_target": "product/polecat",
            "dispatch_formula": "mol-discord-fix-issue",
        }
        common.save_request(request)
        common.save_workflow_link(request["workflow_key"], request["request_id"])

        with mock.patch.object(
            service,
            "run_fix_dispatch",
            return_value={"status": "dispatch_failed", "reason": "dispatch_failed", "bead_id": "bd-1"},
        ):
            service.process_request(request["request_id"])

        saved = common.load_request(request["request_id"])
        self.assertEqual(saved["status"], "dispatch_failed")
        self.assertIsNone(common.load_workflow_link(request["workflow_key"]))

    def test_process_request_posts_failure_followup_for_async_dispatch_failure(self) -> None:
        common.save_bot_token("bot-token")
        request = {
            "request_id": "dc-4-fix",
            "workflow_key": "dc:guild:1:conversation:4:fix",
            "command": "fix",
            "summary": "Crash on startup",
            "channel_id": "22",
            "thread_id": "44",
            "dispatch_target": "product/polecat",
            "dispatch_formula": "mol-discord-fix-issue",
        }
        common.save_request(request)

        with mock.patch.object(
            service,
            "run_fix_dispatch",
            return_value={"status": "dispatch_failed", "reason": "dispatch_failed", "bead_id": "bd-1"},
        ), mock.patch.object(
            common,
            "post_channel_message",
            return_value={"id": "msg-1"},
        ) as post_channel_message:
            service.process_request(request["request_id"])

        saved = common.load_request(request["request_id"])
        self.assertEqual(saved["failure_message_id"], "msg-1")
        post_channel_message.assert_called_once()
        self.assertEqual(post_channel_message.call_args.args[0], "44")
        self.assertIn("could not be started", post_channel_message.call_args.args[1])

    def test_process_request_releases_workflow_link_when_request_file_is_missing(self) -> None:
        common.save_workflow_link("dc:guild:1:conversation:44:fix", "dc-missing")

        with mock.patch.object(common, "load_request", return_value=None):
            service.process_request("dc-missing")

        self.assertIsNone(common.load_workflow_link("dc:guild:1:conversation:44:fix"))

    def test_process_request_sanitizes_internal_error_followup(self) -> None:
        common.save_bot_token("bot-token")
        request = {
            "request_id": "dc-5-fix",
            "workflow_key": "dc:guild:1:conversation:5:fix",
            "command": "fix",
            "summary": "Crash on startup",
            "channel_id": "22",
            "dispatch_target": "product/polecat",
            "dispatch_formula": "mol-discord-fix-issue",
        }
        common.save_request(request)

        with mock.patch.object(service, "run_fix_dispatch", side_effect=RuntimeError("leak this path")), mock.patch.object(
            common,
            "post_channel_message",
            return_value={"id": "msg-2"},
        ) as post_channel_message:
            service.process_request(request["request_id"])

        saved = common.load_request(request["request_id"])
        self.assertEqual(saved["reason"], "internal_error")
        self.assertEqual(saved["error_message"], "leak this path")
        self.assertIn("internal error occurred", post_channel_message.call_args.args[1])
        self.assertNotIn("leak this path", post_channel_message.call_args.args[1])

    def test_parse_application_command_extracts_rig_option(self) -> None:
        payload = {
            "data": {
                "name": "gc",
                "options": [
                    {
                        "type": 1,
                        "name": "fix",
                        "options": [
                            {"type": 3, "name": "rig", "value": "mission-control"},
                            {"type": 3, "name": "prompt", "value": "crash on startup"},
                        ],
                    }
                ],
            }
        }

        parsed = service.parse_application_command(payload, "gc")

        self.assertEqual(parsed["command"], "fix")
        self.assertEqual(parsed["rig"], "mission-control")
        self.assertEqual(parsed["prompt"], "crash on startup")

    def test_accept_fix_request_routes_via_rig_mapping(self) -> None:
        common.import_app_config(common.load_config(), {"application_id": "1", "public_key": "ab" * 32})
        common.set_rig_mapping(common.load_config(), "1", "mission-control", "mission-control/polecat", "mol-discord-fix-issue")
        common.save_bot_token("bot-token")
        payload = {
            "id": "interaction-2",
            "guild_id": "1",
            "channel_id": "55",
            "member": {"user": {"id": "99", "username": "alice"}, "roles": []},
        }

        with mock.patch.object(service, "enqueue_request") as enqueue_request:
            response, receipt = service.accept_fix_request(payload, "Crash on startup", "unset env X", "interaction-2", rig_name="mission-control")

        self.assertEqual(response["type"], 4)
        self.assertIn("Accepted /gc fix", response["data"]["content"])
        request = common.list_recent_requests(limit=1)[0]
        self.assertEqual(request["dispatch_target"], "mission-control/polecat")
        self.assertEqual(request["workflow_key"], "dc:guild:1:conversation:55:fix")
        enqueue_request.assert_called_once()

    def test_reserve_request_rejects_distinct_rig_workflows_in_same_conversation(self) -> None:
        behavior = service.command_behavior("fix")
        first = {
            "request_id": "dc-rig-1",
            "workflow_key": "dc:guild:1:conversation:2:fix",
            "command": "fix",
            "guild_id": "1",
            "conversation_id": "2",
        }
        second = {
            "request_id": "dc-rig-2",
            "workflow_key": "dc:guild:1:conversation:2:fix",
            "command": "fix",
            "guild_id": "1",
            "conversation_id": "2",
        }

        self.assertIsNone(service.reserve_request(first, behavior, "interaction-rig-1"))
        duplicate = service.reserve_request(second, behavior, "interaction-rig-2")
        self.assertIsNotNone(duplicate)
        assert duplicate is not None
        self.assertEqual(duplicate["request_id"], "dc-rig-1")

    def test_accept_fix_request_routes_threaded_rig_mapping_with_parent_channel_context(self) -> None:
        config = common.import_app_config(
            common.load_config(),
            {
                "application_id": "1",
                "public_key": "ab" * 32,
                "channel_allowlist": ["22"],
            },
        )
        common.set_rig_mapping(config, "1", "mission-control", "mission-control/polecat", "mol-discord-fix-issue")
        common.save_bot_token("bot-token")
        payload = {
            "id": "interaction-thread",
            "guild_id": "1",
            "channel_id": "55",
            "channel": {"parent_id": "22"},
            "member": {"user": {"id": "99", "username": "alice"}, "roles": []},
        }

        with mock.patch.object(service, "enqueue_request") as enqueue_request:
            response, receipt = service.accept_fix_request(payload, "Crash on startup", "unset env X", "interaction-thread", rig_name="mission-control")

        self.assertEqual(response["type"], 4)
        request = common.list_recent_requests(limit=1)[0]
        self.assertEqual(request["channel_id"], "22")
        self.assertEqual(request["thread_id"], "55")
        enqueue_request.assert_called_once()

    def test_accept_fix_request_rejects_unknown_rig(self) -> None:
        common.import_app_config(common.load_config(), {"application_id": "1", "public_key": "ab" * 32})
        common.save_bot_token("bot-token")
        payload = {
            "id": "interaction-3",
            "guild_id": "1",
            "channel_id": "55",
            "member": {"user": {"id": "99", "username": "alice"}, "roles": []},
        }

        response, receipt = service.accept_fix_request(payload, "Crash", "", "interaction-3", rig_name="nonexistent")

        self.assertIn("no rig mapping", response["data"]["content"])
        self.assertEqual(response["data"]["flags"], 64)

    def test_accept_fix_request_surfaces_thread_lookup_failure(self) -> None:
        common.import_app_config(common.load_config(), {"application_id": "1", "public_key": "ab" * 32})
        common.save_bot_token("bot-token")
        payload = {
            "id": "interaction-lookup-1",
            "guild_id": "1",
            "channel_id": "55",
            "member": {"user": {"id": "99", "username": "alice"}, "roles": []},
        }

        with mock.patch.object(
            common,
            "load_channel_context",
            return_value={"mapping": {}, "lookup_error": "GET failed"},
        ):
            response, receipt = service.accept_fix_request(payload, "Crash", "", "interaction-lookup-1")

        self.assertIn("lookup failed", response["data"]["content"].lower())
        self.assertEqual(receipt["response_kind"], "message")

    def test_accept_fix_request_rejects_rig_mapping_when_thread_lookup_fails(self) -> None:
        common.import_app_config(common.load_config(), {"application_id": "1", "public_key": "ab" * 32})
        common.set_rig_mapping(common.load_config(), "1", "mission-control", "mission-control/polecat", "mol-discord-fix-issue")
        common.save_bot_token("bot-token")
        payload = {
            "id": "interaction-lookup-2",
            "guild_id": "1",
            "channel_id": "55",
            "channel": {"type": 11},
            "member": {"user": {"id": "99", "username": "alice"}, "roles": []},
        }

        with mock.patch.object(
            common,
            "load_channel_context",
            return_value={"mapping": {}, "lookup_error": "GET failed"},
        ):
            response, receipt = service.accept_fix_request(
                payload, "Crash", "", "interaction-lookup-2", rig_name="mission-control"
            )

        self.assertIn("lookup failed", response["data"]["content"].lower())
        self.assertEqual(receipt["response_kind"], "message")

    def test_rig_workdir_rejects_paths_outside_city_root(self) -> None:
        beads_dir = pathlib.Path(self.tempdir.name, ".beads")
        beads_dir.mkdir(parents=True, exist_ok=True)
        pathlib.Path(beads_dir, "routes.jsonl").write_text('{"path":"../../tmp"}\n', encoding="utf-8")

        self.assertEqual(service.rig_workdir("../../tmp"), "")

    def test_rig_workdir_rejects_symlink_target_outside_city_root(self) -> None:
        beads_dir = pathlib.Path(self.tempdir.name, ".beads")
        beads_dir.mkdir(parents=True, exist_ok=True)
        outside_dir = pathlib.Path(self.tempdir.name).parent / "discord-outside-rig"
        outside_dir.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: outside_dir.exists() and outside_dir.rmdir())
        rig_link = pathlib.Path(self.tempdir.name, "product")
        rig_link.symlink_to(outside_dir, target_is_directory=True)
        pathlib.Path(beads_dir, "routes.jsonl").write_text('{"path":"product"}\n', encoding="utf-8")

        self.assertEqual(service.rig_workdir("product"), "")

    def test_create_fix_bead_fails_closed_when_rig_workdir_is_missing(self) -> None:
        request = {
            "request_id": "dc-route-missing",
            "summary": "Crash on startup",
        }

        with mock.patch.object(service, "run_subprocess") as run_subprocess:
            outcome = service.create_fix_bead(request, "mission-control/polecat")

        self.assertEqual(outcome["status"], "dispatch_failed")
        self.assertEqual(outcome["reason"], "rig_workdir_missing")
        run_subprocess.assert_not_called()

    def test_extract_json_output_ignores_warning_braces_before_payload(self) -> None:
        payload = service.extract_json_output('warning: field {name} missing\n{"id":"bd-1"}\n')

        self.assertEqual(payload["id"], "bd-1")

    def test_recover_incomplete_requests_marks_request_failed_and_releases_workflow(self) -> None:
        request = {
            "request_id": "dc-recover-1",
            "workflow_key": "dc:guild:1:conversation:9:fix",
            "status": "received",
            "command": "fix",
            "dispatch_target": "mission-control/polecat",
            "channel_id": "9",
            "conversation_id": "9",
        }
        common.save_request(request)
        common.save_workflow_link(request["workflow_key"], request["request_id"])

        with mock.patch.object(service, "maybe_notify_dispatch_failure", side_effect=lambda payload: payload) as notify_failure:
            recovered = service.recover_incomplete_requests()

        self.assertEqual(recovered, 1)
        saved = common.load_request(request["request_id"])
        assert saved is not None
        self.assertEqual(saved["status"], "internal_error")
        self.assertEqual(saved["reason"], "service_restarted_before_dispatch")
        self.assertIsNone(common.load_workflow_link(request["workflow_key"]))
        notify_failure.assert_called_once()

    def test_recover_incomplete_requests_closes_persisted_bead(self) -> None:
        request = {
            "request_id": "dc-recover-2",
            "workflow_key": "dc:guild:1:conversation:10:fix",
            "status": "bead_created",
            "command": "fix",
            "bead_id": "bd-10",
            "dispatch_target": "mission-control/polecat",
        }
        common.save_request(request)
        common.save_workflow_link(request["workflow_key"], request["request_id"])

        with mock.patch.object(service, "close_failed_bead", return_value=True) as close_failed_bead, mock.patch.object(
            service,
            "maybe_notify_dispatch_failure",
            side_effect=lambda payload: payload,
        ):
            recovered = service.recover_incomplete_requests()

        self.assertEqual(recovered, 1)
        close_failed_bead.assert_called_once_with("bd-10", "service_restarted_before_dispatch", "mission-control")
        saved = common.load_request(request["request_id"])
        assert saved is not None
        self.assertTrue(saved["bead_closed"])

    def test_recover_incomplete_requests_preserves_workflow_lock_when_cleanup_fails(self) -> None:
        request = {
            "request_id": "dc-recover-3",
            "workflow_key": "dc:guild:1:conversation:11:fix",
            "status": "bead_created",
            "command": "fix",
            "bead_id": "bd-11",
            "dispatch_target": "mission-control/polecat",
        }
        common.save_request(request)
        common.save_workflow_link(request["workflow_key"], request["request_id"])

        with mock.patch.object(service, "close_failed_bead", return_value=False), mock.patch.object(
            service,
            "maybe_notify_dispatch_failure",
            side_effect=lambda payload: payload,
        ):
            recovered = service.recover_incomplete_requests()

        self.assertEqual(recovered, 1)
        self.assertIsNotNone(common.load_workflow_link(request["workflow_key"]))

    def test_recover_incomplete_requests_skips_recent_dispatching_request(self) -> None:
        request = {
            "request_id": "dc-recover-4",
            "workflow_key": "dc:guild:1:conversation:12:fix",
            "status": "dispatching",
            "dispatch_started_at": common.utcnow(),
            "command": "fix",
            "bead_id": "bd-12",
            "dispatch_target": "mission-control/polecat",
        }
        common.save_request(request)
        common.save_workflow_link(request["workflow_key"], request["request_id"])

        with mock.patch.object(service, "maybe_notify_dispatch_failure", side_effect=lambda payload: payload) as notify_failure:
            recovered = service.recover_incomplete_requests()

        self.assertEqual(recovered, 0)
        self.assertIsNotNone(common.load_workflow_link(request["workflow_key"]))
        notify_failure.assert_not_called()

    def test_recover_incomplete_requests_reclaims_stale_dispatching_request(self) -> None:
        request = {
            "request_id": "dc-recover-5",
            "workflow_key": "dc:guild:1:conversation:13:fix",
            "status": "dispatching",
            "dispatch_started_at": "2000-01-01T00:00:00Z",
            "command": "fix",
            "bead_id": "bd-13",
            "dispatch_target": "mission-control/polecat",
        }
        common.save_request(request)
        common.save_workflow_link(request["workflow_key"], request["request_id"])

        with mock.patch.object(service, "dispatch_recovery_state", return_value="inactive"), mock.patch.object(
            service, "close_failed_bead", return_value=True
        ) as close_failed_bead, mock.patch.object(
            service,
            "maybe_notify_dispatch_failure",
            side_effect=lambda payload: payload,
        ):
            recovered = service.recover_incomplete_requests()

        self.assertEqual(recovered, 1)
        close_failed_bead.assert_called_once_with("bd-13", "service_restarted_during_dispatch", "mission-control")
        saved = common.load_request(request["request_id"])
        assert saved is not None
        self.assertEqual(saved["reason"], "service_restarted_during_dispatch")

    def test_recover_incomplete_requests_marks_dispatched_when_bead_is_already_routed(self) -> None:
        request = {
            "request_id": "dc-recover-6",
            "workflow_key": "dc:guild:1:conversation:14:fix",
            "status": "dispatching",
            "dispatch_started_at": "2000-01-01T00:00:00Z",
            "command": "fix",
            "bead_id": "bd-14",
            "dispatch_target": "mission-control/polecat",
        }
        common.save_request(request)
        common.save_workflow_link(request["workflow_key"], request["request_id"])

        with mock.patch.object(service, "dispatch_recovery_state", return_value="active"), mock.patch.object(
            service, "close_failed_bead"
        ) as close_failed_bead, mock.patch.object(
            service,
            "maybe_notify_dispatch_failure",
            side_effect=lambda payload: payload,
        ) as notify_failure:
            recovered = service.recover_incomplete_requests()

        self.assertEqual(recovered, 0)
        close_failed_bead.assert_not_called()
        notify_failure.assert_not_called()
        saved = common.load_request(request["request_id"])
        assert saved is not None
        self.assertEqual(saved["status"], "dispatched")
        self.assertEqual(saved["dispatch_recovery_reason"], "bead_already_routed")
        self.assertIsNotNone(common.load_workflow_link(request["workflow_key"]))

    def test_recover_incomplete_requests_defers_when_bead_state_is_unavailable(self) -> None:
        request = {
            "request_id": "dc-recover-7",
            "workflow_key": "dc:guild:1:conversation:15:fix",
            "status": "dispatching",
            "dispatch_started_at": "2000-01-01T00:00:00Z",
            "command": "fix",
            "bead_id": "bd-15",
            "dispatch_target": "mission-control/polecat",
        }
        common.save_request(request)
        common.save_workflow_link(request["workflow_key"], request["request_id"])

        with mock.patch.object(service, "dispatch_recovery_state", return_value="unknown"), mock.patch.object(
            service, "close_failed_bead"
        ) as close_failed_bead, mock.patch.object(
            service,
            "maybe_notify_dispatch_failure",
            side_effect=lambda payload: payload,
        ) as notify_failure:
            recovered = service.recover_incomplete_requests()

        self.assertEqual(recovered, 0)
        close_failed_bead.assert_not_called()
        notify_failure.assert_not_called()
        saved = common.load_request(request["request_id"])
        assert saved is not None
        self.assertEqual(saved["status"], "dispatching")
        self.assertEqual(saved["dispatch_recovery_reason"], "bead_state_unavailable")
        self.assertIsNotNone(common.load_workflow_link(request["workflow_key"]))

    def test_dispatch_recovery_state_treats_assigned_bead_as_active(self) -> None:
        self.write_rig_route("mission-control")
        request = {
            "bead_id": "bd-21",
            "dispatch_target": "mission-control/polecat",
        }

        with mock.patch.object(
            service,
            "run_subprocess",
            return_value=mock.Mock(
                returncode=0,
                stdout='{"id":"bd-21","status":"open","assignee":"mission-control/polecat","metadata":{"molecule_id":"gc-2"}}\n',
                stderr="",
            ),
        ):
            state = service.dispatch_recovery_state(request)

        self.assertEqual(state, "active")

    def test_dispatch_recovery_state_treats_open_unassigned_bead_as_inactive(self) -> None:
        self.write_rig_route("mission-control")
        request = {
            "bead_id": "bd-22",
            "dispatch_target": "mission-control/polecat",
        }

        with mock.patch.object(
            service,
            "run_subprocess",
            return_value=mock.Mock(
                returncode=0,
                stdout='{"id":"bd-22","status":"open","assignee":"","metadata":{"molecule_id":"gc-3"}}\n',
                stderr="",
            ),
        ):
            state = service.dispatch_recovery_state(request)

        self.assertEqual(state, "inactive")

    def test_dispatch_recovery_state_treats_closed_failed_bead_as_inactive(self) -> None:
        self.write_rig_route("mission-control")
        request = {
            "bead_id": "bd-23",
            "dispatch_target": "mission-control/polecat",
        }

        with mock.patch.object(
            service,
            "run_subprocess",
            return_value=mock.Mock(
                returncode=0,
                stdout='{"id":"bd-23","status":"closed","metadata":{"close_reason":"discord:dispatch_failed"}}\n',
                stderr="",
            ),
        ):
            state = service.dispatch_recovery_state(request)

        self.assertEqual(state, "inactive")

    def test_dispatch_recovery_state_treats_closed_bead_without_failure_reason_as_active(self) -> None:
        self.write_rig_route("mission-control")
        request = {
            "bead_id": "bd-24",
            "dispatch_target": "mission-control/polecat",
        }

        with mock.patch.object(
            service,
            "run_subprocess",
            return_value=mock.Mock(
                returncode=0,
                stdout='{"id":"bd-24","status":"closed","metadata":{}}\n',
                stderr="",
            ),
        ):
            state = service.dispatch_recovery_state(request)

        self.assertEqual(state, "active")

    def test_maybe_prune_request_state_rate_limits_repeated_calls(self) -> None:
        with mock.patch.object(common, "prune_requests") as prune_requests, mock.patch.object(
            common, "prune_receipts"
        ) as prune_receipts, mock.patch.object(common, "prune_pending_modals") as prune_pending_modals, mock.patch(
            "discord_intake_service.time.monotonic",
            side_effect=[100.0, 100.5, 161.0],
        ):
            self.assertTrue(service.maybe_prune_request_state())
            self.assertFalse(service.maybe_prune_request_state())
            self.assertTrue(service.maybe_prune_request_state())

        self.assertEqual(prune_requests.call_count, 2)
        self.assertEqual(prune_receipts.call_count, 2)
        self.assertEqual(prune_pending_modals.call_count, 2)

    def test_should_run_request_recovery_only_for_interactions_service(self) -> None:
        with mock.patch.object(common, "current_service_name", return_value=common.ADMIN_SERVICE_NAME):
            self.assertFalse(service.should_run_request_recovery())
        with mock.patch.object(common, "current_service_name", return_value=common.INTERACTIONS_SERVICE_NAME):
            self.assertTrue(service.should_run_request_recovery())

    def test_finalize_modal_origin_receipt_replaces_stale_modal_replay(self) -> None:
        common.save_interaction_receipt("slash-1", {"response_kind": "modal", "modal_nonce": "nonce-1"})
        response = service.build_message_response("Accepted /gc fix for this conversation.", ephemeral=False)
        receipt = {"response_kind": "accepted", "request_id": "dc-1", "response": response}

        service.finalize_modal_origin_receipt("slash-1", response, receipt)

        receipt = common.load_interaction_receipt("slash-1")
        self.assertEqual(receipt["response_kind"], "accepted")
        replayed = service.replay_response_from_receipt(receipt)
        self.assertEqual(replayed, response)

    def test_persist_interaction_receipt_makes_modal_submit_replay_safe(self) -> None:
        response = service.build_message_response("Accepted /gc fix for this conversation.", ephemeral=False)
        receipt = {"response_kind": "accepted", "request_id": "dc-2", "response": response}

        service.persist_interaction_receipt("modal-submit-1", receipt)

        saved = common.load_interaction_receipt("modal-submit-1")
        self.assertEqual(saved["response_kind"], "accepted")
        self.assertEqual(service.replay_response_from_receipt(saved), response)

    def test_persist_interaction_receipt_saves_prompt_path_response(self) -> None:
        response = service.build_message_response("guild only", ephemeral=True)
        receipt = service.receipt_payload(response, response_kind="message")

        service.persist_interaction_receipt("interaction-1", receipt)

        saved = common.load_interaction_receipt("interaction-1")
        self.assertEqual(saved["response_kind"], "message")
        self.assertEqual(service.replay_response_from_receipt(saved), response)


if __name__ == "__main__":
    unittest.main()
