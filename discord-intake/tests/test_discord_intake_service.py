from __future__ import annotations

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

    def tearDown(self) -> None:
        os.environ.clear()
        os.environ.update(self._old_environ)

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
            response = service.accept_fix_request(payload, "Crash on startup", "unset env X", "interaction-1")

        self.assertEqual(response["type"], 4)
        self.assertIn("Accepted /gc fix", response["data"]["content"])
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

        response = service.accept_fix_request(payload, "Crash on startup", "unset env X", "interaction-1")

        self.assertEqual(response["type"], 4)
        self.assertIn("not fully configured", response["data"]["content"])
        self.assertEqual(response["data"]["flags"], 64)
        self.assertEqual(common.list_recent_requests(limit=20), [])

    def test_create_fix_bead_parses_json_after_cli_noise(self) -> None:
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
            side_effect=[mock.Mock(returncode=0), mock.Mock(returncode=0)],
        ) as run_subprocess:
            outcome = service.run_fix_dispatch(request)

        self.assertEqual(outcome["status"], "dispatch_failed")
        self.assertEqual(outcome["bead_id"], "bd-1")
        self.assertTrue(outcome["bead_closed"])
        commands = [call.args[0] for call in run_subprocess.call_args_list]
        self.assertEqual(commands[0], ["bd", "update", "bd-1", "--set-metadata", "close_reason=discord-intake:bead_update_failed"])
        self.assertEqual(commands[1], ["bd", "close", "bd-1"])
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


if __name__ == "__main__":
    unittest.main()
