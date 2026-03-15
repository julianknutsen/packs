from __future__ import annotations

import pathlib
import tempfile
import unittest

import os
import sys
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "scripts"))

import github_intake_service as service


class GitHubIntakeServiceTests(unittest.TestCase):
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

        self.assertEqual(behavior["workflow_scope"], "issue")

    def test_unknown_command_behavior_is_empty(self) -> None:
        self.assertEqual(service.command_behavior("review"), {})

    def test_rig_from_target_extracts_rig_name(self) -> None:
        self.assertEqual(service.rig_from_target("product/polecat"), "product")
        self.assertEqual(service.rig_from_target("product/polecat-2"), "product")
        self.assertEqual(service.rig_from_target("polecat"), "")

    def test_extract_json_output_accepts_dict_and_list_shapes(self) -> None:
        self.assertEqual(service.extract_json_output('{"id":"bd-1"}')["id"], "bd-1")
        self.assertEqual(service.extract_json_output('[{"id":"bd-2"}]')["id"], "bd-2")
        self.assertEqual(service.extract_json_output("not json"), {})

    def test_build_fix_bead_notes_includes_issue_and_context(self) -> None:
        request = {
            "repository_full_name": "owner/repo",
            "issue_number": "42",
            "issue_url": "https://github.com/owner/repo/issues/42",
            "comment_url": "https://github.com/owner/repo/issues/42#issuecomment-99",
            "request_id": "gh-123-99-fix",
            "comment_author": "alice",
            "comment_body": "I think this is in foo.py\n/gc fix missing env guard\nrepro: unset X",
            "issue_title": "Crash on startup",
            "issue_body": "The app crashes if X is unset.",
            "command_context": "missing env guard\nsteps to reproduce",
        }

        notes = service.build_fix_bead_notes(request)

        self.assertIn("## GitHub Source", notes)
        self.assertIn("Crash on startup", notes)
        self.assertIn("I think this is in foo.py", notes)
        self.assertIn("missing env guard", notes)
        self.assertIn("gh-123-99-fix", notes)

    def test_reserve_request_deduplicates_issue_workflow(self) -> None:
        behavior = service.command_behavior("fix")
        first = {
            "request_id": "gh-123-99-fix",
            "workflow_key": "gh:123:issue:42:fix",
            "command": "fix",
            "issue_number": "42",
            "repository_full_name": "owner/repo",
        }
        second = {
            "request_id": "gh-123-100-fix",
            "workflow_key": "gh:123:issue:42:fix",
            "command": "fix",
            "issue_number": "42",
            "repository_full_name": "owner/repo",
        }

        self.assertIsNone(service.reserve_request(first, behavior))
        duplicate = service.reserve_request(second, behavior)

        self.assertIsNotNone(duplicate)
        assert duplicate is not None
        self.assertEqual(duplicate["request_id"], "gh-123-99-fix")

    def test_run_fix_issue_dispatch_returns_bead_init_failure_without_slinging(self) -> None:
        request = {
            "installation_id": "88",
            "repository_owner": "owner",
            "repository_name": "repo",
            "comment_author": "alice",
        }
        mapping = {"target": "product/polecat"}
        command_cfg = {"formula": "mol-github-fix-issue"}
        app_cfg = {"app_id": "1"}

        with mock.patch.object(service.common, "repository_permission", return_value="write"), mock.patch.object(
            service,
            "create_fix_bead",
            return_value={"status": "dispatch_failed", "reason": "bead_update_failed", "bead_id": "bd-1"},
        ), mock.patch.object(service, "run_subprocess") as run_subprocess:
            outcome = service.run_fix_issue_dispatch(request, mapping, command_cfg, app_cfg)

        self.assertEqual(outcome["status"], "dispatch_failed")
        self.assertEqual(outcome["reason"], "bead_update_failed")
        self.assertEqual(outcome["bead_id"], "bd-1")
        commands = [call.args[0] for call in run_subprocess.call_args_list]
        self.assertEqual(commands[0], ["bd", "close", "bd-1", "--reason", "github-intake:bead_update_failed"])
        self.assertNotIn("gc", [command[0] for command in commands])

    def test_close_failed_bead_updates_and_closes(self) -> None:
        result = mock.Mock(returncode=0)
        with mock.patch.object(service, "run_subprocess", return_value=result) as run_subprocess:
            closed = service.close_failed_bead("bd-1", "dispatch_failed")

        self.assertTrue(closed)
        commands = [call.args[0] for call in run_subprocess.call_args_list]
        self.assertEqual(commands[0], ["bd", "close", "bd-1", "--reason", "github-intake:dispatch_failed"])

    def test_process_request_releases_workflow_link_after_dispatch_failure_with_bead(self) -> None:
        request = {
            "request_id": "gh-123-99-fix",
            "workflow_key": "gh:123:issue:42:fix",
            "command": "fix",
            "repository_full_name": "owner/repo",
            "repository_id": "123",
            "issue_number": "42",
            "installation_id": "88",
            "repository_owner": "owner",
            "repository_name": "repo",
            "comment_author": "alice",
        }
        mapping = {
            "target": "product/polecat",
            "commands": {"fix": {"formula": "mol-github-fix-issue"}},
        }
        service.common.save_request(request)
        service.common.save_workflow_link(request["workflow_key"], request["request_id"])

        with mock.patch.object(service.common, "load_config", return_value={"app": {"app_id": "1"}}), mock.patch.object(
            service.common,
            "resolve_repo_mapping",
            return_value=mapping,
        ), mock.patch.object(
            service,
            "run_fix_issue_dispatch",
            return_value={"status": "dispatch_failed", "reason": "dispatch_failed", "bead_id": "bd-1"},
        ):
            service.process_request(request["request_id"])

        saved = service.common.load_request(request["request_id"])
        self.assertIsNotNone(saved)
        assert saved is not None
        self.assertEqual(saved["status"], "dispatch_failed")
        self.assertIsNone(service.common.load_workflow_link(request["workflow_key"]))

    def test_process_request_keeps_workflow_link_when_cleanup_fails(self) -> None:
        request = {
            "request_id": "gh-123-101-fix",
            "workflow_key": "gh:123:issue:43:fix",
            "command": "fix",
            "repository_full_name": "owner/repo",
            "repository_id": "123",
            "issue_number": "43",
        }
        mapping = {
            "target": "product/polecat",
            "commands": {"fix": {"formula": "mol-github-fix-issue"}},
        }
        service.common.save_request(request)
        service.common.save_workflow_link(request["workflow_key"], request["request_id"])

        with mock.patch.object(service.common, "load_config", return_value={"app": {"app_id": "1"}}), mock.patch.object(
            service.common,
            "resolve_repo_mapping",
            return_value=mapping,
        ), mock.patch.object(
            service,
            "run_fix_issue_dispatch",
            return_value={
                "status": "dispatch_failed",
                "reason": "dispatch_failed",
                "bead_id": "bd-2",
                "cleanup_failed": True,
            },
        ):
            service.process_request(request["request_id"])

        self.assertIsNotNone(service.common.load_workflow_link(request["workflow_key"]))

    def test_process_request_does_not_remove_newer_workflow_owner(self) -> None:
        request = {
            "request_id": "gh-123-99-fix",
            "workflow_key": "gh:123:issue:44:fix",
            "command": "fix",
            "repository_full_name": "owner/repo",
            "repository_id": "123",
            "issue_number": "44",
        }
        newer = {
            "request_id": "gh-123-100-fix",
            "workflow_key": "gh:123:issue:44:fix",
            "command": "fix",
            "repository_full_name": "owner/repo",
            "issue_number": "44",
        }
        mapping = {
            "target": "product/polecat",
            "commands": {"fix": {"formula": "mol-github-fix-issue"}},
        }
        service.common.save_request(request)
        service.common.save_request(newer)
        service.common.save_workflow_link(request["workflow_key"], newer["request_id"])

        with mock.patch.object(service.common, "load_config", return_value={"app": {"app_id": "1"}}), mock.patch.object(
            service.common,
            "resolve_repo_mapping",
            return_value=mapping,
        ), mock.patch.object(
            service,
            "run_fix_issue_dispatch",
            return_value={"status": "dispatch_failed", "reason": "dispatch_failed"},
        ):
            service.process_request(request["request_id"])

        loaded = service.common.load_workflow_link(request["workflow_key"])
        self.assertIsNotNone(loaded)
        assert loaded is not None
        self.assertEqual(loaded["request_id"], newer["request_id"])


if __name__ == "__main__":
    unittest.main()
