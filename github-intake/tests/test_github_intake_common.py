from __future__ import annotations

import hashlib
import hmac
import json
import os
import pathlib
import tempfile
import unittest

import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "scripts"))

import github_intake_common as common


class GitHubIntakeCommonTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self._old_environ = os.environ.copy()
        os.environ["GC_CITY_ROOT"] = self.tempdir.name
        os.environ["GC_SERVICE_STATE_ROOT"] = os.path.join(self.tempdir.name, ".gc", "services", "github-intake")
        os.environ["GC_PUBLISHED_SERVICES_DIR"] = os.path.join(self.tempdir.name, ".gc", "services", ".published")
        os.makedirs(os.environ["GC_PUBLISHED_SERVICES_DIR"], exist_ok=True)

    def tearDown(self) -> None:
        os.environ.clear()
        os.environ.update(self._old_environ)

    def _write_snapshot(self, name: str, url: str) -> None:
        path = pathlib.Path(os.environ["GC_PUBLISHED_SERVICES_DIR"]) / f"{name}.json"
        path.write_text(
            json.dumps(
                {
                    "service_name": name,
                    "published": bool(url),
                    "visibility": "public",
                    "current_url": url,
                    "url_version": 1,
                }
            ),
            encoding="utf-8",
        )

    def test_build_manifest_uses_published_service_urls(self) -> None:
        self._write_snapshot(common.ADMIN_SERVICE_NAME, "https://admin.example.com")
        self._write_snapshot(common.WEBHOOK_SERVICE_NAME, "https://hook.example.com")

        manifest = common.build_manifest()

        self.assertEqual(manifest["url"], "https://admin.example.com")
        self.assertEqual(
            manifest["hook_attributes"]["url"],
            "https://hook.example.com/v0/github/webhook",
        )
        self.assertEqual(
            manifest["redirect_url"],
            "https://admin.example.com/v0/github/app/manifest/callback",
        )
        self.assertIn("issue_comment", manifest["default_events"])
        self.assertEqual(manifest["default_permissions"]["contents"], "write")
        self.assertEqual(manifest["default_permissions"]["pull_requests"], "write")

    def test_parse_gc_command_extracts_multiline_context(self) -> None:
        parsed = common.parse_gc_command("please take a look\n/gc fix crash on startup\nstack trace line 1\nstack trace line 2")

        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed["command"], "fix")
        self.assertEqual(parsed["inline_context"], "crash on startup")
        self.assertEqual(parsed["context"], "crash on startup\nstack trace line 1\nstack trace line 2")
        self.assertEqual(parsed["command_line"], "/gc fix crash on startup")

    def test_verify_github_signature(self) -> None:
        payload = b'{"ok":true}'
        secret = "top-secret"
        digest = hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()

        self.assertTrue(common.verify_github_signature(secret, payload, f"sha256={digest}"))
        self.assertFalse(common.verify_github_signature(secret, payload, "sha256=deadbeef"))

    def test_extract_issue_comment_request_accepts_issue_comment_and_rejects_pr_comment(self) -> None:
        payload = {
            "action": "created",
            "installation": {"id": 77},
            "issue": {
                "id": 4242,
                "number": 42,
                "title": "Crash on startup",
                "body": "The app crashes when env var X is missing.",
                "html_url": "https://github.com/owner/repo/issues/42",
                "user": {"login": "reporter"},
            },
            "comment": {
                "id": 99,
                "body": "/gc fix missing env guard\nrepro: unset X\nrun the app",
                "html_url": "https://github.com/owner/repo/issues/42#issuecomment-99",
                "user": {"login": "alice"},
                "author_association": "MEMBER",
            },
            "repository": {
                "id": 123,
                "name": "repo",
                "full_name": "Owner/Repo",
                "default_branch": "main",
                "owner": {"login": "Owner"},
            },
        }

        request = common.extract_issue_comment_request(payload)

        self.assertIsNotNone(request)
        assert request is not None
        self.assertEqual(request["request_id"], "gh-123-99-fix")
        self.assertEqual(request["workflow_key"], "gh:123:issue:42:fix")
        self.assertEqual(request["repository_full_name"], "owner/repo")
        self.assertEqual(request["installation_id"], "77")
        self.assertEqual(request["comment_author"], "alice")
        self.assertEqual(request["command"], "fix")
        self.assertEqual(request["command_context"], "missing env guard\nrepro: unset X\nrun the app")
        self.assertEqual(request["issue_url"], "https://github.com/owner/repo/issues/42")
        payload["issue"]["pull_request"] = {"url": "https://api.github.com/repos/o/r/pulls/42"}
        self.assertIsNone(common.extract_issue_comment_request(payload))

    def test_set_repo_mapping_persists_commands(self) -> None:
        config = common.set_repo_mapping(
            common.load_config(),
            "Owner/Repo",
            "product/polecat",
            "mol-fix",
        )

        mapping = common.resolve_repo_mapping(config, "owner/repo")
        self.assertIsNotNone(mapping)
        self.assertEqual(mapping["target"], "product/polecat")
        self.assertEqual(mapping["commands"]["fix"]["formula"], "mol-fix")

    def test_safe_storage_id_sanitizes_delivery_header_values(self) -> None:
        self.assertEqual(common.safe_storage_id("abc-123", "delivery"), "abc-123")
        self.assertTrue(common.safe_storage_id("gh:123:issue:42:fix", "delivery").startswith("delivery-"))
        sanitized = common.safe_storage_id("../../etc/passwd", "delivery")
        self.assertTrue(sanitized.startswith("delivery-"))
        self.assertNotIn("/", sanitized)

    def test_workflow_storage_id_preserves_expected_issue_key_shape(self) -> None:
        self.assertEqual(common.workflow_storage_id("gh:123:issue:42:fix"), "gh:123:issue:42:fix")

    def test_workflow_link_round_trip(self) -> None:
        saved = common.save_workflow_link("gh:123:issue:42:fix", "gh-123-99-fix")

        loaded = common.load_workflow_link("gh:123:issue:42:fix")

        self.assertEqual(saved["request_id"], "gh-123-99-fix")
        self.assertIsNotNone(loaded)
        assert loaded is not None
        self.assertEqual(loaded["request_id"], "gh-123-99-fix")
        common.remove_workflow_link("gh:123:issue:42:fix")
        self.assertIsNone(common.load_workflow_link("gh:123:issue:42:fix"))

    def test_remove_workflow_link_if_request_matches_current_owner(self) -> None:
        common.save_workflow_link("gh:123:issue:42:fix", "gh-123-99-fix")

        removed = common.remove_workflow_link_if_request("gh:123:issue:42:fix", "gh-123-99-fix")

        self.assertTrue(removed)
        self.assertIsNone(common.load_workflow_link("gh:123:issue:42:fix"))

    def test_remove_workflow_link_if_request_leaves_newer_owner_in_place(self) -> None:
        common.save_workflow_link("gh:123:issue:42:fix", "gh-123-100-fix")

        removed = common.remove_workflow_link_if_request("gh:123:issue:42:fix", "gh-123-99-fix")

        self.assertFalse(removed)
        loaded = common.load_workflow_link("gh:123:issue:42:fix")
        self.assertIsNotNone(loaded)
        assert loaded is not None
        self.assertEqual(loaded["request_id"], "gh-123-100-fix")

    def test_find_request_returns_latest_matching_issue_command(self) -> None:
        first = {
            "request_id": "gh-123-99-fix",
            "repository_full_name": "owner/repo",
            "issue_number": "42",
            "command": "fix",
            "workflow_key": "gh:123:issue:42:fix",
            "created_at": "2026-03-15T22:00:00Z",
        }
        second = {
            "request_id": "gh-123-100-fix",
            "repository_full_name": "owner/repo",
            "issue_number": "42",
            "command": "fix",
            "workflow_key": "gh:123:issue:42:fix",
            "created_at": "2026-03-15T22:05:00Z",
        }

        common.save_request(first)
        common.save_request(second)

        found = common.find_request("Owner/Repo", "42", "fix")

        self.assertIsNotNone(found)
        assert found is not None
        self.assertEqual(found["request_id"], "gh-123-100-fix")

    def test_app_identifier_requires_app_id(self) -> None:
        self.assertEqual(common.app_identifier({"app_id": "123456"}), "123456")
        with self.assertRaises(common.GitHubAPIError):
            common.app_identifier({"client_id": "Iv1.only-client-id"})


if __name__ == "__main__":
    unittest.main()
