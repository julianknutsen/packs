from __future__ import annotations

import io
import pathlib
import sys
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "scripts"))

import github_intake_create_pr as create_pr
import github_intake_push_branch as push_branch


class GitHubDeliveryCommandTests(unittest.TestCase):
    def test_push_branch_resolves_separate_delivery_identity(self) -> None:
        app = {"app_id": "delivery-app", "private_key_pem": "pem", "installation_id": "77"}
        argv = [
            "github_intake_push_branch.py",
            "owner/repo",
            "--github-app-identity",
            "delivery",
            "--branch",
            "fix-42",
        ]

        with mock.patch.object(sys, "argv", argv), mock.patch.object(
            push_branch.common,
            "github_app_config_for_identity",
            return_value=app,
        ) as resolve_app, mock.patch.object(
            push_branch.common,
            "git_push_branch",
            return_value={"status": "pushed"},
        ) as git_push_branch, mock.patch("sys.stdout", new_callable=io.StringIO):
            self.assertEqual(push_branch.main(), 0)

        resolve_app.assert_called_once_with("delivery")
        git_push_branch.assert_called_once_with(app, "77", "owner/repo", "fix-42", ref="HEAD")

    def test_create_pr_resolves_separate_delivery_identity(self) -> None:
        app = {"app_id": "delivery-app", "private_key_pem": "pem", "installation_id": "88"}
        argv = [
            "github_intake_create_pr.py",
            "owner/repo",
            "--github-app-identity",
            "delivery",
            "--base",
            "main",
            "--head",
            "fix-42",
            "--title",
            "fix: widget",
        ]

        with mock.patch.object(sys, "argv", argv), mock.patch.object(
            create_pr.common,
            "github_app_config_for_identity",
            return_value=app,
        ) as resolve_app, mock.patch.object(
            create_pr.common,
            "create_pull_request",
            return_value={"number": 42},
        ) as create_pull_request, mock.patch("sys.stdout", new_callable=io.StringIO):
            self.assertEqual(create_pr.main(), 0)

        resolve_app.assert_called_once_with("delivery")
        create_pull_request.assert_called_once_with(
            app,
            "88",
            "owner",
            "repo",
            "fix: widget",
            "fix-42",
            "main",
            "",
        )


if __name__ == "__main__":
    unittest.main()
