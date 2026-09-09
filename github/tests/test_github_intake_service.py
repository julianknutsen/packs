from __future__ import annotations

import io
import json
import urllib.parse
import pathlib
import tempfile
import unittest

import os
import sys
import subprocess
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "scripts"))

import github_intake_service as service

# GC_BIN: the assertions below expect the literal `gc`, which is what
# `os.environ.get("GC_BIN", "gc")` falls back to in the service. A Gas City seat
# exports GC_BIN, so 14 of these tests fail for anyone running the suite from
# inside a city, and pass in CI only because CI has not installed gc yet at the
# step that runs them. Each setUp pins the fallback rather than depending on the
# variable's absence.


class DummyWebhookHandler:
    def __init__(self, body: bytes, headers: dict[str, str]) -> None:
        self.headers = headers
        self.rfile = io.BytesIO(body)
        self.wfile = io.BytesIO()
        self.status: int | None = None
        self.response_headers: list[tuple[str, str]] = []
        self._headers_buffer: list[bytes] = []

    def send_response(self, status: int) -> None:
        self.status = status

    def send_header(self, key: str, value: str) -> None:
        self.response_headers.append((key, value))

    def end_headers(self) -> None:
        pass


class GitHubIntakeServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self._old_environ = os.environ.copy()
        os.environ["GC_CITY_ROOT"] = self.tempdir.name
        os.environ.pop("GC_BIN", None)  # see GC_BIN note at the top of this file

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

    def test_github_event_env_includes_pr_context_and_payload_file(self) -> None:
        payload = {
            "action": "labeled",
            "installation": {"id": 88},
            "label": {"name": "status/needs-review"},
            "repository": {
                "id": 123,
                "name": "repo",
                "full_name": "Owner/Repo",
                "owner": {"login": "Owner"},
            },
            "pull_request": {
                "number": 42,
                "html_url": "https://github.com/owner/repo/pull/42",
                "state": "open",
            },
            "sender": {"login": "alice"},
        }

        env = service.github_event_env("pull_request", "delivery-1", payload, "/tmp/payload.json")

        self.assertEqual(env["GC_GITHUB_EVENT"], "pull_request")
        self.assertEqual(env["GC_GITHUB_REPO"], "owner/repo")
        self.assertEqual(env["GC_GITHUB_PR_NUMBER"], "42")
        self.assertEqual(env["GC_GITHUB_PR_URL"], "https://github.com/owner/repo/pull/42")
        self.assertEqual(env["GC_GITHUB_LABEL_NAME"], "status/needs-review")
        self.assertEqual(env["GC_GITHUB_ITEM_KIND"], "pr")
        self.assertEqual(env["GC_GITHUB_ITEM_NUMBER"], "42")
        self.assertEqual(env["GC_GITHUB_EVENT_PAYLOAD_FILE"], "/tmp/payload.json")

    def test_github_event_env_includes_issue_context(self) -> None:
        payload = {
            "action": "labeled",
            "installation": {"id": 88},
            "label": {"name": "status/needs-triage"},
            "repository": {
                "id": 123,
                "name": "repo",
                "full_name": "Owner/Repo",
                "owner": {"login": "Owner"},
            },
            "issue": {
                "number": 43,
                "html_url": "https://github.com/owner/repo/issues/43",
                "state": "open",
            },
            "sender": {"login": "alice"},
        }

        env = service.github_event_env("issues", "delivery-1", payload, "/tmp/payload.json")

        self.assertEqual(env["GC_GITHUB_EVENT"], "issues")
        self.assertEqual(env["GC_GITHUB_ISSUE_NUMBER"], "43")
        self.assertEqual(env["GC_GITHUB_ISSUE_URL"], "https://github.com/owner/repo/issues/43")
        self.assertEqual(env["GC_GITHUB_ITEM_KIND"], "issue")
        self.assertEqual(env["GC_GITHUB_ITEM_NUMBER"], "43")

    def test_order_action_runs_gc_order_with_event_env_and_installation_token(self) -> None:
        payload = {
            "action": "labeled",
            "installation": {"id": 88},
            "label": {"name": "status/needs-review"},
            "repository": {
                "id": 123,
                "name": "repo",
                "full_name": "Owner/Repo",
                "owner": {"login": "Owner"},
            },
            "pull_request": {
                "number": 42,
                "html_url": "https://github.com/owner/repo/pull/42",
                "state": "open",
            },
        }
        action = {"type": "order", "name": "pr-review-request", "github_app_token_env": "GH_TOKEN"}
        completed = mock.Mock(returncode=0, stdout="ok\n", stderr="")

        with mock.patch.object(service.common, "create_installation_token", return_value="token-123"), mock.patch.object(
            service,
            "run_subprocess",
            return_value=completed,
        ) as run_subprocess:
            outcome = service.execute_rule_action(
                {"id": "rule"},
                action,
                "pull_request",
                "delivery-1",
                payload,
                "/tmp/payload.json",
                {"app_id": "1", "private_key_pem": "pem"},
            )

        self.assertEqual(outcome["status"], "success")
        command, cwd = run_subprocess.call_args.args[:2]
        env = run_subprocess.call_args.kwargs["env"]
        self.assertEqual(command, ["gc", "order", "run", "pr-review-request"])
        self.assertEqual(cwd, self.tempdir.name)
        self.assertEqual(env["GC_GITHUB_PR_URL"], "https://github.com/owner/repo/pull/42")
        self.assertEqual(env["GH_TOKEN"], "token-123")
        self.assertTrue(outcome["github_app_token_injected"])

    def test_order_action_routes_exact_repository_to_owning_city_and_rig(self) -> None:
        payload = {
            "repository": {"full_name": "Owner/Repo"},
            "issue": {"number": 42, "html_url": "https://github.com/owner/repo/issues/42"},
        }
        rules = {
            "repos": [
                {
                    "full_name": "owner/repo",
                    "city": "product-city",
                    "rig": "product",
                }
            ]
        }
        action = {"type": "order", "name": "work-sync", "route_from_repo": True}
        completed = mock.Mock(returncode=0, stdout="ok\n", stderr="")

        with mock.patch.object(service, "run_subprocess", return_value=completed) as run_subprocess:
            outcome = service.execute_rule_action(
                {"id": "rule"},
                action,
                "issues",
                "delivery-1",
                payload,
                "/tmp/payload.json",
                {},
                rules,
            )

        self.assertEqual(outcome["status"], "success")
        command, cwd = run_subprocess.call_args.args[:2]
        env = run_subprocess.call_args.kwargs["env"]
        self.assertEqual(
            command,
            ["gc", "--city", "product-city", "order", "run", "work-sync", "--rig", "product"],
        )
        self.assertEqual(cwd, self.tempdir.name)
        self.assertEqual(env["GC_GITHUB_DISPATCH_CITY"], "product-city")
        self.assertEqual(env["GC_GITHUB_DISPATCH_RIG"], "product")

    def test_order_action_repo_route_fails_closed_without_exact_mapping(self) -> None:
        payload = {"repository": {"full_name": "owner/unknown"}}
        action = {"type": "order", "name": "work-sync", "route_from_repo": True}

        with mock.patch.object(service, "run_subprocess") as run_subprocess:
            outcome = service.execute_rule_action(
                {"id": "rule"},
                action,
                "issues",
                "delivery-1",
                payload,
                "/tmp/payload.json",
                {},
                {"repos": []},
            )

        self.assertEqual(outcome["status"], "failed")
        self.assertEqual(outcome["reason"], "repository_route_not_configured")
        run_subprocess.assert_not_called()

    def test_process_event_rules_executes_matching_order_rule_and_persists_result(self) -> None:
        rules_dir = pathlib.Path(self.tempdir.name) / "config" / "github-intake"
        rules_dir.mkdir(parents=True)
        (rules_dir / "rules.toml").write_text(
            """
version = 1

[[rule]]
id = "pr-review-on-needs-review-label"
event = "pull_request"

[rule.match]
action = "labeled"
label.name = "status/needs-review"
pull_request.state = "open"

[[rule.action]]
type = "order"
name = "pr-review-request"
""",
            encoding="utf-8",
        )
        payload = {
            "action": "labeled",
            "label": {"name": "status/needs-review"},
            "repository": {"full_name": "owner/repo"},
            "pull_request": {"number": 42, "state": "open"},
        }
        completed = mock.Mock(returncode=0, stdout="", stderr="")

        with mock.patch.object(service, "run_subprocess", return_value=completed):
            summaries = service.process_event_rules("pull_request", "delivery-1", payload, {})

        self.assertEqual(summaries[0]["rule_id"], "pr-review-on-needs-review-label")
        self.assertEqual(summaries[0]["status"], "success")
        results = service.common.list_recent_rule_results()
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["rule_id"], "pr-review-on-needs-review-label")

    def test_process_event_rules_allows_self_bot_only_for_opted_in_rules(self) -> None:
        rules_dir = pathlib.Path(self.tempdir.name) / "config" / "github-intake"
        rules_dir.mkdir(parents=True)
        (rules_dir / "rules.toml").write_text(
            """
version = 1

[[rule]]
id = "self-allowed-needs-triage"
event = "pull_request"
allow_self = true

[rule.match]
action = "labeled"
label.name = "status/needs-triage"

[[rule.action]]
type = "order"
name = "triage-patrol"

[[rule]]
id = "self-blocked-needs-review"
event = "pull_request"

[rule.match]
action = "labeled"
label.name = "status/needs-triage"

[[rule.action]]
type = "order"
name = "pr-review-request"
""",
            encoding="utf-8",
        )
        payload = {
            "action": "labeled",
            "label": {"name": "status/needs-triage"},
            "repository": {"full_name": "owner/repo"},
            "pull_request": {"number": 42, "state": "open"},
            "sender": {"login": "mayor[bot]"},
        }
        completed = mock.Mock(returncode=0, stdout="", stderr="")

        with mock.patch.object(service, "run_subprocess", return_value=completed) as run_subprocess:
            summaries = service.process_event_rules("pull_request", "delivery-1", payload, {"slug": "mayor"})

        self.assertEqual([summary["rule_id"] for summary in summaries], ["self-allowed-needs-triage"])
        self.assertEqual(run_subprocess.call_args.args[0], ["gc", "order", "run", "triage-patrol"])

    def test_process_addressed_comment_creates_source_and_defers_ack_to_handler(self) -> None:
        rules = {
            "repos": [
                {
                    "full_name": "owner/repo",
                    "rig": "product",
                    "authorized_users": ["alice"],
                    "addresses": [
                        {
                            "address": "@mayor",
                            "pool": "mayor",
                            "target": "product/mayor",
                            "formula": "github-addressed-message",
                        }
                    ],
                }
            ]
        }
        payload = {
            "action": "created",
            "installation": {"id": 88},
            "issue": {"number": 42, "html_url": "https://github.com/owner/repo/issues/42"},
            "comment": {
                "id": 99,
                "body": "@mayor please triage this",
                "html_url": "https://github.com/owner/repo/issues/42#issuecomment-99",
                "user": {"login": "alice", "type": "User"},
            },
            "repository": {
                "id": 123,
                "name": "repo",
                "full_name": "owner/repo",
                "owner": {"login": "owner"},
            },
        }

        with mock.patch.object(service.common, "load_rules", return_value=rules), mock.patch.object(
            service,
            "create_addressed_source",
            return_value={"status": "created", "bead_id": "ga-addr1", "source_key": "github-comment:123:99:@mayor"},
        ) as create_addressed_source, mock.patch.object(
            service.common,
            "post_issue_comment",
            return_value={"id": "ack-1", "html_url": "https://github.com/owner/repo/issues/42#issuecomment-100"},
        ) as post_issue_comment, mock.patch.object(
            service,
            "enqueue_addressed_router",
            return_value="queued",
        ) as enqueue_addressed_router:
            outcome = service.process_addressed_comment(
                "issue_comment",
                "delivery-1",
                payload,
                {"app_id": "1", "private_key_pem": "pem"},
            )

        self.assertEqual(outcome["status"], "accepted")
        self.assertEqual(outcome["created_count"], 1)
        self.assertEqual(outcome["router_kick"], "queued")
        request = create_addressed_source.call_args.args[0]
        self.assertEqual(request["cleaned_body"], "please triage this")
        self.assertEqual(request["source_key"], "github-comment:123:99:@mayor")
        self.assertEqual(request["target"], "product/mayor")
        enqueue_addressed_router.assert_called_once_with("delivery-1", ["github-comment:123:99:@mayor"])
        post_issue_comment.assert_not_called()
        self.assertNotIn("reply_comment_id", outcome)

    def test_process_addressed_comment_carries_profile_app_for_handler_ack(self) -> None:
        rules = {
            "repos": [
                {
                    "full_name": "owner/repo",
                    "rig": "product",
                    "authorized_users": ["alice"],
                    "addresses": [
                        {
                            "address": "@mayor",
                            "pool": "mayor",
                            "target": "product/mayor",
                            "formula": "github-addressed-message",
                            "profile": "mayor",
                            "github_app_identity": "mayor",
                            "installation_id": "profile-installation",
                        }
                    ],
                }
            ]
        }
        payload = {
            "action": "created",
            "installation": {"id": 88},
            "issue": {"number": 42, "html_url": "https://github.com/owner/repo/issues/42"},
            "comment": {
                "id": 99,
                "body": "@mayor please triage this",
                "html_url": "https://github.com/owner/repo/issues/42#issuecomment-99",
                "user": {"login": "alice", "type": "User"},
            },
            "repository": {
                "id": 123,
                "name": "repo",
                "full_name": "owner/repo",
                "owner": {"login": "owner"},
            },
        }

        with mock.patch.object(service.common, "load_rules", return_value=rules), mock.patch.object(
            service,
            "create_addressed_source",
            return_value={"status": "created", "bead_id": "ga-addr1", "source_key": "github-comment:123:99:@mayor"},
        ) as create_addressed_source, mock.patch.object(
            service,
            "load_profile_github_app",
            return_value={"app_id": "mayor-app", "private_key_pem": "mayor-pem"},
        ) as load_profile_github_app, mock.patch.object(
            service.common,
            "post_issue_comment",
            return_value={"id": "ack-1", "html_url": "https://github.com/owner/repo/issues/42#issuecomment-100"},
        ) as post_issue_comment, mock.patch.object(
            service,
            "enqueue_addressed_router",
            return_value="queued",
        ):
            outcome = service.process_addressed_comment(
                "issue_comment",
                "delivery-1",
                payload,
                {"app_id": "mayor-app", "private_key_pem": "mayor-pem"},
            )

        self.assertEqual(outcome["status"], "accepted")
        request = create_addressed_source.call_args.args[0]
        self.assertEqual(request["profile"], "mayor")
        self.assertEqual(request["profile_github_app_identity"], "mayor")
        self.assertEqual(request["profile_installation_id"], "profile-installation")
        load_profile_github_app.assert_not_called()
        post_issue_comment.assert_not_called()

    def test_load_profile_github_app_uses_identity_resolver(self) -> None:
        service.PROFILE_APP_CACHE.clear()
        result = subprocess.CompletedProcess(
            ["resolve", "mayor"],
            0,
            stdout=json.dumps(
                {
                    "schema_version": service.common.GITHUB_APP_IDENTITY_SCHEMA_VERSION,
                    "app_id": "mayor-app",
                    "installation_id": "profile-installation",
                    "private_key_pem": "pem",
                    "ready": "true",
                }
            ),
            stderr="",
        )

        with mock.patch.dict(
            service.os.environ,
            {"GITHUB_INTAKE_IDENTITY_RESOLVER": "resolve --json"},
            clear=True,
        ), mock.patch.object(service, "run_subprocess", return_value=result) as run_subprocess:
            app = service.load_profile_github_app("mayor")

        self.assertEqual(app["app_id"], "mayor-app")
        self.assertEqual(app["installation_id"], "profile-installation")
        self.assertEqual(app["private_key_pem"], "pem")
        run_subprocess.assert_called_once()
        self.assertEqual(run_subprocess.call_args.args[0], ["resolve", "--json", "mayor"])

    def test_load_profile_github_app_uses_city_workspace_env_resolver(self) -> None:
        service.PROFILE_APP_CACHE.clear()
        pathlib.Path(self.tempdir.name, "city.toml").write_text(
            """
[workspace]
[workspace.env]
GITHUB_INTAKE_IDENTITY_RESOLVER = "resolve --json"
GITHUB_INTAKE_APP_IDENTITY = "mayor"
GITHUB_INTAKE_STORE_NAMESPACE = "internal"
GITHUB_INTAKE_STORE_SECRET_PREFIX = "agents"
GITHUB_INTAKE_STORE_SESSION_ENV = "/tmp/session.env"
UNRELATED_KEY = "must-not-pass-through"
""".strip(),
            encoding="utf-8",
        )
        result = subprocess.CompletedProcess(
            ["resolve", "mayor"],
            0,
            stdout=json.dumps(
                {
                    "schema_version": service.common.GITHUB_APP_IDENTITY_SCHEMA_VERSION,
                    "app_id": "mayor-app",
                    "installation_id": "profile-installation",
                    "private_key_pem": "pem",
                    "ready": "true",
                }
            ),
            stderr="",
        )

        with mock.patch.dict(
            service.os.environ,
            {"GC_CITY_ROOT": self.tempdir.name},
            clear=True,
        ), mock.patch.object(service, "run_subprocess", return_value=result) as run_subprocess:
            app = service.load_profile_github_app("mayor")

        self.assertEqual(app["app_id"], "mayor-app")
        self.assertEqual(run_subprocess.call_args.args[0], ["resolve", "--json", "mayor"])
        env = run_subprocess.call_args.kwargs["env"]
        self.assertEqual(env["GITHUB_INTAKE_IDENTITY_RESOLVER"], "resolve --json")
        self.assertEqual(env["GITHUB_INTAKE_APP_IDENTITY"], "mayor")
        self.assertEqual(env["GITHUB_INTAKE_STORE_NAMESPACE"], "internal")
        self.assertEqual(env["GITHUB_INTAKE_STORE_SECRET_PREFIX"], "agents")
        self.assertEqual(env["GITHUB_INTAKE_STORE_SESSION_ENV"], "/tmp/session.env")
        self.assertNotIn("UNRELATED_KEY", env)

    def test_configured_github_app_identity_uses_city_workspace_env(self) -> None:
        pathlib.Path(self.tempdir.name, "city.toml").write_text(
            """
[workspace]
[workspace.env]
GITHUB_INTAKE_APP_IDENTITY = "mayor"
""".strip(),
            encoding="utf-8",
        )

        with mock.patch.dict(service.os.environ, {"GC_CITY_ROOT": self.tempdir.name}, clear=True):
            identity = service.configured_github_app_identity()

        self.assertEqual(identity, "mayor")

    def test_sync_github_app_config_from_identity_persists_resolved_bundle(self) -> None:
        service.PROFILE_APP_CACHE.clear()

        with mock.patch.object(
            service,
            "load_profile_github_app",
            return_value={
                "app_id": "3506340",
                "installation_id": "127147095",
                "webhook_secret": "secret",
                "private_key_pem": "pem",
                "slug": "release-manager",
            },
        ) as load_profile_github_app:
            outcome = service.sync_github_app_config_from_identity("mayor")

        self.assertEqual(outcome["status"], "updated")
        self.assertEqual(outcome["identity"], "mayor")
        load_profile_github_app.assert_called_once_with("mayor")
        config = service.common.load_config()
        self.assertEqual(config["app"]["app_id"], "3506340")
        self.assertEqual(config["app"]["installation_id"], "127147095")
        self.assertEqual(config["app"]["webhook_secret"], "secret")
        self.assertEqual(config["app"]["private_key_pem"], "pem")

    def test_load_profile_github_app_requires_identity_schema_version(self) -> None:
        service.PROFILE_APP_CACHE.clear()
        result = subprocess.CompletedProcess(
            ["resolve", "mayor"],
            0,
            stdout=json.dumps({"app_id": "mayor-app", "private_key_pem": "pem"}),
            stderr="",
        )

        with mock.patch.dict(
            service.os.environ,
            {"GITHUB_INTAKE_IDENTITY_RESOLVER": "resolve"},
            clear=True,
        ), mock.patch.object(service, "run_subprocess", return_value=result):
            with self.assertRaisesRegex(RuntimeError, "schema_version"):
                service.load_profile_github_app("mayor")

    def test_load_profile_github_app_requires_identity_resolver(self) -> None:
        service.PROFILE_APP_CACHE.clear()
        with mock.patch.dict(service.os.environ, {}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "GITHUB_INTAKE_IDENTITY_RESOLVER"):
                service.load_profile_github_app("mayor")

    def test_process_addressed_comment_honors_ack_false(self) -> None:
        rules = {
            "repos": [
                {
                    "full_name": "owner/repo",
                    "authorized_users": ["alice"],
                    "addresses": [
                        {
                            "address": "@mayor",
                            "pool": "mayor",
                            "formula": "github-addressed-message",
                            "ack": False,
                        }
                    ],
                }
            ]
        }
        payload = {
            "action": "created",
            "installation": {"id": 88},
            "issue": {"number": 42, "html_url": "https://github.com/owner/repo/issues/42"},
            "comment": {"id": 99, "body": "@mayor please triage this", "user": {"login": "alice", "type": "User"}},
            "repository": {"id": 123, "name": "repo", "full_name": "owner/repo", "owner": {"login": "owner"}},
        }

        with mock.patch.object(service.common, "load_rules", return_value=rules), mock.patch.object(
            service,
            "create_addressed_source",
            return_value={"status": "created", "bead_id": "ga-addr1", "source_key": "github-comment:123:99:@mayor"},
        ), mock.patch.object(
            service.common,
            "post_issue_comment",
        ) as post_issue_comment, mock.patch.object(
            service,
            "enqueue_addressed_router",
            return_value="queued",
        ) as enqueue_addressed_router:
            outcome = service.process_addressed_comment("issue_comment", "delivery-1", payload, {"app_id": "1"})

        self.assertEqual(outcome["status"], "accepted")
        self.assertEqual(outcome["created_count"], 1)
        self.assertEqual(outcome["router_kick"], "queued")
        enqueue_addressed_router.assert_called_once_with("delivery-1", ["github-comment:123:99:@mayor"])
        post_issue_comment.assert_not_called()

    def test_process_addressed_comment_kicks_router_for_duplicate_source(self) -> None:
        rules = {
            "repos": [
                {
                    "full_name": "owner/repo",
                    "authorized_users": ["alice"],
                    "addresses": [{"address": "@mayor", "pool": "mayor", "formula": "github-addressed-message"}],
                }
            ]
        }
        payload = {
            "action": "created",
            "installation": {"id": 88},
            "issue": {"number": 42, "html_url": "https://github.com/owner/repo/issues/42"},
            "comment": {"id": 99, "body": "@mayor please triage this", "user": {"login": "alice", "type": "User"}},
            "repository": {"id": 123, "name": "repo", "full_name": "owner/repo", "owner": {"login": "owner"}},
        }

        with mock.patch.object(service.common, "load_rules", return_value=rules), mock.patch.object(
            service,
            "create_addressed_source",
            return_value={
                "status": "duplicate",
                "bead_id": "ga-addr1",
                "source_key": "github-comment:123:99:@mayor",
            },
        ), mock.patch.object(
            service.common,
            "post_issue_comment",
        ) as post_issue_comment, mock.patch.object(
            service,
            "enqueue_addressed_router",
            return_value="queued",
        ) as enqueue_addressed_router:
            outcome = service.process_addressed_comment("issue_comment", "delivery-1", payload, {"app_id": "1"})

        self.assertEqual(outcome["status"], "accepted")
        self.assertEqual(outcome["created_count"], 0)
        self.assertEqual(outcome["duplicate_count"], 1)
        self.assertEqual(outcome["router_kick"], "queued")
        enqueue_addressed_router.assert_called_once_with("delivery-1", ["github-comment:123:99:@mayor"])
        post_issue_comment.assert_not_called()

    def test_enqueue_addressed_router_runs_scan_in_background(self) -> None:
        class ImmediateThread:
            def __init__(self, target, args=(), daemon=None) -> None:
                self.target = target
                self.args = args
                self.daemon = daemon

            def start(self) -> None:
                self.target(*self.args)

        with mock.patch.object(
            service.threading,
            "Thread",
            ImmediateThread,
        ), mock.patch.object(
            service,
            "run_addressed_router",
            return_value={"status": "ok", "started_count": 1, "started": [{"bead_id": "ga-src1"}]},
        ) as run_addressed_router, mock.patch.object(
            service.common,
            "save_address_result",
        ) as save_address_result:
            status = service.enqueue_addressed_router("delivery-1", ["github-comment:123:99:@mayor"])

        self.assertEqual(status, "queued")
        run_addressed_router.assert_called_once_with(limit=50)
        result = save_address_result.call_args.args[0]
        self.assertEqual(result["result_id"], "delivery-1-addressed-router-kick")
        self.assertEqual(result["event"], "addressed-router-kick")
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["source_keys"], ["github-comment:123:99:@mayor"])
        self.assertEqual(result["router"]["started_count"], 1)

    def test_process_addressed_comment_rejects_unauthorized_sender_with_reply(self) -> None:
        rules = {
            "repos": [
                {
                    "full_name": "owner/repo",
                    "authorized_users": ["alice"],
                    "addresses": [{"address": "@mayor", "pool": "mayor", "formula": "github-addressed-message"}],
                }
            ]
        }
        payload = {
            "action": "created",
            "installation": {"id": 88},
            "issue": {"number": 42, "html_url": "https://github.com/owner/repo/issues/42"},
            "comment": {
                "id": 99,
                "body": "@mayor please triage this",
                "user": {"login": "mallory", "type": "User"},
            },
            "repository": {"id": 123, "name": "repo", "full_name": "owner/repo", "owner": {"login": "owner"}},
        }

        with mock.patch.object(service.common, "load_rules", return_value=rules), mock.patch.object(
            service,
            "create_addressed_source",
        ) as create_addressed_source, mock.patch.object(
            service.common,
            "post_issue_comment",
            return_value={},
        ) as post_issue_comment:
            outcome = service.process_addressed_comment("issue_comment", "delivery-1", payload, {"app_id": "1"})

        self.assertEqual(outcome["status"], "rejected")
        self.assertEqual(outcome["reason"], "sender_not_authorized")
        create_addressed_source.assert_not_called()
        self.assertIn("not authorized to use `@mayor`", post_issue_comment.call_args.args[5])

    def test_process_addressed_comment_does_not_repeat_rejection_reply(self) -> None:
        rules = {
            "repos": [
                {
                    "full_name": "owner/repo",
                    "authorized_users": ["alice"],
                    "addresses": [{"address": "@mayor", "pool": "mayor", "formula": "github-addressed-message"}],
                }
            ]
        }
        payload = {
            "action": "created",
            "installation": {"id": 88},
            "issue": {"number": 42, "html_url": "https://github.com/owner/repo/issues/42"},
            "comment": {"id": 99, "body": "@mayor please triage this", "user": {"login": "mallory", "type": "User"}},
            "repository": {"id": 123, "name": "repo", "full_name": "owner/repo", "owner": {"login": "owner"}},
        }
        service.common.save_address_result(
            {
                "result_id": "github-comment:123:99:@mayor:sender-not-authorized",
                "created_at": "2026-05-28T12:00:00Z",
                "status": "rejected",
            }
        )

        with mock.patch.object(service.common, "load_rules", return_value=rules), mock.patch.object(
            service.common,
            "post_issue_comment",
        ) as post_issue_comment:
            outcome = service.process_addressed_comment("issue_comment", "delivery-2", payload, {"app_id": "1"})

        self.assertEqual(outcome["status"], "duplicate")
        self.assertEqual(outcome["reason"], "sender_not_authorized_already_reported")
        post_issue_comment.assert_not_called()

    def test_create_addressed_source_dedupes_existing_closed_source(self) -> None:
        existing = {"id": "ga-closed", "status": "closed", "metadata": {"external.source_key": "github-comment:123:99:@mayor"}}
        list_result = mock.Mock(returncode=0, stdout=json.dumps([existing]), stderr="")

        with mock.patch.object(service, "run_subprocess", return_value=list_result) as run_subprocess:
            outcome = service.create_addressed_source(
                {
                    "source_key": "github-comment:123:99:@mayor",
                    "address": "@mayor",
                    "cleaned_body": "please triage",
                    "pool": "mayor",
                    "rig": "github-owner-repo",
                    "target": "github-owner-repo/mayor",
                    "formula": "github-addressed-message",
                }
            )

        self.assertEqual(outcome["status"], "duplicate")
        self.assertEqual(outcome["bead_id"], "ga-closed")
        command = run_subprocess.call_args.args[0]
        self.assertIn("--all", command)
        self.assertNotIn("--status", command)

    def test_create_addressed_source_stores_repo_derived_target_metadata(self) -> None:
        list_result = mock.Mock(returncode=0, stdout="[]", stderr="")
        create_result = mock.Mock(returncode=0, stdout='{"id":"ga-src1"}\n', stderr="")

        with mock.patch.object(service, "run_subprocess", side_effect=[list_result, create_result]) as run_subprocess:
            outcome = service.create_addressed_source(
                {
                    "source_key": "github-comment:123:99:@mayor",
                    "address": "@mayor",
                    "cleaned_body": "please triage",
                    "repository_full_name": "owner/repo",
                    "repository_id": "123",
                    "item_number": "42",
                    "pool": "mayor",
                    "rig": "github-owner-repo",
                    "target": "github-owner-repo/mayor",
                    "formula": "github-addressed-message",
                    "ack": False,
                    "profile": "mayor",
                    "profile_github_app_identity": "mayor",
                    "profile_installation_id": "profile-installation",
                }
            )

        self.assertEqual(outcome["status"], "created")
        create_command = run_subprocess.call_args_list[1].args[0]
        metadata = json.loads(create_command[create_command.index("--metadata") + 1])
        self.assertEqual(metadata["addressed.rig"], "github-owner-repo")
        self.assertEqual(metadata["addressed.pool"], "mayor")
        self.assertEqual(metadata["addressed.target"], "github-owner-repo/mayor")
        self.assertEqual(metadata["addressed.profile"], "mayor")
        self.assertEqual(metadata["addressed.github_app_identity"], "mayor")
        self.assertEqual(metadata["addressed.github_app_installation_id"], "profile-installation")
        self.assertEqual(metadata["addressed.ack_requested"], "false")

    def test_addressed_route_target_derives_from_github_repo_at_dispatch_time(self) -> None:
        with mock.patch.object(
            service.common,
            "load_rules",
            return_value={"repos": [{"full_name": "owner/repo", "rig": "product"}]},
        ):
            target = service.addressed_route_target(
                {
                    "github.repo": "owner/repo",
                    "addressed.pool": "mayor",
                    "addressed.target": "wrong-rig/mayor",
                }
            )

        self.assertEqual(target, "product/mayor")

    def test_addressed_route_target_falls_back_to_github_slug_without_mapping(self) -> None:
        target = service.addressed_route_target(
            {
                "github.repo": "owner/repo",
                "addressed.pool": "mayor",
                "addressed.target": "wrong-rig/mayor",
            }
        )

        self.assertEqual(target, "github-owner-repo/mayor")
        self.assertEqual(service.addressed_route_target({"addressed.pool": "mayor"}), "")
        self.assertEqual(service.addressed_route_target({"github.repo": "owner/repo", "addressed.pool": "wrong/mayor"}), "")

    def test_addressed_rig_launch_description_includes_github_response_contract(self) -> None:
        description = service.addressed_rig_launch_description(
            "ga-src1",
            {
                "addressed.address": "@mayor",
                "github.repo": "owner/repo",
                "github.issue_number": "42",
                "github.item_url": "https://github.com/owner/repo/issues/42",
                "github.comment_url": "https://github.com/owner/repo/issues/42#issuecomment-99",
                "github.sender": "octocat",
            },
            "github-addressed-message",
        )

        self.assertIn("## GitHub Response Contract", description)
        self.assertIn("The GitHub thread is the user-visible response channel.", description)
        self.assertIn("gc github comment-issue owner/repo 42", description)
        self.assertIn("Local session output alone does not complete this request.", description)

    def test_addressed_formula_requires_final_github_comment(self) -> None:
        formula = (pathlib.Path(__file__).resolve().parents[1] / "formulas" / "github-addressed-message.formula.toml").read_text(
            encoding="utf-8"
        )

        self.assertIn("## GitHub Response Contract", formula)
        self.assertIn("The GitHub thread is the user-visible response channel.", formula)
        self.assertIn("Dry-run requests still require a GitHub comment", formula)
        self.assertIn("Local session output, bead metadata, and in-session summaries are not completion.", formula)

    def test_run_addressed_router_slings_open_sources_and_closes_them(self) -> None:
        source = {
            "id": "ga-src1",
            "status": "open",
            "title": "GitHub addressed message @mayor in owner/repo#42",
            "metadata": {
                "external.source_key": "github-comment:123:99:@mayor",
                "external.kind": "addressed-message",
                "addressed.address": "@mayor",
                "addressed.cleaned_body": "please triage this",
                "addressed.pool": "mayor",
                "addressed.formula": "github-addressed-message",
                "addressed.ack_requested": "true",
                "addressed.router_status": "failed",
                "addressed.router_reason": "sling_failed",
                "addressed.workflow_store": "rig:old",
                "github.repo": "owner/repo",
                "github.issue_number": "42",
                "github.item_url": "https://github.com/owner/repo/issues/42",
                "github.comment_url": "https://github.com/owner/repo/issues/42#issuecomment-99",
                "github.installation_id": "repo-installation",
                "addressed.github_app_identity": "mayor",
                "addressed.github_app_installation_id": "profile-installation",
            },
        }
        list_result = mock.Mock(returncode=0, stdout=json.dumps([source]), stderr="")
        starting_result = mock.Mock(returncode=0, stdout="", stderr="")
        create_result = mock.Mock(returncode=0, stdout='{"id":"ga-launch"}\n', stderr="")
        sling_result = mock.Mock(returncode=0, stdout='{"workflow_id":"ga-root","bead_id":"ga-root"}\n', stderr="")
        update_result = mock.Mock(returncode=0, stdout="", stderr="")
        close_result = mock.Mock(returncode=0, stdout="", stderr="")

        with mock.patch.object(
            service,
            "run_subprocess",
            side_effect=[list_result, starting_result, create_result, sling_result, update_result, close_result],
        ) as run_subprocess:
            outcome = service.run_addressed_router(limit=10)

        self.assertEqual(outcome["status"], "ok")
        self.assertEqual(outcome["started"][0]["rig_launch_bead_id"], "ga-launch")
        self.assertEqual(outcome["started"][0]["workflow_root_id"], "ga-root")
        commands = [call.args[0] for call in run_subprocess.call_args_list]
        self.assertEqual(
            commands[2][:8],
            [
                "gc",
                "--city",
                self.tempdir.name,
                "--rig",
                "github-owner-repo",
                "bd",
                "create",
                "GitHub addressed message @mayor in owner/repo#42",
            ],
        )
        create_metadata = json.loads(commands[2][commands[2].index("--metadata") + 1])
        self.assertEqual(create_metadata["addressed.city_source_bead_id"], "ga-src1")
        self.assertEqual(create_metadata["addressed.status"], "rig_launch_created")
        self.assertEqual(create_metadata["addressed.workflow_target"], "github-owner-repo/mayor")
        self.assertNotIn("addressed.router_status", create_metadata)
        self.assertNotIn("addressed.router_reason", create_metadata)
        self.assertNotIn("addressed.workflow_store", create_metadata)
        self.assertEqual(commands[2][-1], "--json")
        self.assertEqual(
            commands[3][:8],
            ["gc", "--rig", "github-owner-repo", "sling", "--json", "github-owner-repo/mayor", "ga-launch", "--force"],
        )
        self.assertIn("--var", commands[3])
        sling_vars = {
            item.split("=", 1)[0]: item.split("=", 1)[1]
            for index, item in enumerate(commands[3])
            if commands[3][index - 1] == "--var" and "=" in item
        }
        self.assertEqual(sling_vars["github_issue_number"], "42")
        self.assertEqual(sling_vars["city_root"], self.tempdir.name)
        self.assertEqual(sling_vars["github_app_installation_id"], "profile-installation")
        self.assertEqual(sling_vars["github_app_identity"], "mayor")
        self.assertEqual(sling_vars["acknowledgement_requested"], "true")
        city_prefix = ["gc", "--city", self.tempdir.name, "bd"]
        self.assertEqual(commands[1][0:6], city_prefix + ["update", "ga-src1"])
        self.assertEqual(commands[4][0:6], city_prefix + ["update", "ga-src1"])
        self.assertEqual(
            commands[5],
            city_prefix + ["close", "ga-src1", "--reason", "github addressed message dispatched"],
        )

    def test_route_addressed_source_marks_failed_when_post_sling_update_fails(self) -> None:
        source = {
            "id": "ga-src1",
            "status": "open",
            "metadata": {
                "external.source_key": "github-comment:123:99:@mayor",
                "external.kind": "addressed-message",
                "addressed.address": "@mayor",
                "addressed.cleaned_body": "please triage this",
                "addressed.pool": "mayor",
                "addressed.formula": "github-addressed-message",
                "github.repo": "owner/repo",
            },
        }
        starting_result = mock.Mock(returncode=0, stdout="", stderr="")
        create_result = mock.Mock(returncode=0, stdout='{"id":"ga-launch"}\n', stderr="")
        sling_result = mock.Mock(returncode=0, stdout='{"workflow_id":"ga-root"}\n', stderr="")
        update_failed = mock.Mock(returncode=1, stdout="", stderr="bd busy")
        failure_mark = mock.Mock(returncode=0, stdout="", stderr="")

        with mock.patch.object(
            service,
            "run_subprocess",
            side_effect=[starting_result, create_result, sling_result, update_failed, failure_mark],
        ) as run_subprocess:
            outcome = service.route_addressed_source(source)

        self.assertEqual(outcome["status"], "failed")
        self.assertEqual(outcome["reason"], "source_update_failed")
        self.assertEqual(outcome["rig_launch_bead_id"], "ga-launch")
        self.assertEqual(outcome["workflow_root_id"], "ga-root")
        commands = [call.args[0] for call in run_subprocess.call_args_list]
        self.assertIn("addressed.router_status=failed", commands[4])
        self.assertNotIn("close", [command[1] for command in commands])

    def test_route_addressed_source_recloses_already_dispatched_open_source(self) -> None:
        source = {
            "id": "ga-src1",
            "status": "open",
            "metadata": {
                "external.source_key": "github-comment:123:99:@mayor",
                "external.kind": "addressed-message",
                "addressed.workflow_root": "ga-root",
            },
        }
        close_result = mock.Mock(returncode=0, stdout="", stderr="")

        with mock.patch.object(service, "run_subprocess", return_value=close_result) as run_subprocess:
            outcome = service.route_addressed_source(source)

        self.assertEqual(outcome["status"], "skipped")
        self.assertEqual(outcome["reason"], "already_dispatched_closed")
        self.assertEqual(outcome["workflow_root_id"], "ga-root")
        run_subprocess.assert_called_once_with(
            [
                "gc",
                "--city",
                self.tempdir.name,
                "bd",
                "close",
                "ga-src1",
                "--reason",
                "github addressed message dispatched",
            ],
            self.tempdir.name,
        )

    def test_non_comment_webhook_queues_rule_processing_before_responding(self) -> None:
        payload = {
            "action": "labeled",
            "repository": {"full_name": "owner/repo"},
            "pull_request": {"number": 42, "state": "open"},
            "label": {"name": "status/needs-review"},
        }
        body = json.dumps(payload).encode("utf-8")
        handler = DummyWebhookHandler(
            body,
            {
                "Content-Length": str(len(body)),
                "X-Hub-Signature-256": "sha256=test",
                "X-GitHub-Delivery": "delivery-1",
                "X-GitHub-Event": "pull_request",
            },
        )

        with mock.patch.object(
            service.common,
            "load_effective_config",
            return_value={"app": {"webhook_secret": "secret"}},
        ), mock.patch.object(
            service.common,
            "verify_github_signature",
            return_value=True,
        ), mock.patch.object(
            service.common,
            "save_delivery",
        ) as save_delivery, mock.patch.object(
            service,
            "enqueue_event_rules",
            return_value="queued",
        ) as enqueue_event_rules, mock.patch.object(
            service,
            "process_event_rules",
            side_effect=AssertionError("rule processing must not run inline"),
        ):
            service.IntakeHandler._do_webhook_post(handler, urllib.parse.urlparse("/v0/github/webhook"))

        self.assertEqual(handler.status, 202)
        response = json.loads(handler.wfile.getvalue().decode("utf-8"))
        self.assertEqual(response["status"], "accepted")
        self.assertEqual(response["rule_processing"], "queued")
        save_delivery.assert_called_once()
        enqueue_event_rules.assert_called_once_with(
            "pull_request",
            "delivery-1",
            payload,
            {"webhook_secret": "secret"},
        )

    def test_webhook_post_syncs_app_config_when_webhook_secret_missing(self) -> None:
        payload = {
            "action": "labeled",
            "repository": {"full_name": "owner/repo"},
            "pull_request": {"number": 42, "state": "open"},
            "label": {"name": "status/needs-review"},
        }
        body = json.dumps(payload).encode("utf-8")
        handler = DummyWebhookHandler(
            body,
            {
                "Content-Length": str(len(body)),
                "X-Hub-Signature-256": "sha256=test",
                "X-GitHub-Delivery": "delivery-1",
                "X-GitHub-Event": "pull_request",
            },
        )

        with mock.patch.object(
            service,
            "configured_github_app_identity",
            return_value="mayor",
        ), mock.patch.object(
            service,
            "load_profile_github_app",
            return_value={
                "app_id": "3506340",
                "installation_id": "127147095",
                "webhook_secret": "secret",
                "private_key_pem": "pem",
            },
        ) as load_profile_github_app, mock.patch.object(
            service.common,
            "verify_github_signature",
            return_value=True,
        ) as verify_github_signature, mock.patch.object(
            service.common,
            "save_delivery",
        ), mock.patch.object(
            service,
            "enqueue_event_rules",
            return_value="queued",
        ):
            service.IntakeHandler._do_webhook_post(handler, urllib.parse.urlparse("/v0/github/webhook"))

        self.assertEqual(handler.status, 202)
        load_profile_github_app.assert_called_once_with("mayor")
        verify_github_signature.assert_called_once_with("secret", body, "sha256=test")
        config = service.common.load_config()
        self.assertEqual(config["app"]["app_id"], "3506340")
        self.assertEqual(config["app"]["installation_id"], "127147095")

    def test_addressed_comment_response_marks_legacy_gc_parser_skipped(self) -> None:
        payload = {
            "action": "created",
            "repository": {"full_name": "owner/repo"},
            "issue": {"number": 42, "html_url": "https://github.com/owner/repo/issues/42"},
            "comment": {"id": 99, "body": "@mayor /gc fix crash", "user": {"login": "alice"}},
        }
        body = json.dumps(payload).encode("utf-8")
        handler = DummyWebhookHandler(
            body,
            {
                "Content-Length": str(len(body)),
                "X-Hub-Signature-256": "sha256=test",
                "X-GitHub-Delivery": "delivery-1",
                "X-GitHub-Event": "issue_comment",
            },
        )

        with mock.patch.object(
            service.common,
            "load_effective_config",
            return_value={"app": {"webhook_secret": "secret"}},
        ), mock.patch.object(
            service.common,
            "verify_github_signature",
            return_value=True,
        ), mock.patch.object(
            service.common,
            "save_delivery",
        ), mock.patch.object(
            service,
            "enqueue_event_rules",
            return_value="queued",
        ), mock.patch.object(
            service,
            "process_addressed_comment",
            return_value={"status": "accepted", "created_count": 1, "duplicate_count": 0, "failure_count": 0},
        ), mock.patch.object(
            service.common,
            "extract_issue_comment_request",
        ) as extract_issue_comment_request:
            service.IntakeHandler._do_webhook_post(handler, urllib.parse.urlparse("/v0/github/webhook"))

        self.assertEqual(handler.status, 202)
        response = json.loads(handler.wfile.getvalue().decode("utf-8"))
        self.assertEqual(response["status"], "accepted")
        self.assertEqual(response["command_processing"], "skipped_addressed_message")
        extract_issue_comment_request.assert_not_called()

    def test_bot_addressed_comment_does_not_fall_through_to_legacy_gc_parser(self) -> None:
        payload = {
            "action": "created",
            "repository": {"full_name": "owner/repo"},
            "issue": {"number": 42, "html_url": "https://github.com/owner/repo/issues/42"},
            "comment": {"id": 99, "body": "@mayor please triage\n/gc fix crash", "user": {"login": "renovate[bot]"}},
        }
        body = json.dumps(payload).encode("utf-8")
        handler = DummyWebhookHandler(
            body,
            {
                "Content-Length": str(len(body)),
                "X-Hub-Signature-256": "sha256=test",
                "X-GitHub-Delivery": "delivery-1",
                "X-GitHub-Event": "issue_comment",
            },
        )

        with mock.patch.object(
            service.common,
            "load_effective_config",
            return_value={"app": {"webhook_secret": "secret"}},
        ), mock.patch.object(
            service.common,
            "verify_github_signature",
            return_value=True,
        ), mock.patch.object(
            service.common,
            "save_delivery",
        ), mock.patch.object(
            service,
            "enqueue_event_rules",
            return_value="queued",
        ), mock.patch.object(
            service,
            "process_addressed_comment",
            return_value={"status": "ignored", "reason": "comment_from_bot", "addresses": ["@mayor"]},
        ), mock.patch.object(
            service.common,
            "extract_issue_comment_request",
        ) as extract_issue_comment_request:
            service.IntakeHandler._do_webhook_post(handler, urllib.parse.urlparse("/v0/github/webhook"))

        self.assertEqual(handler.status, 202)
        response = json.loads(handler.wfile.getvalue().decode("utf-8"))
        self.assertEqual(response["status"], "ignored")
        self.assertEqual(response["addressed_message"]["reason"], "comment_from_bot")
        self.assertEqual(response["command_processing"], "skipped_addressed_message")
        extract_issue_comment_request.assert_not_called()

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
        ), mock.patch.object(
            service,
            "run_subprocess",
            side_effect=[mock.Mock(returncode=0), mock.Mock(returncode=0)],
        ) as run_subprocess:
            outcome = service.run_fix_issue_dispatch(request, mapping, command_cfg, app_cfg)

        self.assertEqual(outcome["status"], "dispatch_failed")
        self.assertEqual(outcome["reason"], "bead_update_failed")
        self.assertEqual(outcome["bead_id"], "bd-1")
        self.assertTrue(outcome["bead_closed"])
        commands = [call.args[0] for call in run_subprocess.call_args_list]
        prefix = ["gc", "--city", self.tempdir.name, "--rig", "product", "bd"]
        self.assertEqual(
            commands[0],
            prefix + ["update", "bd-1", "--set-metadata", "close_reason=github:bead_update_failed"],
        )
        self.assertEqual(commands[1], prefix + ["close", "bd-1"])

    def test_run_fix_bugflow_dispatch_creates_source_and_routes_with_app_token(self) -> None:
        request = {
            "installation_id": "88",
            "repository_owner": "owner",
            "repository_name": "repo",
            "repository_full_name": "owner/repo",
            "issue_number": "42",
            "issue_url": "https://github.com/owner/repo/issues/42",
            "comment_author": "alice",
        }
        create_result = mock.Mock(
            returncode=0,
            stdout='{"bead_id":"mc-source","status":"created"}\n',
            stderr="",
        )
        router_result = mock.Mock(
            returncode=0,
            stdout='{"status":"ok","started":[{"bead_id":"mc-source","workflow_root_id":"ga-root","rig_launch_bead_id":"ga-launch"}]}\n',
            stderr="",
        )

        with mock.patch.object(service.common, "repository_permission", return_value="write"), mock.patch.object(
            service.common,
            "create_installation_token",
            return_value="token-123",
        ), mock.patch.object(
            service.common,
            "post_issue_comment",
            return_value={},
        ), mock.patch.object(
            service,
            "run_subprocess",
            side_effect=[create_result, router_result],
        ) as run_subprocess:
            outcome = service.run_fix_bugflow_dispatch(request, {"app_id": "1", "private_key_pem": "pem"})

        self.assertEqual(outcome["status"], "dispatched")
        self.assertEqual(outcome["dispatch_formula"], "mol-bug-report-flow-v2")
        self.assertEqual(outcome["bead_id"], "mc-source")
        self.assertEqual(outcome["workflow_root_id"], "ga-root")
        commands = [call.args[0] for call in run_subprocess.call_args_list]
        self.assertEqual(commands[0], ["gc", "workflows", "bugflow", "create", "https://github.com/owner/repo/issues/42"])
        self.assertEqual(commands[1], ["gc", "workflows", "bugflow", "router-scan"])
        self.assertEqual(run_subprocess.call_args_list[0].kwargs["env"]["GH_TOKEN"], "token-123")
        self.assertEqual(run_subprocess.call_args_list[1].kwargs["env"]["GH_TOKEN"], "token-123")

    def test_run_fix_bugflow_dispatch_posts_acknowledgement_comment(self) -> None:
        request = {
            "installation_id": "88",
            "repository_owner": "owner",
            "repository_name": "repo",
            "repository_full_name": "owner/repo",
            "issue_number": "42",
            "issue_url": "https://github.com/owner/repo/issues/42",
            "comment_author": "alice",
        }
        create_result = mock.Mock(
            returncode=0,
            stdout='{"bead_id":"mc-source","status":"created"}\n',
            stderr="",
        )
        router_result = mock.Mock(
            returncode=0,
            stdout='{"status":"ok","started":[{"bead_id":"mc-source","workflow_root_id":"ga-root","rig_launch_bead_id":"ga-launch"}]}\n',
            stderr="",
        )

        with mock.patch.object(service.common, "repository_permission", return_value="write"), mock.patch.object(
            service.common,
            "create_installation_token",
            return_value="token-123",
        ), mock.patch.object(
            service.common,
            "post_issue_comment",
            return_value={"id": "comment-123", "html_url": "https://github.com/owner/repo/issues/42#issuecomment-123"},
        ) as post_issue_comment, mock.patch.object(
            service,
            "run_subprocess",
            side_effect=[create_result, router_result],
        ):
            outcome = service.run_fix_bugflow_dispatch(request, {"app_id": "1", "private_key_pem": "pem"})

        self.assertEqual(outcome["status"], "dispatched")
        self.assertEqual(outcome["acknowledgement_comment_id"], "comment-123")
        self.assertEqual(outcome["acknowledgement_comment_url"], "https://github.com/owner/repo/issues/42#issuecomment-123")
        comment_args = post_issue_comment.call_args.args
        self.assertEqual(comment_args[1:5], ("88", "owner", "repo", "42"))
        self.assertIn("`/gc fix` request is queued", comment_args[5])
        self.assertNotIn("Mayor", comment_args[5])
        self.assertIn("`mc-source`", comment_args[5])
        self.assertIn("`ga-root`", comment_args[5])

    def test_run_fix_bugflow_dispatch_treats_duplicate_source_as_dispatched(self) -> None:
        request = {
            "installation_id": "88",
            "repository_owner": "owner",
            "repository_name": "repo",
            "repository_full_name": "owner/repo",
            "issue_number": "42",
            "issue_url": "https://github.com/owner/repo/issues/42",
            "comment_author": "alice",
        }
        create_result = mock.Mock(
            returncode=1,
            stdout="",
            stderr="bugflow-request: open bugflow source bead already exists for https://github.com/owner/repo/issues/42: mc-source\n",
        )
        router_result = mock.Mock(returncode=0, stdout='{"status":"ok","started":[]}\n', stderr="")

        with mock.patch.object(service.common, "repository_permission", return_value="write"), mock.patch.object(
            service.common,
            "create_installation_token",
            return_value="token-123",
        ), mock.patch.object(
            service.common,
            "post_issue_comment",
            return_value={},
        ), mock.patch.object(
            service,
            "run_subprocess",
            side_effect=[create_result, router_result],
        ):
            outcome = service.run_fix_bugflow_dispatch(request, {"app_id": "1", "private_key_pem": "pem"})

        self.assertEqual(outcome["status"], "dispatched")
        self.assertEqual(outcome["reason"], "duplicate_open_bead")
        self.assertEqual(outcome["dispatch_formula"], "mol-bug-report-flow-v2")

    def test_close_failed_bead_updates_and_closes(self) -> None:
        result = mock.Mock(returncode=0)
        with mock.patch.object(service, "run_subprocess", return_value=result) as run_subprocess:
            closed = service.close_failed_bead("bd-1", "dispatch_failed")

        self.assertTrue(closed)
        commands = [call.args[0] for call in run_subprocess.call_args_list]
        prefix = ["gc", "--city", self.tempdir.name, "bd"]
        self.assertEqual(
            commands[0],
            prefix + ["update", "bd-1", "--set-metadata", "close_reason=github:dispatch_failed"],
        )
        self.assertEqual(commands[1], prefix + ["close", "bd-1"])

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
        service.common.save_request(request)
        service.common.save_workflow_link(request["workflow_key"], request["request_id"])

        with mock.patch.object(service.common, "load_config", return_value={"app": {"app_id": "1"}}), mock.patch.object(
            service.common,
            "resolve_repo_mapping",
            return_value=None,
        ), mock.patch.object(
            service,
            "run_fix_bugflow_dispatch",
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
        service.common.save_request(request)
        service.common.save_workflow_link(request["workflow_key"], request["request_id"])

        with mock.patch.object(service.common, "load_config", return_value={"app": {"app_id": "1"}}), mock.patch.object(
            service.common,
            "resolve_repo_mapping",
            return_value=None,
        ), mock.patch.object(
            service,
            "run_fix_bugflow_dispatch",
            return_value={
                "status": "dispatch_failed",
                "reason": "dispatch_failed",
                "bead_id": "bd-2",
                "cleanup_failed": True,
            },
        ):
            service.process_request(request["request_id"])

        self.assertIsNotNone(service.common.load_workflow_link(request["workflow_key"]))

    def test_process_request_closes_existing_bead_on_internal_error(self) -> None:
        request = {
            "request_id": "gh-123-102-fix",
            "workflow_key": "gh:123:issue:45:fix",
            "command": "fix",
            "repository_full_name": "owner/repo",
            "repository_id": "123",
            "issue_number": "45",
            "bead_id": "bd-9",
        }
        service.common.save_request(request)
        service.common.save_workflow_link(request["workflow_key"], request["request_id"])

        with mock.patch.object(service.common, "load_config", return_value={"app": {"app_id": "1"}}), mock.patch.object(
            service.common,
            "resolve_repo_mapping",
            return_value=None,
        ), mock.patch.object(
            service,
            "run_fix_bugflow_dispatch",
            side_effect=RuntimeError("boom"),
        ), mock.patch.object(
            service,
            "close_failed_bead",
            return_value=True,
        ) as close_failed_bead:
            service.process_request(request["request_id"])

        close_failed_bead.assert_called_once_with("bd-9", "internal_error", "")
        self.assertIsNone(service.common.load_workflow_link(request["workflow_key"]))

    def test_process_request_skips_reclosing_bead_already_closed_by_dispatch_failure(self) -> None:
        request = {
            "request_id": "gh-123-103-fix",
            "workflow_key": "gh:123:issue:46:fix",
            "command": "fix",
            "repository_full_name": "owner/repo",
            "repository_id": "123",
            "issue_number": "46",
            "bead_id": "bd-10",
        }
        service.common.save_request(request)
        service.common.save_workflow_link(request["workflow_key"], request["request_id"])

        def dispatch_then_blow_up(current_request: dict[str, object], *_args: object, **_kwargs: object) -> dict[str, object]:
            current_request["bead_closed"] = True
            raise RuntimeError("save failed after cleanup")

        with mock.patch.object(service.common, "load_config", return_value={"app": {"app_id": "1"}}), mock.patch.object(
            service.common,
            "resolve_repo_mapping",
            return_value=None,
        ), mock.patch.object(
            service,
            "run_fix_bugflow_dispatch",
            side_effect=dispatch_then_blow_up,
        ), mock.patch.object(
            service,
            "close_failed_bead",
            return_value=True,
        ) as close_failed_bead:
            service.process_request(request["request_id"])

        close_failed_bead.assert_not_called()
        self.assertIsNone(service.common.load_workflow_link(request["workflow_key"]))

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
        service.common.save_request(request)
        service.common.save_request(newer)
        service.common.save_workflow_link(request["workflow_key"], newer["request_id"])

        with mock.patch.object(service.common, "load_config", return_value={"app": {"app_id": "1"}}), mock.patch.object(
            service.common,
            "resolve_repo_mapping",
            return_value=None,
        ), mock.patch.object(
            service,
            "run_fix_bugflow_dispatch",
            return_value={"status": "dispatch_failed", "reason": "dispatch_failed"},
        ):
            service.process_request(request["request_id"])

        loaded = service.common.load_workflow_link(request["workflow_key"])
        self.assertIsNotNone(loaded)
        assert loaded is not None
        self.assertEqual(loaded["request_id"], newer["request_id"])


class PublishImportedIdentityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self._old_environ = os.environ.copy()
        os.environ["GC_CITY_ROOT"] = self.tempdir.name
        os.environ.pop("GC_BIN", None)  # see GC_BIN note at the top of this file
        os.environ.pop("GITHUB_INTAKE_IDENTITY_PUBLISHER", None)
        os.environ.pop("GITHUB_INTAKE_APP_IDENTITY", None)

    def tearDown(self) -> None:
        os.environ.clear()
        os.environ.update(self._old_environ)

    def test_publish_imported_identity_resolves_settings_from_workspace_env(self) -> None:
        config = {"app": {"app_id": "7", "private_key_pem": "PEM"}}
        with mock.patch.object(
            service,
            "city_workspace_env",
            return_value={
                "GITHUB_INTAKE_IDENTITY_PUBLISHER": "publish-cmd",
                "GITHUB_INTAKE_APP_IDENTITY": "mayor",
            },
        ), mock.patch.object(
            service.common, "publish_identity", return_value={"status": "published", "detail": ""}
        ) as publish:
            result = service.publish_imported_identity(config)

        self.assertEqual(result["status"], "published")
        publish.assert_called_once_with(
            {"app_id": "7", "private_key_pem": "PEM"},
            identity="mayor",
            publisher="publish-cmd",
        )

    def test_publish_imported_identity_prefers_process_env(self) -> None:
        os.environ["GITHUB_INTAKE_IDENTITY_PUBLISHER"] = "env-cmd"
        os.environ["GITHUB_INTAKE_APP_IDENTITY"] = "mayor"
        with mock.patch.object(service, "city_workspace_env", return_value={}), mock.patch.object(
            service.common, "publish_identity", return_value={"status": "published", "detail": ""}
        ) as publish:
            service.publish_imported_identity({"app": {"app_id": "9"}})

        publish.assert_called_once_with({"app_id": "9"}, identity="mayor", publisher="env-cmd")

    def test_manifest_callback_publishes_identity_after_import(self) -> None:
        handler = DummyWebhookHandler(b"", {})
        config = {"app": {"app_id": "7", "slug": "demo-app"}}
        with mock.patch.object(
            service.common, "exchange_manifest_code", return_value={"app_id": "7"}
        ), mock.patch.object(
            service.common, "import_app_config", return_value=config
        ), mock.patch.object(
            service.common, "load_config", return_value={}
        ), mock.patch.object(
            service, "publish_imported_identity", return_value={"status": "published", "detail": ""}
        ) as publish:
            service.IntakeHandler._do_admin_get(
                handler, urllib.parse.urlparse("/v0/github/app/manifest/callback?code=abc")
            )

        self.assertEqual(handler.status, 200)
        publish.assert_called_once_with(config)

    def test_admin_import_post_publishes_identity(self) -> None:
        body = json.dumps({"app_id": "7"}).encode("utf-8")
        handler = DummyWebhookHandler(body, {"Content-Length": str(len(body))})
        handler._read_json_body = service.IntakeHandler._read_json_body.__get__(handler)
        config = {"app": {"app_id": "7"}}
        with mock.patch.object(
            service.common, "import_app_config", return_value=config
        ), mock.patch.object(
            service.common, "load_config", return_value={}
        ), mock.patch.object(
            service, "publish_imported_identity", return_value={"status": "skipped", "reason": "no publisher configured"}
        ) as publish:
            service.IntakeHandler._do_admin_post(
                handler, urllib.parse.urlparse("/v0/github/app/import")
            )

        self.assertEqual(handler.status, 200)
        publish.assert_called_once_with(config)



class RenderAdminHomeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self._old_environ = os.environ.copy()
        os.environ["GC_CITY_ROOT"] = self.tempdir.name
        os.environ.pop("GC_BIN", None)  # see GC_BIN note at the top of this file

    def tearDown(self) -> None:
        os.environ.clear()
        os.environ.update(self._old_environ)

    def test_register_form_offers_organization_target(self) -> None:
        snapshot = {
            "config": {"app": {}},
            "admin_url": "https://admin.example",
            "webhook_url": "https://hook.example",
            "recent_requests": [],
            "recent_address_results": [],
        }
        with mock.patch.object(service.common, "build_status_snapshot", return_value=snapshot), mock.patch.object(
            service.common, "build_manifest", return_value={"name": "demo"}
        ):
            page = service.render_admin_home()

        self.assertIn('action="https://github.com/settings/apps/new"', page)
        self.assertIn('id="org-name"', page)
        self.assertIn('"https://github.com/organizations/" + encodeURIComponent(org) + "/settings/apps/new"', page)

    def test_manifest_failure_renders_warning_not_form(self) -> None:
        snapshot = {
            "config": {"app": {}},
            "admin_url": "",
            "webhook_url": "",
            "recent_requests": [],
            "recent_address_results": [],
        }
        with mock.patch.object(service.common, "build_status_snapshot", return_value=snapshot), mock.patch.object(
            service.common, "build_manifest", side_effect=ValueError("published admin and webhook URLs are required")
        ):
            page = service.render_admin_home()

        self.assertNotIn('id="org-name"', page)
        self.assertIn("published admin and webhook URLs are required", page)



if __name__ == "__main__":
    unittest.main()
