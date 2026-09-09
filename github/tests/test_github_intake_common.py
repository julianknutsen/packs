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
        os.environ["GC_SERVICE_STATE_ROOT"] = os.path.join(self.tempdir.name, ".gc", "services", "github")
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
        self.assertIn("issues", manifest["default_events"])
        self.assertIn("pull_request", manifest["default_events"])
        self.assertEqual(
            manifest["default_permissions"],
            {
                "issues": "write",
                "metadata": "read",
                "pull_requests": "read",
                "organization_projects": "write",
                "issue_types": "read",
                "issue_fields": "read",
            },
        )

    def test_effective_config_merges_github_app_env_secrets(self) -> None:
        os.environ["GITHUB_APP_ID"] = "123"
        os.environ["GITHUB_WEBHOOK_SECRET"] = "env-secret"
        os.environ["GITHUB_APP_PRIVATE_KEY_PEM"] = "pem"
        config = {"schema_version": 1, "app": {"webhook_secret": "state-secret"}, "repositories": {}}

        effective = common.effective_config(config)

        self.assertEqual(effective["app"]["app_id"], "123")
        self.assertEqual(effective["app"]["webhook_secret"], "env-secret")
        self.assertEqual(effective["app"]["private_key_pem"], "pem")

    def test_import_app_config_persists_installation_id(self) -> None:
        config = common.import_app_config(
            common.load_config(),
            {
                "app_id": "123",
                "installation_id": "456",
                "webhook_secret": "secret",
                "private_key_pem": "pem",
            },
        )

        self.assertEqual(config["app"]["app_id"], "123")
        self.assertEqual(config["app"]["installation_id"], "456")

    def test_load_rules_reads_city_owned_toml_and_flattens_match(self) -> None:
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
github_app_token_env = "GH_TOKEN"
""",
            encoding="utf-8",
        )

        rules = common.load_rules()

        self.assertEqual(rules["path"], str(rules_dir / "rules.toml"))
        self.assertEqual(len(rules["rules"]), 1)
        rule = rules["rules"][0]
        self.assertEqual(rule["id"], "pr-review-on-needs-review-label")
        self.assertEqual(rule["match"]["label.name"], "status/needs-review")
        self.assertEqual(rule["match"]["pull_request.state"], "open")
        self.assertFalse(rule["allow_self"])
        self.assertEqual(rule["action"][0]["type"], "order")

    def test_load_rules_reads_repo_scoped_address_config(self) -> None:
        rules_dir = pathlib.Path(self.tempdir.name) / "config" / "github-intake"
        rules_dir.mkdir(parents=True)
        (rules_dir / "rules.toml").write_text(
            """
version = 1

[[repo]]
full_name = "Owner/Repo"
authorized_users = ["Alice", "bob"]
installation_id = "88"

[[repo.address]]
address = "@mayor"
pool = "mayor"
formula = "github-addressed-message"
profile = "mayor"
github_app_identity = "mayor"
installation_id = "99"
ack = true
""",
            encoding="utf-8",
        )

        rules = common.load_rules()

        self.assertEqual(len(rules["repos"]), 1)
        repo = rules["repos"][0]
        self.assertEqual(repo["full_name"], "owner/repo")
        self.assertEqual(repo["rig"], "github-owner-repo")
        self.assertEqual(repo["authorized_users"], ["alice", "bob"])
        self.assertEqual(repo["installation_id"], "88")
        self.assertEqual(repo["addresses"][0]["address"], "@mayor")
        self.assertEqual(repo["addresses"][0]["pool"], "mayor")
        self.assertEqual(repo["addresses"][0]["target"], "github-owner-repo/mayor")
        self.assertEqual(repo["addresses"][0]["formula"], "github-addressed-message")
        self.assertEqual(repo["addresses"][0]["profile"], "mayor")
        self.assertEqual(repo["addresses"][0]["github_app_identity"], "mayor")
        self.assertEqual(repo["addresses"][0]["installation_id"], "99")
        self.assertTrue(repo["addresses"][0]["ack"])

    def test_load_rules_maps_github_repo_to_configured_local_rig(self) -> None:
        rules_dir = pathlib.Path(self.tempdir.name) / "config" / "github-intake"
        rules_dir.mkdir(parents=True)
        (rules_dir / "rules.toml").write_text(
            """
version = 1

[[repo]]
full_name = "Owner/Repo"
city = "product-city"
rig = "product"
authorized_users = ["Alice"]

[[repo.address]]
address = "@mayor"
pool = "mayor"
formula = "github-addressed-message"
""",
            encoding="utf-8",
        )

        rules = common.load_rules()

        repo = rules["repos"][0]
        self.assertEqual(repo["full_name"], "owner/repo")
        self.assertEqual(repo["city"], "product-city")
        self.assertEqual(repo["github_rig"], "github-owner-repo")
        self.assertEqual(repo["rig"], "product")
        self.assertEqual(repo["addresses"][0]["target"], "product/mayor")
        self.assertEqual(common.github_repo_dispatch_rig("Owner/Repo", rules), "product")
        self.assertEqual(
            common.github_repo_dispatch_route("Owner/Repo", rules),
            {"city": "product-city", "rig": "product"},
        )

    def test_github_repo_dispatch_route_requires_exact_city_and_rig_mapping(self) -> None:
        rules = {
            "repos": [
                {"full_name": "owner/missing-city", "city": "", "rig": "product"},
                {"full_name": "owner/missing-rig", "city": "product-city", "rig": ""},
            ]
        }

        self.assertEqual(common.github_repo_dispatch_route("owner/unknown", rules), {})
        self.assertEqual(common.github_repo_dispatch_route("owner/missing-city", rules), {})
        self.assertEqual(common.github_repo_dispatch_route("owner/missing-rig", rules), {})

    def test_load_rules_rejects_path_like_github_app_identity(self) -> None:
        rules_dir = pathlib.Path(self.tempdir.name) / "config" / "github-intake"
        rules_dir.mkdir(parents=True)
        (rules_dir / "rules.toml").write_text(
            """
version = 1

[[repo]]
full_name = "owner/repo"

[[repo.address]]
address = "@mayor"
pool = "mayor"
formula = "github-addressed-message"
github_app_identity = "secret/store/path"
""",
            encoding="utf-8",
        )

        with self.assertRaisesRegex(ValueError, "github_app_identity"):
            common.load_rules()

    def test_github_repo_rig_name_derives_stable_rig_from_org_repo(self) -> None:
        self.assertEqual(common.github_repo_rig_name("Owner/Repo.Name"), "github-owner-repo-name")
        self.assertEqual(common.github_repo_rig_name(" example-org/Gas_City "), "github-example-org-gas_city")

    def test_github_repo_dispatch_rig_falls_back_to_slug_when_unconfigured(self) -> None:
        self.assertEqual(common.github_repo_dispatch_rig("Owner/Repo", {"repos": []}), "github-owner-repo")

    def test_load_rules_rejects_rig_scoped_address_pool(self) -> None:
        rules_dir = pathlib.Path(self.tempdir.name) / "config" / "github-intake"
        rules_dir.mkdir(parents=True)
        (rules_dir / "rules.toml").write_text(
            """
version = 1

[[repo]]
full_name = "owner/repo"
authorized_users = ["alice"]

[[repo.address]]
address = "@mayor"
pool = "other-rig/mayor"
formula = "github-addressed-message"
""",
            encoding="utf-8",
        )

        with self.assertRaisesRegex(ValueError, "pool must not include a rig"):
            common.load_rules()

    def test_load_rules_rejects_invalid_repo_rig_mapping(self) -> None:
        rules_dir = pathlib.Path(self.tempdir.name) / "config" / "github-intake"
        rules_dir.mkdir(parents=True)
        (rules_dir / "rules.toml").write_text(
            """
version = 1

[[repo]]
full_name = "owner/repo"
rig = "other-rig/mayor"
authorized_users = ["alice"]

[[repo.address]]
address = "@mayor"
pool = "mayor"
formula = "github-addressed-message"
""",
            encoding="utf-8",
        )

        with self.assertRaisesRegex(ValueError, "rig must be a local rig name"):
            common.load_rules()

    def test_load_rules_rejects_city_route_without_explicit_rig(self) -> None:
        rules_dir = pathlib.Path(self.tempdir.name) / "config" / "github-intake"
        rules_dir.mkdir(parents=True)
        (rules_dir / "rules.toml").write_text(
            """
version = 1

[[repo]]
full_name = "owner/repo"
city = "product-city"
""",
            encoding="utf-8",
        )

        with self.assertRaisesRegex(ValueError, "city requires an explicit rig"):
            common.load_rules()

    def test_extract_addressed_comment_requests_repo_scoped_matches(self) -> None:
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
                            "ack": True,
                        },
                        {
                            "address": "@deacon",
                            "pool": "deacon",
                            "target": "product/deacon",
                            "formula": "github-addressed-message",
                            "ack": True,
                        },
                    ],
                }
            ]
        }
        payload = {
            "action": "created",
            "installation": {"id": 77},
            "issue": {
                "id": 4242,
                "number": 42,
                "title": "Crash on startup",
                "body": "The app crashes.",
                "html_url": "https://github.com/owner/repo/issues/42",
                "user": {"login": "reporter"},
                "pull_request": {"url": "https://api.github.com/repos/owner/repo/pulls/42"},
            },
            "comment": {
                "id": 99,
                "body": "@mayor @deacon please triage this",
                "html_url": "https://github.com/owner/repo/issues/42#issuecomment-99",
                "user": {"login": "Alice", "type": "User"},
                "created_at": "2026-05-28T12:00:00Z",
                "updated_at": "2026-05-28T12:00:00Z",
            },
            "repository": {
                "id": 123,
                "name": "repo",
                "full_name": "Owner/Repo",
                "default_branch": "main",
                "owner": {"login": "Owner"},
            },
        }

        result = common.extract_addressed_comment_requests(payload, rules)

        self.assertIsNotNone(result)
        assert result is not None
        self.assertTrue(result["authorized"])
        self.assertEqual(result["cleaned_body"], "please triage this")
        self.assertEqual([request["address"] for request in result["requests"]], ["@mayor", "@deacon"])
        self.assertEqual(result["requests"][0]["source_key"], "github-comment:123:99:@mayor")
        self.assertEqual(result["requests"][0]["item_kind"], "pr")
        self.assertEqual(result["requests"][0]["rig"], "product")
        self.assertEqual(result["requests"][0]["pool"], "mayor")
        self.assertEqual(result["requests"][0]["target"], "product/mayor")
        self.assertEqual(result["requests"][0]["profile"], "mayor")

    def test_extract_addressed_comment_requests_ignores_edits_and_unconfigured_mentions(self) -> None:
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
            "action": "edited",
            "issue": {"number": 42, "html_url": "https://github.com/owner/repo/issues/42"},
            "comment": {"id": 99, "body": "@mayor edited request", "user": {"login": "alice"}},
            "repository": {"id": 123, "full_name": "owner/repo"},
        }

        self.assertIsNone(common.extract_addressed_comment_requests(payload, rules))
        payload["action"] = "created"
        payload["comment"]["body"] = "@unknown do work"
        self.assertIsNone(common.extract_addressed_comment_requests(payload, rules))

    def test_rule_can_allow_self_events(self) -> None:
        rules_dir = pathlib.Path(self.tempdir.name) / "config" / "github-intake"
        rules_dir.mkdir(parents=True)
        (rules_dir / "rules.toml").write_text(
            """
version = 1

[[rule]]
id = "triage-on-needs-triage-label"
event = "pull_request"
allow_self = true

[rule.match]
action = "labeled"
label.name = "status/needs-triage"

[[rule.action]]
type = "order"
name = "triage-patrol"
""",
            encoding="utf-8",
        )

        rules = common.load_rules()

        self.assertTrue(rules["rules"][0]["allow_self"])

    def test_matching_rules_uses_exact_dotted_payload_values(self) -> None:
        rules = {
            "rules": [
                {
                    "id": "pr-review",
                    "event": "pull_request",
                    "match": {
                        "action": "labeled",
                        "label.name": "status/needs-review",
                        "pull_request.state": "open",
                    },
                    "action": [{"type": "order", "name": "pr-review-request"}],
                }
            ]
        }
        payload = {
            "action": "labeled",
            "label": {"name": "status/needs-review"},
            "pull_request": {"state": "open"},
        }

        self.assertEqual([rule["id"] for rule in common.matching_rules("pull_request", payload, rules)], ["pr-review"])
        payload["label"]["name"] = "status/reviewing"
        self.assertEqual(common.matching_rules("pull_request", payload, rules), [])

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


class GitHubIntakePublicUrlOverrideTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self._old_environ = os.environ.copy()
        os.environ["GC_CITY_ROOT"] = self.tempdir.name
        for key in (
            "GITHUB_INTAKE_ADMIN_PUBLIC_URL",
            "GITHUB_INTAKE_WEBHOOK_PUBLIC_URL",
            "GITHUB_INTAKE_WEBHOOK_HOOK_URL",
        ):
            os.environ.pop(key, None)

    def tearDown(self) -> None:
        os.environ.clear()
        os.environ.update(self._old_environ)

    def test_admin_and_webhook_env_overrides_win_over_snapshots(self) -> None:
        os.environ["GITHUB_INTAKE_ADMIN_PUBLIC_URL"] = "https://admin.example/svc"
        os.environ["GITHUB_INTAKE_WEBHOOK_PUBLIC_URL"] = "https://hooks.example/base"

        self.assertEqual(common.admin_url(), "https://admin.example/svc")
        self.assertEqual(common.webhook_url(), "https://hooks.example/base")

    def test_overrides_resolve_from_city_workspace_env(self) -> None:
        pathlib.Path(self.tempdir.name, "city.toml").write_text(
            '[workspace]\n[workspace.env]\n'
            'GITHUB_INTAKE_ADMIN_PUBLIC_URL = "https://admin.city/svc"\n'
            'GITHUB_INTAKE_WEBHOOK_HOOK_URL = "https://hooks.city/me/github/webhook"\n',
            encoding="utf-8",
        )

        self.assertEqual(common.admin_url(), "https://admin.city/svc")
        manifest = common.build_manifest()
        self.assertEqual(manifest["hook_attributes"]["url"], "https://hooks.city/me/github/webhook")
        self.assertEqual(
            manifest["redirect_url"],
            "https://admin.city/svc/v0/github/app/manifest/callback",
        )

    def test_hook_url_override_used_verbatim_over_derived_path(self) -> None:
        os.environ["GITHUB_INTAKE_ADMIN_PUBLIC_URL"] = "https://admin.example"
        os.environ["GITHUB_INTAKE_WEBHOOK_PUBLIC_URL"] = "https://hooks.example/base"
        os.environ["GITHUB_INTAKE_WEBHOOK_HOOK_URL"] = "https://edge.example/paxel-city/github/webhook"

        manifest = common.build_manifest()

        self.assertEqual(manifest["hook_attributes"]["url"], "https://edge.example/paxel-city/github/webhook")

    def test_build_manifest_still_requires_some_urls(self) -> None:
        with self.assertRaisesRegex(ValueError, "published admin and webhook URLs"):
            common.build_manifest()


class GitHubIntakePublishIdentityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self._old_environ = os.environ.copy()
        os.environ["GC_CITY_ROOT"] = self.tempdir.name
        os.environ.pop("GITHUB_INTAKE_IDENTITY_PUBLISHER", None)
        os.environ.pop("GITHUB_INTAKE_APP_IDENTITY", None)

    def tearDown(self) -> None:
        os.environ.clear()
        os.environ.update(self._old_environ)

    def _capture_publisher(self) -> tuple[str, pathlib.Path]:
        capture = pathlib.Path(self.tempdir.name) / "captured.json"
        script = pathlib.Path(self.tempdir.name) / "capture_publisher.py"
        script.write_text(
            "import json, os, pathlib, sys\n"
            "out = pathlib.Path(os.environ['PUBLISH_CAPTURE_PATH'])\n"
            "out.write_text(json.dumps({'argv': sys.argv[1:], 'stdin': json.load(sys.stdin)}))\n"
            "print('stored')\n",
            encoding="utf-8",
        )
        os.environ["PUBLISH_CAPTURE_PATH"] = str(capture)
        import sys as _sys

        return f"{_sys.executable} {script}", capture

    def test_publish_identity_skips_when_no_publisher_configured(self) -> None:
        result = common.publish_identity({"app_id": "7"}, identity="mayor")

        self.assertEqual(result["status"], "skipped")
        self.assertIn("no publisher configured", result["reason"])

    def test_publish_identity_skips_when_identity_is_not_configured(self) -> None:
        result = common.publish_identity({"app_id": "7"}, publisher="true")

        self.assertEqual(result["status"], "skipped")
        self.assertIn("GITHUB_INTAKE_APP_IDENTITY", result["reason"])

    def test_publish_identity_invokes_publisher_with_identity_arg_and_json_stdin(self) -> None:
        publisher, capture = self._capture_publisher()

        result = common.publish_identity(
            {"app_id": "7", "private_key_pem": "PEM\nLINE2", "webhook_secret": "s", "empty": ""},
            identity="mayor",
            publisher=publisher,
        )

        self.assertEqual(result["status"], "published")
        recorded = json.loads(capture.read_text(encoding="utf-8"))
        self.assertEqual(recorded["argv"], ["mayor"])
        self.assertEqual(recorded["stdin"]["app_id"], "7")
        self.assertEqual(recorded["stdin"]["private_key_pem"], "PEM\nLINE2")
        self.assertEqual(
            recorded["stdin"]["schema_version"], common.GITHUB_APP_IDENTITY_SCHEMA_VERSION
        )
        self.assertNotIn("empty", recorded["stdin"])

    def test_publish_identity_reads_identity_and_publisher_from_environment(self) -> None:
        publisher, capture = self._capture_publisher()
        os.environ["GITHUB_INTAKE_IDENTITY_PUBLISHER"] = publisher
        os.environ["GITHUB_INTAKE_APP_IDENTITY"] = "mayor"

        result = common.publish_identity({"app_id": "9"})

        self.assertEqual(result["status"], "published")
        recorded = json.loads(capture.read_text(encoding="utf-8"))
        self.assertEqual(recorded["argv"], ["mayor"])

    def test_publish_identity_reports_publisher_failure_without_raising(self) -> None:
        script = pathlib.Path(self.tempdir.name) / "failing_publisher.py"
        script.write_text(
            "import sys\nprint('store unavailable', file=sys.stderr)\nsys.exit(3)\n",
            encoding="utf-8",
        )
        import sys as _sys

        result = common.publish_identity(
            {"app_id": "7"}, identity="mayor", publisher=f"{_sys.executable} {script}"
        )

        self.assertEqual(result["status"], "error")
        self.assertIn("store unavailable", result["detail"])

    def test_publish_identity_rejects_invalid_identity_without_raising(self) -> None:
        result = common.publish_identity({"app_id": "7"}, identity="../escape", publisher="true")

        self.assertEqual(result["status"], "error")
        self.assertIn("must match", result["detail"])



if __name__ == "__main__":
    unittest.main()
