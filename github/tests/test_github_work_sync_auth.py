from __future__ import annotations

import copy
import contextlib
import email.message
import io
import json
import os
import pathlib
import sys
import time
import unittest
import urllib.request
import urllib.response
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "scripts"))
import github_intake_common as common
import github_work_sync as work_sync


class WorkSyncTokenScopeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.permissions = {"metadata": "read", "issues": "write", "organization_projects": "write"}
        self.response = {
            "token": "synthetic-installation-token",
            "expires_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() + 3500)),
            "permissions": dict(self.permissions),
            "repository_selection": "selected",
        }
        self.repositories = {"total_count": 1, "repositories": [{"full_name": "owner/product"}]}
        self.calls = []

    def request(self, method, path, **kwargs):
        self.calls.append((method, path, kwargs))
        if method == "POST":
            return copy.deepcopy(self.response)
        self.assertEqual((method, path), ("GET", "/installation/repositories?per_page=100"))
        return copy.deepcopy(self.repositories)

    def context(self):
        return work_sync.installation_token_context(
            repository_full_name="owner/product",
            token_env="WORK_SYNC_SCOPE_TEST_TOKEN",
            common=common,
            permissions=self.permissions,
        )

    def patches(self, *, mock_http=True):
        stack = contextlib.ExitStack()
        stack.enter_context(mock.patch.dict(os.environ, {}, clear=True))
        stack.enter_context(mock.patch.object(common, "load_rules", return_value={"repos": [
            {"full_name": "owner/product", "installation_id": "123"},
        ]}))
        stack.enter_context(mock.patch.object(common, "github_app_config_for_identity", return_value={"app_id": "1"}))
        stack.enter_context(mock.patch.object(common, "build_app_jwt", return_value="synthetic-jwt"))
        if mock_http:
            stack.enter_context(mock.patch.object(common, "github_api_request", side_effect=self.request))
        return stack

    def test_invalid_api_origin_fails_before_signing_or_network(self):
        for origin in (
            "http://api.example.invalid", "https:///missing-host",
            "https://user:pass@api.example.invalid", "https://api.example.invalid/path",
            "https://api.example.invalid?query=1", "https://api.example.invalid#fragment",
            "https://api.example.invalid?", "https://api.example.invalid#",
            "https://api.example.invalid:bad", "https://api.example.invalid:99999",
            " https://api.example.invalid", "https://api.example.invalid\n",
        ):
            with self.subTest(origin=origin), self.patches(), mock.patch.object(common, "GITHUB_API_BASE", origin):
                with self.assertRaisesRegex((RuntimeError, ValueError), "HTTPS origin"):
                    with self.context():
                        self.fail("invalid API origin was accepted")
                common.build_app_jwt.assert_not_called()
                common.github_api_request.assert_not_called()

    @contextlib.contextmanager
    def offline_http(self, *, redirect_at=0, destination="", status=302):
        """Keep the real urllib opener/redirect chain, replacing only network I/O."""
        requests = []

        def respond(_handler, request):
            requests.append(request)
            headers = email.message.Message()
            code = 200
            if len(requests) == redirect_at:
                headers["Location"] = destination
                code = status
            data = self.response if request.full_url.endswith("access_tokens") else self.repositories
            response = urllib.response.addinfourl(
                io.BytesIO(json.dumps(data).encode()), headers, request.full_url, code,
            )
            response.msg = "Found" if code != 200 else "OK"
            return response

        with mock.patch.object(urllib.request, "_opener", None), \
             mock.patch.object(urllib.request.HTTPSHandler, "https_open", respond), \
             mock.patch.object(urllib.request.HTTPHandler, "http_open", respond):
            yield requests

    def test_scoped_auth_never_follows_redirects_with_credentials(self):
        for redirect_at in (1, 2):
            for destination in (
                "https://other.example.invalid/access_tokens",
                "http://api.example.invalid/access_tokens",
                "https://api.example.invalid/access_tokens",
            ):
                for status in (301, 302, 303, 307, 308):
                    with self.subTest(step=redirect_at, destination=destination, status=status), \
                         self.patches(mock_http=False), \
                         mock.patch.object(common, "GITHUB_API_BASE", "https://api.example.invalid"), \
                         self.offline_http(redirect_at=redirect_at, destination=destination, status=status) as requests:
                        with self.assertRaises(RuntimeError):
                            with self.context():
                                self.fail("redirected token was accepted")
                        self.assertEqual(len(requests), redirect_at, "credential-bearing redirect escaped")
                        self.assertNotIn("WORK_SYNC_SCOPE_TEST_TOKEN", os.environ)

    def test_scoped_auth_uses_real_https_helper_without_redirects(self):
        with self.patches(mock_http=False), \
             mock.patch.object(common, "GITHUB_API_BASE", "https://api.example.invalid:8443/"), \
             self.offline_http() as requests:
            with self.context():
                self.assertEqual(len(requests), 2)
                self.assertEqual(requests[0].full_url, "https://api.example.invalid:8443/app/installations/123/access_tokens")
                self.assertEqual(requests[1].full_url, "https://api.example.invalid:8443/installation/repositories?per_page=100")

    def test_work_sync_transport_rejects_invalid_origin(self):
        for origin in (
            "https://user:pass@api.example.invalid", "https://api.example.invalid?query=1",
            "https://api.example.invalid#fragment", "https://api.example.invalid:bad",
            "https://api.example.invalid?", "https://api.example.invalid#",
        ):
            with self.subTest(origin=origin), self.assertRaisesRegex(ValueError, "HTTPS origin"):
                work_sync.GitHubHTTPTransport(token_env="WORK_SYNC_SCOPE_TEST_TOKEN", api_url=origin)

    def test_work_sync_transport_never_redirects_scoped_token(self):
        for method in ("GET", "POST", "PATCH"):
            for destination in ("https://other.example.invalid/collect", "http://api.example.invalid/collect"):
                with self.subTest(method=method, destination=destination), \
                     self.patches(mock_http=False), \
                     self.offline_http(redirect_at=1, destination=destination) as requests:
                    os.environ["WORK_SYNC_SCOPE_TEST_TOKEN"] = "synthetic-installation-token"
                    transport = work_sync.GitHubHTTPTransport(
                        token_env="WORK_SYNC_SCOPE_TEST_TOKEN", api_url="https://api.example.invalid",
                    )
                    with self.assertRaises(RuntimeError):
                        transport.rest(method, "/repos/owner/product")
                    self.assertEqual(len(requests), 1, "credential-bearing redirect escaped")

    def test_mint_has_exact_scope_and_effective_readback_before_token_use(self):
        with self.patches():
            with self.context():
                self.assertEqual(len(self.calls), 2)
                self.assertEqual(self.calls[0][2].get("payload"), {
                    "repositories": ["product"], "permissions": self.permissions,
                })
                self.assertEqual(os.environ["WORK_SYNC_SCOPE_TEST_TOKEN"], "synthetic-installation-token")
            self.assertNotIn("WORK_SYNC_SCOPE_TEST_TOKEN", os.environ)

    def test_ambient_token_is_not_reused_as_scope_proof(self):
        with self.patches():
            os.environ["WORK_SYNC_SCOPE_TEST_TOKEN"] = "unproven-ambient-token"
            with self.context():
                self.assertEqual(len(self.calls), 2)
                self.assertEqual(os.environ["WORK_SYNC_SCOPE_TEST_TOKEN"], "synthetic-installation-token")
            self.assertEqual(os.environ["WORK_SYNC_SCOPE_TEST_TOKEN"], "unproven-ambient-token")

    def test_malformed_or_broader_token_is_never_exposed(self):
        cases = [
            {"permissions": {**self.permissions, "contents": "write"}},
            {"permissions": {"issues": "read"}},
            {"permissions": None},
            {"token": {"private": "not-a-token"}},
            {"token": ""},
            {"expires_at": "invalid"},
            {"expires_at": "2000-01-01T00:00:00Z"},
            {"expires_at": "2100-01-01T00:00:00Z"},
        ]
        original = copy.deepcopy(self.response)
        for delta in cases:
            with self.subTest(delta=delta), self.patches():
                self.response = {**original, **delta}
                with self.assertRaisesRegex(RuntimeError, "token"):
                    with self.context():
                        self.fail("unverified token was exposed")
                self.assertNotIn("WORK_SYNC_SCOPE_TEST_TOKEN", os.environ)

    def test_wrong_extra_or_incomplete_repository_readback_is_rejected(self):
        for data in (
            {"total_count": 1, "repositories": [{"full_name": "other/product"}]},
            {"total_count": 2, "repositories": [{"full_name": "owner/product"}]},
            {"total_count": 2, "repositories": [{"full_name": "owner/product"}, {"full_name": "owner/other"}]},
            {"total_count": True, "repositories": [{"full_name": "owner/product"}]},
            {},
        ):
            with self.subTest(data=data), self.patches():
                self.repositories = data
                with self.assertRaisesRegex(RuntimeError, "token"):
                    with self.context():
                        self.fail("wrong-scope token was exposed")

    def test_invalid_requested_scope_fails_before_jwt_or_network(self):
        for permissions in (None, {}, {"issues": "admin"}, {"issues": True}):
            with self.subTest(permissions=permissions), self.patches():
                self.permissions = permissions
                with self.assertRaisesRegex(RuntimeError, "token"):
                    with self.context():
                        self.fail("invalid requested scope was accepted")
                common.build_app_jwt.assert_not_called()
                common.github_api_request.assert_not_called()

    def test_exception_restores_ambient_environment_without_leaking_minted_token(self):
        with self.patches():
            os.environ["WORK_SYNC_SCOPE_TEST_TOKEN"] = "unproven-ambient-token"
            with self.assertRaisesRegex(RuntimeError, "consumer failed"):
                with self.context():
                    raise RuntimeError("consumer failed")
            self.assertEqual(os.environ["WORK_SYNC_SCOPE_TEST_TOKEN"], "unproven-ambient-token")


if __name__ == "__main__":
    unittest.main()
