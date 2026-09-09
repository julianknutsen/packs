from __future__ import annotations

import json
import hashlib
import contextlib
import io
import os
import pathlib
import subprocess
import stat
import sys
import tempfile
import unittest
import urllib.error
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "scripts"))

import github_work_sync as work_sync


def operation(direction: str, kind: str, field: str, value: object, revision: int = 5) -> dict[str, object]:
    return {
        "direction": direction,
        "kind": kind,
        "field": field,
        "value": value,
        "expected_revision": revision,
    }


def execution_plan(
    *operations: dict[str, object],
    status: str = "green",
    execution_safe: bool | None = None,
) -> dict[str, object]:
    if execution_safe is None:
        execution_safe = status == "green"
    return {
        "status": status,
        "execution_safe": execution_safe,
        "counts": {
            "beads": 1,
            "issues": 1,
            "plans": 1,
            "operations": len(operations),
            "duplicates": 0,
            "orphans": 0,
        },
        "reason_codes": [],
        "plans": [
            {
                "state": status,
                "execution_safe": execution_safe,
                "reason_codes": [],
                "identity": {
                    "bead_id": "ga-1",
                    "repository": "product",
                    "repository_id": 123,
                    "issue_node_id": "I_1",
                    "issue_number": 42,
                    "project_node_id": "P_1",
                    "project_item_id": "PI_1",
                },
                "github_precondition": {
                    "github_updated_at": "2026-09-02T10:00:00Z",
                    "project_field_hash": "b" * 64,
                    "projection_hash": "a" * 64,
                },
                "beads_convergence_base": {
                    "bead_revision": 5,
                    "github_updated_at": "2026-09-02T10:00:00Z",
                    "project_field_hash": "b" * 64,
                    "projection_hash": "a" * 64,
                    "delivery_id": "delivery-1",
                },
                "operations": list(operations),
            }
        ],
    }


def converged_execution_plan() -> dict[str, object]:
    return execution_plan()


class FakeCAS:
    def __init__(self, outcome: str = "updated") -> None:
        self.outcome = outcome
        self.calls: list[dict[str, object]] = []

    def apply(self, bead_id: str, expected_revision: int, patch: dict[str, object]) -> dict[str, object]:
        self.calls.append(
            {
                "bead_id": bead_id,
                "expected_revision": expected_revision,
                "patch": patch,
            }
        )
        result: dict[str, object] = {"outcome": self.outcome, "revision": 6}
        comments = patch.get("comments")
        if isinstance(comments, list):
            result["imported_comment_ids"] = [
                item["external_id"] for item in comments if isinstance(item, dict)
            ]
        return result


class FakeGitHub:
    def __init__(self) -> None:
        self.apply_calls: list[dict[str, object]] = []
        self.readback_calls: list[dict[str, object]] = []
        self.failures: list[BaseException] = []
        self.readback_result: bool | None = True
        self.binding_calls: list[dict[str, object]] = []
        self.preflight_calls: list[dict[str, object]] = []
        self.preflight_result: bool | None = True
        self.imported_comment_ids: list[str] = []

    def preflight(
        self,
        identity: dict[str, object],
        precondition: dict[str, object],
    ) -> bool | None:
        self.preflight_calls.append({
            "identity": dict(identity),
            "precondition": dict(precondition),
        })
        return self.preflight_result

    def apply(self, identity: dict[str, object], item: dict[str, object]) -> None:
        self.apply_calls.append(item)
        if self.failures:
            raise self.failures.pop(0)

    def readback(self, identity: dict[str, object], item: dict[str, object]) -> bool | None:
        self.readback_calls.append(item)
        return self.readback_result

    def binding(self, identity: dict[str, object]) -> dict[str, object]:
        self.binding_calls.append(dict(identity))
        return {
            "external_ref": "https://github.com/owner/product/issues/42",
            "repository_id": 123,
            "issue_node_id": "I_1",
            "issue_number": 42,
            "project_node_id": "P_1",
            "project_item_id": "PI_1",
            "github_updated_at": "2026-09-02T10:00:00Z",
            "project_field_hash": "b" * 64,
            "projection_hash": "a" * 64,
            "delivery_id": "delivery-1",
            "imported_comment_ids": list(self.imported_comment_ids),
        }

    def set_imported_comment_ids(
        self, identity: dict[str, object], imported_comment_ids: list[str]
    ) -> None:
        self.imported_comment_ids = list(imported_comment_ids)

    def pending_write(self, identity: dict[str, object], item: dict[str, object]) -> dict[str, object]:
        return {"kind": "github-write", "before_hash": "c" * 64, "after_hash": "d" * 64}


class FakeReceiptStore:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []
        self.pending: list[dict[str, object]] = []
        self.creation_pending: list[dict[str, object]] = []

    def begin(self, identity: dict[str, object], bead_revision: int, binding: dict[str, object], pending: dict[str, object]) -> None:
        self.pending.append(dict(pending))

    def begin_creation(self, identity: dict[str, object], bead_revision: int, pending: dict[str, object]) -> None:
        self.creation_pending.append(dict(pending))

    def begin_binding(self, identity: dict[str, object], bead_revision: int, binding: dict[str, object], pending: dict[str, object]) -> None:
        self.pending.append(dict(pending))

    def save(
        self,
        identity: dict[str, object],
        bead_revision: int,
        binding: dict[str, object],
        *,
        applied_bead_revision: int | None = None,
    ) -> dict[str, object]:
        call = {
            "identity": dict(identity),
            "bead_revision": bead_revision,
            "binding": dict(binding),
        }
        if applied_bead_revision is not None:
            call["applied_bead_revision"] = applied_bead_revision
        self.calls.append(call)
        return call


class SequencedReader:
    def __init__(self, values: list[object]) -> None:
        self.values = list(values)
        self.calls = 0

    def read(self) -> object:
        self.calls += 1
        if not self.values:
            raise AssertionError("unexpected reader call")
        return self.values.pop(0)


class SequencedGitHubReader:
    def __init__(self, values: list[object]) -> None:
        self.values = list(values)
        self.calls: list[dict[str, object]] = []

    def reconciliation_records(self, **kwargs: object) -> object:
        self.calls.append(dict(kwargs))
        if not self.values:
            raise AssertionError("unexpected GitHub reader call")
        return self.values.pop(0)


class SequencedPlanner:
    def __init__(self, values: list[dict[str, object]]) -> None:
        self.values = list(values)
        self.calls: list[dict[str, object]] = []

    def plan(self, snapshots: dict[str, object], **kwargs: object) -> dict[str, object]:
        self.calls.append({"snapshots": snapshots, **kwargs})
        if not self.values:
            raise AssertionError("unexpected planner call")
        return self.values.pop(0)


class FakeHTTPResponse:
    def __init__(self, payload: object, status: int = 200) -> None:
        self.payload = json.dumps(payload).encode("utf-8")
        self.status = status

    def __enter__(self) -> "FakeHTTPResponse":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self) -> bytes:
        return self.payload


def managed_projection_body(bead_id: str = "ga-1") -> tuple[str, str]:
    machine = {
        "acceptance": "acceptance",
        "status": "Todo",
        "priority": "High",
        "bead_type": "feature",
        "issue_type": "Feature",
        "dependencies": [],
        "lifecycle_phase": "development",
        "evidence": {},
    }
    encoded = json.dumps(
        machine,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    projection_hash = hashlib.sha256(encoded).hexdigest()
    managed = {"bead_id": bead_id, **machine, "projection_hash": projection_hash}
    body = "human\n<!-- opsime-space:managed:v2:start -->\n{0}\n<!-- opsime-space:managed:v2:end -->".format(
        json.dumps(managed, ensure_ascii=False, sort_keys=True, indent=2)
    )
    return body, projection_hash


def canonical_runtime_contract() -> dict[str, object]:
    options = {
        "Status": ["Todo", "Done"],
        "Priority": ["High"],
        "Bead type": ["feature"],
        "Lifecycle phase": ["development"],
        "City": ["product-city"],
        "Risk tier": ["standard"],
        "Delivery profile": ["product"],
        "Data class": ["internal"],
    }
    return {
        "schema_version": "1",
        "organization": "opsime-space",
        "token_permissions": {"metadata": "read", "issues": "write", "organization_projects": "write"},
        "route": {
            "repository": "product",
            "rig": "product",
            "city": "product-city",
            "owning_project": "Product",
            "risk_tier": "standard",
            "delivery_profile": "product",
            "data_class": "internal",
        },
        "projects": {
            "Product": {
                "static_fields": {
                    "City": "product-city",
                    "Risk tier": "standard",
                    "Delivery profile": "product",
                    "Data class": "internal",
                }
            },
            "opsime-space": {
                "static_fields": {
                    "City": "product-city",
                    "Risk tier": "standard",
                    "Delivery profile": "product",
                    "Data class": "internal",
                }
            },
        },
        "project_schema": {
            "required_fields": [
                "Status", "Priority", "Bead ID", "Bead type",
                "Lifecycle phase", "City", "Risk tier",
                "Delivery profile", "Data class",
            ],
            "single_select_options": options,
        },
        "managed_block": {
            "schema_version": 2,
            "start_marker": "<!-- opsime-space:managed:v2:start -->",
            "end_marker": "<!-- opsime-space:managed:v2:end -->",
            "fields": [
                "bead_id", "acceptance", "status", "priority",
                "bead_type", "issue_type", "dependencies",
                "lifecycle_phase", "evidence", "projection_hash",
            ],
        },
        "projection_writer": "gascity-github-intake",
        "live_mutations": False,
        "cross_city_project": "opsime-space",
        "cross_city_bead_types": ["epic"],
        "max_pages_per_run": 100,
        "max_identical_attempts": 2,
    }


def resolved_project_routes() -> dict[str, object]:
    field_ids = {
        "status": "F_STATUS",
        "priority": "F_PRIORITY",
        "bead_id": "F_BEAD_ID",
        "bead_type": "F_BEAD_TYPE",
        "lifecycle_phase": "F_PHASE",
        "city": "F_CITY",
        "risk_tier": "F_RISK",
        "delivery_profile": "F_DELIVERY",
        "data_class": "F_DATA",
    }
    option_ids = {
        "status": {"Todo": "O_TODO", "Done": "O_DONE"},
        "priority": {"High": "O_HIGH"},
        "bead_type": {"feature": "O_FEATURE", "epic": "O_EPIC"},
        "lifecycle_phase": {"development": "O_DEVELOPMENT"},
        "city": {"product-city": "O_CITY"},
        "risk_tier": {"standard": "O_STANDARD"},
        "delivery_profile": {"product": "O_PRODUCT"},
        "data_class": {"internal": "O_INTERNAL"},
    }
    static = {
        "city": "product-city",
        "risk_tier": "standard",
        "delivery_profile": "product",
        "data_class": "internal",
    }
    return {
        "Product": {
            "project_node_id": "P_1",
            "field_ids": dict(field_ids),
            "option_ids": json.loads(json.dumps(option_ids)),
            "static_fields": dict(static),
        },
        "opsime-space": {
            "project_node_id": "P_CROSS",
            "field_ids": {key: value + "_CROSS" for key, value in field_ids.items()},
            "option_ids": json.loads(json.dumps(option_ids)),
            "static_fields": dict(static),
        },
    }


class FakeProjectionTransport:
    def __init__(self) -> None:
        self.rest_calls: list[tuple[str, str, object]] = []
        self.graphql_calls: list[tuple[str, dict[str, object]]] = []

    def rest(self, method: str, path: str, payload: object = None) -> object:
        self.rest_calls.append((method, path, payload))
        if method == "POST" and path.endswith("/issues"):
            return {
                "id": 456,
                "node_id": "I_1",
                "number": 42,
                "html_url": "https://github.com/opsime-space/product/issues/42",
                "updated_at": "2026-09-02T10:00:00Z",
            }
        return {"node_id": "I_1", "number": 42}

    def graphql(self, query: str, variables: dict[str, object]) -> object:
        self.graphql_calls.append((query, variables))
        if "addProjectV2ItemById" in query:
            return {"data": {"addProjectV2ItemById": {"item": {"id": "PI_1"}}}}
        if "updateProjectV2ItemFieldValue" in query:
            return {"data": {"updateProjectV2ItemFieldValue": {"projectV2Item": {"id": "PI_1"}}}}
        if "unarchiveProjectV2Item" in query:
            return {"data": {"unarchiveProjectV2Item": {"item": {"id": "PI_1"}}}}
        raise AssertionError(query)


class FakeEffectiveSnapshotReader:
    def __init__(self, body: str) -> None:
        self.body = body
        self.calls: list[dict[str, object]] = []
        self.absent = True

    def projection_candidates(self) -> list[dict[str, object]]:
        self.calls.append({"projection_candidates": True})
        if self.absent:
            return []
        return [{"node_id": "I_existing", "number": 99, "body": self.body}]

    def read_bound(self, **kwargs: object) -> dict[str, object]:
        self.calls.append(dict(kwargs))
        return {
            "repository": {
                "id": 123,
                "full_name": "opsime-space/product",
            },
            "issue": {
                "node_id": "I_1",
                "number": 42,
                "updated_at": "2026-09-02T10:00:00Z",
                "state": "open",
                "title": "private title",
                "body": self.body,
            },
            "comments": [],
            "project": {
                "project_node_id": "P_1",
                "project_item_id": "PI_1",
                "archived": False,
                "fields": {
                    "status": "Todo",
                    "priority": "High",
                    "bead_type": "feature",
                    "issue_type": "Feature",
                    "lifecycle_phase": "development",
                },
            },
            "event": {
                "delivery_id": "delivery-1",
                "origin": "github-human",
            },
            "projection_actor_node_id": "BOT_1",
        }


class FakeProjectSchemaTransport:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.nodes: list[dict[str, object]] = [{
            "id": "P_1",
            "title": "Product",
            "fields": {
                "nodes": [
                    {"id": "F_STATUS", "name": "Status", "options": [
                        {"id": "O_TODO", "name": "Todo"},
                        {"id": "O_DONE", "name": "Done"},
                    ]},
                    {"id": "F_PRIORITY", "name": "Priority", "options": [
                        {"id": "O_HIGH", "name": "High"},
                    ]},
                    {"id": "F_BEAD_TYPE", "name": "Bead type", "options": [
                        {"id": "O_FEATURE", "name": "feature"},
                    ]},
                    {"id": "F_PHASE", "name": "Lifecycle phase", "options": [
                        {"id": "O_DEVELOPMENT", "name": "development"},
                    ]},
                ]
            },
        }]

    def graphql(self, query: str, variables: dict[str, object]) -> object:
        self.calls.append((query, variables))
        return {
            "data": {
                "organization": {
                    "projectsV2": {
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                        "nodes": self.nodes,
                    }
                }
            }
        }


class GitHubWorkSyncRuntimeTests(unittest.TestCase):
    def test_runtime_uses_canonical_live_switch_and_explicit_dry_run(self) -> None:
        for live, requested_dry_run, expected_dry_run in (
            (False, False, True),
            (False, True, True),
            (True, False, False),
            (True, True, True),
        ):
            with self.subTest(live=live, dry_run=requested_dry_run), contextlib.ExitStack() as stack:
                contract = canonical_runtime_contract()
                contract["live_mutations"] = live
                stack.enter_context(mock.patch.dict(os.environ, {
                    "GC_RIG": "product",
                    "GC_CITY_PATH": "/city/product-city",
                    "GC_SERVICE_STATE_ROOT": "/city/ingress/.gc/services/github",
                    "GC_WORK_SYNC_POLICY_BIN": "/opt/bin/agent-platform",
                    "GC_WORK_SYNC_POLICY_ROOT": "/opt/agent-platform",
                    "GC_GITHUB_WORK_SYNC_TOKEN": "synthetic-test-token",
                }, clear=True))
                stack.enter_context(mock.patch.object(
                    work_sync.ContractRunner, "read", return_value=contract,
                ))
                auth = stack.enter_context(mock.patch.object(
                    work_sync, "installation_token_context", return_value=contextlib.nullcontext(),
                ))
                stack.enter_context(mock.patch.object(
                    work_sync, "runtime_event",
                    return_value=({"delivery_id": "delivery-1", "origin": "github-human"}, "BOT_1"),
                ))
                stack.enter_context(mock.patch.object(
                    work_sync.GitHubProjectSchemaReader, "read_contract",
                    return_value=resolved_project_routes(),
                ))
                run = stack.enter_context(mock.patch.object(
                    work_sync.WorkSyncReconciler, "run", return_value={"status": "dry_run"},
                ))
                receipts = stack.enter_context(mock.patch.object(
                    work_sync, "WorkSyncReceiptStore", wraps=work_sync.WorkSyncReceiptStore,
                ))

                work_sync.execute_runtime_from_environment(dry_run=requested_dry_run)

                self.assertEqual(auth.call_args.kwargs["permissions"], contract["token_permissions"])
                run.assert_called_once_with(dry_run=expected_dry_run)
                receipts.assert_called_once_with(
                    repository="product", store_ref="rig:product",
                    data_root="/city/product-city/.gc/services/github/data",
                )

    def test_runtime_event_classifies_projection_actor_by_node_id_not_login(self) -> None:
        contract = canonical_runtime_contract()
        with tempfile.TemporaryDirectory() as tempdir:
            payload = pathlib.Path(tempdir) / "payload.json"
            payload.write_text(json.dumps({
                "repository": {"full_name": "opsime-space/product"},
                "sender": {"node_id": "BOT_1", "login": "renamed-bot"},
            }), encoding="utf-8")
            transport = mock.Mock()
            transport.graphql.return_value = {
                "data": {"viewer": {"id": "BOT_1"}}
            }

            event, actor = work_sync.runtime_event(
                {
                    "repository_full_name": "opsime-space/product",
                    "delivery_id": "delivery-1",
                    "payload_file": str(payload),
                },
                contract,
                transport,
            )

        self.assertEqual(actor, "BOT_1")
        self.assertEqual(event, {
            "delivery_id": "delivery-1",
            "origin": "gascity-github-intake",
        })
        self.assertIn("WorkSyncViewer", transport.graphql.call_args.args[0])

    def test_reconciler_duplicate_delivery_fails_closed_when_receipt_base_changed(self) -> None:
        contract = canonical_runtime_contract()
        contract["live_mutations"] = True
        receipt = {
            "delivery_id": "delivery-1",
            "bead_revision": 5,
            "github_updated_at": "2026-09-02T10:00:00Z",
            "project_field_hash": "b" * 64,
            "projection_hash": "a" * 64,
        }
        beads = [{"issue": {"id": "ga-1", "revision": 5}, "receipt": receipt}]
        issue = {
            "issue": {"body": managed_projection_body()[0]},
            "project": {},
            "event": {"delivery_id": "delivery-1", "origin": "github-human"},
        }
        writer = mock.Mock()
        writer._snapshot_fingerprint.return_value = {
            "github_updated_at": "2026-09-02T10:01:00Z",
            "project_field_hash": "b" * 64,
            "projection_hash": "a" * 64,
        }

        with self.assertRaisesRegex(RuntimeError, "replay conflict"):
            work_sync.WorkSyncReconciler(
                contract=contract,
                projects=resolved_project_routes(),
                event={"delivery_id": "delivery-1", "origin": "github-human"},
                beads_reader=SequencedReader([beads]),
                github_reader=SequencedGitHubReader([[issue]]),
                planner=SequencedPlanner([converged_execution_plan()]),
                beads_cas=FakeCAS(),
                github_writer=writer,
                receipt_store=FakeReceiptStore(),
                policy_bin="/opt/bin/agent-platform",
                policy_root="/opt/agent-platform",
            ).run()

    def test_main_emits_only_content_free_runtime_result(self) -> None:
        stdout = io.StringIO()
        with mock.patch.object(
            work_sync,
            "execute_runtime_from_environment",
            return_value={
                "status": "green",
                "terminal_readback": True,
                "attempts": 1,
                "counts": {"beads": 3, "issues": 3, "operations": 0},
            },
        ), contextlib.redirect_stdout(stdout):
            code = work_sync.main([])

        self.assertEqual(code, 0)
        self.assertEqual(json.loads(stdout.getvalue())["status"], "green")
        self.assertNotIn("body", stdout.getvalue())
    def test_runtime_environment_requires_exact_city_rig_repository_and_platform_paths(self) -> None:
        environment = work_sync.runtime_environment({
            "GC_RIG": "product",
            "GC_CITY_PATH": "/city/product-city",
            "GC_WORK_SYNC_POLICY_BIN": "/opt/bin/agent-platform",
            "GC_WORK_SYNC_POLICY_ROOT": "/opt/agent-platform",
            "GC_GITHUB_REPO": "opsime-space/product",
            "GC_GITHUB_DELIVERY_ID": "delivery-1",
        }, canonical_runtime_contract())

        self.assertEqual(environment["rig"], "product")
        self.assertEqual(environment["repository"], "product")
        self.assertEqual(environment["city"], "/city/product-city")
        self.assertEqual(environment["delivery_id"], "delivery-1")

        with self.assertRaisesRegex(RuntimeError, "route"):
            work_sync.runtime_environment({
                "GC_RIG": "other",
                "GC_CITY_PATH": "/city/product-city",
                "GC_WORK_SYNC_POLICY_BIN": "/opt/bin/agent-platform",
                "GC_WORK_SYNC_POLICY_ROOT": "/opt/agent-platform",
                "GC_GITHUB_REPO": "opsime-space/other",
            }, canonical_runtime_contract())

    def test_installation_token_context_remints_and_restores_the_caller_environment(self) -> None:
        token_env = "WORK_SYNC_TEST_TOKEN"
        old = os.environ.pop(token_env, None)
        self.addCleanup(
            lambda: os.environ.__setitem__(token_env, old)
            if old is not None
            else os.environ.pop(token_env, None)
        )
        common = mock.Mock()
        common.load_rules.return_value = {"repos": [{
            "full_name": "opsime-space/product",
            "installation_id": "123",
        }]}
        common.github_app_config_for_identity.return_value = {"app_id": "1"}
        common.create_installation_token.return_value = "minted-token"
        permissions = {"metadata": "read", "issues": "write"}

        with work_sync.installation_token_context(
            repository_full_name="opsime-space/product",
            token_env=token_env,
            common=common,
            permissions=permissions,
        ):
            self.assertEqual(os.environ[token_env], "minted-token")
        self.assertNotIn(token_env, os.environ)
        common.create_installation_token.assert_called_once_with(
            {"app_id": "1"}, "123",
            repository_full_name="opsime-space/product", permissions=permissions,
        )

        os.environ[token_env] = "injected-token"
        common.reset_mock()
        with work_sync.installation_token_context(
            repository_full_name="opsime-space/product",
            token_env=token_env,
            common=common,
            permissions=permissions,
        ):
            self.assertEqual(os.environ[token_env], "minted-token")
        self.assertEqual(os.environ[token_env], "injected-token")
        common.create_installation_token.assert_called_once_with(
            {"app_id": "1"}, "123",
            repository_full_name="opsime-space/product", permissions=permissions,
        )

    def test_work_sync_uses_existing_rig_scoped_order_engine(self) -> None:
        root = pathlib.Path(__file__).resolve().parents[1]
        self.assertFalse((root / "orders" / "work-sync.toml").exists())
        order = (root / "work-sync" / "orders" / "work-sync.toml").read_text(encoding="utf-8")
        script = (root / "work-sync" / "orders" / "scripts" / "work-sync.sh").read_text(
            encoding="utf-8"
        )

        self.assertIn('trigger = "cooldown"', order)
        self.assertIn('interval = "5m"', order)
        self.assertIn('"$ORDER_DIR/scripts/work-sync.sh"', order)
        self.assertNotIn('scope = "city"', order)
        self.assertIn('python3 "$pack_root/scripts/github_work_sync.py"', script)
        self.assertNotIn("cron", order.lower())
    def test_reconciler_requires_terminal_zero_operation_readback(self) -> None:
        contract = canonical_runtime_contract()
        contract["live_mutations"] = True
        beads = SequencedReader([[{"issue": {"id": "ga-1"}}], [{"issue": {"id": "ga-1"}}]])
        github = SequencedGitHubReader([[], []])
        planned = execution_plan(operation(
            "beads-to-github",
            "replace-managed-block",
            "projection",
            {"body": "private", "projection_hash": "a" * 64},
        ))
        planner = SequencedPlanner([planned, converged_execution_plan()])
        apply_calls: list[dict[str, object]] = []

        result = work_sync.WorkSyncReconciler(
            contract=contract,
            projects=resolved_project_routes(),
            event={"delivery_id": "delivery-1", "origin": "github-human"},
            beads_reader=beads,
            github_reader=github,
            planner=planner,
            beads_cas=FakeCAS(),
            github_writer=FakeGitHub(),
            receipt_store=FakeReceiptStore(),
            policy_bin="/opt/bin/agent-platform",
            policy_root="/opt/agent-platform",
            apply_plan=lambda plan, *_args, **_kwargs: (
                apply_calls.append(plan) or {"status": "green", "applied_operations": 1}
            ),
        ).run()

        self.assertEqual(result["status"], "green")
        self.assertIs(result["terminal_readback"], True)
        self.assertEqual(len(apply_calls), 1)
        self.assertEqual(beads.calls, 2)
        self.assertEqual(len(github.calls), 2)
        self.assertEqual(len(planner.calls), 2)

    def test_reconciler_bounds_bidirectional_replan_to_contract_attempts(self) -> None:
        contract = canonical_runtime_contract()
        contract["live_mutations"] = True
        contract["max_identical_attempts"] = 2
        pull_then_push = execution_plan(
            operation("github-to-beads", "update-field", "title", "human"),
            operation(
                "beads-to-github", "replace-managed-block", "projection",
                {"body": "private", "projection_hash": "a" * 64},
            ),
        )
        push = execution_plan(operation(
            "beads-to-github", "replace-managed-block", "projection",
            {"body": "private", "projection_hash": "a" * 64},
        ))
        planner = SequencedPlanner([pull_then_push, push, converged_execution_plan()])
        outcomes = [
            {"status": "replan_required", "reason": "beads_revision_advanced"},
            {"status": "green", "applied_operations": 1},
        ]

        result = work_sync.WorkSyncReconciler(
            contract=contract,
            projects=resolved_project_routes(),
            event={"delivery_id": "delivery-1", "origin": "github-human"},
            beads_reader=SequencedReader([[], [], []]),
            github_reader=SequencedGitHubReader([[], [], []]),
            planner=planner,
            beads_cas=FakeCAS(),
            github_writer=FakeGitHub(),
            receipt_store=FakeReceiptStore(),
            policy_bin="/opt/bin/agent-platform",
            policy_root="/opt/agent-platform",
            apply_plan=lambda *_args, **_kwargs: outcomes.pop(0),
        ).run()

        self.assertEqual(result["status"], "green")
        self.assertEqual(result["attempts"], 2)
        self.assertEqual(len(planner.calls), 3)

    def test_reconciler_refuses_mutation_while_canonical_live_switch_is_false(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "live mutations"):
            work_sync.WorkSyncReconciler(
                contract=canonical_runtime_contract(),
                projects=resolved_project_routes(),
                event={"delivery_id": "delivery-1", "origin": "github-human"},
                beads_reader=SequencedReader([[]]),
                github_reader=SequencedGitHubReader([[]]),
                planner=SequencedPlanner([converged_execution_plan()]),
                beads_cas=FakeCAS(),
                github_writer=FakeGitHub(),
                receipt_store=FakeReceiptStore(),
                policy_bin="/opt/bin/agent-platform",
                policy_root="/opt/agent-platform",
            ).run()

    def test_reconciler_allows_canonical_false_only_for_dry_run(self) -> None:
        planned = execution_plan(operation(
            "beads-to-github", "replace-managed-block", "projection",
            {"body": "private", "projection_hash": "a" * 64},
        ))
        calls: list[bool] = []
        result = work_sync.WorkSyncReconciler(
            contract=canonical_runtime_contract(),
            projects=resolved_project_routes(),
            event={"delivery_id": "delivery-1", "origin": "github-human"},
            beads_reader=SequencedReader([[]]),
            github_reader=SequencedGitHubReader([[]]),
            planner=SequencedPlanner([planned]),
            beads_cas=FakeCAS(),
            github_writer=FakeGitHub(),
            receipt_store=FakeReceiptStore(),
            policy_bin="/opt/bin/agent-platform",
            policy_root="/opt/agent-platform",
            apply_plan=lambda _plan, *_args, **kwargs: (
                calls.append(bool(kwargs.get("dry_run"))) or {"status": "dry_run"}
            ),
        ).run(dry_run=True)

        self.assertEqual(result["status"], "dry_run")
        self.assertEqual(calls, [True])
    def test_contract_runner_uses_private_output_and_exact_repository(self) -> None:
        observed: dict[str, object] = {}

        def run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
            observed["command"] = command
            output = command[command.index("--output") + 1]
            observed["output_parent_mode"] = os.stat(os.path.dirname(output)).st_mode & 0o777
            pathlib.Path(output).write_text(
                json.dumps(canonical_runtime_contract()),
                encoding="utf-8",
            )
            os.chmod(output, 0o600)
            return subprocess.CompletedProcess(command, 0, '{"status":"ok"}\n', "")

        result = work_sync.ContractRunner(run=run).read(
            repository="product",
            policy_bin="/opt/bin/agent-platform",
            policy_root="/opt/agent-platform",
        )

        self.assertEqual(result["route"]["repository"], "product")
        self.assertEqual(observed["output_parent_mode"], 0o700)
        self.assertEqual(
            observed["command"],
            [
                "/opt/bin/agent-platform", "--json", "work-sync",
                "runtime-contract", "--repository", "product",
                "--output", observed["command"][7],
                "--platform-root", "/opt/agent-platform",
            ],
        )

    def test_project_schema_reader_resolves_full_contract_including_text_and_static_fields(self) -> None:
        transport = FakeProjectSchemaTransport()
        transport.nodes[0]["fields"]["nodes"].extend([
            {"id": "F_BEAD_ID", "name": "Bead ID", "dataType": "TEXT"},
            {"id": "F_CITY", "name": "City", "dataType": "SINGLE_SELECT", "options": [
                {"id": "O_CITY", "name": "product-city"},
            ]},
            {"id": "F_RISK", "name": "Risk tier", "dataType": "SINGLE_SELECT", "options": [
                {"id": "O_STANDARD", "name": "standard"},
            ]},
            {"id": "F_DELIVERY", "name": "Delivery profile", "dataType": "SINGLE_SELECT", "options": [
                {"id": "O_PRODUCT", "name": "product"},
            ]},
            {"id": "F_DATA", "name": "Data class", "dataType": "SINGLE_SELECT", "options": [
                {"id": "O_INTERNAL", "name": "internal"},
            ]},
        ])
        for node in transport.nodes[0]["fields"]["nodes"]:
            node.setdefault("dataType", "SINGLE_SELECT")
        cross = json.loads(json.dumps(transport.nodes[0]))
        cross["id"] = "P_CROSS"
        cross["title"] = "opsime-space"
        transport.nodes.append(cross)

        result = work_sync.GitHubProjectSchemaReader(
            organization="opsime-space",
            transport=transport,
        ).read_contract(canonical_runtime_contract())

        product = result["Product"]
        self.assertEqual(product["project_node_id"], "P_1")
        self.assertEqual(product["field_ids"]["bead_id"], "F_BEAD_ID")
        self.assertEqual(product["option_ids"]["city"], {"product-city": "O_CITY"})
        self.assertEqual(
            product["static_fields"],
            {
                "city": "product-city",
                "risk_tier": "standard",
                "delivery_profile": "product",
                "data_class": "internal",
            },
        )

    def test_beads_snapshot_reader_loads_existing_receipt_for_every_bead(self) -> None:
        calls: list[tuple[str, str, str]] = []

        def run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
            return subprocess.CompletedProcess(command, 0, json.dumps({
                "schema_version": "1",
                "ok": True,
                "store_ref": "rig:product",
                "beads": [{
                    "id": "ga-1",
                    "title": "title",
                    "description": "body",
                    "acceptance_criteria": "acceptance",
                    "issue_type": "feature",
                    "status": "open",
                    "priority": 1,
                    "revision": 5,
                    "dependency_count": 0,
                    "dependencies": [],
                    "metadata": {"lifecycle_phase": "development"},
                }],
            }), "")

        reader = work_sync.BeadsSnapshotReader(
            city="product-city",
            rig="product",
            repository="product",
            run=run,
            load_receipt=lambda bead_id, repository, store_ref: (
                calls.append((bead_id, repository, store_ref)) or {"receipt": True}
            ),
        )

        records = reader.read()

        self.assertEqual(calls, [("ga-1", "product", "rig:product")])
        self.assertEqual(records[0]["receipt"], {"receipt": True})
    def test_http_transport_uses_env_token_versioned_rest_and_graphql_without_retaining_secret(self) -> None:
        requests: list[object] = []
        old = os.environ.get("WORK_SYNC_TEST_TOKEN")
        os.environ["WORK_SYNC_TEST_TOKEN"] = "short-lived-installation-token"
        self.addCleanup(
            lambda: (
                os.environ.__setitem__("WORK_SYNC_TEST_TOKEN", old)
                if old is not None
                else os.environ.pop("WORK_SYNC_TEST_TOKEN", None)
            )
        )

        def open_request(request: object, *, timeout: float) -> FakeHTTPResponse:
            requests.append((request, timeout))
            return FakeHTTPResponse({"data": {"ok": True}})

        transport = work_sync.GitHubHTTPTransport(
            token_env="WORK_SYNC_TEST_TOKEN",
            urlopen=open_request,
        )

        self.assertEqual(
            transport.rest("PATCH", "/repos/opsime-space/product/issues/42", {"state": "closed"}),
            {"data": {"ok": True}},
        )
        self.assertEqual(
            transport.graphql("query X { viewer { id } }", {"x": 1}),
            {"data": {"ok": True}},
        )
        first = requests[0][0]
        self.assertEqual(first.full_url, "https://api.github.com/repos/opsime-space/product/issues/42")
        self.assertEqual(first.get_method(), "PATCH")
        self.assertEqual(first.headers["X-github-api-version"], "2026-03-10")
        self.assertEqual(first.headers["Authorization"], "Bearer short-lived-installation-token")
        self.assertEqual(requests[1][0].full_url, "https://api.github.com/graphql")
        self.assertNotIn("short-lived-installation-token", repr(transport))

    def test_http_transport_classifies_read_failure_as_transient_and_write_failure_as_ambiguous(self) -> None:
        old = os.environ.get("WORK_SYNC_TEST_TOKEN")
        os.environ["WORK_SYNC_TEST_TOKEN"] = "short-lived-installation-token"
        self.addCleanup(
            lambda: (
                os.environ.__setitem__("WORK_SYNC_TEST_TOKEN", old)
                if old is not None
                else os.environ.pop("WORK_SYNC_TEST_TOKEN", None)
            )
        )

        def fail(_request: object, *, timeout: float) -> FakeHTTPResponse:
            raise urllib.error.URLError("private transport detail")

        transport = work_sync.GitHubHTTPTransport(
            token_env="WORK_SYNC_TEST_TOKEN",
            urlopen=fail,
        )
        with self.assertRaises(work_sync.TransientTransportError) as read_error:
            transport.rest("GET", "/repos/opsime-space/product")
        with self.assertRaises(work_sync.AmbiguousTransportError) as write_error:
            transport.rest("PATCH", "/repos/opsime-space/product/issues/42", {"state": "closed"})
        self.assertNotIn("private transport detail", str(read_error.exception))
        self.assertNotIn("private transport detail", str(write_error.exception))

    def test_projection_writer_creates_issue_adds_project_then_sets_fields_and_reads_effective_binding(self) -> None:
        body, projection_hash = managed_projection_body()
        transport = FakeProjectionTransport()
        snapshot = FakeEffectiveSnapshotReader(body)
        writer = work_sync.GitHubProjectionWriter(
            organization="opsime-space",
            repository="product",
            transport=transport,
            snapshot_reader=snapshot,
            event={"delivery_id": "delivery-1", "origin": "github-human"},
            projects={"Product": resolved_project_routes()["Product"]},
            managed_block=canonical_runtime_contract()["managed_block"],
        )
        identity = {"bead_id": "ga-1", "repository": "product"}
        item = operation(
            "beads-to-github",
            "create-projection",
            "projection",
            {
                "route": {"repository": "product", "project": "Product"},
                "issue": {
                    "title": "private title",
                    "body": body,
                    "issue_type": "Feature",
                    "issue_state": "open",
                    "project": {
                        "status": "Todo",
                        "priority": "High",
                        "bead_type": "feature",
                        "issue_type": "Feature",
                        "lifecycle_phase": "development",
                    },
                    "projection_hash": projection_hash,
                },
            },
        )

        writer.apply(identity, item)

        self.assertEqual(
            transport.rest_calls[0],
            (
                "POST",
                "/repos/opsime-space/product/issues",
                {"title": "private title", "body": body, "type": "Feature"},
            ),
        )
        self.assertIn("addProjectV2ItemById", transport.graphql_calls[0][0])
        self.assertEqual(len(transport.graphql_calls), 10)
        text_calls = [
            variables
            for query, variables in transport.graphql_calls
            if "$text: String!" in query
        ]
        self.assertEqual(text_calls, [{
            "project": "P_1",
            "item": "PI_1",
            "field": "F_BEAD_ID",
            "text": "ga-1",
        }])
        self.assertTrue(writer.readback(identity, item))
        binding = writer.binding(identity)
        self.assertEqual(
            {key: binding[key] for key in (
                "repository_id", "issue_node_id", "issue_number",
                "project_node_id", "project_item_id", "projection_hash",
            )},
            {
                "repository_id": 123,
                "issue_node_id": "I_1",
                "issue_number": 42,
                "project_node_id": "P_1",
                "project_item_id": "PI_1",
                "projection_hash": projection_hash,
            },
        )
        self.assertEqual(binding["delivery_id"], "delivery-1")
        self.assertEqual(binding["imported_comment_ids"], [])

    def test_projection_writer_maps_bound_issue_project_and_restore_operations(self) -> None:
        body, _projection_hash = managed_projection_body()
        transport = FakeProjectionTransport()
        writer = work_sync.GitHubProjectionWriter(
            organization="opsime-space",
            repository="product",
            transport=transport,
            snapshot_reader=FakeEffectiveSnapshotReader(body),
            event={"delivery_id": "delivery-1", "origin": "github-human"},
            projects={"Product": resolved_project_routes()["Product"]},
            managed_block=canonical_runtime_contract()["managed_block"],
        )
        identity = execution_plan()["plans"][0]["identity"]
        operations = [
            operation("beads-to-github", "replace-managed-block", "projection", {"body": body, "projection_hash": "a" * 64}),
            operation("beads-to-github", "update-project-field", "status", "Done"),
            operation("beads-to-github", "update-issue-type", "issue_type", "Feature"),
            operation("beads-to-github", "update-issue-state", "issue_state", "closed"),
            operation("beads-to-github", "restore-project-item", "project_archived", False),
        ]

        for item in operations:
            writer.apply(identity, item)

        self.assertIn(
            ("PATCH", "/repos/opsime-space/product/issues/42", {"body": body}),
            transport.rest_calls,
        )
        self.assertIn(
            ("PATCH", "/repos/opsime-space/product/issues/42", {"type": "Feature"}),
            transport.rest_calls,
        )
        self.assertIn(
            ("PATCH", "/repos/opsime-space/product/issues/42", {"state": "closed"}),
            transport.rest_calls,
        )
        self.assertTrue(any("updateProjectV2ItemFieldValue" in call[0] for call in transport.graphql_calls))
        self.assertTrue(any("unarchiveProjectV2Item" in call[0] for call in transport.graphql_calls))

    def test_project_schema_reader_resolves_exact_field_and_option_ids_from_graphql(self) -> None:
        transport = FakeProjectSchemaTransport()

        result = work_sync.GitHubProjectSchemaReader(
            organization="opsime-space",
            transport=transport,
        ).read({
            "Product": {
                "status": ["Todo", "Done"],
                "priority": ["High"],
                "bead_type": ["feature"],
                "lifecycle_phase": ["development"],
            }
        })

        self.assertEqual(result["Product"]["project_node_id"], "P_1")
        self.assertEqual(result["Product"]["field_ids"]["status"], "F_STATUS")
        self.assertEqual(
            result["Product"]["option_ids"]["status"],
            {"Todo": "O_TODO", "Done": "O_DONE"},
        )
        self.assertEqual(transport.calls[0][1]["organization"], "opsime-space")

    def test_project_schema_reader_fails_closed_on_duplicate_project_title(self) -> None:
        transport = FakeProjectSchemaTransport()
        transport.nodes.append(dict(transport.nodes[0]))

        with self.assertRaisesRegex(RuntimeError, "ambiguous"):
            work_sync.GitHubProjectSchemaReader(
                organization="opsime-space",
                transport=transport,
            ).read({
                "Product": {
                    "status": ["Todo", "Done"],
                    "priority": ["High"],
                    "bead_type": ["feature"],
                    "lifecycle_phase": ["development"],
                }
            })

    def test_projection_writer_preflight_uses_effective_hashes_and_absence_scan(self) -> None:
        body, projection_hash = managed_projection_body()
        snapshot = FakeEffectiveSnapshotReader(body)
        writer = work_sync.GitHubProjectionWriter(
            organization="opsime-space",
            repository="product",
            transport=FakeProjectionTransport(),
            snapshot_reader=snapshot,
            event={"delivery_id": "delivery-1", "origin": "github-human"},
            projects={"Product": resolved_project_routes()["Product"]},
            managed_block=canonical_runtime_contract()["managed_block"],
        )
        identity = execution_plan()["plans"][0]["identity"]
        binding = writer.binding(identity)

        self.assertTrue(writer.preflight(identity, {
            "github_updated_at": binding["github_updated_at"],
            "project_field_hash": binding["project_field_hash"],
            "projection_hash": projection_hash,
        }))
        unbound = {"bead_id": "ga-1", "repository": "product"}
        self.assertTrue(writer.preflight(unbound, {"projection_absent": True}))
        snapshot.absent = False
        self.assertFalse(writer.preflight(unbound, {"projection_absent": True}))

    def test_receipt_store_reuses_existing_pack_state_and_round_trips_closed_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            store = work_sync.WorkSyncReceiptStore(
                repository="product",
                store_ref="rig:product",
                data_root=tempdir,
            )
            identity = {
                "bead_id": "ga-1",
                "repository": "product",
                "repository_id": 123,
                "issue_node_id": "I_1",
                "issue_number": 42,
                "project_node_id": "P_1",
                "project_item_id": "PI_1",
            }
            binding = FakeGitHub().binding(identity)

            receipt = store.save(identity, 5, binding)

            self.assertEqual(
                set(receipt),
                {
                    "schema_version",
                    "bead_id",
                    "repository",
                    "store_ref",
                    "bead_revision",
                    "github_updated_at",
                    "project_field_hash",
                    "projection_hash",
                    "delivery_id",
                    "imported_comment_ids",
                },
            )
            self.assertNotIn("external_ref", receipt)
            self.assertEqual(store.load("ga-1", "product", "rig:product"), receipt)
            files = list(pathlib.Path(tempdir, "work-sync-receipts").glob("*.json"))
            self.assertEqual(len(files), 1)
            self.assertEqual(os.stat(files[0]).st_mode & 0o777, 0o600)

            files[0].write_text(
                json.dumps({**receipt, "repository": "other"}),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(RuntimeError, "receipt"):
                store.load("ga-1", "product", "rig:product")

    def test_receipt_replace_is_fsynced_through_the_parent_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            store = work_sync.WorkSyncReceiptStore(
                repository="product", store_ref="rig:product", data_root=tempdir,
            )
            identity = execution_plan()["plans"][0]["identity"]
            binding = FakeGitHub().binding(identity)
            synced: list[str] = []
            real_fsync = os.fsync

            def observed_fsync(descriptor: int) -> None:
                synced.append("directory" if stat.S_ISDIR(os.fstat(descriptor).st_mode) else "file")
                real_fsync(descriptor)

            with mock.patch.object(work_sync.os, "fsync", side_effect=observed_fsync):
                store.save(identity, 5, binding)

            self.assertEqual(synced[-2:], ["file", "directory"])

    def test_creation_intent_round_trips_without_private_projection_content(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            store = work_sync.WorkSyncReceiptStore(
                repository="product", store_ref="rig:product", data_root=tempdir,
            )
            identity = {"bead_id": "ga-1", "repository": "product"}
            pending = {
                "kind": "create-projection",
                "operation_hash": "a" * 64,
                "projection_hash": "b" * 64,
            }

            receipt = store.begin_creation(identity, 5, pending)

            self.assertEqual(
                store.load("ga-1", "product", "rig:product"), receipt,
            )
            self.assertEqual(set(receipt), {
                "schema_version", "bead_id", "repository", "store_ref", "bead_revision", "pending",
            })
            self.assertNotIn("title", json.dumps(receipt))

    def test_beads_snapshot_reader_uses_one_exact_store_and_raw_records(self) -> None:
        observed: dict[str, object] = {}

        def run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
            observed["command"] = command
            observed["kwargs"] = kwargs
            return subprocess.CompletedProcess(
                command,
                0,
                json.dumps(
                    {
                        "schema_version": "1",
                        "ok": True,
                        "store_ref": "rig:product",
                        "beads": [
                            {
                                "id": "ga-1",
                                "title": "private title",
                                "description": "private body",
                                "acceptance_criteria": "acceptance",
                                "issue_type": "feature",
                                "status": "open",
                                "priority": 1,
                                "revision": -17,
                                "dependency_count": 1,
                                "dependencies": [
                                    {"id": "ga-0", "dependency_type": "blocks"}
                                ],
                                "metadata": {"lifecycle_phase": "development"},
                            }
                        ],
                    }
                ),
                "",
            )

        records = work_sync.BeadsSnapshotReader(
            city="product-city",
            rig="product",
            repository="product",
            run=run,
        ).read()

        self.assertEqual(
            observed["command"],
            [
                "gc",
                "--city",
                "product-city",
                "beads",
                "snapshot",
                "--store-ref=rig:product",
                "--json",
            ],
        )
        self.assertTrue(observed["kwargs"]["capture_output"])
        self.assertEqual(records[0]["repository"], "product")
        self.assertIsNone(records[0]["receipt"])
        self.assertEqual(records[0]["issue"]["revision"], -17)
        self.assertNotIn("dependencies", records[0]["issue"])
        self.assertEqual(
            records[0]["dependencies"],
            [{"id": "ga-0", "dependency_type": "blocks"}],
        )

    def test_beads_snapshot_reader_rejects_wrong_store_without_leaking_content(self) -> None:
        def run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
            return subprocess.CompletedProcess(
                command,
                0,
                json.dumps(
                    {
                        "schema_version": "1",
                        "ok": True,
                        "store_ref": "rig:other",
                        "beads": [{"title": "private title"}],
                    }
                ),
                "",
            )

        with self.assertRaisesRegex(RuntimeError, "exact store") as raised:
            work_sync.BeadsSnapshotReader(
                city="product-city",
                rig="product",
                repository="product",
                run=run,
            ).read()

        self.assertNotIn("private title", str(raised.exception))

    def test_github_snapshot_reader_uses_effective_rest_and_graphql_readback(self) -> None:
        transport = FakeGitHubReadTransport()
        reader = work_sync.GitHubSnapshotReader(
            organization="opsime-space",
            repository="product",
            transport=transport,
            projection_actor_node_id="BOT_1",
        )

        record = reader.read_bound(
            issue_number=42,
            project_node_id="P_1",
            project_item_id="PI_1",
            event={"delivery_id": "delivery-1", "origin": "github-human"},
        )

        self.assertEqual(
            [call[1] for call in transport.rest_calls],
            [
                "/repos/opsime-space/product",
                "/repos/opsime-space/product/issues/42",
                "/repos/opsime-space/product/issues/42/comments?per_page=100&page=1",
            ],
        )
        self.assertEqual(transport.graphql_calls[0][1], {"item": "PI_1"})
        self.assertEqual(record["repository"]["id"], 123)
        self.assertEqual(record["issue"]["node_id"], "I_1")
        self.assertEqual(record["comments"][0]["body"], "private human comment")
        self.assertEqual(record["project"]["project_node_id"], "P_1")
        self.assertEqual(
            record["project"]["fields"],
            {
                "status": "Todo",
                "priority": "High",
                "bead_type": "feature",
                "issue_type": "Feature",
                "lifecycle_phase": "development",
            },
        )
        self.assertEqual(record["projection_actor_node_id"], "BOT_1")

    def test_github_snapshot_reader_fails_closed_on_project_identity_mismatch(self) -> None:
        transport = FakeGitHubReadTransport()
        transport.project_item["content"]["number"] = 99

        with self.assertRaisesRegex(RuntimeError, "stable identity"):
            work_sync.GitHubSnapshotReader(
                organization="opsime-space",
                repository="product",
                transport=transport,
                projection_actor_node_id="BOT_1",
            ).read_bound(
                issue_number=42,
                project_node_id="P_1",
                project_item_id="PI_1",
                event={"delivery_id": "delivery-1", "origin": "github-human"},
            )

    def test_github_snapshot_reader_compares_repository_node_ids_without_coercion(self) -> None:
        for rest_node, graphql_node, database_id in (
            (None, None, 123), ("", "", 123), (123, 123, 123),
            ("R_1", "R_other", 123), ("R_1", 123, 123),
            ("R_1", "R_1", None), ("R_1", "R_1", True),
            ("R_1", "R_1", 0), ("R_1", "R_1", "123"),
        ):
            with self.subTest(rest_node=rest_node, graphql_node=graphql_node, database_id=database_id):
                transport = FakeGitHubReadTransport()
                transport.repository["id"] = database_id
                transport.repository["node_id"] = rest_node
                transport.project_item["content"]["repository"]["id"] = graphql_node
                with self.assertRaisesRegex(RuntimeError, "stable identity"):
                    work_sync.GitHubSnapshotReader(
                        organization="opsime-space", repository="product",
                        transport=transport, projection_actor_node_id="BOT_1",
                    ).read_bound(
                        issue_number=42, project_node_id="P_1", project_item_id="PI_1",
                        event={"delivery_id": "delivery-1", "origin": "github-human"},
                    )

    def test_github_snapshot_reader_enumerates_exact_repository_issues_for_absence_proof(self) -> None:
        transport = FakeGitHubReadTransport()
        reader = work_sync.GitHubSnapshotReader(
            organization="opsime-space",
            repository="product",
            transport=transport,
            projection_actor_node_id="BOT_1",
        )

        candidates = reader.projection_candidates()

        self.assertEqual(candidates, [{
            "node_id": "I_1",
            "number": 42,
            "body": "private body",
        }])
        self.assertEqual(
            transport.rest_calls[-1][1],
            "/repos/opsime-space/product/issues?state=all&per_page=100&page=1",
        )

    def test_reconciliation_accepts_null_body_for_unmanaged_issue(self) -> None:
        transport = FakeGitHubReadTransport()
        transport.issue_body = None
        reader = work_sync.GitHubSnapshotReader(
            organization="opsime-space", repository="product",
            transport=transport, projection_actor_node_id="BOT_1",
        )
        self.assertEqual(reader.projection_candidates(), [{
            "node_id": "I_1", "number": 42, "body": "",
        }])

    def test_reconciliation_rejects_non_text_non_null_issue_body(self) -> None:
        for body in (False, 123, [], {}):
            with self.subTest(body=body):
                transport = FakeGitHubReadTransport()
                transport.issue_body = body
                with self.assertRaisesRegex(RuntimeError, "identity is malformed"):
                    work_sync.GitHubSnapshotReader(
                        organization="opsime-space", repository="product",
                        transport=transport, projection_actor_node_id="BOT_1",
                    ).projection_candidates()

    def test_reconciliation_records_inventory_all_managed_issues_and_relevant_project_items(self) -> None:
        body, _projection_hash = managed_projection_body()
        transport = FakeGitHubReadTransport()
        transport.issue_body = body
        reader = work_sync.GitHubSnapshotReader(
            organization="opsime-space",
            repository="product",
            transport=transport,
            projection_actor_node_id="BOT_1",
        )

        records = reader.reconciliation_records(
            projects=resolved_project_routes(),
            managed_block=canonical_runtime_contract()["managed_block"],
            owning_project="Product",
            cross_city_project="opsime-space",
            cross_city_bead_types=["epic"],
            event={"delivery_id": "delivery-1", "origin": "github-human"},
        )

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["issue"]["node_id"], "I_1")
        self.assertEqual(records[0]["project"]["project_item_id"], "PI_1")
        self.assertEqual(
            [variables["project"] for query, variables in transport.graphql_calls if "WorkSyncProjectItems" in query],
            ["P_1", "P_CROSS"],
        )

    def test_reconciliation_rejects_non_current_managed_block_without_write(self) -> None:
        transport = FakeGitHubReadTransport()
        transport.issue_body = (
            "private body\n<!-- opsime-space:managed:v1:start -->\n{}\n"
            "<!-- opsime-space:managed:v1:end -->"
        )

        with self.assertRaisesRegex(ValueError, "non-current"):
            work_sync.GitHubSnapshotReader(
                organization="opsime-space",
                repository="product",
                transport=transport,
                projection_actor_node_id="BOT_1",
            ).reconciliation_records(
                projects=resolved_project_routes(),
                managed_block=canonical_runtime_contract()["managed_block"],
                owning_project="Product",
                cross_city_project="opsime-space",
                cross_city_bead_types=["epic"],
                event={"delivery_id": "delivery-1", "origin": "github-human"},
            )

    def test_reconciliation_records_fail_closed_for_managed_issue_without_project_item(self) -> None:
        body, _projection_hash = managed_projection_body()
        transport = FakeGitHubReadTransport()
        transport.issue_body = body
        transport.project_items = {"P_1": [], "P_CROSS": []}

        with self.assertRaisesRegex(RuntimeError, "unprojected"):
            work_sync.GitHubSnapshotReader(
                organization="opsime-space",
                repository="product",
                transport=transport,
                projection_actor_node_id="BOT_1",
            ).reconciliation_records(
                projects=resolved_project_routes(),
                managed_block=canonical_runtime_contract()["managed_block"],
                owning_project="Product",
                cross_city_project="opsime-space",
                cross_city_bead_types=["epic"],
                event={"delivery_id": "delivery-1", "origin": "github-human"},
            )

    def test_reconciliation_records_fail_closed_for_wrong_or_duplicate_project_route(self) -> None:
        body, _projection_hash = managed_projection_body()
        transport = FakeGitHubReadTransport()
        transport.issue_body = body
        duplicate = json.loads(json.dumps(transport.project_items["P_1"][0]))
        duplicate["id"] = "PI_CROSS"
        duplicate["project"] = {"id": "P_CROSS"}
        transport.project_items["P_CROSS"] = [duplicate]

        with self.assertRaisesRegex(RuntimeError, "route"):
            work_sync.GitHubSnapshotReader(
                organization="opsime-space",
                repository="product",
                transport=transport,
                projection_actor_node_id="BOT_1",
            ).reconciliation_records(
                projects=resolved_project_routes(),
                managed_block=canonical_runtime_contract()["managed_block"],
                owning_project="Product",
                cross_city_project="opsime-space",
                cross_city_bead_types=["epic"],
                event={"delivery_id": "delivery-1", "origin": "github-human"},
            )

    def test_reconciliation_records_fail_closed_for_static_field_or_bead_id_drift(self) -> None:
        body, _projection_hash = managed_projection_body()
        for field_name, replacement, reason in (
            ("City", "other-city", "static"),
            ("Bead ID", "other-bead", "Bead ID"),
        ):
            with self.subTest(field=field_name):
                transport = FakeGitHubReadTransport()
                transport.issue_body = body
                for node in transport.project_item["fieldValues"]["nodes"]:
                    if node["field"]["name"] == field_name:
                        if "name" in node:
                            node["name"] = replacement
                        else:
                            node["text"] = replacement

                with self.assertRaisesRegex(RuntimeError, reason):
                    work_sync.GitHubSnapshotReader(
                        organization="opsime-space",
                        repository="product",
                        transport=transport,
                        projection_actor_node_id="BOT_1",
                    ).reconciliation_records(
                        projects=resolved_project_routes(),
                        managed_block=canonical_runtime_contract()["managed_block"],
                        owning_project="Product",
                        cross_city_project="opsime-space",
                        cross_city_bead_types=["epic"],
                        event={"delivery_id": "delivery-1", "origin": "github-human"},
                    )

    def test_github_snapshot_reader_bounds_comment_pagination(self) -> None:
        transport = FakeGitHubReadTransport()
        transport.comments = [
            {"node_id": "IC_" + str(index), "body": "body", "user": {"node_id": "U_1"}}
            for index in range(100)
        ]
        with self.assertRaisesRegex(RuntimeError, "pagination"):
            work_sync.GitHubSnapshotReader(
                organization="opsime-space",
                repository="product",
                transport=transport,
                projection_actor_node_id="BOT_1",
                max_pages=2,
            ).read_bound(
                issue_number=42,
                project_node_id="P_1",
                project_item_id="PI_1",
                event={"delivery_id": "delivery-1", "origin": "github-human"},
            )

    def test_planner_uses_private_files_and_does_not_put_content_in_argv(self) -> None:
        observed: dict[str, object] = {}

        def run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
            observed["command"] = command
            observed["input_mode"] = os.stat(command[command.index("--input") + 1]).st_mode & 0o777
            output = command[command.index("--output") + 1]
            observed["output_parent_mode"] = os.stat(os.path.dirname(output)).st_mode & 0o777
            pathlib.Path(output).write_text(json.dumps(execution_plan()), encoding="utf-8")
            os.chmod(output, 0o600)
            return subprocess.CompletedProcess(command, 0, '{"status":"green"}\n', "")

        snapshots = {"beads": [{"title": "private title"}], "issues": []}
        result = work_sync.PlannerRunner(run=run).plan(
            snapshots,
            policy_bin="/opt/bin/agent-platform",
            policy_root="/opt/agent-platform",
        )

        self.assertEqual(result["status"], "green")
        self.assertNotIn("private title", " ".join(observed["command"]))
        self.assertEqual(observed["input_mode"], 0o600)
        self.assertEqual(observed["output_parent_mode"], 0o700)

    def test_pull_groups_human_fields_and_transition_into_one_beads_cas(self) -> None:
        cas = FakeCAS()
        github = FakeGitHub()
        receipts = FakeReceiptStore()
        plan = execution_plan(
            operation("github-to-beads", "update-field", "title", "new title"),
            operation("github-to-beads", "update-field", "description", "new body"),
            operation("github-to-beads", "transition-request", "status", "in_progress"),
        )

        result = work_sync.apply_execution_plan(plan, cas, github, receipts)

        self.assertEqual(result["status"], "green")
        self.assertEqual(
            cas.calls,
            [{
                "bead_id": "ga-1",
                "expected_revision": 5,
                "patch": {"title": "new title", "description": "new body", "status": "in_progress"},
            }],
        )
        self.assertEqual(github.apply_calls, [])
        self.assertEqual(receipts.calls[0]["bead_revision"], 5)
        self.assertEqual(receipts.calls[0]["applied_bead_revision"], 6)

    def test_internal_zero_operation_plan_does_not_block_projected_work(self) -> None:
        cas, github, receipts = FakeCAS(), FakeGitHub(), FakeReceiptStore()
        plan = execution_plan(operation("github-to-beads", "update-field", "title", "human title"))
        plan["plans"].append({
            "state": "green", "execution_safe": True,
            "reason_codes": ["internal-bead-not-projected"],
            "identity": {"bead_id": "internal-1", "repository": "product"},
            "operations": [], "github_precondition": {"projection_absent": True},
        })
        result = work_sync.apply_execution_plan(plan, cas, github, receipts)
        self.assertEqual(result["status"], "green")
        self.assertEqual(len(cas.calls), 1)
        self.assertEqual(cas.calls[0]["bead_id"], "ga-1")

    def test_internal_skipped_plan_cannot_smuggle_a_mutation(self) -> None:
        cas, github, receipts = FakeCAS(), FakeGitHub(), FakeReceiptStore()
        plan = execution_plan(operation("github-to-beads", "update-field", "title", "human title"))
        plan["plans"][0]["reason_codes"] = ["internal-bead-not-projected"]
        plan["plans"][0]["github_precondition"] = {"projection_absent": True}
        result = work_sync.apply_execution_plan(plan, cas, github, receipts)
        self.assertEqual(result["status"], "red")
        self.assertEqual(cas.calls, [])
        self.assertEqual(github.apply_calls, [])

    def test_push_requires_github_readback_then_persists_pack_receipt_without_bead_write(self) -> None:
        cas = FakeCAS()
        github = FakeGitHub()
        receipts = FakeReceiptStore()
        plan = execution_plan(
            operation(
                "beads-to-github",
                "replace-managed-block",
                "projection",
                {"body": "private body", "projection_hash": "a" * 64},
            ),
        )

        result = work_sync.apply_execution_plan(plan, cas, github, receipts)

        self.assertEqual(result["status"], "green")
        self.assertEqual(len(github.apply_calls), 1)
        self.assertEqual(len(github.readback_calls), 1)
        self.assertEqual(cas.calls, [])
        self.assertEqual(receipts.calls[0]["bead_revision"], 5)
        self.assertEqual(receipts.calls[0]["binding"]["projection_hash"], "a" * 64)

    def test_bidirectional_plan_applies_beads_cas_and_requires_replan_before_github_write(self) -> None:
        cas = FakeCAS()
        github = FakeGitHub()
        receipts = FakeReceiptStore()
        plan = execution_plan(
            operation("github-to-beads", "update-field", "title", "human title"),
            operation(
                "beads-to-github",
                "replace-managed-block",
                "projection",
                {"body": "private", "projection_hash": "a" * 64},
            ),
        )

        result = work_sync.apply_execution_plan(plan, cas, github, receipts)

        self.assertEqual(result["status"], "replan_required")
        self.assertEqual(len(cas.calls), 1)
        self.assertEqual(github.apply_calls, [])
        self.assertEqual(len(receipts.calls), 1)
        self.assertEqual(receipts.calls[0]["bead_revision"], 5)

    def test_dry_run_performs_no_mutations(self) -> None:
        cas = FakeCAS()
        github = FakeGitHub()
        receipts = FakeReceiptStore()
        plan = execution_plan(
            operation("github-to-beads", "update-field", "title", "human title"),
            operation(
                "beads-to-github",
                "replace-managed-block",
                "projection",
                {"body": "private", "projection_hash": "a" * 64},
            ),
        )

        result = work_sync.apply_execution_plan(
            plan, cas, github, receipts, dry_run=True
        )

        self.assertEqual(result["status"], "dry_run")
        self.assertEqual(cas.calls, [])
        self.assertEqual(github.apply_calls, [])

    def test_transient_github_failure_retries_once_and_reads_back(self) -> None:
        cas = FakeCAS()
        github = FakeGitHub()
        receipts = FakeReceiptStore()
        github.failures = [work_sync.TransientTransportError("reset")]
        plan = execution_plan(
            operation(
                "beads-to-github",
                "replace-managed-block",
                "projection",
                {"body": "private", "projection_hash": "a" * 64},
            ),
        )

        result = work_sync.apply_execution_plan(plan, cas, github, receipts)

        self.assertEqual(result["status"], "green")
        self.assertEqual(len(github.apply_calls), 2)
        self.assertEqual(len(github.readback_calls), 1)

    def test_ambiguous_github_commit_uses_readback_without_duplicate_write(self) -> None:
        cas = FakeCAS()
        github = FakeGitHub()
        receipts = FakeReceiptStore()
        github.failures = [work_sync.AmbiguousTransportError("timeout")]
        plan = execution_plan(
            operation(
                "beads-to-github",
                "replace-managed-block",
                "projection",
                {"body": "private", "projection_hash": "a" * 64},
            ),
        )

        result = work_sync.apply_execution_plan(plan, cas, github, receipts)

        self.assertEqual(result["status"], "green")
        self.assertEqual(len(github.apply_calls), 1)
        self.assertEqual(len(github.readback_calls), 1)

    def test_beads_conflict_stops_without_github_write(self) -> None:
        cas = FakeCAS(outcome="conflict")
        github = FakeGitHub()
        plan = execution_plan(
            operation("github-to-beads", "update-field", "title", "human title"),
        )

        result = work_sync.apply_execution_plan(
            plan, cas, github, FakeReceiptStore()
        )

        self.assertEqual(result["status"], "red")
        self.assertEqual(result["reason"], "beads_revision_conflict")
        self.assertEqual(github.apply_calls, [])

    def test_changed_github_precondition_stops_before_beads_or_github_mutation(self) -> None:
        cas = FakeCAS()
        github = FakeGitHub()
        github.preflight_result = False
        plan = execution_plan(
            operation("github-to-beads", "update-field", "title", "human title"),
        )

        result = work_sync.apply_execution_plan(
            plan, cas, github, FakeReceiptStore()
        )

        self.assertEqual(result, {
            "status": "red",
            "reason": "github_precondition_changed",
        })
        self.assertEqual(cas.calls, [])
        self.assertEqual(github.apply_calls, [])

    def test_missing_stable_identity_fails_closed_before_mutation(self) -> None:
        cas = FakeCAS()
        github = FakeGitHub()
        plan = execution_plan(operation(
            "beads-to-github",
            "replace-managed-block",
            "projection",
            {"body": "private", "projection_hash": "a" * 64},
        ))
        del plan["plans"][0]["identity"]["issue_node_id"]

        result = work_sync.apply_execution_plan(
            plan, cas, github, FakeReceiptStore()
        )

        self.assertEqual(result["status"], "red")
        self.assertEqual(result["reason"], "invalid_stable_identity")
        self.assertEqual(cas.calls, [])
        self.assertEqual(github.apply_calls, [])

    def test_comment_import_uses_one_atomic_beads_cas_and_persists_exact_receipt(self) -> None:
        cas = FakeCAS()
        github = FakeGitHub()
        receipts = FakeReceiptStore()
        plan = execution_plan(
            operation(
                "github-to-beads",
                "append-comment",
                "comments",
                {
                    "node_id": "IC_1",
                    "body": "human comment",
                    "created_at": "2026-09-02T12:34:56Z",
                },
            )
        )

        result = work_sync.apply_execution_plan(
            plan, cas, github, receipts
        )

        self.assertEqual(result["status"], "green")
        self.assertEqual(cas.calls[0]["patch"], {
            "comments": [{
                "external_id": "IC_1",
                "body": "human comment",
                "created_at": "2026-09-02T12:34:56Z",
            }]
        })
        self.assertEqual(github.imported_comment_ids, ["IC_1"])
        self.assertEqual(
            receipts.calls[0]["binding"]["imported_comment_ids"], ["IC_1"]
        )

    def test_beads_cas_comment_apply_reads_back_authoritative_receipt(self) -> None:
        calls: list[dict[str, object]] = []

        def run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
            calls.append({"command": list(command), "kwargs": dict(kwargs)})
            if "update-cas" in command:
                return subprocess.CompletedProcess(command, 0, json.dumps({
                    "schema_version": "1",
                    "ok": True,
                    "bead_id": "ga-1",
                    "store_ref": "rig:product",
                    "outcome": "updated",
                    "expected_revision": 5,
                    "revision": 6,
                }))
            return subprocess.CompletedProcess(command, 0, json.dumps({
                "schema_version": "1",
                "ok": True,
                "store_ref": "rig:product",
                "beads": [{
                    "id": "ga-1",
                    "revision": 6,
                    "metadata": {
                        "github.imported_comment_ids": json.dumps(["IC_old", "IC_1"]),
                    },
                }],
            }))

        outcome = work_sync.BeadsCAS(
            city="/city", rig="product", gc_bin="/bin/gc", run=run
        ).apply("ga-1", 5, {
            "comments": [{
                "external_id": "IC_1",
                "body": "private human comment",
                "created_at": "2026-09-02T12:34:56Z",
            }]
        })

        self.assertEqual(outcome["imported_comment_ids"], ["IC_old", "IC_1"])
        self.assertIn("update-cas", calls[0]["command"])
        self.assertIn("snapshot", calls[1]["command"])
        self.assertIn("private human comment", calls[0]["kwargs"]["input"])

    def test_safe_red_missing_projection_creates_then_cas_binds_stable_identity(self) -> None:
        cas = FakeCAS()
        github = FakeGitHub()
        receipts = FakeReceiptStore()
        plan = execution_plan(
            operation(
                "beads-to-github",
                "create-projection",
                "projection",
                {
                    "route": {"repository": "product", "project": "Product"},
                    "issue": {
                        "title": "private title",
                        "body": "private body",
                        "issue_type": "Feature",
                        "issue_state": "open",
                        "project": {
                            "status": "Todo",
                            "priority": "High",
                            "bead_type": "feature",
                            "issue_type": "Feature",
                            "lifecycle_phase": "development",
                        },
                        "projection_hash": "a" * 64,
                    },
                },
            ),
            status="red",
            execution_safe=True,
        )
        plan["reason_codes"] = ["missing-github-projection", "orphan-bead"]
        child = plan["plans"][0]
        child["reason_codes"] = ["missing-github-projection"]
        for field in (
            "repository_id",
            "issue_node_id",
            "issue_number",
            "project_node_id",
            "project_item_id",
        ):
            del child["identity"][field]
        child["github_precondition"] = {"projection_absent": True}

        result = work_sync.apply_execution_plan(plan, cas, github, receipts)

        self.assertEqual(result["status"], "green")
        self.assertEqual(len(github.apply_calls), 1)
        self.assertEqual(len(cas.calls), 1)
        self.assertEqual(cas.calls[0]["expected_revision"], 5)
        self.assertEqual(
            cas.calls[0]["patch"],
            {
                "external_ref": "https://github.com/owner/product/issues/42",
                "metadata": {
                    "github.repository_id": "123",
                    "github.issue_node_id": "I_1",
                    "github.issue_number": "42",
                    "github.project_node_id": "P_1",
                    "github.project_item_id": "PI_1",
                },
            },
        )
        self.assertEqual(receipts.calls[0]["bead_revision"], 6)
        self.assertEqual(receipts.creation_pending[0]["kind"], "create-projection")
        self.assertEqual(
            set(receipts.creation_pending[0]),
            {"kind", "operation_hash", "projection_hash"},
        )
        self.assertNotIn("private", json.dumps(receipts.creation_pending[0]))
        self.assertEqual(len(receipts.pending), 1)
        self.assertEqual(
            receipts.pending[0],
            {
                "kind": "bind-bead",
                "before_revision": 5,
                "stable_identity": {
                    "repository_id": 123,
                    "issue_node_id": "I_1",
                    "issue_number": 42,
                    "project_node_id": "P_1",
                    "project_item_id": "PI_1",
                },
            },
        )

        cas, github, receipts = FakeCAS(), FakeGitHub(), FakeReceiptStore()
        def reject_creation(*args: object, **kwargs: object) -> None:
            raise OSError("synthetic creation receipt failure")
        receipts.begin_creation = reject_creation
        failed = work_sync.apply_execution_plan(plan, cas, github, receipts)
        self.assertEqual(failed["reason"], "pending_creation_persistence_unproven")
        self.assertEqual(github.apply_calls, [])
        self.assertEqual(cas.calls, [])

        cas, github, receipts = FakeCAS(), FakeGitHub(), FakeReceiptStore()
        def reject_binding(*args: object, **kwargs: object) -> None:
            raise OSError("synthetic binding receipt failure")
        receipts.begin_binding = reject_binding
        failed = work_sync.apply_execution_plan(plan, cas, github, receipts)
        self.assertEqual(failed["reason"], "pending_binding_persistence_unproven")
        self.assertEqual(len(github.apply_calls), 1)
        self.assertEqual(cas.calls, [])

    def test_proved_existing_projection_binds_bead_without_rewriting_github(self) -> None:
        cas, github, receipts = FakeCAS(), FakeGitHub(), FakeReceiptStore()
        plan = execution_plan(operation(
            "beads-to-github", "bind-existing-projection", "identity",
            {"projection_hash": "a" * 64},
        ))

        result = work_sync.apply_execution_plan(plan, cas, github, receipts)

        self.assertEqual(result["status"], "green", result)
        self.assertEqual(github.apply_calls, [])
        self.assertEqual(len(cas.calls), 1)
        self.assertEqual(cas.calls[0]["patch"]["metadata"]["github.issue_node_id"], "I_1")
        self.assertEqual(receipts.calls[0]["bead_revision"], 6)

    def test_red_conflict_cannot_execute_even_if_flag_is_forged(self) -> None:
        plan = execution_plan(status="red", execution_safe=True)
        plan["reason_codes"] = ["concurrent-machine-field-change"]
        plan["plans"][0]["reason_codes"] = ["concurrent-machine-field-change"]

        result = work_sync.apply_execution_plan(
            plan, FakeCAS(), FakeGitHub(), FakeReceiptStore()
        )

        self.assertEqual(result, {
            "status": "red",
            "reason": "planner_not_execution_safe",
        })

    def test_refresh_binding_updates_receipt_without_github_or_beads_write(self) -> None:
        cas = FakeCAS()
        github = FakeGitHub()
        receipts = FakeReceiptStore()
        plan = execution_plan(operation(
            "beads-to-github",
            "refresh-binding",
            "identity",
            {
                "bead_revision": 5,
                "github_updated_at": "2026-09-02T10:00:00Z",
                "project_field_hash": "b" * 64,
                "projection_hash": "a" * 64,
            },
        ))

        result = work_sync.apply_execution_plan(plan, cas, github, receipts)

        self.assertEqual(result["status"], "green")
        self.assertEqual(github.apply_calls, [])
        self.assertEqual(cas.calls, [])
        self.assertEqual(receipts.calls[0]["bead_revision"], 5)


class FakeGitHubReadTransport:
    def __init__(self) -> None:
        self.rest_calls: list[tuple[str, str, object]] = []
        self.graphql_calls: list[tuple[str, dict[str, object]]] = []
        self.comments = [
            {
                "node_id": "IC_1",
                "body": "private human comment",
                "created_at": "2026-09-02T12:34:56Z",
                "user": {"node_id": "U_1"},
            }
        ]
        self.issue_body = "private body"
        self.repository = {"id": 123, "node_id": "R_1", "full_name": "opsime-space/product"}
        self.project_item: dict[str, object] = {
            "id": "PI_1",
            "isArchived": False,
            "project": {"id": "P_1"},
            "content": {
                "id": "I_1",
                "number": 42,
                "repository": {"id": "R_1", "nameWithOwner": "opsime-space/product"},
            },
            "fieldValues": {
                "nodes": [
                    {"name": "Todo", "field": {"name": "Status"}},
                    {"name": "High", "field": {"name": "Priority"}},
                    {"name": "feature", "field": {"name": "Bead type"}},
                    {"name": "development", "field": {"name": "Lifecycle phase"}},
                    {"text": "ga-1", "field": {"name": "Bead ID"}},
                    {"name": "product-city", "field": {"name": "City"}},
                    {"name": "standard", "field": {"name": "Risk tier"}},
                    {"name": "product", "field": {"name": "Delivery profile"}},
                    {"name": "internal", "field": {"name": "Data class"}},
                ]
            },
        }
        self.project_items: dict[str, list[dict[str, object]]] = {
            "P_1": [self.project_item],
            "P_CROSS": [],
        }

    def rest(self, method: str, path: str, payload: object = None) -> object:
        self.rest_calls.append((method, path, payload))
        if path == "/repos/opsime-space/product/issues?state=all&per_page=100&page=1":
            return [
                {"node_id": "I_1", "number": 42, "body": self.issue_body},
                {
                    "node_id": "PR_1",
                    "number": 43,
                    "body": "pull request",
                    "pull_request": {},
                },
            ]
        if path == "/repos/opsime-space/product":
            return dict(self.repository)
        if path == "/repos/opsime-space/product/issues/42":
            return {
                "node_id": "I_1",
                "number": 42,
                "updated_at": "2026-09-02T10:00:00Z",
                "state": "open",
                "title": "private title",
                "body": self.issue_body,
                "type": {"name": "Feature"},
            }
        if "/comments?" in path:
            return list(self.comments)
        raise AssertionError(path)

    def graphql(self, query: str, variables: dict[str, object]) -> dict[str, object]:
        self.graphql_calls.append((query, variables))
        if "WorkSyncProjectItems" in query:
            project = str(variables["project"])
            return {
                "data": {
                    "node": {
                        "id": project,
                        "items": {
                            "pageInfo": {"hasNextPage": False, "endCursor": None},
                            "nodes": self.project_items.get(project, []),
                        },
                    }
                }
            }
        return {"data": {"node": self.project_item}}


if __name__ == "__main__":
    unittest.main()
