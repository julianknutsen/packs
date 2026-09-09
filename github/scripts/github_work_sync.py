#!/usr/bin/env python3
"""Fail-closed runtime adapter for external GitHub work-sync policy plans.

The configured policy provider owns pure planning. This module owns only the Gas City
runtime boundary: private snapshot/plan files, exact-store Beads CAS calls, and
GitHub writes with bounded retry plus mandatory readback.
"""

from __future__ import annotations

import argparse
import contextlib
import copy
import fcntl
import hashlib
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import uuid
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from typing import Any, Callable


_WORK_SYNC_TOKEN_ENV = "GC_GITHUB_WORK_SYNC_TOKEN"


def runtime_environment(
    environ: dict[str, str] | os._Environ[str],
    contract: dict[str, object],
) -> dict[str, str]:
    route = contract.get("route") if isinstance(contract, dict) else None
    organization = contract.get("organization") if isinstance(contract, dict) else None
    if not isinstance(route, dict) or not isinstance(organization, str) or not organization:
        raise RuntimeError("canonical work-sync route is unavailable")
    repository = route.get("repository")
    rig = environ.get("GC_RIG", "").strip()
    city = (
        environ.get("GC_CITY_PATH", "").strip()
        or environ.get("GC_CITY_ROOT", "").strip()
    )
    policy_bin = environ.get("GC_WORK_SYNC_POLICY_BIN", "").strip()
    policy_root = environ.get("GC_WORK_SYNC_POLICY_ROOT", "").strip()
    full_name = environ.get("GC_GITHUB_REPO", "").strip().lower()
    dispatch_city = environ.get("GC_GITHUB_DISPATCH_CITY", "").strip()
    dispatch_rig = environ.get("GC_GITHUB_DISPATCH_RIG", "").strip()
    expected_full_name = organization + "/" + str(repository)
    if (
        not isinstance(repository, str)
        or not repository
        or route.get("rig") != repository
        or rig != route.get("rig")
        or not city
        or not os.path.isabs(city)
        or not os.path.isabs(policy_bin)
        or not os.path.isabs(policy_root)
        or (full_name and full_name != expected_full_name)
        or (dispatch_city and dispatch_city != route.get("city"))
        or (dispatch_rig and dispatch_rig != route.get("rig"))
    ):
        raise RuntimeError("work-sync execution environment route mismatch")
    delivery_id = environ.get("GC_GITHUB_DELIVERY_ID", "").strip()
    if not delivery_id:
        delivery_id = "reconciliation:" + str(uuid.uuid4())
    return {
        "repository": repository,
        "repository_full_name": expected_full_name,
        "rig": rig,
        "city": city,
        "policy_bin": policy_bin,
        "policy_root": policy_root,
        "delivery_id": delivery_id,
        "payload_file": environ.get("GC_GITHUB_EVENT_PAYLOAD_FILE", "").strip(),
    }


@contextlib.contextmanager
def installation_token_context(
    *,
    repository_full_name: str,
    token_env: str,
    common: object,
    permissions: dict[str, str] | None = None,
) -> object:
    # An ambient token is not evidence of its repository/permission scope.
    previous_token = os.environ.get(token_env)
    rules = common.load_rules()
    repos = rules.get("repos") if isinstance(rules, dict) else None
    matches = [
        repo
        for repo in repos or []
        if isinstance(repo, dict)
        and str(repo.get("full_name", "")).lower() == repository_full_name.lower()
    ]
    if len(matches) != 1:
        raise RuntimeError("work-sync installation route is missing or ambiguous")
    installation_id = str(matches[0].get("installation_id", "")).strip()
    if not installation_id.isdigit() or int(installation_id) <= 0:
        raise RuntimeError("work-sync installation identity is unavailable")
    app_config = common.github_app_config_for_identity()
    token = common.create_installation_token(
        app_config,
        installation_id,
        repository_full_name=repository_full_name,
        permissions=permissions,
    )
    if not isinstance(token, str) or not token:
        raise RuntimeError("work-sync installation token could not be minted")
    os.environ[token_env] = token
    try:
        yield
    finally:
        if previous_token is None:
            os.environ.pop(token_env, None)
        else:
            os.environ[token_env] = previous_token


_VIEWER_QUERY = """
query WorkSyncViewer {
  viewer { id }
}
""".strip()


def runtime_event(
    environment: dict[str, str],
    contract: dict[str, object],
    transport: object,
) -> tuple[dict[str, object], str]:
    response = transport.graphql(_VIEWER_QUERY, {})
    data = response.get("data") if isinstance(response, dict) else None
    viewer = data.get("viewer") if isinstance(data, dict) else None
    actor_node_id = viewer.get("id") if isinstance(viewer, dict) else None
    if not isinstance(actor_node_id, str) or not actor_node_id:
        raise RuntimeError("GitHub projection actor identity is unavailable")
    origin = "github-human"
    payload_file = environment.get("payload_file", "")
    if payload_file:
        if (
            not os.path.isabs(payload_file)
            or not os.path.isfile(payload_file)
            or os.path.islink(payload_file)
        ):
            raise RuntimeError("GitHub webhook payload file is invalid")
        try:
            with open(payload_file, "r", encoding="utf-8") as source:
                payload = json.load(source)
        except (OSError, TypeError, ValueError) as exc:
            raise RuntimeError("GitHub webhook payload readback failed") from exc
        repository = payload.get("repository") if isinstance(payload, dict) else None
        sender = payload.get("sender") if isinstance(payload, dict) else None
        sender_node_id = sender.get("node_id") if isinstance(sender, dict) else None
        if (
            not isinstance(repository, dict)
            or repository.get("full_name") != environment.get("repository_full_name")
            or not isinstance(sender_node_id, str)
            or not sender_node_id
        ):
            raise RuntimeError("GitHub webhook stable route is invalid")
        if sender_node_id == actor_node_id:
            origin = str(contract.get("projection_writer", ""))
    if origin not in {"github-human", contract.get("projection_writer")}:
        raise RuntimeError("GitHub webhook origin is outside the contract")
    return {
        "delivery_id": environment["delivery_id"],
        "origin": origin,
    }, actor_node_id


class TransientTransportError(RuntimeError):
    """The transport proves the failed attempt did not commit."""


class AmbiguousTransportError(RuntimeError):
    """The transport cannot prove whether the attempted write committed."""


class GitHubHTTPTransport:
    """Versioned GitHub REST/GraphQL transport with no retained credential."""

    def __init__(
        self,
        *,
        token_env: str,
        api_url: str = "https://api.github.com",
        timeout: float = 30.0,
        urlopen: Callable[..., object] | None = None,
    ) -> None:
        import github_intake_common as common

        if not token_env or not isinstance(token_env, str):
            raise ValueError("GitHub installation token environment is required")
        api_url = common.github_https_origin(api_url)
        if timeout <= 0:
            raise ValueError("GitHub transport timeout must be positive")
        self.token_env = token_env
        self.api_url = api_url.rstrip("/")
        self.timeout = timeout
        self.urlopen = urlopen if urlopen is not None else common.github_no_redirect_urlopen

    @staticmethod
    def _is_write(method: str) -> bool:
        return method not in {"GET", "HEAD", "OPTIONS"}

    @staticmethod
    def _rate_limited(status: int, headers: object) -> bool:
        if status == 429:
            return True
        if status != 403 or headers is None:
            return False
        get = getattr(headers, "get", None)
        if not callable(get):
            return False
        return bool(get("Retry-After")) or get("X-RateLimit-Remaining") == "0"

    def _raise_transport_failure(
        self,
        method: str,
        *,
        status: int | None = None,
        headers: object = None,
    ) -> None:
        if status is not None and self._rate_limited(status, headers):
            raise TransientTransportError("GitHub rate limit rejected the request")
        if status is not None and status < 500:
            raise RuntimeError("GitHub request was rejected")
        if self._is_write(method):
            raise AmbiguousTransportError(
                "GitHub write transport outcome is ambiguous"
            )
        raise TransientTransportError("GitHub read transport failed")

    def _request(
        self,
        method: str,
        path: str,
        payload: object = None,
        *,
        graphql_write: bool = False,
    ) -> object:
        method = method.upper()
        if not path.startswith("/") or path.startswith("//"):
            raise ValueError("GitHub request path must be absolute")
        token = os.environ.get(self.token_env, "")
        if not token:
            raise RuntimeError("GitHub installation token is unavailable")
        headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": "Bearer " + token,
            "User-Agent": "gascity-github-work-sync",
            "X-GitHub-Api-Version": "2026-03-10",
        }
        data = None
        if payload is not None:
            try:
                data = json.dumps(
                    payload,
                    ensure_ascii=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            except (TypeError, ValueError) as exc:
                raise ValueError("GitHub request payload must be JSON") from exc
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(
            self.api_url + path,
            data=data,
            headers=headers,
            method=method,
        )
        effective_method = "POST" if graphql_write else method
        try:
            with self.urlopen(request, timeout=self.timeout) as response:
                status = int(getattr(response, "status", 200))
                if status < 200 or status >= 300:
                    self._raise_transport_failure(
                        effective_method,
                        status=status,
                        headers=getattr(response, "headers", None),
                    )
                raw = response.read()
        except urllib.error.HTTPError as exc:
            self._raise_transport_failure(
                effective_method,
                status=exc.code,
                headers=exc.headers,
            )
        except (urllib.error.URLError, TimeoutError, OSError):
            self._raise_transport_failure(effective_method)
        if not raw:
            return {}
        try:
            result = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, TypeError, ValueError) as exc:
            raise RuntimeError("GitHub response was not valid JSON") from exc
        if not isinstance(result, (dict, list)):
            raise RuntimeError("GitHub response JSON has an unsupported shape")
        return result

    def rest(self, method: str, path: str, payload: object = None) -> object:
        """Call a version-pinned GitHub REST endpoint."""
        return self._request(method, path, payload)

    def graphql(self, query: str, variables: dict[str, object]) -> object:
        """Call GitHub GraphQL and classify mutation errors as ambiguous."""
        if not isinstance(query, str) or not query.strip() or not isinstance(variables, dict):
            raise ValueError("GitHub GraphQL query and variables are required")
        is_mutation = query.lstrip().startswith("mutation")
        result = self._request(
            "POST",
            "/graphql",
            {"query": query, "variables": variables},
            graphql_write=is_mutation,
        )
        if isinstance(result, dict) and result.get("errors"):
            if is_mutation:
                raise AmbiguousTransportError(
                    "GitHub GraphQL mutation outcome is ambiguous"
                )
            raise RuntimeError("GitHub GraphQL query failed")
        return result


def _private_json(path: str, value: object) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        json.dump(value, handle, ensure_ascii=False, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())


_RECEIPT_FIELDS = {
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
}


def _valid_pending_push(value: object) -> bool:
    return bool(
        isinstance(value, dict)
        and set(value) == {"kind", "before_hash", "after_hash"}
        and value.get("kind") == "github-write"
        and _sha256(value.get("before_hash"))
        and _sha256(value.get("after_hash"))
    )


def _unique_comment_ids(value: object) -> bool:
    return (
        isinstance(value, list)
        and all(isinstance(item, str) and item for item in value)
        and len(value) == len(set(value))
    )


def _valid_pending_pull(value: object) -> bool:
    if not isinstance(value, dict) or set(value) != {
        "kind", "field_hashes", "before_comment_ids", "after_comment_ids", "base",
    } or value.get("kind") != "beads-cas":
        return False
    fields = value.get("field_hashes")
    before, after = value.get("before_comment_ids"), value.get("after_comment_ids")
    return bool(
        isinstance(fields, dict)
        and _valid_convergence_base(value.get("base"))
        and set(fields).issubset({"title", "description", "status", "priority", "type"})
        and all(_sha256(digest) for digest in fields.values())
        and _unique_comment_ids(before) and _unique_comment_ids(after)
        and set(before).issubset(after)
        and (fields or set(after) != set(before))
    )


def _valid_pending_creation(value: object) -> bool:
    return bool(
        isinstance(value, dict)
        and set(value) == {"kind", "operation_hash", "projection_hash"}
        and value.get("kind") == "create-projection"
        and _sha256(value.get("operation_hash"))
        and _sha256(value.get("projection_hash"))
    )


_STABLE_GITHUB_ID_FIELDS = (
    "repository_id", "issue_node_id", "issue_number", "project_node_id", "project_item_id",
)


def _valid_pending_binding(value: object) -> bool:
    if not isinstance(value, dict) or set(value) != {
        "kind", "before_revision", "stable_identity",
    } or value.get("kind") != "bind-bead":
        return False
    revision = value.get("before_revision")
    identity = value.get("stable_identity")
    return bool(
        isinstance(revision, int) and not isinstance(revision, bool) and revision != 0
        and isinstance(identity, dict) and set(identity) == set(_STABLE_GITHUB_ID_FIELDS)
        and isinstance(identity.get("repository_id"), int)
        and not isinstance(identity["repository_id"], bool) and identity["repository_id"] > 0
        and isinstance(identity.get("issue_number"), int)
        and not isinstance(identity["issue_number"], bool) and identity["issue_number"] > 0
        and all(isinstance(identity.get(field), str) and identity[field]
                for field in ("issue_node_id", "project_node_id", "project_item_id"))
    )


def _pending_binding(revision: int, binding: dict[str, object]) -> dict[str, object]:
    return {
        "kind": "bind-bead", "before_revision": revision,
        "stable_identity": {field: binding[field] for field in _STABLE_GITHUB_ID_FIELDS},
    }


def _pending_creation(item: dict[str, object]) -> dict[str, object]:
    value = item.get("value")
    issue = value.get("issue") if isinstance(value, dict) else None
    projection_hash = issue.get("projection_hash") if isinstance(issue, dict) else None
    if not _sha256(projection_hash):
        raise ValueError("create projection hash is invalid")
    return {
        "kind": "create-projection",
        "operation_hash": hashlib.sha256(json.dumps(
            item, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        ).encode("utf-8")).hexdigest(),
        "projection_hash": projection_hash,
    }


def _valid_convergence_base(value: object) -> bool:
    return bool(
        isinstance(value, dict) and set(value) == {
            "bead_revision", "github_updated_at", "project_field_hash", "projection_hash", "delivery_id",
        }
        and isinstance(value.get("bead_revision"), int)
        and not isinstance(value["bead_revision"], bool) and value["bead_revision"] != 0
        and _sha256(value.get("project_field_hash")) and _sha256(value.get("projection_hash"))
        and all(isinstance(value.get(field), str) and value[field] for field in ("github_updated_at", "delivery_id"))
    )


def _pending_pull(patch: dict[str, object], binding: dict[str, object], base: object) -> dict[str, object]:
    if not _valid_convergence_base(base):
        raise ValueError("pull requires a proved convergence base")
    before = binding["imported_comment_ids"]
    return {
        "kind": "beads-cas",
        "base": dict(base),
        "field_hashes": {
            field: hashlib.sha256(json.dumps(
                value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
            ).encode("utf-8")).hexdigest()
            for field, value in patch.items() if field != "comments"
        },
        "before_comment_ids": list(before),
        "after_comment_ids": sorted(set(before) | {
            comment["external_id"] for comment in patch.get("comments", [])
        }),
    }


class WorkSyncReceiptStore:
    """Mutable convergence bases inside the existing GitHub Pack data root."""

    def __init__(
        self,
        *,
        repository: str,
        store_ref: str,
        data_root: str | None = None,
    ) -> None:
        if (
            not isinstance(repository, str)
            or not repository
            or "/" in repository
            or not isinstance(store_ref, str)
            or not store_ref.startswith("rig:")
            or "/" in store_ref
        ):
            raise ValueError("receipt store requires an exact repository and Rig store")
        self.repository = repository
        self.store_ref = store_ref
        if data_root is None:
            import github_intake_common as common

            data_root = common.data_dir()
        self.root = os.path.join(data_root, "work-sync-receipts")

    def _path(self, bead_id: str) -> str:
        if not isinstance(bead_id, str) or not bead_id:
            raise ValueError("receipt bead identity is required")
        digest = hashlib.sha256(
            json.dumps(
                [bead_id, self.repository, self.store_ref],
                ensure_ascii=True,
                separators=(",", ":"),
            ).encode("ascii")
        ).hexdigest()
        return os.path.join(self.root, digest + ".json")

    def _validate(
        self,
        payload: object,
        *,
        bead_id: str,
        repository: str,
        store_ref: str,
    ) -> dict[str, object]:
        creation_fields = {
            "schema_version", "bead_id", "repository", "store_ref", "bead_revision", "pending",
        }
        if isinstance(payload, dict) and set(payload) == creation_fields:
            if (
                payload.get("schema_version") == 1
                and payload.get("bead_id") == bead_id
                and payload.get("repository") == repository
                and payload.get("store_ref") == store_ref
                and isinstance(payload.get("bead_revision"), int)
                and not isinstance(payload["bead_revision"], bool)
                and payload["bead_revision"] != 0
                and _valid_pending_creation(payload.get("pending"))
            ):
                return dict(payload)
            raise RuntimeError("work-sync creation receipt is malformed or belongs to another identity")
        if (
            not isinstance(payload, dict)
            or not _RECEIPT_FIELDS.issubset(payload)
            or set(payload) - _RECEIPT_FIELDS - {"applied_bead_revision", "pending"}
            or ("pending" in payload and not (
                _valid_pending_push(payload["pending"]) or _valid_pending_pull(payload["pending"])
                or _valid_pending_binding(payload["pending"])
            ))
            or payload.get("schema_version") != 1
            or payload.get("bead_id") != bead_id
            or payload.get("repository") != repository
            or payload.get("store_ref") != store_ref
            or not isinstance(payload.get("bead_revision"), int)
            or isinstance(payload["bead_revision"], bool)
            or payload["bead_revision"] == 0
            or not isinstance(payload.get("applied_bead_revision", payload["bead_revision"]), int)
            or isinstance(payload.get("applied_bead_revision"), bool)
            or payload.get("applied_bead_revision", payload["bead_revision"]) == 0
            or not isinstance(payload.get("github_updated_at"), str)
            or not payload["github_updated_at"]
            or not _sha256(payload.get("project_field_hash"))
            or not _sha256(payload.get("projection_hash"))
            or not isinstance(payload.get("delivery_id"), str)
            or not payload["delivery_id"]
            or not isinstance(payload.get("imported_comment_ids"), list)
            or any(
                not isinstance(item, str) or not item
                for item in payload["imported_comment_ids"]
            )
            or len(payload["imported_comment_ids"])
            != len(set(payload["imported_comment_ids"]))
        ):
            raise RuntimeError("work-sync receipt is malformed or belongs to another identity")
        return dict(payload)

    def load(self, bead_id: str, repository: str, store_ref: str) -> object:
        if repository != self.repository or store_ref != self.store_ref:
            raise RuntimeError("work-sync receipt route mismatch")
        path = self._path(bead_id)
        try:
            with open(path, "r", encoding="utf-8") as source:
                payload = json.load(source)
        except FileNotFoundError:
            return None
        except (OSError, TypeError, ValueError) as exc:
            raise RuntimeError("work-sync receipt read failed") from exc
        return self._validate(
            payload,
            bead_id=bead_id,
            repository=repository,
            store_ref=store_ref,
        )

    def save(
        self,
        identity: dict[str, object],
        bead_revision: int,
        binding: dict[str, object],
        *,
        applied_bead_revision: int | None = None,
        pending: dict[str, object] | None = None,
    ) -> dict[str, object]:
        if (
            not isinstance(identity, dict)
            or identity.get("repository") != self.repository
            or not isinstance(identity.get("bead_id"), str)
            or not identity["bead_id"]
        ):
            raise ValueError("receipt identity is outside the configured route")
        normalized = _normalized_binding(
            binding,
            identity,
            allow_unbound=not _stable_identity(identity),
        )
        payload = {
            "schema_version": 1,
            "bead_id": identity["bead_id"],
            "repository": self.repository,
            "store_ref": self.store_ref,
            "bead_revision": bead_revision,
            "github_updated_at": normalized["github_updated_at"],
            "project_field_hash": normalized["project_field_hash"],
            "projection_hash": normalized["projection_hash"],
            "delivery_id": normalized["delivery_id"],
            "imported_comment_ids": list(normalized["imported_comment_ids"]),
        }
        if applied_bead_revision is not None:
            payload["applied_bead_revision"] = applied_bead_revision
        if pending is not None:
            payload["pending"] = dict(pending)
        receipt = self._validate(
            payload,
            bead_id=str(identity["bead_id"]),
            repository=self.repository,
            store_ref=self.store_ref,
        )
        return self._persist(str(identity["bead_id"]), receipt)

    def _persist(
        self, bead_id: str, receipt: dict[str, object],
    ) -> dict[str, object]:
        path = self._path(bead_id)
        os.makedirs(self.root, mode=0o750, exist_ok=True)
        lock_path = path + ".lock"
        descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            temporary = tempfile.NamedTemporaryFile(
                dir=self.root,
                prefix=".work-sync-receipt-",
                delete=False,
            )
            temporary_path = temporary.name
            try:
                with temporary:
                    temporary.write(
                        json.dumps(
                            receipt,
                            ensure_ascii=False,
                            indent=2,
                            sort_keys=True,
                        ).encode("utf-8")
                        + b"\n"
                    )
                    temporary.flush()
                    os.fchmod(temporary.fileno(), 0o600)
                    os.fsync(temporary.fileno())
                os.replace(temporary_path, path)
                directory = os.open(self.root, os.O_RDONLY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
            finally:
                try:
                    os.unlink(temporary_path)
                except FileNotFoundError:
                    pass
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)
        return receipt

    def begin(
        self, identity: dict[str, object], bead_revision: int,
        binding: dict[str, object], pending: dict[str, object],
    ) -> dict[str, object]:
        """Persist content-free intent before the external side effect."""
        return self.save(identity, bead_revision, binding, pending=pending)

    def begin_creation(
        self, identity: dict[str, object], bead_revision: int, pending: dict[str, object],
    ) -> dict[str, object]:
        if (
            not isinstance(identity, dict)
            or identity.get("repository") != self.repository
            or not isinstance(identity.get("bead_id"), str)
            or not identity["bead_id"]
        ):
            raise ValueError("creation receipt identity is outside the configured route")
        payload = self._validate({
            "schema_version": 1,
            "bead_id": identity["bead_id"],
            "repository": self.repository,
            "store_ref": self.store_ref,
            "bead_revision": bead_revision,
            "pending": dict(pending),
        }, bead_id=str(identity["bead_id"]), repository=self.repository, store_ref=self.store_ref)
        return self._persist(str(identity["bead_id"]), payload)

    def begin_binding(
        self, identity: dict[str, object], bead_revision: int,
        binding: dict[str, object], pending: dict[str, object],
    ) -> dict[str, object]:
        if not _valid_pending_binding(pending):
            raise ValueError("binding receipt intent is invalid")
        return self.save(identity, bead_revision, binding, pending=pending)


class BeadsSnapshotReader:
    """Capture one exact Gas City store without exposing private row content."""

    def __init__(
        self,
        *,
        city: str,
        rig: str,
        repository: str,
        gc_bin: str = "gc",
        run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
        load_receipt: Callable[[str, str, str], object] | None = None,
    ) -> None:
        if not city or not rig or "/" in rig or not repository or "/" in repository:
            raise ValueError("exact City, local Rig, and repository are required")
        self.city = city
        self.rig = rig
        self.repository = repository
        self.gc_bin = gc_bin
        self.run = run
        self.load_receipt = load_receipt or (lambda _bead_id, _repository, _store_ref: None)

    def read(self) -> list[dict[str, object]]:
        command = [
            self.gc_bin,
            "--city",
            self.city,
            "beads",
            "snapshot",
            "--store-ref=rig:" + self.rig,
            "--json",
        ]
        try:
            result = self.run(
                command,
                capture_output=True,
                text=True,
                check=False,
            )
        except OSError as exc:
            raise RuntimeError("gc beads snapshot failed") from exc
        if result.returncode != 0:
            raise RuntimeError("gc beads snapshot failed")
        try:
            payload = json.loads(result.stdout)
        except (TypeError, ValueError) as exc:
            raise RuntimeError("gc beads snapshot returned malformed JSON") from exc
        expected_ref = "rig:" + self.rig
        if (
            not isinstance(payload, dict)
            or payload.get("schema_version") != "1"
            or payload.get("ok") is not True
            or payload.get("store_ref") != expected_ref
            or not isinstance(payload.get("beads"), list)
        ):
            raise RuntimeError("gc beads snapshot did not prove the exact store")
        records: list[dict[str, object]] = []
        for raw in payload["beads"]:
            if not isinstance(raw, dict):
                raise RuntimeError("gc beads snapshot returned a malformed row")
            issue = dict(raw)
            dependencies = issue.pop("dependencies", None)
            count = issue.get("dependency_count")
            if (
                not isinstance(dependencies, list)
                or not isinstance(count, int)
                or isinstance(count, bool)
                or count != len(dependencies)
                or any(not isinstance(item, dict) for item in dependencies)
            ):
                raise RuntimeError("gc beads snapshot returned incomplete dependencies")
            records.append(
                {
                    "repository": self.repository,
                    "issue": issue,
                    "dependencies": dependencies,
                    "receipt": self.load_receipt(
                        str(issue.get("id", "")),
                        self.repository,
                        expected_ref,
                    ),
                }
            )
        return records


class ContractRunner:
    """Read one canonical repository contract through the work-sync policy provider."""

    def __init__(
        self,
        run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    ) -> None:
        self.run = run

    def read(
        self,
        *,
        repository: str,
        policy_bin: str,
        policy_root: str,
    ) -> dict[str, object]:
        if (
            not isinstance(repository, str)
            or not repository
            or "/" in repository
            or not os.path.isabs(policy_bin)
            or not os.path.isabs(policy_root)
        ):
            raise ValueError("exact repository, planner binary, and policy root are required")
        with tempfile.TemporaryDirectory(prefix="github-work-sync-contract-") as tempdir:
            os.chmod(tempdir, 0o700)
            output_path = os.path.join(tempdir, "runtime-contract.json")
            command = [
                policy_bin,
                "--json",
                "work-sync",
                "runtime-contract",
                "--repository",
                repository,
                "--output",
                output_path,
                "--platform-root",
                policy_root,
            ]
            result = self.run(
                command,
                capture_output=True,
                text=True,
                check=False,
            )
            if result.returncode != 0:
                raise RuntimeError("work-sync runtime contract failed")
            if not os.path.isfile(output_path) or os.path.islink(output_path):
                raise RuntimeError("work-sync runtime contract is not a regular file")
            if os.stat(output_path).st_mode & 0o777 != 0o600:
                raise RuntimeError("work-sync runtime contract must have mode 0600")
            try:
                with open(output_path, "r", encoding="utf-8") as source:
                    contract = json.load(source)
            except (OSError, TypeError, ValueError) as exc:
                raise RuntimeError("work-sync runtime contract is malformed") from exc
        route = contract.get("route") if isinstance(contract, dict) else None
        if not isinstance(route, dict) or route.get("repository") != repository:
            raise RuntimeError("work-sync runtime contract route mismatch")
        return contract


_BOUND_PROJECT_ITEM_QUERY = """
query WorkSyncProjectItem($item: ID!) {
  node(id: $item) {
    ... on ProjectV2Item {
      id
      isArchived
      project { id }
      content {
        ... on Issue {
          id
          number
          repository { id nameWithOwner }
        }
      }
      fieldValues(first: 100) {
        nodes {
          ... on ProjectV2ItemFieldSingleSelectValue {
            name
            field { ... on ProjectV2FieldCommon { name } }
          }
          ... on ProjectV2ItemFieldTextValue {
            text
            field { ... on ProjectV2FieldCommon { name } }
          }
        }
      }
    }
  }
}
""".strip()

_PROJECT_ITEMS_QUERY = """
query WorkSyncProjectItems($project: ID!, $after: String) {
  node(id: $project) {
    ... on ProjectV2 {
      id
      items(first: 100, after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          isArchived
          project { id }
          content {
            ... on Issue {
              id
              number
              repository { id nameWithOwner }
            }
          }
        }
      }
    }
  }
}
""".strip()


class GitHubSnapshotReader:
    """Re-read effective Issue, comments, and ProjectV2 item before planning."""

    def __init__(
        self,
        *,
        organization: str,
        repository: str,
        transport: object,
        projection_actor_node_id: str,
        max_pages: int = 100,
    ) -> None:
        if (
            not organization
            or "/" in organization
            or not repository
            or "/" in repository
            or not projection_actor_node_id
            or not isinstance(max_pages, int)
            or isinstance(max_pages, bool)
            or max_pages <= 0
            or max_pages > 100
        ):
            raise ValueError("invalid GitHub snapshot routing or pagination policy")
        self.organization = organization
        self.repository = repository
        self.transport = transport
        self.projection_actor_node_id = projection_actor_node_id
        self.max_pages = max_pages

    def _base_path(self) -> str:
        return "/repos/{0}/{1}".format(
            urllib.parse.quote(self.organization, safe=""),
            urllib.parse.quote(self.repository, safe=""),
        )

    def _comments(self, issue_number: int) -> list[dict[str, object]]:
        comments: list[dict[str, object]] = []
        for page in range(1, self.max_pages + 1):
            rows = self.transport.rest(
                "GET",
                self._base_path()
                + "/issues/{0}/comments?per_page=100&page={1}".format(
                    issue_number, page
                ),
            )
            if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
                raise RuntimeError("GitHub comment readback is malformed")
            for row in rows:
                user = row.get("user")
                comments.append(
                    {
                        "node_id": row.get("node_id"),
                        "body": row.get("body"),
                        "created_at": row.get("created_at"),
                        "user": {
                            "node_id": user.get("node_id")
                            if isinstance(user, dict)
                            else None
                        },
                    }
                )
            if len(rows) < 100:
                return comments
        raise RuntimeError("GitHub comment pagination exceeded the bounded policy")

    def projection_candidates(self) -> list[dict[str, object]]:
        """Enumerate every Issue in the exact repository for absence proof."""
        candidates: list[dict[str, object]] = []
        for page in range(1, self.max_pages + 1):
            rows = self.transport.rest(
                "GET",
                self._base_path()
                + "/issues?state=all&per_page=100&page={0}".format(page),
            )
            if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
                raise RuntimeError("GitHub Issue reconciliation page is malformed")
            for row in rows:
                if "pull_request" in row:
                    continue
                body = row.get("body")
                if (
                    not isinstance(row.get("node_id"), str)
                    or not row["node_id"]
                    or not isinstance(row.get("number"), int)
                    or isinstance(row["number"], bool)
                    or row["number"] <= 0
                    or "body" not in row
                    or (body is not None and not isinstance(body, str))
                ):
                    raise RuntimeError("GitHub Issue reconciliation identity is malformed")
                candidates.append(
                    {
                        "node_id": row["node_id"],
                        "number": row["number"],
                        "body": "" if body is None else body,
                    }
                )
            if len(rows) < 100:
                return candidates
        raise RuntimeError("GitHub Issue reconciliation exceeded the bounded policy")

    def _project_items(
        self,
        project_title: str,
        project_id: str,
    ) -> list[tuple[str, dict[str, object]]]:
        result: list[tuple[str, dict[str, object]]] = []
        after: str | None = None
        for _page in range(self.max_pages):
            response = self.transport.graphql(
                _PROJECT_ITEMS_QUERY,
                {"project": project_id, "after": after},
            )
            data = response.get("data") if isinstance(response, dict) else None
            project = data.get("node") if isinstance(data, dict) else None
            items = project.get("items") if isinstance(project, dict) else None
            nodes = items.get("nodes") if isinstance(items, dict) else None
            page_info = items.get("pageInfo") if isinstance(items, dict) else None
            if (
                not isinstance(project, dict)
                or project.get("id") != project_id
                or not isinstance(nodes, list)
                or any(not isinstance(item, dict) for item in nodes)
                or not isinstance(page_info, dict)
                or not isinstance(page_info.get("hasNextPage"), bool)
            ):
                raise RuntimeError("GitHub Project item inventory is malformed")
            result.extend((project_title, item) for item in nodes)
            if not page_info["hasNextPage"]:
                return result
            after = page_info.get("endCursor")
            if not isinstance(after, str) or not after:
                raise RuntimeError("GitHub Project item pagination cursor is missing")
        raise RuntimeError("GitHub Project item pagination exceeded the bounded policy")

    def reconciliation_records(
        self,
        *,
        projects: dict[str, object],
        managed_block: dict[str, object],
        owning_project: str,
        cross_city_project: str,
        cross_city_bead_types: list[str],
        event: dict[str, object],
    ) -> list[dict[str, object]]:
        """Inventory the whole managed Issue/Project surface for one repository."""
        expected_titles = {owning_project, cross_city_project}
        if (
            not isinstance(projects, dict)
            or set(projects) != expected_titles
            or not isinstance(managed_block, dict)
            or not isinstance(cross_city_bead_types, list)
            or not cross_city_bead_types
            or len(cross_city_bead_types) != len(set(cross_city_bead_types))
            or any(not isinstance(value, str) or not value for value in cross_city_bead_types)
            or not isinstance(event, dict)
        ):
            raise ValueError("GitHub reconciliation contract is malformed")
        project_ids: dict[str, str] = {}
        for title, config in projects.items():
            project_id = config.get("project_node_id") if isinstance(config, dict) else None
            if not isinstance(project_id, str) or not project_id:
                raise ValueError("GitHub reconciliation Project identity is missing")
            project_ids[str(title)] = project_id

        candidates = self.projection_candidates()
        managed_by_node: dict[str, tuple[dict[str, object], dict[str, object]]] = {}
        numbers: set[int] = set()
        for candidate in candidates:
            node_id = candidate.get("node_id") if isinstance(candidate, dict) else None
            number = candidate.get("number") if isinstance(candidate, dict) else None
            body = candidate.get("body") if isinstance(candidate, dict) else None
            if (
                not isinstance(node_id, str)
                or not node_id
                or not isinstance(number, int)
                or isinstance(number, bool)
                or number <= 0
                or not isinstance(body, str)
                or node_id in managed_by_node
                or number in numbers
            ):
                raise RuntimeError("GitHub Issue inventory identity is ambiguous")
            has_marker = "<!-- opsime-space:managed:" in body
            if not has_marker:
                continue
            managed, _projection_hash = _parse_managed_projection(
                body,
                None,
                managed_block,
            )
            managed_by_node[node_id] = (candidate, managed)
            numbers.add(number)

        items_by_issue: dict[str, list[tuple[str, dict[str, object]]]] = {}
        seen_item_ids: set[str] = set()
        expected_full_name = self.organization + "/" + self.repository
        for title in sorted(project_ids):
            for project_title, item in self._project_items(title, project_ids[title]):
                item_id = item.get("id")
                project = item.get("project")
                content = item.get("content")
                if not isinstance(content, dict):
                    continue
                repository = content.get("repository")
                full_name = repository.get("nameWithOwner") if isinstance(repository, dict) else None
                if full_name != expected_full_name:
                    continue
                node_id = content.get("id")
                number = content.get("number")
                if (
                    not isinstance(item_id, str)
                    or not item_id
                    or item_id in seen_item_ids
                    or not isinstance(project, dict)
                    or project.get("id") != project_ids[project_title]
                    or not isinstance(node_id, str)
                    or not node_id
                    or not isinstance(number, int)
                    or isinstance(number, bool)
                    or number <= 0
                ):
                    raise RuntimeError("GitHub Project item stable identity is ambiguous")
                seen_item_ids.add(item_id)
                if node_id not in managed_by_node:
                    continue
                candidate = managed_by_node[node_id][0]
                if candidate.get("number") != number:
                    raise RuntimeError("GitHub Project item Issue identity mismatches inventory")
                items_by_issue.setdefault(node_id, []).append((project_title, item))

        records: list[dict[str, object]] = []
        cross_types = set(cross_city_bead_types)
        for node_id, (candidate, managed) in sorted(
            managed_by_node.items(),
            key=lambda item: int(item[1][0]["number"]),
        ):
            matches = items_by_issue.get(node_id, [])
            expected_project = (
                cross_city_project
                if managed.get("bead_type") in cross_types
                else owning_project
            )
            if not matches:
                raise RuntimeError("managed GitHub Issue is unprojected")
            if len(matches) != 1 or matches[0][0] != expected_project:
                raise RuntimeError("managed GitHub Issue Project route is ambiguous")
            project_title, item = matches[0]
            records.append(
                self.read_bound(
                    issue_number=int(candidate["number"]),
                    project_node_id=project_ids[project_title],
                    project_item_id=str(item["id"]),
                    event=event,
                    project_config=projects[project_title],
                    bead_id=str(managed["bead_id"]),
                )
            )
        return records

    def read_bound(
        self,
        *,
        issue_number: int,
        project_node_id: str,
        project_item_id: str,
        event: dict[str, object],
        project_config: dict[str, object] | None = None,
        bead_id: str | None = None,
    ) -> dict[str, object]:
        if (
            not isinstance(issue_number, int)
            or isinstance(issue_number, bool)
            or issue_number <= 0
            or not project_node_id
            or not project_item_id
            or not isinstance(event, dict)
            or ((project_config is None) != (bead_id is None))
        ):
            raise ValueError("bound GitHub identity and event are required")
        repository = self.transport.rest("GET", self._base_path())
        issue = self.transport.rest(
            "GET", self._base_path() + "/issues/" + str(issue_number)
        )
        comments = self._comments(issue_number)
        project_response = self.transport.graphql(
            _BOUND_PROJECT_ITEM_QUERY,
            {"item": project_item_id},
        )
        if (
            not isinstance(repository, dict)
            or not isinstance(issue, dict)
            or not isinstance(project_response, dict)
            or project_response.get("errors")
        ):
            raise RuntimeError("GitHub effective readback is malformed")
        data = project_response.get("data")
        item = data.get("node") if isinstance(data, dict) else None
        project = item.get("project") if isinstance(item, dict) else None
        content = item.get("content") if isinstance(item, dict) else None
        repo_id = repository.get("id")
        # GraphQL id is the REST node_id, not the numeric REST database id.
        repo_node_id = repository.get("node_id")
        full_name = repository.get("full_name")
        expected_full_name = self.organization + "/" + self.repository
        if (
            not isinstance(repo_id, int)
            or isinstance(repo_id, bool)
            or repo_id <= 0
            or not isinstance(repo_node_id, str)
            or not repo_node_id
            or not isinstance(item, dict)
            or item.get("id") != project_item_id
            or not isinstance(project, dict)
            or project.get("id") != project_node_id
            or not isinstance(content, dict)
            or content.get("id") != issue.get("node_id")
            or content.get("number") != issue_number
            or not isinstance(content.get("repository"), dict)
            or content["repository"].get("id") != repo_node_id
            or content["repository"].get("nameWithOwner") != expected_full_name
            or full_name != expected_full_name
        ):
            raise RuntimeError("GitHub Project item stable identity mismatch")

        field_values = item.get("fieldValues")
        nodes = field_values.get("nodes") if isinstance(field_values, dict) else None
        if not isinstance(nodes, list):
            raise RuntimeError("GitHub Project field readback is malformed")
        names = {
            "Status": "status",
            "Priority": "priority",
            "Bead type": "bead_type",
            "Lifecycle phase": "lifecycle_phase",
        }
        normalized_fields: dict[str, str] = {}
        route_values: dict[str, str] = {}
        route_names = {
            "Bead ID": "bead_id",
            "City": "city",
            "Risk tier": "risk_tier",
            "Delivery profile": "delivery_profile",
            "Data class": "data_class",
        }
        for node in nodes:
            if not isinstance(node, dict):
                raise RuntimeError("GitHub Project field readback is malformed")
            field = node.get("field")
            source_name = field.get("name") if isinstance(field, dict) else None
            target_name = names.get(source_name)
            route_name = route_names.get(source_name)
            if target_name is None and route_name is None:
                continue
            value = node.get("text") if route_name == "bead_id" else node.get("name")
            destination = normalized_fields if target_name is not None else route_values
            destination_name = target_name if target_name is not None else route_name
            assert destination_name is not None
            if (
                not isinstance(value, str)
                or not value
                or destination_name in destination
            ):
                raise RuntimeError("GitHub Project field readback is ambiguous")
            destination[destination_name] = value
        issue_type = issue.get("type")
        issue_type_name = issue_type.get("name") if isinstance(issue_type, dict) else None
        if set(normalized_fields) != set(names.values()) or not isinstance(issue_type_name, str) or not issue_type_name:
            raise RuntimeError("GitHub Project field readback is incomplete")
        normalized_fields["issue_type"] = issue_type_name
        if project_config is not None:
            expected_static = project_config.get("static_fields")
            if (
                not isinstance(bead_id, str)
                or not bead_id
                or route_values.get("bead_id") != bead_id
            ):
                raise RuntimeError("GitHub Project Bead ID readback mismatches")
            if (
                not isinstance(expected_static, dict)
                or set(expected_static) != _STATIC_PROJECT_FIELDS
                or {
                    field: route_values.get(field)
                    for field in _STATIC_PROJECT_FIELDS
                }
                != expected_static
            ):
                raise RuntimeError("GitHub Project static field readback drifts")

        raw_issue = {
            key: issue.get(key)
            for key in ("node_id", "number", "updated_at", "state", "title", "body")
        }
        return {
            "repository": {"id": repo_id, "full_name": full_name},
            "issue": raw_issue,
            "comments": comments,
            "project": {
                "project_node_id": project_node_id,
                "project_item_id": project_item_id,
                "archived": item.get("isArchived"),
                "fields": normalized_fields,
            },
            "event": dict(event),
            "projection_actor_node_id": self.projection_actor_node_id,
        }


_ADD_PROJECT_ITEM_MUTATION = """
mutation WorkSyncAddProjectItem($project: ID!, $content: ID!) {
  addProjectV2ItemById(input: {projectId: $project, contentId: $content}) {
    item { id }
  }
}
""".strip()

_UPDATE_PROJECT_FIELD_MUTATION = """
mutation WorkSyncUpdateProjectField(
  $project: ID!, $item: ID!, $field: ID!, $option: String!
) {
  updateProjectV2ItemFieldValue(input: {
    projectId: $project,
    itemId: $item,
    fieldId: $field,
    value: {singleSelectOptionId: $option}
  }) {
    projectV2Item { id }
  }
}
""".strip()

_UPDATE_PROJECT_TEXT_FIELD_MUTATION = """
mutation WorkSyncUpdateProjectTextField(
  $project: ID!, $item: ID!, $field: ID!, $text: String!
) {
  updateProjectV2ItemFieldValue(input: {
    projectId: $project,
    itemId: $item,
    fieldId: $field,
    value: {text: $text}
  }) {
    projectV2Item { id }
  }
}
""".strip()

_UNARCHIVE_PROJECT_ITEM_MUTATION = """
mutation WorkSyncUnarchiveProjectItem($project: ID!, $item: ID!) {
  unarchiveProjectV2Item(input: {projectId: $project, itemId: $item}) {
    item { id }
  }
}
""".strip()

_PROJECT_FIELDS = {"status", "priority", "bead_type", "lifecycle_phase"}
_EFFECTIVE_PROJECT_FIELDS = _PROJECT_FIELDS | {"issue_type"}
_STATIC_PROJECT_FIELDS = {"city", "risk_tier", "delivery_profile", "data_class"}
_SELECT_PROJECT_FIELDS = _PROJECT_FIELDS | _STATIC_PROJECT_FIELDS
_ALL_PROJECT_FIELDS = _SELECT_PROJECT_FIELDS | {"bead_id"}

_PROJECT_SCHEMA_QUERY = """
query WorkSyncProjectSchema($organization: String!, $after: String) {
  organization(login: $organization) {
    projectsV2(first: 100, after: $after) {
      pageInfo { hasNextPage endCursor }
      nodes {
        id
        title
        fields(first: 100) {
          nodes {
            ... on ProjectV2Field {
              id
              name
              dataType
            }
            ... on ProjectV2SingleSelectField {
              id
              name
              dataType
              options { id name }
            }
          }
        }
      }
    }
  }
}
""".strip()


class GitHubProjectSchemaReader:
    """Resolve exact Project, field, and option node IDs from GraphQL."""

    def __init__(
        self,
        *,
        organization: str,
        transport: object,
        max_pages: int = 100,
    ) -> None:
        if (
            not isinstance(organization, str)
            or not organization
            or "/" in organization
            or not isinstance(max_pages, int)
            or isinstance(max_pages, bool)
            or max_pages <= 0
            or max_pages > 100
        ):
            raise ValueError("invalid GitHub Project schema route")
        self.organization = organization
        self.transport = transport
        self.max_pages = max_pages

    @staticmethod
    def _validate_desired(
        desired: object,
    ) -> dict[str, dict[str, list[str]]]:
        if not isinstance(desired, dict) or not desired:
            raise ValueError("desired GitHub Project schemas are required")
        normalized: dict[str, dict[str, list[str]]] = {}
        for title, raw in desired.items():
            if (
                not isinstance(title, str)
                or not title
                or not isinstance(raw, dict)
                or set(raw) != _PROJECT_FIELDS
            ):
                raise ValueError("desired GitHub Project schema is malformed")
            fields: dict[str, list[str]] = {}
            for field, values in raw.items():
                if (
                    not isinstance(values, list)
                    or not values
                    or len(values) != len(set(values))
                    or any(not isinstance(value, str) or not value for value in values)
                ):
                    raise ValueError("desired GitHub Project options are malformed")
                fields[str(field)] = list(values)
            normalized[title] = fields
        return normalized

    @staticmethod
    def _project_config(
        node: dict[str, object],
        desired: dict[str, list[str]],
    ) -> dict[str, object]:
        project_id = node.get("id")
        fields_wrapper = node.get("fields")
        nodes = fields_wrapper.get("nodes") if isinstance(fields_wrapper, dict) else None
        if (
            not isinstance(project_id, str)
            or not project_id
            or not isinstance(nodes, list)
            or any(not isinstance(field, dict) for field in nodes)
        ):
            raise RuntimeError("GitHub Project schema readback is malformed")
        display_names = {
            "status": "Status",
            "priority": "Priority",
            "bead_type": "Bead type",
            "lifecycle_phase": "Lifecycle phase",
        }
        field_ids: dict[str, str] = {}
        option_ids: dict[str, dict[str, str]] = {}
        for target, display in display_names.items():
            matches = [field for field in nodes if field.get("name") == display]
            if len(matches) != 1:
                raise RuntimeError("GitHub Project field schema is missing or ambiguous")
            field = matches[0]
            field_id = field.get("id")
            options = field.get("options")
            if (
                not isinstance(field_id, str)
                or not field_id
                or not isinstance(options, list)
                or any(not isinstance(option, dict) for option in options)
            ):
                raise RuntimeError("GitHub Project field readback is malformed")
            by_name: dict[str, str] = {}
            for option in options:
                name = option.get("name")
                option_id = option.get("id")
                if (
                    not isinstance(name, str)
                    or not name
                    or name in by_name
                    or not isinstance(option_id, str)
                    or not option_id
                ):
                    raise RuntimeError("GitHub Project option schema is ambiguous")
                by_name[name] = option_id
            if set(by_name) != set(desired[target]):
                raise RuntimeError("GitHub Project option schema drifts from the contract")
            field_ids[target] = field_id
            option_ids[target] = {
                value: by_name[value]
                for value in desired[target]
            }
        return {
            "project_node_id": project_id,
            "field_ids": field_ids,
            "option_ids": option_ids,
        }

    def read(
        self,
        desired: dict[str, dict[str, list[str]]],
    ) -> dict[str, dict[str, object]]:
        """Read all pages so duplicate titles cannot silently bind."""
        expected = self._validate_desired(desired)
        matches: dict[str, list[dict[str, object]]] = {
            title: [] for title in expected
        }
        after: str | None = None
        for _page in range(self.max_pages):
            response = self.transport.graphql(
                _PROJECT_SCHEMA_QUERY,
                {"organization": self.organization, "after": after},
            )
            data = response.get("data") if isinstance(response, dict) else None
            organization = data.get("organization") if isinstance(data, dict) else None
            projects = (
                organization.get("projectsV2")
                if isinstance(organization, dict)
                else None
            )
            nodes = projects.get("nodes") if isinstance(projects, dict) else None
            page_info = (
                projects.get("pageInfo") if isinstance(projects, dict) else None
            )
            if (
                not isinstance(nodes, list)
                or any(not isinstance(node, dict) for node in nodes)
                or not isinstance(page_info, dict)
                or not isinstance(page_info.get("hasNextPage"), bool)
            ):
                raise RuntimeError("GitHub Project pagination readback is malformed")
            for node in nodes:
                title = node.get("title")
                if isinstance(title, str) and title in matches:
                    matches[title].append(node)
            if not page_info["hasNextPage"]:
                break
            after = page_info.get("endCursor")
            if not isinstance(after, str) or not after:
                raise RuntimeError("GitHub Project pagination cursor is missing")
        else:
            raise RuntimeError("GitHub Project pagination exceeded the bounded policy")
        result: dict[str, dict[str, object]] = {}
        for title, nodes in matches.items():
            if len(nodes) != 1:
                raise RuntimeError("GitHub Project title is missing or ambiguous")
            result[title] = self._project_config(nodes[0], expected[title])
        return result

    def read_contract(
        self,
        contract: dict[str, object],
    ) -> dict[str, dict[str, object]]:
        """Resolve the full canonical Project schema for all routed Projects."""
        expected_keys = {
            "schema_version",
            "organization",
            "route",
            "projects",
            "project_schema",
            "managed_block",
            "projection_writer",
            "token_permissions",
            "live_mutations",
            "cross_city_project",
            "cross_city_bead_types",
            "max_pages_per_run",
            "max_identical_attempts",
        }
        projects = contract.get("projects") if isinstance(contract, dict) else None
        schema = contract.get("project_schema") if isinstance(contract, dict) else None
        if (
            not isinstance(contract, dict)
            or set(contract) != expected_keys
            or contract.get("schema_version") != "1"
            or contract.get("organization") != self.organization
            or not isinstance(projects, dict)
            or not projects
            or not isinstance(schema, dict)
            or set(schema) != {"required_fields", "single_select_options"}
        ):
            raise ValueError("canonical GitHub runtime contract is malformed")
        required = schema.get("required_fields")
        options = schema.get("single_select_options")
        display_to_key = {
            "Status": "status",
            "Priority": "priority",
            "Bead ID": "bead_id",
            "Bead type": "bead_type",
            "Lifecycle phase": "lifecycle_phase",
            "City": "city",
            "Risk tier": "risk_tier",
            "Delivery profile": "delivery_profile",
            "Data class": "data_class",
        }
        if (
            not isinstance(required, list)
            or required != list(display_to_key)
            or not isinstance(options, dict)
            or set(options) != set(display_to_key) - {"Bead ID"}
        ):
            raise ValueError("canonical GitHub Project field contract is malformed")
        normalized_options: dict[str, list[str]] = {}
        for name, values in options.items():
            if (
                not isinstance(values, list)
                or not values
                or len(values) != len(set(values))
                or any(not isinstance(value, str) or not value for value in values)
            ):
                raise ValueError("canonical GitHub Project options are malformed")
            normalized_options[str(name)] = list(values)

        project_nodes = self._read_project_nodes(set(projects))
        result: dict[str, dict[str, object]] = {}
        static_display = {"City", "Risk tier", "Delivery profile", "Data class"}
        for title, raw_project in projects.items():
            if (
                not isinstance(title, str)
                or not title
                or not isinstance(raw_project, dict)
                or set(raw_project) != {"static_fields"}
                or not isinstance(raw_project.get("static_fields"), dict)
                or set(raw_project["static_fields"]) != static_display
            ):
                raise ValueError("canonical GitHub Project route is malformed")
            nodes = project_nodes.get(title, [])
            if len(nodes) != 1:
                raise RuntimeError("GitHub Project title is missing or ambiguous")
            result[title] = self._contract_project_config(
                nodes[0],
                display_to_key=display_to_key,
                options=normalized_options,
                static_fields=raw_project["static_fields"],
            )
        return result

    def _read_project_nodes(
        self,
        titles: set[str],
    ) -> dict[str, list[dict[str, object]]]:
        matches: dict[str, list[dict[str, object]]] = {
            title: [] for title in titles
        }
        after: str | None = None
        for _page in range(self.max_pages):
            response = self.transport.graphql(
                _PROJECT_SCHEMA_QUERY,
                {"organization": self.organization, "after": after},
            )
            data = response.get("data") if isinstance(response, dict) else None
            organization = data.get("organization") if isinstance(data, dict) else None
            projects = organization.get("projectsV2") if isinstance(organization, dict) else None
            nodes = projects.get("nodes") if isinstance(projects, dict) else None
            page_info = projects.get("pageInfo") if isinstance(projects, dict) else None
            if (
                not isinstance(nodes, list)
                or any(not isinstance(node, dict) for node in nodes)
                or not isinstance(page_info, dict)
                or not isinstance(page_info.get("hasNextPage"), bool)
            ):
                raise RuntimeError("GitHub Project pagination readback is malformed")
            for node in nodes:
                title = node.get("title")
                if isinstance(title, str) and title in matches:
                    matches[title].append(node)
            if not page_info["hasNextPage"]:
                return matches
            after = page_info.get("endCursor")
            if not isinstance(after, str) or not after:
                raise RuntimeError("GitHub Project pagination cursor is missing")
        raise RuntimeError("GitHub Project pagination exceeded the bounded policy")

    @staticmethod
    def _contract_project_config(
        node: dict[str, object],
        *,
        display_to_key: dict[str, str],
        options: dict[str, list[str]],
        static_fields: dict[str, object],
    ) -> dict[str, object]:
        project_id = node.get("id")
        wrapper = node.get("fields")
        nodes = wrapper.get("nodes") if isinstance(wrapper, dict) else None
        if (
            not isinstance(project_id, str)
            or not project_id
            or not isinstance(nodes, list)
            or any(not isinstance(field, dict) for field in nodes)
        ):
            raise RuntimeError("GitHub Project schema readback is malformed")
        field_ids: dict[str, str] = {}
        option_ids: dict[str, dict[str, str]] = {}
        for display, key in display_to_key.items():
            matches = [field for field in nodes if field.get("name") == display]
            if len(matches) != 1:
                raise RuntimeError("GitHub Project field schema is missing or ambiguous")
            field = matches[0]
            field_id = field.get("id")
            expected_type = "TEXT" if display == "Bead ID" else "SINGLE_SELECT"
            if (
                not isinstance(field_id, str)
                or not field_id
                or field.get("dataType") != expected_type
            ):
                raise RuntimeError("GitHub Project field type drifts from the contract")
            field_ids[key] = field_id
            if display == "Bead ID":
                continue
            raw_options = field.get("options")
            if not isinstance(raw_options, list) or any(
                not isinstance(option, dict) for option in raw_options
            ):
                raise RuntimeError("GitHub Project option readback is malformed")
            by_name: dict[str, str] = {}
            for option in raw_options:
                name = option.get("name")
                option_id = option.get("id")
                if (
                    not isinstance(name, str)
                    or not name
                    or name in by_name
                    or not isinstance(option_id, str)
                    or not option_id
                ):
                    raise RuntimeError("GitHub Project option schema is ambiguous")
                by_name[name] = option_id
            if set(by_name) != set(options[display]):
                raise RuntimeError("GitHub Project option schema drifts from the contract")
            option_ids[key] = {
                value: by_name[value]
                for value in options[display]
            }
        normalized_static: dict[str, str] = {}
        for display, value in static_fields.items():
            key = display_to_key[display]
            if not isinstance(value, str) or value not in option_ids[key]:
                raise ValueError("GitHub Project static field is outside the contract")
            normalized_static[key] = value
        return {
            "project_node_id": project_id,
            "field_ids": field_ids,
            "option_ids": option_ids,
            "static_fields": normalized_static,
        }


class GitHubProjectionWriter:
    """Apply planner operations through native Issues and Projects v2 APIs."""

    def __init__(
        self,
        *,
        organization: str,
        repository: str,
        transport: object,
        snapshot_reader: object,
        projects: dict[str, object],
        managed_block: dict[str, object],
        event: dict[str, object],
    ) -> None:
        if (
            not isinstance(organization, str)
            or not organization
            or "/" in organization
            or not isinstance(repository, str)
            or not repository
            or "/" in repository
            or not isinstance(event, dict)
            or not isinstance(event.get("delivery_id"), str)
            or not event["delivery_id"]
        ):
            raise ValueError("exact GitHub route and delivery event are required")
        self.organization = organization
        self.repository = repository
        self.transport = transport
        self.snapshot_reader = snapshot_reader
        self.event = dict(event)
        self.projects = self._validate_projects(projects)
        self.managed_block = self._validate_managed_block(managed_block)
        self._pending: dict[str, dict[str, object]] = {}
        self._imported_comment_ids: dict[str, list[str]] = {}
        self._verified: dict[str, dict[str, object]] = {}
        self._expected_readback: dict[str, tuple[dict[str, object], bool]] = {}

    @staticmethod
    def _validate_projects(
        projects: object,
    ) -> dict[str, dict[str, object]]:
        if not isinstance(projects, dict) or not projects:
            raise ValueError("at least one exact GitHub Project route is required")
        normalized: dict[str, dict[str, object]] = {}
        project_ids: set[str] = set()
        for name, raw in projects.items():
            if (
                not isinstance(name, str)
                or not name
                or not isinstance(raw, dict)
                or set(raw)
                != {"project_node_id", "field_ids", "option_ids", "static_fields"}
            ):
                raise ValueError("GitHub Project route is malformed")
            project_id = raw.get("project_node_id")
            field_ids = raw.get("field_ids")
            option_ids = raw.get("option_ids")
            static_fields = raw.get("static_fields")
            if (
                not isinstance(project_id, str)
                or not project_id
                or project_id in project_ids
                or not isinstance(field_ids, dict)
                or set(field_ids) != _ALL_PROJECT_FIELDS
                or any(not isinstance(value, str) or not value for value in field_ids.values())
                or len(set(field_ids.values())) != len(field_ids)
                or not isinstance(option_ids, dict)
                or set(option_ids) != _SELECT_PROJECT_FIELDS
                or not isinstance(static_fields, dict)
                or set(static_fields) != _STATIC_PROJECT_FIELDS
            ):
                raise ValueError("GitHub Project IDs must be complete and unique")
            copied_options: dict[str, dict[str, str]] = {}
            for field in sorted(_SELECT_PROJECT_FIELDS):
                options = option_ids.get(field)
                if (
                    not isinstance(options, dict)
                    or not options
                    or any(
                        not isinstance(option, str)
                        or not option
                        or not isinstance(option_id, str)
                        or not option_id
                        for option, option_id in options.items()
                    )
                ):
                    raise ValueError("GitHub Project option IDs must be explicit")
                copied_options[field] = dict(options)
            if any(
                not isinstance(value, str)
                or not value
                or value not in copied_options[field]
                for field, value in static_fields.items()
            ):
                raise ValueError("GitHub Project static fields must use exact options")
            normalized[name] = {
                "project_node_id": project_id,
                "field_ids": dict(field_ids),
                "option_ids": copied_options,
                "static_fields": dict(static_fields),
            }
            project_ids.add(project_id)
        return normalized

    @staticmethod
    def _validate_managed_block(
        raw: object,
    ) -> dict[str, object]:
        if (
            not isinstance(raw, dict)
            or set(raw) != {
                "schema_version",
                "start_marker",
                "end_marker",
                "fields",
            }
            or not isinstance(raw.get("schema_version"), int)
            or isinstance(raw["schema_version"], bool)
            or raw["schema_version"] <= 0
            or not isinstance(raw.get("start_marker"), str)
            or not raw["start_marker"]
            or not isinstance(raw.get("end_marker"), str)
            or not raw["end_marker"]
            or raw["start_marker"] == raw["end_marker"]
            or not isinstance(raw.get("fields"), list)
            or len(raw["fields"]) != len(set(raw["fields"]))
            or set(raw["fields"]) < {"bead_id", "projection_hash"}
        ):
            raise ValueError("managed block contract is malformed")
        return {
            "schema_version": raw["schema_version"],
            "start_marker": raw["start_marker"],
            "end_marker": raw["end_marker"],
            "fields": list(raw["fields"]),
        }

    def _base_path(self) -> str:
        return "/repos/{0}/{1}".format(
            urllib.parse.quote(self.organization, safe=""),
            urllib.parse.quote(self.repository, safe=""),
        )

    def _project_by_id(self, project_id: object) -> dict[str, object]:
        matches = [
            project
            for project in self.projects.values()
            if project["project_node_id"] == project_id
        ]
        if len(matches) != 1:
            raise ValueError("GitHub Project stable identity is outside the route")
        return matches[0]

    def _effective_identity(
        self,
        identity: dict[str, object],
        *,
        require_bound: bool = True,
    ) -> dict[str, object]:
        if (
            not isinstance(identity, dict)
            or identity.get("repository") != self.repository
            or not isinstance(identity.get("bead_id"), str)
            or not identity["bead_id"]
        ):
            raise ValueError("GitHub identity is outside the configured route")
        effective = dict(identity)
        effective.update(self._pending.get(str(identity["bead_id"]), {}))
        if require_bound and not _stable_identity(effective):
            raise ValueError("GitHub stable identity is incomplete")
        return effective

    @staticmethod
    def _graphql_node(
        response: object,
        operation: str,
        field: str,
    ) -> dict[str, object]:
        data = response.get("data") if isinstance(response, dict) else None
        payload = data.get(operation) if isinstance(data, dict) else None
        node = payload.get(field) if isinstance(payload, dict) else None
        if not isinstance(node, dict) or not isinstance(node.get("id"), str) or not node["id"]:
            raise RuntimeError("GitHub GraphQL mutation readback is malformed")
        return node

    def _set_project_field(
        self,
        project: dict[str, object],
        item_id: str,
        field: str,
        value: str,
    ) -> None:
        field_ids = project["field_ids"]
        option_ids = project["option_ids"]
        assert isinstance(field_ids, dict) and isinstance(option_ids, dict)
        options = option_ids.get(field)
        if not isinstance(options, dict) or value not in options:
            raise ValueError("GitHub Project option is not present in exact config")
        response = self.transport.graphql(
            _UPDATE_PROJECT_FIELD_MUTATION,
            {
                "project": project["project_node_id"],
                "item": item_id,
                "field": field_ids[field],
                "option": options[value],
            },
        )
        node = self._graphql_node(
            response,
            "updateProjectV2ItemFieldValue",
            "projectV2Item",
        )
        if node["id"] != item_id:
            raise RuntimeError("GitHub Project field mutation changed item identity")

    def _set_project_text(
        self,
        project: dict[str, object],
        item_id: str,
        field: str,
        value: str,
    ) -> None:
        field_ids = project["field_ids"]
        assert isinstance(field_ids, dict)
        if field != "bead_id" or not isinstance(value, str) or not value:
            raise ValueError("GitHub Project text field value is invalid")
        response = self.transport.graphql(
            _UPDATE_PROJECT_TEXT_FIELD_MUTATION,
            {
                "project": project["project_node_id"],
                "item": item_id,
                "field": field_ids[field],
                "text": value,
            },
        )
        node = self._graphql_node(
            response,
            "updateProjectV2ItemFieldValue",
            "projectV2Item",
        )
        if node["id"] != item_id:
            raise RuntimeError("GitHub Project text mutation changed item identity")

    def _create(self, identity: dict[str, object], item: dict[str, object]) -> None:
        value = item.get("value")
        route = value.get("route") if isinstance(value, dict) else None
        issue = value.get("issue") if isinstance(value, dict) else None
        if (
            not isinstance(route, dict)
            or route.get("repository") != self.repository
            or not isinstance(route.get("project"), str)
            or route["project"] not in self.projects
            or not isinstance(issue, dict)
        ):
            raise ValueError("create projection route is not exact")
        project = self.projects[str(route["project"])]
        bead_id = str(identity["bead_id"])
        pending = self._pending.setdefault(bead_id, {})
        if not isinstance(pending.get("issue_node_id"), str):
            response = self.transport.rest(
                "POST",
                self._base_path() + "/issues",
                {
                    "title": issue.get("title"),
                    "body": issue.get("body"),
                    "type": issue.get("issue_type"),
                },
            )
            if not isinstance(response, dict):
                raise RuntimeError("GitHub Issue create readback is malformed")
            issue_node_id = response.get("node_id")
            issue_number = response.get("number")
            expected_url = "https://github.com/{0}/{1}/issues/{2}".format(
                self.organization,
                self.repository,
                issue_number,
            )
            if (
                not isinstance(issue_node_id, str)
                or not issue_node_id
                or not isinstance(issue_number, int)
                or isinstance(issue_number, bool)
                or issue_number <= 0
                or response.get("html_url") != expected_url
            ):
                raise RuntimeError("GitHub Issue create stable identity is malformed")
            pending.update(
                {
                    "issue_node_id": issue_node_id,
                    "issue_number": issue_number,
                    "project_node_id": project["project_node_id"],
                }
            )
        if not isinstance(pending.get("project_item_id"), str):
            response = self.transport.graphql(
                _ADD_PROJECT_ITEM_MUTATION,
                {
                    "project": project["project_node_id"],
                    "content": pending["issue_node_id"],
                },
            )
            node = self._graphql_node(response, "addProjectV2ItemById", "item")
            pending["project_item_id"] = node["id"]
        project_fields = issue.get("project")
        if not isinstance(project_fields, dict) or set(project_fields) != _EFFECTIVE_PROJECT_FIELDS:
            raise ValueError("create projection Project fields are incomplete")
        for field in sorted(_PROJECT_FIELDS):
            value_for_field = project_fields[field]
            if not isinstance(value_for_field, str) or not value_for_field:
                raise ValueError("create projection Project field is invalid")
            self._set_project_field(
                project,
                str(pending["project_item_id"]),
                field,
                value_for_field,
            )
        self._set_project_text(
            project,
            str(pending["project_item_id"]),
            "bead_id",
            bead_id,
        )
        static_fields = project["static_fields"]
        assert isinstance(static_fields, dict)
        for field in sorted(_STATIC_PROJECT_FIELDS):
            self._set_project_field(
                project,
                str(pending["project_item_id"]),
                field,
                str(static_fields[field]),
            )
        if issue.get("issue_state") == "closed":
            self.transport.rest(
                "PATCH",
                self._base_path() + "/issues/" + str(pending["issue_number"]),
                {"state": "closed"},
            )

    def apply(self, identity: dict[str, object], item: dict[str, object]) -> None:
        """Apply exactly one validated planner operation."""
        if not isinstance(item, dict):
            raise ValueError("GitHub operation must be an object")
        kind = item.get("kind")
        field = item.get("field")
        value = item.get("value")
        if kind == "create-projection":
            self._effective_identity(identity, require_bound=False)
            self._create(identity, item)
            return
        effective = self._effective_identity(identity)
        bead_id = str(identity["bead_id"])
        verified = self._verified.get(bead_id)
        if verified is not None:
            current = self._concurrency_state(self._snapshot(identity))
            if current != verified:
                raise ValueError("GitHub changed since the last verified operation")
            self._expected_readback[bead_id] = self._next_state(verified, item)
        issue_path = self._base_path() + "/issues/" + str(effective["issue_number"])
        project = self._project_by_id(effective["project_node_id"])
        if kind == "replace-managed-block" and field == "projection":
            if not isinstance(value, dict) or not isinstance(value.get("body"), str):
                raise ValueError("GitHub body operation is malformed")
            self.transport.rest("PATCH", issue_path, {"body": value["body"]})
            return
        if kind == "update-project-field" and field in _PROJECT_FIELDS:
            if not isinstance(value, str) or not value:
                raise ValueError("GitHub Project field value is invalid")
            self._set_project_field(
                project,
                str(effective["project_item_id"]),
                str(field),
                value,
            )
            return
        if kind == "update-issue-type" and field == "issue_type":
            self.transport.rest("PATCH", issue_path, {"type": value})
            return
        if kind == "update-issue-state" and field == "issue_state":
            self.transport.rest("PATCH", issue_path, {"state": value})
            return
        if kind == "restore-project-item" and field == "project_archived" and value is False:
            response = self.transport.graphql(
                _UNARCHIVE_PROJECT_ITEM_MUTATION,
                {
                    "project": effective["project_node_id"],
                    "item": effective["project_item_id"],
                },
            )
            node = self._graphql_node(response, "unarchiveProjectV2Item", "item")
            if node["id"] != effective["project_item_id"]:
                raise RuntimeError("GitHub Project restore changed item identity")
            return
        raise ValueError("unsupported GitHub projection operation")

    def _snapshot(self, identity: dict[str, object]) -> dict[str, object]:
        effective = self._effective_identity(identity, require_bound=False)
        if (
            not isinstance(effective.get("issue_number"), int)
            or isinstance(effective["issue_number"], bool)
            or effective["issue_number"] <= 0
            or not isinstance(effective.get("project_node_id"), str)
            or not effective["project_node_id"]
            or not isinstance(effective.get("project_item_id"), str)
            or not effective["project_item_id"]
        ):
            raise ValueError("GitHub readback identity is incomplete")
        snapshot = self.snapshot_reader.read_bound(
            issue_number=effective["issue_number"],
            project_node_id=effective["project_node_id"],
            project_item_id=effective["project_item_id"],
            event=self.event,
            project_config=self._project_by_id(effective["project_node_id"]),
            bead_id=str(identity["bead_id"]),
        )
        if not isinstance(snapshot, dict):
            raise RuntimeError("GitHub effective readback is not an object")
        return snapshot

    def _readback_matches(
        self,
        snapshot: dict[str, object],
        identity: dict[str, object],
        item: dict[str, object],
    ) -> bool:
        issue = snapshot.get("issue")
        project = snapshot.get("project")
        if not isinstance(issue, dict) or not isinstance(project, dict):
            raise RuntimeError("GitHub effective readback is incomplete")
        fields = project.get("fields")
        if not isinstance(fields, dict):
            raise RuntimeError("GitHub effective Project readback is incomplete")
        kind = item.get("kind")
        field = item.get("field")
        value = item.get("value")
        if kind == "create-projection":
            desired = value.get("issue") if isinstance(value, dict) else None
            try:
                _managed, actual_hash = self._managed_projection(
                    issue.get("body"),
                    str(identity["bead_id"]),
                )
            except ValueError:
                return False
            return bool(
                isinstance(desired, dict)
                and issue.get("title") == desired.get("title")
                and issue.get("body") == desired.get("body")
                and issue.get("state") == desired.get("issue_state")
                and fields == desired.get("project")
                and project.get("archived") is False
                and actual_hash == desired.get("projection_hash")
            )
        if kind == "replace-managed-block":
            if not isinstance(value, dict) or issue.get("body") != value.get("body"):
                return False
            try:
                _managed, actual_hash = self._managed_projection(
                    issue.get("body"),
                    str(identity["bead_id"]),
                )
            except ValueError:
                return False
            return actual_hash == value.get("projection_hash")
        if kind == "update-project-field":
            return fields.get(field) == value
        if kind == "update-issue-type":
            return fields.get("issue_type") == value
        if kind == "update-issue-state":
            return issue.get("state") == value
        if kind == "restore-project-item":
            return project.get("archived") is False
        raise ValueError("unsupported GitHub projection readback")

    def readback(
        self,
        identity: dict[str, object],
        item: dict[str, object],
    ) -> bool | None:
        """Prove the operation against effective REST and GraphQL state."""
        try:
            snapshot = self._snapshot(identity)
        except (TransientTransportError, AmbiguousTransportError):
            return None
        except (AttributeError, RuntimeError, TypeError, ValueError):
            return None
        if not self._readback_matches(snapshot, identity, item):
            return False
        bead_id = str(identity["bead_id"])
        expected_readback = self._expected_readback.get(bead_id)
        if expected_readback is not None:
            actual = self._concurrency_state(snapshot)
            expected, issue_write = copy.deepcopy(expected_readback)
            if issue_write:
                # GitHub chooses this timestamp for our own Issue mutation.
                # Every other field, including comments and Project fields,
                # must match the last verified state plus exactly our write.
                expected["issue"]["updated_at"] = actual["issue"]["updated_at"]
            if actual != expected:
                return False
            self._verified[bead_id] = actual
            self._expected_readback.pop(bead_id, None)
        return True

    @staticmethod
    def _concurrency_state(snapshot: dict[str, object]) -> dict[str, object]:
        # Delivery metadata is supplied locally, not remote mutable state.
        return copy.deepcopy({key: value for key, value in snapshot.items() if key != "event"})

    @staticmethod
    def _next_state(state: dict[str, object], item: dict[str, object]) -> tuple[dict[str, object], bool]:
        expected = copy.deepcopy(state)
        kind, field, value = item.get("kind"), item.get("field"), item.get("value")
        issue_write = kind in {
            "replace-managed-block", "update-issue-type", "update-issue-state",
        }
        if kind == "replace-managed-block" and isinstance(value, dict):
            expected["issue"]["body"] = value.get("body")
        elif kind == "update-issue-state":
            expected["issue"]["state"] = value
        elif kind in {"update-project-field", "update-issue-type"}:
            expected["project"]["fields"][field] = value
        elif kind == "restore-project-item":
            expected["project"]["archived"] = False
        else:
            raise ValueError("unsupported pending GitHub write")
        return expected, issue_write

    @classmethod
    def _recovery_hash(cls, snapshot: dict[str, object]) -> str:
        value = cls._concurrency_state(snapshot)
        value["issue"].pop("updated_at", None)
        return hashlib.sha256(json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        ).encode("utf-8")).hexdigest()

    def pending_write(self, identity: dict[str, object], item: dict[str, object]) -> dict[str, object]:
        verified = self._verified.get(str(identity["bead_id"]))
        if verified is None:
            raise ValueError("pending write requires a verified GitHub base")
        expected, _ = self._next_state(verified, item)
        return {
            "kind": "github-write", "before_hash": self._recovery_hash(verified),
            "after_hash": self._recovery_hash(expected),
        }

    def _managed_projection(
        self,
        body: object,
        bead_id: str | None,
    ) -> tuple[dict[str, object], str]:
        return _parse_managed_projection(body, bead_id, self.managed_block)

    def _snapshot_fingerprint(
        self,
        snapshot: dict[str, object],
        bead_id: str,
    ) -> dict[str, str]:
        issue = snapshot.get("issue")
        project = snapshot.get("project")
        if not isinstance(issue, dict) or not isinstance(project, dict):
            raise ValueError("GitHub effective readback is incomplete")
        managed, projection_hash = self._managed_projection(
            issue.get("body"),
            bead_id,
        )
        fields = project.get("fields")
        issue_state = issue.get("state")
        archived = project.get("archived")
        if (
            not isinstance(fields, dict)
            or set(fields) != _EFFECTIVE_PROJECT_FIELDS
            or any(not isinstance(value, str) or not value for value in fields.values())
            or issue_state not in {"open", "closed"}
            or not isinstance(archived, bool)
            or not isinstance(issue.get("updated_at"), str)
            or not issue["updated_at"]
        ):
            raise ValueError("GitHub effective machine fields are incomplete")
        hash_fields = (
            _EFFECTIVE_PROJECT_FIELDS
            if "bead_type" in managed
            else {"status", "priority", "issue_type", "lifecycle_phase"}
        )
        encoded_fields = json.dumps(
            {
                **{field: fields[field] for field in sorted(hash_fields)},
                "issue_state": issue_state,
                "project_archived": archived,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return {
            "github_updated_at": str(issue["updated_at"]),
            "project_field_hash": hashlib.sha256(encoded_fields).hexdigest(),
            "projection_hash": projection_hash,
        }

    def preflight(
        self,
        identity: dict[str, object],
        precondition: dict[str, object],
    ) -> bool | None:
        """Re-read the exact GitHub base immediately before any mutation."""
        if precondition == {"projection_absent": True}:
            self._effective_identity(identity, require_bound=False)
            try:
                candidates = self.snapshot_reader.projection_candidates()
            except (AttributeError, RuntimeError, TypeError, ValueError):
                return None
            if not isinstance(candidates, list):
                return None
            matches = 0
            try:
                for candidate in candidates:
                    if not isinstance(candidate, dict):
                        return None
                    body = candidate.get("body")
                    if not isinstance(body, str):
                        return None
                    if "<!-- opsime-space:managed:" not in body:
                        continue
                    managed, _projection_hash = self._managed_projection(
                        body,
                        None,
                    )
                    if managed["bead_id"] == identity["bead_id"]:
                        matches += 1
            except ValueError:
                return None
            return matches == 0
        if not _valid_github_precondition(
            precondition,
            projection_absent=False,
        ):
            raise ValueError("GitHub execution precondition is malformed")
        try:
            snapshot = self._snapshot(identity)
            current = self._snapshot_fingerprint(
                snapshot,
                str(identity["bead_id"]),
            )
        except (RuntimeError, TypeError, ValueError):
            return None
        if current != precondition:
            return False
        self._verified[str(identity["bead_id"])] = self._concurrency_state(snapshot)
        return True

    def binding(self, identity: dict[str, object]) -> dict[str, object]:
        """Build a closed convergence receipt from effective GitHub state."""
        self._effective_identity(identity, require_bound=False)
        snapshot = self._snapshot(identity)
        verified = self._verified.get(str(identity["bead_id"]))
        if verified is not None and self._concurrency_state(snapshot) != verified:
            raise ValueError("GitHub changed before convergence receipt readback")
        repository = snapshot.get("repository")
        issue = snapshot.get("issue")
        project = snapshot.get("project")
        event = snapshot.get("event")
        if (
            not isinstance(repository, dict)
            or repository.get("full_name")
            != self.organization + "/" + self.repository
            or not isinstance(repository.get("id"), int)
            or isinstance(repository["id"], bool)
            or repository["id"] <= 0
            or not isinstance(issue, dict)
            or not isinstance(project, dict)
            or not isinstance(event, dict)
        ):
            raise ValueError("GitHub binding readback is incomplete")
        fingerprint = self._snapshot_fingerprint(
            snapshot,
            str(identity["bead_id"]),
        )
        delivery_id = event.get("delivery_id")
        imported = self._imported_comment_ids.get(
            str(identity["bead_id"]),
            event.get("imported_comment_ids", []),
        )
        if (
            not isinstance(delivery_id, str)
            or not delivery_id
            or not isinstance(imported, list)
            or any(not isinstance(comment, str) or not comment for comment in imported)
            or len(imported) != len(set(imported))
        ):
            raise ValueError("GitHub delivery receipt identity is invalid")
        binding = {
            "external_ref": "https://github.com/{0}/{1}/issues/{2}".format(
                self.organization,
                self.repository,
                issue.get("number"),
            ),
            "repository_id": repository["id"],
            "issue_node_id": issue.get("node_id"),
            "issue_number": issue.get("number"),
            "project_node_id": project.get("project_node_id"),
            "project_item_id": project.get("project_item_id"),
            "github_updated_at": fingerprint["github_updated_at"],
            "project_field_hash": fingerprint["project_field_hash"],
            "projection_hash": fingerprint["projection_hash"],
            "delivery_id": delivery_id,
            "imported_comment_ids": list(imported),
        }
        self._pending[str(identity["bead_id"])] = {
            field: binding[field]
            for field in (
                "repository_id",
                "issue_node_id",
                "issue_number",
                "project_node_id",
                "project_item_id",
            )
        }
        return binding

    def set_imported_comment_ids(
        self,
        identity: dict[str, object],
        imported_comment_ids: list[str],
    ) -> None:
        bead_id = identity.get("bead_id") if isinstance(identity, dict) else None
        if (
            not isinstance(bead_id, str)
            or not bead_id
            or not isinstance(imported_comment_ids, list)
            or any(not isinstance(item, str) or not item for item in imported_comment_ids)
            or len(imported_comment_ids) != len(set(imported_comment_ids))
        ):
            raise ValueError("GitHub imported comment receipt is invalid")
        self._imported_comment_ids[bead_id] = list(imported_comment_ids)


class PlannerRunner:
    def __init__(self, run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run) -> None:
        self.run = run

    def plan(
        self,
        snapshots: dict[str, object],
        *,
        policy_bin: str,
        policy_root: str,
    ) -> dict[str, object]:
        if set(snapshots) != {"beads", "issues"}:
            raise ValueError("snapshots must contain exactly beads and issues")
        if not os.path.isabs(policy_bin) or not os.path.isabs(policy_root):
            raise ValueError("planner binary and platform root must be absolute")
        with tempfile.TemporaryDirectory(prefix="github-work-sync-") as tempdir:
            os.chmod(tempdir, 0o700)
            input_path = os.path.join(tempdir, "snapshots.json")
            output_path = os.path.join(tempdir, "execution-plan.json")
            _private_json(input_path, snapshots)
            command = [
                policy_bin,
                "--json",
                "work-sync",
                "plan",
                "--input",
                input_path,
                "--output",
                output_path,
                "--platform-root",
                policy_root,
            ]
            result = self.run(
                command,
                capture_output=True,
                text=True,
                check=False,
            )
            if result.returncode != 0:
                detail = (result.stderr or result.stdout or "planner failed").strip()
                raise RuntimeError("work-sync planner failed: " + detail[:1000])
            if not os.path.isfile(output_path) or os.path.islink(output_path):
                raise RuntimeError("work-sync planner did not create a regular execution plan")
            if os.stat(output_path).st_mode & 0o777 != 0o600:
                raise RuntimeError("work-sync execution plan must have mode 0600")
            with open(output_path, "r", encoding="utf-8") as source:
                plan = json.load(source)
        if not isinstance(plan, dict):
            raise RuntimeError("work-sync planner returned a non-object plan")
        return plan


class WorkSyncReconciler:
    """Run bounded plan/apply/readback cycles on one exact repository route."""

    def __init__(
        self,
        *,
        contract: dict[str, object],
        projects: dict[str, object],
        event: dict[str, object],
        beads_reader: object,
        github_reader: object,
        planner: object,
        beads_cas: object,
        github_writer: object,
        receipt_store: object,
        policy_bin: str,
        policy_root: str,
        apply_plan: Callable[..., dict[str, object]] | None = None,
    ) -> None:
        route = contract.get("route") if isinstance(contract, dict) else None
        if (
            not isinstance(route, dict)
            or not isinstance(route.get("repository"), str)
            or not route["repository"]
            or not isinstance(route.get("owning_project"), str)
            or not route["owning_project"]
            or not isinstance(contract.get("managed_block"), dict)
            or not isinstance(contract.get("cross_city_project"), str)
            or not contract["cross_city_project"]
            or not isinstance(contract.get("cross_city_bead_types"), list)
            or not contract["cross_city_bead_types"]
            or not isinstance(contract.get("max_identical_attempts"), int)
            or isinstance(contract["max_identical_attempts"], bool)
            or contract["max_identical_attempts"] <= 0
            or contract["max_identical_attempts"] > 2
            or not isinstance(contract.get("live_mutations"), bool)
            or not isinstance(projects, dict)
            or not isinstance(event, dict)
            or not isinstance(event.get("delivery_id"), str)
            or not event["delivery_id"]
            or event.get("origin")
            not in {"github-human", contract.get("projection_writer")}
            or not os.path.isabs(policy_bin)
            or not os.path.isabs(policy_root)
        ):
            raise ValueError("work-sync reconciler contract is malformed")
        self.contract = contract
        self.projects = projects
        self.event = dict(event)
        self.beads_reader = beads_reader
        self.github_reader = github_reader
        self.planner = planner
        self.beads_cas = beads_cas
        self.github_writer = github_writer
        self.receipt_store = receipt_store
        self.policy_bin = policy_bin
        self.policy_root = policy_root
        self.apply_plan = apply_plan or apply_execution_plan

    def _plan(self) -> dict[str, object]:
        beads = self.beads_reader.read()
        issues = self.github_reader.reconciliation_records(
            projects=self.projects,
            managed_block=self.contract["managed_block"],
            owning_project=self.contract["route"]["owning_project"],
            cross_city_project=self.contract["cross_city_project"],
            cross_city_bead_types=self.contract["cross_city_bead_types"],
            event=self.event,
        )
        if not isinstance(beads, list) or not isinstance(issues, list):
            raise RuntimeError("work-sync snapshot readers returned malformed data")
        self._prepare_issues(beads, issues)
        plan = self.planner.plan(
            {"beads": beads, "issues": issues},
            policy_bin=self.policy_bin,
            policy_root=self.policy_root,
        )
        if not isinstance(plan, dict):
            raise RuntimeError("work-sync planner returned malformed data")
        return plan

    def _prepare_issues(
        self,
        beads: list[dict[str, object]],
        issues: list[dict[str, object]],
    ) -> None:
        bead_by_id: dict[str, dict[str, object]] = {}
        duplicate_beads: set[str] = set()
        for record in beads:
            raw = record.get("issue") if isinstance(record, dict) else None
            bead_id = raw.get("id") if isinstance(raw, dict) else None
            if not isinstance(bead_id, str) or not bead_id:
                continue
            if bead_id in bead_by_id:
                duplicate_beads.add(bead_id)
            bead_by_id[bead_id] = record

        for record in issues:
            raw_issue = record.get("issue") if isinstance(record, dict) else None
            body = raw_issue.get("body") if isinstance(raw_issue, dict) else None
            managed, projection_hash = _parse_managed_projection(
                body,
                None,
                self.contract["managed_block"],
            )
            bead_id = str(managed["bead_id"])
            bead_record = bead_by_id.get(bead_id)
            if (
                self.event["origin"] == self.contract["projection_writer"]
                and bead_record is not None
                and bead_id not in duplicate_beads
            ):
                raw_bead = bead_record.get("issue")
                revision = raw_bead.get("revision") if isinstance(raw_bead, dict) else None
                event = record.get("event")
                if isinstance(event, dict) and isinstance(revision, int) and not isinstance(revision, bool):
                    event["bead_revision"] = revision
                    event["projection_hash"] = projection_hash

            if bead_record is None or bead_id in duplicate_beads:
                continue
            receipt = bead_record.get("receipt")
            if isinstance(receipt, dict) and "imported_comment_ids" in receipt:
                imported = receipt.get("imported_comment_ids")
                try:
                    self.github_writer.set_imported_comment_ids(
                        {"bead_id": bead_id}, imported
                    )
                except (AttributeError, TypeError, ValueError) as exc:
                    raise RuntimeError("work-sync imported comment receipt is invalid") from exc
            if not isinstance(receipt, dict) or receipt.get("delivery_id") != self.event["delivery_id"]:
                continue
            if _valid_pending_push(receipt.get("pending")) or _valid_pending_pull(receipt.get("pending")):
                # The pure planner verifies exact before/after snapshot hashes
                # and the target Bead revision before allowing resume writes.
                continue
            raw_bead = bead_record.get("issue")
            revision = raw_bead.get("revision") if isinstance(raw_bead, dict) else None
            try:
                fingerprint = self.github_writer._snapshot_fingerprint(record, bead_id)
            except (AttributeError, RuntimeError, TypeError, ValueError) as exc:
                raise RuntimeError("work-sync delivery replay conflict") from exc
            if (
                receipt.get("applied_bead_revision", receipt.get("bead_revision")) != revision
                or any(
                    receipt.get(field) != fingerprint.get(field)
                    for field in (
                        "github_updated_at",
                        "project_field_hash",
                        "projection_hash",
                    )
                )
            ):
                raise RuntimeError("work-sync delivery replay conflict")

    @staticmethod
    def _terminal(plan: dict[str, object]) -> bool:
        counts = plan.get("counts")
        plans = plan.get("plans")
        return bool(
            plan.get("status") == "green"
            and plan.get("execution_safe") is True
            and isinstance(counts, dict)
            and counts.get("operations") == 0
            and isinstance(plans, list)
            and all(
                isinstance(item, dict)
                and item.get("state") == "green"
                and item.get("execution_safe") is True
                and item.get("operations") == []
                for item in plans
            )
        )

    @staticmethod
    def _public_counts(plan: dict[str, object]) -> dict[str, object]:
        counts = plan.get("counts")
        if not isinstance(counts, dict):
            return {}
        return {
            key: counts[key]
            for key in (
                "beads",
                "issues",
                "plans",
                "operations",
                "duplicates",
                "orphans",
            )
            if key in counts
        }

    def run(self, *, dry_run: bool = False) -> dict[str, object]:
        if not dry_run and self.contract["live_mutations"] is not True:
            raise RuntimeError("canonical work-sync live mutations are disabled")
        plan = self._plan()
        if dry_run:
            result = self.apply_plan(
                plan,
                self.beads_cas,
                self.github_writer,
                self.receipt_store,
                dry_run=True,
            )
            return {
                **result,
                "terminal_readback": False,
                "counts": self._public_counts(plan),
            }

        attempts = 0
        max_attempts = int(self.contract["max_identical_attempts"])
        while attempts < max_attempts:
            if self._terminal(plan):
                return {
                    "status": "green",
                    "terminal_readback": True,
                    "attempts": attempts,
                    "counts": self._public_counts(plan),
                }
            attempts += 1
            result = self.apply_plan(
                plan,
                self.beads_cas,
                self.github_writer,
                self.receipt_store,
            )
            if result.get("status") not in {"green", "replan_required"}:
                return {
                    **result,
                    "terminal_readback": False,
                    "attempts": attempts,
                    "counts": self._public_counts(plan),
                }
            plan = self._plan()

        if self._terminal(plan):
            return {
                "status": "green",
                "terminal_readback": True,
                "attempts": attempts,
                "counts": self._public_counts(plan),
            }
        return {
            "status": "unknown",
            "reason": "bounded_reconciliation_not_converged",
            "terminal_readback": False,
            "attempts": attempts,
            "counts": self._public_counts(plan),
        }


def _stable_identity(identity: object, *, allow_unbound: bool = False) -> bool:
    if not isinstance(identity, dict):
        return False
    if not isinstance(identity.get("bead_id"), str) or not identity["bead_id"]:
        return False
    if not isinstance(identity.get("repository"), str) or not identity["repository"]:
        return False
    if allow_unbound:
        return True
    text_fields = ("issue_node_id", "project_node_id", "project_item_id")
    if any(not isinstance(identity.get(field), str) or not identity[field] for field in text_fields):
        return False
    if not isinstance(identity.get("repository_id"), int) or isinstance(identity["repository_id"], bool):
        return False
    if not isinstance(identity.get("issue_number"), int) or isinstance(identity["issue_number"], bool):
        return False
    return identity["repository_id"] > 0 and identity["issue_number"] > 0


def _operation_lists(plan: dict[str, object]) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    pull: list[dict[str, object]] = []
    push: list[dict[str, object]] = []
    raw = plan.get("operations")
    if not isinstance(raw, list):
        raise ValueError("plan operations must be an array")
    for item in raw:
        if not isinstance(item, dict):
            raise ValueError("plan operation must be an object")
        direction = item.get("direction")
        if direction == "github-to-beads":
            pull.append(item)
        elif direction == "beads-to-github":
            push.append(item)
        else:
            raise ValueError("unknown work-sync operation direction")
    return pull, push


def _pull_patch(operations: list[dict[str, object]]) -> tuple[int, dict[str, object]]:
    expected: int | None = None
    patch: dict[str, object] = {}
    for item in operations:
        revision = item.get("expected_revision")
        if not isinstance(revision, int) or isinstance(revision, bool) or revision == 0:
            raise ValueError("operation expected_revision must be a nonzero integer")
        if expected is None:
            expected = revision
        elif expected != revision:
            raise ValueError("one plan cannot mix Beads revisions")
        kind = item.get("kind")
        field = item.get("field")
        value = item.get("value")
        if kind == "append-comment" and field == "comments" and isinstance(value, dict):
            node_id = value.get("node_id")
            body = value.get("body")
            created_at = value.get("created_at")
            if (
                not isinstance(node_id, str)
                or not node_id
                or not isinstance(body, str)
                or not body.strip()
                or not _rfc3339_timestamp(created_at)
            ):
                raise ValueError("comment import requires node_id/body/created_at")
            comments = patch.setdefault("comments", [])
            if not isinstance(comments, list):
                raise ValueError("comment patch collides with another field")
            if any(
                isinstance(comment, dict)
                and comment.get("external_id") == node_id
                for comment in comments
            ):
                raise ValueError("duplicate comment import identity")
            comments.append({
                "external_id": node_id,
                "body": body,
                "created_at": created_at,
            })
            continue
        if kind == "update-field" and field in {"title", "description"} and isinstance(value, str):
            patch[str(field)] = value
            continue
        if kind == "transition-request" and field == "status" and isinstance(value, str):
            patch["status"] = value
            continue
        if kind == "transition-request" and field == "priority" and isinstance(value, int):
            patch["priority"] = value
            continue
        if kind == "transition-request" and field == "bead_type" and isinstance(value, str):
            patch["type"] = value
            continue
        raise ValueError("unsupported github-to-beads operation")
    if expected is None or not patch:
        raise ValueError("empty github-to-beads patch")
    return expected, patch


def _rfc3339_timestamp(value: object) -> bool:
    if not isinstance(value, str) or not value:
        return False
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return False
    return parsed.tzinfo is not None


_BINDING_FIELDS = {
    "external_ref",
    "repository_id",
    "issue_node_id",
    "issue_number",
    "project_node_id",
    "project_item_id",
    "github_updated_at",
    "project_field_hash",
    "projection_hash",
    "delivery_id",
    "imported_comment_ids",
}


def _sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _parse_managed_projection(
    body: object,
    bead_id: str | None,
    managed_block: dict[str, object],
) -> tuple[dict[str, object], str]:
    if not isinstance(body, str):
        raise ValueError("GitHub Issue managed body is not text")
    if not isinstance(managed_block, dict):
        raise ValueError("GitHub Issue managed block contract is malformed")
    start = str(managed_block.get("start_marker", ""))
    end = str(managed_block.get("end_marker", ""))
    fields = managed_block.get("fields")
    if (
        not start
        or not end
        or start == end
        or not isinstance(fields, list)
        or len(fields) != len(set(fields))
    ):
        raise ValueError("GitHub Issue managed block contract is malformed")
    if "<!-- opsime-space:managed:" in body and start not in body and end not in body:
        raise ValueError("GitHub Issue non-current managed block is forbidden")
    if body.count(start) != 1 or body.count(end) != 1:
        raise ValueError("GitHub Issue requires exactly one managed block")
    start_index = body.index(start) + len(start)
    end_index = body.index(end, start_index)
    try:
        managed = json.loads(body[start_index:end_index].strip())
    except (TypeError, ValueError) as exc:
        raise ValueError("GitHub Issue managed block JSON is malformed") from exc
    if (
        not isinstance(managed, dict)
        or set(managed) != set(fields)
        or (bead_id is not None and managed.get("bead_id") != bead_id)
        or not isinstance(managed.get("bead_id"), str)
        or not managed["bead_id"]
        or not _sha256(managed.get("projection_hash"))
    ):
        raise ValueError("GitHub Issue managed block identity is invalid")
    payload = {
        field: managed[field]
        for field in managed
        if field not in {"bead_id", "projection_hash"}
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    if hashlib.sha256(encoded).hexdigest() != managed["projection_hash"]:
        raise ValueError("GitHub Issue managed projection hash mismatches")
    return managed, str(managed["projection_hash"])


def _normalized_binding(
    binding: object,
    identity: dict[str, object],
    *,
    allow_unbound: bool = False,
) -> dict[str, object]:
    if not isinstance(binding, dict) or set(binding) != _BINDING_FIELDS:
        raise ValueError("GitHub readback binding must match the closed receipt schema")
    external_ref = binding.get("external_ref")
    if not isinstance(external_ref, str) or not external_ref.startswith("https://github.com/"):
        raise ValueError("GitHub readback binding lacks canonical external_ref")
    integer_fields = ("repository_id", "issue_number")
    if any(
        not isinstance(binding.get(field), int)
        or isinstance(binding[field], bool)
        or binding[field] <= 0
        for field in integer_fields
    ):
        raise ValueError("GitHub readback numeric identity is invalid")
    text_fields = (
        "issue_node_id",
        "project_node_id",
        "project_item_id",
        "github_updated_at",
        "delivery_id",
    )
    if any(
        not isinstance(binding.get(field), str) or not binding[field]
        for field in text_fields
    ):
        raise ValueError("GitHub readback text identity is invalid")
    if not _sha256(binding.get("project_field_hash")) or not _sha256(
        binding.get("projection_hash")
    ):
        raise ValueError("GitHub readback hashes are invalid")
    imported = binding.get("imported_comment_ids")
    if (
        not isinstance(imported, list)
        or any(not isinstance(item, str) or not item for item in imported)
        or len(imported) != len(set(imported))
    ):
        raise ValueError("GitHub imported comment identity is invalid")
    if not allow_unbound:
        for field in (
            "repository_id",
            "issue_node_id",
            "issue_number",
            "project_node_id",
            "project_item_id",
        ):
            if identity.get(field) != binding[field]:
                raise ValueError("GitHub readback stable identity mismatch")
    return dict(binding)


def _stable_binding_patch(binding: dict[str, object]) -> dict[str, object]:
    return {
        "external_ref": binding["external_ref"],
        "metadata": {
            "github.repository_id": str(binding["repository_id"]),
            "github.issue_node_id": str(binding["issue_node_id"]),
            "github.issue_number": str(binding["issue_number"]),
            "github.project_node_id": str(binding["project_node_id"]),
            "github.project_item_id": str(binding["project_item_id"]),
        },
    }


def _save_receipt(
    receipt_store: object,
    identity: dict[str, object],
    bead_revision: int,
    binding: dict[str, object],
    *,
    applied_bead_revision: int | None = None,
) -> None:
    if (
        not isinstance(bead_revision, int)
        or isinstance(bead_revision, bool)
        or bead_revision == 0
    ):
        raise ValueError("Bead receipt revision must be a nonzero integer")
    if applied_bead_revision is None:
        receipt_store.save(identity, bead_revision, binding)
    else:
        receipt_store.save(
            identity, bead_revision, binding,
            applied_bead_revision=applied_bead_revision,
        )


def _valid_push_operation(item: dict[str, object]) -> bool:
    kind = item.get("kind")
    field = item.get("field")
    value = item.get("value")
    if kind == "create-projection" and field == "projection":
        if not isinstance(value, dict) or set(value) != {"route", "issue"}:
            return False
        route = value.get("route")
        issue = value.get("issue")
        if not isinstance(route, dict) or not isinstance(issue, dict):
            return False
        return (
            set(issue)
            == {
                "title",
                "body",
                "issue_type",
                "issue_state",
                "project",
                "projection_hash",
            }
            and all(
                isinstance(issue.get(name), str) and issue[name]
                for name in ("title", "body", "issue_type")
            )
            and issue.get("issue_state") in {"open", "closed"}
            and isinstance(issue.get("project"), dict)
            and _sha256(issue.get("projection_hash"))
        )
    if kind == "bind-existing-projection" and field == "identity":
        return (
            isinstance(value, dict)
            and set(value) == {"projection_hash"}
            and _sha256(value.get("projection_hash"))
        )
    if kind == "replace-managed-block" and field == "projection":
        return (
            isinstance(value, dict)
            and set(value) == {"body", "projection_hash"}
            and isinstance(value.get("body"), str)
            and _sha256(value.get("projection_hash"))
        )
    if kind == "update-project-field":
        return field in {"status", "priority", "bead_type", "lifecycle_phase"} and isinstance(value, str) and bool(value)
    if kind == "update-issue-type":
        return field == "issue_type" and isinstance(value, str) and bool(value)
    if kind == "update-issue-state":
        return field == "issue_state" and value in {"open", "closed"}
    if kind == "restore-project-item":
        return field == "project_archived" and value is False
    if kind == "refresh-binding" and field == "identity":
        return (
            isinstance(value, dict)
            and set(value)
            == {
                "bead_revision",
                "github_updated_at",
                "project_field_hash",
                "projection_hash",
            }
            and isinstance(value.get("bead_revision"), int)
            and not isinstance(value["bead_revision"], bool)
            and value["bead_revision"] != 0
            and isinstance(value.get("github_updated_at"), str)
            and bool(value["github_updated_at"])
            and _sha256(value.get("project_field_hash"))
            and _sha256(value.get("projection_hash"))
        )
    return False


def _valid_github_precondition(
    precondition: object,
    *,
    projection_absent: bool,
) -> bool:
    if projection_absent:
        return precondition == {"projection_absent": True}
    return bool(
        isinstance(precondition, dict)
        and set(precondition)
        == {
            "github_updated_at",
            "project_field_hash",
            "projection_hash",
        }
        and isinstance(precondition.get("github_updated_at"), str)
        and precondition["github_updated_at"]
        and _sha256(precondition.get("project_field_hash"))
        and _sha256(precondition.get("projection_hash"))
    )


def _execution_allowed(execution: dict[str, object], plans: object) -> bool:
    if execution.get("execution_safe") is not True or not isinstance(plans, list):
        return False
    status = execution.get("status")
    if status not in {"green", "red"}:
        return False
    if status == "red":
        reasons = execution.get("reason_codes")
        if (
            not isinstance(reasons, list)
            or not reasons
            or not set(reasons).issubset(
                {"missing-github-projection", "orphan-bead"}
            )
        ):
            return False
    for plan in plans:
        if not isinstance(plan, dict) or plan.get("execution_safe") is not True:
            return False
        state = plan.get("state")
        operations = plan.get("operations")
        if not isinstance(operations, list):
            return False
        if (
            state == "green"
            and not operations
            and plan.get("reason_codes") == ["internal-bead-not-projected"]
            and _valid_github_precondition(
                plan.get("github_precondition"), projection_absent=True,
            )
        ):
            continue
        creates = bool(operations) and all(
            isinstance(item, dict) and item.get("kind") == "create-projection"
            for item in operations
        )
        if not _valid_github_precondition(
            plan.get("github_precondition"),
            projection_absent=creates,
        ):
            return False
        if state == "green":
            continue
        if (
            state != "red"
            or plan.get("reason_codes") != ["missing-github-projection"]
            or not operations
            or any(
                not isinstance(item, dict)
                or item.get("kind") != "create-projection"
                for item in operations
            )
        ):
            return False
    return True


def _apply_github_with_readback(
    github: object,
    identity: dict[str, object],
    item: dict[str, object],
) -> str:
    for attempt in range(2):
        try:
            github.apply(identity, item)
        except AmbiguousTransportError:
            readback = github.readback(identity, item)
            if readback is True:
                return "applied"
            if readback is None:
                return "unknown"
            if attempt == 0:
                continue
            return "failed"
        except TransientTransportError:
            if attempt == 0:
                continue
            return "failed"
        except (RuntimeError, ValueError):
            return "failed"
        readback = github.readback(identity, item)
        if readback is True:
            return "applied"
        if readback is None:
            return "unknown"
        return "failed"
    return "failed"


def apply_execution_plan(
    execution: dict[str, object],
    beads_cas: object,
    github: object,
    receipt_store: object,
    *,
    dry_run: bool = False,
) -> dict[str, object]:
    """Apply one planner result without last-write-wins or blind retries."""
    if not isinstance(execution, dict):
        return {"status": "red", "reason": "malformed_execution_plan"}
    plans = execution.get("plans")
    if not _execution_allowed(execution, plans):
        return {"status": "red", "reason": "planner_not_execution_safe"}
    assert isinstance(plans, list)
    if dry_run:
        return {
            "status": "dry_run",
            "planned_operations": sum(
                len(plan.get("operations", []))
                for plan in plans
                if isinstance(plan, dict) and isinstance(plan.get("operations"), list)
            ),
        }

    applied = 0
    for plan in plans:
        assert isinstance(plan, dict)
        try:
            pull, push = _operation_lists(plan)
        except ValueError:
            return {"status": "red", "reason": "malformed_execution_plan"}
        if not pull and not push:
            continue
        identity = plan.get("identity")
        allow_unbound = bool(push) and all(item.get("kind") == "create-projection" for item in push)
        if not _stable_identity(identity, allow_unbound=allow_unbound):
            return {"status": "red", "reason": "invalid_stable_identity"}
        assert isinstance(identity, dict)
        if allow_unbound:
            # A newly created Issue cannot have any previously imported comment
            # identities. Do not inherit delivery metadata from another event.
            github.set_imported_comment_ids(identity, [])
        elif "imported_comment_ids" in identity:
            imported = identity["imported_comment_ids"]
            if not _unique_comment_ids(imported):
                return {"status": "red", "reason": "invalid_imported_comment_identity"}
            github.set_imported_comment_ids(identity, imported)

        try:
            preflight = github.preflight(
                identity,
                plan["github_precondition"],
            )
        except (AttributeError, RuntimeError, TypeError, ValueError):
            return {"status": "unknown", "reason": "github_precondition_unproven"}
        if preflight is None:
            return {"status": "unknown", "reason": "github_precondition_unproven"}
        if preflight is not True:
            return {"status": "red", "reason": "github_precondition_changed"}

        if pull:
            try:
                expected_revision, patch = _pull_patch(pull)
            except RuntimeError as exc:
                return {"status": "red", "reason": str(exc)}
            except ValueError:
                return {"status": "red", "reason": "unsupported_beads_operation"}
            try:
                binding = _normalized_binding(github.binding(identity), identity)
                receipt_store.begin(identity, expected_revision, binding, _pending_pull(
                    patch, binding, plan.get("beads_convergence_base"),
                ))
            except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
                return {"status": "unknown", "reason": "pending_cas_persistence_unproven"}
            outcome = beads_cas.apply(str(identity["bead_id"]), expected_revision, patch)
            if not isinstance(outcome, dict) or outcome.get("outcome") == "conflict":
                return {"status": "red", "reason": "beads_revision_conflict"}
            if outcome.get("outcome") not in {"updated", "already_applied"}:
                return {"status": "unknown", "reason": "beads_cas_unproven"}
            comments = patch.get("comments")
            if isinstance(comments, list):
                imported = outcome.get("imported_comment_ids")
                requested = {
                    item.get("external_id")
                    for item in comments
                    if isinstance(item, dict)
                }
                if (
                    not isinstance(imported, list)
                    or any(not isinstance(item, str) or not item for item in imported)
                    or len(imported) != len(set(imported))
                    or not requested.issubset(set(imported))
                ):
                    return {"status": "unknown", "reason": "beads_comment_readback_unproven"}
                try:
                    github.set_imported_comment_ids(identity, imported)
                except (AttributeError, TypeError, ValueError):
                    return {"status": "unknown", "reason": "github_comment_receipt_unproven"}
            applied += len(pull)
            try:
                binding = _normalized_binding(github.binding(identity), identity)
                revision = outcome.get("revision")
                if not isinstance(revision, int) or isinstance(revision, bool) or revision == 0:
                    raise ValueError("Beads CAS revision is unproven")
                # Machine-field requests (or a concurrent outgoing change)
                # still need an export. Human-only imports do not alter the
                # managed projection and can advance its convergence base.
                receipt_revision = (
                    expected_revision if push or set(patch) & {"status", "priority", "type"}
                    else revision
                )
                _save_receipt(
                    receipt_store, identity, receipt_revision, binding,
                    applied_bead_revision=revision,
                )
            except (AttributeError, OSError, TypeError, ValueError):
                return {"status": "unknown", "reason": "receipt_readback_unproven"}
            if push:
                return {
                    "status": "replan_required",
                    "reason": "beads_revision_advanced",
                    "applied_operations": applied,
                }

        if push:
            revisions = {item.get("expected_revision") for item in push}
            if len(revisions) != 1:
                return {"status": "red", "reason": "mixed_expected_revisions"}
            expected_revision = next(iter(revisions))
            if not isinstance(expected_revision, int) or isinstance(expected_revision, bool) or expected_revision == 0:
                return {"status": "red", "reason": "invalid_expected_revision"}
            if any(not _valid_push_operation(item) for item in push):
                return {"status": "red", "reason": "unsupported_github_operation"}
            refresh = [item for item in push if item.get("kind") == "refresh-binding"]
            bind_existing = [item for item in push if item.get("kind") == "bind-existing-projection"]
            if (refresh or bind_existing) and len(push) != 1:
                return {"status": "red", "reason": "mixed_refresh_operation"}
            for item in (push if not refresh and not bind_existing else []):
                if allow_unbound:
                    try:
                        receipt_store.begin_creation(
                            identity, expected_revision, _pending_creation(item),
                        )
                    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
                        return {"status": "unknown", "reason": "pending_creation_persistence_unproven"}
                if not allow_unbound:
                    try:
                        binding = _normalized_binding(github.binding(identity), identity)
                        pending = github.pending_write(identity, item)
                        receipt_store.begin(identity, expected_revision, binding, pending)
                    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
                        return {"status": "unknown", "reason": "pending_write_persistence_unproven"}
                outcome = _apply_github_with_readback(github, identity, item)
                if outcome == "unknown":
                    return {"status": "unknown", "reason": "github_readback_unknown"}
                if outcome != "applied":
                    return {"status": "red", "reason": "github_write_failed"}
                applied += 1
            try:
                binding = _normalized_binding(
                    github.binding(identity),
                    identity,
                    allow_unbound=allow_unbound,
                )
            except (AttributeError, TypeError, ValueError):
                return {"status": "unknown", "reason": "github_binding_readback_invalid"}
            if refresh:
                expected = refresh[0]["value"]
                assert isinstance(expected, dict)
                if any(
                    expected[field] != binding[field]
                    for field in (
                        "github_updated_at",
                        "project_field_hash",
                        "projection_hash",
                    )
                ) or expected["bead_revision"] != expected_revision:
                    return {"status": "unknown", "reason": "refresh_readback_mismatch"}
                receipt_revision = expected_revision
            elif bind_existing or allow_unbound:
                if bind_existing:
                    value = bind_existing[0]["value"]
                    assert isinstance(value, dict)
                    if binding["projection_hash"] != value["projection_hash"]:
                        return {"status": "red", "reason": "pending_creation_projection_changed"}
                try:
                    receipt_store.begin_binding(
                        identity, expected_revision, binding,
                        _pending_binding(expected_revision, binding),
                    )
                except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
                    return {"status": "unknown", "reason": "pending_binding_persistence_unproven"}
                outcome = beads_cas.apply(
                    str(identity["bead_id"]), expected_revision,
                    _stable_binding_patch(binding),
                )
                if not isinstance(outcome, dict) or outcome.get("outcome") == "conflict":
                    return {"status": "red", "reason": "binding_revision_conflict"}
                if outcome.get("outcome") not in {"updated", "already_applied"}:
                    return {"status": "unknown", "reason": "binding_cas_unproven"}
                receipt_revision = outcome.get("revision")
            else:
                receipt_revision = expected_revision
            try:
                _save_receipt(
                    receipt_store,
                    identity,
                    receipt_revision,
                    binding,
                )
            except (AttributeError, OSError, TypeError, ValueError):
                return {"status": "unknown", "reason": "receipt_persistence_unproven"}

    return {"status": "green", "applied_operations": applied}


class BeadsCAS:
    """Exact-store adapter over `gc beads update-cas`; content stays on stdin."""

    def __init__(
        self,
        *,
        city: str,
        rig: str,
        gc_bin: str = "gc",
        run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    ) -> None:
        if not city or not rig or "/" in rig:
            raise ValueError("exact City and local Rig are required")
        self.city = city
        self.rig = rig
        self.gc_bin = gc_bin
        self.run = run

    def apply(self, bead_id: str, expected_revision: int, patch: dict[str, object]) -> dict[str, object]:
        command = [
            self.gc_bin,
            "--city",
            self.city,
            "beads",
            "update-cas",
            bead_id,
            "--store-ref=rig:" + self.rig,
            "--expected-revision=" + str(expected_revision),
            "--request-file=-",
            "--json",
        ]
        result = self.run(
            command,
            input=json.dumps(patch, ensure_ascii=False, sort_keys=True) + "\n",
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError("gc beads update-cas failed")
        try:
            payload = json.loads(result.stdout)
        except (TypeError, ValueError) as exc:
            raise RuntimeError("gc beads update-cas returned malformed JSON") from exc
        if (
            not isinstance(payload, dict)
            or payload.get("schema_version") != "1"
            or payload.get("ok") is not True
            or payload.get("bead_id") != bead_id
            or payload.get("store_ref") != "rig:" + self.rig
            or payload.get("expected_revision") != expected_revision
            or payload.get("outcome") not in {"updated", "already_applied", "conflict"}
            or not isinstance(payload.get("revision"), int)
            or isinstance(payload["revision"], bool)
            or payload["revision"] == 0
        ):
            raise RuntimeError("gc beads update-cas returned non-object JSON")
        comments = patch.get("comments")
        if isinstance(comments, list) and payload["outcome"] != "conflict":
            payload["imported_comment_ids"] = self._comment_receipt_readback(
                bead_id=bead_id,
                revision=int(payload["revision"]),
                comments=comments,
            )
        return payload

    def _comment_receipt_readback(
        self,
        *,
        bead_id: str,
        revision: int,
        comments: list[object],
    ) -> list[str]:
        command = [
            self.gc_bin,
            "--city",
            self.city,
            "beads",
            "snapshot",
            "--store-ref=rig:" + self.rig,
            "--json",
        ]
        result = self.run(
            command,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError("gc beads snapshot failed after comment CAS")
        try:
            payload = json.loads(result.stdout)
        except (TypeError, ValueError) as exc:
            raise RuntimeError("gc beads snapshot returned malformed JSON") from exc
        rows = payload.get("beads") if isinstance(payload, dict) else None
        if (
            not isinstance(payload, dict)
            or payload.get("schema_version") != "1"
            or payload.get("ok") is not True
            or payload.get("store_ref") != "rig:" + self.rig
            or not isinstance(rows, list)
        ):
            raise RuntimeError("gc beads snapshot did not prove comment CAS route")
        matches = [row for row in rows if isinstance(row, dict) and row.get("id") == bead_id]
        if len(matches) != 1 or matches[0].get("revision") != revision:
            raise RuntimeError("gc beads snapshot did not prove comment CAS revision")
        metadata = matches[0].get("metadata")
        raw_ids = metadata.get("github.imported_comment_ids") if isinstance(metadata, dict) else None
        try:
            imported = json.loads(raw_ids) if isinstance(raw_ids, str) else None
        except ValueError as exc:
            raise RuntimeError("gc beads comment receipt is malformed") from exc
        requested = {
            item.get("external_id")
            for item in comments
            if isinstance(item, dict)
        }
        if (
            not isinstance(imported, list)
            or any(not isinstance(item, str) or not item for item in imported)
            or len(imported) != len(set(imported))
            or not requested.issubset(set(imported))
        ):
            raise RuntimeError("gc beads comment receipt readback is incomplete")
        return list(imported)


def execute_runtime_from_environment(*, dry_run: bool = False) -> dict[str, object]:
    rig = os.environ.get("GC_RIG", "").strip()
    policy_bin = os.environ.get("GC_WORK_SYNC_POLICY_BIN", "").strip()
    policy_root = os.environ.get("GC_WORK_SYNC_POLICY_ROOT", "").strip()
    if (
        not rig
        or "/" in rig
        or not os.path.isabs(policy_bin)
        or not os.path.isabs(policy_root)
    ):
        raise RuntimeError("work-sync canonical runtime inputs are unavailable")
    contract = ContractRunner().read(
        repository=rig,
        policy_bin=policy_bin,
        policy_root=policy_root,
    )
    environment = runtime_environment(os.environ, contract)

    import github_intake_common as common

    with installation_token_context(
        repository_full_name=environment["repository_full_name"],
        token_env=_WORK_SYNC_TOKEN_ENV,
        common=common,
        permissions=contract.get("token_permissions"),
    ):
        transport = GitHubHTTPTransport(
            token_env=_WORK_SYNC_TOKEN_ENV,
            api_url=os.environ.get("GC_GITHUB_API_BASE", "https://api.github.com"),
        )
        event, actor_node_id = runtime_event(environment, contract, transport)
        max_pages = contract.get("max_pages_per_run")
        if (
            not isinstance(max_pages, int)
            or isinstance(max_pages, bool)
            or max_pages <= 0
            or max_pages > 100
        ):
            raise RuntimeError("work-sync pagination contract is invalid")
        projects = GitHubProjectSchemaReader(
            organization=str(contract["organization"]),
            transport=transport,
            max_pages=max_pages,
        ).read_contract(contract)
        receipt_store = WorkSyncReceiptStore(
            repository=environment["repository"],
            store_ref="rig:" + environment["rig"],
            # Cross-City dispatch inherits the ingress service environment.
            # Reconciliation and webhook execution must share the target
            # City's existing GitHub Pack state, not the ingress City's root.
            data_root=os.path.join(environment["city"], ".gc", "services", "github", "data"),
        )
        beads_reader = BeadsSnapshotReader(
            city=environment["city"],
            rig=environment["rig"],
            repository=environment["repository"],
            load_receipt=receipt_store.load,
        )
        github_reader = GitHubSnapshotReader(
            organization=str(contract["organization"]),
            repository=environment["repository"],
            transport=transport,
            projection_actor_node_id=actor_node_id,
            max_pages=max_pages,
        )
        github_writer = GitHubProjectionWriter(
            organization=str(contract["organization"]),
            repository=environment["repository"],
            transport=transport,
            snapshot_reader=github_reader,
            projects=projects,
            managed_block=contract["managed_block"],
            event=event,
        )
        return WorkSyncReconciler(
            contract=contract,
            projects=projects,
            event=event,
            beads_reader=beads_reader,
            github_reader=github_reader,
            planner=PlannerRunner(),
            beads_cas=BeadsCAS(
                city=environment["city"],
                rig=environment["rig"],
            ),
            github_writer=github_writer,
            receipt_store=receipt_store,
            policy_bin=environment["policy_bin"],
            policy_root=environment["policy_root"],
        ).run(dry_run=dry_run or contract["live_mutations"] is not True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="github-work-sync")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    dry_run = args.dry_run or os.environ.get("GC_GITHUB_WORK_SYNC_DRY_RUN") == "1"
    try:
        result = execute_runtime_from_environment(dry_run=dry_run)
    except (OSError, RuntimeError, TypeError, ValueError):
        result = {
            "status": "unknown",
            "reason": "runtime_failed",
            "terminal_readback": False,
        }
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    if result.get("status") in {"green", "dry_run"}:
        return 0
    if result.get("status") == "red":
        return 2
    return 3


if __name__ == "__main__":
    sys.exit(main())
