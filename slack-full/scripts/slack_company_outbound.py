#!/usr/bin/env python3
"""Company-rooms outbound surface for the slack-full pack (company rooms 2b).

Python owns company *outbound* (the Discord ownership split): posting
intents, per-agent token files, ``chat.postMessage``, delegation-record
creation, receipt-based lazy recovery, and pruning. Go owns ingress
(author resolution, trust, result-claim/expiry transitions, the mutable
current-turn pointer, and immutable by-reference turn records). Both sides
share on-disk state under the adapter state root, serialized by the lock
contract and validated fail-closed on both sides.

This module is a library: the ``gc slack delegate`` verb
(``cmd_delegate``/``cmd_cancel``) and ``reply-current``'s company-context
path both call into it. Every filename/lock/nonce derivation here is a
byte-for-byte match of ``adapter/company_sanitize.go`` — the golden
fixtures in ``tests/fixtures/company/`` pin the two sides together.
"""

from __future__ import annotations

import errno
import fcntl
import hashlib
import json
import os
import pathlib
import re
import stat
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any

SCHEMA_VERSION = 1

DEFAULT_SLACK_API_BASE = "https://slack.com/api"

# Intent lifetime and retry policy defaults (shared cross-language contract).
INTENT_TTL_SECONDS = 86400
DEFAULT_MAX_ATTEMPTS = 3
RETRY_DEADLINE_SECONDS = 120

# Terminal/expired records and terminal intents are pruned after this many
# seconds. Pinned at 7 days (receipt/Discord parity); a caller-supplied
# retention below the floor is clamped up, never honored — terminal state is
# the crash-recovery memory the reconciler leans on.
PRUNE_RETENTION_SECONDS = 7 * 86400
# One-hour clock-skew margin added to the maximum record ttl to derive the
# prune retention floor: a record must never be prunable on Python's clock
# while it is still S2-compatible on Go's clock (Phase 2 pruning contract).
PRUNE_CLOCK_SKEW_MARGIN_SECONDS = 3600
# Retention floor: ``>=`` the maximum record ``ttl_seconds`` plus the clock-skew
# margin. Every record uses ``INTENT_TTL_SECONDS`` so this is the static floor;
# ``prune`` lifts it further if it observes a larger ttl on disk.
PRUNE_RETENTION_FLOOR_SECONDS = INTENT_TTL_SECONDS + PRUNE_CLOCK_SKEW_MARGIN_SECONDS

# Slack app-level errors (HTTP 200 with ok:false) that are worth retrying —
# everything else at ok:false is a definitive rejection (bad channel, bad
# auth, not-in-channel, ...) that must fail the intent, never repost.
_TRANSIENT_SLACK_ERRORS = frozenset({
    "ratelimited",
    "internal_error",
    "service_unavailable",
    "fatal_error",
    "request_timeout",
    "timeout",
})

# Seam so tests can drive the retry loop without real sleeping.
_sleep = time.sleep


class OutboundError(RuntimeError):
    """Raised on an invalid input or an unrecoverable outbound failure."""


class DefinitivePostError(RuntimeError):
    """A provider rejection that must mark the intent ``failed`` (no repost)."""


class TransientPostError(RuntimeError):
    """A timeout/5xx/rate-limit that must reconcile before any repost."""


# --- path resolution ------------------------------------------------------
#
# Precedence mirrors Phase 1 exactly: explicit env override >
# <GC_CITY_PATH>/.gc/slack/<leaf> > /tmp/gc-slack-adapter/<leaf>.

def _state_dir(env_override: str, leaf: str) -> pathlib.Path:
    override = os.environ.get(env_override, "").strip()
    if override:
        return pathlib.Path(override)
    city = os.environ.get("GC_CITY_PATH", "").strip()
    if city:
        return pathlib.Path(city) / ".gc" / "slack" / leaf
    return pathlib.Path("/tmp/gc-slack-adapter") / leaf


def secrets_dir() -> pathlib.Path:
    return _state_dir("SLACK_COMPANY_SECRETS_DIR", "secrets")


def intents_dir() -> pathlib.Path:
    return _state_dir("SLACK_COMPANY_INTENTS_DIR", "company-delegation-intents")


def delegations_dir() -> pathlib.Path:
    return _state_dir("SLACK_COMPANY_DELEGATIONS_DIR", "company-delegations")


def turns_dir() -> pathlib.Path:
    return _state_dir("SLACK_COMPANY_TURNS_DIR", "company-current-turn")


def locks_dir() -> pathlib.Path:
    return _state_dir("SLACK_COMPANY_LOCKS_DIR", "locks")


def ingress_dir() -> pathlib.Path:
    return _state_dir("SLACK_COMPANY_INGRESS_DIR", "chat-ingress")


def bodies_dir() -> pathlib.Path:
    """The body sidecar subdirectory under the ingress store (Phase 5 split)."""
    return ingress_dir() / "bodies"


# --- filename / lock / nonce sanitizer (byte-for-byte cross-language) ------

def component_safe(component: str) -> bool:
    """Whether a filename component passes the sanitizer unchanged.

    A component is hashed when it contains a byte outside ``[A-Za-z0-9._-]``,
    begins with ``.``, equals ``..``, or exceeds 64 bytes. Length and the
    byte alphabet are checked on the UTF-8 encoding so this matches Go's
    ``companyComponentSafe`` byte for byte.
    """
    raw = component.encode("utf-8")
    if len(raw) > 64 or component == ".." or component.startswith("."):
        return False
    for ch in raw:
        if (
            0x41 <= ch <= 0x5A  # A-Z
            or 0x61 <= ch <= 0x7A  # a-z
            or 0x30 <= ch <= 0x39  # 0-9
            or ch in (0x2E, 0x5F, 0x2D)  # . _ -
        ):
            continue
        return False
    return True


def sanitize_component(component: str) -> str:
    """``component`` when filename-safe, else ``h`` + sha256hex(component)[:16]."""
    if component_safe(component):
        return component
    digest = hashlib.sha256(component.encode("utf-8")).hexdigest()
    return "h" + digest[:16]


def tuple_digest12(team_id: str, channel_id: str, ts: str) -> str:
    """12-hex disambiguating suffix over the raw NUL-joined origin."""
    joined = f"{team_id}\x00{channel_id}\x00{ts}".encode("utf-8")
    return hashlib.sha256(joined).hexdigest()[:12]


def delegation_filename(team_id: str, channel_id: str, ts: str) -> str:
    """Delegations-registry filename keyed by (team, channel, posted ts)."""
    return (
        "dg-"
        + sanitize_component(team_id) + "-"
        + sanitize_component(channel_id) + "-"
        + sanitize_component(ts) + "-"
        + tuple_digest12(team_id, channel_id, ts) + ".json"
    )


def lock_filename(label: str, *key_fields: str) -> str:
    """``<label>-<sha256hex(NUL-joined key fields)[:16]>.lock``."""
    joined = "\x00".join(key_fields).encode("utf-8")
    return label + "-" + hashlib.sha256(joined).hexdigest()[:16] + ".lock"


def dtuple_lock_name(
    team_id: str,
    channel_id: str,
    thread_root_ts: str,
    responder_bot_user_id: str,
    requester_bot_user_id: str,
) -> str:
    return lock_filename(
        "dtuple", team_id, channel_id, thread_root_ts,
        responder_bot_user_id, requester_bot_user_id,
    )


def intent_lock_name(nonce: str) -> str:
    return lock_filename("intent", nonce)


# --- synthesis group + lock-name parity (Phase 3) --------------------------
#
# Python never takes ``dgroup`` or ``dgser`` (delegate/cancel stay
# ``dtuple``-only, per the lock-ordering contract); these derivations exist
# purely so the fixture parity pins prove both languages compute identical
# ``.lock`` names for the same key fields.

# The canonical durable root shared by sibling delegations, in this pinned
# field order (S: synthesis group, D1). All five must be non-empty for a
# record to belong to a group.
_SYNTHESIS_GROUP_FIELDS = (
    "team_id", "channel_id", "thread_root_ts",
    "requester_bot_user_id", "requester_session",
)


def synthesis_group(record: dict[str, Any]) -> tuple[str, ...] | None:
    """The 5-tuple synthesis group for ``record``, or ``None``.

    ``(team_id, channel_id, thread_root_ts, requester_bot_user_id,
    requester_session)`` iff all five fields are non-empty strings; otherwise
    the record has no group and is never claimable.
    """
    if not isinstance(record, dict):
        return None
    fields = tuple(record.get(k, "") for k in _SYNTHESIS_GROUP_FIELDS)
    if any(not isinstance(f, str) or not f for f in fields):
        return None
    return fields


def dgroup_lock_name(*fields: str) -> str:
    """``dgroup`` lock filename over ``fields`` (parity only; never taken here).

    Called with the 5 group fields in pinned order, or with the 6-field
    fallback key (``"unavailable"`` + team, channel, thread_root_ts,
    responder_bot_user_id, requester_bot_user_id) when a claim's preflight scan
    finds no matching record.
    """
    return lock_filename("dgroup", *fields)


def dgser_lock_name(team_id: str, channel_id: str, thread_root_ts: str) -> str:
    """``dgser`` root-serialization lock filename (parity only, never taken here)."""
    return lock_filename("dgser", team_id, channel_id, thread_root_ts)


# --- synthesis snapshot validator (S10, cross-language normalizer) ----------

def _nonneg_int(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _unavailable_synthesis_state() -> dict[str, Any]:
    """The normalized *unavailable* shape (version 0, zero counts, ready false)."""
    return {
        "synthesis_state_version": 0,
        "synthesis_state_available": False,
        "compatible_delegation_count": 0,
        "responded_delegation_count": 0,
        "pending_delegation_count": 0,
        "pending_delegations": [],
        "synthesis_ready": False,
        "synthesis_snapshot_at": "",
    }


def synthesis_state(record: dict[str, Any]) -> dict[str, Any]:
    """S10 normalizer over a delegation record's stored synthesis snapshot.

    Identical semantics to the Go ``normalizeSynthesisState``: a stored
    snapshot is *available* iff ``synthesis_state_version == 1``,
    ``synthesis_state_available == true``, all three counts are non-negative
    ints with ``responded + pending == compatible`` and ``compatible > 0``, the
    pending list length equals ``pending_delegation_count`` with unique
    non-empty ``delegation_ts``, ``synthesis_snapshot_at`` is non-empty, and the
    stored ``synthesis_ready`` equals the recomputed ``compatible > 0 &&
    responded == compatible && pending == 0``. Anything else normalizes to the
    unavailable shape. Consumers (the envelope, the reply-current gate) read
    only the normalized form.

    Canonical cross-language rule (pinned; Go mirrors it byte-for-byte):
    ``synthesis_state_version`` and the three counts must be *exact* JSON
    integers — a bool, a float (including ``1.0``), or a numeric string is
    rejected (Python's ``bool`` is an ``int`` subclass, so it is excluded
    explicitly via :func:`_nonneg_int`). No string field is whitespace-trimmed:
    a padded ``synthesis_snapshot_at`` or ``delegation_ts`` is taken verbatim,
    so padded values stay non-empty and stay distinct.
    """
    if not isinstance(record, dict):
        return _unavailable_synthesis_state()
    if _nonneg_int(record.get("synthesis_state_version")) != 1:
        return _unavailable_synthesis_state()
    if record.get("synthesis_state_available") is not True:
        return _unavailable_synthesis_state()

    compatible = _nonneg_int(record.get("compatible_delegation_count"))
    responded = _nonneg_int(record.get("responded_delegation_count"))
    pending = _nonneg_int(record.get("pending_delegation_count"))
    if compatible is None or responded is None or pending is None:
        return _unavailable_synthesis_state()
    if responded + pending != compatible or compatible <= 0:
        return _unavailable_synthesis_state()

    raw_pending = record.get("pending_delegations")
    if not isinstance(raw_pending, list) or len(raw_pending) != pending:
        return _unavailable_synthesis_state()
    seen: set[str] = set()
    norm_pending: list[dict[str, str]] = []
    for entry in raw_pending:
        if not isinstance(entry, dict):
            return _unavailable_synthesis_state()
        dts = entry.get("delegation_ts")
        if not isinstance(dts, str) or not dts or dts in seen:
            return _unavailable_synthesis_state()
        seen.add(dts)

        def _s(key: str) -> str:
            val = entry.get(key, "")
            return val if isinstance(val, str) else ""

        norm_pending.append({
            "delegation_ts": dts,
            "delegation_key": _s("delegation_key"),
            "expected_responder_agent": _s("expected_responder_agent"),
            "expected_responder_bot_user_id": _s("expected_responder_bot_user_id"),
        })

    snapshot_at = record.get("synthesis_snapshot_at")
    if not isinstance(snapshot_at, str) or not snapshot_at:
        return _unavailable_synthesis_state()

    recomputed_ready = compatible > 0 and responded == compatible and pending == 0
    stored_ready = record.get("synthesis_ready")
    if not isinstance(stored_ready, bool) or stored_ready != recomputed_ready:
        return _unavailable_synthesis_state()

    return {
        "synthesis_state_version": 1,
        "synthesis_state_available": True,
        "compatible_delegation_count": compatible,
        "responded_delegation_count": responded,
        "pending_delegation_count": pending,
        "pending_delegations": norm_pending,
        "synthesis_ready": recomputed_ready,
        "synthesis_snapshot_at": snapshot_at,
    }


def compute_nonce(
    *,
    source_app_id: str,
    source_bot_user_id: str,
    target_agent: str,
    target_bot_user_id: str,
    team_id: str,
    channel_id: str,
    human_root_ts: str,
    body_sha256: str,
    retry_seq: int,
) -> str:
    """``gcs-`` + first 20 hex of sha256 over the canonical anticipated record.

    The nine fields are NUL-joined (``retry_seq`` as its decimal string), so a
    fresh body or a fresh ``retry_seq`` mints a distinct nonce while a crash
    retry of the same in-flight delegation reproduces it exactly.
    """
    fields = (
        source_app_id,
        source_bot_user_id,
        target_agent,
        target_bot_user_id,
        team_id,
        channel_id,
        human_root_ts,
        body_sha256,
        str(int(retry_seq)),
    )
    digest = hashlib.sha256("\x00".join(fields).encode("utf-8")).hexdigest()
    return "gcs-" + digest[:20]


def body_sha256(body: str) -> str:
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def derive_human_root_ts(thread_ts: str, ts: str) -> str:
    """Normative root derivation: ``thread_ts`` when non-empty, else ``ts``.

    ``thread_ts`` on ``chat.postMessage`` must always be a parent — Slack
    documents replying to a reply as invalid — so a threaded trigger derives
    the parent root and an unthreaded trigger roots at its own ts.
    """
    thread_ts = (thread_ts or "").strip()
    return thread_ts if thread_ts else (ts or "").strip()


# --- time helpers ---------------------------------------------------------

def _now() -> datetime:
    return datetime.now(timezone.utc)


def _rfc3339(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_rfc3339(value: str) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


# --- advisory lock --------------------------------------------------------

class _FlockGuard:
    """A held ``flock(LOCK_EX)``; released on context exit, nil-safe."""

    def __init__(self, fd: int, path: pathlib.Path) -> None:
        self._fd = fd
        self._path = path

    def __enter__(self) -> "_FlockGuard":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.release()

    def release(self) -> None:
        if self._fd < 0:
            return
        try:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
        finally:
            os.close(self._fd)
            self._fd = -1


def acquire_lock(name: str) -> _FlockGuard:
    """Take an exclusive advisory lock on ``locks/<name>`` (blocking)."""
    ldir = locks_dir()
    ldir.mkdir(parents=True, exist_ok=True, mode=0o700)
    path = ldir / name
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
    except BaseException:
        os.close(fd)
        raise
    return _FlockGuard(fd, path)


# --- token loader ---------------------------------------------------------

def load_bot_token(agent: str) -> str:
    """Read ``secrets/bot-token-<agent>.txt`` with permission/symlink refusal.

    The directory must be 0700, the token file 0600, and neither may be a
    symlink. These are validation (not trust) checks: a token file with lax
    permissions is refused so a same-UID squatter cannot substitute one.
    """
    if not agent:
        raise OutboundError("token load requires an agent name")
    sdir = secrets_dir()
    try:
        dinfo = os.lstat(sdir)
    except OSError as exc:
        raise OutboundError(f"secrets dir {sdir} is unavailable: {exc}") from exc
    if stat.S_ISLNK(dinfo.st_mode):
        raise OutboundError(f"secrets dir {sdir} is a symlink; refusing")
    if not stat.S_ISDIR(dinfo.st_mode):
        raise OutboundError(f"secrets dir {sdir} is not a directory")
    if stat.S_IMODE(dinfo.st_mode) != 0o700:
        raise OutboundError(
            f"secrets dir {sdir} must be mode 0700, got {stat.S_IMODE(dinfo.st_mode):04o}")

    path = sdir / f"bot-token-{agent}.txt"
    try:
        finfo = os.lstat(path)
    except OSError as exc:
        raise OutboundError(f"no bot token for agent {agent!r} at {path}: {exc}") from exc
    if stat.S_ISLNK(finfo.st_mode):
        raise OutboundError(f"token file {path} is a symlink; refusing")
    if not stat.S_ISREG(finfo.st_mode):
        raise OutboundError(f"token file {path} is not a regular file")
    if stat.S_IMODE(finfo.st_mode) != 0o600:
        raise OutboundError(
            f"token file {path} must be mode 0600, got {stat.S_IMODE(finfo.st_mode):04o}")
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd, "r", encoding="utf-8") as fh:
            token = fh.read().strip()
    except OSError as exc:
        raise OutboundError(f"cannot read token file {path}: {exc}") from exc
    if not token:
        raise OutboundError(f"token file {path} is empty")
    return token


# --- atomic / create-once writers -----------------------------------------

def _dumps(obj: dict[str, Any]) -> bytes:
    return (json.dumps(obj, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _atomic_write_json(path: pathlib.Path, obj: dict[str, Any]) -> None:
    """Atomic tmp+fsync+rename, 0600 file in a 0700 dir; symlinked dest refused."""
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    try:
        dest = os.lstat(path)
    except FileNotFoundError:
        dest = None
    if dest is not None and stat.S_ISLNK(dest.st_mode):
        raise OutboundError(f"refusing to write {path}: destination is a symlink")
    data = _dumps(obj)
    fd, tmp_name = tempfile.mkstemp(dir=str(path.parent), prefix=f".{path.name}.", suffix=".tmp")
    tmp = pathlib.Path(tmp_name)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    try:
        dfd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)
    except OSError:
        pass


def _create_once_json(path: pathlib.Path, obj: dict[str, Any]) -> bool:
    """Create ``path`` exactly once (``O_EXCL``). Returns False on ``EEXIST``.

    An existing file is never overwritten — the caller adopts it. The write is
    fsynced before the fd closes so a crash leaves either nothing or the full
    record.
    """
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    except FileExistsError:
        return False
    except OSError as exc:
        if exc.errno == errno.EEXIST:
            return False
        raise
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(_dumps(obj))
            fh.flush()
            os.fsync(fh.fileno())
    except BaseException:
        try:
            os.unlink(path)
        except OSError:
            pass
        raise
    return True


def _read_json(path: pathlib.Path) -> dict[str, Any] | None:
    """Read+parse a JSON object, refusing symlinks. None if absent/corrupt."""
    try:
        info = os.lstat(path)
    except OSError:
        return None
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        return None
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd, "rb") as fh:
            raw = fh.read()
    except OSError:
        return None
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    return data


# --- record parsers (fail-closed; validate the golden fixtures) -----------

_INTENT_STATUSES = frozenset({"prepared", "posting", "published", "failed", "expired"})
_INTENT_TERMINAL = frozenset({"published", "failed", "expired"})
# An intent's ``op`` labels which outbound post it durably backs. The receipt
# scan gates on the op's Slack metadata ``event_type`` so a delegation echo can
# never be mistaken for a result echo carrying the same (delegation) nonce.
_INTENT_OPS = frozenset({"delegation", "result", "synthesis", "dm"})
_OP_EVENT_TYPES = {
    "delegation": "gc_delegation",
    "result": "gc_delegation_result",
    "synthesis": "gc_delegation_synthesis",
    "dm": "gc_dm_reply",
}
_DELEGATION_STATUSES = frozenset({"pending", "result_claimed", "expired"})
_DELEGATION_TERMINAL = frozenset({"result_claimed", "expired"})
_TURN_KINDS = frozenset({
    "ambient", "thread_ambient", "targeted", "peer_delegation", "peer_result",
    "peer_input", "dm", "mpim",
})
# The direct-message family (per-agent DM + group DM) shares the empty-room
# relaxation, the required owner_app_id, and the dm_bindings spoof guard.
_DM_FAMILY_KINDS = frozenset({"dm", "mpim"})
_TURN_REF_RE = re.compile(r"^gct-[0-9a-f]{20}$")

_INTENT_STR_FIELDS = (
    "nonce", "status", "created_at", "updated_at", "retry_deadline",
    "source_agent", "source_app_id", "source_bot_user_id",
    "target_agent", "target_bot_user_id", "team_id", "channel_id", "room",
    "human_root_ts", "requester_session", "body_sha256",
)
_DELEGATION_STR_FIELDS = (
    "nonce", "room", "team_id", "channel_id", "ts", "thread_root_ts",
    "requester_agent", "requester_bot_user_id", "requester_session",
    "expected_responder_agent", "expected_responder_bot_user_id",
    "created_at", "status",
)
_TURN_STR_FIELDS = (
    "session", "receipt_id", "team_id", "channel_id", "ts", "room", "kind",
    "thread_root_ts", "agent", "delivered_at",
)


def _require(data: dict[str, Any], key: str, ctx: str) -> Any:
    if key not in data:
        raise OutboundError(f"{ctx} missing required field {key!r}")
    return data[key]


def _require_str(data: dict[str, Any], key: str, ctx: str, *, allow_empty: bool = False) -> str:
    val = _require(data, key, ctx)
    if not isinstance(val, str) or (not allow_empty and not val):
        raise OutboundError(f"{ctx} field {key!r} must be a non-empty string, got {val!r}")
    return val


def parse_intent(data: dict[str, Any]) -> dict[str, Any]:
    ctx = "intent"
    if not isinstance(data, dict):
        raise OutboundError("intent must be an object")
    if data.get("schema_version") != SCHEMA_VERSION:
        raise OutboundError(f"intent schema_version must be {SCHEMA_VERSION}")
    # A DM reply intent (op=dm) legitimately has no room — the pointer's room is
    # empty for kind dm — so `room` is permitted-empty there. Every other op
    # keeps room required non-empty. Intents are Python-owned (the outbound
    # side of the ownership split), so this relaxation is Python-local.
    op_for_room = data.get("op", "delegation")
    for key in _INTENT_STR_FIELDS:
        allow_empty = key in ("created_at", "updated_at", "retry_deadline") or (
            key == "room" and op_for_room == "dm")
        _require_str(data, key, ctx, allow_empty=allow_empty)
    if data["status"] not in _INTENT_STATUSES:
        raise OutboundError(f"intent status invalid: {data['status']!r}")
    if not data["nonce"].startswith("gcs-"):
        raise OutboundError(f"intent nonce must start with 'gcs-', got {data['nonce']!r}")
    # ``op``/``metadata_nonce`` are additive (default to a delegation post whose
    # metadata carries its own nonce) so a pre-op intent parses unchanged.
    op = data.setdefault("op", "delegation")
    if op not in _INTENT_OPS:
        raise OutboundError(f"intent op invalid: {op!r}")
    mnonce = data.setdefault("metadata_nonce", data["nonce"])
    if not isinstance(mnonce, str) or not mnonce:
        raise OutboundError("intent metadata_nonce must be a non-empty string")
    for key in ("retry_seq", "attempts", "max_attempts", "ttl_seconds"):
        val = data.get(key)
        if isinstance(val, bool) or not isinstance(val, int) or val < 0:
            raise OutboundError(f"intent field {key!r} must be a non-negative int, got {val!r}")
    # posted_ts is present but may be empty until published.
    if not isinstance(data.get("posted_ts", ""), str):
        raise OutboundError("intent posted_ts must be a string")
    return data


def parse_delegation(data: dict[str, Any]) -> dict[str, Any]:
    ctx = "delegation"
    if not isinstance(data, dict):
        raise OutboundError("delegation must be an object")
    if data.get("schema_version") != SCHEMA_VERSION:
        raise OutboundError(f"delegation schema_version must be {SCHEMA_VERSION}")
    gen = data.get("generation")
    if isinstance(gen, bool) or not isinstance(gen, int) or gen < 1:
        raise OutboundError(f"delegation generation must be a positive int, got {gen!r}")
    for key in _DELEGATION_STR_FIELDS:
        _require_str(data, key, ctx)
    if data["status"] not in _DELEGATION_STATUSES:
        raise OutboundError(f"delegation status invalid: {data['status']!r}")
    ttl = data.get("ttl_seconds")
    if isinstance(ttl, bool) or not isinstance(ttl, int) or ttl < 0:
        raise OutboundError(f"delegation ttl_seconds must be a non-negative int, got {ttl!r}")
    for key in ("result_ts", "result_claimed_at"):
        if not isinstance(data.get(key, ""), str):
            raise OutboundError(f"delegation {key!r} must be a string")
    return data


def parse_current_turn(data: dict[str, Any]) -> dict[str, Any]:
    ctx = "current_turn"
    if not isinstance(data, dict):
        raise OutboundError("current_turn must be an object")
    if data.get("schema_version") != SCHEMA_VERSION:
        raise OutboundError(f"current_turn schema_version must be {SCHEMA_VERSION}")
    kind = data.get("kind")
    if kind not in _TURN_KINDS:
        raise OutboundError(f"current_turn kind invalid: {kind!r}")
    for key in _TURN_STR_FIELDS:
        # A dm-family turn (dm/mpim) has no room: the pointer's `room` field is
        # permitted-empty for those kinds (validator relaxation, cross-language).
        # Every other field stays required non-empty exactly as before.
        allow_empty = key == "room" and kind in _DM_FAMILY_KINDS
        _require_str(data, key, ctx, allow_empty=allow_empty)
    # owner_app_id is additive: required non-empty for a dm-family turn (the app
    # whose token posts the reply and whose directory join names the acting
    # agent), absent/empty for room pointers — a pre-Phase-4 room pointer parses
    # byte-unchanged (setdefault only touches the in-memory copy, never disk).
    # For dm the app is the sole owner; for mpim it is the WOKEN agent's own app.
    owner_app_id = data.setdefault("owner_app_id", "")
    if not isinstance(owner_app_id, str):
        raise OutboundError("current_turn owner_app_id must be a string")
    if kind in _DM_FAMILY_KINDS and not owner_app_id:
        raise OutboundError(f"current_turn kind {kind} requires a non-empty owner_app_id")
    # A delegation_key is required only for the correlated peer kinds
    # (peer_delegation/peer_result). Uncorrelated peer wakes (peer_input),
    # room-context turns (ambient/thread_ambient/targeted), and DM turns carry
    # no key — Go emits them keyless, and the reply path posts into the thread
    # root without a record.
    if kind in ("peer_delegation", "peer_result"):
        _require_str(data, "delegation_key", ctx)
    # ``turn_ref`` is additive so pre-immutable mutable pointers remain
    # readable during rollout.  Immutable records require it at their reader
    # boundary below; when present anywhere it must use the opaque, filename-
    # safe format minted by the Go delivery worker.
    turn_ref = data.setdefault("turn_ref", "")
    if not isinstance(turn_ref, str):
        raise OutboundError("current_turn turn_ref must be a string")
    if turn_ref and _TURN_REF_RE.fullmatch(turn_ref) is None:
        raise OutboundError(f"current_turn turn_ref invalid: {turn_ref!r}")
    return data


# --- intent store (create-once + attempts-CAS under the intent lock) -------

def _intent_path(nonce: str) -> pathlib.Path:
    return intents_dir() / f"{nonce}.json"


def list_intents() -> list[dict[str, Any]]:
    """Every parseable intent record (corrupt files skipped)."""
    out: list[dict[str, Any]] = []
    idir = intents_dir()
    try:
        names = sorted(os.listdir(idir))
    except OSError:
        return out
    for name in names:
        if not name.endswith(".json"):
            continue
        data = _read_json(idir / name)
        if data is None:
            continue
        try:
            out.append(parse_intent(data))
        except OutboundError:
            continue
    return out


def _tuple8(data: dict[str, Any]) -> tuple[str, ...]:
    return (
        data["source_app_id"], data["source_bot_user_id"],
        data["target_agent"], data["target_bot_user_id"],
        data["team_id"], data["channel_id"],
        data["human_root_ts"], data["body_sha256"],
    )


def next_retry_seq(tuple8: tuple[str, ...]) -> int:
    """One past the highest ``retry_seq`` seen for the tuple (0 when none).

    A monotonic max+1 (not a surviving-file count) so pruning terminal intents
    can never shrink the sequence and re-mint a nonce a stale receipt could
    still reconcile against — the pruner always retains the highest-seq intent
    per tuple as the watermark.
    """
    seqs = [int(i.get("retry_seq", 0)) for i in list_intents() if _tuple8(i) == tuple8]
    return (max(seqs) + 1) if seqs else 0


def find_nonterminal_intent_for_tuple(tuple8: tuple[str, ...]) -> dict[str, Any] | None:
    for intent in list_intents():
        if _tuple8(intent) == tuple8 and intent["status"] not in _INTENT_TERMINAL:
            return intent
    return None


def reusable_intent_for_tuple(tuple8: tuple[str, ...]) -> dict[str, Any] | None:
    """The intent a re-run should reuse for this tuple, or None.

    Prefers a ``published`` intent (the post already landed → idempotent
    success, never repost) over a non-terminal one (``posting`` reconciles;
    ``prepared`` resumes). A failed/expired intent is ignored so a fresh
    higher-seq retry can proceed.
    """
    published: dict[str, Any] | None = None
    nonterminal: dict[str, Any] | None = None
    for intent in list_intents():
        if _tuple8(intent) != tuple8:
            continue
        status = intent["status"]
        if status == "published":
            published = intent
        elif status not in _INTENT_TERMINAL:
            nonterminal = intent
    return published or nonterminal


def build_intent(
    *,
    nonce: str,
    retry_seq: int,
    source_agent: str,
    source_app_id: str,
    source_bot_user_id: str,
    target_agent: str,
    target_bot_user_id: str,
    team_id: str,
    channel_id: str,
    room: str,
    human_root_ts: str,
    requester_session: str,
    body_hex: str,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    op: str = "delegation",
    metadata_nonce: str = "",
) -> dict[str, Any]:
    now = _rfc3339(_now())
    return {
        "schema_version": SCHEMA_VERSION,
        "nonce": nonce,
        "op": op,
        "metadata_nonce": metadata_nonce or nonce,
        "retry_seq": int(retry_seq),
        "status": "prepared",
        "attempts": 0,
        "max_attempts": int(max_attempts),
        "created_at": now,
        "updated_at": now,
        "retry_deadline": "",
        "ttl_seconds": INTENT_TTL_SECONDS,
        "source_agent": source_agent,
        "source_app_id": source_app_id,
        "source_bot_user_id": source_bot_user_id,
        "target_agent": target_agent,
        "target_bot_user_id": target_bot_user_id,
        "team_id": team_id,
        "channel_id": channel_id,
        "room": room,
        "human_root_ts": human_root_ts,
        "requester_session": requester_session,
        "body_sha256": body_hex,
        "posted_ts": "",
    }


def write_intent(intent: dict[str, Any]) -> None:
    _atomic_write_json(_intent_path(intent["nonce"]), intent)


def create_intent_once(intent: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    """Create the intent file exactly once. On ``EEXIST`` adopt the existing one."""
    path = _intent_path(intent["nonce"])
    if _create_once_json(path, intent):
        return intent, True
    existing = _read_json(path)
    if existing is None:
        raise OutboundError(f"intent {intent['nonce']} exists but is unreadable")
    return parse_intent(existing), False


def update_intent(nonce: str, mutate) -> dict[str, Any]:
    """Read-modify-write the intent under its lock (the attempts-CAS section)."""
    with acquire_lock(intent_lock_name(nonce)):
        data = _read_json(_intent_path(nonce))
        if data is None:
            raise OutboundError(f"intent {nonce} vanished under the lock")
        intent = parse_intent(data)
        mutate(intent)
        intent["updated_at"] = _rfc3339(_now())
        write_intent(intent)
        return intent


def intent_mark_posting(nonce: str) -> dict[str, Any]:
    def _m(intent: dict[str, Any]) -> None:
        intent["status"] = "posting"
        intent["attempts"] = int(intent.get("attempts", 0)) + 1
        if not intent.get("retry_deadline"):
            deadline = _now().timestamp() + RETRY_DEADLINE_SECONDS
            intent["retry_deadline"] = _rfc3339(datetime.fromtimestamp(deadline, timezone.utc))
    return update_intent(nonce, _m)


def intent_mark_published(nonce: str, posted_ts: str) -> dict[str, Any]:
    def _m(intent: dict[str, Any]) -> None:
        intent["status"] = "published"
        intent["posted_ts"] = posted_ts
    return update_intent(nonce, _m)


def intent_mark_failed(nonce: str, detail: str = "") -> dict[str, Any]:
    def _m(intent: dict[str, Any]) -> None:
        intent["status"] = "failed"
        if detail:
            intent["detail"] = detail
    return update_intent(nonce, _m)


# --- delegation records (create-once; O_EXCL, EEXIST adopts) ---------------

def _delegation_path(team_id: str, channel_id: str, ts: str) -> pathlib.Path:
    return delegations_dir() / delegation_filename(team_id, channel_id, ts)


def list_delegations() -> list[tuple[pathlib.Path, dict[str, Any]]]:
    out: list[tuple[pathlib.Path, dict[str, Any]]] = []
    ddir = delegations_dir()
    try:
        names = sorted(os.listdir(ddir))
    except OSError:
        return out
    for name in names:
        if not name.endswith(".json"):
            continue
        path = ddir / name
        data = _read_json(path)
        if data is None:
            continue
        try:
            out.append((path, parse_delegation(data)))
        except OutboundError:
            continue
    return out


def build_delegation(intent: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "generation": 1,
        "nonce": intent["nonce"],
        "room": intent["room"],
        "team_id": intent["team_id"],
        "channel_id": intent["channel_id"],
        "ts": intent["posted_ts"],
        "thread_root_ts": intent["human_root_ts"],
        "requester_agent": intent["source_agent"],
        "requester_bot_user_id": intent["source_bot_user_id"],
        "requester_session": intent["requester_session"],
        "expected_responder_agent": intent["target_agent"],
        "expected_responder_bot_user_id": intent["target_bot_user_id"],
        "created_at": _rfc3339(_now()),
        "ttl_seconds": int(intent.get("ttl_seconds", INTENT_TTL_SECONDS)),
        "status": "pending",
        "result_ts": "",
        "result_claimed_at": "",
    }


def materialize_delegation(intent: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    """Create the delegation record once, keyed by the posted (team, channel, ts).

    ``O_EXCL`` create; ``EEXIST`` adopts the existing record and never
    overwrites it (Go writes claim/expiry transitions to the same file).
    """
    if not intent.get("posted_ts"):
        raise OutboundError("cannot materialize a delegation before the post ts is known")
    record = build_delegation(intent)
    path = _delegation_path(intent["team_id"], intent["channel_id"], intent["posted_ts"])
    if _create_once_json(path, record):
        return record, True
    existing = _read_json(path)
    if existing is None:
        raise OutboundError(f"delegation {path.name} exists but is unreadable")
    return parse_delegation(existing), False


def _record_expired(record: dict[str, Any], now: datetime) -> bool:
    created = _parse_rfc3339(record.get("created_at", ""))
    if created is None:
        return False
    ttl = int(record.get("ttl_seconds", INTENT_TTL_SECONDS))
    return (now - created).total_seconds() > ttl


def expire_delegation(path: pathlib.Path, record: dict[str, Any]) -> dict[str, Any]:
    """Rewrite a pending record to ``expired`` (generation bumped), in place."""
    record = dict(record)
    record["status"] = "expired"
    record["generation"] = int(record.get("generation", 1)) + 1
    _atomic_write_json(path, record)
    return record


def find_pending_delegation(
    team_id: str,
    channel_id: str,
    thread_root_ts: str,
    responder_bot_user_id: str,
    requester_bot_user_id: str,
    *,
    expire_stale: bool = True,
) -> tuple[pathlib.Path, dict[str, Any]] | None:
    """The one pending record for the tuple, or None.

    A TTL-expired ``pending`` record counts as not-pending: it is rewritten
    ``expired`` under the caller's tuple lock and skipped. The caller must
    already hold the ``dtuple`` lock.
    """
    now = _now()
    for path, record in list_delegations():
        if (
            record["team_id"] == team_id
            and record["channel_id"] == channel_id
            and record["thread_root_ts"] == thread_root_ts
            and record["expected_responder_bot_user_id"] == responder_bot_user_id
            and record["requester_bot_user_id"] == requester_bot_user_id
            and record["status"] == "pending"
        ):
            if _record_expired(record, now):
                if expire_stale:
                    expire_delegation(path, record)
                continue
            return path, record
    return None


# --- composition / escaping ------------------------------------------------

def escape_body(body: str) -> str:
    """Entity-escape ``&`` ``<`` ``>`` so agent text can never be a live entity."""
    return body.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# Slack member ids are ``U``/``W`` + uppercase alphanumerics. The body is
# entity-escaped so agent text can never be a live entity, but the service
# constructs ``<@id>`` around a directory/record-sourced id verbatim; a hostile
# id like ``U0> <!channel`` would break out into a live broadcast entity. Ids
# are validated to this charset before interpolation so the constructed mention
# stays the only live entity (composition contract).
_BOT_USER_ID_RE = re.compile(r"^[UW][A-Z0-9]{2,}$")


def _validate_bot_user_id(bot_user_id: str, *, role: str) -> str:
    if not isinstance(bot_user_id, str) or not _BOT_USER_ID_RE.match(bot_user_id):
        raise OutboundError(
            f"{role} bot_user_id {bot_user_id!r} is not a valid Slack member id "
            "(^[UW][A-Z0-9]{2,}$); refusing to interpolate into a mention")
    return bot_user_id


def compose_delegation_text(target_bot_user_id: str, body: str) -> str:
    _validate_bot_user_id(target_bot_user_id, role="delegation target")
    return f"<@{target_bot_user_id}> {escape_body(body)}"


def compose_result_text(requester_bot_user_id: str, body: str) -> str:
    _validate_bot_user_id(requester_bot_user_id, role="result requester")
    return f"<@{requester_bot_user_id}> {escape_body(body)}"


def compose_synthesis_text(body: str) -> str:
    return escape_body(body)


def delegation_metadata(nonce: str, root_ts: str, requester: str, target: str) -> dict[str, Any]:
    return {
        "event_type": "gc_delegation",
        "event_payload": {"v": 1, "nonce": nonce, "root_ts": root_ts,
                          "requester": requester, "target": target},
    }


def result_metadata(nonce: str, delegation_ts: str) -> dict[str, Any]:
    return {
        "event_type": "gc_delegation_result",
        "event_payload": {"v": 1, "nonce": nonce, "delegation_ts": delegation_ts},
    }


def synthesis_metadata(nonce: str) -> dict[str, Any]:
    """Metadata for a synthesis post: carries only the reconcile nonce.

    A synthesis has no live mention (Go wakes nobody on it), so this event_type
    is inert to the router; it exists solely so a timed-out synthesis post can
    be reconciled against its own switchboard echo instead of reposted.
    """
    return {
        "event_type": "gc_delegation_synthesis",
        "event_payload": {"v": 1, "nonce": nonce},
    }


def dm_metadata(nonce: str) -> dict[str, Any]:
    """Metadata for a per-agent DM reply: carries only the reconcile nonce.

    A DM reply has no live mention (it answers a human in a 1:1 im). The owner
    app's self-echo of this post is admitted as a receipt whose stored metadata
    embeds this nonce, so a timed-out DM post reconciles against that echo via
    :func:`_scan_receipt_for_nonce` instead of reposting (chat.postMessage is
    not idempotent).
    """
    return {
        "event_type": "gc_dm_reply",
        "event_payload": {"v": 1, "nonce": nonce},
    }


# --- provider POST (bounded retries, honor Retry-After) --------------------

def _slack_api_base() -> str:
    return os.environ.get("SLACK_API_BASE_URL", DEFAULT_SLACK_API_BASE).rstrip("/")


def _slack_web_post(
    method: str,
    token: str,
    payload: dict[str, Any],
    *,
    api_base: str,
    timeout: float,
) -> tuple[int, dict[str, str], dict[str, Any]]:
    """POST a JSON Slack Web API call. Returns (http_status, headers, body).

    Network errors and timeouts raise :class:`TransientPostError` — they are
    never a definitive rejection. This is the single seam tests monkeypatch.
    """
    url = f"{api_base}/{method}"
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            status = resp.status
            headers = {k: v for k, v in resp.headers.items()}
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        status = exc.code
        headers = {k: v for k, v in (exc.headers or {}).items()}
    except (TimeoutError, urllib.error.URLError, OSError) as exc:
        raise TransientPostError(f"{method} transport failure: {exc}") from exc
    try:
        body = json.loads(raw.decode("utf-8")) if raw else {}
    except (UnicodeDecodeError, json.JSONDecodeError):
        body = {}
    if not isinstance(body, dict):
        body = {}
    return status, headers, body


def _retry_after_seconds(headers: dict[str, str], body: dict[str, Any]) -> float:
    raw = ""
    for key, val in headers.items():
        if key.lower() == "retry-after":
            raw = val
            break
    try:
        secs = float(raw)
    except (TypeError, ValueError):
        secs = 1.0
    if secs < 0:
        secs = 1.0
    return min(secs, 30.0)


def post_message(
    token: str,
    *,
    channel: str,
    text: str,
    thread_ts: str = "",
    metadata: dict[str, Any] | None = None,
    api_base: str | None = None,
    timeout: float = 15.0,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
) -> str:
    """Post a company message and return the Slack ``ts``.

    Composition contract: top-level ``text`` only — no ``blocks``,
    ``link_names``, ``reply_broadcast``, or ``parse``. ``429`` honors
    ``Retry-After`` within ``max_attempts``; a definitive 4xx raises
    :class:`DefinitivePostError`; a timeout/5xx raises
    :class:`TransientPostError` (the caller reconciles before any repost).
    """
    base = api_base or _slack_api_base()
    payload: dict[str, Any] = {"channel": channel, "text": text}
    if thread_ts:
        payload["thread_ts"] = thread_ts
    if metadata is not None:
        payload["metadata"] = metadata

    for attempt in range(1, max_attempts + 1):
        status, headers, body = _slack_web_post(
            "chat.postMessage", token, payload, api_base=base, timeout=timeout)
        if status == 200 and body.get("ok"):
            ts = body.get("ts") or (body.get("message") or {}).get("ts")
            if not ts:
                raise DefinitivePostError("chat.postMessage ok but returned no ts")
            return str(ts)
        if status == 429:
            if attempt < max_attempts:
                _sleep(_retry_after_seconds(headers, body))
                continue
            raise TransientPostError("chat.postMessage rate-limited: attempts exhausted")
        if 500 <= status <= 599:
            raise TransientPostError(f"chat.postMessage http {status}")
        if status != 200:
            raise DefinitivePostError(f"chat.postMessage http {status}")
        err = str(body.get("error", "")) or "unknown_error"
        if err in _TRANSIENT_SLACK_ERRORS:
            if err in ("ratelimited", "rate_limited") and attempt < max_attempts:
                _sleep(_retry_after_seconds(headers, body))
                continue
            raise TransientPostError(f"chat.postMessage transient error: {err}")
        raise DefinitivePostError(f"chat.postMessage error: {err}")
    raise TransientPostError("chat.postMessage exhausted attempts")


# --- receipt-based reconciliation (read-only receipt scan) -----------------

def _iter_receipts() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    rdir = ingress_dir()
    try:
        names = sorted(os.listdir(rdir))
    except OSError:
        return out
    for name in names:
        if not name.endswith(".json"):
            continue
        data = _read_json(rdir / name)
        if isinstance(data, dict):
            out.append(data)
    return out


_RECEIPT_ID_RE = re.compile(r"^in-[A-Za-z0-9._-]+$")


def _read_body_bytes(body_ref: str) -> bytes | None:
    """Raw bytes of a body sidecar, refusing symlinks. None if absent/oversized."""
    path = bodies_dir() / (body_ref + ".body.json")
    try:
        info = os.lstat(path)
    except OSError:
        return None
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        return None
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd, "rb") as fh:
            return fh.read(4 << 20)  # bounded like the Go reader
    except OSError:
        return None


def receipt_body(receipt: dict[str, Any]) -> dict[str, Any] | None:
    """The receipt's raw inner Slack event, resolving the body sidecar.

    Every Python reader goes through here. A legacy embedded receipt (no
    ``body_ref``) returns its inline ``event``; a body-split receipt reads its
    ``bodies/<body_ref>.body.json`` sidecar and verifies the bytes against the
    receipt's immutable ``event_digest``. A redacted tombstone, a missing sidecar,
    a digest mismatch, or a non-object body all return ``None`` — so a reader
    degrades to a no-match exactly as an unparseable embedded event always has.
    Both shapes stay valid forever; there is no migration rewrite.
    """
    body_ref = receipt.get("body_ref")
    if not body_ref:
        event = receipt.get("event")
        return event if isinstance(event, dict) else None
    if not isinstance(body_ref, str) or not _RECEIPT_ID_RE.match(body_ref):
        return None
    raw = _read_body_bytes(body_ref)
    if raw is None:
        return None
    try:
        parsed = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(parsed, dict):
        return None
    if parsed.get("redacted") is True:
        return None  # explicit redacted tombstone → no-match
    if hashlib.sha256(raw).hexdigest() != receipt.get("event_digest"):
        return None  # integrity mismatch → handled as body-missing (no-match)
    return parsed


def _receipt_bot_authored(event: dict[str, Any]) -> bool:
    return bool(
        event.get("bot_id")
        or event.get("app_id")
        or event.get("bot_profile")
        or event.get("subtype") == "bot_message"
    )


def _receipt_author_matches(event: dict[str, Any], intent: dict[str, Any]) -> bool:
    """Whether a bot-authored receipt was posted by the intent's own identity.

    The nonce is workspace-visible (it rides in the message's own metadata), so
    a same-nonce message from any *other* bot in the room must never be adopted
    as this intent's post. Match the receipt's authoring app (``app_id``, incl.
    the ``bot_profile`` envelope) against ``source_app_id``, or the raw bot
    ``user`` against ``source_bot_user_id`` when no app id is present.
    """
    source_app = intent.get("source_app_id")
    app_id = event.get("app_id")
    if not app_id:
        bp = event.get("bot_profile")
        if isinstance(bp, dict):
            app_id = bp.get("app_id")
    if app_id:
        return bool(source_app) and app_id == source_app
    user = event.get("user") or ""
    return bool(user) and user == intent.get("source_bot_user_id")


def _scan_receipt_for_nonce(intent: dict[str, Any]) -> str | None:
    """Origin ts of the intent's own bot-authored receipt carrying its nonce.

    Read-only: the post, arriving back through the switchboard, is admitted as a
    bot-message receipt whose raw event (embedded, or in the body sidecar —
    resolved through :func:`receipt_body`) carries the posted metadata.
    Reconciliation adopts it only when the receipt is in (team, channel), was
    authored by the intent's own identity, carries the op's ``event_type``, and
    embeds the intent's metadata nonce. Reconciliation never calls the Slack API.
    """
    want_nonce = intent.get("metadata_nonce") or intent["nonce"]
    want_event_type = _OP_EVENT_TYPES.get(intent.get("op", "delegation"))
    for receipt in _iter_receipts():
        origin = receipt.get("origin") or {}
        if origin.get("team_id") != intent["team_id"]:
            continue
        if origin.get("channel_id") != intent["channel_id"]:
            continue
        event = receipt_body(receipt)
        if event is None:
            continue
        if not _receipt_bot_authored(event):
            continue
        if not _receipt_author_matches(event, intent):
            continue
        metadata = event.get("metadata") or {}
        if want_event_type is not None and metadata.get("event_type") != want_event_type:
            continue
        payload = metadata.get("event_payload") or {}
        if payload.get("nonce") != want_nonce:
            continue
        ts = origin.get("ts")
        if ts:
            return str(ts)
    return None


def reconcile_intent(intent: dict[str, Any]) -> dict[str, Any] | None:
    """Resolve one ``posting`` intent against the receipts.

    Found → adopt the origin ts as ``posted_ts``, mark ``published``, and
    materialize the delegation record if absent; returns the published intent.
    Not found → returns None (the intent stays parked; never reposted).
    """
    nonce = intent["nonce"]
    with acquire_lock(intent_lock_name(nonce)):
        data = _read_json(_intent_path(nonce))
        current = parse_intent(data) if data is not None else intent
        if current["status"] == "published":
            _ensure_record(current)
            return current
        if current["status"] != "posting":
            return None
        posted_ts = _scan_receipt_for_nonce(current)
        if posted_ts is None:
            return None
        current["status"] = "published"
        current["posted_ts"] = posted_ts
        current["updated_at"] = _rfc3339(_now())
        write_intent(current)
    _ensure_record(current)
    return current


def _ensure_record(intent: dict[str, Any]) -> None:
    # Only a delegation post owns a delegation record. Result/synthesis posts
    # are durable-but-recordless (Go owns the result-claim transition), so
    # reconciling them must never materialize a spurious pending record.
    if intent.get("op", "delegation") == "delegation" and intent.get("posted_ts"):
        materialize_delegation(intent)


def reconcile_posting_intents() -> int:
    """Bounded lazy-recovery pass over ``posting`` intents. Returns count resolved."""
    resolved = 0
    for intent in list_intents():
        if intent["status"] != "posting":
            continue
        try:
            if reconcile_intent(intent) is not None:
                resolved += 1
        except OutboundError:
            continue
    return resolved


# --- pruning (lazy; terminal intents + terminal records) -------------------

def prune(retention_seconds: int = PRUNE_RETENTION_SECONDS) -> dict[str, int]:
    """Remove terminal intents and terminal/expired records older than retention.

    Retention is clamped up to ``max(record ttl) + 1h`` — never honored below
    that floor — so a record can never be prunable on Python's clock while
    still S2-compatible on Go's (terminal state is crash-recovery memory).
    """
    now = _now()
    removed = {"intents": 0, "delegations": 0}

    # Retain, per tuple, the single highest-seq intent (ties broken by the most
    # recent update) as the retry_seq watermark; keeping it makes next_retry_seq
    # monotonic across prunes so a re-mint can never reuse a nonce a stale
    # receipt might still reconcile against.
    all_intents = list_intents()
    all_delegations = list_delegations()

    # Floor = max observed record ttl + clock-skew margin (never below the
    # static floor). Snapshot scans tolerate concurrent deletion by skipping
    # vanished files.
    max_ttl = INTENT_TTL_SECONDS
    for intent in all_intents:
        ttl = intent.get("ttl_seconds", 0)
        if isinstance(ttl, int) and not isinstance(ttl, bool):
            max_ttl = max(max_ttl, ttl)
    for _path, record in all_delegations:
        ttl = record.get("ttl_seconds", 0)
        if isinstance(ttl, int) and not isinstance(ttl, bool):
            max_ttl = max(max_ttl, ttl)
    floor = max(PRUNE_RETENTION_FLOOR_SECONDS, max_ttl + PRUNE_CLOCK_SKEW_MARGIN_SECONDS)
    retention = max(int(retention_seconds), floor)
    watermark_nonce: dict[tuple[str, ...], str] = {}
    watermark_rank: dict[tuple[str, ...], tuple[int, float]] = {}
    for intent in all_intents:
        key = _tuple8(intent)
        stamp = _parse_rfc3339(intent.get("updated_at", "")) or _parse_rfc3339(intent.get("created_at", ""))
        rank = (int(intent.get("retry_seq", 0)), stamp.timestamp() if stamp else 0.0)
        if key not in watermark_rank or rank > watermark_rank[key]:
            watermark_rank[key] = rank
            watermark_nonce[key] = intent["nonce"]

    for intent in all_intents:
        if intent["status"] not in _INTENT_TERMINAL:
            continue
        if watermark_nonce.get(_tuple8(intent)) == intent["nonce"]:
            continue  # retain the per-tuple watermark
        updated = _parse_rfc3339(intent.get("updated_at", "")) or _parse_rfc3339(intent.get("created_at", ""))
        if updated is None or (now - updated).total_seconds() <= retention:
            continue
        try:
            os.unlink(_intent_path(intent["nonce"]))
            removed["intents"] += 1
        except OSError:
            pass

    for path, record in all_delegations:
        if record["status"] not in _DELEGATION_TERMINAL:
            continue
        stamp = (
            _parse_rfc3339(record.get("result_claimed_at", ""))
            or _parse_rfc3339(record.get("created_at", ""))
        )
        if stamp is None or (now - stamp).total_seconds() <= retention:
            continue
        try:
            os.unlink(path)
            removed["delegations"] += 1
        except OSError:
            pass
    return removed


# --- current-turn pointer + directory context ------------------------------

def session_name_aliases(session: str) -> list[str]:
    """Deterministic name aliases for a gc session.

    gc sanitizes configured named-session identifiers by replacing dots
    with double underscores (config name ``teams.it`` runs with
    ``GC_SESSION_NAME=teams__it``), while company bindings and delivery
    address the configured dot form. Both spellings identify the same
    session, so pointer lookups and anti-spoof comparisons accept either.
    """
    if not session:
        return []
    aliases = [session]
    if "__" in session:
        aliases.append(session.replace("__", "."))
    elif "." in session:
        aliases.append(session.replace(".", "__"))
    return aliases


def read_current_turn(session: str) -> dict[str, Any] | None:
    """Parse the room current-turn pointer for ``session`` (None if absent)."""
    for name in session_name_aliases(session):
        data = _read_json(turns_dir() / f"{name}.json")
        if data is not None:
            return parse_current_turn(data)
    return None


def read_current_turn_dm(session: str) -> dict[str, Any] | None:
    """Parse the DM current-turn pointer ``dm/<session>.json`` (None if absent).

    DM turns live in a dedicated ``company-current-turn/dm/`` subdirectory so a
    room wake can never clobber a DM turn and misdirect a private reply into a
    room (spec review C6 / m2 / m13): the shared sanitizer passes interior dots
    through, so a flat ``<session>.dm.json`` file would collide with the room
    pointer of a session literally named ``<session>.dm``. Shares the dot/dunder
    session aliasing with the room pointer reader.
    """
    dm_dir = turns_dir() / "dm"
    for name in session_name_aliases(session):
        data = _read_json(dm_dir / f"{name}.json")
        if data is not None:
            return parse_current_turn(data)
    return None


def read_current_turn_mpim(session: str) -> dict[str, Any] | None:
    """Parse the group-DM current-turn pointer ``mpim/<session>.json`` (None if
    absent).

    mpim turns get their OWN pointer namespace, disjoint from both the room and
    the DM namespaces, so a group-DM wake can never clobber an unanswered 1:1 DM
    turn (spec §Pointer). Shares the dot/dunder session aliasing with the other
    pointer readers.
    """
    mpim_dir = turns_dir() / "mpim"
    for name in session_name_aliases(session):
        data = _read_json(mpim_dir / f"{name}.json")
        if data is not None:
            return parse_current_turn(data)
    return None


def read_turn_ref(session: str, turn_ref: str) -> dict[str, Any] | None:
    """Read one immutable delivery record by its opaque turn reference.

    The record lives outside all mutable room/DM pointer namespaces, so a
    later wake cannot change the channel or thread selected by this lookup.
    ``session`` is checked here (and again by the verb spoof guards) so a turn
    reference copied from another agent session cannot be used to act as it.
    """
    if _TURN_REF_RE.fullmatch(turn_ref or "") is None:
        raise OutboundError(
            f"--turn-ref must match gct- followed by 20 lowercase hex digits, "
            f"got {turn_ref!r}")
    data = _read_json(turns_dir() / "by-ref" / f"{turn_ref}.json")
    if data is None:
        return None
    turn = parse_current_turn(data)
    if turn.get("turn_ref") != turn_ref:
        raise OutboundError(
            f"immutable turn record {turn_ref!r} contains mismatched "
            f"turn_ref {turn.get('turn_ref')!r}")
    if turn["session"] not in session_name_aliases(session):
        raise OutboundError(
            f"immutable turn {turn_ref!r} belongs to session "
            f"{turn['session']!r}, not GC_SESSION_NAME {session!r} (spoof guard)")
    return turn


# Reply-pointer source priority for tie-breaking newest-wins: the more private /
# more specific surface wins a delivered_at tie, so a reply is never auto-routed
# from a private turn into a public room (nor from a 1:1 DM into a group) — an
# operator disambiguates with --kind.
_POINTER_SOURCE_PRIORITY = {"dm": 3, "mpim": 2, "room": 1}


def resolve_reply_pointer_source(
    session: str, *, kind_override: str = "", turn_ref: str = "",
) -> str | None:
    """Decide whether ``reply-current`` acts on the room, DM, or mpim pointer.

    Returns ``"room"``, ``"dm"``, ``"mpim"``, or ``None`` (no company pointer at
    all). With more than one live, the NEWEST by ``delivered_at`` wins;
    ``kind_override`` (``room``/``dm``/``mpim``) forces one and errors if that
    pointer is absent. On an equal (second-granularity) ``delivered_at``, or when
    any present pointer carries an unparseable timestamp, the most-private
    present surface wins (dm > mpim > room) — never auto-route a private reply
    into a public room, nor a 1:1 reply into a group, when they are just as
    current; an operator disambiguates with ``--kind``.
    """
    if turn_ref:
        turn = read_turn_ref(session, turn_ref)
        if turn is None:
            raise OutboundError(
                f"no immutable company turn record for --turn-ref {turn_ref!r}")
        source = (
            "dm" if turn["kind"] == "dm" else
            "mpim" if turn["kind"] == "mpim" else
            "room"
        )
        if kind_override and kind_override != source:
            raise OutboundError(
                f"--kind {kind_override!r} does not match --turn-ref "
                f"{turn_ref!r} surface {source!r}")
        return source

    present: dict[str, dict[str, Any]] = {}
    for src, reader in (
        ("room", read_current_turn),
        ("dm", read_current_turn_dm),
        ("mpim", read_current_turn_mpim),
    ):
        try:
            turn = reader(session)
        except OutboundError:
            # A corrupt/poisoned pointer for ONE kind (e.g. an empty owner_app_id
            # written for a vanished woken agent) must not brick reply-current on
            # the session's OTHER live surfaces: treat that kind as absent for
            # auto-resolution. An explicit --kind targeting the corrupt pointer
            # still surfaces the parse error below (we re-raise for it here).
            if kind_override == src:
                raise
            continue
        if turn is not None:
            present[src] = turn
    if not present:
        # Not a company session at all: the legacy reply path owns it, and any
        # --kind is the legacy conversation kind (dm/room/thread), not a company
        # pointer override — never hijack it here.
        return None
    if kind_override in _POINTER_SOURCE_PRIORITY:
        if kind_override not in present:
            raise OutboundError(
                f"--kind {kind_override}: session {session!r} has no "
                f"{kind_override} current-turn pointer")
        return kind_override
    if kind_override:
        raise OutboundError(
            f"--kind must be 'room', 'dm', or 'mpim', got {kind_override!r}")
    parsed = {src: _parse_rfc3339(t.get("delivered_at", "")) for src, t in present.items()}
    if any(v is None for v in parsed.values()):
        # An unparseable timestamp anywhere: fall back to the most-private
        # present surface rather than trusting any ordering (privacy-safe).
        for pref in ("dm", "mpim", "room"):
            if pref in present:
                return pref
    best_src = None
    best_key: tuple | None = None
    for src in present:
        key = (parsed[src], _POINTER_SOURCE_PRIORITY[src])
        if best_key is None or key > best_key:
            best_key, best_src = key, src
    return best_src


def _load_context():
    import slack_company_directory as directory
    try:
        dir_data = directory.load_directory()
        bind_data = directory.load_bindings()
    except directory.DirectoryError as exc:
        raise OutboundError(str(exc)) from exc
    agents = {a["name"]: a for a in dir_data.get("agents", [])}
    rooms = {r["name"]: r for r in dir_data.get("rooms", [])}
    bindings = bind_data.get("bindings", [])
    return agents, rooms, bindings


def _session_for_binding(bindings: list[dict[str, Any]], room: str, agent: str) -> str | None:
    for entry in bindings:
        if entry.get("room") == room and entry.get("agent") == agent:
            return entry.get("session")
    return None


def _check_turn_identity(turn: dict[str, Any], session_name: str, origin_ts: str) -> None:
    """Origin-ts pin + session-alias spoof guard shared by room and DM turns."""
    if origin_ts and origin_ts != turn["ts"]:
        raise OutboundError(
            f"--origin-ts {origin_ts!r} does not match the current turn ts "
            f"{turn['ts']!r}; a newer wake overwrote the pointer — pass "
            f"--origin-ts {turn['ts']} to act on that turn")
    if turn["session"] not in session_name_aliases(session_name):
        raise OutboundError(
            f"current-turn pointer session {turn['session']!r} does not match "
            f"GC_SESSION_NAME {session_name!r} (spoof guard)")


def _verify_turn_ref(
    session_name: str, origin_ts: str, turn_ref: str,
) -> dict[str, Any]:
    if not session_name:
        raise OutboundError(
            "GC_SESSION_NAME is not set; company verbs need the bound session name")
    turn = read_turn_ref(session_name, turn_ref)
    if turn is None:
        raise OutboundError(
            f"no immutable company turn record for --turn-ref {turn_ref!r}; "
            "the turn may have expired")
    _check_turn_identity(turn, session_name, origin_ts)
    return turn


def _require_turn_ref_for_new_pointer(turn: dict[str, Any]) -> None:
    """Fail closed instead of silently using a mutable post-rollout pointer."""
    turn_ref = turn.get("turn_ref") or ""
    if turn_ref:
        raise OutboundError(
            "this company turn has immutable routing context; pass "
            f"--turn-ref {turn_ref} exactly as shown in the Slack reminder")


def _verify_pointer(
    session_name: str, origin_ts: str, turn_ref: str = "",
) -> dict[str, Any]:
    """Load + anti-spoof the room current-turn pointer for this session."""
    if turn_ref:
        turn = _verify_turn_ref(session_name, origin_ts, turn_ref)
        if turn["kind"] in _DM_FAMILY_KINDS:
            raise OutboundError(
                f"--turn-ref {turn_ref!r} is a {turn['kind']} turn, not a room turn")
        return turn
    if not session_name:
        raise OutboundError(
            "GC_SESSION_NAME is not set; company verbs need the bound session name")
    turn = read_current_turn(session_name)
    if turn is None:
        raise OutboundError(
            f"no company current-turn pointer for session {session_name!r}; "
            "this session has no active company turn")
    _require_turn_ref_for_new_pointer(turn)
    _check_turn_identity(turn, session_name, origin_ts)
    return turn


def _verify_pointer_dm(
    session_name: str, origin_ts: str, turn_ref: str = "",
) -> dict[str, Any]:
    """Load + anti-spoof the DM current-turn pointer (``dm/<session>.json``)."""
    if turn_ref:
        turn = _verify_turn_ref(session_name, origin_ts, turn_ref)
        if turn["kind"] != "dm":
            raise OutboundError(
                f"--turn-ref {turn_ref!r} has kind {turn['kind']!r}, not 'dm'")
        return turn
    if not session_name:
        raise OutboundError(
            "GC_SESSION_NAME is not set; company verbs need the bound session name")
    turn = read_current_turn_dm(session_name)
    if turn is None:
        raise OutboundError(
            f"no DM current-turn pointer for session {session_name!r}; "
            "this session has no active DM turn")
    if turn["kind"] != "dm":
        raise OutboundError(
            f"DM pointer for session {session_name!r} has kind {turn['kind']!r}, "
            "not 'dm'")
    _require_turn_ref_for_new_pointer(turn)
    _check_turn_identity(turn, session_name, origin_ts)
    return turn


def _verify_pointer_mpim(
    session_name: str, origin_ts: str, turn_ref: str = "",
) -> dict[str, Any]:
    """Load + anti-spoof the group-DM current-turn pointer (``mpim/<session>.json``)."""
    if turn_ref:
        turn = _verify_turn_ref(session_name, origin_ts, turn_ref)
        if turn["kind"] != "mpim":
            raise OutboundError(
                f"--turn-ref {turn_ref!r} has kind {turn['kind']!r}, not 'mpim'")
        return turn
    if not session_name:
        raise OutboundError(
            "GC_SESSION_NAME is not set; company verbs need the bound session name")
    turn = read_current_turn_mpim(session_name)
    if turn is None:
        raise OutboundError(
            f"no mpim current-turn pointer for session {session_name!r}; "
            "this session has no active mpim turn")
    if turn["kind"] != "mpim":
        raise OutboundError(
            f"mpim pointer for session {session_name!r} has kind {turn['kind']!r}, "
            "not 'mpim'")
    _require_turn_ref_for_new_pointer(turn)
    _check_turn_identity(turn, session_name, origin_ts)
    return turn


def _verify_binding(rooms, bindings, turn: dict[str, Any], session_name: str) -> None:
    room = turn["room"]
    agent = turn["agent"]
    if room not in rooms:
        raise OutboundError(f"room {room!r} is not in the company directory")
    bound = _session_for_binding(bindings, room, agent)
    if bound not in session_name_aliases(session_name):
        raise OutboundError(
            f"(room={room}, agent={agent}) is not bound to session "
            f"{session_name!r} (bound to {bound!r}); refusing to act (spoof guard)")
    room_rec = rooms[room]
    expected_route = (room_rec.get("team_id"), room_rec.get("channel_id"))
    actual_route = (turn.get("team_id"), turn.get("channel_id"))
    if actual_route != expected_route:
        raise OutboundError(
            f"turn route team/channel {actual_route!r} does not match room "
            f"{room!r} directory route {expected_route!r} (spoof guard)")


def _load_dm_context():
    """Directory agents (by name) + the DM bindings list (fail-closed)."""
    import slack_company_directory as directory
    try:
        dir_data = directory.load_directory()
        dm_data = directory.load_dm_bindings()
    except directory.DirectoryError as exc:
        raise OutboundError(str(exc)) from exc
    agents = {a["name"]: a for a in dir_data.get("agents", [])}
    dm_bindings = dm_data.get("dm_bindings", [])
    return agents, dm_bindings


def _dm_session_for_agent(dm_bindings: list[dict[str, Any]], agent: str) -> str | None:
    for entry in dm_bindings:
        if entry.get("agent") == agent:
            return entry.get("session")
    return None


def _verify_dm_binding(dm_bindings, turn: dict[str, Any], session_name: str) -> None:
    """DM spoof guard: the session must be the pointer owner agent's dm-bound one.

    Extends the room guard (keyed against ``company_bindings.json``) to the DM
    surface: a kind-``dm`` reply is admitted only from the session that
    ``dm_bindings`` binds to the turn's owner agent, with the same dot/dunder
    session aliasing.
    """
    agent = turn["agent"]
    bound = _dm_session_for_agent(dm_bindings, agent)
    if bound not in session_name_aliases(session_name):
        raise OutboundError(
            f"DM agent {agent!r} is not bound to session {session_name!r} "
            f"(bound to {bound!r}); refusing to act (spoof guard)")


# --- verb: delegate --------------------------------------------------------

def run_delegate(
    *, to: str, body: str, origin_ts: str, session_name: str,
    turn_ref: str = "",
) -> dict[str, Any]:
    """Post ``<@to> body`` into the human root's thread as the acting agent.

    Durably persists a posting intent before the POST, enforces one pending
    delegation per tuple under the ``dtuple`` lock, materializes the record on
    success, and reconciles-before-repost on a transient failure (never
    reposts on ambiguity).
    """
    if not body.strip():
        raise OutboundError("--body/--body-file must be non-empty")

    # Every company verb runs lazy recovery + pruning first.
    reconcile_posting_intents()
    prune()

    turn = _verify_pointer(session_name, origin_ts, turn_ref)
    # A dm-family-rooted turn (dm/mpim) carries no delegation authority (spec
    # §Kind-dispatch inventory: "delegation human-root gate | dm-family refused").
    # The room pointer this verb reads is a distinct file from the dm/ and mpim/
    # pointers, so a dm-family kind cannot normally reach here — the explicit
    # refusal is the human-root gate's kind check extended to name it.
    if turn["kind"] in _DM_FAMILY_KINDS:
        raise OutboundError(
            f"delegate is refused from a {turn['kind'].upper()}-rooted turn: a "
            "direct message carries no delegation authority (delegations and "
            "results stay visible in company rooms).")
    # S8 strict one-hop: delegate proceeds only from a human-rooted turn
    # (ambient/thread_ambient/targeted) and hard-errors on every peer kind
    # alike. A delegated recipient may reply-current a result but must not
    # redelegate, and a requester issues every sibling delegation from its
    # human turn(s) before waiting — delegating from a result turn would enable
    # a second synthesis.
    if turn["kind"] not in ("ambient", "thread_ambient", "targeted"):
        raise OutboundError(
            f"delegate is one-hop: it runs only from a human-rooted turn "
            f"(ambient/thread_ambient/targeted), not a {turn['kind']!r} turn. Issue every "
            "sibling delegation from your human-rooted turn before waiting; a "
            "delegated recipient may `reply-current` a result but must not "
            "redelegate.")
    agents, rooms, bindings = _load_context()
    _verify_binding(rooms, bindings, turn, session_name)

    room = turn["room"]
    source_agent = turn["agent"]
    if to == source_agent:
        raise OutboundError("self-targeting is rejected")
    src = agents.get(source_agent)
    if src is None:
        raise OutboundError(f"acting agent {source_agent!r} is not in the directory")
    tgt = agents.get(to)
    if tgt is None:
        raise OutboundError(f"target agent {to!r} is not in the company directory")
    room_rec = rooms[room]
    if to not in (room_rec.get("members") or []):
        raise OutboundError(f"agent {to!r} is not a member of room {room!r}")
    if to not in (room_rec.get("mention_wake") or []):
        raise OutboundError(f"agent {to!r} is not mention-eligible in room {room!r}")

    team = turn["team_id"]
    channel = turn["channel_id"]
    human_root_ts = turn["thread_root_ts"]
    body_hex = body_sha256(body)
    responder_bot = _validate_bot_user_id(tgt["bot_user_id"], role="delegation target")
    requester_bot = _validate_bot_user_id(src["bot_user_id"], role="delegation requester")

    lock_name = dtuple_lock_name(team, channel, human_root_ts, responder_bot, requester_bot)
    with acquire_lock(lock_name):
        pending = find_pending_delegation(team, channel, human_root_ts, responder_bot, requester_bot)
        if pending is not None:
            _, rec = pending
            turn_flag = f" --turn-ref {turn_ref}" if turn_ref else ""
            raise OutboundError(
                f"a pending delegation to {to!r} already exists for this thread "
                f"(record {delegation_filename(team, channel, rec['ts'])}); "
                f"await its result or `gc slack delegate{turn_flag} "
                f"--cancel --to {to}`")

        tuple8 = (
            src["app_id"], requester_bot, to, responder_bot,
            team, channel, human_root_ts, body_hex,
        )
        inflight = find_nonterminal_intent_for_tuple(tuple8)
        if inflight is not None and inflight["status"] == "posting":
            # A prior attempt reached the provider POST: reconcile against the
            # receipts, never repost on ambiguity (chat.postMessage isn't
            # idempotent).
            resolved = reconcile_intent(inflight)
            if resolved is not None:
                return _delegate_report("published_resumed", resolved, team, channel)
            return _delegate_report("parked", inflight, team, channel)

        if inflight is not None and inflight["status"] == "prepared":
            # A crash between create_intent_once and mark_posting left a
            # prepared intent: prepared provably precedes any provider POST, so
            # adopt it and resume posting rather than parking it forever.
            intent = inflight
        else:
            retry_seq = next_retry_seq(tuple8)
            nonce = compute_nonce(
                source_app_id=src["app_id"], source_bot_user_id=requester_bot,
                target_agent=to, target_bot_user_id=responder_bot,
                team_id=team, channel_id=channel, human_root_ts=human_root_ts,
                body_sha256=body_hex, retry_seq=retry_seq,
            )
            intent = build_intent(
                nonce=nonce, retry_seq=retry_seq,
                source_agent=source_agent, source_app_id=src["app_id"],
                source_bot_user_id=requester_bot, target_agent=to,
                target_bot_user_id=responder_bot, team_id=team, channel_id=channel,
                room=room, human_root_ts=human_root_ts,
                requester_session=session_name, body_hex=body_hex,
            )
            intent, _created = create_intent_once(intent)

        nonce = intent["nonce"]
        token = load_bot_token(source_agent)
        intent = intent_mark_posting(nonce)
        text = compose_delegation_text(responder_bot, body)
        meta = delegation_metadata(nonce, human_root_ts, source_agent, to)
        try:
            posted_ts = post_message(
                token, channel=channel, text=text,
                thread_ts=human_root_ts, metadata=meta,
                max_attempts=int(intent.get("max_attempts", DEFAULT_MAX_ATTEMPTS)))
        except DefinitivePostError as exc:
            intent_mark_failed(nonce, str(exc))
            raise OutboundError(f"delegation post failed (definitive): {exc}") from exc
        except TransientPostError as exc:
            resolved = reconcile_intent(intent)
            if resolved is not None:
                return _delegate_report("published_resumed", resolved, team, channel)
            return _delegate_report("parked", intent, team, channel, note=str(exc))

        intent = intent_mark_published(nonce, posted_ts)
        materialize_delegation(intent)
        return _delegate_report("published", intent, team, channel)


def _delegate_report(status: str, intent: dict[str, Any], team: str, channel: str,
                     note: str = "") -> dict[str, Any]:
    posted_ts = intent.get("posted_ts", "")
    report: dict[str, Any] = {
        "status": status,
        "nonce": intent["nonce"],
        "target_agent": intent["target_agent"],
        "posted_ts": posted_ts,
    }
    if posted_ts:
        report["delegation_key"] = delegation_filename(team, channel, posted_ts)
    if note:
        report["note"] = note
    return report


# --- verb: delegate --cancel ----------------------------------------------

def run_cancel(
    *, to: str, origin_ts: str, session_name: str, turn_ref: str = "",
) -> dict[str, Any]:
    """Transition the caller's own pending delegation for the tuple to ``expired``."""
    reconcile_posting_intents()
    prune()

    turn = _verify_pointer(session_name, origin_ts, turn_ref)
    agents, rooms, bindings = _load_context()
    _verify_binding(rooms, bindings, turn, session_name)

    source_agent = turn["agent"]
    src = agents.get(source_agent)
    tgt = agents.get(to)
    if src is None or tgt is None:
        raise OutboundError(f"unknown agent in cancel (source={source_agent!r}, target={to!r})")

    team = turn["team_id"]
    channel = turn["channel_id"]
    human_root_ts = turn["thread_root_ts"]
    responder_bot = tgt["bot_user_id"]
    requester_bot = src["bot_user_id"]

    lock_name = dtuple_lock_name(team, channel, human_root_ts, responder_bot, requester_bot)
    with acquire_lock(lock_name):
        target: tuple[pathlib.Path, dict[str, Any]] | None = None
        for path, record in list_delegations():
            if (
                record["team_id"] == team
                and record["channel_id"] == channel
                and record["thread_root_ts"] == human_root_ts
                and record["expected_responder_bot_user_id"] == responder_bot
                and record["requester_bot_user_id"] == requester_bot
                and record["status"] == "pending"
            ):
                target = (path, record)
                break
        if target is None:
            return {"status": "no_pending", "target_agent": to}
        path, record = target
        expire_delegation(path, record)
        return {"status": "cancelled", "target_agent": to, "delegation_key": path.name}


# --- reply-current company path (peer result / synthesis / root reply) ------

def _durable_company_post(
    *,
    op: str,
    source_agent: str,
    source_app_id: str,
    source_bot_user_id: str,
    target_agent: str,
    target_bot_user_id: str,
    team: str,
    channel: str,
    room: str,
    human_root_ts: str,
    requester_session: str,
    body: str,
    text: str,
    thread_ts: str,
    token: str,
    metadata_nonce: str,
    make_metadata,
) -> tuple[dict[str, Any], str]:
    """Post ``text`` through a durable intent, keyed by the (source, target,
    body) tuple. Returns ``(intent, outcome)`` where outcome is one of
    ``posted`` (fresh POST), ``reused`` (already posted / reconciled — no
    repost), or ``parked`` (transient failure; recovery on the next verb).

    The same reconcile-before-repost discipline as delegations: the intent is
    created before the provider POST, a timeout/5xx reconciles against the
    receipt scan (never a blind repost), and a definitive 4xx fails the intent.
    """
    body_hex = body_sha256(body)
    tuple8 = (
        source_app_id, source_bot_user_id, target_agent, target_bot_user_id,
        team, channel, human_root_ts, body_hex,
    )

    existing = reusable_intent_for_tuple(tuple8)
    if existing is not None and existing["status"] == "published":
        return existing, "reused"
    if existing is not None and existing["status"] == "posting":
        resolved = reconcile_intent(existing)
        return (resolved, "reused") if resolved is not None else (existing, "parked")

    if existing is not None and existing["status"] == "prepared":
        # Crash between create and mark_posting: prepared precedes any POST, so
        # adopt it and resume.
        intent = existing
    else:
        retry_seq = next_retry_seq(tuple8)
        nonce = compute_nonce(
            source_app_id=source_app_id, source_bot_user_id=source_bot_user_id,
            target_agent=target_agent, target_bot_user_id=target_bot_user_id,
            team_id=team, channel_id=channel, human_root_ts=human_root_ts,
            body_sha256=body_hex, retry_seq=retry_seq,
        )
        intent = build_intent(
            nonce=nonce, retry_seq=retry_seq, source_agent=source_agent,
            source_app_id=source_app_id, source_bot_user_id=source_bot_user_id,
            target_agent=target_agent, target_bot_user_id=target_bot_user_id,
            team_id=team, channel_id=channel, room=room,
            human_root_ts=human_root_ts, requester_session=requester_session,
            body_hex=body_hex, op=op, metadata_nonce=metadata_nonce or nonce,
        )
        intent, _created = create_intent_once(intent)
        if intent["status"] == "published":
            return intent, "reused"
        if intent["status"] == "posting":
            resolved = reconcile_intent(intent)
            return (resolved, "reused") if resolved is not None else (intent, "parked")

    nonce = intent["nonce"]
    effective_nonce = intent.get("metadata_nonce") or nonce
    intent = intent_mark_posting(nonce)
    meta = make_metadata(effective_nonce)
    try:
        posted_ts = post_message(
            token, channel=channel, text=text, thread_ts=thread_ts, metadata=meta,
            max_attempts=int(intent.get("max_attempts", DEFAULT_MAX_ATTEMPTS)))
    except DefinitivePostError as exc:
        intent_mark_failed(nonce, str(exc))
        raise OutboundError(f"{op} post failed (definitive): {exc}") from exc
    except TransientPostError:
        resolved = reconcile_intent(intent)
        if resolved is not None:
            return resolved, "reused"
        current = _read_json(_intent_path(nonce))
        return (parse_intent(current) if current is not None else intent), "parked"

    intent = intent_mark_published(nonce, posted_ts)
    return intent, "posted"


def _company_post_report(
    intent: dict[str, Any], outcome: str, *, kind: str, delegation_key: str = "",
) -> dict[str, Any]:
    report: dict[str, Any] = {
        "status": "posted" if outcome in ("posted", "reused") else "parked",
        "kind": kind,
        "nonce": intent["nonce"],
        "posted_ts": intent.get("posted_ts", ""),
    }
    if delegation_key:
        report["delegation_key"] = delegation_key
    if outcome == "reused":
        report["note"] = "reconciled to the existing post; not reposted"
    elif outcome == "parked":
        report["note"] = ("post parked on a transient failure; recovery is automatic "
                          "once the receipt lands")
    return report


def post_peer_result(
    *, body: str, origin_ts: str, session_name: str, turn_ref: str = "",
) -> dict[str, Any]:
    """Post a delegation result into the human root's thread (responder → requester).

    The acting agent is the delegation's expected responder; the result posts
    with its own token, mentioning only the recorded requester, and carries the
    result metadata gate (``gc_delegation_result`` + nonce + delegation_ts).
    Routed through a durable intent (op=result): created before the POST,
    reconciled-before-repost on a timeout/5xx, failed on a definitive 4xx.
    """
    if not body.strip():
        raise OutboundError("--body/--body-file must be non-empty")
    reconcile_posting_intents()
    prune()

    turn = _verify_pointer(session_name, origin_ts, turn_ref)
    if turn["kind"] != "peer_delegation":
        raise OutboundError(f"current turn kind is {turn['kind']!r}, not peer_delegation")
    agents, rooms, bindings = _load_context()
    _verify_binding(rooms, bindings, turn, session_name)

    data = _read_json(delegations_dir() / turn["delegation_key"])
    if data is None:
        raise OutboundError(f"delegation record {turn['delegation_key']!r} is missing or corrupt")
    record = parse_delegation(data)

    acting = turn["agent"]
    if acting != record["expected_responder_agent"]:
        raise OutboundError(
            f"session agent {acting!r} is not the expected responder "
            f"{record['expected_responder_agent']!r} for this delegation")
    src = agents.get(acting)
    if src is None:
        raise OutboundError(f"acting agent {acting!r} is not in the directory")

    token = load_bot_token(acting)
    requester_bot = record["requester_bot_user_id"]
    # compose_result_text validates the requester id before interpolation.
    text = compose_result_text(requester_bot, body)
    delegation_nonce = record["nonce"]
    delegation_ts = record["ts"]
    intent, outcome = _durable_company_post(
        op="result",
        source_agent=acting, source_app_id=src["app_id"],
        source_bot_user_id=src["bot_user_id"],
        target_agent=record["requester_agent"], target_bot_user_id=requester_bot,
        team=turn["team_id"], channel=turn["channel_id"], room=turn["room"],
        human_root_ts=record["thread_root_ts"], requester_session=session_name,
        body=body, text=text, thread_ts=record["thread_root_ts"], token=token,
        metadata_nonce=delegation_nonce,
        make_metadata=lambda n: result_metadata(n, delegation_ts),
    )
    return _company_post_report(
        intent, outcome, kind="peer_result", delegation_key=turn["delegation_key"])


def _synthesis_gate(turn: dict[str, Any], *, allow_partial: bool) -> dict[str, Any]:
    """The D2 readiness gate over the claimed record's frozen snapshot (S10).

    Returns report extras to merge (``{"allow_partial": True}`` when a
    not-ready snapshot is forced). Raises :class:`OutboundError` (exit 1) on a
    refused not-ready synthesis; warns to stderr and proceeds on an unavailable
    (legacy/malformed/pruned) snapshot — never wedges on cosmetic corruption.
    """
    key = turn.get("delegation_key") or ""
    record: dict[str, Any] | None = None
    if key:
        data = _read_json(delegations_dir() / key)
        if isinstance(data, dict):
            try:
                record = parse_delegation(data)
            except OutboundError:
                record = None
    state = synthesis_state(record if record is not None else {})

    if not state["synthesis_state_available"] or state["synthesis_state_version"] != 1:
        print(
            f"warning: synthesis snapshot for delegation record {key!r} is "
            "unavailable (legacy, malformed, or pruned record); proceeding "
            "without the readiness gate",
            file=sys.stderr,
        )
        return {}

    if state["synthesis_ready"]:
        return {}

    if allow_partial:
        return {"allow_partial": True}

    pending_lines = "\n".join(
        f"  - {pd['expected_responder_agent']} (delegation_ts {pd['delegation_ts']})"
        for pd in state["pending_delegations"]
    )
    turn_ref = turn.get("turn_ref") or ""
    turn_flag = f" --turn-ref {turn_ref}" if turn_ref else ""
    raise OutboundError(
        "synthesis is not ready: "
        f"{state['responded_delegation_count']} of "
        f"{state['compatible_delegation_count']} compatible sibling "
        "delegations have durably claimed a result. Still pending:\n"
        f"{pending_lines}\n"
        "remedies: wait for the remaining sibling result wake, or "
        f"`gc slack delegate{turn_flag} --cancel --to <agent>` for a dead sibling "
        "(expires it out of the compatible set, so the next claim freezes "
        "ready); or pass --allow-partial to synthesize the partial set now.")


def post_peer_synthesis(
    *, body: str, origin_ts: str, session_name: str, allow_partial: bool = False,
    turn_ref: str = "",
) -> dict[str, Any]:
    """Post a synthesis into the human root thread with no live agent mentions.

    The D2 gate (S10-normalized snapshot on the claimed record) decides
    readiness: ready proceeds; not-ready hard-errors listing each pending
    sibling and the two remedies unless ``allow_partial`` is set (which
    proceeds and records ``allow_partial: true`` in the report); an unavailable
    snapshot warns and proceeds. Routed through a durable intent (op=synthesis)
    so a timed-out synthesis post is reconciled against its own echo rather than
    reposted.
    """
    if not body.strip():
        raise OutboundError("--body/--body-file must be non-empty")
    reconcile_posting_intents()
    prune()

    turn = _verify_pointer(session_name, origin_ts, turn_ref)
    if turn["kind"] != "peer_result":
        raise OutboundError(f"current turn kind is {turn['kind']!r}, not peer_result")
    agents, rooms, bindings = _load_context()
    _verify_binding(rooms, bindings, turn, session_name)

    report_extra = _synthesis_gate(turn, allow_partial=allow_partial)

    acting = turn["agent"]
    src = agents.get(acting)
    if src is None:
        raise OutboundError(f"acting agent {acting!r} is not in the directory")
    token = load_bot_token(acting)
    text = compose_synthesis_text(body)
    intent, outcome = _durable_company_post(
        op="synthesis",
        source_agent=acting, source_app_id=src["app_id"],
        source_bot_user_id=src["bot_user_id"],
        # No mention: key the tuple on the acting identity itself.
        target_agent=acting, target_bot_user_id=src["bot_user_id"],
        team=turn["team_id"], channel=turn["channel_id"], room=turn["room"],
        human_root_ts=turn["thread_root_ts"], requester_session=session_name,
        body=body, text=text, thread_ts=turn["thread_root_ts"], token=token,
        metadata_nonce="",
        make_metadata=synthesis_metadata,
    )
    report = _company_post_report(intent, outcome, kind="peer_synthesis")
    report.update(report_extra)
    return report


def post_company_root_reply(
    *, body: str, origin_ts: str, session_name: str, turn_ref: str = "",
) -> dict[str, Any]:
    """Post an ordinary reply into the company room's thread root.

    For ambient/thread_ambient/targeted/peer_input turns (a human message or an
    uncorrelated peer wake): the acting agent answers into ``thread_root_ts``
    with its own token and no live mentions (escaped body, no gc metadata). No
    delegation record is involved. This replaces the legacy resolution, which
    the company delivery path never feeds.
    """
    if not body.strip():
        raise OutboundError("--body/--body-file must be non-empty")
    reconcile_posting_intents()
    prune()

    turn = _verify_pointer(session_name, origin_ts, turn_ref)
    if turn["kind"] not in ("ambient", "thread_ambient", "targeted", "peer_input"):
        raise OutboundError(
            f"current turn kind is {turn['kind']!r}, not a room-reply kind")
    agents, rooms, bindings = _load_context()
    _verify_binding(rooms, bindings, turn, session_name)

    acting = turn["agent"]
    if acting not in agents:
        raise OutboundError(f"acting agent {acting!r} is not in the directory")
    token = load_bot_token(acting)
    text = compose_synthesis_text(body)  # escaped, no live mention
    posted_ts = post_message(
        token, channel=turn["channel_id"], text=text,
        thread_ts=turn["thread_root_ts"])
    return {"status": "posted", "kind": turn["kind"], "posted_ts": posted_ts}


def _post_dm_family_reply(
    turn: dict[str, Any], *, body: str, session_name: str, report_kind: str,
) -> dict[str, Any]:
    """Shared dm-family (dm/mpim) reply core over an already-verified pointer.

    The acting agent is the pointer's ``agent``; the reply posts with that
    agent's OWN bot token (``load_bot_token``), escaped with no live mentions,
    through the SAME durable-intent machinery as room replies (op=``dm`` for both
    kinds — reconciliation matches the admitted echo receipts by nonce exactly as
    a DM; a distinct op would break ``_scan_receipt_for_nonce``'s gates). Created
    before the POST, reconciled against the admitted self-echo on a timeout/5xx
    (never a blind repost), failed on a definitive 4xx. The spoof guard requires
    the calling session to be the acting agent's ``dm_bindings`` session.
    """
    agents, dm_bindings = _load_dm_context()
    _verify_dm_binding(dm_bindings, turn, session_name)

    acting = turn["agent"]
    src = agents.get(acting)
    if src is None:
        raise OutboundError(f"{report_kind} owner agent {acting!r} is not in the directory")
    # The pointer's owner_app_id must join to the acting agent's directory app_id
    # (for dm the sole owner; for mpim the woken agent's own app — the per-target
    # pointer field): defense in depth, since token/self-echo reconciliation both
    # key on the acting identity.
    owner_app_id = turn.get("owner_app_id", "")
    if owner_app_id and owner_app_id != src.get("app_id"):
        raise OutboundError(
            f"{report_kind} pointer owner_app_id {owner_app_id!r} does not match agent "
            f"{acting!r} directory app_id {src.get('app_id')!r}")

    token = load_bot_token(acting)
    text = compose_synthesis_text(body)  # escaped, no live mention
    # Direct messages are flat conversations: a reply to a top-level message
    # posts in-channel. Threading applies only when the human themselves replied
    # inside a thread (the turn's root differs from its own ts). The intent tuple
    # still keys on thread_root_ts either way.
    thread_ts = "" if turn["thread_root_ts"] == turn["ts"] else turn["thread_root_ts"]
    intent, outcome = _durable_company_post(
        op="dm",
        source_agent=acting, source_app_id=src["app_id"],
        source_bot_user_id=src["bot_user_id"],
        # No mention: key the tuple on the acting identity itself (like synthesis).
        target_agent=acting, target_bot_user_id=src["bot_user_id"],
        team=turn["team_id"], channel=turn["channel_id"], room="",
        human_root_ts=turn["thread_root_ts"], requester_session=session_name,
        body=body, text=text, thread_ts=thread_ts, token=token,
        metadata_nonce="",
        make_metadata=dm_metadata,
    )
    return _company_post_report(intent, outcome, kind=report_kind)


def post_company_dm_reply(
    *, body: str, origin_ts: str, session_name: str, turn_ref: str = "",
) -> dict[str, Any]:
    """Post a per-agent DM reply into the DM channel as the owner agent.

    The acting/owner agent is the DM pointer's ``agent``; see
    :func:`_post_dm_family_reply` for the durable-intent + spoof-guard contract.
    """
    if not body.strip():
        raise OutboundError("--body/--body-file must be non-empty")
    reconcile_posting_intents()
    prune()

    turn = _verify_pointer_dm(session_name, origin_ts, turn_ref)
    return _post_dm_family_reply(
        turn, body=body, session_name=session_name, report_kind="dm")


def post_company_mpim_reply(
    *, body: str, origin_ts: str, session_name: str, turn_ref: str = "",
) -> dict[str, Any]:
    """Post a group-DM (mpim) reply into the mpim channel as the woken agent.

    The acting agent is the mpim pointer's ``agent`` (the WOKEN agent for this
    per-target, per-session pointer); its own bot token posts the reply, flat
    unless the human threaded. Routed through the same durable-intent machinery
    (op=``dm``) so a timed-out reply reconciles against the owner app's admitted
    mpim self-echo (mpim_bot_author receipt carrying the nonce) rather than a
    blind repost. The spoof guard requires the calling session to be the woken
    agent's ``dm_bindings`` session.
    """
    if not body.strip():
        raise OutboundError("--body/--body-file must be non-empty")
    reconcile_posting_intents()
    prune()

    turn = _verify_pointer_mpim(session_name, origin_ts, turn_ref)
    return _post_dm_family_reply(
        turn, body=body, session_name=session_name, report_kind="mpim")


# --- CLI entry point -------------------------------------------------------

def _load_body_arg(body: str, body_file: str) -> str:
    if body and body_file:
        raise OutboundError("pass --body OR --body-file, not both")
    if body_file:
        try:
            return pathlib.Path(body_file).read_text(encoding="utf-8")
        except OSError as exc:
            raise OutboundError(f"cannot read --body-file {body_file}: {exc}") from exc
    if body:
        return body
    raise OutboundError("either --body or --body-file is required")


def cmd_delegate(argv: list[str]) -> int:
    import argparse
    parser = argparse.ArgumentParser(prog="slack delegate")
    parser.add_argument("--to", required=True, help="Target agent name (slug)")
    parser.add_argument("--body", default="")
    parser.add_argument("--body-file", default="")
    parser.add_argument("--cancel", action="store_true",
                        help="Expire the caller's own pending delegation to --to")
    parser.add_argument("--origin-ts", default="",
                        help="Pin a specific turn ts when a newer wake overwrote the pointer")
    parser.add_argument("--turn-ref", default="",
                        help="Immutable turn reference from the Slack reminder")
    parser.add_argument(
        "--raw", action="store_true",
        help=("Send the body verbatim, skipping the accidental-mrkdwn guard "
              "(by default, tildes that would pair into unintended Slack "
              "strikethrough are neutralized)."))
    args = parser.parse_args(argv)

    session_name = os.environ.get("GC_SESSION_NAME", "").strip()
    if args.cancel:
        result = run_cancel(
            to=args.to, origin_ts=args.origin_ts, session_name=session_name,
            turn_ref=args.turn_ref)
    else:
        body = _load_body_arg(args.body, args.body_file)
        if not args.raw:
            # gp-o42: tilde pairs render as strikethrough in Slack mrkdwn.
            # Local import keeps the library surface of this module stdlib-only.
            import slack_mrkdwn
            body = slack_mrkdwn.escape_accidental_mrkdwn(body)
        result = run_delegate(
            to=args.to, body=body, origin_ts=args.origin_ts,
            session_name=session_name, turn_ref=args.turn_ref)
    print(json.dumps(result, indent=2, sort_keys=True))
    if result.get("status") == "parked":
        print(
            f"note: delegation parked (nonce {result.get('nonce')}); not reposting on "
            "ambiguity — recovery is automatic once the post's receipt lands",
            file=sys.stderr,
        )
    return 0


def main(argv: list[str]) -> int:
    if not argv:
        print("error: expected a subcommand (delegate)", file=sys.stderr)
        return 2
    sub, rest = argv[0], argv[1:]
    try:
        if sub == "delegate":
            return cmd_delegate(rest)
        print(f"error: unknown subcommand {sub!r}", file=sys.stderr)
        return 2
    except OutboundError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
