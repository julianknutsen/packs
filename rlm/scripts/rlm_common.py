from __future__ import annotations

import fnmatch
import glob
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import fcntl

try:
    import tomllib
except ModuleNotFoundError:
    tomllib = None


RUNTIME_SCHEMA = 1
RUNTIME_VERSION = "0.1.0"
DEFAULT_SECRET_DENYLIST = [
    ".env",
    ".env.*",
    "*.pem",
    "*.key",
    "*credentials*.json",
    "*credentials*.yaml",
    "*credentials*.yml",
    "*token*.json",
    "*token*.yaml",
    "*token*.yml",
]
DEFAULT_ALLOWED_ENVIRONMENTS = ["docker"]
DEFAULT_MAX_DEPTH = 2
DEFAULT_MAX_DEPTH_CEILING = 3
DEFAULT_MAX_ITERATIONS = 16
DEFAULT_MAX_ITERATIONS_CEILING = 24
DEFAULT_MAX_CALLS_PER_HOUR = 12
DEFAULT_MAX_DURATION_SECONDS = 300
DEFAULT_MAX_TOKENS_PER_CALL = 120000
DEFAULT_LOG_RETENTION_DAYS = 7
MAX_STAGED_BYTES = 128 * 1024 * 1024
MAX_INLINE_BYTES = 256 * 1024
MAX_TOOL_CHARS = 20000
MAX_TOOL_LINES = 400
MAX_LIST_FILES = 500
MAX_GREP_RESULTS = 200
EXCLUDED_WALK_DIRS = {".git", ".gc", ".beads", "__pycache__"}


class CLIError(RuntimeError):
    def __init__(self, message: str, exit_code: int = 1):
        super().__init__(message)
        self.exit_code = exit_code


@dataclass
class RuntimeConfig:
    schema: int = RUNTIME_SCHEMA
    backend: str = "openai"
    model: str = "gpt-5-mini"
    base_url: str = ""
    backend_api_key_env: str = "OPENAI_API_KEY"
    remote_backend_allowed: bool = False
    allowed_environments: list[str] = field(
        default_factory=lambda: DEFAULT_ALLOWED_ENVIRONMENTS.copy()
    )
    default_environment: str = "docker"
    max_depth: int = DEFAULT_MAX_DEPTH
    max_depth_ceiling: int = DEFAULT_MAX_DEPTH_CEILING
    max_iterations: int = DEFAULT_MAX_ITERATIONS
    max_iterations_ceiling: int = DEFAULT_MAX_ITERATIONS_CEILING
    max_calls_per_hour: int = DEFAULT_MAX_CALLS_PER_HOUR
    max_duration_seconds: int = DEFAULT_MAX_DURATION_SECONDS
    max_tokens_per_call: int = DEFAULT_MAX_TOKENS_PER_CALL
    log_enabled: bool = True
    log_retention_days: int = DEFAULT_LOG_RETENTION_DAYS
    ignore_gitignore: bool = False
    secret_denylist: list[str] = field(default_factory=lambda: DEFAULT_SECRET_DENYLIST.copy())
    docker_image: str = ""
    installed_at: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RuntimeConfig":
        cfg = cls(
            schema=int(data.get("schema", RUNTIME_SCHEMA)),
            backend=str(data.get("backend", "openai")),
            model=str(data.get("model", "gpt-5-mini")),
            base_url=str(data.get("base_url", "")),
            backend_api_key_env=str(data.get("backend_api_key_env", "OPENAI_API_KEY")),
            remote_backend_allowed=bool(data.get("remote_backend_allowed", False)),
            allowed_environments=list(data.get("allowed_environments", ["docker"])),
            default_environment=str(data.get("default_environment", "docker")),
            max_depth=int(data.get("max_depth", DEFAULT_MAX_DEPTH)),
            max_depth_ceiling=int(data.get("max_depth_ceiling", DEFAULT_MAX_DEPTH_CEILING)),
            max_iterations=int(data.get("max_iterations", DEFAULT_MAX_ITERATIONS)),
            max_iterations_ceiling=int(
                data.get("max_iterations_ceiling", DEFAULT_MAX_ITERATIONS_CEILING)
            ),
            max_calls_per_hour=int(data.get("max_calls_per_hour", DEFAULT_MAX_CALLS_PER_HOUR)),
            max_duration_seconds=int(
                data.get("max_duration_seconds", DEFAULT_MAX_DURATION_SECONDS)
            ),
            max_tokens_per_call=int(
                data.get("max_tokens_per_call", DEFAULT_MAX_TOKENS_PER_CALL)
            ),
            log_enabled=bool(data.get("log_enabled", True)),
            log_retention_days=int(
                data.get("log_retention_days", DEFAULT_LOG_RETENTION_DAYS)
            ),
            ignore_gitignore=bool(data.get("ignore_gitignore", False)),
            secret_denylist=list(data.get("secret_denylist", DEFAULT_SECRET_DENYLIST)),
            docker_image=str(data.get("docker_image", "")),
            installed_at=str(data.get("installed_at", "")),
        )
        cfg.validate()
        return cfg

    def validate(self) -> None:
        if self.schema != RUNTIME_SCHEMA:
            raise CLIError(
                f"Unsupported .gc/rlm/config.toml schema {self.schema}; expected {RUNTIME_SCHEMA}.",
                exit_code=2,
            )
        self.backend = self.backend.strip().lower()
        if self.backend not in {"openai"}:
            raise CLIError(
                f"Unsupported backend {self.backend!r}; phase-1 support is limited to OpenAI-compatible backends.",
                exit_code=2,
            )
        allowed = []
        for value in self.allowed_environments:
            env = str(value).strip().lower()
            if env and env not in allowed:
                allowed.append(env)
        self.allowed_environments = allowed
        self.default_environment = self.default_environment.strip().lower()
        if self.default_environment not in {"docker", "local"}:
            raise CLIError(
                f"Unsupported default environment {self.default_environment!r}.",
                exit_code=2,
            )
        if self.default_environment not in self.allowed_environments:
            raise CLIError(
                "default_environment must be present in allowed_environments.",
                exit_code=2,
            )
        if self.default_environment == "docker" and is_loopback_url(self.base_url):
            raise CLIError(
                "Docker execution cannot use a loopback base_url. Use --environment local or a routable host.",
                exit_code=2,
            )
        if self.max_depth < 1 or self.max_depth > self.max_depth_ceiling:
            raise CLIError("Invalid max_depth policy in .gc/rlm/config.toml.", exit_code=2)
        if self.max_iterations < 1 or self.max_iterations > self.max_iterations_ceiling:
            raise CLIError(
                "Invalid max_iterations policy in .gc/rlm/config.toml.",
                exit_code=2,
            )
        if self.max_calls_per_hour < 1:
            raise CLIError("max_calls_per_hour must be positive.", exit_code=2)
        if self.max_duration_seconds < 1:
            raise CLIError("max_duration_seconds must be positive.", exit_code=2)
        if self.max_tokens_per_call < 1:
            raise CLIError("max_tokens_per_call must be positive.", exit_code=2)
        if self.log_retention_days < 0:
            raise CLIError("log_retention_days must be non-negative.", exit_code=2)

    def to_toml(self) -> str:
        lines = [
            f"schema = {self.schema}",
            "",
            f'backend = {toml_quote(self.backend)}',
            f'model = {toml_quote(self.model)}',
            f'base_url = {toml_quote(self.base_url)}',
            f'backend_api_key_env = {toml_quote(self.backend_api_key_env)}',
            f"remote_backend_allowed = {toml_bool(self.remote_backend_allowed)}",
            "",
            f"allowed_environments = {toml_list(self.allowed_environments)}",
            f'default_environment = {toml_quote(self.default_environment)}',
            "",
            f"max_depth = {self.max_depth}",
            f"max_depth_ceiling = {self.max_depth_ceiling}",
            f"max_iterations = {self.max_iterations}",
            f"max_iterations_ceiling = {self.max_iterations_ceiling}",
            f"max_calls_per_hour = {self.max_calls_per_hour}",
            f"max_duration_seconds = {self.max_duration_seconds}",
            f"max_tokens_per_call = {self.max_tokens_per_call}",
            "",
            f"log_enabled = {toml_bool(self.log_enabled)}",
            f"log_retention_days = {self.log_retention_days}",
            "",
            f"ignore_gitignore = {toml_bool(self.ignore_gitignore)}",
            f"secret_denylist = {toml_list(self.secret_denylist)}",
            "",
            f'docker_image = {toml_quote(self.docker_image)}',
            f'installed_at = {toml_quote(self.installed_at)}',
            "",
        ]
        return "\n".join(lines)


@dataclass
class CorpusFile:
    display_path: str
    original_path: str
    staged_relpath: str
    size_bytes: int
    line_count: int
    sha256: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class CorpusBundle:
    run_id: str
    run_dir: Path
    context_dir: Path
    output_dir: Path
    files: list[CorpusFile]
    inline_files: dict[str, str]
    truncated_paths: list[str]
    roots: list[str]
    total_bytes: int
    file_count: int

    def manifest_dicts(self) -> list[dict[str, Any]]:
        return [entry.to_dict() for entry in self.files]


def toml_quote(value: str) -> str:
    escaped = (
        value.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )
    return f'"{escaped}"'


def toml_bool(value: bool) -> str:
    return "true" if value else "false"


def toml_list(values: list[str]) -> str:
    return "[" + ", ".join(toml_quote(str(value)) for value in values) + "]"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def city_root_from_env() -> Path:
    raw = os.environ.get("GC_CITY_PATH")
    if not raw:
        raise CLIError("Missing GC_CITY_PATH.", exit_code=2)
    return Path(raw).resolve()


def pack_dir_from_env() -> Path:
    raw = os.environ.get("GC_PACK_DIR")
    if not raw:
        raise CLIError("Missing GC_PACK_DIR.", exit_code=2)
    return Path(raw).resolve()


def runtime_dir(city_root: Path) -> Path:
    return city_root / ".gc" / "rlm"


def venv_python(city_root: Path) -> Path:
    return runtime_dir(city_root) / "venv" / "bin" / "python"


def config_path(city_root: Path) -> Path:
    return runtime_dir(city_root) / "config.toml"


def logs_dir(city_root: Path) -> Path:
    return runtime_dir(city_root) / "logs"


def cache_dir(city_root: Path) -> Path:
    return runtime_dir(city_root) / "cache"


def lock_path(city_root: Path) -> Path:
    return runtime_dir(city_root) / "install.lock"


def ensure_runtime_layout(city_root: Path) -> None:
    root = runtime_dir(city_root)
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(root, 0o700)
    ignore = root / ".gitignore"
    if not ignore.exists():
        ignore.write_text("*\n!.gitignore\n", encoding="utf-8")
        os.chmod(ignore, 0o600)
    logs = logs_dir(city_root)
    logs.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(logs, 0o700)
    cache = cache_dir(city_root)
    cache.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(cache, 0o700)


@contextmanager
def file_lock(path: Path, *, exclusive: bool = True):
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        try:
            handle = os.fdopen(fd, "a+", encoding="utf-8")
        except Exception:
            os.close(fd)
            raise
        with handle:
            mode = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
            fcntl.flock(handle.fileno(), mode)
            yield handle
    finally:
        # fd is closed by fdopen context manager
        pass


def require_tomllib() -> Any:
    if tomllib is None:
        raise CLIError("python3 3.11+ is required for the rlm pack.", exit_code=2)
    return tomllib


def load_runtime_config(city_root: Path) -> RuntimeConfig:
    path = config_path(city_root)
    if not path.exists():
        raise CLIError(
            "RLM runtime is not installed for this city. Run 'gc rlm install' first.",
            exit_code=2,
        )
    with path.open("rb") as handle:
        data = require_tomllib().load(handle)
    return RuntimeConfig.from_dict(data)


def save_runtime_config(city_root: Path, cfg: RuntimeConfig) -> None:
    cfg.validate()
    path = config_path(city_root)
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        handle.write(cfg.to_toml())
    os.chmod(path, 0o600)


def run(
    args: list[str],
    *,
    capture_output: bool = False,
    text: bool = True,
    env: dict[str, str] | None = None,
    cwd: Path | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            args,
            capture_output=capture_output,
            text=text,
            env=env,
            cwd=str(cwd) if cwd else None,
            check=check,
        )
    except FileNotFoundError as exc:
        raise CLIError(f"Command not found: {args[0]}", exit_code=1) from exc
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr.strip() if exc.stderr else ""
        message = stderr or f"Command failed: {' '.join(args)}"
        raise CLIError(message, exit_code=1) from exc
    return result


def default_backend_api_key_env(backend: str) -> str:
    backend = backend.strip().lower()
    if backend == "openai":
        return "OPENAI_API_KEY"
    return ""


def is_loopback_url(base_url: str) -> bool:
    if not base_url:
        return False
    parsed = urlparse(base_url)
    host = (parsed.hostname or "").lower()
    return host in {"127.0.0.1", "localhost", "::1"}


def backend_requires_network(cfg: RuntimeConfig) -> bool:
    if cfg.base_url:
        return not is_loopback_url(cfg.base_url)
    return cfg.backend.strip().lower() == "openai"


def ensure_remote_backend_policy(cfg: RuntimeConfig) -> None:
    if backend_requires_network(cfg) and not cfg.remote_backend_allowed:
        raise CLIError(
            "This backend requires network access. Re-run 'gc rlm install' with --allow-remote-backend.",
            exit_code=2,
        )


def docker_image_tag(pack_dir: Path) -> str:
    hasher = hashlib.sha256()
    for rel in ["requirements.lock", "docker/Dockerfile"]:
        path = pack_dir / rel
        if path.exists():
            hasher.update(path.read_bytes())
    scripts_dir = pack_dir / "scripts"
    if scripts_dir.exists():
        for path in sorted(scripts_dir.glob("*.py")):
            hasher.update(path.read_bytes())
    return f"gascity-rlm:{RUNTIME_VERSION}-{hasher.hexdigest()[:12]}"


def check_python_version() -> tuple[int, ...]:
    version_info = tuple(sys.version_info[:3])
    if tuple(version_info) < (3, 11, 0):
        raise CLIError("python3 3.11+ is required for the rlm pack.", exit_code=2)
    return tuple(version_info)


def require_docker() -> None:
    run(["docker", "info"], capture_output=True)


def require_runtime_python(city_root: Path) -> Path:
    python = venv_python(city_root)
    if not python.exists():
        raise CLIError(
            "RLM runtime is not installed for this city. Run 'gc rlm install' first.",
            exit_code=2,
        )
    return python


def prune_old_logs(city_root: Path, days: int) -> None:
    if days < 0:
        return
    cutoff = time.time() - (days * 86400)
    for path in logs_dir(city_root).glob("*"):
        try:
            if path.stat().st_mtime < cutoff:
                if path.is_dir():
                    shutil.rmtree(path, ignore_errors=True)
                else:
                    path.unlink(missing_ok=True)
        except FileNotFoundError:
            continue


def prune_stale_cache_runs(city_root: Path, max_age_seconds: int = 3600) -> None:
    cutoff = time.time() - max_age_seconds
    for path in cache_dir(city_root).glob("rlm-*"):
        try:
            if path.is_dir() and path.stat().st_mtime < cutoff:
                shutil.rmtree(path, ignore_errors=True)
        except FileNotFoundError:
            continue


def recent_run_summaries(city_root: Path) -> list[dict[str, Any]]:
    summaries = []
    for path in logs_dir(city_root).glob("*.summary.json"):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            data["_path"] = str(path)
            summaries.append(data)
        except (OSError, json.JSONDecodeError):
            continue
    summaries.sort(key=lambda item: item.get("started_at", ""), reverse=True)
    return summaries


def update_rate_limit(city_root: Path, limit: int) -> None:
    state_path = cache_dir(city_root) / "rate_limit.json"
    now = time.time()
    with file_lock(cache_dir(city_root) / "rate_limit.lock"):
        entries: list[float] = []
        if state_path.exists():
            try:
                entries = json.loads(state_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                entries = []
        cutoff = now - 3600
        entries = [float(value) for value in entries if float(value) >= cutoff]
        if len(entries) >= limit:
            next_allowed = datetime.fromtimestamp(min(entries) + 3600, tz=timezone.utc)
            raise CLIError(
                f"RLM rate limit exceeded ({limit} calls/hour). Next call allowed around {next_allowed.isoformat()}.",
                exit_code=4,
            )
        entries.append(now)
        state_path.write_text(json.dumps(entries), encoding="utf-8")
        os.chmod(state_path, 0o600)


def resolve_input_path(raw: str, cwd: Path) -> Path:
    path = Path(raw)
    if not path.is_absolute():
        path = cwd / path
    return path.resolve()


def display_path(path: Path, cwd: Path) -> str:
    try:
        return path.relative_to(cwd).as_posix()
    except ValueError:
        return path.as_posix()


def safe_stage_relpath(display_value: str) -> Path:
    normalized = display_value.replace("\\", "/")
    if normalized.startswith("/"):
        normalized = f"_abs{normalized}"
    if normalized in {"", ".", ".."}:
        normalized = "_root"
    normalized = normalized.replace(":", "_")
    return Path(normalized)


def reserve_staged_path(context_dir: Path, staged_rel: Path) -> Path:
    candidate = context_dir / staged_rel
    if not candidate.exists():
        return candidate
    stem = candidate.stem
    suffix = candidate.suffix
    index = 1
    while True:
        alt = candidate.with_name(f"{stem}__{index}{suffix}")
        if not alt.exists():
            return alt
        index += 1


def is_binary_blob(data: bytes) -> bool:
    if not data:
        return False
    if b"\x00" in data:
        return True
    try:
        data.decode("utf-8")
        return False
    except UnicodeDecodeError:
        pass
    text_bytes = sum(
        1 for byte in data if byte in b"\t\n\r\f\b" or 32 <= byte <= 126
    )
    return (text_bytes / len(data)) < 0.70


def read_text_file(path: Path) -> tuple[str, int, str]:
    data = path.read_bytes()
    if is_binary_blob(data):
        raise CLIError(f"Binary file not allowed in staged corpus: {path}", exit_code=5)
    text = data.decode("utf-8", errors="replace")
    digest = hashlib.sha256(data).hexdigest()
    return text, len(data), digest


def maybe_find_git_root(path: Path) -> Path | None:
    probe = path if path.is_dir() else path.parent
    try:
        proc = subprocess.run(
            ["git", "-C", str(probe), "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        return None
    if proc.returncode != 0:
        return None
    root = proc.stdout.strip()
    return Path(root).resolve() if root else None


def filter_gitignored(paths: list[Path], respect_gitignore: bool) -> tuple[list[Path], list[str]]:
    if not respect_gitignore:
        return paths, []
    groups: dict[Path, list[Path]] = {}
    passthrough: list[Path] = []
    for path in paths:
        git_root = maybe_find_git_root(path)
        if git_root is None:
            passthrough.append(path)
            continue
        groups.setdefault(git_root, []).append(path)

    kept = list(passthrough)
    ignored: list[str] = []
    for git_root, members in groups.items():
        rels = []
        reverse: dict[str, Path] = {}
        for member in members:
            rel = os.path.relpath(member, git_root)
            rels.append(rel)
            reverse[rel] = member
        try:
            proc = subprocess.run(
                ["git", "-C", str(git_root), "check-ignore", "--stdin"],
                input="\n".join(rels) + ("\n" if rels else ""),
                capture_output=True,
                text=True,
            )
        except FileNotFoundError:
            kept.extend(members)
            continue
        ignored_set = {line.strip() for line in proc.stdout.splitlines() if line.strip()}
        for rel in rels:
            if rel in ignored_set:
                ignored.append(reverse[rel].as_posix())
            else:
                kept.append(reverse[rel])
    return kept, ignored


def matches_secret_denylist(path: Path, denylist: list[str], cwd: Path) -> bool:
    candidates = [path.name, display_path(path, cwd)]
    return any(fnmatch.fnmatch(candidate, pattern) for candidate in candidates for pattern in denylist)


def is_within_root(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def gather_candidates(
    *,
    cwd: Path,
    path_args: list[str],
    glob_args: list[str],
) -> tuple[list[Path], list[str]]:
    candidates: list[Path] = []
    roots: list[str] = []

    for raw in path_args:
        resolved = resolve_input_path(raw, cwd)
        if not resolved.exists():
            raise CLIError(f"Path not found: {raw}", exit_code=2)
        roots.append(display_path(resolved, cwd))
        if resolved.is_file():
            candidates.append(resolved)
            continue
        for dirpath, dirnames, filenames in os.walk(resolved, followlinks=False):
            current = Path(dirpath)
            dirnames[:] = [
                name
                for name in dirnames
                if name not in EXCLUDED_WALK_DIRS and not (current / name).is_symlink()
            ]
            for filename in filenames:
                candidate = (current / filename)
                if candidate.is_symlink():
                    try:
                        if not candidate.resolve().is_relative_to(resolved.resolve()):
                            continue
                    except ValueError:
                        continue
                candidates.append(candidate.resolve())

    for pattern in glob_args:
        matches = glob.glob(pattern, root_dir=str(cwd), recursive=True)
        roots.append(f"glob:{pattern}")
        for match in matches:
            resolved = resolve_input_path(match, cwd)
            if resolved.is_file():
                if not is_within_root(resolved, cwd):
                    continue
                candidates.append(resolved)
            elif resolved.is_dir():
                for dirpath, dirnames, filenames in os.walk(resolved, followlinks=False):
                    current = Path(dirpath)
                    dirnames[:] = [
                        name
                        for name in dirnames
                        if name not in EXCLUDED_WALK_DIRS and not (current / name).is_symlink()
                    ]
                    for filename in filenames:
                        candidate = (current / filename)
                        if candidate.is_symlink():
                            if not is_within_root(candidate, cwd):
                                continue
                        resolved_candidate = candidate.resolve()
                        if not is_within_root(resolved_candidate, cwd):
                            continue
                        candidates.append(resolved_candidate)

    deduped: list[Path] = []
    seen: set[Path] = set()
    for candidate in candidates:
        if candidate not in seen:
            seen.add(candidate)
            deduped.append(candidate)
    return deduped, roots


def stage_corpus(
    *,
    city_root: Path,
    cwd: Path,
    path_args: list[str],
    glob_args: list[str],
    stdin_text: str | None,
    cfg: RuntimeConfig,
) -> CorpusBundle:
    run_id = uuid.uuid4().hex
    run_dir = Path(tempfile.mkdtemp(prefix=f"rlm-{run_id}-", dir=cache_dir(city_root)))
    try:
        context_dir = run_dir / "context"
        output_dir = run_dir / "output"
        context_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        output_dir.mkdir(mode=0o700, parents=True, exist_ok=True)

        candidates, roots = gather_candidates(cwd=cwd, path_args=path_args, glob_args=glob_args)
        respect_gitignore = not cfg.ignore_gitignore
        candidates, ignored_paths = filter_gitignored(candidates, respect_gitignore)

        files: list[CorpusFile] = []
        inline_files: dict[str, str] = {}
        truncated_paths = list(ignored_paths)
        total_bytes = 0
        inline_budget = MAX_INLINE_BYTES

        for candidate in candidates:
            if not candidate.is_file():
                continue
            if matches_secret_denylist(candidate, cfg.secret_denylist, cwd):
                truncated_paths.append(display_path(candidate, cwd))
                continue
            try:
                text, size_bytes, digest = read_text_file(candidate)
            except CLIError:
                truncated_paths.append(display_path(candidate, cwd))
                continue
            if total_bytes + size_bytes > MAX_STAGED_BYTES:
                truncated_paths.append(display_path(candidate, cwd))
                continue

            total_bytes += size_bytes
            shown_path = display_path(candidate, cwd)
            staged_rel = safe_stage_relpath(shown_path)
            staged_path = reserve_staged_path(context_dir, staged_rel)
            staged_rel = staged_path.relative_to(context_dir)
            staged_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            staged_path.write_text(text, encoding="utf-8")
            line_count = text.count("\n") + (1 if text else 0)
            files.append(
                CorpusFile(
                    display_path=shown_path,
                    original_path=candidate.as_posix(),
                    staged_relpath=staged_rel.as_posix(),
                    size_bytes=size_bytes,
                    line_count=line_count,
                    sha256=digest,
                )
            )
            text_bytes = len(text.encode("utf-8"))
            if text_bytes <= inline_budget and len(inline_files) < 12:
                inline_files[shown_path] = text
                inline_budget -= text_bytes

        if stdin_text:
            staged_rel = Path("_stdin/stdin.txt")
            staged_path = reserve_staged_path(context_dir, staged_rel)
            staged_rel = staged_path.relative_to(context_dir)
            staged_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            staged_path.write_text(stdin_text, encoding="utf-8")
            digest = hashlib.sha256(stdin_text.encode("utf-8")).hexdigest()
            line_count = stdin_text.count("\n") + (1 if stdin_text else 0)
            files.append(
                CorpusFile(
                    display_path="stdin.txt",
                    original_path="<stdin>",
                    staged_relpath=staged_rel.as_posix(),
                    size_bytes=len(stdin_text.encode("utf-8")),
                    line_count=line_count,
                    sha256=digest,
                )
            )
            if len(stdin_text.encode("utf-8")) <= inline_budget:
                inline_files["stdin.txt"] = stdin_text

        if not files:
            raise CLIError(
                "No eligible text files were staged for RLM analysis. Expand the scope, disable the deny-list patterns, or pass --stdin.",
                exit_code=2,
            )

        return CorpusBundle(
            run_id=run_id,
            run_dir=run_dir,
            context_dir=context_dir,
            output_dir=output_dir,
            files=files,
            inline_files=inline_files,
            truncated_paths=sorted(set(truncated_paths)),
            roots=roots,
            total_bytes=total_bytes,
            file_count=len(files),
        )
    except BaseException:
        shutil.rmtree(run_dir, ignore_errors=True)
        raise


def build_context_payload(bundle: CorpusBundle) -> dict[str, Any]:
    preview = [entry.to_dict() for entry in bundle.files[:200]]
    extra = max(0, len(bundle.files) - len(preview))
    return {
        "run_id": bundle.run_id,
        "manifest_summary": {
            "file_count": bundle.file_count,
            "total_bytes": bundle.total_bytes,
            "roots": bundle.roots,
            "preview_count": len(preview),
            "extra_files_not_in_preview": extra,
        },
        "manifest_preview": preview,
        "inline_files": bundle.inline_files,
        "truncated_paths": bundle.truncated_paths,
        "notes": [
            "Use list_files(), read_file(), and grep() for deeper inspection of the staged corpus.",
            "Paths in the manifest are relative to the caller working directory when possible.",
            "Do not fabricate citations; prefer returning the paths you actually inspected.",
        ],
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def strip_code_fence(text: str) -> str:
    stripped = text.strip()
    if stripped.startswith("```") and stripped.endswith("```"):
        lines = stripped.splitlines()
        if len(lines) >= 2:
            return "\n".join(lines[1:-1]).strip()
    return stripped


def summarize_error(exc: BaseException) -> str:
    return str(exc).strip() or exc.__class__.__name__


def iso_to_sortable(value: str) -> str:
    return value or ""


def docker_image_exists(tag: str) -> bool:
    if not tag:
        return False
    try:
        proc = run(["docker", "image", "inspect", tag], capture_output=True, check=False)
    except CLIError:
        return False
    return proc.returncode == 0


def maybe_read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def install_summary_payload(
    *,
    cfg: RuntimeConfig,
    rlms_version: str,
    docker_ready: bool,
) -> dict[str, Any]:
    return {
        "runtime_version": RUNTIME_VERSION,
        "rlms_version": rlms_version,
        "backend": cfg.backend,
        "model": cfg.model,
        "base_url": cfg.base_url,
        "default_environment": cfg.default_environment,
        "remote_backend_allowed": cfg.remote_backend_allowed,
        "docker_image": cfg.docker_image,
        "docker_ready": docker_ready,
        "installed_at": cfg.installed_at,
    }


def latest_run(summary_items: list[dict[str, Any]]) -> dict[str, Any] | None:
    return summary_items[0] if summary_items else None


def latest_failed_run(summary_items: list[dict[str, Any]]) -> dict[str, Any] | None:
    for item in summary_items:
        if item.get("status") != "ok":
            return item
    return None
