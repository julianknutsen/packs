from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

from rlm_common import (
    CLIError,
    DEFAULT_LOG_RETENTION_DAYS,
    DEFAULT_MAX_CALLS_PER_HOUR,
    DEFAULT_MAX_DEPTH,
    DEFAULT_MAX_DEPTH_CEILING,
    DEFAULT_MAX_DURATION_SECONDS,
    DEFAULT_MAX_ITERATIONS,
    DEFAULT_MAX_ITERATIONS_CEILING,
    DEFAULT_MAX_TOKENS_PER_CALL,
    RuntimeConfig,
    backend_requires_network,
    build_context_payload,
    check_python_version,
    city_root_from_env,
    default_backend_api_key_env,
    docker_image_exists,
    docker_image_tag,
    ensure_remote_backend_policy,
    ensure_runtime_layout,
    file_lock,
    install_summary_payload,
    latest_failed_run,
    latest_run,
    load_runtime_config,
    lock_path,
    logs_dir,
    maybe_read_json,
    pack_dir_from_env,
    prune_old_logs,
    prune_stale_cache_runs,
    recent_run_summaries,
    require_docker,
    require_runtime_python,
    run,
    runtime_dir,
    save_runtime_config,
    stage_corpus,
    update_rate_limit,
    utc_now_iso,
    venv_python,
    write_json,
)


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be >= 1")
    return parsed


def install_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="gc rlm install")
    parser.add_argument("--backend", choices=["openai"], default="openai")
    parser.add_argument("--model", required=True)
    parser.add_argument("--environment", choices=["docker", "local"], default="docker")
    parser.add_argument("--base-url", default="")
    parser.add_argument("--backend-api-key-env", default=None)
    parser.add_argument("--allow-remote-backend", action="store_true")
    parser.add_argument("--max-depth", type=positive_int, default=DEFAULT_MAX_DEPTH)
    parser.add_argument("--max-depth-ceiling", type=positive_int, default=DEFAULT_MAX_DEPTH_CEILING)
    parser.add_argument("--max-iterations", type=positive_int, default=DEFAULT_MAX_ITERATIONS)
    parser.add_argument(
        "--max-iterations-ceiling",
        type=positive_int,
        default=DEFAULT_MAX_ITERATIONS_CEILING,
    )
    parser.add_argument("--max-calls-per-hour", type=positive_int, default=DEFAULT_MAX_CALLS_PER_HOUR)
    parser.add_argument(
        "--max-duration-seconds",
        type=positive_int,
        default=DEFAULT_MAX_DURATION_SECONDS,
    )
    parser.add_argument(
        "--max-tokens-per-call",
        type=positive_int,
        default=DEFAULT_MAX_TOKENS_PER_CALL,
    )
    parser.add_argument("--log-retention-days", type=int, default=DEFAULT_LOG_RETENTION_DAYS)
    parser.add_argument("--ignore-gitignore", action="store_true")
    parser.add_argument("--disable-logging", action="store_true")
    return parser


def ask_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="gc rlm ask")
    parser.add_argument("--prompt", required=True)
    parser.add_argument("--path", action="append", default=[])
    parser.add_argument("--glob", dest="globs", action="append", default=[])
    parser.add_argument("--stdin", action="store_true")
    parser.add_argument("--output", choices=["text", "json"], default="text")
    parser.add_argument("--no-log-content", action="store_true")
    parser.add_argument("--max-depth", type=positive_int)
    parser.add_argument("--max-iterations", type=positive_int)
    return parser


def status_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="gc rlm status")
    parser.add_argument("--json", action="store_true")
    return parser


def uninstall_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="gc rlm uninstall")
    parser.add_argument("--purge-logs", action="store_true")
    parser.add_argument("--keep-image", action="store_true")
    return parser


def create_runtime_config(args: argparse.Namespace, pack_dir: Path) -> RuntimeConfig:
    backend_api_key_env = args.backend_api_key_env
    if backend_api_key_env is None:
        backend_api_key_env = default_backend_api_key_env(args.backend)
    cfg = RuntimeConfig(
        backend=args.backend.strip().lower(),
        model=args.model.strip(),
        base_url=args.base_url.strip(),
        backend_api_key_env=backend_api_key_env,
        remote_backend_allowed=bool(args.allow_remote_backend),
        allowed_environments=[args.environment],
        default_environment=args.environment,
        max_depth=args.max_depth,
        max_depth_ceiling=args.max_depth_ceiling,
        max_iterations=args.max_iterations,
        max_iterations_ceiling=args.max_iterations_ceiling,
        max_calls_per_hour=args.max_calls_per_hour,
        max_duration_seconds=args.max_duration_seconds,
        max_tokens_per_call=args.max_tokens_per_call,
        log_enabled=not args.disable_logging,
        log_retention_days=args.log_retention_days,
        ignore_gitignore=bool(args.ignore_gitignore),
        docker_image=docker_image_tag(pack_dir) if args.environment == "docker" else "",
        installed_at=utc_now_iso(),
    )
    cfg.validate()
    if backend_requires_network(cfg) and not cfg.remote_backend_allowed:
        raise CLIError(
            "This install uses a networked backend. Pass --allow-remote-backend to acknowledge that policy.",
            exit_code=2,
        )
    return cfg


def clamp_policy_override(override: int | None, default: int, ceiling: int) -> int:
    if override is None:
        return default
    return min(override, ceiling)


def install_runtime(args: argparse.Namespace) -> int:
    check_python_version()
    city_root = city_root_from_env()
    pack_dir = pack_dir_from_env()
    ensure_runtime_layout(city_root)

    with file_lock(lock_path(city_root)):
        cfg = create_runtime_config(args, pack_dir)
        if "docker" in cfg.allowed_environments:
            require_docker()

        venv_dir = runtime_dir(city_root) / "venv"
        run([sys.executable, "-m", "venv", str(venv_dir)])
        python = venv_python(city_root)
        run([str(python), "-m", "pip", "install", "--upgrade", "pip"])
        run(
            [
                str(python),
                "-m",
                "pip",
                "install",
                "--require-hashes",
                "-r",
                str(pack_dir / "requirements.lock"),
            ]
        )

        if "docker" in cfg.allowed_environments:
            run(
                [
                    "docker",
                    "build",
                    "-t",
                    cfg.docker_image,
                    "-f",
                    str(pack_dir / "docker" / "Dockerfile"),
                    str(pack_dir),
                ]
            )

        smoke = run(
            [
                str(python),
                "-c",
                (
                    "from importlib.metadata import version; "
                    "import rlm; "
                    "print(version('rlms'))"
                ),
            ],
            capture_output=True,
        )
        rlms_version = smoke.stdout.strip()
        save_runtime_config(city_root, cfg)
        write_json(
            runtime_dir(city_root) / "install-summary.json",
            install_summary_payload(
                cfg=cfg,
                rlms_version=rlms_version,
                docker_ready=docker_image_exists(cfg.docker_image) if cfg.docker_image else False,
            ),
        )

    print(f"Installed rlms {rlms_version} under {runtime_dir(city_root)}")
    if cfg.docker_image:
        print(f"Docker image: {cfg.docker_image}")
    return 0


def build_runner_spec(
    *,
    args: argparse.Namespace,
    cfg: RuntimeConfig,
    bundle: Any,
    cwd: Path,
    city_root: Path,
    container_mode: bool,
) -> Path:
    logs = logs_dir(city_root)
    spec = {
        "run_id": bundle.run_id,
        "prompt": args.prompt,
        "output": args.output,
        "no_log_content": bool(args.no_log_content),
        "log_enabled": cfg.log_enabled,
        "context_root": "/workspace/context" if container_mode else bundle.context_dir.as_posix(),
        "logs_dir": "/workspace/logs" if container_mode else logs.as_posix(),
        "context_payload": build_context_payload(bundle),
        "manifest": bundle.manifest_dicts(),
        "truncated_paths": bundle.truncated_paths,
        "backend": cfg.backend,
        "model": cfg.model,
        "base_url": cfg.base_url,
        "backend_api_key_env": cfg.backend_api_key_env,
        "max_depth": clamp_policy_override(args.max_depth, cfg.max_depth, cfg.max_depth_ceiling),
        "max_iterations": clamp_policy_override(
            args.max_iterations,
            cfg.max_iterations,
            cfg.max_iterations_ceiling,
        ),
        "max_duration_seconds": cfg.max_duration_seconds,
        "max_tokens_per_call": cfg.max_tokens_per_call,
        "default_environment": cfg.default_environment,
        "cwd": cwd.as_posix(),
        "city_root": city_root.as_posix(),
    }
    spec_path = bundle.output_dir / "spec.json"
    write_json(spec_path, spec)
    return spec_path


def ask_runtime(args: argparse.Namespace) -> int:
    city_root = city_root_from_env()
    ensure_runtime_layout(city_root)
    with file_lock(lock_path(city_root), exclusive=False):
        cfg = load_runtime_config(city_root)
        cfg.validate()
        ensure_remote_backend_policy(cfg)
        prune_old_logs(city_root, cfg.log_retention_days)
        prune_stale_cache_runs(city_root)
        cwd = Path.cwd().resolve()

        stdin_text = sys.stdin.read() if args.stdin else None
        if not args.path and not args.globs and not stdin_text:
            raise CLIError("Pass at least one --path/--glob or supply --stdin.", exit_code=2)

        bundle = stage_corpus(
            city_root=city_root,
            cwd=cwd,
            path_args=args.path,
            glob_args=args.globs,
            stdin_text=stdin_text,
            cfg=cfg,
        )
        update_rate_limit(city_root, cfg.max_calls_per_hour)
        try:
            if cfg.default_environment == "docker":
                require_docker()
                spec_path = build_runner_spec(
                    args=args,
                    cfg=cfg,
                    bundle=bundle,
                    cwd=cwd,
                    city_root=city_root,
                    container_mode=True,
                )
                command = [
                    "docker",
                    "run",
                    "--rm",
                    "--user",
                    f"{os.getuid()}:{os.getgid()}",
                    "--read-only",
                    "--cap-drop",
                    "ALL",
                    "--security-opt",
                    "no-new-privileges",
                    "--pids-limit",
                    "256",
                    "--memory",
                    "2g",
                    "--mount",
                    f"type=bind,src={bundle.context_dir},dst=/workspace/context,ro",
                    "--mount",
                    f"type=bind,src={bundle.output_dir},dst=/workspace/output,rw",
                    "--mount",
                    f"type=bind,src={logs_dir(city_root)},dst=/workspace/logs,rw",
                    "--mount",
                    "type=tmpfs,dst=/tmp,tmpfs-size=256m",
                    "--workdir",
                    "/workspace/output",
                ]
                if not cfg.remote_backend_allowed:
                    command.extend(["--network", "none"])
                if cfg.backend_api_key_env:
                    value = os.environ.get(cfg.backend_api_key_env, "")
                    if value:
                        command.extend(["-e", cfg.backend_api_key_env])
                command.extend([cfg.docker_image, "--spec", "/workspace/output/spec.json"])
                proc = run(command, check=False)
                return proc.returncode

            python = require_runtime_python(city_root)
            spec_path = build_runner_spec(
                args=args,
                cfg=cfg,
                bundle=bundle,
                cwd=cwd,
                city_root=city_root,
                container_mode=False,
            )
            proc = run(
                [str(python), str(pack_dir_from_env() / "scripts" / "rlm_runner.py"), "--spec", str(spec_path)],
                check=False,
            )
            return proc.returncode
        finally:
            shutil.rmtree(bundle.run_dir, ignore_errors=True)


def status_runtime(args: argparse.Namespace) -> int:
    city_root = city_root_from_env()
    pack_dir = pack_dir_from_env()
    runtime_root = runtime_dir(city_root)
    installed = config_path(city_root).exists() and venv_python(city_root).exists()
    summaries = recent_run_summaries(city_root) if runtime_root.exists() else []
    newest = latest_run(summaries)
    failure = latest_failed_run(summaries)

    payload: dict[str, Any] = {
        "installed": installed,
        "runtime_dir": runtime_root.as_posix(),
        "pack_dir": pack_dir.as_posix(),
        "newest_run": newest,
        "last_failure": failure,
    }

    if installed:
        cfg = load_runtime_config(city_root)
        payload["config"] = cfg.to_toml()
        payload["config_dict"] = cfg.__dict__
        try:
            proc = run(
                [str(venv_python(city_root)), "-c", "from importlib.metadata import version; print(version('rlms'))"],
                capture_output=True,
            )
            payload["rlms_version"] = proc.stdout.strip()
        except CLIError:
            payload["rlms_version"] = ""
        payload["docker_image_present"] = docker_image_exists(cfg.docker_image)
    else:
        payload["hint"] = "Run 'gc rlm install' to create .gc/rlm/ for this city."

    if args.json:
        printable = payload.copy()
        printable.pop("config", None)
        print(json.dumps(printable, indent=2, sort_keys=True))
        return 0

    print(f"installed: {'yes' if installed else 'no'}")
    print(f"runtime_dir: {runtime_root}")
    if not installed:
        print("hint: run 'gc rlm install'")
        return 0

    cfg = load_runtime_config(city_root)
    print(f"rlms_version: {payload.get('rlms_version', 'unknown')}")
    print(f"backend: {cfg.backend}")
    print(f"model: {cfg.model}")
    print(f"base_url: {cfg.base_url or '(default)'}")
    print(f"default_environment: {cfg.default_environment}")
    print(f"allowed_environments: {', '.join(cfg.allowed_environments)}")
    print(f"remote_backend_allowed: {'yes' if cfg.remote_backend_allowed else 'no'}")
    if cfg.docker_image:
        present = "yes" if payload.get("docker_image_present") else "no"
        print(f"docker_image: {cfg.docker_image} ({present})")
    if newest:
        print(
            "newest_run: "
            f"{newest.get('started_at', 'unknown')} "
            f"{newest.get('status', 'unknown')} "
            f"{newest.get('run_id', '')}"
        )
    if failure:
        print(
            "last_failure: "
            f"{failure.get('started_at', 'unknown')} "
            f"{failure.get('status', 'unknown')} "
            f"{failure.get('error', '')}"
        )
    return 0


def uninstall_runtime(args: argparse.Namespace) -> int:
    city_root = city_root_from_env()
    runtime_root = runtime_dir(city_root)
    with file_lock(lock_path(city_root)):
        cfg = maybe_read_json(runtime_root / "install-summary.json")
        docker_image = ""
        try:
            docker_image = load_runtime_config(city_root).docker_image
        except CLIError:
            docker_image = cfg.get("docker_image", "") if cfg else ""
        for rel in ["venv", "cache", "config.toml", "install-summary.json"]:
            path = runtime_root / rel
            if path.is_dir():
                shutil.rmtree(path, ignore_errors=True)
            else:
                path.unlink(missing_ok=True)
        if args.purge_logs:
            shutil.rmtree(logs_dir(city_root), ignore_errors=True)
        if docker_image and not args.keep_image and docker_image_exists(docker_image):
            run(["docker", "image", "rm", "-f", docker_image], check=False)

    print(f"Removed runtime state from {runtime_root}")
    if args.purge_logs:
        print("Logs removed.")
    return 0


def main(command: str, argv: list[str] | None = None) -> int:
    argv = list(argv or sys.argv[1:])
    if command == "install":
        args = install_parser().parse_args(argv)
        return install_runtime(args)
    if command == "ask":
        args = ask_parser().parse_args(argv)
        return ask_runtime(args)
    if command == "status":
        args = status_parser().parse_args(argv)
        return status_runtime(args)
    if command == "uninstall":
        args = uninstall_parser().parse_args(argv)
        return uninstall_runtime(args)
    raise CLIError(f"Unknown command: {command}", exit_code=2)


def entrypoint(command: str) -> None:
    try:
        raise SystemExit(main(command))
    except CLIError as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(exc.exit_code) from exc
