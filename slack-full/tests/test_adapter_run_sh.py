"""Behavioral tests for adapter/run.sh — the [[service]] entrypoint.

run.sh is the pack's defense against pin-bump outages: the adapter
binary is a gitignored build artifact, and ``gc import install``
re-materializes the pack cache git-only, so a service command pointing
straight at the binary dies on every pin bump. run.sh is checked in and
rebuilds the binary on missing before exec'ing it.

These tests run the real script in a temp dir that mimics a fresh
git-only materialization, with a stub ``go`` toolchain on PATH so they
are fast and hermetic (no real compile, no network):

  * missing binary -> exactly one ``go build`` -> built binary exec'd,
  * existing gc-slack-adapter -> exec'd directly, no build (idempotency),
  * env file is sourced and reaches the adapter process,
  * a missing *defaulted* env file warns but still starts, while a
    missing *explicitly configured* one is fatal (starting on ambient
    credentials would post to whatever workspace is in the environment),
  * the self-heal's failure paths exit 1 and publish nothing: no Go new
    enough for go.mod under ``GOTOOLCHAIN=local``, and ``go build``
    failing,
  * the self-heal survives the minimal supervisor environment it
    targets (no HOME) by supplying a build cache,
  * pack.toml keeps pointing the service at a checked-in command.

The harness owns HOME/XDG_CONFIG_HOME so the *defaulted* env-file path
resolves somewhere hermetic and absent, rather than reading the
developer's real ``~/.config/gc-slack-adapter/env``.
"""

from __future__ import annotations

import os
import pathlib
import shutil
import subprocess

import pytest

PACK_DIR = pathlib.Path(__file__).resolve().parent.parent
RUN_SH = PACK_DIR / "adapter" / "run.sh"
GO_MOD = PACK_DIR / "adapter" / "go.mod"

GO_STUB = """#!/usr/bin/env bash
# Stub Go toolchain: records every invocation (with its cwd, so tests
# can pin WHERE the build ran), and on `build` writes an executable at
# the -o target that prints a marker instead of compiling. Two knobs let
# tests drive run.sh's failure paths: GO_STUB_VERSION (what `go version`
# reports, defaulting high enough to satisfy any go.mod directive) and
# GO_STUB_BUILD_FAILS (make `build` exit non-zero).
echo "go $* cwd=$PWD" >> "$GO_STUB_LOG"
if [ "$1" = "version" ]; then
  echo "go version go${GO_STUB_VERSION:-99.0.0} stub/stub"
  exit 0
fi
if [ "$1" = "build" ]; then
  echo "buildenv GOCACHE=${GOCACHE:-unset} GOPATH=${GOPATH:-unset}" >> "$GO_STUB_LOG"
  if [ -n "${GO_STUB_BUILD_FAILS:-}" ]; then
    echo "go stub: simulated compile failure" >&2
    exit 1
  fi
  out=""
  prev=""
  for a in "$@"; do
    [ "$prev" = "-o" ] && out="$a"
    prev="$a"
  done
  [ -n "$out" ] || { echo "go stub: no -o target" >&2; exit 2; }
  printf '%s\\n' '#!/usr/bin/env bash' \\
    'echo "STUB_ADAPTER_RAN pwd=$PWD MARKER_VAR=${MARKER_VAR:-unset}"' > "$out"
  chmod +x "$out"
  exit 0
fi
echo "go stub: unexpected invocation: $*" >&2
exit 2
"""


def required_go_version() -> str:
    """The `go` directive run.sh asserts the toolchain against."""
    for line in GO_MOD.read_text().splitlines():
        if line.startswith("go "):
            return line.split(None, 1)[1].strip()
    raise AssertionError(f"no `go` directive in {GO_MOD}")


@pytest.fixture()
def harness(tmp_path: pathlib.Path):
    """Adapter dir holding what a git-only materialization ships —
    run.sh and go.mod, no binary — plus a stub `go` on PATH."""
    adapter = tmp_path / "adapter"
    adapter.mkdir()
    shutil.copy(RUN_SH, adapter / "run.sh")
    (adapter / "run.sh").chmod(0o755)
    # run.sh reads go.mod's `go` directive to assert the toolchain, and a
    # real materialization always ships it, so the harness must too.
    shutil.copy(GO_MOD, adapter / "go.mod")

    stub_bin = tmp_path / "stubbin"
    stub_bin.mkdir()
    go = stub_bin / "go"
    go.write_text(GO_STUB)
    go.chmod(0o755)

    # A HOME the tests own: the defaulted env-file path must resolve
    # somewhere hermetic and absent, never the developer's real one.
    fake_home = tmp_path / "home"
    (fake_home / ".config").mkdir(parents=True)

    go_log = tmp_path / "go-invocations.log"

    def run(
        env_file: pathlib.Path | None = None,
        extra_env: dict | None = None,
        drop_env: list[str] | None = None,
    ):
        env = dict(os.environ)
        env["PATH"] = f"{stub_bin}:{env['PATH']}"
        env["GO_STUB_LOG"] = str(go_log)
        env["HOME"] = str(fake_home)
        env["XDG_CONFIG_HOME"] = str(fake_home / ".config")
        # An explicitly set but missing GC_SLACK_ADAPTER_ENV is fatal by
        # design, so the "no env file" baseline is the defaulted path.
        env.pop("GC_SLACK_ADAPTER_ENV", None)
        if env_file is not None:
            env["GC_SLACK_ADAPTER_ENV"] = str(env_file)
        env.update(extra_env or {})
        for key in drop_env or []:
            env.pop(key, None)
        return subprocess.run(
            [str(adapter / "run.sh")],
            capture_output=True,
            text=True,
            env=env,
            timeout=30,
        )

    def go_invocations() -> list[str]:
        if not go_log.exists():
            return []
        return [l for l in go_log.read_text().splitlines() if l.startswith("go build")]

    def build_environments() -> list[str]:
        if not go_log.exists():
            return []
        return [l for l in go_log.read_text().splitlines() if l.startswith("buildenv ")]

    return adapter, run, go_invocations, build_environments


def test_missing_binary_is_rebuilt_and_execd(harness):
    adapter, run, go_invocations, _ = harness
    proc = run()
    assert proc.returncode == 0, proc.stderr
    assert "STUB_ADAPTER_RAN" in proc.stdout
    # Loud logging: the self-heal explains itself on stderr.
    assert "rebuilding from source" in proc.stderr
    assert "gc import install" in proc.stderr
    # Exactly one build, of the package (".") FROM the adapter dir —
    # the colocated sources, not whatever cwd the supervisor used.
    builds = go_invocations()
    assert len(builds) == 1, builds
    assert " . cwd=" in builds[0], builds[0]
    build_cwd = builds[0].split(" cwd=", 1)[1]
    assert pathlib.Path(build_cwd).resolve() == adapter.resolve(), builds[0]
    # Binary published atomically at its canonical path, no temp left.
    assert (adapter / "gc-slack-adapter").exists()
    assert not list(adapter.glob("gc-slack-adapter.build.*"))


def test_existing_binary_skips_build(harness):
    adapter, run, go_invocations, _ = harness
    prebuilt = adapter / "gc-slack-adapter"
    prebuilt.write_text("#!/usr/bin/env bash\necho PREBUILT_RAN\n")
    prebuilt.chmod(0o755)
    proc = run()
    assert proc.returncode == 0, proc.stderr
    assert "PREBUILT_RAN" in proc.stdout
    assert go_invocations() == []


def test_self_heal_is_idempotent_across_restarts(harness):
    adapter, run, go_invocations, _ = harness
    first = run()
    second = run()
    assert first.returncode == 0 and second.returncode == 0
    assert "STUB_ADAPTER_RAN" in second.stdout
    # Only the first start built; the restart exec'd the existing binary.
    assert len(go_invocations()) == 1


def test_env_file_is_sourced_into_adapter_env(harness):
    adapter, run, go_invocations, _ = harness
    env_file = adapter.parent / "envfile"
    env_file.write_text("MARKER_VAR=from-env-file\n")
    proc = run(env_file=env_file)
    assert proc.returncode == 0, proc.stderr
    assert "MARKER_VAR=from-env-file" in proc.stdout
    # And no missing-env-file warning when the file exists.
    assert "env file not found" not in proc.stderr


def test_missing_default_env_file_warns_but_still_starts(harness):
    """Nothing was configured, so the absent default is ordinary:
    supervised deployments legitimately inject the env another way, and
    the adapter itself rejects a missing required key."""
    adapter, run, go_invocations, _ = harness
    proc = run()
    assert proc.returncode == 0, proc.stderr
    assert "env file not found" in proc.stderr
    assert "STUB_ADAPTER_RAN" in proc.stdout


def test_explicit_missing_env_file_is_fatal(harness):
    """An operator-named GC_SLACK_ADAPTER_ENV that does not exist must
    stop startup. The adapter validates that credentials are *present*,
    never where they came from, so continuing would boot it against
    whatever Slack token is in the ambient environment — a
    wrong-workspace start, silently, behind one warning line."""
    adapter, run, go_invocations, _ = harness
    missing = adapter.parent / "no-such-env"
    proc = run(env_file=missing)
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert f"GC_SLACK_ADAPTER_ENV={missing} does not exist" in proc.stderr
    assert "ambient credentials" in proc.stderr
    # Nothing started, and the failure came before any build.
    assert "STUB_ADAPTER_RAN" not in proc.stdout
    assert go_invocations() == []


def test_explicit_env_file_is_still_fatal_when_a_binary_exists(harness):
    """The check must precede the exec fast path — otherwise the common
    case (binary already built) skips it entirely."""
    adapter, run, go_invocations, _ = harness
    prebuilt = adapter / "gc-slack-adapter"
    prebuilt.write_text("#!/usr/bin/env bash\necho PREBUILT_RAN\n")
    prebuilt.chmod(0o755)
    proc = run(env_file=adapter.parent / "no-such-env")
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert "PREBUILT_RAN" not in proc.stdout


def test_self_heal_builds_when_home_is_unset(harness):
    """Supervisor environments may not set HOME. Under `set -u` an
    unguarded $HOME aborts before the fast path — and `go build` then
    fails a second way, because it can only locate a build cache via
    GOCACHE, XDG_CACHE_HOME, or HOME. Assert the *build* survives, not
    merely the shell."""
    adapter, run, go_invocations, build_environments = harness
    proc = run(
        drop_env=[
            "HOME",
            "XDG_CONFIG_HOME",
            "XDG_CACHE_HOME",
            "GOCACHE",
            "GOPATH",
            # GOMODCACHE too: it is the other way Go can locate a module
            # cache, so leaving the developer's own exported value in
            # place would stop this test simulating the bare supervisor
            # environment it is named for.
            "GOMODCACHE",
            "GC_SLACK_ADAPTER_ENV",
        ]
    )
    assert proc.returncode == 0, proc.stderr
    assert "STUB_ADAPTER_RAN" in proc.stdout
    assert len(go_invocations()) == 1, go_invocations()
    # The build was handed a usable cache and module path rather than
    # inheriting neither, which is what actually fails on such a host.
    tmp = os.environ.get("TMPDIR", "/tmp").rstrip("/")
    assert build_environments() == [
        f"buildenv GOCACHE={tmp}/gc-slack-adapter-gocache "
        f"GOPATH={tmp}/gc-slack-adapter-gopath"
    ], build_environments()
    assert (adapter / "gc-slack-adapter").exists()


@pytest.mark.parametrize("cache_var", ["XDG_CACHE_HOME", "GOCACHE"])
def test_gopath_is_defaulted_whenever_home_is_unset(harness, tmp_path, cache_var):
    """GOPATH must not be gated on the build-cache condition: the module
    cache is located via GOMODCACHE or GOPATH, and GOPATH derives from
    HOME *alone*. The operator who partially remediates — reads the cache
    error, exports GOCACHE, leaves HOME unset — would otherwise still
    lose a toolchain fetch to ``module cache not found: neither
    GOMODCACHE nor GOPATH is set``, the same stranding this self-heal
    exists to remove. An already-usable cache is left alone."""
    adapter, run, go_invocations, build_environments = harness
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    drop = [
        "HOME",
        "XDG_CONFIG_HOME",
        "XDG_CACHE_HOME",
        "GOCACHE",
        "GOPATH",
        "GOMODCACHE",
        "GC_SLACK_ADAPTER_ENV",
    ]
    drop.remove(cache_var)  # extra_env is applied before drop_env
    proc = run(extra_env={cache_var: str(cache_dir)}, drop_env=drop)
    assert proc.returncode == 0, proc.stderr
    assert "STUB_ADAPTER_RAN" in proc.stdout
    assert len(go_invocations()) == 1, go_invocations()
    tmp = os.environ.get("TMPDIR", "/tmp").rstrip("/")
    expected_gocache = str(cache_dir) if cache_var == "GOCACHE" else "unset"
    assert build_environments() == [
        f"buildenv GOCACHE={expected_gocache} "
        f"GOPATH={tmp}/gc-slack-adapter-gopath"
    ], build_environments()


def test_no_default_env_file_path_is_never_advertised(harness):
    """With neither HOME nor XDG_CONFIG_HOME set — the supervisor
    environment this script targets — the "default" env file path
    degenerates to the root-owned /.config/gc-slack-adapter/env. Never
    print it: it reads as a remedy, and it is one the operator cannot
    carry out."""
    adapter, run, go_invocations, _ = harness
    proc = run(drop_env=["HOME", "XDG_CONFIG_HOME", "GC_SLACK_ADAPTER_ENV"])
    assert proc.returncode == 0, proc.stderr
    assert "/.config/gc-slack-adapter/env" not in proc.stderr, proc.stderr
    assert "no default env file path" in proc.stderr
    # Still only a warning: the adapter fail-fasts on a missing key itself.
    assert "STUB_ADAPTER_RAN" in proc.stdout


def test_shell_function_named_go_does_not_shadow_the_toolchain(harness):
    """``command -v go`` returns the bare name ``go`` for a shell
    function, which the relative-path normalization turns into a
    nonexistent <cwd>/go while suppressing the absolute-path fallback —
    so a real installed Go goes unfound. Exported functions do propagate
    into this script, and the env file it sources is arbitrary shell, so
    the lookup must force a PATH search."""
    adapter, run, go_invocations, _ = harness
    env_file = adapter.parent / "envfile-with-function"
    env_file.write_text("go() { echo 'shadowed go was called' >&2; return 127; }\n")
    proc = run(env_file=env_file)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "shadowed go was called" not in proc.stderr
    assert "no Go toolchain found" not in proc.stderr
    # The stub on PATH built it, not the function and not a bogus ./go.
    assert len(go_invocations()) == 1, go_invocations()
    assert "STUB_ADAPTER_RAN" in proc.stdout


def test_toolchain_older_than_go_mod_is_rejected_when_pinned(harness):
    """GOTOOLCHAIN=local means Go cannot upgrade itself, so a toolchain
    below go.mod's directive can never build: say so with a remedy
    instead of leaking a raw compiler error."""
    adapter, run, go_invocations, _ = harness
    proc = run(extra_env={"GO_STUB_VERSION": "1.0.0", "GOTOOLCHAIN": "local"})
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert f"need Go >= {required_go_version()}, found 1.0.0" in proc.stderr
    assert "GOTOOLCHAIN=local" in proc.stderr
    # Refused before building, and nothing was published.
    assert go_invocations() == []
    assert not (adapter / "gc-slack-adapter").exists()
    assert not list(adapter.glob("gc-slack-adapter.build.*"))


def test_toolchain_older_than_go_mod_still_builds_when_downloadable(harness):
    """Under the default GOTOOLCHAIN=auto, Go fetches the required
    toolchain itself. Warn, but do not refuse — hosts that build fine
    today must keep building."""
    adapter, run, go_invocations, _ = harness
    proc = run(extra_env={"GO_STUB_VERSION": "1.0.0"}, drop_env=["GOTOOLCHAIN"])
    assert proc.returncode == 0, proc.stderr
    assert f"go.mod needs >= {required_go_version()}" in proc.stderr
    assert "STUB_ADAPTER_RAN" in proc.stdout
    assert len(go_invocations()) == 1, go_invocations()


def test_go_build_failure_exits_1_and_publishes_nothing(harness):
    """The other operational failure path: a real compile error must
    exit non-zero with the manual-fix hint, leave no half-built binary
    at the canonical path, and clean up its temp."""
    adapter, run, go_invocations, _ = harness
    proc = run(extra_env={"GO_STUB_BUILD_FAILS": "1"})
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert "go build failed" in proc.stderr
    assert "manual fix: cd " in proc.stderr
    assert "STUB_ADAPTER_RAN" not in proc.stdout
    assert not (adapter / "gc-slack-adapter").exists()
    assert not list(adapter.glob("gc-slack-adapter.build.*"))


def test_stale_build_temp_is_ignored(harness):
    """A crash mid-build (SIGKILL, host loss) can leave a PID-suffixed
    temp behind; it must never be exec'd, and a fresh start must still
    build and run the real binary."""
    adapter, run, go_invocations, _ = harness
    stale = adapter / "gc-slack-adapter.build.99999"
    stale.write_text("#!/usr/bin/env bash\necho STALE_TEMP_RAN\n")
    stale.chmod(0o755)
    proc = run()
    assert proc.returncode == 0, proc.stderr
    assert "STALE_TEMP_RAN" not in proc.stdout
    assert "STUB_ADAPTER_RAN" in proc.stdout
    assert len(go_invocations()) == 1


def test_pack_service_command_is_the_checked_in_run_sh():
    """The [[service]] command must stay a checked-in path — pointing it
    back at the gitignored binary reintroduces the stranded-service
    outage on the next pin bump."""
    try:
        import tomllib
    except ModuleNotFoundError:  # pragma: no cover - py<3.11
        pytest.skip("tomllib unavailable")
    with open(PACK_DIR / "pack.toml", "rb") as fh:
        pack = tomllib.load(fh)
    services = {s["name"]: s for s in pack.get("service", [])}
    assert "slack" in services, "slack [[service]] block missing from pack.toml"
    command = services["slack"]["process"]["command"]
    assert command == ["./adapter/run.sh"], command
    rel = pathlib.Path(command[0])
    target = PACK_DIR / rel
    assert target.exists() and os.access(target, os.X_OK)
    # Not gitignored (check-ignore: 1 = not ignored; 0 = ignored;
    # anything else = git itself failed and proves nothing):
    ignored = subprocess.run(
        ["git", "check-ignore", str(rel)],
        cwd=PACK_DIR,
        capture_output=True,
        text=True,
    )
    assert ignored.returncode == 1, (
        f"{rel}: git check-ignore exited {ignored.returncode} "
        f"(0 means gitignored — service would strand): {ignored.stderr}"
    )
    # And actually tracked, so a materialization really ships it:
    tracked = subprocess.run(
        ["git", "ls-files", "--error-unmatch", str(rel)],
        cwd=PACK_DIR,
        capture_output=True,
        text=True,
    )
    assert tracked.returncode == 0, f"{rel} is not tracked by git: {tracked.stderr}"
