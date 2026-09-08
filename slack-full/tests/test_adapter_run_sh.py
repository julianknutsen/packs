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
    enough for go.mod under ``GOTOOLCHAIN=local``, no Go new enough
    while running *supervised* and the newer toolchain not cached (the
    fetch it would otherwise start cannot fit the readiness window) —
    while the same host with that toolchain already cached still
    builds — and ``go build`` failing,
  * the self-heal survives the minimal supervisor environment it
    targets (no HOME) by supplying a build cache, and that cache is
    private to this user rather than a shared /tmp name,
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
# reports, defaulting high enough to satisfy any go.mod directive),
# GO_STUB_BUILD_FAILS (make `build` exit non-zero) and
# GO_STUB_TOOLCHAIN_CACHED (see the toolchain-switch model below).
echo "go $* cwd=$PWD" >> "$GO_STUB_LOG"
if [ "$1" = "version" ]; then
  # Model the toolchain switch. Run inside a module whose go.mod asks
  # for a newer Go than itself, the real go command resolves that
  # toolchain before doing anything else — from the local module cache
  # if it is already there, over the network otherwise. GOPROXY=off
  # forbids the network, so the call fails outright when the toolchain
  # is not cached. run.sh uses exactly that combination to tell "this
  # build would download a toolchain" from "the toolchain is already
  # local", and only refuses to start in the first case, so the stub has
  # to distinguish them. GOPROXY=off therefore means "this is the probe"
  # (the harness clears any inherited GOPROXY so it cannot mean anything
  # else), and the default answer is the production case F1 is about:
  # not cached, a download would be required.
  if [ "${GOPROXY:-}" = "off" ] && [ -z "${GO_STUB_TOOLCHAIN_CACHED:-}" ]; then
    echo "go: downloading go1.99.0 (unavailable: module lookup disabled by GOPROXY=off)" >&2
    exit 1
  fi
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


# Stub toolchain versions for the two version-gate tests. Both are older
# than go.mod's directive, but they get there differently and only the
# pair covers the comparator: "1.0.0" is lower numerically AND lexically,
# so a plain string compare passes it; "1.9.0" is lower ONLY numerically
# ("1.9" sorts above "1.25" as text). run.sh's version_key exists for
# exactly that second case, so a test suite driving only "1.0.0" stays
# green if the comparator regresses to a string compare.
OLD_GO_VERSIONS = ["1.0.0", "1.9.0"]


def private_tmp(tmpdir: pathlib.Path, name: str) -> str:
    """The per-user cache path run.sh derives under TMPDIR. The uid
    suffix is the point: a fixed shared name in world-writable /tmp can
    be pre-created by another local user, and Go trusts what it finds in
    these caches.

    Tests that assert on these paths hand run.sh their own TMPDIR — the
    dirs are deliberately stable across runs, so pointing them at the
    real /tmp would leave state behind and make the assertions depend on
    what an earlier run left there."""
    return f"{str(tmpdir).rstrip('/')}/{name}.{os.geteuid()}"


# Where the stub records its invocations. Named once so a test can read
# the log the harness wrote without re-deriving the path.
GO_LOG_NAME = "go-invocations.log"


def go_version_calls(tmp_path: pathlib.Path) -> list[str]:
    """The stub's `go version` invocations, each with the cwd it ran in."""
    log = tmp_path / GO_LOG_NAME
    if not log.exists():
        return []
    return [l for l in log.read_text().splitlines() if l.startswith("go version")]


def assert_older_than_go_mod(stub_version: str) -> None:
    """Pin the parametrization's premise so a future `go` directive bump
    cannot quietly turn these into no-op tests."""
    required = required_go_version()

    def key(v: str) -> tuple:
        return tuple(int(p) for p in (v.split(".") + ["0", "0"])[:3])

    assert key(stub_version) < key(required), (
        f"test premise broken: stub Go {stub_version} is not older than "
        f"go.mod's {required}, so the version gate would not fire"
    )


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

    go_log = tmp_path / GO_LOG_NAME

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
        # run.sh reads these to decide it is running supervised, which
        # changes the too-old-toolchain path from warn-and-build to
        # refuse. The suite itself can run inside a gc service or agent
        # that exports them, so the baseline must be *unsupervised*
        # explicitly — inheriting them would flip an unrelated test's
        # expected outcome depending on who ran pytest.
        env.pop("GC_SERVICE_NAME", None)
        env.pop("GC_SERVICE_URL_PREFIX", None)
        # run.sh sets GOPROXY=off itself, for the single `go version`
        # call that probes whether the newer toolchain is already
        # cached. The stub reads GOPROXY=off as "this is that probe"; an
        # inherited one (a developer building offline, say) would make
        # every other `go version` look like the probe too.
        env.pop("GOPROXY", None)
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


def test_self_heal_builds_when_home_is_unset(harness, tmp_path):
    """Supervisor environments may not set HOME. Under `set -u` an
    unguarded $HOME aborts before the fast path — and `go build` then
    fails a second way, because it can only locate a build cache via
    GOCACHE, XDG_CACHE_HOME, or HOME. Assert the *build* survives, not
    merely the shell."""
    adapter, run, go_invocations, build_environments = harness
    cache_tmp = tmp_path / "tmpdir"
    cache_tmp.mkdir()
    proc = run(
        extra_env={"TMPDIR": str(cache_tmp)},
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
    assert build_environments() == [
        f"buildenv GOCACHE={private_tmp(cache_tmp, 'gc-slack-adapter-gocache')} "
        f"GOPATH={private_tmp(cache_tmp, 'gc-slack-adapter-gopath')}"
    ], build_environments()
    assert (adapter / "gc-slack-adapter").exists()
    # Private to this user, not whatever mode the ambient umask gives.
    for name in ("gc-slack-adapter-gocache", "gc-slack-adapter-gopath"):
        created = pathlib.Path(private_tmp(cache_tmp, name))
        assert created.is_dir(), created
        assert created.stat().st_mode & 0o077 == 0, oct(created.stat().st_mode)


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
    cache_tmp = tmp_path / "tmpdir"
    cache_tmp.mkdir()
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
    proc = run(
        extra_env={cache_var: str(cache_dir), "TMPDIR": str(cache_tmp)},
        drop_env=drop,
    )
    assert proc.returncode == 0, proc.stderr
    assert "STUB_ADAPTER_RAN" in proc.stdout
    assert len(go_invocations()) == 1, go_invocations()
    expected_gocache = str(cache_dir) if cache_var == "GOCACHE" else "unset"
    assert build_environments() == [
        f"buildenv GOCACHE={expected_gocache} "
        f"GOPATH={private_tmp(cache_tmp, 'gc-slack-adapter-gopath')}"
    ], build_environments()


def test_tmp_cache_is_reused_across_starts(harness, tmp_path):
    """The per-user path must stay STABLE, not be re-derived per start.
    A cold build does not fit the supervisor's readiness window, and it
    only converges across the resulting kill/restart cycles because
    `go build` finds its earlier per-package results still cached — a
    fresh dir each attempt would loop forever on exactly the HOME-less
    hosts this fallback serves."""
    adapter, run, _, build_environments = harness
    cache_tmp = tmp_path / "tmpdir"
    cache_tmp.mkdir()
    drop = [
        "HOME",
        "XDG_CONFIG_HOME",
        "XDG_CACHE_HOME",
        "GOCACHE",
        "GOPATH",
        "GOMODCACHE",
        "GC_SLACK_ADAPTER_ENV",
    ]
    first = run(extra_env={"TMPDIR": str(cache_tmp)}, drop_env=drop)
    assert first.returncode == 0, first.stderr
    # Second start would exec the binary the first one published, so
    # clear it to force another build.
    (adapter / "gc-slack-adapter").unlink()
    second = run(extra_env={"TMPDIR": str(cache_tmp)}, drop_env=drop)
    assert second.returncode == 0, second.stderr
    envs = build_environments()
    assert len(envs) == 2, envs
    assert envs[0] == envs[1], envs


def test_squatted_tmp_cache_is_not_adopted(harness, tmp_path):
    """/tmp is world-writable, so another local user can pre-create the
    predictable cache path — and Go trusts these caches: a seeded GOCACHE
    can serve object files into a binary holding the Slack bot token, and
    GOPATH is where a fetched toolchain is unpacked and re-exec'd. A path
    we did not create must not be adopted.

    A symlink stands in for the foreign-owned directory, which a test
    cannot create without a second uid; run.sh rejects both through the
    same `-O`/`! -L` check."""
    adapter, run, go_invocations, build_environments = harness
    cache_tmp = tmp_path / "tmpdir"
    cache_tmp.mkdir()
    attacker = tmp_path / "attacker-cache"
    attacker.mkdir()
    squatted = pathlib.Path(private_tmp(cache_tmp, "gc-slack-adapter-gocache"))
    squatted.symlink_to(attacker)

    proc = run(
        extra_env={"TMPDIR": str(cache_tmp)},
        drop_env=[
            "HOME",
            "XDG_CONFIG_HOME",
            "XDG_CACHE_HOME",
            "GOCACHE",
            "GOPATH",
            "GOMODCACHE",
            "GC_SLACK_ADAPTER_ENV",
        ],
    )
    # Availability is preserved — it still builds and starts...
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "STUB_ADAPTER_RAN" in proc.stdout
    assert len(go_invocations()) == 1, go_invocations()
    # ...but not through the squatted path, and it says so.
    assert "refusing to reuse it" in proc.stderr
    envs = build_environments()
    assert len(envs) == 1, envs
    assert f"GOCACHE={squatted}" not in envs[0], envs[0]
    assert str(attacker) not in envs[0], envs[0]
    # The fallback is still under TMPDIR, just not the predictable name.
    assert f"GOCACHE={cache_tmp}/" in envs[0], envs[0]
    # And the symlink was left alone rather than followed into.
    assert squatted.is_symlink()
    assert list(attacker.iterdir()) == []


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


@pytest.mark.parametrize("stub_version", OLD_GO_VERSIONS)
def test_toolchain_older_than_go_mod_is_rejected_when_pinned(harness, stub_version):
    """GOTOOLCHAIN=local means Go cannot upgrade itself, so a toolchain
    below go.mod's directive can never build: say so with a remedy
    instead of leaking a raw compiler error.

    Driven with both an all-round-older version and one that is older
    only numerically, so the gate cannot pass on a string compare."""
    assert_older_than_go_mod(stub_version)
    adapter, run, go_invocations, _ = harness
    proc = run(extra_env={"GO_STUB_VERSION": stub_version, "GOTOOLCHAIN": "local"})
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert f"need Go >= {required_go_version()}, found {stub_version}" in proc.stderr
    assert "GOTOOLCHAIN=local" in proc.stderr
    # Refused before building, and nothing was published.
    assert go_invocations() == []
    assert not (adapter / "gc-slack-adapter").exists()
    assert not list(adapter.glob("gc-slack-adapter.build.*"))


@pytest.mark.parametrize("stub_version", OLD_GO_VERSIONS)
def test_toolchain_older_than_go_mod_still_builds_when_downloadable(
    harness, stub_version
):
    """Run by hand under the default GOTOOLCHAIN=auto, Go fetches the
    required toolchain itself. Warn, but do not refuse — there is no
    readiness deadline here and hosts that build fine today must keep
    building.

    Same two-version parametrization: "1.9.0" only trips the gate if the
    comparison is numeric."""
    assert_older_than_go_mod(stub_version)
    adapter, run, go_invocations, _ = harness
    proc = run(extra_env={"GO_STUB_VERSION": stub_version}, drop_env=["GOTOOLCHAIN"])
    assert proc.returncode == 0, proc.stderr
    assert f"is {stub_version} but go.mod needs >= {required_go_version()}" in proc.stderr
    assert "STUB_ADAPTER_RAN" in proc.stdout
    assert len(go_invocations()) == 1, go_invocations()


@pytest.mark.parametrize("signal", ["GC_SERVICE_NAME", "GC_SERVICE_URL_PREFIX"])
def test_toolchain_fetch_is_refused_under_the_supervisor(harness, signal):
    """A too-old toolchain under GOTOOLCHAIN=auto means the build starts
    by DOWNLOADING a toolchain. gc gives a starting proxy_process ~5s to
    become ready, then kills the process group and restarts on a 1s
    backoff with no cap — and unlike a compile, a toolchain fetch keeps
    no partial progress, so every cycle would re-download it from
    scratch, forever. Refuse instead: each cycle then exits immediately
    with a named remedy on stderr rather than burning the network.

    Either supervisor-exported variable is enough to detect this; gc
    exports both into every proxy_process."""
    adapter, run, go_invocations, _ = harness
    proc = run(
        extra_env={"GO_STUB_VERSION": "1.9.0", signal: "slack"},
        drop_env=["GOTOOLCHAIN"],
    )
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert f"need Go >= {required_go_version()}, found 1.9.0" in proc.stderr
    assert "it is not in the local module cache" in proc.stderr
    assert "refusing to start that download under the gc supervisor" in proc.stderr
    # A named remedy, not a raw failure.
    assert "manual fix: install Go >=" in proc.stderr
    # Refused before building, and nothing was published.
    assert go_invocations() == []
    assert "STUB_ADAPTER_RAN" not in proc.stdout
    assert not (adapter / "gc-slack-adapter").exists()
    assert not list(adapter.glob("gc-slack-adapter.build.*"))


def test_supervised_build_proceeds_when_the_toolchain_is_already_cached(
    harness, tmp_path
):
    """The refusal above is about a DOWNLOAD, not about the version
    numbers. Once the newer toolchain sits in the local module cache
    (someone prebuilt by hand, or an earlier unsupervised run fetched
    it), the supervised build needs no network and converges exactly like
    any other warm rebuild — refusing it on the version comparison alone
    would turn a self-healing host into a permanently dead service, which
    is the outage this script exists to prevent.

    run.sh tells the two apart by asking Go itself, offline: a
    `GOPROXY=off go version` from inside the module directory, where the
    toolchain switch actually happens. Nothing in the version numbers
    distinguishes this case from the one above — only that probe does."""
    adapter, run, go_invocations, _ = harness
    proc = run(
        extra_env={
            "GO_STUB_VERSION": "1.9.0",
            "GO_STUB_TOOLCHAIN_CACHED": "1",
            "GC_SERVICE_NAME": "slack",
        },
        drop_env=["GOTOOLCHAIN"],
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "already in the local module cache" in proc.stderr
    assert "refusing to start that download" not in proc.stderr
    # Built and exec'd, same as any warm supervised rebuild.
    assert "STUB_ADAPTER_RAN" in proc.stdout
    assert len(go_invocations()) == 1, go_invocations()
    # The probe has to run in the module directory: that is the only cwd
    # where the go command performs the switch this asks about. Probing
    # from anywhere else answers a different question — "does the local
    # go run at all" — which is always yes, so the refusal would never
    # fire again.
    calls = go_version_calls(tmp_path)
    probes = [
        c
        for c in calls
        if " cwd=" in c
        and pathlib.Path(c.split(" cwd=", 1)[1]).resolve() == adapter.resolve()
    ]
    assert probes, calls


def test_supervised_build_is_untouched_when_the_toolchain_is_new_enough(harness):
    """The supervised refusal is scoped to the toolchain-fetch case. A
    host whose Go already satisfies go.mod must still self-heal under the
    supervisor — that is the outage this whole script exists to fix."""
    adapter, run, go_invocations, _ = harness
    proc = run(extra_env={"GC_SERVICE_NAME": "slack"})
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "STUB_ADAPTER_RAN" in proc.stdout
    assert "refusing to start that download" not in proc.stderr
    assert len(go_invocations()) == 1, go_invocations()


def test_missing_go_mod_does_not_kill_the_script_silently(harness):
    """go.mod is tracked, so only a corrupted checkout hits this — but
    the failure mode was the script's one silent exit: under `set -e` +
    `pipefail` the sed reading go.mod exited 2 with its stderr
    suppressed, killing run.sh at the assignment with no message. An
    unreadable directive must just skip the version gate and let the
    compiler be the backstop."""
    adapter, run, go_invocations, _ = harness
    (adapter / "go.mod").unlink()
    proc = run()
    assert proc.returncode == 0, proc.stdout + proc.stderr
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
