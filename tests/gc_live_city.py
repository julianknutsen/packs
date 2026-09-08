"""Stand a pack up in a scratch city and ask a real `gc` binary about it.

A pack's own unit tests read its TOML and confirm the file parses. `gc lint`
reads one pack in isolation. Neither can say what Gas City does when it loads
that file into a city, which is the only place a user meets a pack -- and the
gap is not theoretical: every pack we maintain linted clean while `pr-pipeline`
put a deprecation warning into `gc doctor` for every city that imported it.

This module is the shared harness for tests that close that gap. It offers three
things a caller composes:

`write_city` builds a throwaway city that imports whatever packs the caller
names, at city scope, rig scope, or both.

`attributable` runs `gc doctor` twice -- once against that city, once against a
baseline city with the same shape and no packs under test -- and returns the
difference. A doctor run on a developer's machine reports a couple of dozen
findings that have nothing to do with any pack (no tmux, no bead store, no git
in the scratch rig). Hand-listing them would be a waiver set that rots.
Subtracting a baseline cancels them without naming any of them.

`write_canary_pack` is the control every caller of `attributable` owes. An empty
delta is also what a doctor that stopped reporting produces, and what a city
that failed to load before doctor reached the packs produces. The canary is a
pack whose formula declares a deprecated contract on purpose, so a test can
require the subtraction to surface a known finding through this fixture shape,
on this binary, in this environment.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import subprocess
import textwrap
import tomllib

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]

CANARY_BINDING = "requirements-canary"
BASELINE_BINDING = "delta-baseline"
CANARY_CHECK = "formula-requirements"

# `  ⚠ rig-pack-coverage — 1 rig-scoped named_session(s) not covered ...`
DOCTOR_FINDING = re.compile(r"^\s*[⚠✗]\s+(\S+)\s+—")

# Findings the delta cannot cancel, with the reason. Every other environment
# finding cancels because both cities run on the same host in the same second;
# these do not, so they are named rather than tolerated silently.
UNSTABLE_CHECKS = frozenset(
    {
        # Host-wide processes-per-second, sampled independently in each run and
        # compared against a fixed threshold. It moves with whatever else the
        # machine is doing, including the other doctor run in this same test.
        "fork-rate",
    }
)


@dataclass(frozen=True)
class Workspace:
    city_dir: Path
    rig_dir: Path
    env: dict[str, str]


@pytest.fixture(scope="session")
def gc_test_bin() -> Path:
    configured = os.environ.get("GC_TEST_BIN")
    if not configured:
        pytest.skip("set GC_TEST_BIN to run real Gas City CLI integration tests")

    binary = Path(configured).expanduser().resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        pytest.fail(f"GC_TEST_BIN is not an executable file: {binary}")
    return binary


def read_pack_manifest(pack_dir: Path) -> dict:
    return tomllib.loads(pack_dir.joinpath("pack.toml").read_text(encoding="utf-8"))


def declares_a_service(pack_dir: Path) -> bool:
    """Whether the pack ships a `[[service]]`, which pins it to city scope.

    Gas City rejects a rig-scoped import carrying one outright ("[[service]] is
    only allowed in city-scoped packs"), so a caller cannot choose the wiring
    freely -- the pack decides it. Read from the pack rather than listed per
    pack here, so adding a service to a pack changes how it is tested.
    """
    return bool(read_pack_manifest(pack_dir).get("service"))


def declares_a_rig_scoped_session(pack_dir: Path) -> bool:
    """Whether the pack ships a rig-scoped `[[named_session]]`.

    A pack that does has to be bound per rig as well as at city scope, or
    `gc doctor` reports `rig-pack-coverage` -- the session it declares has no
    rig to run in. Also derived rather than listed, for the same reason.
    """
    return any(
        session.get("scope") == "rig"
        for session in read_pack_manifest(pack_dir).get("named_session", ())
    )


def discover_agents(pack_dir: Path) -> set[str]:
    """The agent roles a pack ships, from `agents/<name>/agent.toml`."""
    agents_dir = pack_dir / "agents"
    if not agents_dir.is_dir():
        return set()
    return {path.parent.name for path in agents_dir.glob("*/agent.toml")}


def discover_command_words(pack_dir: Path) -> set[tuple[str, ...]]:
    """The command paths a pack ships, as the word tuples `gc` will register.

    Pack commands are discovered by convention -- a directory under `commands/`
    holding a `run.sh` or a `command.toml` is a leaf, and nested directories
    become nested command words. Nothing declares them, which is what makes the
    surface easy to lose silently: a moved directory or a renamed script drops a
    verb with no error anywhere.

    Derived rather than listed so a pack that grows a command gets it tested.
    Callers must reject an empty result; a derivation that silently finds
    nothing would make every assertion built on it vacuous.
    """
    commands_dir = pack_dir / "commands"
    if not commands_dir.is_dir():
        return set()

    words = {
        leaf.parent.relative_to(commands_dir).parts
        for name in ("run.sh", "command.toml")
        for leaf in commands_dir.rglob(name)
    }
    # A runner sitting directly in `commands/` names no verb. Nothing in this
    # repo does that, and the empty tuple would silently become a `words[-1]`
    # crash in a caller rather than a readable failure here.
    assert () not in words, (
        f"{pack_dir.name} has a command runner directly in commands/ rather "
        "than in a leaf directory, so it names no verb"
    )
    return words


def discover_formulas(pack_dir: Path) -> set[str]:
    """The formula names a pack ships, from its `formulas/` directory."""
    formulas_dir = pack_dir / "formulas"
    if not formulas_dir.is_dir():
        return set()
    return {path.name.split(".", 1)[0] for path in formulas_dir.glob("*.formula.toml")}


def write_canary_pack(root: Path) -> Path:
    """A pack whose one formula declares the deprecated contract on purpose."""
    pack_dir = root / "canary-pack"
    (pack_dir / "formulas").mkdir(parents=True)
    pack_dir.joinpath("pack.toml").write_text(
        textwrap.dedent(
            f"""\
            [pack]
            name = "{CANARY_BINDING}"
            schema = 2
            """
        ),
        encoding="utf-8",
    )
    pack_dir.joinpath("formulas", "mol-requirements-canary.formula.toml").write_text(
        textwrap.dedent(
            """\
            description = "Fixture. Never dispatched."
            formula = "mol-requirements-canary"
            version = 1
            contract = "graph.v2"
            pour = true

            [[steps]]
            id = "noop"
            title = "Never dispatched"
            prompt = "Fixture step."
            """
        ),
        encoding="utf-8",
    )
    return pack_dir


def write_baseline_pack(root: Path) -> Path:
    """An empty pack with the SHAPE of a real one: one agent, one order.

    The baseline has to be comparable, not merely empty. Several doctor checks
    only run once a city has something to check -- `order-firing-current` needs
    a scheduled order, `v2-routed-to-namespace` needs a binding-qualified route
    target -- so an empty baseline skips them, and every one of them shows up in
    the delta as though the pack under test had caused it. It had not: an order
    that has never fired in a city that has never started is stale in any city,
    and a route check that cannot reach a bead store is skipped in any city.
    Give the baseline the same shape and both cancel, without either one being
    named in a waiver list that would then also hide a real regression.
    """
    pack_dir = root / "baseline-pack"
    (pack_dir / "orders").mkdir(parents=True)
    agent_dir = pack_dir / "agents" / "inert"
    agent_dir.mkdir(parents=True)
    pack_dir.joinpath("pack.toml").write_text(
        textwrap.dedent(
            f"""\
            [pack]
            name = "{BASELINE_BINDING}"
            schema = 2
            """
        ),
        encoding="utf-8",
    )
    pack_dir.joinpath("orders", "inert.toml").write_text(
        textwrap.dedent(
            """\
            [order]
            description = "Fixture. Never fires; exists so order checks run."
            trigger = "cooldown"
            interval = "15m"
            exec = "true"
            timeout = "30s"
            """
        ),
        encoding="utf-8",
    )
    agent_dir.joinpath("agent.toml").write_text(
        textwrap.dedent(
            """\
            scope = "rig"
            work_dir = "."
            """
        ),
        encoding="utf-8",
    )
    agent_dir.joinpath("prompt.template.md").write_text(
        "Fixture agent. Never dispatched.\n", encoding="utf-8"
    )
    return pack_dir


def write_city(
    root: Path,
    imports: dict[str, Path],
    rig_imports: dict[str, Path] | None = None,
) -> Workspace:
    """A scratch city importing `imports` at city scope, `rig_imports` at rig."""
    city_dir = root / "city"
    rig_dir = root / "demo"
    gc_home = root / "gc-home"
    home = root / "home"
    (city_dir / ".gc").mkdir(parents=True)
    (rig_dir / ".gc").mkdir(parents=True)
    gc_home.mkdir()
    home.mkdir()

    bindings = "".join(
        f"\n[imports.{name}]\nsource = {json.dumps(str(path.resolve()))}\n"
        for name, path in sorted(imports.items())
    )
    city_dir.joinpath("pack.toml").write_text(
        textwrap.dedent(
            """\
            [pack]
            name = "pack-under-test"
            schema = 2
            """
        )
        + bindings,
        encoding="utf-8",
    )

    rig_bindings = "".join(
        f"\n[rigs.imports.{name}]\nsource = {json.dumps(str(path.resolve()))}\n"
        for name, path in sorted((rig_imports or {}).items())
    )
    city_dir.joinpath("city.toml").write_text(
        textwrap.dedent(
            """\
            [workspace]
            provider = "claude"

            [providers.claude]
            base = "builtin:claude"

            [[rigs]]
            name = "demo"
            """
        )
        + rig_bindings,
        encoding="utf-8",
    )
    city_dir.joinpath(".gc", "site.toml").write_text(
        textwrap.dedent(
            f"""\
            workspace_name = "pack-under-test"

            [[rig]]
            name = "demo"
            path = {json.dumps(str(rig_dir))}
            """
        ),
        encoding="utf-8",
    )

    brief = REPO_ROOT / "oversight-rig" / "agents" / "project-lead"
    brief = brief / "project-brief.template.md"
    if brief.is_file():
        rig_dir.joinpath(".gc", "project-brief.md").write_text(
            brief.read_text(encoding="utf-8"), encoding="utf-8"
        )

    # Strip the caller's Gas City and beads environment rather than inheriting
    # it. `BEADS_DOLT_SERVER_PORT` in particular overrides what a city's own
    # metadata names, so a developer running this suite inside a live city
    # would have these scratch invocations reach that city's canonical store.
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("GC_", "BEADS_", "XDG_"))
    }
    env.update(
        {
            "HOME": str(home),
            # Pinning HOME is not enough. Pack code that resolves config the
            # portable way reads `${XDG_CONFIG_HOME:-$HOME/.config}`, and a set
            # XDG_CONFIG_HOME beats the pinned HOME, so the scratch city reads
            # the developer's real dotfiles. Caught by CI, not locally:
            # `slack-full`'s doctor/check-env.sh found this operator's
            # ~/.config/gc-slack-adapter/env and passed here, while a clean
            # runner with no such file reported `slack-full:env`. Redirect the
            # whole XDG set, since data/state/cache override HOME identically.
            "XDG_CONFIG_HOME": str(home / ".config"),
            "XDG_DATA_HOME": str(home / ".local" / "share"),
            "XDG_STATE_HOME": str(home / ".local" / "state"),
            "XDG_CACHE_HOME": str(home / ".cache"),
            "GC_HOME": str(gc_home),
            "GC_CITY": str(city_dir),
            "GC_CITY_PATH": str(city_dir),
            "GC_CITY_ROOT": str(city_dir),
            "GC_RIG": "demo",
        }
    )
    return Workspace(city_dir=city_dir, rig_dir=rig_dir, env=env)


def run_gc(
    gc_test_bin: Path, workspace: Workspace, *args: str
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(gc_test_bin), *args],
        cwd=workspace.rig_dir,
        env=workspace.env,
        text=True,
        capture_output=True,
        timeout=300,
    )


def gc_output(gc_test_bin: Path, workspace: Workspace, *args: str) -> str:
    """Run gc, refusing to return output from a command that failed.

    Returning stdout+stderr alone makes "the command ran and listed nothing"
    indistinguishable from "the command never got far enough to list", and
    every presence assertion built on this is a search through the string.
    """
    result = run_gc(gc_test_bin, workspace, *args)
    output = result.stdout + result.stderr
    assert result.returncode == 0, (
        f"gc {' '.join(args)} exited {result.returncode}; an absent name in "
        f"this output is a failed command, not a missing surface. Output:\n{output}"
    )
    return output


def doctor_findings(gc_test_bin: Path, workspace: Workspace) -> set[str]:
    """Check names `gc doctor` reported as warning or failure.

    Deliberately not asserting on the exit status: doctor exits non-zero
    whenever anything is off, and in a scratch city with no store something
    always is. The delta is the signal, not the code.
    """
    result = run_gc(gc_test_bin, workspace, "doctor")
    output = result.stdout + result.stderr
    findings = {
        match.group(1)
        for match in (DOCTOR_FINDING.match(line) for line in output.splitlines())
        if match is not None
    }
    assert findings, (
        "gc doctor named no check at all, so this run cannot contribute to a "
        f"delta. Output:\n{output}"
    )
    return findings - UNSTABLE_CHECKS


def attributable(
    gc_test_bin: Path,
    tmp_path: Path,
    imports: dict[str, Path],
    rig_imports: dict[str, Path] | None = None,
) -> set[str]:
    """Doctor findings these imports add to a city that would not have them."""
    shape = {BASELINE_BINDING: write_baseline_pack(tmp_path / "shape")}
    baseline = doctor_findings(
        gc_test_bin, write_city(tmp_path / "baseline", shape, shape)
    )
    loaded = doctor_findings(
        gc_test_bin, write_city(tmp_path / "loaded", imports, rig_imports)
    )
    return loaded - baseline
