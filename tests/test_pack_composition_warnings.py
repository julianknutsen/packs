"""Every agent-bearing pack must compose into a city without config advisories.

A pack's own unit tests read its TOML and can tell you the file parses. They
cannot tell you what Gas City says when it loads that file into a city, which
is where a user meets the pack. These tests stand each pack up in an isolated
scratch city and run the real binary.

Each case also imports a fixture pack whose one agent sets both `idle_timeout`
and `sleep_after_idle` on purpose. That canary must be reported in the same
invocation. Without it a green result is ambiguous: a pack with no advisories
and a binary that stopped emitting advisories look identical.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import subprocess
import textwrap

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]

CANARY_BINDING = "idle-canary"
CANARY_AGENT = "canary"
IDLE_ADVISORY = "idle_timeout and sleep_after_idle are both set"

# `agent "gastown.mayor"` at city scope, `agent "fixture/gastown.refinery"`
# for a rig-scoped one. Both forms name the same pack binding.
ADVISORY_AGENT = re.compile(r'agent "([^"]+)"')


def advisory_binding(line: str) -> str | None:
    """The pack binding an advisory line is about, or None if it names no agent."""
    match = ADVISORY_AGENT.search(line)
    if match is None:
        return None
    return match.group(1).rsplit("/", 1)[-1].split(".", 1)[0]


def agent_bearing_packs() -> list[str]:
    """Pack directories that ship at least one agent definition."""
    names = {
        path.parents[2].name
        for path in REPO_ROOT.glob("*/agents/*/agent.toml")
    }
    return sorted(names)


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


def write_canary_pack(root: Path) -> Path:
    """A pack whose single agent sets both idle keys, so the check can go red."""
    pack_dir = root / "canary-pack"
    agent_dir = pack_dir / "agents" / CANARY_AGENT
    agent_dir.mkdir(parents=True)
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
    agent_dir.joinpath("agent.toml").write_text(
        textwrap.dedent(
            """\
            scope = "city"
            work_dir = "."
            idle_timeout = "1h"
            sleep_after_idle = "60s"
            """
        ),
        encoding="utf-8",
    )
    agent_dir.joinpath("prompt.template.md").write_text(
        "Fixture agent. Never dispatched.\n", encoding="utf-8"
    )
    return pack_dir


def write_city(root: Path, pack_name: str) -> Workspace:
    city_dir = root / "city"
    rig_dir = root / "fixture"
    gc_home = root / "gc-home"
    home = root / "home"
    (city_dir / ".gc").mkdir(parents=True)
    rig_dir.mkdir()
    gc_home.mkdir()
    home.mkdir()

    canary_pack = write_canary_pack(root)
    pack_source = (REPO_ROOT / pack_name).resolve()

    city_dir.joinpath("pack.toml").write_text(
        textwrap.dedent(
            f"""\
            [pack]
            name = "composition-warnings"
            schema = 2

            [imports.{pack_name}]
            source = {json.dumps(str(pack_source))}

            [imports.{CANARY_BINDING}]
            source = {json.dumps(str(canary_pack))}
            """
        ),
        encoding="utf-8",
    )
    city_dir.joinpath("city.toml").write_text(
        textwrap.dedent(
            """\
            [workspace]
            provider = "codex"

            [providers.codex]
            base = "builtin:codex"

            [[rigs]]
            name = "fixture"
            """
        ),
        encoding="utf-8",
    )
    city_dir.joinpath(".gc", "site.toml").write_text(
        textwrap.dedent(
            f"""\
            workspace_name = "composition-warnings"

            [[rig]]
            name = "fixture"
            path = {json.dumps(str(rig_dir))}
            """
        ),
        encoding="utf-8",
    )

    env = {
        **os.environ,
        "HOME": str(home),
        "GC_HOME": str(gc_home),
        "GC_CITY": str(city_dir),
        "GC_CITY_PATH": str(city_dir),
        "GC_CITY_ROOT": str(city_dir),
        "GC_RIG": "fixture",
    }
    return Workspace(city_dir=city_dir, rig_dir=rig_dir, env=env)


def config_show(gc_test_bin: Path, workspace: Workspace) -> str:
    result = subprocess.run(
        [str(gc_test_bin), "config", "show"],
        cwd=workspace.rig_dir,
        env=workspace.env,
        text=True,
        capture_output=True,
        timeout=120,
    )
    return result.stdout + result.stderr


@pytest.mark.parametrize("pack_name", agent_bearing_packs())
def test_pack_agents_compose_without_idle_advisory(
    tmp_path: Path, gc_test_bin: Path, pack_name: str
) -> None:
    output = config_show(gc_test_bin, write_city(tmp_path, pack_name))

    advisories = [line for line in output.splitlines() if IDLE_ADVISORY in line]

    bindings = [(advisory_binding(line), line) for line in advisories]

    # The canary proves the advisory is reachable in this run, on this binary,
    # through this fixture shape, and that the binding parse above finds it.
    # Assert it first: without it, an empty `offenders` list is not evidence.
    assert any(binding == CANARY_BINDING for binding, _ in bindings), (
        f"fixture canary {CANARY_BINDING}.{CANARY_AGENT} did not surface as an "
        f"idle advisory; this check cannot go red. Output:\n{output}"
    )

    offenders = [line for binding, line in bindings if binding == pack_name]
    assert not offenders, (
        f"{pack_name} sets both idle_timeout and sleep_after_idle on an agent; "
        "idle_timeout wins and sleep_after_idle only applies when the session "
        "survives it. Drop one:\n" + "\n".join(offenders)
    )
