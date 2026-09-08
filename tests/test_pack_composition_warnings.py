"""Every agent-bearing pack must compose into a city without config advisories.

A pack's own unit tests read its TOML and can tell you the file parses. They
cannot tell you what Gas City says when it loads that file into a city, which
is where a user meets the pack. These tests stand each pack up in an isolated
scratch city -- built by the shared `gc_live_city` harness, the same one
`test_maintained_packs_live_gc.py` uses -- and run the real binary.

Each case also imports a fixture pack whose one agent sets both `idle_timeout`
and `sleep_after_idle` on purpose. That canary must be reported in the same
invocation. Without it a green result is ambiguous: a pack with no advisories
and a binary that stopped emitting advisories look identical.

The both-keys *pairing* is the tripwire, not either key on its own, and the two
are not alternatives. `idle_timeout` is a lifecycle stop; whether a session
idle-sleeps is decided by a separate switch that reads `sleep_after_idle` and
never consults `idle_timeout` at all (gc `cmd/gc/compute_awake_set.go`). Which
key is safe to remove therefore depends on the agent -- the offender assertion
below spells out the condition rather than saying "drop one".
"""

from __future__ import annotations

from pathlib import Path
import re
import subprocess
import textwrap

import pytest

from gc_live_city import (
    REPO_ROOT,
    gc_output,
    gc_test_bin,  # noqa: F401 -- pytest fixture, used by name
    write_city,
)


CANARY_BINDING = "idle-canary"
# The canary's declared pack name deliberately differs from its import binding,
# mirroring `gascity/roles` (declared `gc-roles`, bound `gascity-roles`) -- the
# one shipped pack with that mismatch and the pack this suite exists to protect.
# `gc` attributes an advisory by import binding today, so the offender match
# keys on CANARY_BINDING; were a future `gc` to switch to declared-name
# attribution, the canary would surface as CANARY_DECL_NAME, the CANARY_BINDING
# assertion below would fail, and the regression would be caught, not hidden.
CANARY_DECL_NAME = "idle-canary-decl"
CANARY_AGENT = "canary"
IDLE_ADVISORY = "idle_timeout and sleep_after_idle are both set"

# `agent "gastown.mayor"` at city scope, `agent "demo/gastown.refinery"`
# for a rig-scoped one. Both forms name the same pack binding.
ADVISORY_AGENT = re.compile(r'agent "([^"]+)"')


def advisory_binding(line: str) -> str | None:
    """The pack binding an advisory line is about, or None if it names no agent."""
    match = ADVISORY_AGENT.search(line)
    if match is None:
        return None
    return match.group(1).rsplit("/", 1)[-1].split(".", 1)[0]


def tracked_paths() -> frozenset[Path]:
    """Every file git tracks in this repo, as absolute paths.

    Enumerating from git rather than walking the filesystem keeps the pack set
    identical no matter where the suite is invoked from. An `rglob` descends
    into the nested checkouts under `.gc/worktrees/`, `.claude/worktrees/` and
    `worktrees/`, multiplying every real pack into stale copies bound under
    mangled keys -- most of them starting with a dot, which renders
    `[imports..gc-worktrees-...]` and fails the TOML parse, reddening the case.
    A clean checkout collects the eight real packs; the same tree from a live
    rig collected hundreds, and the number climbs as worktrees accumulate. No
    fixed count is recorded here for that reason. `git ls-files` sees only this
    repo's tracked source, so CI and a working checkout parametrize the same
    packs. Same "tracked source, not filesystem" scan as
    `tests/test_no_bare_bd_commands.tracked_files`.
    """
    result = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
    )
    return frozenset(
        REPO_ROOT / path for path in result.stdout.decode().split("\0") if path
    )


def owning_pack(agent_toml: Path, tracked: frozenset[Path]) -> Path | None:
    """The nearest ancestor of an agent.toml that is itself a tracked pack."""
    for parent in agent_toml.parents:
        if parent == REPO_ROOT.parent:
            return None
        if parent / "pack.toml" in tracked:
            return parent
    return None


def agent_bearing_packs() -> list[str]:
    """Pack directories that ship at least one agent definition, REPO_ROOT-relative.

    Derived from `pack.toml` presence rather than from a fixed depth. A glob of
    `*/agents/*/agent.toml` reads as "every pack" and silently drops any pack that
    is not a direct child of the repo root: `gascity/roles` is a pack with its own
    `pack.toml` and its own agents, and it was never parameterized. The nested case
    is the one a depth-pinned pattern always misses, so key on the marker file.

    Both halves read from `git ls-files` rather than the filesystem: the agent
    files, and the ancestor `pack.toml` that attributes each one. An untracked
    `pack.toml` left in a parent directory would otherwise re-attribute every
    pack beneath it -- the same invocation-dependence in a quieter form.
    """
    tracked = tracked_paths()
    agent_tomls = [
        path
        for path in tracked
        if path.name == "agent.toml"
        and len(path.parts) >= 3
        and path.parts[-3] == "agents"
    ]
    packs = {
        pack.relative_to(REPO_ROOT).as_posix()
        for pack in (owning_pack(path, tracked) for path in agent_tomls)
        if pack is not None
    }
    # pytest SKIPS a test whose parameter set is empty rather than failing it, so
    # a derivation that silently found nothing would read as "nothing to check"
    # forever. Fail collection loudly instead.
    assert packs, (
        f"no tracked agents/*/agent.toml under {REPO_ROOT} resolved to a tracked "
        "pack; pack discovery is broken, and an empty parameter set would "
        "silently skip every assertion in this file"
    )
    return sorted(packs)


def binding_name(pack_path: str) -> str:
    """The import binding for a pack path; `gascity/roles` cannot be a bare key."""
    return pack_path.replace("/", "-")


def write_idle_canary_pack(root: Path) -> Path:
    """A pack whose single agent sets both idle keys, so the check can go red.

    A sibling of `gc_live_city.write_canary_pack` rather than a use of it: that
    canary declares a deprecated formula contract and surfaces through
    `gc doctor`, while this one has to raise an idle advisory out of
    `gc config show`.
    """
    pack_dir = root / "idle-canary-pack"
    agent_dir = pack_dir / "agents" / CANARY_AGENT
    agent_dir.mkdir(parents=True)
    pack_dir.joinpath("pack.toml").write_text(
        textwrap.dedent(
            f"""\
            [pack]
            name = "{CANARY_DECL_NAME}"
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


@pytest.mark.parametrize("pack_path", agent_bearing_packs())
def test_pack_agents_compose_without_idle_advisory(
    tmp_path: Path, gc_test_bin: Path, pack_path: str
) -> None:
    pack_name = binding_name(pack_path)
    workspace = write_city(
        tmp_path,
        {
            pack_name: REPO_ROOT / pack_path,
            CANARY_BINDING: write_idle_canary_pack(tmp_path),
        },
    )

    # `gc_output` refuses to return the output of a nonzero exit, and that
    # refusal is load-bearing here rather than merely tidy: every assertion
    # below searches this string for an ABSENT advisory, and a city that failed
    # to load emits no advisory either. The canary covers failed loads that
    # still emit some advisories; the exit code is the only thing separating
    # the rest from a genuinely clean pack.
    output = gc_output(gc_test_bin, workspace, "config", "show")

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
        f"{pack_name} sets both idle_timeout and sleep_after_idle on an agent. "
        "Drop sleep_after_idle when that agent is an on-demand named session "
        "and its value matches the 300s on-demand default -- that is the case "
        "where dropping it is behavior-preserving. Otherwise dropping it "
        "removes idle sleep from the agent entirely, because the awake set "
        "reads sleep_after_idle and never idle_timeout; there, idle_timeout "
        "(a lifecycle stop, not an idle-sleep policy) is the key to "
        "reconsider:\n" + "\n".join(offenders)
    )
