from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import subprocess
import textwrap

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
GASCITY_CORE_SOURCE = (
    "https://github.com/gastownhall/gascity.git//internal/bootstrap/packs/core"
)


@dataclass(frozen=True)
class PromptWorkspace:
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


def write_prompt_workspace(
    root: Path,
    *,
    city_binding: str,
    city_pack: Path,
) -> PromptWorkspace:
    city_dir = root / "city"
    rig_dir = root / "fixture"
    gc_home = root / "gc-home"
    home = root / "home"
    (city_dir / ".gc").mkdir(parents=True)
    rig_dir.mkdir()
    gc_home.mkdir()
    home.mkdir()

    roles_source = (REPO_ROOT / "gascity" / "roles").resolve()
    city_pack = city_pack.resolve()
    city_dir.joinpath("pack.toml").write_text(
        textwrap.dedent(
            f"""\
            [pack]
            name = "prompt-integration"
            schema = 2

            [imports.{city_binding}]
            source = {json.dumps(str(city_pack))}
            """
        ),
        encoding="utf-8",
    )
    city_dir.joinpath("city.toml").write_text(
        textwrap.dedent(
            f"""\
            [workspace]
            provider = "codex"

            [providers.codex]
            base = "builtin:codex"

            [[rigs]]
            name = "fixture"

            [rigs.imports.gc]
            source = {json.dumps(str(roles_source))}
            """
        ),
        encoding="utf-8",
    )
    city_dir.joinpath(".gc", "site.toml").write_text(
        textwrap.dedent(
            f"""\
            workspace_name = "prompt-integration"

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
    return PromptWorkspace(city_dir=city_dir, rig_dir=rig_dir, env=env)


def write_gastown_sling_workspace(root: Path) -> PromptWorkspace:
    city_dir = root / "city"
    rig_dir = root / "fixture"
    gc_home = root / "gc-home"
    home = root / "home"
    (city_dir / ".gc").mkdir(parents=True)
    rig_dir.mkdir()
    gc_home.mkdir()
    home.mkdir()

    gastown_source = (REPO_ROOT / "gastown").resolve()
    city_dir.joinpath("pack.toml").write_text(
        textwrap.dedent(
            f"""\
            [pack]
            name = "gastown-sling-integration"
            schema = 2

            [imports.core]
            source = {json.dumps(GASCITY_CORE_SOURCE)}
            """
        ),
        encoding="utf-8",
    )
    city_dir.joinpath("city.toml").write_text(
        textwrap.dedent(
            f"""\
            [workspace]
            provider = "opencode"

            [providers.opencode]
            base = "builtin:opencode"

            [beads]
            provider = "file"

            [[rigs]]
            name = "fixture"
            prefix = "gc"
            default_branch = "main"

            [rigs.imports.gastown]
            source = {json.dumps(str(gastown_source))}
            """
        ),
        encoding="utf-8",
    )
    city_dir.joinpath(".gc", "site.toml").write_text(
        textwrap.dedent(
            f"""\
            workspace_name = "gastown-sling-integration"

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
        "GC_BEADS": "file",
    }
    return PromptWorkspace(city_dir=city_dir, rig_dir=rig_dir, env=env)


def run_gc(
    gc_test_bin: Path,
    workspace: PromptWorkspace,
    *args: str,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(gc_test_bin), *args],
        cwd=cwd or workspace.rig_dir,
        env=env or workspace.env,
        text=True,
        capture_output=True,
        timeout=30,
    )


def assert_clean_worker_render(prompt: str, persona_heading: str) -> None:
    assert persona_heading in prompt
    assert prompt.count("# GC Role Worker") == 1
    assert '{{ template "gc-role-worker" . }}' not in prompt
    assert "gc gc claim" in prompt
    assert "CLAIMED_BEAD_ID" in prompt
    assert "CLAIMED_ROOT_BEAD_ID" in prompt
    assert "CLAIMED_CONTINUATION_GROUP" in prompt
    assert 'gc bd update "$CLAIMED_BEAD_ID"' in prompt
    assert "An empty continuation group is a hard session boundary" in prompt


def test_city_scoped_gascity_registers_claim_command_from_rig(
    tmp_path: Path, gc_test_bin: Path
) -> None:
    workspace = write_prompt_workspace(
        tmp_path,
        city_binding="gc",
        city_pack=REPO_ROOT / "gascity",
    )

    result = run_gc(gc_test_bin, workspace, "gc", "claim", "--help")

    assert result.returncode == 0, result.stderr
    assert "Atomically claim one routed workflow bead" in result.stdout
    assert "Usage:\n  gc gc claim" in result.stdout
    assert "Gas City CLI — orchestration-builder" not in result.stdout


def test_plain_gastown_polecat_sling_starts_default_graph_workflow(
    tmp_path: Path, gc_test_bin: Path
) -> None:
    workspace = write_gastown_sling_workspace(tmp_path)

    result = run_gc(
        gc_test_bin,
        workspace,
        "--city",
        str(workspace.city_dir),
        "--rig",
        "fixture",
        "sling",
        "fixture/gastown.polecat",
        "Exercise the default polecat workflow",
        "--json",
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["target"] == "fixture/gastown.polecat"
    assert payload["method"] == "default-on-formula"
    assert payload["formula"] == "mol-polecat-work"
    assert payload["workflow_id"]
    assert payload["bead_id"] == payload["workflow_id"]
    assert "molecule_id" not in payload


@pytest.mark.parametrize(
    ("city_binding", "pack_path", "agent", "persona_heading"),
    (
        ("gc", "gascity", "gc.implementation-worker", "# GC Role Worker"),
        ("bmad", "bmad", "bmad.story-implementer", "# BMAD Story Implementer"),
        (
            "superpowers",
            "superpowers",
            "superpowers.implementer",
            "# Superpowers Implementer",
        ),
        ("gstack", "gstack", "gstack.implementer", "# gstack Implementer"),
        (
            "compound-engineering",
            "compound-engineering",
            "compound-engineering.ce-work",
            "# Compound Engineering Worker",
        ),
        ("profiler", "profiler", "profiler.profile-analyst", "# Profile Analyst"),
    ),
)
def test_role_prompts_render_public_worker_fragment(
    tmp_path: Path,
    gc_test_bin: Path,
    city_binding: str,
    pack_path: str,
    agent: str,
    persona_heading: str,
) -> None:
    workspace = write_prompt_workspace(
        tmp_path,
        city_binding=city_binding,
        city_pack=REPO_ROOT / pack_path,
    )

    result = run_gc(
        gc_test_bin,
        workspace,
        "--city",
        str(workspace.city_dir),
        "prime",
        "--strict",
        f"fixture/{agent}",
        cwd=workspace.city_dir,
    )

    assert result.returncode == 0, result.stderr
    assert_clean_worker_render(result.stdout, persona_heading)


def test_registered_claim_command_dispatches_store_aware_show_and_normalizes_json(
    tmp_path: Path, gc_test_bin: Path
) -> None:
    workspace = write_prompt_workspace(
        tmp_path,
        city_binding="gc",
        city_pack=REPO_ROOT / "gascity",
    )
    fake_bin = tmp_path / "fake-bin"
    fake_bin.mkdir()
    calls = tmp_path / "nested-gc-calls"
    nested_gc = fake_bin / "gc"
    nested_gc.write_text(
        textwrap.dedent(
            """\
            #!/bin/sh
            printf '%s\n' "$*" >>"$GC_TEST_CALLS"
            if [ "$1" = hook ] && [ "$2" = --claim ] && [ "$3" = --drain-ack ] && [ "$4" = --json ]; then
              printf '%s\n' '{"action":"work","bead_id":"bd-123","assignee":"worker","route":"gc.implementation-worker"}'
            elif [ "$1" = b""d ] && [ "$2" = show ] && [ "$3" = bd-123 ] && [ "$4" = --json ]; then
              printf '%s\n' '{"id":"bd-123","status":"in_progress","assignee":"worker","metadata":{"gc.routed_to":"gc.implementation-worker","gc.root_bead_id":"root-1","gc.continuation_group":"group-1"}}'
            else
              printf 'unexpected nested gc invocation: %s\n' "$*" >&2
              exit 2
            fi
            """
        ),
        encoding="utf-8",
    )
    nested_gc.chmod(0o755)
    env = {
        **workspace.env,
        "BEADS_ACTOR": "worker",
        "GC_AGENT": "gc.implementation-worker",
        "GC_TEST_CALLS": str(calls),
        "PATH": f"{fake_bin}:/usr/bin:/bin",
    }

    result = run_gc(gc_test_bin, workspace, "gc", "claim", env=env)

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["bead_id"] == "bd-123"
    assert payload["root_bead_id"] == "root-1"
    assert payload["continuation_group"] == "group-1"
    assert payload["bead"]["id"] == "bd-123"
    assert calls.read_text(encoding="utf-8").splitlines() == [
        "hook --claim --drain-ack --json",
        " ".join(("b" + "d", "show", "bd-123", "--json")),
    ]
