from __future__ import annotations

from dataclasses import replace
import json
import os
import re
import shutil
import shlex
import subprocess
from pathlib import Path

import pytest

from scripts import gascity_pack_inference_gate


def gate_workspace(root: Path) -> gascity_pack_inference_gate.GateWorkspace:
    workspace = gascity_pack_inference_gate.GateWorkspace(
        root=root,
        city_dir=root / "city",
        rig_dir=root / "fixture",
        gc_home=root / "gc-home",
        runtime_dir=root / "runtime",
        claude_config_dir=root / "gc-home" / ".claude",
        city_name="inference-city",
        rig_name="fixture",
    )
    workspace.city_dir.mkdir()
    workspace.rig_dir.mkdir()
    return workspace


def test_write_gate_workspace_uses_city_and_rig_scope_imports(tmp_path) -> None:
    pack_source = tmp_path / "repo" / "gascity"
    roles_source = pack_source / "roles"
    pack_source.mkdir(parents=True)
    roles_source.mkdir()

    workspace = gascity_pack_inference_gate.write_gate_workspace(
        tmp_path / "gate",
        pack_source=pack_source,
        roles_source=roles_source,
        city_name="inference-city",
        rig_name="fixture",
    )

    city_toml = (workspace.city_dir / "city.toml").read_text(encoding="utf-8")
    pack_toml = (workspace.city_dir / "pack.toml").read_text(encoding="utf-8")
    site_toml = (workspace.city_dir / ".gc" / "site.toml").read_text(encoding="utf-8")

    assert '[workspace]\nprovider = "claude"\n' in city_toml
    assert "includes =" not in city_toml
    assert "[workspace.env]" in city_toml
    assert f"HOME = \"{workspace.gc_home}\"" in city_toml
    assert '[providers.claude]\nbase = "builtin:claude"\n' in city_toml
    assert "args_append" not in city_toml
    assert "accept_startup_dialogs" not in city_toml
    assert "[beads]" not in city_toml
    assert 'provider = "file"' not in city_toml
    assert "[session]\n" in city_toml
    assert "provider = \"tmux\"" not in city_toml
    assert "socket =" not in city_toml
    assert 'startup_timeout = "3m"' in city_toml
    assert "[[rigs]]" in city_toml
    assert 'name = "fixture"' in city_toml
    assert "[rigs.imports.gc]" in city_toml
    assert f'source = "{roles_source}"' in city_toml
    assert 'workspace_name = "inference-city"' in site_toml
    assert "[[rig]]" in site_toml
    assert 'name = "fixture"' in site_toml
    assert f'path = "{workspace.rig_dir}"' in site_toml

    assert '[pack]\nname = "gascity-pack-inference-gate"\nschema = 2\n' in pack_toml
    assert "[imports.core]" in pack_toml
    assert "[imports.maintenance]" not in pack_toml
    assert "[imports.bd]" in pack_toml
    assert "[imports.gc]" in pack_toml
    assert f'source = "{pack_source}"' in pack_toml

    assert not (workspace.rig_dir / gascity_pack_inference_gate.REVIEW_SUBJECT_PATH).exists()
    subject_path = gascity_pack_inference_gate.write_review_subject(workspace.rig_dir)
    subject = subject_path.read_text(encoding="utf-8")
    assert "shell=True" in subject
    assert "destination" in subject

    slugger = (workspace.rig_dir / "slugger.py").read_text(encoding="utf-8")
    slugger_test = (workspace.rig_dir / "tests" / "test_slugger.py").read_text(encoding="utf-8")
    assert "NotImplementedError" in slugger
    assert "slugify" in slugger_test
    assert "Hello, World!" in slugger_test


def test_write_gate_workspace_materializes_pack_check_scripts(tmp_path) -> None:
    pack_source = tmp_path / "repo" / "gascity"
    roles_source = pack_source / "roles"
    checks_source = pack_source / "assets" / "scripts" / "checks"
    schemas_source = pack_source / "schemas" / "build"
    checks_source.mkdir(parents=True)
    schemas_source.mkdir(parents=True)
    roles_source.mkdir()

    check_script = checks_source / "build-artifact-valid.sh"
    check_script.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    check_script.chmod(0o755)
    validator = pack_source / "assets" / "scripts" / "validate_build_artifact.py"
    validator.write_text("print('ok')\n", encoding="utf-8")
    schema = schemas_source / "requirements.v1.yaml"
    schema.write_text("schema_id: gc.build.requirements.v1\n", encoding="utf-8")

    workspace = gascity_pack_inference_gate.write_gate_workspace(
        tmp_path / "gate",
        pack_source=pack_source,
        roles_source=roles_source,
        city_name="inference-city",
        rig_name="fixture",
    )

    materialized_check = workspace.rig_dir / ".gc" / "scripts" / "checks" / "build-artifact-valid.sh"
    materialized_validator = workspace.rig_dir / ".gc" / "scripts" / "validate_build_artifact.py"
    materialized_schema = workspace.rig_dir / "schemas" / "build" / "requirements.v1.yaml"

    assert materialized_check.read_text(encoding="utf-8") == "#!/usr/bin/env bash\nexit 0\n"
    assert os.access(materialized_check, os.X_OK)
    assert materialized_validator.read_text(encoding="utf-8") == "print('ok')\n"
    assert materialized_schema.read_text(encoding="utf-8") == "schema_id: gc.build.requirements.v1\n"


def test_write_gate_workspace_imports_selected_pack_and_shared_validator(tmp_path) -> None:
    pack_source = tmp_path / "repo" / "superpowers"
    roles_source = tmp_path / "repo" / "gascity" / "roles"
    validator_source = tmp_path / "repo" / "gascity"
    checks_source = validator_source / "assets" / "scripts" / "checks"
    checks_source.mkdir(parents=True)
    roles_source.mkdir(parents=True)
    pack_source.mkdir(parents=True)

    check_script = checks_source / "build-artifact-valid.sh"
    check_script.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    validator = validator_source / "assets" / "scripts" / "validate_build_artifact.py"
    validator.write_text("print('shared-validator')\n", encoding="utf-8")

    workspace = gascity_pack_inference_gate.write_gate_workspace(
        tmp_path / "gate",
        pack_source=pack_source,
        roles_source=roles_source,
        validator_source=validator_source,
        pack_binding="superpowers",
        pack_name="superpowers",
        city_name="superpowers-inference-city",
        rig_name="fixture",
    )

    city_toml = (workspace.city_dir / "city.toml").read_text(encoding="utf-8")
    pack_toml = (workspace.city_dir / "pack.toml").read_text(encoding="utf-8")

    assert '[pack]\nname = "superpowers-pack-inference-gate"\nschema = 2\n' in pack_toml
    assert "[imports.superpowers]" in pack_toml
    assert f'source = "{pack_source}"' in pack_toml
    assert "[rigs.imports.gc]" in city_toml
    assert f'source = "{roles_source}"' in city_toml
    assert "[rigs.imports.superpowers]" in city_toml
    assert f'source = "{pack_source}"' in city_toml
    assert (
        workspace.rig_dir / ".gc" / "scripts" / "validate_build_artifact.py"
    ).read_text(encoding="utf-8") == "print('shared-validator')\n"


def test_write_gate_workspace_wires_gastown_city_and_rig_imports(tmp_path) -> None:
    pack_source = tmp_path / "repo" / "gastown"
    roles_source = tmp_path / "repo" / "gascity" / "roles"
    pack_source.mkdir(parents=True)
    roles_source.mkdir(parents=True)

    workspace = gascity_pack_inference_gate.write_gate_workspace(
        tmp_path / "gate",
        pack_source=pack_source,
        roles_source=roles_source,
        pack_binding="gastown",
        pack_name="gastown",
        gastown=True,
        city_name="gastown-inference-city",
        rig_name="fixture",
    )

    city_toml = (workspace.city_dir / "city.toml").read_text(encoding="utf-8")
    pack_toml = (workspace.city_dir / "pack.toml").read_text(encoding="utf-8")

    assert 'global_fragments = ["command-glossary", "operational-awareness"]' in city_toml
    assert "[defaults.rig.imports.gastown]" not in city_toml
    assert "[rigs.imports.gastown]" in city_toml
    assert f'source = "{pack_source}"' in city_toml
    assert "[rigs.imports.gc]" not in city_toml
    assert "[imports.gastown]" in pack_toml
    assert f'source = "{pack_source}"' in pack_toml


def test_write_gate_workspace_can_limit_gastown_smoke_to_rig_scope(tmp_path) -> None:
    pack_source = tmp_path / "repo" / "gastown"
    roles_source = tmp_path / "repo" / "gascity" / "roles"
    pack_source.mkdir(parents=True)
    roles_source.mkdir(parents=True)

    workspace = gascity_pack_inference_gate.write_gate_workspace(
        tmp_path / "gate",
        pack_source=pack_source,
        roles_source=roles_source,
        pack_binding="gastown",
        pack_name="gastown",
        gastown=True,
        include_pack_at_city_scope=False,
        city_name="gastown-inference-city",
        rig_name="fixture",
    )

    pack_toml = (workspace.city_dir / "pack.toml").read_text(encoding="utf-8")
    city_toml = (workspace.city_dir / "city.toml").read_text(encoding="utf-8")
    assert "[imports.gastown]" not in pack_toml
    assert "[rigs.imports.gastown]" in city_toml


def test_build_gate_env_uses_nightly_ollama_auth_shape(tmp_path) -> None:
    workspace = gascity_pack_inference_gate.GateWorkspace(
        root=tmp_path,
        city_dir=tmp_path / "city",
        rig_dir=tmp_path / "fixture",
        gc_home=tmp_path / "gc-home",
        runtime_dir=tmp_path / "runtime",
        claude_config_dir=tmp_path / "gc-home" / ".claude",
        city_name="inference-city",
        rig_name="fixture",
    )
    workspace.gc_home.mkdir(parents=True)

    env = gascity_pack_inference_gate.build_gate_env(
        "/usr/bin/gc",
        workspace,
        inherited={
            "PATH": "/usr/bin",
            "HOME": str(tmp_path / "home"),
            "OLLAMA_API_KEY": "ollama-secret",
        },
    )

    assert env["ANTHROPIC_BASE_URL"] == "https://ollama.com"
    assert env["ANTHROPIC_AUTH_TOKEN"] == "ollama-secret"
    assert env["GC_INFERENCE_EXPECTED_MODEL"] == "kimi-k2.7-code"
    assert env["HOME"] == str(tmp_path / "home")
    assert "ANTHROPIC_API_KEY" not in env
    assert "GC_SESSION" not in env
    assert "GC_BEADS" not in env
    assert "GC_DOLT" not in env
    assert env["DOLT_ROOT_PATH"] == str(workspace.gc_home)
    dolt_config = json.loads((workspace.gc_home / ".dolt" / "config_global.json").read_text(encoding="utf-8"))
    assert dolt_config["user.email"] == "gascity-pack-gate@example.invalid"


def test_build_gate_env_prefers_explicit_bd_binary(tmp_path) -> None:
    workspace = gascity_pack_inference_gate.GateWorkspace(
        root=tmp_path,
        city_dir=tmp_path / "city",
        rig_dir=tmp_path / "fixture",
        gc_home=tmp_path / "gc-home",
        runtime_dir=tmp_path / "runtime",
        claude_config_dir=tmp_path / "gc-home" / ".claude",
        city_name="inference-city",
        rig_name="fixture",
    )
    workspace.gc_home.mkdir(parents=True)

    env = gascity_pack_inference_gate.build_gate_env(
        "/opt/gc/bin/gc",
        workspace,
        bd_bin="/opt/bd/bin/bd",
        inherited={"PATH": "/usr/bin", "HOME": str(tmp_path / "home")},
    )

    path_parts = env["PATH"].split(os.pathsep)
    assert path_parts[1:3] == ["/opt/gc/bin", "/opt/bd/bin"]
    assert path_parts[-1] == "/usr/bin"


def test_beads_module_version_guard_rejects_gc_bd_schema_drift() -> None:
    gc_metadata = "dep\tgithub.com/steveyegge/beads\tv1.1.0\th1:abc\n"
    bd_metadata = "mod\tgithub.com/steveyegge/beads\t(devel)\n"

    assert gascity_pack_inference_gate.beads_module_version(gc_metadata) == "v1.1.0"
    assert gascity_pack_inference_gate.beads_module_version(bd_metadata) == "devel"
    with pytest.raises(gascity_pack_inference_gate.GateError, match="incompatible gc/bd beads modules"):
        gascity_pack_inference_gate.require_matching_beads_modules(gc_metadata, bd_metadata)


def test_beads_module_version_guard_accepts_matching_modules() -> None:
    gc_metadata = "dep\tgithub.com/steveyegge/beads\tv1.1.0\th1:abc\n"
    bd_metadata = "mod\tgithub.com/steveyegge/beads\tv1.1.0+dirty\th1:abc\n"

    gascity_pack_inference_gate.require_matching_beads_modules(gc_metadata, bd_metadata)


def test_parser_accepts_explicit_bd_binary() -> None:
    args = gascity_pack_inference_gate.build_parser().parse_args(["--bd-bin", "/tmp/bd"])

    assert args.bd_bin == "/tmp/bd"


def inference_env(**overrides: str) -> dict[str, str]:
    env = {
        "OLLAMA_API_KEY": "ollama-secret",
        "ANTHROPIC_BASE_URL": "https://works.gascity.com/manifold-api",
        "ANTHROPIC_AUTH_TOKEN": "manifold-secret",
        "ANTHROPIC_DEFAULT_HAIKU_MODEL": "kimi-k2.7-code",
        "ANTHROPIC_DEFAULT_SONNET_MODEL": "kimi-k2.7-code",
        "ANTHROPIC_DEFAULT_OPUS_MODEL": "kimi-k2.7-code",
        "CLAUDE_CODE_SUBAGENT_MODEL": "kimi-k2.7-code",
        "GC_INFERENCE_EXPECTED_MODEL": "kimi-k2.7-code",
    }
    env.update(overrides)
    return env


def test_validate_inference_env_requires_all_claude_routes_to_use_the_expected_model() -> None:
    env = inference_env(ANTHROPIC_DEFAULT_SONNET_MODEL="claude-fable-5")

    with pytest.raises(gascity_pack_inference_gate.GateError, match="ANTHROPIC_DEFAULT_SONNET_MODEL=.*claude-fable-5"):
        gascity_pack_inference_gate.validate_inference_env(env)


def test_validate_inference_env_rejects_an_anthropic_model_as_the_expected_model() -> None:
    env = inference_env(
        GC_INFERENCE_EXPECTED_MODEL="claude-fable-5",
        ANTHROPIC_DEFAULT_HAIKU_MODEL="claude-fable-5",
        ANTHROPIC_DEFAULT_SONNET_MODEL="claude-fable-5",
        ANTHROPIC_DEFAULT_OPUS_MODEL="claude-fable-5",
        CLAUDE_CODE_SUBAGENT_MODEL="claude-fable-5",
    )

    with pytest.raises(gascity_pack_inference_gate.GateError, match="GC_INFERENCE_EXPECTED_MODEL must be kimi-k2.7-code"):
        gascity_pack_inference_gate.validate_inference_env(env)


def test_preflight_inference_model_accepts_the_requested_model_usage(monkeypatch) -> None:
    expected = "kimi-k2.7-code"
    calls: list[tuple[list[str], dict[str, str]]] = []

    def fake_run_checked(command, *, env, **_kwargs) -> str:
        calls.append((list(command), dict(env)))
        return 'notice\n{"type":"result","is_error":false,"modelUsage":{"kimi-k2.7-code[1m]":{"inputTokens":1}}}'

    monkeypatch.setattr(gascity_pack_inference_gate, "run_checked", fake_run_checked)

    gascity_pack_inference_gate.preflight_inference_model(expected, env=inference_env())

    assert calls == [
        (
            ["claude", "-p", "--model", expected, "--output-format", "json", "Reply with exactly OK."],
            inference_env(),
        )
    ]


def test_preflight_inference_model_rejects_a_successful_fallback_model(monkeypatch) -> None:
    def fake_run_checked(*_args, **_kwargs) -> str:
        return '{"type":"result","is_error":false,"modelUsage":{"claude-fable-5[1m]":{"inputTokens":1}}}'

    monkeypatch.setattr(gascity_pack_inference_gate, "run_checked", fake_run_checked)

    with pytest.raises(gascity_pack_inference_gate.GateError, match="reported modelUsage.*claude-fable-5"):
        gascity_pack_inference_gate.preflight_inference_model("kimi-k2.7-code", env=inference_env())


def test_preflight_inference_model_surfaces_a_json_error_from_claude(monkeypatch) -> None:
    def fake_run_checked(*_args, **_kwargs) -> str:
        raise subprocess.CalledProcessError(
            1,
            "claude",
            output='{"type":"result","is_error":true,"result":"model requires gc-models entitlement","modelUsage":{}}',
        )

    monkeypatch.setattr(gascity_pack_inference_gate, "run_checked", fake_run_checked)

    with pytest.raises(gascity_pack_inference_gate.GateError, match="rejected model.*gc-models entitlement"):
        gascity_pack_inference_gate.preflight_inference_model("kimi-k2.7-code", env=inference_env())


def test_supported_pack_nightly_workflow_uses_manifold_shape_and_pack_matrix() -> None:
    workflow = (gascity_pack_inference_gate.REPO_ROOT / ".github" / "workflows" / "supported-pack-nightly.yml").read_text(
        encoding="utf-8"
    )

    assert "name: Supported Pack Nightly" in workflow
    assert "\n  schedule:" in workflow
    assert "\n  pull_request:" not in workflow
    assert "\n  push:" not in workflow
    assert 'default: main' in workflow
    assert "description: \"Supported pack or group to exercise for manual subset checks.\"" in workflow
    assert "description: \"Inference gate to run for manual subset checks.\"" in workflow
    assert '[ "$EVENT_NAME" != "workflow_dispatch" ]' in workflow
    assert "id: subset" in workflow
    assert "INPUT_PACK: ${{ github.event.inputs.pack }}" in workflow
    assert "INPUT_GATE: ${{ github.event.inputs.gate }}" in workflow
    assert "MATRIX_PACK: ${{ matrix.pack }}" in workflow
    assert "MATRIX_GATE: ${{ matrix.gate }}" in workflow
    assert "github.event.inputs.pack == matrix.pack" not in workflow
    assert "github.event.inputs.gate == matrix.gate" not in workflow
    assert "run_gate=true" in workflow
    assert "if: steps.subset.outputs.run_gate == 'true'" in workflow
    assert "if: always() && steps.subset.outputs.run_gate == 'true'" in workflow
    assert "model-smoke)" in workflow
    assert "superpowers|compound-engineering|gstack|bmad)" in workflow
    assert "max-parallel: 2" in workflow
    assert "runs-on: blacksmith-2vcpu-ubuntu-2404" in workflow
    assert "runs-on: blacksmith-32vcpu-ubuntu-2404" in workflow
    assert "GATE_TIMEOUT: ${{ github.event.inputs.timeout || matrix.gate_timeout }}" in workflow
    assert '--timeout "$GATE_TIMEOUT"' in workflow
    assert 'DOLT_VERSION: "2.1.7"' in workflow
    assert 'BD_VERSION: "v1.1.0"' in workflow
    assert 'go-version: "1.26.5"' in workflow
    assert "ANTHROPIC_BASE_URL: https://works.gascity.com/manifold-api" in workflow
    assert "ANTHROPIC_AUTH_TOKEN: ${{ secrets.MANIFOLD_AUTH_TOKEN }}" in workflow
    assert "OLLAMA_API_KEY: ${{ secrets.OLLAMA_API_KEY }}" in workflow
    assert "ANTHROPIC_API_KEY:" not in workflow
    for model_var in (
        "GC_INFERENCE_EXPECTED_MODEL",
        "GC_WORKER_INFERENCE_CLAUDE_MANIFOLD_HAIKU_MODEL",
        "GC_WORKER_INFERENCE_CLAUDE_MANIFOLD_SONNET_MODEL",
        "GC_WORKER_INFERENCE_CLAUDE_MANIFOLD_OPUS_MODEL",
        "GC_WORKER_INFERENCE_CLAUDE_MANIFOLD_SUBAGENT_MODEL",
    ):
        assert model_var in workflow
    def matrix_entry(pack: str) -> str:
        match = re.search(
            rf"(?ms)^\s*- pack: {re.escape(pack)}\s*$([\s\S]*?)(?=^\s*- pack:|^    env:)",
            workflow,
        )
        assert match, f"matrix entry for {pack} was not found"
        return match.group(0)

    gascity = matrix_entry("gascity")
    assert re.search(r"(?m)^\s*gate: build$", gascity)
    assert re.search(r"(?m)^\s*timeout_minutes: 30$", gascity)
    assert re.search(r"(?m)^\s*gate_timeout: 30m$", gascity)
    for pack in ("superpowers", "compound-engineering", "gstack", "bmad", "gastown"):
        entry = matrix_entry(pack)
        assert re.search(r"(?m)^\s*gate: smoke$", entry)
        assert re.search(r"(?m)^\s*timeout_minutes: 25$", entry)
        assert re.search(r"(?m)^\s*gate_timeout: 25m$", entry)
    assert "GATE_TIMEOUT: ${{ github.event.inputs.timeout || matrix.gate_timeout }}" in workflow
    assert '--pack "${{ matrix.pack }}"' in workflow
    assert '--gate "${{ matrix.gate }}"' in workflow
    assert "name: supported-pack-nightly-${{ matrix.pack }}-${{ matrix.gate }}" in workflow
    assert "!${{ runner.temp }}/supported-pack-nightly/${{ matrix.pack }}/gc-home/.dolt/eventsData/**" in workflow
    assert "!${{ runner.temp }}/supported-pack-nightly/${{ matrix.pack }}/**/.beads/eventsData/**" in workflow
    assert "include-hidden-files: true" in workflow


def test_dispatch_inference_workflow_is_manual_or_external_only() -> None:
    workflow = (gascity_pack_inference_gate.REPO_ROOT / ".github" / "workflows" / "gascity-pack-inference.yml").read_text(
        encoding="utf-8"
    )

    assert "repository_dispatch:" in workflow
    assert "workflow_dispatch:" in workflow
    assert "\n  schedule:" not in workflow
    assert "\n  pull_request:" not in workflow
    assert "\n  push:" not in workflow
    assert "runs-on: blacksmith-32vcpu-ubuntu-2404" in workflow
    assert 'DOLT_VERSION: "2.1.7"' in workflow
    assert 'BD_VERSION: "v1.1.0"' in workflow
    assert 'go-version: "1.26.5"' in workflow
    assert "include-hidden-files: true" in workflow
    assert "ANTHROPIC_BASE_URL: https://works.gascity.com/manifold-api" in workflow
    assert "ANTHROPIC_AUTH_TOKEN: ${{ secrets.MANIFOLD_AUTH_TOKEN }}" in workflow
    assert "OLLAMA_API_KEY: ${{ secrets.OLLAMA_API_KEY }}" in workflow
    assert "ANTHROPIC_API_KEY:" not in workflow


def test_ci_workflows_use_blacksmith_runner_labels() -> None:
    expected = {
        ".github/workflows/ci.yml": "runs-on: blacksmith-32vcpu-ubuntu-2404",
        ".github/workflows/codeql.yml": "runs-on: blacksmith-32vcpu-ubuntu-2404",
        ".github/workflows/pack-release-compatibility.yml": "runs-on: blacksmith-32vcpu-ubuntu-2404",
    }

    for relative_path, marker in expected.items():
        workflow = (gascity_pack_inference_gate.REPO_ROOT / relative_path).read_text(encoding="utf-8")
        assert marker in workflow


def test_readme_includes_blacksmith_sponsor_badge() -> None:
    readme = (gascity_pack_inference_gate.REPO_ROOT / "README.md").read_text(encoding="utf-8")
    readme_lines = {line.strip() for line in readme.splitlines()}

    assert "## Sponsors" in readme
    assert '<a href="https://blacksmith.sh/">' in readme_lines
    assert "docs/images/blacksmith-powered.png" in readme
    assert (gascity_pack_inference_gate.REPO_ROOT / "docs" / "images" / "blacksmith-powered.png").is_file()


def test_build_gate_env_exposes_host_pytest_to_isolated_runtime(tmp_path) -> None:
    workspace = gascity_pack_inference_gate.GateWorkspace(
        root=tmp_path,
        city_dir=tmp_path / "city",
        rig_dir=tmp_path / "fixture",
        gc_home=tmp_path / "gc-home",
        runtime_dir=tmp_path / "runtime",
        claude_config_dir=tmp_path / "gc-home" / ".claude",
        city_name="inference-city",
        rig_name="fixture",
    )
    workspace.gc_home.mkdir(parents=True)
    existing_pythonpath = str(tmp_path / "existing-pythonpath")

    env = gascity_pack_inference_gate.build_gate_env(
        "/usr/bin/gc",
        workspace,
        inherited={
            "PATH": "/usr/bin",
            "HOME": str(tmp_path / "home"),
            "OLLAMA_API_KEY": "ollama-secret",
            "PYTHONPATH": existing_pythonpath,
        },
    )

    pytest_root = Path(pytest.__file__).resolve().parent.parent
    pythonpath_parts = env["PYTHONPATH"].split(os.pathsep)
    assert pythonpath_parts[0] == existing_pythonpath
    assert str(pytest_root) in pythonpath_parts


def test_seed_claude_project_state_writes_home_and_config_state(tmp_path) -> None:
    home = tmp_path / "home"
    config_dir = tmp_path / "claude-config"
    city_dir = tmp_path / "city"
    rig_dir = tmp_path / "rig"
    city_dir.mkdir()
    rig_dir.mkdir()

    gascity_pack_inference_gate.seed_claude_project_state(
        home=home,
        config_dir=config_dir,
        project_paths=[city_dir, rig_dir],
    )

    for state_path in (home / ".claude.json", config_dir / ".claude.json"):
        data = json.loads(state_path.read_text(encoding="utf-8"))
        assert data["hasCompletedOnboarding"] is True
        assert data["theme"] == "light"
        for project in (city_dir.resolve(), rig_dir.resolve()):
            entry = data["projects"][str(project)]
            assert entry["hasCompletedProjectOnboarding"] is True
            assert entry["hasTrustDialogAccepted"] is True
            assert entry["projectOnboardingSeenCount"] == 1


def test_find_unique_bead_by_title_rejects_missing_or_ambiguous() -> None:
    beads = [
        {"id": "bd-1", "title": "other", "status": "open"},
        {"id": "bd-2", "title": "gate title", "status": "open"},
    ]

    assert gascity_pack_inference_gate.find_unique_bead_by_title(beads, "gate title")["id"] == "bd-2"
    assert gascity_pack_inference_gate.find_unique_bead_by_title(beads, "missing") is None
    assert gascity_pack_inference_gate.find_unique_bead_by_title(beads + [beads[1]], "gate title") is None


def test_extract_sling_root_id_searches_nested_json() -> None:
    output = """
    warning: ignored line
    {"dispatch": {"root_bead_id": "rv-123", "nested": [{"id": "other"}]}}
    """

    assert gascity_pack_inference_gate.extract_sling_root_id(output) == "rv-123"
    assert gascity_pack_inference_gate.extract_sling_root_id("not json") is None


def test_list_beads_uses_gc_bd_list_when_file_store_absent(tmp_path) -> None:
    workspace = gascity_pack_inference_gate.GateWorkspace(
        root=tmp_path,
        city_dir=tmp_path / "city",
        rig_dir=tmp_path / "fixture",
        gc_home=tmp_path / "gc-home",
        runtime_dir=tmp_path / "runtime",
        claude_config_dir=tmp_path / "gc-home" / ".claude",
        city_name="inference-city",
        rig_name="fixture",
    )
    workspace.city_dir.mkdir()
    workspace.rig_dir.mkdir()
    fake_gc = tmp_path / "gc"
    args_path = tmp_path / "gc-args.txt"
    fake_gc.write_text(
        f"""#!/bin/sh
printf '%s\\n' "$@" > {shlex.quote(str(args_path))}
printf 'warning: noisy config refresh\\n' >&2
printf '[{{"id":"fi-1","title":"root","status":"open"}}]\\n'
""",
        encoding="utf-8",
    )
    fake_gc.chmod(0o755)

    beads = gascity_pack_inference_gate.list_beads(str(fake_gc), workspace, env={})

    assert beads == [{"id": "fi-1", "title": "root", "status": "open"}]
    assert args_path.read_text(encoding="utf-8").splitlines()[-4:] == ["--all", "--json", "--limit", "1000"]


def test_list_beads_falls_back_to_city_event_log_when_live_list_is_empty(tmp_path) -> None:
    workspace = gascity_pack_inference_gate.GateWorkspace(
        root=tmp_path,
        city_dir=tmp_path / "city",
        rig_dir=tmp_path / "fixture",
        gc_home=tmp_path / "gc-home",
        runtime_dir=tmp_path / "runtime",
        claude_config_dir=tmp_path / "gc-home" / ".claude",
        city_name="inference-city",
        rig_name="fixture",
    )
    (workspace.city_dir / ".gc").mkdir(parents=True)
    workspace.rig_dir.mkdir()
    fake_gc = tmp_path / "gc"
    fake_gc.write_text("#!/bin/sh\nprintf '[]\\n'\n", encoding="utf-8")
    fake_gc.chmod(0o755)

    event = {
        "type": "bead.updated",
        "payload": {
            "bead": {
                "id": "fi-dk42",
                "title": "Write review report",
                "status": "closed",
                "metadata": {
                    "gc.run_target": "gc.implementation-reviewer",
                    "gc.routed_to": "fixture/gc.implementation-reviewer",
                },
            }
        },
    }
    (workspace.city_dir / ".gc" / "events.jsonl").write_text(json.dumps(event) + "\n", encoding="utf-8")

    beads = gascity_pack_inference_gate.list_beads(str(fake_gc), workspace, env={})

    assert beads == [event["payload"]["bead"]]
    gascity_pack_inference_gate.validate_required_routes(
        beads,
        ["gc.implementation-reviewer"],
        context="replayed review gate",
    )


def test_list_beads_falls_back_to_current_city_event_log_shape(tmp_path) -> None:
    workspace = gascity_pack_inference_gate.GateWorkspace(
        root=tmp_path,
        city_dir=tmp_path / "city",
        rig_dir=tmp_path / "fixture",
        gc_home=tmp_path / "gc-home",
        runtime_dir=tmp_path / "runtime",
        claude_config_dir=tmp_path / "gc-home" / ".claude",
        city_name="inference-city",
        rig_name="fixture",
    )
    (workspace.city_dir / ".gc").mkdir(parents=True)
    workspace.rig_dir.mkdir()
    fake_gc = tmp_path / "gc"
    fake_gc.write_text("#!/bin/sh\nprintf '[]\\n'\n", encoding="utf-8")
    fake_gc.chmod(0o755)

    event = {
        "type": "bead.closed",
        "payload": {
            "id": "fi-dk42",
            "title": "Write review report",
            "status": "closed",
            "metadata": {
                "gc.run_target": "gc.implementation-reviewer",
                "gc.routed_to": "fixture/gc.implementation-reviewer",
            },
        },
    }
    (workspace.city_dir / ".gc" / "events.jsonl").write_text(json.dumps(event) + "\n", encoding="utf-8")

    beads = gascity_pack_inference_gate.list_beads(str(fake_gc), workspace, env={})

    assert beads == [event["payload"]]
    gascity_pack_inference_gate.validate_required_routes(
        beads,
        ["gc.implementation-reviewer"],
        context="replayed current event gate",
    )


def test_list_beads_merges_event_route_history_when_live_list_is_incomplete(tmp_path) -> None:
    workspace = gascity_pack_inference_gate.GateWorkspace(
        root=tmp_path,
        city_dir=tmp_path / "city",
        rig_dir=tmp_path / "fixture",
        gc_home=tmp_path / "gc-home",
        runtime_dir=tmp_path / "runtime",
        claude_config_dir=tmp_path / "gc-home" / ".claude",
        city_name="inference-city",
        rig_name="fixture",
    )
    (workspace.city_dir / ".gc").mkdir(parents=True)
    workspace.rig_dir.mkdir()
    fake_gc = tmp_path / "gc"
    fake_gc.write_text(
        """#!/bin/sh
printf '[{"id":"fi-hm7i","title":"Write review report","assignee":"fixture--control-dispatcher"}]\n'
""",
        encoding="utf-8",
    )
    fake_gc.chmod(0o755)

    event = {
        "type": "bead.updated",
        "payload": {
            "bead": {
                "id": "fi-glhz",
                "title": "Write review report",
                "status": "closed",
                "metadata": {
                    "gc.run_target": "gc.implementation-reviewer",
                    "gc.routed_to": "fixture/gc.implementation-reviewer",
                },
            }
        },
    }
    (workspace.city_dir / ".gc" / "events.jsonl").write_text(json.dumps(event) + "\n", encoding="utf-8")

    beads = gascity_pack_inference_gate.list_beads(str(fake_gc), workspace, env={})

    assert beads[0]["id"] == "fi-hm7i"
    assert beads[1]["id"] == "__gc_event_route_history__"
    assert gascity_pack_inference_gate.find_unique_bead_by_title(beads, "Write review report")["id"] == "fi-hm7i"
    gascity_pack_inference_gate.validate_required_routes(
        beads,
        ["gc.implementation-reviewer"],
        context="review gate with live list",
    )


def test_wait_for_workflow_pass_uses_bd_show_for_closed_root(tmp_path) -> None:
    workspace = gascity_pack_inference_gate.GateWorkspace(
        root=tmp_path,
        city_dir=tmp_path / "city",
        rig_dir=tmp_path / "fixture",
        gc_home=tmp_path / "gc-home",
        runtime_dir=tmp_path / "runtime",
        claude_config_dir=tmp_path / "gc-home" / ".claude",
        city_name="inference-city",
        rig_name="fixture",
    )
    workspace.city_dir.mkdir()
    workspace.rig_dir.mkdir()
    fake_gc = tmp_path / "gc"
    args_path = tmp_path / "gc-args.txt"
    fake_gc.write_text(
        f"""#!/bin/sh
printf '%s\\n' "$*" >> {shlex.quote(str(args_path))}
case "$*" in
  *"bd show fi-root --json"*) # gc-bd-argv-tail: fake gc receives the wrapper's argv tail
    printf '[{{"id":"fi-root","title":"root","status":"closed","metadata":{{"gc.outcome":"pass"}}}}]\\n'
    ;;
  *"bd list --json --limit 1000"*) # gc-bd-argv-tail: fake gc receives the wrapper's argv tail
    printf '[{{"id":"fi-other","title":"other","status":"open"}}]\\n'
    ;;
  *)
    printf '{{}}\\n'
    ;;
esac
""",
        encoding="utf-8",
    )
    fake_gc.chmod(0o755)

    bead = gascity_pack_inference_gate.wait_for_workflow_pass(
        str(fake_gc),
        workspace,
        "fi-root",
        env={},
        timeout=5,
        poll_interval=0,
    )

    assert bead["id"] == "fi-root"
    assert "bd show fi-root --json" in args_path.read_text(encoding="utf-8")  # gc-bd-argv-tail
    assert "--include-comments" in args_path.read_text(encoding="utf-8")


def test_validate_review_report_requires_blocking_base_gascity_report(tmp_path) -> None:
    workspace = gate_workspace(tmp_path)
    report_path = workspace.rig_dir / gascity_pack_inference_gate.REVIEW_REPORT_PATH
    report_path.parent.mkdir(parents=True)
    report_path.write_text(valid_review_artifact(status="changes_required"), encoding="utf-8")

    gascity_pack_inference_gate.validate_review_report(
        {"metadata": {}},
        workspace,
        env={},
        pack_spec=gascity_pack_inference_gate.PACK_SPECS["gascity"],
    )


def test_validate_review_report_accepts_methodology_fix_summary_from_metadata(tmp_path) -> None:
    workspace = gate_workspace(tmp_path)
    report_path = workspace.rig_dir / ".gc" / "inference-gate" / "artifacts" / "review-fix-summary.md"
    report_path.parent.mkdir(parents=True)
    report_path.write_text(valid_review_artifact(status="approved"), encoding="utf-8")
    pack_spec = replace(
        gascity_pack_inference_gate.PACK_SPECS["superpowers"],
        validator_source=gascity_pack_inference_gate.REPO_ROOT / "gascity",
    )

    gascity_pack_inference_gate.validate_review_report(
        {"metadata": {"gc.build.code_review_report_path": str(report_path.relative_to(workspace.rig_dir))}},
        workspace,
        env={},
        pack_spec=pack_spec,
    )


def test_validate_review_report_rejects_approved_base_gascity_report(tmp_path) -> None:
    workspace = gate_workspace(tmp_path)
    report_path = workspace.rig_dir / gascity_pack_inference_gate.REVIEW_REPORT_PATH
    report_path.parent.mkdir(parents=True)
    report_path.write_text(valid_review_artifact(status="approved"), encoding="utf-8")

    with pytest.raises(gascity_pack_inference_gate.GateError, match="valid expected review artifact"):
        gascity_pack_inference_gate.validate_review_report(
            {"metadata": {}},
            workspace,
            env={},
            pack_spec=gascity_pack_inference_gate.PACK_SPECS["gascity"],
        )


def test_expand_gate_selection_supports_build_basic_and_all() -> None:
    assert gascity_pack_inference_gate.expand_gate_selection("review") == ["review"]
    assert gascity_pack_inference_gate.expand_gate_selection("build") == ["build-basic"]
    assert gascity_pack_inference_gate.expand_gate_selection("build-basic") == ["build-basic"]
    assert gascity_pack_inference_gate.expand_gate_selection("all") == ["review", "build-basic"]


def test_expand_gate_selection_is_pack_specific() -> None:
    superpowers = gascity_pack_inference_gate.PACK_SPECS["superpowers"]
    gastown = gascity_pack_inference_gate.PACK_SPECS["gastown"]

    assert gascity_pack_inference_gate.expand_gate_selection("all", superpowers) == ["review", "build"]
    assert gascity_pack_inference_gate.expand_gate_selection("build", superpowers) == ["build"]
    with pytest.raises(ValueError, match="build-basic"):
        gascity_pack_inference_gate.expand_gate_selection("build-basic", superpowers)

    assert gascity_pack_inference_gate.expand_gate_selection("all", gastown) == ["gastown-orchestration"]
    with pytest.raises(ValueError, match="review"):
        gascity_pack_inference_gate.expand_gate_selection("review", gastown)


def test_expand_pack_selection_supports_supported_pack_groups() -> None:
    assert gascity_pack_inference_gate.expand_pack_selection("methodology") == list(
        gascity_pack_inference_gate.METHODOLOGY_PACKS
    )
    assert gascity_pack_inference_gate.expand_pack_selection("all-supported") == list(
        gascity_pack_inference_gate.PACK_SPECS.keys()
    )


def test_model_smoke_selection_covers_the_five_non_canary_packs() -> None:
    assert gascity_pack_inference_gate.expand_pack_selection("model-smoke") == [
        "superpowers",
        "compound-engineering",
        "gstack",
        "bmad",
        "gastown",
    ]


def test_model_smoke_skips_only_the_deep_gastown_orchestration_contract() -> None:
    assert not gascity_pack_inference_gate.should_validate_gastown_orchestration_contract(
        [gascity_pack_inference_gate.SMOKE_GATE]
    )
    assert gascity_pack_inference_gate.should_validate_gastown_orchestration_contract(
        [gascity_pack_inference_gate.GASTOWN_ORCHESTRATION_GATE]
    )


def test_smoke_task_requests_the_required_acknowledgment_on_one_line() -> None:
    task = gascity_pack_inference_gate.smoke_task(gascity_pack_inference_gate.PACK_SPECS["superpowers"])

    assert "`PACK_SMOKE_OK: superpowers`" in task


def test_require_smoke_ack_accepts_the_pack_specific_sentinel() -> None:
    gascity_pack_inference_gate.require_smoke_ack(
        {"notes": "PACK_SMOKE_OK: superpowers\n"},
        gascity_pack_inference_gate.PACK_SPECS["superpowers"],
    )


def test_require_smoke_ack_accepts_the_pack_specific_comment() -> None:
    gascity_pack_inference_gate.require_smoke_ack(
        {"comments": [{"text": "PACK_SMOKE_OK: superpowers"}]},
        gascity_pack_inference_gate.PACK_SPECS["superpowers"],
    )


def test_require_smoke_ack_rejects_a_closed_bead_without_the_sentinel() -> None:
    with pytest.raises(gascity_pack_inference_gate.GateError, match="PACK_SMOKE_OK: superpowers"):
        gascity_pack_inference_gate.require_smoke_ack(
            {"notes": "Completed without the requested compatibility proof."},
            gascity_pack_inference_gate.PACK_SPECS["superpowers"],
        )


def test_pack_specs_cover_supported_formula_entrypoints() -> None:
    for pack_name in gascity_pack_inference_gate.METHODOLOGY_PACKS:
        spec = gascity_pack_inference_gate.PACK_SPECS[pack_name]
        assert (spec.source / "pack.toml").is_file()
        assert spec.review_formula
        assert spec.build_formula
        assert (spec.source / "formulas" / f"{spec.review_formula}.formula.toml").is_file()
        assert (spec.source / "formulas" / f"{spec.build_formula}.formula.toml").is_file()

    gastown = gascity_pack_inference_gate.PACK_SPECS["gastown"]
    assert gastown.gastown is True
    for formula in gastown.setup_formulas:
        assert (gastown.source / "formulas" / f"{formula}.toml").is_file()


def test_supported_step_formulas_do_not_combine_expand_and_check() -> None:
    formula_roots = [
        gascity_pack_inference_gate.REPO_ROOT / pack_name / "formulas"
        for pack_name in (
            "gascity",
            *gascity_pack_inference_gate.METHODOLOGY_PACKS,
        )
    ]
    offenders: list[str] = []
    for formula_root in formula_roots:
        for path in sorted(formula_root.glob("*.toml")):
            text = path.read_text(encoding="utf-8")
            for block in re.split(r"(?m)^\[\[steps\]\]\s*$", text)[1:]:
                step_id_match = re.search(r'(?m)^id\s*=\s*"([^"]+)"', block)
                step_id = step_id_match.group(1) if step_id_match else "<unknown>"
                if re.search(r"(?m)^expand\s*=", block) and re.search(r"(?m)^\[steps\.check\]\s*$", block):
                    offenders.append(f"{path.relative_to(gascity_pack_inference_gate.REPO_ROOT)}:{step_id}")

    assert offenders == []


def test_validate_required_routes_accepts_metadata_and_prefixed_assignees() -> None:
    beads = [
        {"metadata": {"gc.run_target": "superpowers.brainstorming"}},
        {"assignee": "fixture/superpowers.implementer"},
        {"metadata": {"custom.run_target": ["gstack.qa-lead"]}},
        {"metadata": {"gc.execution_routed_to": "fixture/gc.implementation-reviewer"}},
    ]

    gascity_pack_inference_gate.validate_required_routes(
        beads,
        [
            "superpowers.brainstorming",
            "superpowers.implementer",
            "gstack.qa-lead",
            "gc.implementation-reviewer",
        ],
        context="route test",
    )


def test_validate_required_routes_rejects_missing_expected_agent() -> None:
    with pytest.raises(gascity_pack_inference_gate.GateError, match="missing.agent"):
        gascity_pack_inference_gate.validate_required_routes(
            [{"metadata": {"gc.run_target": "superpowers.brainstorming"}}],
            ["missing.agent"],
            context="route test",
        )


def test_review_signal_accepts_a_discovered_blocking_upstream_artifact(tmp_path) -> None:
    report = tmp_path / ".gc" / "inference-gate" / "review-report.md"
    artifacts = report.parent / "artifacts"
    artifacts.mkdir(parents=True)
    report.write_text(
        """---
schema: gc.build.review.v1
status: approved
---

The command injection finding was fixed after review.
""",
        encoding="utf-8",
    )
    upstream_report = artifacts / "implementation-review-report.md"
    upstream_report.write_text(
        """# Implementation Review Report

The code uses subprocess.run with shell=True, creating a shell injection risk.

## Verdict

**`iterate`**

This cannot be approved until the subprocess call uses an argument vector.
""",
        encoding="utf-8",
    )

    candidates = gascity_pack_inference_gate.review_report_candidates(
        {},
        tmp_path,
        gascity_pack_inference_gate.PACK_SPECS["superpowers"],
    )

    assert upstream_report.resolve() in candidates
    gascity_pack_inference_gate.require_expected_review_signal(upstream_report)


def test_gastown_session_matching_accepts_bound_and_unbound_identities() -> None:
    sessions = [
        {"name": "mayor"},
        {"agent_id": "gastown.deacon"},
        {"session": "boot"},
        {"agent": "fixture/gastown.witness"},
    ]

    assert gascity_pack_inference_gate.missing_session_agents(
        sessions,
        gascity_pack_inference_gate.GASTOWN_ALWAYS_ON_AGENTS,
    ) == []
    assert gascity_pack_inference_gate.missing_session_agents(sessions, ["refinery"]) == ["refinery"]


def test_gastown_review_assignment_is_review_only() -> None:
    description = gascity_pack_inference_gate.gastown_review_assignment_description()

    assert "Do not execute the" in description
    assert "the subject of the review" in description


def test_require_gastown_review_report_checks_structured_notes() -> None:
    gascity_pack_inference_gate.require_gastown_review_report(
        {
            "notes": """\
## Summary
The review leg completed.

## Findings
Refinery is on demand and should not be required as an active startup session.

## Recommendation
Check its configured formula and named-session surface instead.
"""
        }
    )


def test_validate_gastown_orchestration_contract_accepts_current_pack() -> None:
    gascity_pack_inference_gate.validate_gastown_orchestration_contract(
        gascity_pack_inference_gate.PACK_SPECS["gastown"].source
    )


def test_validate_gastown_orchestration_contract_rejects_missing_build_handoff(tmp_path) -> None:
    formulas = tmp_path / "gastown" / "formulas"
    formulas.mkdir(parents=True)
    for formula_name, fragments in gascity_pack_inference_gate.all_gastown_formula_contracts().items():
        text = "\n".join(fragments)
        if formula_name == "mol-polecat-work":
            text = text.replace("--assignee=\"$REFINERY_TARGET\"", "")
        (formulas / f"{formula_name}.toml").write_text(text, encoding="utf-8")

    with pytest.raises(gascity_pack_inference_gate.GateError, match="mol-polecat-work"):
        gascity_pack_inference_gate.validate_gastown_orchestration_contract(tmp_path / "gastown")


def test_validate_gastown_orchestration_contract_rejects_missing_refinery_false_completion_guard(tmp_path) -> None:
    formulas = tmp_path / "gastown" / "formulas"
    formulas.mkdir(parents=True)
    for formula_name, fragments in gascity_pack_inference_gate.all_gastown_formula_contracts().items():
        text = "\n".join(fragments)
        if formula_name == "mol-refinery-patrol":
            text = text.replace("branch_has_real_change", "")
        (formulas / f"{formula_name}.toml").write_text(text, encoding="utf-8")

    with pytest.raises(gascity_pack_inference_gate.GateError, match="mol-refinery-patrol"):
        gascity_pack_inference_gate.validate_gastown_orchestration_contract(tmp_path / "gastown")


def test_validate_gastown_orchestration_contract_rejects_missing_refinery_idle_contract(tmp_path) -> None:
    spec = gascity_pack_inference_gate.PACK_SPECS["gastown"]
    pack_source = tmp_path / "gastown"
    shutil.copytree(spec.source, pack_source)
    agent = pack_source / "agents" / "refinery" / "agent.toml"
    agent.write_text(
        agent.read_text(encoding="utf-8").replace('sleep_after_idle = "300s"', 'sleep_after_idle = ""'),
        encoding="utf-8",
    )

    with pytest.raises(gascity_pack_inference_gate.GateError, match="sleep_after_idle"):
        gascity_pack_inference_gate.validate_gastown_orchestration_contract(pack_source)


def test_validate_methodology_flow_contracts_accept_current_packs() -> None:
    for pack_name in gascity_pack_inference_gate.METHODOLOGY_PACKS:
        gascity_pack_inference_gate.validate_methodology_flow_contract(
            gascity_pack_inference_gate.PACK_SPECS[pack_name]
        )


def test_validate_methodology_flow_contract_rejects_missing_specialist_review_lane(tmp_path) -> None:
    spec = gascity_pack_inference_gate.PACK_SPECS["superpowers"]
    pack_source = tmp_path / "superpowers"
    shutil.copytree(spec.source / "formulas", pack_source / "formulas")
    review_expansion = pack_source / "formulas" / "superpowers-code-review.formula.toml"
    review_expansion.write_text(
        review_expansion.read_text(encoding="utf-8").replace(
            "superpowers.code-quality-reviewer",
            "superpowers.code-reviewer",
        ),
        encoding="utf-8",
    )

    with pytest.raises(gascity_pack_inference_gate.GateError, match="superpowers.code-quality-reviewer"):
        gascity_pack_inference_gate.validate_methodology_flow_contract(
            replace(spec, source=pack_source)
        )


def test_validate_methodology_flow_contract_rejects_missing_gstack_release_readiness(tmp_path) -> None:
    spec = gascity_pack_inference_gate.PACK_SPECS["gstack"]
    pack_source = tmp_path / "gstack"
    shutil.copytree(spec.source / "formulas", pack_source / "formulas")
    build_formula = pack_source / "formulas" / "gstack-build.formula.toml"
    build_formula.write_text(
        build_formula.read_text(encoding="utf-8").replace('id = "release-readiness"', 'id = "release-check"'),
        encoding="utf-8",
    )

    with pytest.raises(gascity_pack_inference_gate.GateError, match="release-readiness"):
        gascity_pack_inference_gate.validate_methodology_flow_contract(
            replace(spec, source=pack_source)
        )


def test_gastown_build_workflow_contract_covers_orchestration_roles() -> None:
    contracts = gascity_pack_inference_gate.GASTOWN_BUILD_WORKFLOW_CONTRACTS

    assert set(contracts) == {
        "mol-polecat-work",
        "mol-refinery-patrol",
        "mol-witness-patrol",
        "mol-deacon-patrol",
        "mol-idea-to-plan",
    }
    assert "gc session wake \"$REFINERY_TARGET\"" in contracts["mol-polecat-work"]
    assert 'git worktree add --detach "$MERGE_WT" "origin/$TARGET"' in contracts["mol-refinery-patrol"]
    assert 'gc bd close "$WORK" --reason "Merged to $TARGET at $MERGED_SHORT"' in contracts["mol-refinery-patrol"]
    assert "gc bd close $WORK --reason \"Pull request ready: $PR_URL\"" in contracts["mol-refinery-patrol"]
    assert "FAIL-SAFE: empty liveness map" in contracts["mol-witness-patrol"]
    assert "gc bd create --type=task --labels=warrant" in contracts["mol-deacon-patrol"]
    assert "gc bd dep add" in contracts["mol-idea-to-plan"]


def test_build_basic_work_item_targets_code_and_pytest() -> None:
    text = gascity_pack_inference_gate.build_basic_work_item()

    assert text.splitlines()[0] == gascity_pack_inference_gate.BUILD_SOURCE_TITLE
    assert "slugger.py" in text
    assert "pytest -q" in text
    assert "Do not change tests" in text


def test_validate_build_basic_result_accepts_worktree_with_passing_tests(tmp_path) -> None:
    rig_dir = tmp_path / "fixture"
    worktree = rig_dir / "worktrees" / "fi-source"
    gascity_pack_inference_gate.write_build_basic_fixture(rig_dir)
    gascity_pack_inference_gate.write_build_basic_fixture(worktree)
    (worktree / "slugger.py").write_text(
        """\
import re


def slugify(value: str) -> str:
    parts = re.findall(r"[a-z0-9]+", value.lower())
    return "-".join(parts)
""",
        encoding="utf-8",
    )

    selected = gascity_pack_inference_gate.validate_build_basic_result(
        rig_dir,
        [{"metadata": {"work_dir": str(worktree), "gc.work_dir": str(rig_dir)}}],
        env={},
        timeout=30,
    )

    assert selected == worktree


def test_validate_build_basic_result_finds_nested_worktree_from_rc_metadata(tmp_path) -> None:
    rig_dir = tmp_path / "fixture"
    launcher_dir = rig_dir / "fi-prepare-item-worktree"
    worktree = launcher_dir / "worktrees" / "fi-source"
    gascity_pack_inference_gate.write_build_basic_fixture(rig_dir)
    gascity_pack_inference_gate.write_build_basic_fixture(worktree)
    (worktree / "slugger.py").write_text(
        """\
import re


def slugify(value: str) -> str:
    parts = re.findall(r"[a-z0-9]+", value.lower())
    return "-".join(parts)
""",
        encoding="utf-8",
    )
    summary_path = worktree / ".gc" / "inference-gate" / "build-basic" / "implementation-summary.md"
    summary_path.parent.mkdir(parents=True)
    summary_path.write_text("implementation complete\n", encoding="utf-8")

    selected = gascity_pack_inference_gate.validate_build_basic_result(
        rig_dir,
        [
            {
                "metadata": {
                    "gc.work_dir": str(launcher_dir),
                    "gc.implementation.summary_path": str(summary_path),
                }
            }
        ],
        env={},
        timeout=30,
    )

    assert selected == worktree


def test_validate_build_basic_result_accepts_completed_code_in_rc_rig_root(tmp_path) -> None:
    rig_dir = tmp_path / "fixture"
    gascity_pack_inference_gate.write_build_basic_fixture(rig_dir)
    (rig_dir / "slugger.py").write_text(
        """\
import re


def slugify(value: str) -> str:
    parts = re.findall(r"[a-z0-9]+", value.lower())
    return "-".join(parts)
""",
        encoding="utf-8",
    )
    summary_path = rig_dir / ".gc" / "inference-gate" / "build-basic" / "implementation-summary.md"
    summary_path.parent.mkdir(parents=True)
    summary_path.write_text("implementation complete\n", encoding="utf-8")

    selected = gascity_pack_inference_gate.validate_build_basic_result(
        rig_dir,
        [
            {
                "metadata": {
                    "gc.work_dir": str(rig_dir),
                    "gc.build.implementation_summary_path": str(summary_path),
                }
            }
        ],
        env={},
        timeout=30,
    )

    assert selected == rig_dir


def test_run_build_gate_includes_closed_root_metadata_in_code_validation(tmp_path, monkeypatch) -> None:
    workspace = gate_workspace(tmp_path)
    root_bead = {
        "id": "fi-root",
        "status": "closed",
        "metadata": {"gc.build.implementation_summary_path": str(workspace.rig_dir / ".gc" / "summary.md")},
    }
    captured: dict[str, object] = {}

    monkeypatch.setattr(gascity_pack_inference_gate, "launch_build_formula", lambda *_args, **_kwargs: "fi-root")
    monkeypatch.setattr(gascity_pack_inference_gate, "wait_for_workflow_pass", lambda *_args, **_kwargs: root_bead)
    monkeypatch.setattr(gascity_pack_inference_gate, "validate_build_basic_artifacts", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(gascity_pack_inference_gate, "list_beads", lambda *_args, **_kwargs: [{"id": "open-child"}])
    monkeypatch.setattr(gascity_pack_inference_gate, "validate_required_routes", lambda *_args, **_kwargs: None)

    def capture_result(_rig_dir, beads, **_kwargs):
        captured["beads"] = beads
        return workspace.rig_dir

    monkeypatch.setattr(gascity_pack_inference_gate, "validate_build_basic_result", capture_result)

    gascity_pack_inference_gate.run_build_gate(
        "gc",
        workspace,
        env={},
        pack_spec=gascity_pack_inference_gate.PACK_SPECS["gascity"],
        timeout=30,
        poll_interval=1,
    )

    assert captured["beads"] == [root_bead, {"id": "open-child"}]


def test_validate_build_basic_result_rejects_launcher_only_false_pass(tmp_path) -> None:
    rig_dir = tmp_path / "fixture"
    worktree = rig_dir / "worktrees" / "fi-source"
    gascity_pack_inference_gate.write_build_basic_fixture(rig_dir)
    gascity_pack_inference_gate.write_build_basic_fixture(worktree)
    (rig_dir / "slugger.py").write_text(
        """\
import re


def slugify(value: str) -> str:
    parts = re.findall(r"[a-z0-9]+", value.lower())
    return "-".join(parts)
""",
        encoding="utf-8",
    )

    with pytest.raises(gascity_pack_inference_gate.GateError, match="NotImplementedError"):
        gascity_pack_inference_gate.validate_build_basic_result(
            rig_dir,
            [{"metadata": {"work_dir": str(worktree), "gc.work_dir": str(rig_dir)}}],
            env={},
            timeout=30,
        )


def test_validate_build_basic_artifacts_accepts_declared_markdown_artifacts(tmp_path) -> None:
    rig_dir = tmp_path / "fixture"
    rig_dir.mkdir()
    metadata: dict[str, str] = {}

    for metadata_key, schema in gascity_pack_inference_gate.BUILD_BASIC_ARTIFACT_CONTRACTS:
        artifact_path = rig_dir / f"{metadata_key.rsplit('.', 1)[-1]}.md"
        artifact_path.write_text(valid_build_artifact(schema), encoding="utf-8")
        metadata[metadata_key] = str(artifact_path)

    gascity_pack_inference_gate.validate_build_basic_artifacts(
        {"metadata": metadata},
        rig_dir=rig_dir,
        env={},
        validator_source=gascity_pack_inference_gate.REPO_ROOT / "gascity",
    )


def test_validate_build_basic_artifacts_rejects_json_artifacts(tmp_path) -> None:
    rig_dir = tmp_path / "fixture"
    rig_dir.mkdir()
    bad_path = rig_dir / "requirements.json"
    bad_path.write_text('{"schema":"gc.build.requirements.v1"}\n', encoding="utf-8")

    with pytest.raises(gascity_pack_inference_gate.GateError, match="failed validation"):
        gascity_pack_inference_gate.validate_build_basic_artifacts(
            {"metadata": {"gc.build.requirements_path": str(bad_path)}},
            rig_dir=rig_dir,
            env={},
            validator_source=gascity_pack_inference_gate.REPO_ROOT / "gascity",
        )


def valid_review_artifact(status: str) -> str:
    artifact = valid_build_artifact("gc.build.review.v1").replace("status: approved", f"status: {status}", 1)
    return (
        artifact
        + """
## Security Finding

The reviewed diff uses `subprocess.run(..., shell=True)` with user-controlled
paths, which creates a shell injection risk.

## Remediation

The terminal report verifies the fix: use an argument vector / argument list
with `shell=False`, and mark SEC-001 covered after tests pass.
"""
    )


def valid_build_artifact(schema: str) -> str:
    sections_by_schema = {
        "gc.build.requirements.v1": [
            "Problem Statement",
            "W6H",
            "User Stories",
            "Technical Stories",
            "Behavior Requirements",
            "Example Mapping",
            "Acceptance Criteria",
            "Out Of Scope",
            "Open Questions",
        ],
        "gc.build.plan.v1": [
            "Summary",
            "Current System",
            "Proposed Implementation",
            "Non-Goals",
            "Verification",
        ],
        "gc.build.decomposition.v1": [
            "Summary",
            "Selected Downstream Formulas",
            "Implementation Convoy",
            "Work Items",
        ],
        "gc.build.implementation-summary.v1": [
            "Summary",
            "Intended Behavior",
            "Changed Files",
            "Verification",
            "Remaining Risks",
        ],
        "gc.build.review.v1": [
            "Verdict",
            "Findings",
            "Verification",
        ],
        "gc.build.final-report.v1": [
            "Summary",
            "Outcome",
            "Artifacts",
            "Remaining Risks",
        ],
    }
    sections = sections_by_schema[schema]
    body = "\n".join(f"## {section}\n\nCovered.\n" for section in sections)
    return f"""\
---
schema: {schema}
workflow:
  id: fi-root
  formula: build-basic
methodology:
  pack: gascity
  name: build-basic
producer:
  formula: build-basic
  stage: test
  attempt: 1
status: approved
trace:
  upstream:
    - path: fixture
      hash: literal:test
      ids:
        - AC1
  coverage:
    - id: AC1
      status: covered
---
| ID | Status |
| --- | --- |
| AC1 | covered |

{body}
"""
