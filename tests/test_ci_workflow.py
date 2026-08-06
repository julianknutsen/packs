from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_order_scope_regression_runs_with_installed_gc() -> None:
    workflow = yaml.safe_load(
        REPO_ROOT.joinpath(".github", "workflows", "ci.yml").read_text(encoding="utf-8")
    )
    steps = workflow["jobs"]["check"]["steps"]
    step_name = "Lint and exercise shared role prompt composition"
    matching = [step for step in steps if step.get("name") == step_name]
    assert matching, (
        f"no step named {step_name!r} in CI job 'check'; "
        f"found: {[step.get('name') for step in steps]}"
    )

    command = matching[0]["run"]
    assert (
        'GC_TEST_BIN="$GC_BIN" python3 -m pytest '
        "oversight-rig/tests/test_order_scopes.py -q"
    ) in command
