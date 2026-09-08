from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_order_scope_regression_runs_with_installed_gc() -> None:
    workflow = yaml.safe_load(
        REPO_ROOT.joinpath(".github", "workflows", "ci.yml").read_text(encoding="utf-8")
    )
    jobs = workflow["jobs"]
    assert "check" in jobs, f"no job 'check' in CI; found: {sorted(jobs)}"
    steps = jobs["check"]["steps"]
    step_name = "Lint and exercise shared role prompt composition"
    matching = [step for step in steps if step.get("name") == step_name]
    assert matching, (
        f"no step named {step_name!r} in CI job 'check'; "
        f"found: {[step.get('name') for step in steps]}"
    )

    invocation = (
        'GC_TEST_BIN="$GC_BIN" python3 -m pytest '
        "oversight-rig/tests/test_order_scopes.py -q"
    )
    # Look at the step's *active* lines, not the raw block: a commented-out or
    # `|| true`-suffixed invocation still contains the substring while no
    # longer running, or no longer able to fail the job.
    active = [
        line.strip()
        for line in matching[0]["run"].splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]
    running = [line for line in active if line.startswith(invocation)]
    assert running, (
        f"{step_name!r} does not run the order-scope regression "
        f"(absent or commented out); active lines: {active}"
    )
    assert not any("||" in line for line in running), (
        "the order-scope regression is short-circuited, so a failure cannot "
        f"fail the job: {running}"
    )
