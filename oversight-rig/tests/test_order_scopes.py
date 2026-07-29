from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import textwrap

import pytest


PACK_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def gc_test_bin() -> Path:
    configured = os.environ.get("GC_TEST_BIN")
    if not configured:
        pytest.skip("set GC_TEST_BIN to run real Gas City CLI integration tests")

    binary = Path(configured).expanduser().resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        pytest.fail(f"GC_TEST_BIN is not an executable file: {binary}")
    return binary


def write_order_scope_city(root: Path) -> Path:
    city = root / "city"
    control_pack = root / "control-pack"
    (city / ".gc").mkdir(parents=True)
    (city / "alpha").mkdir()
    (city / "beta").mkdir()
    (control_pack / "orders").mkdir(parents=True)

    city.joinpath("pack.toml").write_text(
        textwrap.dedent(
            f"""\
            [pack]
            name = "order-scope-test"
            schema = 2

            [imports.oversight-rig]
            source = {json.dumps(str(PACK_ROOT))}
            """
        ),
        encoding="utf-8",
    )
    city.joinpath("city.toml").write_text(
        textwrap.dedent(
            f"""\
            [workspace]
            provider = "codex"

            [providers.codex]
            base = "builtin:codex"

            [[rigs]]
            name = "alpha"

            [rigs.imports.oversight-rig]
            source = {json.dumps(str(PACK_ROOT))}

            [rigs.imports.control]
            source = {json.dumps(str(control_pack))}

            [[rigs]]
            name = "beta"

            [rigs.imports.oversight-rig]
            source = {json.dumps(str(PACK_ROOT))}

            [rigs.imports.control]
            source = {json.dumps(str(control_pack))}
            """
        ),
        encoding="utf-8",
    )
    city.joinpath(".gc", "site.toml").write_text(
        textwrap.dedent(
            f"""\
            workspace_name = "order-scope-test"

            [[rig]]
            name = "alpha"
            path = {json.dumps(str(city / "alpha"))}

            [[rig]]
            name = "beta"
            path = {json.dumps(str(city / "beta"))}
            """
        ),
        encoding="utf-8",
    )
    control_pack.joinpath("pack.toml").write_text(
        textwrap.dedent(
            """\
            [pack]
            name = "control"
            schema = 2
            """
        ),
        encoding="utf-8",
    )
    control_pack.joinpath("orders", "rig-health.toml").write_text(
        textwrap.dedent(
            """\
            [order]
            exec = "true"
            trigger = "cooldown"
            interval = "5m"
            """
        ),
        encoding="utf-8",
    )
    return city


def order_identities(
    orders: list[dict[str, object]],
    name: str,
) -> list[tuple[str, object, str]]:
    return [
        (str(order["name"]), order.get("rig"), str(order["scoped_name"]))
        for order in orders
        if order.get("name") == name
    ]


def test_oversight_orders_preserve_declared_scope_across_rig_imports(
    tmp_path: Path,
    gc_test_bin: Path,
) -> None:
    city = write_order_scope_city(tmp_path)

    result = subprocess.run(
        [str(gc_test_bin), "--city", str(city), "order", "list", "--json"],
        cwd=city,
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    orders = json.loads(result.stdout)["orders"]
    patrol = order_identities(orders, "patrol-project-leads")
    assert len(patrol) == 1
    assert patrol == [
        ("patrol-project-leads", None, "patrol-project-leads"),
    ]
    assert order_identities(orders, "escalate-rollups") == [
        ("escalate-rollups", None, "escalate-rollups"),
        ("escalate-rollups", "alpha", "escalate-rollups:rig:alpha"),
        ("escalate-rollups", "beta", "escalate-rollups:rig:beta"),
    ]
    assert order_identities(orders, "rig-health") == [
        ("rig-health", "alpha", "rig-health:rig:alpha"),
        ("rig-health", "beta", "rig-health:rig:beta"),
    ]
