"""Exercise real Gas City import/discovery/dispatch in isolated file-store Rigs.

Set GC_TEST_BIN to the candidate gc binary. No supervisor, GitHub call, user
registry, or live Beads server is involved. Only the external Python runner is
replaced, so the actual shipped order, shell wrapper and gc routing are tested.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import unittest


GITHUB_PACK = Path(__file__).resolve().parents[1]


@unittest.skipUnless(os.environ.get("GC_TEST_BIN"), "set GC_TEST_BIN for native order tests")
class NativeWorkSyncOrderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.binary = Path(os.environ["GC_TEST_BIN"]).resolve()
        self.assertTrue(self.binary.is_file() and os.access(self.binary, os.X_OK))
        temp = tempfile.TemporaryDirectory(prefix="github-order-test-")
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name).resolve()
        self.city = self.root / "city"
        (self.city / ".gc").mkdir(parents=True)
        self.rigs = {name: self.root / name for name in ("alpha", "beta")}
        for path in self.rigs.values():
            (path / ".gc").mkdir(parents=True)
        # Same empty scope-local file-store layout as native gc init. This is
        # only the fixture provider, never a production work-sync store.
        self.city.joinpath(".gc", "file-beads-layout").write_text(
            "scope-local-v1\n", encoding="utf-8",
        )
        for path in (self.city, *self.rigs.values()):
            path.joinpath(".gc", "beads.json").write_text(
                '{"seq":0,"beads":[]}\n', encoding="utf-8",
            )
        self.city.joinpath("pack.toml").write_text(
            '[pack]\nname = "work-sync-fixture"\nschema = 2\n'
            '[imports.github]\nsource = ' + json.dumps(str(GITHUB_PACK)) + '\n',
            encoding="utf-8",
        )
        self.city.joinpath(".gc", "site.toml").write_text(
            'workspace_name = "work-sync-fixture"\nworkspace_prefix = "ws"\n' + "".join(
                f'[[rig]]\nname = "{name}"\npath = {json.dumps(str(path))}\n'
                for name, path in self.rigs.items()
            ), encoding="utf-8",
        )
        # Allowlist only non-secret process settings. Never inherit GC/BEADS
        # server routing, GitHub tokens, resolver commands, or live API URLs.
        self.env = {
            "PATH": os.defpath,
            "GC_HOME": str(self.root / "gc-home"),
            "GC_CITY": str(self.city),
            "GC_CITY_PATH": str(self.city),
            "GC_CITY_ROOT": str(self.city),
            "GC_BEADS": "file",
            "XDG_CONFIG_HOME": str(self.root / "config"),
            "XDG_CACHE_HOME": str(self.root / "cache"),
            "XDG_DATA_HOME": str(self.root / "data"),
            "XDG_STATE_HOME": str(self.root / "state"),
        }

    def configure(self, *, rig_imports: bool) -> None:
        content = '[workspace]\n[beads]\nprovider = "file"\n'
        for name in self.rigs:
            content += f'[[rigs]]\nname = "{name}"\nprefix = "{name}"\n'
            if rig_imports:
                content += '[rigs.imports.github_work_sync]\nsource = '
                content += json.dumps(str(GITHUB_PACK / "work-sync")) + '\n'
        self.city.joinpath("city.toml").write_text(content, encoding="utf-8")

    def gc(self, *args: str, expected_code: int = 0) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            [str(self.binary), *args], cwd=self.city, env=self.env,
            text=True, capture_output=True, timeout=45,
        )
        self.assertEqual(result.returncode, expected_code, result.stdout + result.stderr)
        return result

    def work_sync_orders(self) -> list[dict]:
        result = json.loads(self.gc("order", "list", "--json").stdout)
        return [order for order in result["orders"] if order["name"] == "work-sync"]

    def test_city_ingress_does_not_register_a_rigless_reconciler(self) -> None:
        self.configure(rig_imports=False)
        self.assertEqual(self.work_sync_orders(), [])

    def test_each_opted_in_rig_gets_exactly_one_order(self) -> None:
        self.configure(rig_imports=True)
        orders = self.work_sync_orders()
        self.assertEqual(len(orders), 2, orders)
        self.assertEqual(
            {order["scoped_name"] for order in orders},
            {f"work-sync:rig:{name}" for name in self.rigs},
        )
        for order in orders:
            self.assertEqual(order["type"], "exec")
            self.assertEqual(order["trigger"], "cooldown")
            self.assertEqual(order["interval"], "5m")

    def test_dispatch_routes_and_tracks_each_rig_independently(self) -> None:
        self.configure(rig_imports=True)
        runner_dir = self.root / "runner"
        runner_dir.mkdir()
        runner = runner_dir / "python3"
        script = runner_dir / "runner.py"
        script.write_text(
            "import json, os, sys\n"
            "print(json.dumps({'argv': sys.argv[1:], 'cwd': os.getcwd(), 'env': {"
            "key: os.environ.get(key) for key in "
            "['GC_RIG', 'GC_RIG_ROOT', 'GC_CITY_PATH', 'GC_STORE_ROOT', 'GC_STORE_SCOPE', 'GC_SERVICE_STATE_ROOT']}}))\n",
            encoding="utf-8",
        )
        runner.write_text(
            "#!/bin/sh\nexec " + shlex.quote(sys.executable) + " "
            + shlex.quote(str(script)) + ' "$@"\n', encoding="utf-8",
        )
        runner.chmod(0o755)
        self.env["PATH"] = str(runner_dir) + os.pathsep + os.defpath
        self.env["GC_GITHUB_WORK_SYNC_DRY_RUN"] = "1"
        ingress_state = str(self.root / "other-city" / ".gc/services/github")
        self.env["GC_SERVICE_STATE_ROOT"] = ingress_state
        completed: set[str] = set()
        for name, rig in self.rigs.items():
            with self.subTest(rig=name):
                result = self.gc("order", "run", "work-sync", "--rig", name)
                output = json.loads(result.stdout.splitlines()[0])
                self.assertEqual(output["argv"], [str(GITHUB_PACK / "scripts/github_work_sync.py"), "--dry-run"])
                self.assertEqual(output["cwd"], str(rig))
                self.assertEqual(Path(output["env"]["GC_CITY_PATH"]).resolve(), self.city)
                output["env"]["GC_CITY_PATH"] = str(self.city)
                self.assertEqual(output["env"], {
                    "GC_RIG": name, "GC_RIG_ROOT": str(rig),
                    "GC_CITY_PATH": str(self.city), "GC_STORE_ROOT": str(rig),
                    "GC_STORE_SCOPE": "rig",
                    "GC_SERVICE_STATE_ROOT": ingress_state,
                })
                completed.add(name)
                for other in self.rigs:
                    history = json.loads(self.gc(
                        "order", "history", "work-sync", "--rig", other, "--json",
                    ).stdout)
                    self.assertEqual(history["summary"]["total"], int(other in completed))
                    for entry in history["entries"]:
                        self.assertEqual(entry["order"], "work-sync")
                        self.assertEqual(entry["rig"], other)
                due = json.loads(self.gc("order", "check", "--json").stdout)
                scoped = {row["rig"]: row["due"] for row in due["orders"]
                          if row["name"] == "work-sync"}
                self.assertEqual(scoped, {other: other not in completed for other in self.rigs})
        self.gc("order", "run", "work-sync", "--rig", "unknown", expected_code=1)


if __name__ == "__main__":
    unittest.main()
