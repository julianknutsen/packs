from __future__ import annotations

import pathlib
import subprocess
import tempfile
import unittest

import os


class DiscordDoctorScriptTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self._old_environ = os.environ.copy()
        os.environ["GC_CITY_ROOT"] = self.tempdir.name
        self.script = pathlib.Path(__file__).resolve().parents[1] / "doctor" / "check-legacy-pack-conflict.sh"

    def tearDown(self) -> None:
        os.environ.clear()
        os.environ.update(self._old_environ)

    def test_legacy_pack_conflict_check_passes_without_legacy_state(self) -> None:
        result = subprocess.run([str(self.script)], capture_output=True, text=True, check=False)

        self.assertEqual(result.returncode, 0)
        self.assertIn("no legacy discord-intake state detected", result.stdout)

    def test_legacy_pack_conflict_check_fails_when_legacy_state_exists(self) -> None:
        legacy_config = pathlib.Path(self.tempdir.name, ".gc", "services", "discord-intake", "data", "config.json")
        legacy_config.parent.mkdir(parents=True, exist_ok=True)
        legacy_config.write_text("{}", encoding="utf-8")

        result = subprocess.run([str(self.script)], capture_output=True, text=True, check=False)

        self.assertEqual(result.returncode, 2)
        self.assertIn("legacy discord-intake state detected", result.stdout)


if __name__ == "__main__":
    unittest.main()
