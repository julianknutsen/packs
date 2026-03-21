from __future__ import annotations

import io
import json
import pathlib
import tempfile
import unittest
from contextlib import redirect_stdout

import os
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "scripts"))

import discord_intake_common as common
import discord_intake_release_workflow as release_script


class DiscordReleaseWorkflowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self._old_environ = os.environ.copy()
        os.environ["GC_CITY_ROOT"] = self.tempdir.name

    def tearDown(self) -> None:
        os.environ.clear()
        os.environ.update(self._old_environ)

    def test_release_workflow_by_request_id_only_releases_matching_link(self) -> None:
        workflow_key = "dc:guild:1:conversation:22:fix"
        common.save_request({"request_id": "dc-old", "workflow_key": workflow_key})
        common.save_request({"request_id": "dc-new", "workflow_key": workflow_key})
        common.save_workflow_link(workflow_key, "dc-new")

        stdout = io.StringIO()
        with redirect_stdout(stdout):
            code = release_script.main(["--request-id", "dc-old"])

        self.assertEqual(code, 1)
        payload = json.loads(stdout.getvalue())
        self.assertFalse(payload["released"])
        current = common.load_workflow_link(workflow_key)
        self.assertIsNotNone(current)
        assert current is not None
        self.assertEqual(current["request_id"], "dc-new")

    def test_release_workflow_by_request_id_releases_matching_link(self) -> None:
        workflow_key = "dc:guild:1:conversation:23:fix"
        common.save_request({"request_id": "dc-23", "workflow_key": workflow_key})
        common.save_workflow_link(workflow_key, "dc-23")

        stdout = io.StringIO()
        with redirect_stdout(stdout):
            code = release_script.main(["--request-id", "dc-23"])

        self.assertEqual(code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertTrue(payload["released"])
        self.assertEqual(payload["request_id"], "dc-23")
        self.assertIsNone(common.load_workflow_link(workflow_key))
        request = common.load_request("dc-23")
        self.assertIsNotNone(request)
        assert request is not None
        self.assertIn("workflow_released_at", request)

    def test_release_workflow_by_conversation_releases_rig_scoped_locks(self) -> None:
        workflow_key = "dc:guild:1:conversation:24:fix:rig:mission-control"
        common.save_workflow_link(workflow_key, "dc-rig-24")

        stdout = io.StringIO()
        with redirect_stdout(stdout):
            code = release_script.main(["1", "24"])

        self.assertEqual(code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertTrue(payload["released"])
        self.assertEqual(payload["released_keys"], [workflow_key])
        self.assertIsNone(common.load_workflow_link(workflow_key))

    def test_release_workflow_by_conversation_skips_invalid_json_files(self) -> None:
        workflow_key = "dc:guild:1:conversation:25:fix:rig:mission-control"
        common.save_workflow_link(workflow_key, "dc-rig-25")
        pathlib.Path(common.workflows_dir(), "broken.json").write_text("{not-json", encoding="utf-8")

        stdout = io.StringIO()
        with redirect_stdout(stdout):
            code = release_script.main(["1", "25"])

        self.assertEqual(code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertTrue(payload["released"])
        self.assertEqual(payload["released_keys"], [workflow_key])


if __name__ == "__main__":
    unittest.main()
