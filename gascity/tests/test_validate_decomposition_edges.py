from __future__ import annotations

import contextlib
import io
import json
import os
import pathlib
import stat
import sys
import tempfile
import textwrap
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "assets" / "scripts"))

import validate_build_artifact as build_artifact
import validate_decomposition_edges as edges

FAKE_GC_SOURCE = textwrap.dedent(
    """\
    #!/usr/bin/env python3
    import json
    import os
    import sys

    with open(os.environ["FAKE_GC_DEPS"], encoding="utf-8") as handle:
        deps_by_bead = json.load(handle)

    if sys.argv[1:4] != ["bd", "dep", "list"] or "--json" not in sys.argv:
        sys.stderr.write("fake gc: unsupported invocation: %r\\n" % (sys.argv[1:],))
        raise SystemExit(2)

    bead = sys.argv[4]
    if bead not in deps_by_bead:
        sys.stderr.write("no issue found matching %s\\n" % bead)
        raise SystemExit(1)
    print(json.dumps(deps_by_bead[bead]))
    """
)


def dependency_table(rows: list[tuple[str, str, str]]) -> str:
    lines = [
        "## Work Items",
        "",
        "| ID | Bead | Depends On |",
        "| --- | --- | --- |",
    ]
    lines.extend(f"| {item_id} | {bead} | {depends} |" for item_id, bead, depends in rows)
    return "\n".join(lines) + "\n"


class EdgeCheckTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name)

    def write_artifact(self, body: str) -> pathlib.Path:
        path = self.root / "decomposition.md"
        path.write_text(body, encoding="utf-8")
        return path

    def write_fake_gc(self, deps_by_bead: dict[str, list[dict[str, str]]]) -> pathlib.Path:
        deps_path = self.root / "deps.json"
        deps_path.write_text(json.dumps(deps_by_bead), encoding="utf-8")
        gc_path = self.root / "gc"
        gc_path.write_text(FAKE_GC_SOURCE, encoding="utf-8")
        gc_path.chmod(gc_path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        os.environ["FAKE_GC_DEPS"] = str(deps_path)
        self.addCleanup(os.environ.pop, "FAKE_GC_DEPS", None)
        return gc_path

    def run_main(self, argv: list[str]) -> tuple[int, str, str]:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            status = edges.main(argv)
        return status, stdout.getvalue(), stderr.getvalue()

    def test_skips_artifact_without_dependency_table(self) -> None:
        path = self.write_artifact("## Work Items\n\nProse only, no table.\n")
        status, stdout, stderr = self.run_main(["--path", str(path)])
        self.assertEqual(status, 0)
        self.assertIn("skipping edge verification", stdout)
        self.assertEqual(stderr, "")

    def test_rejects_forward_reference_in_declaration(self) -> None:
        path = self.write_artifact(
            dependency_table(
                [
                    ("WI-1", "gc-aaa111", "WI-2"),
                    ("WI-2", "gc-bbb222", "-"),
                ]
            )
        )
        status, _, stderr = self.run_main(["--path", str(path), "--skip-live"])
        self.assertEqual(status, 1)
        self.assertIn("declared later", stderr)
        self.assertIn("execution order", stderr)

    def test_rejects_status_column_in_dependency_table(self) -> None:
        path = self.write_artifact(
            "## Work Items\n\n"
            "| ID | Bead | Depends On | Status |\n"
            "| --- | --- | --- | --- |\n"
            "| WI-1 | gc-aaa111 | - | open |\n"
        )
        status, _, stderr = self.run_main(["--path", str(path), "--skip-live"])
        self.assertEqual(status, 1)
        self.assertIn("must not carry a Status column", stderr)

    def test_correct_chain_passes_live_verification(self) -> None:
        path = self.write_artifact(
            dependency_table(
                [
                    ("WI-1", "gc-aaa111", "-"),
                    ("WI-2", "gc-bbb222", "WI-1"),
                    ("WI-3", "gc-ccc333", "WI-2"),
                ]
            )
        )
        gc_path = self.write_fake_gc(
            {
                "gc-aaa111": [],
                "gc-bbb222": [{"depends_on_id": "gc-aaa111", "type": "blocks"}],
                "gc-ccc333": [{"depends_on_id": "gc-bbb222", "type": "blocks"}],
            }
        )
        status, stdout, stderr = self.run_main(["--path", str(path), "--gc-bin", str(gc_path)])
        self.assertEqual(status, 0, stderr)
        self.assertIn("3 work items, 2 declared edges", stdout)

    def test_inverted_chain_fails_with_repair_commands(self) -> None:
        # The observed ac-9egumo shape: the declared chain WI-1 -> WI-2 -> WI-3
        # was wired backwards (each EARLIER item depends on its successor).
        path = self.write_artifact(
            dependency_table(
                [
                    ("WI-1", "gc-aaa111", "-"),
                    ("WI-2", "gc-bbb222", "WI-1"),
                    ("WI-3", "gc-ccc333", "WI-2"),
                ]
            )
        )
        gc_path = self.write_fake_gc(
            {
                "gc-aaa111": [{"depends_on_id": "gc-bbb222", "type": "blocks"}],
                "gc-bbb222": [{"depends_on_id": "gc-ccc333", "type": "blocks"}],
                "gc-ccc333": [],
            }
        )
        status, _, stderr = self.run_main(["--path", str(path), "--gc-bin", str(gc_path)])
        self.assertEqual(status, 1)
        self.assertIn("missing declared edge", stderr)
        self.assertIn("inverted edge", stderr)
        self.assertIn("gc bd dep add gc-bbb222 gc-aaa111", stderr)
        self.assertIn("gc bd dep remove gc-aaa111 gc-bbb222", stderr)

    def test_missing_bead_fails(self) -> None:
        path = self.write_artifact(
            dependency_table(
                [
                    ("WI-1", "gc-aaa111", "-"),
                    ("WI-2", "gc-missing", "WI-1"),
                ]
            )
        )
        gc_path = self.write_fake_gc({"gc-aaa111": []})
        status, _, stderr = self.run_main(["--path", str(path), "--gc-bin", str(gc_path)])
        self.assertEqual(status, 1)
        self.assertIn("gc bd dep list gc-missing failed", stderr)

    def test_non_blocking_dependency_types_are_ignored(self) -> None:
        path = self.write_artifact(
            dependency_table(
                [
                    ("WI-1", "gc-aaa111", "-"),
                    ("WI-2", "gc-bbb222", "WI-1"),
                ]
            )
        )
        gc_path = self.write_fake_gc(
            {
                # The declared edge exists; the earlier item also carries a
                # non-blocking edge onto the later one, which must not count
                # as an inversion.
                "gc-aaa111": [{"depends_on_id": "gc-bbb222", "type": "related"}],
                "gc-bbb222": [{"depends_on_id": "gc-aaa111", "type": "waits-for"}],
            }
        )
        status, stdout, stderr = self.run_main(["--path", str(path), "--gc-bin", str(gc_path)])
        self.assertEqual(status, 0, stderr)
        self.assertIn("2 work items, 1 declared edges", stdout)

    def test_dependency_table_does_not_pollute_coverage_matrix(self) -> None:
        body = (
            "## Coverage\n\n"
            "| ID | Status |\n"
            "| --- | --- |\n"
            "| REQ-001 | covered |\n\n"
            + dependency_table(
                [
                    ("WI-1", "gc-aaa111", "-"),
                    ("WI-2", "gc-bbb222", "WI-1"),
                ]
            )
        )
        coverage = build_artifact.parse_markdown_coverage(body)
        self.assertEqual(coverage, {"REQ-001": "covered"})


if __name__ == "__main__":
    unittest.main()
