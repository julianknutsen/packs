"""The budget refusal in mol-pr-review step 1.

These tests EXECUTE the guard's shell with stubbed `gh`/`gc` rather than
asserting on the formula's text. A text assertion here would be worth very
little: this pack already carries a test that pinned a deprecated value by
reading the file back to itself and passed for months while every importing
city threw a doctor warning. The property that matters is behavioural -- does
the guard refuse the big PR, pass the small one, and refuse to guess when the
size cannot be read -- and only running it can answer that.
"""
from __future__ import annotations

import pathlib
import re
import subprocess
import tempfile
import tomllib
import unittest

PACK_ROOT = pathlib.Path(__file__).resolve().parents[1]
FORMULA = PACK_ROOT / "formulas" / "mol-pr-review.formula.toml"

# Real sizes, measured 2026-08-22 on gastownhall/gascity:
#   gh pr list --repo gastownhall/gascity --state open --limit 200 \
#     --json number,additions,deletions,changedFiles
PR_TOO_BIG = (113042, 432)   # #5489, the largest open PR
PR_REPORTED = (47734, 372)   # #5255, the PR that triggered the drain report
PR_TYPICAL = (248, 4)        # the median open PR


def _guard_script() -> str:
    data = tomllib.loads(FORMULA.read_text(encoding="utf-8"))
    intake = data["steps"][0]["description"]
    m = re.search(r"### Budget refusal.*?```bash\n(.*?)```", intake, re.S)
    assert m, "the Budget refusal block vanished from step 1"
    return m.group(1)


def default_max_diff_lines() -> str:
    """Read the shipped default rather than restating it.

    A hardcoded 5000 here would keep passing after someone changed the pack's
    default, testing a number this file invented instead of the one installers
    actually get.
    """
    data = tomllib.loads(FORMULA.read_text(encoding="utf-8"))
    return str(data["vars"]["max_diff_lines"]["default"])


def run_guard(lines, files, max_lines=None, size_readable=True, gh_fails=False):
    """Run the guard with stubbed gh/gc.

    Returns (rc, stdout, gc-calls, report-text, gh-calls). The gh stub logs
    every invocation and refuses anything it was not expected to be asked,
    so "no diff was fetched" can be asserted rather than inferred from where
    a heading sits in the source.
    """
    max_lines = default_max_diff_lines() if max_lines is None else max_lines
    with tempfile.TemporaryDirectory() as td:
        tmp = pathlib.Path(td)
        bin_dir = tmp / "bin"
        bin_dir.mkdir()
        size = f"{lines}" if size_readable else "null"
        fail = "exit 4\n" if gh_fails else ""
        (bin_dir / "gh").write_text(
            "#!/usr/bin/env bash\n"
            f'echo "$*" >> "{tmp}/gh.log"\n'
            f"{fail}"
            "case \"$*\" in\n"
            f'  *changedFiles*) echo {files};;\n'
            f'  *additions*)    echo {size};;\n'
            "  *' pr diff'*|*'pr diff '*) echo UNEXPECTED_DIFF_FETCH; exit 9;;\n"
            "  *) echo UNEXPECTED_GH_CALL; exit 9;;\n"
            "esac\n"
        )
        # `gc` records what it was asked to do so the test can assert the
        # OUTCOME, not merely that the script exited.
        (bin_dir / "gc").write_text(
            "#!/usr/bin/env bash\n"
            f'echo "$*" >> "{tmp}/gc.log"\n'
        )
        for f in ("gh", "gc"):
            (bin_dir / f).chmod(0o755)

        script = _guard_script()
        script = (script.replace("<N>", "123").replace("<repo>", "o/r")
                        .replace("{{max_diff_lines}}", max_lines))
        # Supply ONLY what an earlier part of step 1 genuinely sets. Do NOT
        # define REPORT_PATH here: an earlier version of this harness did, and
        # it concealed a real defect -- the guard wrote to a variable the
        # formula does not assign until after the diff fetch, 2,600 characters
        # further down. A fixture that supplies state the real formula lacks
        # tests the fixture, not the formula.
        script = "ROOT_ID=root-1\nset -u\n" + script
        # A later `exit 0` must be the guard's, so mark the fall-through.
        script += '\necho REACHED_DIFF_FETCH\n'

        sh = tmp / "guard.sh"
        sh.write_text(script)
        proc = subprocess.run(
            ["bash", str(sh)], capture_output=True, text=True,
            env={"PATH": f"{bin_dir}:/usr/bin:/bin", "HOME": str(tmp)}, cwd=td,
        )
        log = (tmp / "gc.log")
        ghlog = (tmp / "gh.log")
        report = (tmp / ".gc/pr-pipeline/reviews/pr-123.md")
        return (proc.returncode, proc.stdout,
                log.read_text() if log.exists() else "",
                report.read_text() if report.exists() else "",
                ghlog.read_text() if ghlog.exists() else "")


class BudgetRefusalTests(unittest.TestCase):
    def test_the_largest_open_pr_is_refused_before_the_diff_is_read(self):
        rc, out, gclog, report, ghlog = run_guard(*PR_TOO_BIG)
        self.assertEqual(rc, 0, "a refusal is a decision, not an error")
        self.assertNotIn("REACHED_DIFF_FETCH", out,
                         "the guard let a 113k-line PR through to the diff fetch")
        self.assertIn("too_large", gclog)
        self.assertIn("do not retry unchanged", gclog)

    def test_the_pr_that_triggered_the_report_is_refused(self):
        rc, out, gclog, _, ghlog = run_guard(*PR_REPORTED)
        self.assertNotIn("REACHED_DIFF_FETCH", out)
        self.assertIn("too_large", gclog)

    def test_a_typical_pr_still_gets_reviewed(self):
        """The green rail. Without this, refusing everything would pass."""
        rc, out, gclog, _, ghlog = run_guard(*PR_TYPICAL)
        self.assertIn("REACHED_DIFF_FETCH", out,
                      "the guard blocked a median-sized PR -- it is over-firing")
        self.assertNotIn("too_large", gclog)

    def test_refusal_is_never_recorded_as_an_approval(self):
        """A refused review must not look like a clean one downstream."""
        _, _, gclog, report, _ = run_guard(*PR_TOO_BIG)
        self.assertNotIn("approve", gclog)
        self.assertIn("Not reviewed", report)
        self.assertIn("no model budget was spent", report)

    def test_an_unreadable_size_refuses_rather_than_reading_as_zero(self):
        """Zero is the reassuring answer and would wave the PR through."""
        rc, out, gclog, _, ghlog = run_guard(0, 0, size_readable=False)
        self.assertNotIn("REACHED_DIFF_FETCH", out)
        self.assertIn("unreadable", gclog)

    def test_zero_disables_the_guard_deliberately(self):
        _, out, gclog, _, _ = run_guard(*PR_TOO_BIG, max_lines="0")
        self.assertIn("REACHED_DIFF_FETCH", out)
        self.assertNotIn("too_large", gclog)

    def test_no_diff_is_fetched_when_the_pr_is_refused(self):
        """The claim is that nothing was spent, so prove nothing was read.

        Textual ordering in the formula is not this property: it says where the
        heading sits, not what ran. The gh stub exits non-zero on any `pr diff`
        and logs every call, so this asserts on the invocations themselves.
        """
        _, _, _, _, ghlog = run_guard(*PR_TOO_BIG)
        self.assertNotIn("pr diff", ghlog)
        self.assertNotIn("UNEXPECTED", ghlog)
        self.assertTrue(ghlog.strip(), "the guard never asked gh anything")

    def test_a_pr_exactly_at_the_limit_is_reviewed(self):
        """`-gt`, not `-ge`: the budget is a ceiling that may be reached."""
        limit = int(default_max_diff_lines())
        _, out, gclog, _, _ = run_guard(limit, 10)
        self.assertIn("REACHED_DIFF_FETCH", out)
        self.assertNotIn("too_large", gclog)

    def test_one_line_over_the_limit_is_refused(self):
        limit = int(default_max_diff_lines())
        _, out, gclog, _, _ = run_guard(limit + 1, 10)
        self.assertNotIn("REACHED_DIFF_FETCH", out)
        self.assertIn("too_large", gclog)

    def test_a_malformed_limit_refuses_instead_of_failing_open(self):
        """The earlier draft let a non-numeric limit error the test and pass
        the PR through -- a guard that fails open on bad config is worse than
        no guard, because it still reads as protection."""
        for bad in ("", "abc", "5000; rm -rf /", "-1"):
            with self.subTest(limit=bad):
                _, out, gclog, _, _ = run_guard(*PR_TOO_BIG, max_lines=bad)
                self.assertNotIn("REACHED_DIFF_FETCH", out)
                self.assertIn("too_large", gclog)

    def test_a_failing_gh_refuses_rather_than_guessing(self):
        _, out, gclog, _, _ = run_guard(*PR_TYPICAL, gh_fails=True)
        self.assertNotIn("REACHED_DIFF_FETCH", out)
        self.assertIn("too_large", gclog)

    def test_the_guard_precedes_the_diff_fetch_in_the_source(self):
        """Ordering is the whole point: a guard after the spend saves nothing."""
        intake = tomllib.loads(FORMULA.read_text(encoding="utf-8"))["steps"][0]
        text = intake["description"]
        self.assertLess(text.index("### Budget refusal"), text.index("gh pr diff"),
                        "the budget check must run before the diff is fetched")


RUN_SH = PACK_ROOT / "commands" / "pr" / "review" / "run.sh"


def _recheck_script() -> str:
    data = tomllib.loads(FORMULA.read_text(encoding="utf-8"))
    intake = data["steps"][0]["description"]
    m = re.search(r"DIFF_LINES=.*?```", intake, re.S)
    assert m, "the post-fetch recheck vanished from step 1"
    return m.group(0)[: -3]


class PostFetchRecheckTests(unittest.TestCase):
    """The size check and the diff fetch are two reads of a moving target.

    A commit pushed between them turns a passing PR into an oversized one, and
    the fetched diff is the thing that actually costs money. So the fetched
    bytes are measured before anything reads them.
    """

    def _run(self, diff_lines, max_lines=None):
        max_lines = default_max_diff_lines() if max_lines is None else max_lines
        with tempfile.TemporaryDirectory() as td:
            tmp = pathlib.Path(td)
            bin_dir = tmp / "bin"
            bin_dir.mkdir()
            (bin_dir / "gc").write_text(
                "#!/usr/bin/env bash\n" + f'echo "$*" >> "{tmp}/gc.log"\n')
            (bin_dir / "gc").chmod(0o755)
            diff = tmp / "pr-123.diff"
            diff.write_text("x\n" * diff_lines)
            script = (_recheck_script().replace("<N>", "123")
                      .replace("{{max_diff_lines}}", max_lines)
                      .replace("/tmp/pr-123.diff", str(diff)))
            script = "ROOT_ID=root-1\nset -u\n" + script + "\necho REACHED_REVIEW\n"
            sh = tmp / "recheck.sh"
            sh.write_text(script)
            proc = subprocess.run(
                ["bash", str(sh)], capture_output=True, text=True,
                env={"PATH": f"{bin_dir}:/usr/bin:/bin"}, cwd=td)
            log = tmp / "gc.log"
            return (proc.stdout, log.read_text() if log.exists() else "",
                    diff.exists())

    def test_a_diff_that_grew_past_the_budget_is_refused_after_the_fetch(self):
        out, gclog, diff_kept = self._run(int(default_max_diff_lines()) + 1)
        self.assertNotIn("REACHED_REVIEW", out)
        self.assertIn("too_large", gclog)
        self.assertIn("post_fetch", gclog)
        self.assertFalse(diff_kept, "the oversized diff was left on disk")

    def test_a_diff_within_the_budget_proceeds(self):
        out, gclog, _ = self._run(10)
        self.assertIn("REACHED_REVIEW", out)
        self.assertNotIn("too_large", gclog)


class CommandPreflightTests(unittest.TestCase):
    """run.sh is the only genuinely pre-spend point.

    The formula's copy runs inside the session whose cost it is avoiding, so
    the command has to refuse before `gc sling` ever starts an agent.
    """

    def _run(self, args, pr_lines=248, gh_fails=False):
        with tempfile.TemporaryDirectory() as td:
            tmp = pathlib.Path(td)
            bin_dir = tmp / "bin"
            bin_dir.mkdir()
            (bin_dir / "gh").write_text(
                "#!/usr/bin/env bash\n"
                f'echo "$*" >> "{tmp}/gh.log"\n'
                + ("exit 4\n" if gh_fails else f'echo "{pr_lines} 4"\n'))
            (bin_dir / "gc").write_text(
                "#!/usr/bin/env bash\n" + f'echo "$*" >> "{tmp}/gc.log"\n')
            for f in ("gh", "gc"):
                (bin_dir / f).chmod(0o755)
            proc = subprocess.run(
                ["sh", str(RUN_SH)] + args, capture_output=True, text=True,
                env={"PATH": f"{bin_dir}:/usr/bin:/bin",
                     "GC_PACK_DIR": str(PACK_ROOT), "GC_RIG": "r"}, cwd=td)
            log = tmp / "gc.log"
            return proc, log.read_text() if log.exists() else ""

    def test_an_oversized_pr_is_refused_before_the_sling(self):
        proc, gclog = self._run(["123"], pr_lines=113042)
        self.assertEqual(proc.returncode, 3,
                         "a budget refusal needs its own non-retryable code")
        self.assertNotIn("sling", gclog, "an agent was started anyway")
        self.assertIn("no model", proc.stderr)

    def test_a_typical_pr_is_slung(self):
        proc, gclog = self._run(["123"])
        self.assertIn("sling", gclog)
        self.assertIn("max_diff_lines=", gclog,
                      "the limit must reach the formula too")

    def test_an_unreadable_size_refuses_rather_than_slinging(self):
        proc, gclog = self._run(["123"], gh_fails=True)
        self.assertEqual(proc.returncode, 1)
        self.assertNotIn("sling", gclog)

    def test_a_malformed_limit_refuses_rather_than_failing_open(self):
        proc, gclog = self._run(["123", "--max-diff-lines", "abc"])
        self.assertEqual(proc.returncode, 2)
        self.assertNotIn("sling", gclog)

    def test_zero_disables_the_preflight_deliberately(self):
        proc, gclog = self._run(["123", "--max-diff-lines=0"], pr_lines=113042)
        self.assertIn("sling", gclog)

    def test_the_command_default_matches_the_formula_default(self):
        """Two copies of a number drift; this is what notices."""
        text = RUN_SH.read_text(encoding="utf-8")
        m = re.search(r'MAX_DIFF_LINES="\$\{GC_PR_MAX_DIFF_LINES:-(\d+)\}"', text)
        self.assertIsNotNone(m, "the command's default is no longer readable")
        self.assertEqual(m.group(1), default_max_diff_lines())


if __name__ == "__main__":
    unittest.main()
