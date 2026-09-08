"""Run the shipped default-branch resolution against real git repositories.

Four assets resolve a repository's default branch: the `prepare-worktree`
workflow step in the `gascity` pack, and the guard that refuses to ship from
the default branch in `contributing` and twice in `pr-pipeline`. All four used
to do it with `git remote show origin`, which contacts the remote on every
invocation.

This test does not assert that the files contain a particular string. It pulls
the shell out of the shipped asset and runs it under `sh` against git
repositories built on disk, so a passing result is a statement about what the
shipped code does, not about how it is written.

The repositories here use a default branch named `trunk` on purpose. `main` is
the answer a broken resolution reaches by accident, through the hardcoded
fallback, so a fixture on `main` cannot tell a working resolution from a
failing one.
"""

from __future__ import annotations

import re
import shutil
import subprocess
import tempfile
import textwrap
import tomllib
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

WORKFLOW_STEP = REPO_ROOT / "gascity/assets/workflows/do-work/prepare-worktree.md"
FORMULAS = (
    REPO_ROOT / "contributing/formulas/mol-contributing-fine-tune.formula.toml",
    REPO_ROOT / "pr-pipeline/formulas/mol-pr-ship.formula.toml",
    REPO_ROOT / "pr-pipeline/formulas/mol-pr-from-issue.formula.toml",
)

# Each asset is matched by two patterns: the shape it carries now, and the
# shape it carried before. The legacy alternative is what makes this test an
# A/B rather than a presence check -- reverting the fix must make these cases
# fail on the WRONG ANSWER at the shipped site, not on "the block is missing".
#
# The current block starts at the local read and ends at the last line that can
# assign the variable. Anchoring on both ends means an edit that moves either
# one fails this test loudly rather than silently narrowing what gets run.
FORMULA_BLOCK = (
    re.compile(
        r"^DEFAULT=\$\(git symbolic-ref.*?^\[ -n \"\$DEFAULT\" \] \|\| DEFAULT=main$",
        re.MULTILINE | re.DOTALL,
    ),
    re.compile(r"^DEFAULT=\$\(git remote show origin.*$", re.MULTILINE),
)
# The fence sits inside a nested list item, so both it and its contents are
# indented. Capture the body and dedent it rather than pinning a column count.
# The legacy form was an inline backticked command in the same list item.
WORKFLOW_BLOCK = (
    re.compile(r"```sh\n(\s*DEFAULT_BRANCH=.*?)\n[ \t]*```", re.DOTALL),
    re.compile(r"`(DEFAULT_BRANCH=\$\(git remote show origin[^`]*)`"),
)

# What the four assets replaced. Kept here as an executable control rather than
# a comment: the `|| echo "main"` reads as a fallback and cannot fire, because
# sed exits 0 on empty input, so a failed `git remote show` yields an empty
# string rather than "main".
SUPERSEDED = (
    """DEFAULT=$(git remote show origin 2>/dev/null """
    """| sed -n 's/.*HEAD branch: //p' || echo "main")"""
)


def git(*args: str, cwd: Path) -> None:
    subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True)


def run_snippet(snippet: str, variable: str, cwd: Path) -> str:
    """Execute the snippet in `cwd` and return what it assigned."""
    proc = subprocess.run(
        ["sh", "-c", f'{snippet}\nprintf "%s" "${variable}"'],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise AssertionError(
            f"snippet exited {proc.returncode}: {proc.stderr[:1000]}"
        )
    return proc.stdout.strip()


def extract(path: Path, patterns: tuple[re.Pattern[str], ...]) -> str:
    if path.suffix == ".toml":
        # Read the rendered command, not the TOML source: the source carries
        # escaping that the shell never sees.
        haystack = "\n".join(
            value
            for value in walk_strings(tomllib.loads(path.read_text(encoding="utf-8")))
        )
    else:
        haystack = path.read_text(encoding="utf-8")
    for pattern in patterns:
        match = pattern.search(haystack)
        if match:
            return textwrap.dedent(
                match.group(1) if match.groups() else match.group(0)
            )
    raise AssertionError(
        f"no default-branch resolution found in {path.relative_to(REPO_ROOT)}, "
        "in either the current or the superseded shape. If the block moved, "
        "update the anchors here; do not delete the test."
    )


def walk_strings(node: object) -> list[str]:
    if isinstance(node, str):
        return [node]
    if isinstance(node, dict):
        return [s for value in node.values() for s in walk_strings(value)]
    if isinstance(node, list):
        return [s for item in node for s in walk_strings(item)]
    return []


class DefaultBranchResolutionTest(unittest.TestCase):
    """Each case builds an origin and a clone, then breaks one thing."""

    def setUp(self) -> None:
        if not shutil.which("git"):
            self.skipTest("git is not available")
        self.tmp = Path(tempfile.mkdtemp(prefix="default-branch-"))
        self.addCleanup(shutil.rmtree, self.tmp, True)

        self.origin = self.tmp / "origin.git"
        seed = self.tmp / "seed"
        seed.mkdir()
        git("init", "--initial-branch", "trunk", cwd=seed)
        git("config", "user.email", "test@example.invalid", cwd=seed)
        git("config", "user.name", "test", cwd=seed)
        (seed / "README").write_text("seed\n", encoding="utf-8")
        git("add", "README", cwd=seed)
        git("commit", "-m", "seed", cwd=seed)
        git("clone", "--bare", str(seed), str(self.origin), cwd=self.tmp)

        self.clone = self.tmp / "clone"
        git("clone", str(self.origin), str(self.clone), cwd=self.tmp)

    def sever_remote(self) -> None:
        """Make origin unreachable without touching the clone's refs."""
        (self.origin).rename(self.tmp / "origin.git.moved")

    def drop_origin_head(self) -> None:
        git("remote", "set-head", "origin", "--delete", cwd=self.clone)

    # --- the shipped formulas -------------------------------------------

    def formula_snippets(self) -> list[tuple[Path, str]]:
        return [(path, extract(path, FORMULA_BLOCK)) for path in FORMULAS]

    def test_formulas_resolve_the_default_branch_without_the_remote(self) -> None:
        """The point of the change: no network call in the common case."""
        self.sever_remote()
        for path, snippet in self.formula_snippets():
            with self.subTest(formula=path.name):
                self.assertEqual(
                    run_snippet(snippet, "DEFAULT", self.clone),
                    "trunk",
                    "resolution must come from refs/remotes/origin/HEAD, "
                    "which is local",
                )

    def test_formulas_refresh_the_ref_when_a_checkout_lacks_it(self) -> None:
        """`git init` plus `git fetch` never writes refs/remotes/origin/HEAD.

        That is how actions/checkout builds a workspace, so this branch runs in
        CI rather than being a defensive nicety.
        """
        self.drop_origin_head()
        for path, snippet in self.formula_snippets():
            with self.subTest(formula=path.name):
                self.assertEqual(
                    run_snippet(snippet, "DEFAULT", self.clone),
                    "trunk",
                )
                # Undo the refresh so the next formula starts from the same
                # state; otherwise only the first one exercises this path.
                self.drop_origin_head()

    def test_formulas_fall_back_only_when_both_paths_fail(self) -> None:
        self.drop_origin_head()
        self.sever_remote()
        for path, snippet in self.formula_snippets():
            with self.subTest(formula=path.name):
                self.assertEqual(run_snippet(snippet, "DEFAULT", self.clone), "main")

    def test_the_superseded_pipeline_returns_empty_not_main(self) -> None:
        """The control, and the reason the fallback was worth replacing.

        Runs the exact line the four assets used to carry. It is not a stub of
        it. With the remote unreachable it yields an empty string, so every
        caller that read `DEFAULT` as "main" was reading "".
        """
        self.sever_remote()
        self.assertEqual(run_snippet(SUPERSEDED, "DEFAULT", self.clone), "")

    def test_the_superseded_pipeline_is_gone_from_every_asset(self) -> None:
        offenders = [
            str(path.relative_to(REPO_ROOT))
            for path in REPO_ROOT.rglob("*")
            if path.is_file()
            and path.suffix in {".toml", ".md", ".sh", ".py"}
            and ".git/" not in str(path)
            and path != Path(__file__)
            and "HEAD branch: " in path.read_text(encoding="utf-8", errors="ignore")
        ]
        self.assertFalse(
            offenders,
            "these still resolve the default branch over the network:\n  "
            + "\n  ".join(offenders),
        )

    # --- the shipped workflow step ---------------------------------------

    def test_workflow_step_resolves_without_the_remote(self) -> None:
        snippet = extract(WORKFLOW_STEP, WORKFLOW_BLOCK)
        self.sever_remote()
        self.assertEqual(run_snippet(snippet, "DEFAULT_BRANCH", self.clone), "trunk")

    def test_workflow_step_fails_closed_rather_than_guessing(self) -> None:
        """It has no `main` fallback, deliberately: #228 landed because basing
        a worktree on the wrong branch is worse than refusing to make one.
        """
        snippet = extract(WORKFLOW_STEP, WORKFLOW_BLOCK)
        self.drop_origin_head()
        self.sever_remote()
        self.assertEqual(run_snippet(snippet, "DEFAULT_BRANCH", self.clone), "")


if __name__ == "__main__":
    unittest.main()
