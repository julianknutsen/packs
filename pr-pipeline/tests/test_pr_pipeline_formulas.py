from __future__ import annotations

import pathlib
import tomllib
import unittest


PACK_ROOT = pathlib.Path(__file__).resolve().parents[1]
FORMULAS = PACK_ROOT / "formulas"

# Bare `issue` is a reserved formulas-v2 alias auto-bound to the routed work
# bead. A formula slung as a routed molecule must not declare its own `issue`
# var or the routed value clobbers it. mol-pr-from-issue is the one such
# formula in this pack; it renames its GitHub-issue input to `issue_number`.
RESERVED_VAR_ALIAS = "issue"


class MolPrFromIssueVarBindingTests(unittest.TestCase):
    """The gate-4 port of mol-pr-from-issue into pr-pipeline (gpk-b3ll)."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.path = FORMULAS / "mol-pr-from-issue.formula.toml"
        cls.text = cls.path.read_text(encoding="utf-8")
        cls.data = tomllib.loads(cls.text)

    def test_formula_is_present_and_well_formed(self) -> None:
        self.assertTrue(self.path.exists(), "mol-pr-from-issue must live in the pr-pipeline pack")
        self.assertEqual(self.data["formula"], "mol-pr-from-issue")

    def test_v2_compiler_requirement_is_declared_in_the_form_gc_reads(self) -> None:
        """`contract = "graph.v2"` is deprecated; `[requires]` is the live form.

        This asserted the deprecated key until 2026-08-17, and passed the whole
        time. It could not have done otherwise: it read the file's own text back
        to itself, and gc was never involved. Meanwhile every city importing this
        pack carried a `formula-requirements` warning from `gc doctor`. The check
        that catches the next one of these is tests/test_reference_city.py, which
        runs the binary; this one just pins the value that fix landed on.
        """
        self.assertNotIn(
            "contract",
            self.data,
            "gc reports `contract = \"graph.v2\"` as deprecated in gc doctor",
        )
        self.assertEqual(self.data["requires"]["formula_compiler"], ">=2.0.0")

    def test_github_issue_input_is_named_issue_number(self) -> None:
        variables = self.data.get("vars", {})
        self.assertIn("issue_number", variables, "GitHub-issue input must be bound as issue_number")
        self.assertTrue(variables["issue_number"]["required"])

    def test_reserved_issue_alias_is_not_declared(self) -> None:
        variables = self.data.get("vars", {})
        self.assertNotIn(
            RESERVED_VAR_ALIAS,
            variables,
            "mol-pr-from-issue must not declare the reserved `issue` var — it is "
            "slung as a routed molecule and the alias would clobber the input",
        )

    def test_no_bare_issue_template_token_remains(self) -> None:
        # The rename must be complete: every {{issue}} became {{issue_number}}.
        self.assertNotIn("{{issue}}", self.text)
        self.assertIn("{{issue_number}}", self.text)

    def test_contract_doc_documents_issue_number(self) -> None:
        # The `## Contract` / `## Variables` prose drives how callers sling it.
        self.assertIn("--var issue_number=<number>", self.text)
        self.assertNotIn("--var issue=<number>", self.text)

    def test_companion_vars_survived_the_port(self) -> None:
        variables = self.data.get("vars", {})
        self.assertIn("skip_open_pr", variables)
        self.assertIn("auto_push", variables)


class MolPrFromIssueConfigPurityTests(unittest.TestCase):
    """Graduation blocker (gpk-4l3q): a published pack must be configuration-pure.

    Author-absolute host paths (`/home/ds/...`) do not resolve in a consumer
    city, and a baked-in target-repo layout is not portable. These guard the
    fixes from PR #117's review (report .gc/pr-pipeline/reviews/pr-117.md).
    """

    @classmethod
    def setUpClass(cls) -> None:
        cls.text = (FORMULAS / "mol-pr-from-issue.formula.toml").read_text(encoding="utf-8")

    def test_no_author_absolute_paths(self) -> None:
        self.assertNotIn("/home/ds", self.text)

    def test_sibling_formula_refs_are_pack_relative(self) -> None:
        # Sibling-formula citations resolve via the pack's own GC_PACK_DIR.
        for sibling in ("mol-pr-start", "mol-pr-ship", "mol-pr-review"):
            self.assertIn(
                f"${{GC_PACK_DIR}}/formulas/{sibling}.formula.toml",
                self.text,
                f"{sibling} reference must be pack-relative via GC_PACK_DIR",
            )

    def test_kill_switch_flag_is_home_relative(self) -> None:
        # The auto-push kill-switch lives in the operator's gc home, not a
        # hardcoded host path.
        self.assertIn("${HOME}/.gc/auto-push-armed.flag", self.text)

    def test_protected_paths_are_configurable(self) -> None:
        # Repo-specific protected surfaces come from the GC_PROTECTED_PATHS
        # env override, not a baked-in target-repo layout.
        self.assertIn("GC_PROTECTED_PATHS", self.text)
        self.assertNotIn("'^cmd/gc/dispatch_runtime", self.text)


if __name__ == "__main__":
    unittest.main()
