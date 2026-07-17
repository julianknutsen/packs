from __future__ import annotations

import json
import pathlib
import unittest
from typing import ClassVar

from coverage_contract import (
    CandidateSignals,
    CoverageVerdictError,
    bare_refs,
    closing_keyword_refs,
    decide_coverage,
    validate_coverage_verdict,
)

FIXTURES = pathlib.Path(__file__).resolve().parent / "fixtures"


def load_pr(number: int) -> dict:
    return json.loads((FIXTURES / f"pr-{number}.json").read_text(encoding="utf-8"))


class ClosingKeywordTests(unittest.TestCase):
    """AC3 — an explicit closing keyword is still sufficient mechanical coverage."""

    def test_each_github_closing_keyword_maps_coverage(self) -> None:
        for phrase in (
            "Closes #42",
            "closes #42",
            "Closed #42",
            "Fixes #42",
            "fix #42",
            "Fixed #42",
            "Resolves #42",
            "resolve #42",
            "Resolved #42",
            "Fixes: #42",
        ):
            with self.subTest(phrase=phrase):
                self.assertEqual(closing_keyword_refs(phrase), frozenset({42}))

    def test_cross_repo_closing_keyword_maps_coverage_for_the_named_repo(self) -> None:
        self.assertEqual(
            closing_keyword_refs("Closes gastownhall/gascity#42", repo="gastownhall/gascity"),
            frozenset({42}),
        )

    def test_a_reference_scoped_to_another_repo_is_not_our_issue(self) -> None:
        # "Fixes octo/other#42" names a DIFFERENT #42. Collapsing the qualifier
        # would bury this repo's live #42 on evidence about someone else's.
        for body in (
            "Fixes octo/other#42",
            "Fixes https://github.com/octo/other/issues/42",
        ):
            with self.subTest(body=body):
                self.assertEqual(
                    closing_keyword_refs(body, repo="gastownhall/gascity"), frozenset()
                )
                decision = decide_coverage(
                    42, body, CandidateSignals(bare_ref=True), repo="gastownhall/gascity"
                )
                self.assertFalse(decision.covered)

    def test_an_unidentifiable_qualified_reference_does_not_demote(self) -> None:
        # With no repo in hand we cannot establish identity, so we must not claim
        # coverage from a qualified reference.
        self.assertEqual(closing_keyword_refs("Closes octo/other#42"), frozenset())

    def test_gh_dash_form_is_a_same_repo_reference(self) -> None:
        self.assertEqual(closing_keyword_refs("Fixes GH-42"), frozenset({42}))
        self.assertEqual(bare_refs("Related GH-42"), frozenset({42}))

    def test_gh_dash_does_not_match_inside_a_longer_token(self) -> None:
        # A JIRA-style key ending in GH-<digits> is not a GitHub reference.
        for body in ("ABGH-42", "SUBGH-42 for context", "PROJGH-42"):
            with self.subTest(body=body):
                self.assertEqual(bare_refs(body), frozenset())
                self.assertEqual(closing_keyword_refs(f"Fixes {body}"), frozenset())

    def test_issue_url_is_a_bare_reference_when_it_names_our_repo(self) -> None:
        self.assertEqual(
            bare_refs(
                "Related https://github.com/gastownhall/gascity/issues/42",
                repo="gastownhall/gascity",
            ),
            frozenset({42}),
        )

    def test_closing_keyword_on_a_non_default_base_is_not_mechanical_coverage(self) -> None:
        # GitHub only honours closing keywords on PRs targeting the default
        # branch; a release-branch PR saying "Fixes #42" never closes #42.
        decision = decide_coverage(
            42, "Fixes #42", CandidateSignals(), targets_default_branch=False
        )
        self.assertFalse(decision.covered)
        self.assertEqual(decision.basis, "no-coverage-evidence")

    def test_multiple_closing_keywords_all_map(self) -> None:
        body = "Fixes #1 and closes #2.\n\nResolves #3."
        self.assertEqual(closing_keyword_refs(body), frozenset({1, 2, 3}))

    def test_keyword_without_a_number_is_not_coverage(self) -> None:
        # "not a fix for them" / "## What this fixes" must never mint a ref.
        self.assertEqual(closing_keyword_refs("not a fix for them"), frozenset())
        self.assertEqual(closing_keyword_refs("## What this fixes\n\nA session."), frozenset())

    def test_bare_reference_is_not_a_closing_keyword(self) -> None:
        self.assertEqual(closing_keyword_refs("See #42 for context."), frozenset())

    def test_closing_url_form_maps_coverage_for_the_named_repo(self) -> None:
        self.assertEqual(
            closing_keyword_refs(
                "Closes https://github.com/gastownhall/gascity/issues/42",
                repo="gastownhall/gascity",
            ),
            frozenset({42}),
        )

    def test_keyword_must_bind_a_ref_on_the_same_line(self) -> None:
        # The dangerous direction. A heading must not bind to a bare ref opening
        # the next paragraph — PR #3954's body really does contain this heading,
        # and a phantom closing ref would bury the very work this fix protects.
        for body in (
            "## What this fixes\n\n#42 is related, not covered.",
            "This fixes\n#42",
            "Resolves\n\n#42",
        ):
            with self.subTest(body=body):
                self.assertEqual(closing_keyword_refs(body), frozenset())
                self.assertEqual(bare_refs(body), frozenset({42}))

    def test_keyword_inside_code_is_not_coverage(self) -> None:
        # A pasted command or log line is prose to GitHub's linker, not a ref.
        for body in (
            "```\ngrep -n fixes #42\n```",          # closed fence
            "Run `fixes #42` locally.",             # inline span
            "``Fixes #42``",                        # double-backtick span
            "```text\nFixes #42",                   # unclosed fence, runs to EOF
            "~~~\nFixes #42\n~~~",                  # tilde fence
        ):
            with self.subTest(body=body):
                self.assertEqual(closing_keyword_refs(body), frozenset())

    def test_keyword_embedded_in_a_longer_word_is_not_coverage(self) -> None:
        for body in ("prefixes #42", "This suffixes #42", "Refixes #42"):
            with self.subTest(body=body):
                self.assertEqual(closing_keyword_refs(body), frozenset())

    def test_hex_color_is_not_a_reference(self) -> None:
        self.assertEqual(bare_refs("color: #fff;"), frozenset())


class BareReferenceTests(unittest.TestCase):
    """Bare refs are candidate context — extracted, but never coverage."""

    def test_bare_refs_are_extracted(self) -> None:
        self.assertEqual(bare_refs("Related: #7, #9."), frozenset({7, 9}))

    def test_closing_keyword_refs_are_not_also_bare_refs(self) -> None:
        self.assertEqual(bare_refs("Closes #7. See also #9."), frozenset({9}))


def legacy_any_hit_demotes(issue: int, body: str) -> bool:
    """The pre-fix rule: 'three methods, ANY hit demotes to Tier 4'.

    Deliberately derived from the fixture body alone. Taking the caller's
    candidate signals here would make this a tautology — hand-passing
    bare_ref=True would satisfy it for any body at all, including one that
    mentions nothing. Scanning the real body is what proves these fixtures
    genuinely reproduce the defect rather than restating the test's own setup.
    """
    return issue in bare_refs(body)


class RealPrBodyRegressionTests(unittest.TestCase):
    """AC1 + AC2 — the two confirmed false positives, from actual PR bodies."""

    def test_the_legacy_rule_is_not_vacuously_true(self) -> None:
        # Guards the guard: an unrelated body must not trip the legacy rule, or
        # the reproduction below would prove nothing.
        self.assertFalse(
            legacy_any_hit_demotes(999999, "Formatting only; no issue is discussed.")
        )

    def test_fixtures_reproduce_the_defect_under_the_legacy_rule(self) -> None:
        # The bug, made visible: every one of these was demoted before the fix,
        # on evidence scanned from the real PR body.
        for pr, issue in ((3880, 2713), (3880, 3005), (3954, 3849)):
            with self.subTest(pr=pr, issue=issue):
                body = load_pr(pr)["body"]
                self.assertTrue(
                    legacy_any_hit_demotes(issue, body),
                    f"fixture PR #{pr} must reproduce the false demotion of #{issue}",
                )
                signals = CandidateSignals(bare_ref=True, timeline_crossref=True)
                self.assertFalse(
                    decide_coverage(issue, body, signals).covered,
                    f"#{issue} must survive the fix",
                )

    def test_pr3954_mentions_3849_only_as_a_bare_ref(self) -> None:
        self.assertIn(3849, bare_refs(load_pr(3954)["body"]))

    def test_pr3880_carries_no_closing_keyword(self) -> None:
        body = load_pr(3880)["body"]
        self.assertEqual(closing_keyword_refs(body), frozenset())

    def test_pr3880_mentions_2713_and_3005_only_as_bare_refs(self) -> None:
        body = load_pr(3880)["body"]
        self.assertTrue({2713, 3005}.issubset(bare_refs(body)))

    def test_pr3880_leaves_2713_and_3005_authorable(self) -> None:
        # AC1. PR #3880 references both issues and explicitly disclaims covering
        # them ("intentionally does not cover", "not a fix for them"). Bare refs
        # plus a timeline cross-reference must not demote either issue.
        body = load_pr(3880)["body"]
        signals = CandidateSignals(bare_ref=True, timeline_crossref=True)
        for issue in (2713, 3005):
            with self.subTest(issue=issue):
                decision = decide_coverage(issue, body, signals)
                self.assertFalse(decision.covered, f"#{issue} must stay authorable")

    def test_pr3954_carries_no_closing_keyword(self) -> None:
        body = load_pr(3954)["body"]
        self.assertEqual(closing_keyword_refs(body), frozenset())

    def test_pr3954_leaves_3849_authorable(self) -> None:
        # AC2. #3849 appears under "## Related" as "adjacent" hardening only.
        body = load_pr(3954)["body"]
        signals = CandidateSignals(bare_ref=True, timeline_crossref=True, title_match=True)
        decision = decide_coverage(3849, body, signals)
        self.assertFalse(decision.covered, "#3849 must stay authorable")


class CandidateSignalsCannotDemoteTests(unittest.TestCase):
    """AC4 — bare refs, fuzzy title hits, and timeline events cannot demote alone."""

    BODY = "Groundwork for the parser.\n\nReferences: #101."

    def test_no_single_candidate_signal_demotes(self) -> None:
        for signals in (
            CandidateSignals(bare_ref=True),
            CandidateSignals(title_match=True),
            CandidateSignals(timeline_crossref=True),
        ):
            with self.subTest(signals=signals):
                self.assertFalse(decide_coverage(101, self.BODY, signals).covered)

    def test_all_candidate_signals_together_still_cannot_demote(self) -> None:
        signals = CandidateSignals(bare_ref=True, title_match=True, timeline_crossref=True)
        decision = decide_coverage(101, self.BODY, signals)
        self.assertFalse(decision.covered)
        self.assertEqual(decision.basis, "no-coverage-evidence")

    def test_closing_keyword_still_demotes_regardless_of_signals(self) -> None:
        decision = decide_coverage(101, "Closes #101.", CandidateSignals())
        self.assertTrue(decision.covered)
        self.assertEqual(decision.basis, "closing-keyword")


class CoverageVerdictSchemaTests(unittest.TestCase):
    """AC5 — keywordless coverage requires a schema-valid, evidence-citing verdict."""

    BODY = "This rewrites the retry loop entirely, superseding the old path."

    def valid_verdict(self, **overrides) -> dict:
        verdict = {
            "issue": 55,
            "covers": True,
            "evidence": ["rewrites the retry loop entirely"],
            "reasoning": "The PR replaces the subsystem the issue reports.",
        }
        verdict.update(overrides)
        return verdict

    def test_valid_verdict_with_cited_evidence_maps_coverage(self) -> None:
        decision = decide_coverage(55, self.BODY, CandidateSignals(bare_ref=True), self.valid_verdict())
        self.assertTrue(decision.covered)
        self.assertEqual(decision.basis, "model-verdict")

    def test_verdict_saying_not_covered_leaves_issue_authorable(self) -> None:
        verdict = self.valid_verdict(covers=False, evidence=[])
        decision = decide_coverage(55, self.BODY, CandidateSignals(bare_ref=True), verdict)
        self.assertFalse(decision.covered)

    def test_covering_verdict_must_cite_evidence(self) -> None:
        with self.assertRaises(CoverageVerdictError):
            validate_coverage_verdict(self.valid_verdict(evidence=[]), self.BODY)

    def test_evidence_must_be_quoted_from_the_pr_body(self) -> None:
        # A fabricated citation is a structural failure, not a judgment call.
        with self.assertRaises(CoverageVerdictError):
            validate_coverage_verdict(
                self.valid_verdict(evidence=["closes the issue completely"]), self.BODY
            )

    def test_missing_and_mistyped_fields_are_rejected(self) -> None:
        for bad in (
            {"covers": True, "evidence": ["x"], "reasoning": "y"},
            {"issue": 55, "evidence": ["x"], "reasoning": "y"},
            self.valid_verdict(covers="yes"),
            self.valid_verdict(issue="55"),
            self.valid_verdict(issue=55.0),
            self.valid_verdict(reasoning=""),
            self.valid_verdict(reasoning="   "),
            self.valid_verdict(evidence="rewrites the retry loop entirely"),
            self.valid_verdict(evidence=[123]),
        ):
            with self.subTest(bad=bad):
                with self.assertRaises(CoverageVerdictError):
                    validate_coverage_verdict(bad, self.BODY)

    def test_boolean_issue_is_rejected(self) -> None:
        # bool subclasses int and True == 1, so an unguarded isinstance(x, int)
        # would read {"issue": true} as a verdict about issue #1 and demote it.
        with self.assertRaises(CoverageVerdictError):
            validate_coverage_verdict(self.valid_verdict(issue=True), self.BODY)
        with self.assertRaises(CoverageVerdictError):
            validate_coverage_verdict(self.valid_verdict(issue=True), self.BODY, expected_issue=1)

    def test_non_object_verdict_is_rejected(self) -> None:
        for bad in (["not", "a", "dict"], "covered", 42, None):
            with self.subTest(bad=bad):
                with self.assertRaises(CoverageVerdictError):
                    validate_coverage_verdict(bad, self.BODY)

    def test_whitespace_only_evidence_span_is_rejected(self) -> None:
        with self.assertRaises(CoverageVerdictError):
            validate_coverage_verdict(self.valid_verdict(evidence=["   "]), self.BODY)

    def test_a_slight_evidence_span_cannot_satisfy_provenance(self) -> None:
        # Adversarial: "." and "a" are quoted from virtually every body, so
        # without a floor the provenance check is vacuous and a one-character
        # citation could bury a live issue.
        for span in (".", "a", "A", ",", "the", "loop"):
            with self.subTest(span=span):
                with self.assertRaises(CoverageVerdictError):
                    validate_coverage_verdict(self.valid_verdict(evidence=[span]), self.BODY)

    def test_a_slight_citation_does_not_demote_via_decide(self) -> None:
        decision = decide_coverage(
            55, self.BODY, CandidateSignals(bare_ref=True), self.valid_verdict(evidence=["."])
        )
        self.assertFalse(decision.covered)
        self.assertEqual(decision.basis, "invalid-verdict")

    def test_fabricated_evidence_is_rejected_even_when_not_claiming_coverage(self) -> None:
        # The contract says cited evidence is quoted verbatim, full stop.
        with self.assertRaises(CoverageVerdictError):
            validate_coverage_verdict(
                self.valid_verdict(covers=False, evidence=["a span that is simply not present"]),
                self.BODY,
            )

    def test_a_padded_citation_is_still_quoted_from_the_body(self) -> None:
        # The floor measures the stripped span, so containment must too, or a
        # genuine quote with incidental padding is rejected as fabricated.
        verdict = self.valid_verdict(evidence=["  rewrites the retry loop entirely  "])
        checked = validate_coverage_verdict(verdict, self.BODY)
        self.assertTrue(checked.covers)

    def test_a_covering_decision_carries_its_cited_evidence(self) -> None:
        decision = decide_coverage(
            55, self.BODY, CandidateSignals(bare_ref=True), self.valid_verdict()
        )
        self.assertTrue(decision.covered)
        self.assertEqual(decision.evidence, ("rewrites the retry loop entirely",))

    def test_verdict_for_a_different_issue_cannot_demote(self) -> None:
        verdict = self.valid_verdict(issue=999)
        with self.assertRaises(CoverageVerdictError):
            validate_coverage_verdict(verdict, self.BODY, expected_issue=55)

    def test_malformed_verdict_does_not_demote_via_decide(self) -> None:
        # decide_coverage must fail safe: a bad verdict leaves work authorable.
        decision = decide_coverage(
            55, self.BODY, CandidateSignals(bare_ref=True), self.valid_verdict(evidence=[])
        )
        self.assertFalse(decision.covered)
        self.assertEqual(decision.basis, "invalid-verdict")


class FormulaContractTests(unittest.TestCase):
    """The prose an agent actually follows must encode the two-phase contract.

    The harness above proves the decision procedure is sound; these guard the
    formula from drifting back to collapsing discovery into coverage.
    """

    text: ClassVar[str]

    @classmethod
    def setUpClass(cls) -> None:
        formula = pathlib.Path(__file__).resolve().parents[1] / "formulas" / "mol-pr-triage.formula.toml"
        # Collapse whitespace: these guard what the prose SAYS, and a phrase that
        # rewraps across a line break is not a contract change.
        cls.text = " ".join(formula.read_text(encoding="utf-8").split())

    def test_the_any_hit_rule_is_gone(self) -> None:
        self.assertNotIn("ANY hit demotes", self.text)

    def test_discovery_is_stated_not_to_demote(self) -> None:
        self.assertIn("it does NOT demote", self.text)

    def test_closing_keyword_is_the_mechanical_branch(self) -> None:
        self.assertIn("Closes|Fixes|Resolves", self.text)

    def test_keywordless_coverage_requires_a_structured_verdict(self) -> None:
        for required in ('"covers": true|false', '"evidence"', '"reasoning"'):
            with self.subTest(required=required):
                self.assertIn(required, self.text)

    def test_evidence_must_be_quoted_verbatim(self) -> None:
        self.assertIn("verbatim", self.text)

    def test_scope_disclaimers_count_against_coverage(self) -> None:
        self.assertIn("evidence AGAINST coverage", self.text)

    def test_report_schema_records_the_coverage_basis(self) -> None:
        for basis in ("closing-keyword", "model-verdict", "no-coverage-evidence"):
            with self.subTest(basis=basis):
                self.assertIn(basis, self.text)

    def test_prose_scopes_a_qualified_reference_to_this_repo(self) -> None:
        self.assertIn("octo/other#42", self.text)

    def test_prose_carries_githubs_default_branch_restriction(self) -> None:
        self.assertIn("baseRefName", self.text)
        # ...and the fetch step must actually retrieve it, or classify cannot comply.
        self.assertIn("--json number,title,body,author,baseRefName", self.text)

    def test_prose_does_not_promise_disclaimers_override_a_closing_keyword(self) -> None:
        self.assertIn("does NOT override a closing keyword", self.text)


if __name__ == "__main__":
    unittest.main()
