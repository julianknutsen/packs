"""mol-pr-start Gate 1 — a mention must not abort an authoring run.

The gate shares mol-pr-triage's decision procedure by construction: everything
here drives `coverage_contract`, the same module `test_pr_triage_coverage.py`
drives. What is new is the aggregation over a candidate SET, and the fact that a
wrong answer here closes the root bead rather than mis-tiering a report line.
"""

from __future__ import annotations

import json
import pathlib
import unittest
from typing import ClassVar

import coverage_contract
from coverage_contract import (
    CandidateSignals,
    CompetingPrGate,
    GateCandidate,
    bare_refs,
    closing_keyword_refs,
    decide_competing_pr_gate,
)

FIXTURES = pathlib.Path(__file__).resolve().parent / "fixtures"
FORMULAS = pathlib.Path(__file__).resolve().parents[1] / "formulas"


def load_pr(number: int) -> dict:
    return json.loads((FIXTURES / f"pr-{number}.json").read_text(encoding="utf-8"))


def candidate_from_fixture(number: int, **overrides) -> GateCandidate:
    """A discovery hit built from a real PR body, as the gate would see it."""
    return GateCandidate(
        number=number,
        body=load_pr(number)["body"],
        signals=CandidateSignals(bare_ref=True, timeline_crossref=True),
        **overrides,
    )


def legacy_gate_blocks(candidates: list[GateCandidate]) -> bool:
    """The pre-fix rule: `COMPETING_PRS` non-empty is dispositive.

    Deliberately this trivial. Discovery WAS the decision — a full-text hit
    blocked and closed the root bead without the PR body ever being read, which
    is why the triage-side model of the same bug (`legacy_any_hit_demotes`, which
    at least scanned for a reference) does not transfer. The reproduction below
    carries its own guard instead: it asserts the issue really is referenced in
    the fixture body, so "this PR was returned by discovery" is grounded in the
    real document rather than in the test's own setup.
    """
    return bool(candidates)


class LiveReproductionTests(unittest.TestCase):
    """AC1 — #2713, #3005 and #3849 must survive discovery against #3880/#3954.

    All three were live-reproduced on 2026-07-17: the formula's literal
    `gh pr list --search "<num>"` returned a PR that disclaims the issue.
    """

    def test_fixtures_reproduce_the_abort_under_the_legacy_rule(self) -> None:
        for pr, issue in ((3880, 2713), (3880, 3005), (3954, 3849)):
            with self.subTest(pr=pr, issue=issue):
                candidates = [candidate_from_fixture(pr)]
                self.assertTrue(
                    legacy_gate_blocks(candidates),
                    f"discovery of PR #{pr} must reproduce the false block of #{issue}",
                )
                self.assertIn(issue, bare_refs(load_pr(pr)["body"]))
                self.assertFalse(
                    decide_competing_pr_gate(issue, candidates).blocked,
                    f"#{issue} must survive the fix",
                )

    def test_pr3880_disclaims_2713_and_3005_and_carries_no_keyword(self) -> None:
        body = load_pr(3880)["body"]
        self.assertEqual(closing_keyword_refs(body), frozenset())
        self.assertTrue({2713, 3005}.issubset(bare_refs(body)))

    def test_pr3954_lists_3849_as_adjacent_and_carries_no_keyword(self) -> None:
        body = load_pr(3954)["body"]
        self.assertEqual(closing_keyword_refs(body), frozenset())
        self.assertIn(3849, bare_refs(body))

    def test_a_non_covering_candidate_is_reported_not_dropped(self) -> None:
        # The run continues, but the caller still learns PR #3880 was checked.
        gate = decide_competing_pr_gate(2713, [candidate_from_fixture(3880)])
        self.assertEqual([number for number, _ in gate.mentions], [3880])
        self.assertEqual(gate.mentions[0][1].basis, "no-coverage-evidence")


class ClosingKeywordStillBlocksTests(unittest.TestCase):
    """AC2 — the gate must still stop when a PR really does close the issue."""

    def test_closing_keyword_bound_to_the_issue_blocks(self) -> None:
        gate = decide_competing_pr_gate(
            42, [GateCandidate(number=7, body="Fixes #42, at last.")]
        )
        self.assertTrue(gate.blocked)
        self.assertEqual([number for number, _ in gate.blocking], [7])
        self.assertEqual(gate.blocking[0][1].basis, "closing-keyword")

    def test_a_keyword_bound_to_another_issue_does_not_block(self) -> None:
        gate = decide_competing_pr_gate(
            42, [GateCandidate(number=7, body="Fixes #43. Related: #42.")]
        )
        self.assertFalse(gate.blocked)

    def test_a_keyword_on_a_non_default_branch_pr_does_not_block(self) -> None:
        # GitHub's own rule: a release-branch PR saying "Fixes #42" never
        # closes #42, so it is not mechanical coverage.
        gate = decide_competing_pr_gate(
            42,
            [GateCandidate(number=7, body="Fixes #42.", targets_default_branch=False)],
        )
        self.assertFalse(gate.blocked)

    def test_one_covering_pr_blocks_among_many_mentioning_ones(self) -> None:
        gate = decide_competing_pr_gate(
            42,
            [
                candidate_from_fixture(3880),
                GateCandidate(number=7, body="Fixes #42."),
                GateCandidate(number=8, body="Groundwork; see #42."),
            ],
        )
        self.assertTrue(gate.blocked)
        self.assertEqual([number for number, _ in gate.blocking], [7])
        self.assertEqual({number for number, _ in gate.mentions}, {3880, 8})


class UntypedDigitMatchTests(unittest.TestCase):
    """AC3 — the gate's search is untyped, so digits alone must not block.

    `--search "1234"` matches the bare digits anywhere in an open PR's text:
    a byte count, a line number, a pasted diff. That is broader than the
    triage-side bug and the reason a bare-mention block is so destructive here.
    """

    def test_digits_with_no_reference_at_all_do_not_block(self) -> None:
        for body in (
            "Shrinks the payload from 1234 bytes to 900.",
            "See parser.go:1234 for the new branch.",
            "Bumped the timeout to 1234ms.",
        ):
            with self.subTest(body=body):
                gate = decide_competing_pr_gate(
                    1234, [GateCandidate(number=7, body=body)]
                )
                self.assertFalse(gate.blocked)
                self.assertEqual(closing_keyword_refs(body), frozenset())

    def test_a_footer_mention_does_not_block(self) -> None:
        gate = decide_competing_pr_gate(
            1234,
            [GateCandidate(number=7, body="## Related\n\n- #1234 — adjacent hardening.")],
        )
        self.assertFalse(gate.blocked)

    def test_every_candidate_signal_at_once_still_does_not_block(self) -> None:
        # The strongest case only: which individual signals fire is
        # `decide_coverage`'s concern and is enumerated in
        # test_pr_triage_coverage.CandidateSignalsCannotDemoteTests.
        gate = decide_competing_pr_gate(
            1234,
            [
                GateCandidate(
                    number=7,
                    body="Groundwork for the parser.\n\nReferences: #1234.",
                    signals=CandidateSignals(
                        bare_ref=True, title_match=True, timeline_crossref=True
                    ),
                )
            ],
        )
        self.assertFalse(gate.blocked)


class ModelVerdictTests(unittest.TestCase):
    """AC4 — keywordless coverage needs a schema-valid, verbatim-cited verdict."""

    BODY = "This supersedes the old resolver entirely; #1234 no longer applies."

    def valid_verdict(self, **overrides) -> dict:
        verdict = {
            "issue": 1234,
            "covers": True,
            "evidence": ["supersedes the old resolver entirely"],
            "reasoning": "The resolver the issue reports on is deleted here.",
        }
        return {**verdict, **overrides}

    def test_a_valid_covering_verdict_blocks_and_carries_its_evidence(self) -> None:
        gate = decide_competing_pr_gate(
            1234,
            [GateCandidate(number=7, body=self.BODY, verdict=self.valid_verdict())],
        )
        self.assertTrue(gate.blocked)
        decision = gate.blocking[0][1]
        self.assertEqual(decision.basis, "model-verdict")
        self.assertEqual(decision.evidence, ("supersedes the old resolver entirely",))

    def test_a_non_covering_verdict_does_not_block(self) -> None:
        gate = decide_competing_pr_gate(
            1234,
            [
                GateCandidate(
                    number=7, body=self.BODY, verdict=self.valid_verdict(covers=False)
                )
            ],
        )
        self.assertFalse(gate.blocked)

    def test_an_unusable_verdict_does_not_block(self) -> None:
        # One case per way a verdict fails validation, not the full schema
        # matrix — that is enumerated in
        # test_pr_triage_coverage.CoverageVerdictSchemaTests. What is checked
        # here is that an unusable verdict routes to `mentions`, so a bad model
        # response leaves the issue authorable instead of aborting the run.
        for label, broken in (
            ("not an object", "covers"),
            ("missing field", {"covers": True, "evidence": [], "reasoning": "x"}),
            ("mistyped field", self.valid_verdict(covers="yes")),
            ("covers with no citation", self.valid_verdict(evidence=[])),
            ("citation absent from body", self.valid_verdict(evidence=["fully fixes the crash"])),
            ("about another issue", self.valid_verdict(issue=99)),
        ):
            with self.subTest(label=label):
                gate = decide_competing_pr_gate(
                    1234, [GateCandidate(number=7, body=self.BODY, verdict=broken)]
                )
                self.assertFalse(gate.blocked)
                self.assertEqual(gate.mentions[0][1].basis, "invalid-verdict")


class EmptyCandidateSetTests(unittest.TestCase):
    def test_no_candidates_means_no_block_and_nothing_to_report(self) -> None:
        gate = decide_competing_pr_gate(1234, [])
        self.assertFalse(gate.blocked)
        self.assertEqual(gate, CompetingPrGate())


class FormulaContractTests(unittest.TestCase):
    """The prose an agent actually follows must encode the two-phase gate.

    The harness above proves the decision is sound; these guard mol-pr-start
    from drifting back to treating a search hit as dispositive.
    """

    text: ClassVar[str]

    @classmethod
    def setUpClass(cls) -> None:
        formula = FORMULAS / "mol-pr-start.formula.toml"
        # Collapse whitespace: these guard what the prose SAYS, and a phrase
        # that rewraps across a line break is not a contract change.
        cls.text = " ".join(formula.read_text(encoding="utf-8").split())

    def test_a_bare_search_hit_is_no_longer_dispositive(self) -> None:
        self.assertNotIn("If `COMPETING_PRS` is non-empty", self.text)
        # The stop condition counts covering PRs. It must not test the JSON
        # string: `[]` is non-empty as a string, which would block every run.
        self.assertIn("If `COVERING_COUNT` is greater than 0", self.text)
        self.assertNotIn("If `COVERING_PRS` is non-empty", self.text)

    def test_discovery_is_stated_not_to_block(self) -> None:
        self.assertIn("it does NOT block", self.text)

    def test_prose_names_the_search_as_untyped_full_text(self) -> None:
        self.assertIn("untyped full-text query", self.text)

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

    def test_prose_scopes_a_qualified_reference_to_this_repo(self) -> None:
        self.assertIn("octo/other#42", self.text)

    def test_prose_carries_githubs_default_branch_restriction(self) -> None:
        self.assertIn("baseRefName", self.text)
        # ...and discovery must actually retrieve body + baseRefName, or
        # Phase 2 cannot be decided at all.
        self.assertIn("--json number,title,body,author,createdAt,baseRefName", self.text)
        self.assertIn("defaultBranchRef", self.text)

    def test_prose_does_not_promise_disclaimers_override_a_closing_keyword(self) -> None:
        self.assertIn("does NOT override a closing keyword", self.text)

    def test_prose_continues_and_reports_when_candidates_only_mention(self) -> None:
        self.assertIn("including when candidates were found but none covers", self.text)
        self.assertIn("mentioning_prs", self.text)

    def test_prose_points_at_the_shared_harness(self) -> None:
        self.assertIn("pr-pipeline/tests/coverage_contract.py", self.text)

    def test_discovery_overrides_ghs_default_result_cap(self) -> None:
        # `gh pr list` silently caps at 30. The search is untyped, so a common
        # number can return more incidental hits than that and truncate the one
        # PR that actually covers the issue — Phase 2 would never see it and the
        # gate would pass an issue that IS being fixed.
        #
        # Asserted against the whole discovery invocation, not a bare
        # "--limit 1000" substring: the paragraph below the command explains the
        # flag by name, so a loose check stays green even after the flag is
        # dropped from the command itself. Verified by deleting the flag.
        self.assertIn(
            'CANDIDATE_PRS=$(gh pr list --repo "$REPO" --state open '
            '--search "{{issue}}" \\\\ --limit 1000 \\\\ '
            "--json number,title,body,author,createdAt,baseRefName)",
            self.text,
        )
        self.assertIn("silently caps at 30", self.text)

    def test_the_phase_two_artifact_path_is_run_scoped(self) -> None:
        # Concurrent polecats share /tmp on this machine. An unscoped path lets
        # one run read another's verdicts, which fabricates both a false block
        # and a false pass depending on whose file wins.
        self.assertIn('COVERING_JSON="/tmp/pr-start-covering-$ROOT_ID.json"', self.text)
        # Every read must go through the scoped variable, so no bare unscoped
        # filename may survive anywhere in the prose.
        self.assertNotIn("/tmp/pr-start-covering.json", self.text)

    def test_the_phase_two_artifact_is_written_atomically(self) -> None:
        self.assertIn('mv -f "$COVERING_JSON.tmp" "$COVERING_JSON"', self.text)

    def test_the_stop_condition_reads_the_scoped_artifact(self) -> None:
        for command in (
            "COVERING_PRS=$(jq -c '.' \"$COVERING_JSON\")",
            "COVERING_COUNT=$(jq 'length' \"$COVERING_JSON\")",
        ):
            with self.subTest(command=command):
                self.assertIn(command, self.text)

    def test_prose_mirrors_the_validators_evidence_floor(self) -> None:
        # The prose claims to state exactly the rules coverage_contract.py
        # enforces. That claim is only true while these numbers agree, and an
        # agent following prose that omits the floor would emit citations the
        # validator rejects. Read from the module so a change to either side
        # fails here rather than drifting silently.
        self.assertIn(
            f"at least {coverage_contract._MIN_EVIDENCE_CHARS} characters and at least "
            f"{coverage_contract._MIN_EVIDENCE_WORDS} words",
            self.text,
        )


class ChainedFormulaContractTests(unittest.TestCase):
    """AC5 — mol-pr-from-issue chains Gate 1, so it must inherit the fix."""

    text: ClassVar[str]

    @classmethod
    def setUpClass(cls) -> None:
        formula = FORMULAS / "mol-pr-from-issue.formula.toml"
        cls.text = " ".join(formula.read_text(encoding="utf-8").split())

    def test_the_chained_gate_no_longer_fires_on_any_addressing_pr(self) -> None:
        self.assertNotIn("any open PR already addresses this issue", self.text)

    def test_the_chained_gate_requires_coverage(self) -> None:
        self.assertIn("shown to COVER this issue", self.text)
        self.assertIn("a bare mention is never coverage", self.text)

    def test_the_chained_gate_continues_on_a_mention(self) -> None:
        self.assertIn("the chain CONTINUES", self.text)


if __name__ == "__main__":
    unittest.main()
