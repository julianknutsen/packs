"""Executable model of mol-pr-triage's PR-coverage decision procedure.

The formula prose is the contract a triage agent follows. This module makes the
mechanical half of that contract executable, so the decision can be regression-
tested against real PR bodies instead of grepped for in formula prose.

It implements only what the contract permits code to decide:

* **Mechanical coverage** — a GitHub closing keyword (`Closes/Fixes/Resolves #N`)
  is sufficient, deterministic evidence that a PR covers issue N.
* **Candidate context** — a bare `#N` reference, a fuzzy title hit, or a timeline
  cross-reference nominates a PR/issue pair for review. It is never coverage.
  A bare footer mention is what *creates* the timeline event, so treating that
  event as independent evidence would let a reference manufacture its own proof.
* **Schema validation** — a keywordless coverage claim must arrive as a
  structured verdict from the triage model. Code checks the verdict's shape and
  that its cited evidence is genuinely quoted from the PR body. Code never
  judges whether the reasoning is correct.

Semantic judgment (does this PR actually resolve this issue?) belongs to the
model, not here.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

# The reference forms GitHub documents: `#N` and `GH-N` (this repo), `owner/repo#N`
# and a full issue URL (explicitly scoped elsewhere).
_REF_FORMS = (
    r"(?:(?P<repo_hash>[\w.-]+/[\w.-]+)#(?P<num_hash>\d+)"
    r"|https?://github\.com/(?P<repo_url>[\w.-]+/[\w.-]+)/issues/(?P<num_url>\d+)"
    r"|(?<![A-Za-z0-9])GH-(?P<num_gh>\d+)"
    r"|#(?P<num_bare>\d+))\b"
)

# A closing keyword must bind its reference on the SAME LINE, separated only by
# spaces/tabs and an optional colon.
#
# The same-line rule is load-bearing, not cosmetic. Allowing `\s` here lets a
# heading like "## What this fixes" bind to a bare `#N` opening the next
# paragraph — PR #3954's body has exactly that heading — which mints a phantom
# closing reference and re-creates the bug this contract exists to prevent.
_CLOSING_KEYWORD_REF = re.compile(
    r"\b(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?)\b[ \t]*:?[ \t]+" + _REF_FORMS,
    re.IGNORECASE,
)

_ANY_REF = re.compile(_REF_FORMS, re.IGNORECASE)

# Fenced and inline code are prose to GitHub's linker, not references: a pasted
# git log or grep line ("grep -n fixes #42") must not read as coverage. An
# unclosed fence runs to end-of-body, and inline spans may use any backtick run.
# Known gap: 4-space indented code is NOT stripped, because list continuations
# are indented the same way and stripping them would lose real references.
_FENCED_CODE = re.compile(
    r"^[ \t]*(?:```|~~~).*?(?:^[ \t]*(?:```|~~~)[ \t]*$|\Z)", re.DOTALL | re.MULTILINE
)
_INLINE_CODE = re.compile(r"(?P<ticks>`+)[^\n]*?(?P=ticks)")

# A citation must be substantial enough to be evidence. Without a floor, "." or
# "a" appears in every body and the provenance check becomes vacuous — an
# adversarial verdict could cite a single character and bury a live issue. This
# is a structural anti-gaming floor, not a quality judgment: whether the quoted
# span actually supports the claim stays the model's call.
_MIN_EVIDENCE_CHARS = 12
_MIN_EVIDENCE_WORDS = 2


def _strip_code(body: str) -> str:
    """Blank out fenced and inline code so it cannot mint references."""
    return _INLINE_CODE.sub(" ", _FENCED_CODE.sub(" ", body))


def _scan_refs(pattern: re.Pattern[str], body: str, repo: str | None) -> frozenset[int]:
    """Extract issue numbers, honouring the repository a reference is scoped to.

    `#N`/`GH-N` are this repo by definition. A reference qualified with another
    repository names a different issue and must not collapse onto ours — that is
    how `Fixes octo/other#42` would otherwise bury this repo's #42. A qualified
    reference counts only when it names `repo`; with no `repo` in hand we cannot
    establish identity, so it is dropped (leaving work authorable, the safe way
    to be wrong).
    """
    found: set[int] = set()
    for match in pattern.finditer(_strip_code(body)):
        same_repo_num = match.group("num_bare") or match.group("num_gh")
        if same_repo_num:
            found.add(int(same_repo_num))
            continue
        qualifier = match.group("repo_hash") or match.group("repo_url")
        number = match.group("num_hash") or match.group("num_url")
        if repo and qualifier and qualifier.casefold() == repo.casefold():
            found.add(int(number))
    return frozenset(found)


class CoverageVerdictError(ValueError):
    """A model coverage verdict is structurally unusable."""


class CoverageBasis(StrEnum):
    """What a coverage decision rests on.

    The first three mirror the formula's report schema. INVALID_VERDICT is a
    diagnostic refinement of NO_COVERAGE_EVIDENCE — the formula tells the agent
    to discard an unusable verdict and leave the issue authorable, so it reports
    the same outcome; the distinct value exists to say why in a test failure.
    """

    CLOSING_KEYWORD = "closing-keyword"
    MODEL_VERDICT = "model-verdict"
    NO_COVERAGE_EVIDENCE = "no-coverage-evidence"
    INVALID_VERDICT = "invalid-verdict"


@dataclass(frozen=True)
class CandidateSignals:
    """Why a PR/issue pair was surfaced for review. Never coverage evidence."""

    bare_ref: bool = False
    title_match: bool = False
    timeline_crossref: bool = False

    @property
    def any_fired(self) -> bool:
        return self.bare_ref or self.title_match or self.timeline_crossref


@dataclass(frozen=True)
class CoverageVerdict:
    """A schema-valid coverage claim from the triage model."""

    issue: int
    covers: bool
    evidence: tuple[str, ...]
    reasoning: str


@dataclass(frozen=True)
class CoverageDecision:
    """The outcome for one PR/issue pair, with the basis it rests on."""

    covered: bool
    basis: CoverageBasis
    signals: CandidateSignals
    evidence: tuple[str, ...] = ()


def closing_keyword_refs(body: str, repo: str | None = None) -> frozenset[int]:
    """Issue numbers this PR body closes via an explicit GitHub keyword."""
    return _scan_refs(_CLOSING_KEYWORD_REF, body, repo)


def bare_refs(body: str, repo: str | None = None) -> frozenset[int]:
    """Issue numbers mentioned without a closing keyword — candidate context."""
    return _scan_refs(_ANY_REF, body, repo) - closing_keyword_refs(body, repo)


def validate_coverage_verdict(
    raw: Any, body: str, expected_issue: int | None = None
) -> CoverageVerdict:
    """Validate a model verdict's shape and citations. Never its meaning.

    Raises CoverageVerdictError when the verdict cannot be trusted structurally:
    a missing or mistyped field, a covering claim that cites nothing, a citation
    that is not actually quoted from `body`, or a verdict about another issue.

    Hand-rolled rather than `jsonschema` on purpose: the two most safety-critical
    checks here — that each cited span is genuinely quoted from `body`, and that
    the verdict is about the issue we asked about — are cross-field/cross-document
    rules plain JSON Schema cannot express. Adding the dependency would still
    leave both written by hand, and would trade field-specific messages (which
    debug a bad model response) for generic ones. Errors report the received type
    because the input is LLM-generated and mistypes are the common failure.
    """
    if not isinstance(raw, dict):
        raise CoverageVerdictError(f"verdict must be an object, got {type(raw).__name__}")

    for field in ("issue", "covers", "evidence", "reasoning"):
        if field not in raw:
            raise CoverageVerdictError(f"verdict is missing required field {field!r}")

    issue = raw["issue"]
    # bool is a subclass of int, and True == 1 — an unguarded check would accept
    # {"issue": true} as a verdict about issue #1.
    if isinstance(issue, bool) or not isinstance(issue, int):
        raise CoverageVerdictError(
            f"verdict field 'issue' must be an integer, got {type(issue).__name__}"
        )

    covers = raw["covers"]
    if not isinstance(covers, bool):
        raise CoverageVerdictError(
            f"verdict field 'covers' must be a boolean, got {type(covers).__name__}"
        )

    evidence = raw["evidence"]
    if not isinstance(evidence, list) or not all(isinstance(span, str) for span in evidence):
        raise CoverageVerdictError(
            f"verdict field 'evidence' must be a list of strings, got {type(evidence).__name__}"
        )

    reasoning = raw["reasoning"]
    if not isinstance(reasoning, str) or not reasoning.strip():
        raise CoverageVerdictError(
            f"verdict field 'reasoning' must be a non-empty string, got {type(reasoning).__name__}"
        )

    if expected_issue is not None and issue != expected_issue:
        raise CoverageVerdictError(
            f"verdict is about issue #{issue}, expected #{expected_issue}"
        )

    if covers and not evidence:
        raise CoverageVerdictError("a covering verdict must cite PR-body evidence")

    # Provenance holds for every span offered, not only on a covering verdict:
    # the contract says cited evidence is quoted verbatim, full stop.
    for span in evidence:
        stripped = span.strip()
        if len(stripped) < _MIN_EVIDENCE_CHARS or len(stripped.split()) < _MIN_EVIDENCE_WORDS:
            raise CoverageVerdictError(
                f"cited evidence is too slight to be evidence "
                f"(need >={_MIN_EVIDENCE_CHARS} chars and >={_MIN_EVIDENCE_WORDS} words): {span!r}"
            )
        if stripped not in body:
            raise CoverageVerdictError(f"cited evidence is not quoted from the PR body: {span!r}")

    return CoverageVerdict(
        issue=issue,
        covers=covers,
        evidence=tuple(evidence),
        reasoning=reasoning,
    )


def decide_coverage(
    issue: int,
    body: str,
    signals: CandidateSignals,
    verdict: Any | None = None,
    repo: str | None = None,
    targets_default_branch: bool = True,
) -> CoverageDecision:
    """Decide whether `body`'s PR covers `issue`.

    `repo` is the repository being triaged ("owner/name"); pass it so a reference
    scoped to another repository cannot be mistaken for one of ours.

    `targets_default_branch` reflects GitHub's own restriction: a closing keyword
    only closes an issue when the PR targets the default branch. A release-branch
    PR saying "Fixes #42" will not close #42, so it is not mechanical coverage.
    It defaults True for the common case, and that default is the one place here
    that assumes rather than verifies — pass the PR's real `baseRefName` answer
    whenever you have it, which the formula's fetch step now retrieves.

    Fails safe everywhere it decides: absent a closing keyword or a schema-valid
    covering verdict, the issue stays authorable. Candidate signals are carried
    onto the decision for the report, and never demote on their own.
    """
    if targets_default_branch and issue in closing_keyword_refs(body, repo):
        return CoverageDecision(True, CoverageBasis.CLOSING_KEYWORD, signals, ())

    if verdict is None:
        return CoverageDecision(False, CoverageBasis.NO_COVERAGE_EVIDENCE, signals, ())

    try:
        checked = validate_coverage_verdict(verdict, body, expected_issue=issue)
    except CoverageVerdictError:
        return CoverageDecision(False, CoverageBasis.INVALID_VERDICT, signals, ())

    return CoverageDecision(checked.covers, CoverageBasis.MODEL_VERDICT, signals, checked.evidence)
