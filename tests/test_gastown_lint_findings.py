"""Hold `gc lint gastown` at a pinned finding set.

gastown is a supported pack. It is exercised by supported-pack-nightly.yml and
it is NOT in the `gc lint` loop in .github/workflows/ci.yml, which is how it
accumulated fourteen findings that nothing failed on: nine prompt templates and
formulas telling agents to run bd flags that do not exist, and five pack-level
diagnostics.

Seven of those were real and are fixed. The rest are defects in gc's own
linter, enumerated with their gc-side cause in
`tests/gastown_lint_upstream_defects.txt`.

A NEW finding fails, which is the regression this exists to catch. A pinned
finding that DISAPPEARS also fails, because that is the upstream fix landing
and the waiver should be deleted in the same change rather than quietly
outliving it.

Which findings gc emits depends on which gc ran, and this test does not paper
over that. `.github/workflows/ci.yml` installs `gc@latest`; twenty-two findings
that a released gc reports were fixed on gascity main by 7724983de and are
absent from a dev build. So the waiver file is sectioned: entries every gc
reports are required, and a version-dependent section is tolerated but has to
be wholly present or wholly absent. That is what the sections are for, and it
is why the first run of this test in CI went red while it was green locally.

Fidelity, and where it stops: this runs the real `gc lint` against the real
pack, so it is not a fixture reimplementation of the linter and it cannot drift
from it. What it cannot do is check a flag `gc lint` does not know about --
internal/bdflags/bdflags.go carries a manifest of bd subcommands, and anything
outside that manifest is skipped by design. It is also line-oriented and does
not join backslash-continued shell lines, so the three multi-line `gc bd create
... \\ --labels=warrant` invocations fixed alongside this test were never
reported by it at all. A green here means "no NEW finding of a kind gc lint can
see", not "every bead command in the pack is valid".
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import unittest
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
PACK = "gastown"
PACK_DIR = REPO_ROOT / PACK
WAIVER = Path(__file__).with_name("gastown_lint_upstream_defects.txt")

SECTION_DIRECTIVE = "# @section:"
UNIVERSAL = "universal"

# The findings gc emits depend on which gc ran. `.github/workflows/ci.yml`
# installs `gc@latest`; a contributor is as likely to have a build from main.
# So the waiver file separates findings every gc reports from findings a
# released gc reports and main no longer does, and this test holds each to the
# assertion that is true of it. Collapsing the two would mean either CI red on
# an upstream release, or a real regression waved through on a dev build.
TOLERATED_SECTIONS = ("pre-5220",)


def gc_binary() -> str | None:
    """The gc binary to lint with, or None when there is none to use.

    GC_TEST_BIN is how .github/workflows/ci.yml already hands a freshly built
    gc to tests/test_gc_role_prompt_integration.py; honor the same variable so
    the two run off one installation.
    """
    pinned = os.environ.get("GC_TEST_BIN", "").strip()
    if pinned:
        return pinned
    return shutil.which("gc")


def waived_findings() -> dict[str, Counter[str]]:
    """The pinned findings by section, read off disk rather than inlined here.

    Counters, not sets: two diagnostics that normalize to the same key are two
    findings, and collapsing them would let a duplicate arrive unnoticed.

    An unknown section name is an error rather than a silent extra bucket. A
    typo there would otherwise waive its lines against nothing and read as a
    clean pass.
    """
    sections: dict[str, Counter[str]] = {
        name: Counter() for name in (UNIVERSAL, *TOLERATED_SECTIONS)
    }
    current = UNIVERSAL
    for raw in WAIVER.read_text().splitlines():
        line = raw.strip()
        if line.startswith(SECTION_DIRECTIVE):
            current = line[len(SECTION_DIRECTIVE) :].strip()
            if current not in sections:
                raise AssertionError(
                    f"{WAIVER.name} names section {current!r}, which this test "
                    f"does not know. Known: {sorted(sections)}."
                )
            continue
        if not line or line.startswith("#"):
            continue
        sections[current][line] += 1
    return sections


# gc lint exits 0 when a pack is clean and 1 when it has findings. Both are
# reports. Anything else is the tool failing, and a failing tool must not be
# read as "no new findings" -- that is the shape where a guard goes green
# because nothing looked, rather than because nothing was wrong.
LINT_REPORTING_EXITS = (0, 1)


def observed_findings(gc_bin: str) -> Counter[str]:
    """Run `gc lint gastown --json` and normalize its diagnostics.

    Paths come back absolute, so they are made relative to the pack directory:
    a finding key must be identical on a contributor's machine and on a runner.

    The line number is deliberately NOT part of the key. It was, for one day.
    #265 rewrote the mayor prompt's rig-routing table -- four rows collapsed
    into one generic row, so the same unknown-flag diagnostic for `--rig`
    moved from lines 118-121 to lines 23 and 114, four occurrences down to two.
    Under a line-keyed waiver that read as two brand-new findings plus a
    partially-reported version section, and every open PR in the repo
    inherited a red main.

    A guard keyed on position reports edits. Keyed on path and message it
    reports findings, and the count is still load-bearing, because these are
    Counters: three identical waiver lines waive exactly three findings and a
    fourth fails. What survives: a new path, a new message, an extra
    occurrence. What no longer fails: the same finding at a different line.
    """
    proc = subprocess.run(
        [gc_bin, "lint", PACK, "--json"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode not in LINT_REPORTING_EXITS:
        raise AssertionError(
            f"gc lint exited {proc.returncode}, which is neither clean (0) nor "
            f"findings-present (1), so its output is not a report.\n"
            f"stdout: {proc.stdout[:2000]}\nstderr: {proc.stderr[:2000]}"
        )
    try:
        report = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise AssertionError(
            f"gc lint emitted no JSON report (exit {proc.returncode}).\n"
            f"stdout: {proc.stdout[:2000]}\nstderr: {proc.stderr[:2000]}"
        ) from exc

    packs = report.get("packs")
    if not isinstance(packs, list) or len(packs) != 1:
        raise AssertionError(
            f"expected exactly one pack report for {PACK!r}, got "
            f"{len(packs) if isinstance(packs, list) else packs!r}"
        )
    # Without this the guard passes on a report about some other pack, which
    # is what a typo in the invocation or a future gc argument change looks
    # like from here.
    reported = packs[0].get("name")
    if reported != PACK:
        raise AssertionError(
            f"gc lint reported on pack {reported!r}, not {PACK!r}"
        )

    return findings_from_pack_report(packs[0])


def findings_from_pack_report(pack_report: dict) -> Counter[str]:
    """Normalize one pack's diagnostics into the keyed, counted form.

    Split out from the subprocess call so the normalization has tests that do
    not need a gc on PATH. Which gc is installed decides which sections of the
    waiver are even reachable -- a post-5220 build emits none of `pre-5220` --
    so without this the largest section of the waiver is exercised by no test
    a contributor can run.
    """
    findings: Counter[str] = Counter()
    for diag in pack_report.get("diagnostics") or []:
        path = Path(diag.get("path", ""))
        try:
            rel = path.relative_to(PACK_DIR)
        except ValueError:
            rel = path
        findings[f"{rel}: {diag.get('message', '')}"] += 1
    return findings


def render(counted: Counter[str]) -> list[str]:
    """Findings as sorted lines, with a count suffix when one repeats."""
    return [
        key if n == 1 else f"{key}   (x{n})" for key, n in sorted(counted.items())
    ]


def partial_sections(observed: Counter[str]) -> dict[str, list[str]]:
    """Tolerated sections this report carries only part of, section to missing.

    The all-present-or-all-absent rule itself, as a function, so the test that
    runs a real gc and the tests that build a synthetic report go through the
    same code. Stated twice it would be two rules, and the synthetic copy would
    keep passing against itself long after the live one moved.

    A section absent in full is not partial -- that is a gc carrying the
    upstream fix, which is the entire reason the section is separate.
    """
    sections = waived_findings()
    partial: dict[str, list[str]] = {}
    for name in TOLERATED_SECTIONS:
        entries = sections[name]
        if not (entries & observed):
            continue
        missing = render(entries - observed)
        if missing:
            partial[name] = missing
    return partial


class GastownLintFindingsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.gc_bin = gc_binary()
        if not self.gc_bin:
            self.skipTest(
                "no gc binary; set GC_TEST_BIN or put gc on PATH. "
                "ci.yml installs one for the shared-role-prompt step."
            )

    def waiver_sets(self) -> tuple[Counter[str], Counter[str]]:
        sections = waived_findings()
        tolerated: Counter[str] = Counter()
        for name in TOLERATED_SECTIONS:
            tolerated += sections[name]
        return sections[UNIVERSAL], tolerated

    def test_gastown_lint_reports_nothing_outside_the_waiver(self) -> None:
        observed = observed_findings(self.gc_bin)
        universal, tolerated = self.waiver_sets()

        new = render(observed - universal - tolerated)
        self.assertFalse(
            new,
            "gc lint gastown reported findings that are not in "
            f"{WAIVER.name}:\n  " + "\n  ".join(new) + "\n"
            "If these are pack defects, fix them. If they are gc linter "
            "defects, add them to that file with the gc-side cause and a "
            "command that refutes the claim.",
        )

    def test_every_universal_waiver_entry_is_still_reported(self) -> None:
        """A waiver that outlives its defect is the failure this catches.

        Only the universal section is held to this. A version-dependent
        section legitimately vanishes when the gc under test carries the fix,
        which is the whole reason it is a separate section.
        """
        observed = observed_findings(self.gc_bin)
        universal, _ = self.waiver_sets()

        gone = render(universal - observed)
        self.assertFalse(
            gone,
            "these findings are pinned in "
            f"{WAIVER.name} but gc lint no longer reports them:\n  "
            + "\n  ".join(gone)
            + "\nThat is the upstream fix landing. Delete the entry (and its "
            "explanatory block) in the same change.",
        )

    def test_a_version_dependent_section_is_all_present_or_all_absent(self) -> None:
        """Tolerating a set is not tolerating an arbitrary subset of it.

        Without this, a real regression that happens to reuse one of these
        path/message pairs is absorbed by the waiver, and the section can rot
        an entry at a time as the pack moves under it.
        """
        partial = partial_sections(observed_findings(self.gc_bin))
        for name, missing in partial.items():
            with self.subTest(section=name):
                self.fail(
                    f"section {name!r} of {WAIVER.name} is partially reported "
                    f"by this gc. Present but missing:\n  "
                    + "\n  ".join(missing)
                    + "\nEither the pack moved under the waiver or the section "
                    "is no longer one upstream change. Re-derive it."
                )

    def test_no_bd_unknown_flag_finding_outside_the_waiver(self) -> None:
        """The pack's own half of the finding set, stated separately.

        The first test would also go red if a `named_session` diagnostic
        changed shape upstream, which is not this pack's problem. This one
        fails only on the class gastown actually owns: a prompt template or
        formula telling an agent to run a bd flag that does not exist.
        """
        observed = observed_findings(self.gc_bin)
        universal, tolerated = self.waiver_sets()
        bd_flag = Counter(
            {k: n for k, n in observed.items() if "bd-unknown-flag:" in k}
        )
        unexpected = render(bd_flag - universal - tolerated)
        self.assertFalse(
            unexpected,
            "a bd flag that does not exist is back in a gastown prompt or "
            "formula:\n  " + "\n  ".join(unexpected),
        )


class WaiverKeyingTest(unittest.TestCase):
    """The keying properties, over synthetic reports, with no gc required.

    The tests above can only see what the installed gc emits, and a post-5220
    build emits none of the `pre-5220` section -- so on a contributor's machine
    the twenty entries that broke main are read by nothing. These build the
    report instead of running for it.

    The synthetic reports are DERIVED FROM the waiver rather than restating it.
    Restating it would put the same twenty lines in two files, where the copy
    that drifts fails as the wrong thing. What is under test here is not which
    findings are waived; it is that a report of exactly the waived pairs passes
    at any line numbers, and that one more or one fewer does not.

    KNOWN GAP, left open deliberately. "One fewer" is exercised through
    `partial_sections`, which covers the tolerated sections only. The universal
    section's all-present rule lives in
    `test_every_universal_waiver_entry_is_still_reported`, which needs a real
    gc, so on a machine without one that rule is unexercised. It is not the
    rule that broke main -- a universal entry going missing means the upstream
    linter fixed something, which is loud and expected, where a tolerated
    section rotting one entry at a time is silent. Raised by the cross-family
    review at head d40a901 and recorded rather than fixed, because closing it
    means a second synthetic path for a rule the live test already covers in
    CI.
    """

    def pack_report(self, keys: Counter[str], first_line: int = 1) -> dict:
        """A gc-shaped pack report for these path/message pairs.

        Line numbers ascend and are otherwise arbitrary, which is the point:
        nothing downstream may depend on them. Paths are made absolute the way
        gc emits them, so the relative-path step is exercised too.
        """
        diagnostics = []
        line = first_line
        for key, count in sorted(keys.items()):
            rel, _, message = key.partition(": ")
            for _ in range(count):
                diagnostics.append(
                    {
                        "severity": "error",
                        "path": str(PACK_DIR / rel),
                        "line": line,
                        "message": message,
                    }
                )
                line += 7
        return {"name": PACK, "ok": False, "diagnostics": diagnostics}

    def all_waived(self) -> Counter[str]:
        sections = waived_findings()
        total: Counter[str] = Counter()
        for entries in sections.values():
            total += entries
        return total

    def test_the_same_findings_at_different_lines_are_the_same_findings(self) -> None:
        """The regression that produced this file. Two line-disjoint reports of
        the same diagnostics must be indistinguishable."""
        waived = self.all_waived()
        low = findings_from_pack_report(self.pack_report(waived, first_line=1))
        high = findings_from_pack_report(self.pack_report(waived, first_line=9000))
        self.assertEqual(low, high)
        self.assertEqual(low, waived)
        self.assertFalse(low - waived)

    def test_repeated_pairs_keep_their_count(self) -> None:
        """Dropping the line number must not collapse duplicates into one.

        The mayor prompt legitimately carries the identical diagnostic twice
        and the refinery prompt carries one four times, so a set here would
        waive an unbounded number of them.
        """
        waived = self.all_waived()
        repeated = {key: n for key, n in waived.items() if n > 1}
        self.assertTrue(
            repeated, "no waived pair repeats; this test is no longer meaningful"
        )
        observed = findings_from_pack_report(self.pack_report(waived))
        for key, n in repeated.items():
            self.assertEqual(observed[key], n, key)

    def test_one_more_occurrence_of_a_waived_pair_is_a_new_finding(self) -> None:
        """What replaces line sensitivity. A fourth of something waived three
        times has to survive subtraction."""
        waived = self.all_waived()
        target = max(waived, key=lambda k: (waived[k], k))
        extra = Counter(waived)
        extra[target] += 1
        observed = findings_from_pack_report(self.pack_report(extra))
        self.assertEqual((observed - waived), Counter({target: 1}))

    def test_a_tolerated_section_reported_short_by_one_is_partial(self) -> None:
        """The all-present-or-all-absent rule, exercised without a v1.4.1 gc."""
        sections = waived_findings()
        for name in TOLERATED_SECTIONS:
            entries = sections[name]
            with self.subTest(section=name):
                self.assertTrue(entries, f"section {name} is empty")
                # One OCCURRENCE, not one key: `del` here would withhold all
                # three of the deacon entry at once, which is a differently
                # shaped failure and renders with a count suffix.
                short = Counter(entries)
                dropped = sorted(short)[0]
                short[dropped] -= 1
                observed = findings_from_pack_report(
                    self.pack_report(sections[UNIVERSAL] + short)
                )
                self.assertTrue(
                    entries & observed, "the section must be partially present"
                )
                self.assertEqual(partial_sections(observed), {name: [dropped]})


# Placed after every class so that `python tests/test_gastown_lint_findings.py`
# runs all of them. It sat above WaiverKeyingTest for one commit, and unittest
# collects by module namespace at call time, so the four keying tests were
# silently excluded from every direct run -- green, and green about nothing.
# pytest imports the whole module and was unaffected, which is exactly why
# nobody would have noticed.
if __name__ == "__main__":
    unittest.main()
