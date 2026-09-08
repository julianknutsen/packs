"""Tests for the accidental-mrkdwn guard (gp-o42).

The repro: the mayor's runway summary in C0BEZ3CQK5X (ts=1787203992.825369)
contained "~$58.5k → *~$16.5k …*" — tildes meaning "approximately" — and
Slack's mrkdwn paired them into strikethrough across half the message.
Slack has no escape sequence for formatting characters, so the guard
substitutes U+223C TILDE OPERATOR for any tilde that could pair
accidentally, while leaving deliberate ~word~ strikethrough, code spans,
lone tildes, and all other formatting untouched.
"""

from __future__ import annotations

import pathlib
import sys

PACK_DIR = pathlib.Path(__file__).resolve().parent.parent
SCRIPTS_DIR = PACK_DIR / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import slack_mrkdwn  # noqa: E402

guard = slack_mrkdwn.escape_accidental_mrkdwn
SUB = slack_mrkdwn.TILDE_SUBSTITUTE


# The line that actually struck through in the wild (verbatim).
REPRO_LINE = "• Total out: ~$58.5k → *~$16.5k left on Sep 30* from a $75k start."

# A fuller reconstruction of the affected message body.
REPRO_BODY = "\n".join([
    "Math first (Aug 20 → Sep 30 = 41 days ≈ 1.35 months):",
    "• Founders $15k/mo + Anthropic $5k/mo + JP WeWork $1.1k/mo = $21.1k/mo × 1.35 ≈ $28.5k",
    "• China team: $30k one-time (covers them to 9/30)",
    REPRO_LINE,
    "",
    "If your $75k doesn't already include that wire, landing it puts you at *~$93.5k on Sep 30*.",
    "1. *Your final $30k draw* — the no-wire branch goes NEGATIVE (~-$13.5k).",
    "4. Small: 1Password needs a card by ~9/2; Shenzhen trip hotels.",
    "The China team needs re-funding (~$22k/mo at the current rate), so all-in run-rate becomes ~$43k/mo.",
])


def test_repro_line_has_no_pairable_tildes() -> None:
    out = guard(REPRO_LINE)
    assert "~" not in out  # both approximately-tildes neutralized
    assert out == REPRO_LINE.replace("~", SUB)
    # Intentional bold on the same line is untouched.
    assert "*~$16.5k left on Sep 30*".replace("~", SUB) in out


def test_repro_body_no_line_can_strike() -> None:
    out = guard(REPRO_BODY)
    # No line retains two ASCII tildes, so Slack can never pair one.
    assert all(line.count("~") < 2 for line in out.splitlines())
    # Nothing else changed: restoring the tildes reproduces the input.
    assert out.replace(SUB, "~") == REPRO_BODY


def test_lone_tilde_lines_keep_their_ascii_byte() -> None:
    for text in (
        "the no-wire branch goes NEGATIVE (~-$13.5k). This is the number.",
        "1Password needs a card by ~9/2 at the latest",
        "cd ~/repo and run make",
        "runway ends ~October",
    ):
        assert guard(text) == text


def test_deliberate_strikethrough_is_preserved() -> None:
    assert guard("that idea is ~dead~ revived") == "that idea is ~dead~ revived"
    assert guard("~all of this~ was wrong") == "~all of this~ was wrong"
    # Two deliberate strikes on one line both survive.
    assert guard("~old~ new and ~stale~ fresh") == "~old~ new and ~stale~ fresh"


def test_approximately_tilde_never_opens_a_pair() -> None:
    # Currency, signed, decimal, and plain-digit forms (two per line so
    # pairing is possible and the guard must engage).
    assert guard("~$58.5k out, ~$16.5k left") == \
        f"{SUB}$58.5k out, {SUB}$16.5k left"
    assert guard("~-$13.5k versus ~+$2k") == f"{SUB}-$13.5k versus {SUB}+$2k"
    assert guard("~.5 days or ~2 weeks") == f"{SUB}.5 days or {SUB}2 weeks"
    assert guard("~5k€ and ~£3k") == f"{SUB}5k€ and {SUB}£3k"


def test_accidental_pair_with_deliberate_neighbor() -> None:
    # The approximately-tilde is neutralized; the deliberate pair stands.
    assert guard("~$5k budget, ~overspent~ fixed") == \
        f"{SUB}$5k budget, ~overspent~ fixed"


def test_code_spans_are_untouched() -> None:
    inline = "run `diff ~/a ~/b` then `x ~ y`"
    assert guard(inline) == inline
    fenced = "before ~$1 and ~$2\n```\n~$58.5k ~$16.5k\ncd ~/repo\n```\nafter"
    out = guard(fenced)
    assert "```\n~$58.5k ~$16.5k\ncd ~/repo\n```" in out
    assert out.startswith(f"before {SUB}$1 and {SUB}$2")


def test_other_formatting_is_untouched() -> None:
    text = "*bold* _italic_ `code` • bullet\n> quote with snake_case_name"
    assert guard(text) == text


def test_idempotent_and_cheap_on_tilde_free_text() -> None:
    text = "plain message, *bold*, no tildes"
    assert guard(text) is text
    once = guard(REPRO_BODY)
    assert guard(once) == once


def test_unclosed_fence_is_code_and_outside_text_is_still_guarded() -> None:
    text = "~$1 vs ~$2\n```unterminated\n~$3 ~$4"
    out = guard(text)
    # Prose before the fence still pairs, so it is guarded.
    assert out.splitlines()[0] == f"{SUB}$1 vs {SUB}$2"
    # Slack renders an unterminated fence as a code block through
    # end-of-message, so those bytes must survive copy-paste verbatim.
    assert out.endswith("```unterminated\n~$3 ~$4")


def test_stray_midline_fence_suspends_the_guard() -> None:
    """Pin the guard's one under-substituting branch, deliberately.

    The unterminated-fence alternative is unanchored, so a triple
    backtick anywhere — including mid-sentence in ordinary prose —
    reads as "code through end-of-message" and passes the remainder
    through untouched. That is the module's only branch that fails
    toward leaving a pairable ASCII tilde (i.e. toward the gp-o42 bug)
    rather than toward substituting; see the premise recorded in the
    module docstring. This test documents the current decision, so
    anchoring the alternative to a line-start fence has to be a
    deliberate change rather than a silent one.
    """
    stray = "type ``` then code, runway ~$58.5k to ~$16.5k"
    assert guard(stray) == stray

    # Control: the identical line without the stray fence IS guarded.
    # Without this, the assertion above would also pass if the tildes
    # had simply stopped being pairable for some unrelated reason.
    control = stray.replace("``` ", "")
    assert guard(control) == control.replace("~$", f"{SUB}$")

    # The suspension runs to end-of-text, across later lines too.
    spanning = "use ``` for code\n\nbudget ~$5k vs ~$7k"
    assert guard(spanning) == spanning


def test_code_span_does_not_reset_the_tilde_census() -> None:
    """A code span between two accidental tildes must not hide the pair.

    Slack applies emphasis *around* inline code, so the two tildes are on
    one rendered line and pair — guarding each inter-span segment on its
    own counted one tilde per segment and let both through.
    """
    spanned = "budget ~$5k (see `calc.py`) vs ~$7k"
    assert guard(spanned) == f"budget {SUB}$5k (see `calc.py`) vs {SUB}$7k"
    # Control: the identical line without the span was always guarded.
    assert guard("budget ~$5k vs ~$7k") == f"budget {SUB}$5k vs {SUB}$7k"
    # A single-line fence segments the same way.
    assert guard("~$1 ```x``` ~$2") == f"{SUB}$1 ```x``` {SUB}$2"
    # The routine agent shape from the report.
    assert guard("bump `MAX_RETRIES` to ~5 and `TIMEOUT` to ~30") == \
        f"bump `MAX_RETRIES` to {SUB}5 and `TIMEOUT` to {SUB}30"
    # The span's own bytes are still returned untouched.
    assert "`calc.py`" in guard(spanned)


def test_deliberate_pair_survives_beside_and_inside_formatting() -> None:
    # A code span on the same line does not disturb a real pair.
    assert guard("run `x` ~word~ done") == "run `x` ~word~ done"
    # Slack renders these as bold/italic strikethrough: the formatting
    # delimiter is a legitimate span boundary, not a stray character.
    assert guard("verdict: *~cancelled~* moving on") == \
        "verdict: *~cancelled~* moving on"
    assert guard("verdict: _~cancelled~_ moving on") == \
        "verdict: _~cancelled~_ moving on"


def test_crlf_body_keeps_a_deliberate_pair() -> None:
    # --body-file bodies can arrive CRLF-terminated; the \r trails the
    # closer, so it has to count as a closer follower.
    assert guard("that is ~wrong~\r\nnext line") == "that is ~wrong~\r\nnext line"


def test_bitcoin_sign_reads_as_approximately() -> None:
    assert guard("~₿0.5 then ~₿0.7") == f"{SUB}₿0.5 then {SUB}₿0.7"
    # The shape that bites: if ₿ were missing from the currency set, this
    # first tilde would read as an opener, pair with the tilde after
    # "today", and Slack would strike the whole clause through.
    assert guard("~₿0.5 today~ and more") == f"{SUB}₿0.5 today{SUB} and more"


def test_twin_home_paths_on_one_line_are_substituted() -> None:
    """The documented limit of the "lone tilde survives" promise.

    "Lone" is per rendered line: two home-relative paths on one line are
    a pairable pair, and substituting is the fail-safe direction (Slack
    would strike the text through otherwise). Copy-paste fidelity there
    needs a code span or --raw — the docs say so; this pins the behavior
    so changing it stays a decision.
    """
    assert guard("rsync -a ~/src ~/dst") == f"rsync -a {SUB}/src {SUB}/dst"
    assert guard("rsync -a `~/src ~/dst`") == "rsync -a `~/src ~/dst`"


def test_tilde_substitute_is_exactly_one_codepoint() -> None:
    # U+223C TILDE OPERATOR, and single-width: the guard masks code spans
    # in place and restores them by offset, which needs a 1:1 substitution.
    assert SUB == "∼"
    assert len(SUB) == 1
