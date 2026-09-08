"""Guard outbound Slack text against accidental mrkdwn strikethrough.

Slack's mrkdwn pairs tilde characters into strikethrough, and the pairing
is looser than any documented rule: a body containing "~$58.5k … ~$16.5k"
(tilde as "approximately") rendered half the message struck through
(gp-o42, C0BEZ3CQK5X ts=1787203992.825369). Slack offers NO escape
sequence for formatting characters — a backslash renders literally and
``mrkdwn: false`` would also kill intentional *bold* and bullets — so the
only way to make a tilde inert is to substitute a visually equivalent
codepoint that Slack's parser does not treat as a delimiter.

The guard is deliberately conservative about which bytes it touches:

* Code spans (``` fences and `inline`) are never modified — Slack does
  not format inside them, and literal bytes there must survive
  copy-paste. An unterminated ``` fence is code too: Slack formats it as
  a code block through end-of-message. Their bytes are masked, not cut
  out, before the census below, because Slack pairs tildes *across* an
  inline span (``~$5k (see `calc.py`) vs ~$7k`` is one pairable line).

  That unterminated-fence rule rests on a premise about Slack's
  renderer, recorded here because it is this module's ONE
  under-substituting branch. Premise: an unmatched triple-backtick
  renders as a code block through end-of-message. Evidence: two
  independently executed reviewer probes (PR #337 iteration-1 review,
  finding S4, codex + gemini), which prescribed this trailing
  alternative under DOTALL. It has never been checked against a live
  Slack fixture. The alternative is unanchored, so ANY triple-backtick,
  including one mid-sentence in prose, suspends the guard for the rest
  of the message — a line reading "type ``` then code, runway ~$58.5k
  to ~$16.5k" passes through with both ASCII tildes intact. Every other
  decision here fails toward substituting (worst case: a cosmetic
  U+223C in text that was fine); this one fails the other way, toward
  leaving a pairable ASCII tilde, which is the gp-o42 bug itself. It is
  kept because corrupting bytes that Slack renders as code is the more
  concrete harm and a stray unmatched fence in prose is rare. If the
  premise is falsified, the narrowing that follows is anchoring this
  alternative to a line-start fence. The behavior is pinned by
  ``test_stray_midline_fence_suspends_the_guard``.
* A line with fewer than two tildes is never modified — strikethrough
  needs a pair on the same line (Slack inline formatting does not cross
  newlines), so a lone ``cd ~/repo`` keeps its ASCII byte. "Lone" is per
  rendered line: ``rsync ~/a ~/b`` puts two on one line and both are
  substituted, so pass twin paths in a code span or use ``--raw``.
* On a line where pairing is possible, a tilde survives only as half of
  a deliberate tight-wrapped pair (``~word~`` / ``~a few words~``): the
  opener sits at start-of-line or after whitespace/bracket/quote or a
  mrkdwn delimiter (``*``/``_``) and is immediately followed by a plain
  character; the closer immediately follows a plain character and is
  followed by end/whitespace/punctuation; the span stays on one line
  and contains no other tilde.
  Per the gp-o42 directive, a tilde immediately preceding a digit or a
  currency symbol (optionally signed: ``~-$13.5k``, ``~.5``) reads as
  "approximately" and is NEVER a delimiter, so it never opens a pair.
* Everything else becomes U+223C TILDE OPERATOR — near-identical glyph,
  guaranteed inert. Callers that need exact bytes end-to-end should use
  a code span or the send commands' ``--raw`` flag.

Underscores and asterisks are deliberately NOT guarded: accidental
italics are low-harm and legible (unlike strikethrough), snake_case
underscores are mid-word and cannot delimit anyway, and substituting
homoglyphs into identifiers would corrupt copy-pasted commands — a worse
papercut than the one being fixed. Revisit only with an observed repro.
"""

from __future__ import annotations

import re

# U+223C TILDE OPERATOR: renders near-identically to ASCII "~" in Slack's
# message font but is never a mrkdwn delimiter.
TILDE_SUBSTITUTE = "∼"

# Closed fenced blocks first (they may span lines), then an unterminated
# fence — which Slack renders as a code block through end-of-message, so its
# bytes are code as well — then single-line inline code.
_CODE_SPAN_RE = re.compile(r"```.*?```|```.*|`[^`\n]+`", re.DOTALL)

# Code-span bytes are masked one-for-one with this character before the
# tilde census runs, so every offset still lines up with the original text.
# It has to read as an ordinary "plain" character: not a tilde, not in either
# boundary set below, and neither a digit nor a currency symbol — otherwise
# masking would itself change which tildes look like deliberate delimiters.
_MASK_CHAR = "\x00"

_CURRENCY = "$€£¥₩₹¢₿"  # $ € £ ¥ ₩ ₹ ¢ ₿
# A mrkdwn delimiter is a legitimate span boundary: Slack renders ``*~text~*``
# as bold strikethrough, so a deliberate pair may be wrapped in one. ``\r`` is
# a closer follower only — ``_guard_segment`` splits on ``\n``, so a CRLF body
# leaves the ``\r`` at end of line, never before an opener.
_OPENER_PRECEDERS = set(" \t([{'\"*_")
_CLOSER_FOLLOWERS = set(" \t.,;:!?)]}'\"*_\r")


def _never_delimiter(line: str, i: int) -> bool:
    """A tilde reading as "approximately": ~ before an optionally-signed
    digit/currency/decimal (``~$58.5k``, ``~-$13.5k``, ``~9/2``, ``~.5``)."""
    rest = line[i + 1:]
    if rest[:1] in ("-", "+"):
        rest = rest[1:]
    if rest[:1] == "." and rest[1:2].isdigit():
        return True
    return rest[:1].isdigit() or rest[:1] in _CURRENCY


def _is_opener(line: str, i: int) -> bool:
    if i > 0 and line[i - 1] not in _OPENER_PRECEDERS:
        return False
    if i + 1 >= len(line):
        return False
    nxt = line[i + 1]
    if nxt in " \t~":
        return False
    return not _never_delimiter(line, i)


def _is_closer(line: str, j: int) -> bool:
    if j == 0 or line[j - 1] in " \t~":
        return False
    return j + 1 >= len(line) or line[j + 1] in _CLOSER_FOLLOWERS


def _guard_line(line: str) -> str:
    positions = [i for i, ch in enumerate(line) if ch == "~"]
    if len(positions) < 2:
        # A lone tilde cannot pair, so it can never strike: keep the byte.
        return line
    keep: set[int] = set()
    k = 0
    while k < len(positions) - 1:
        i, j = positions[k], positions[k + 1]
        # A deliberate strikethrough is a tight-wrapped pair whose span
        # contains no other tilde — the very next tilde must be the closer.
        if _is_opener(line, i) and j > i + 1 and _is_closer(line, j):
            keep.update((i, j))
            k += 2
        else:
            k += 1
    return "".join(
        TILDE_SUBSTITUTE if ch == "~" and i not in keep else ch
        for i, ch in enumerate(line)
    )


def _guard_segment(segment: str) -> str:
    if "~" not in segment:
        return segment
    return "\n".join(_guard_line(ln) for ln in segment.split("\n"))


def escape_accidental_mrkdwn(text: str) -> str:
    """Neutralize tildes that would accidentally strike through, keeping
    deliberate ``~word~`` strikethrough, code spans, and every other
    formatting character (``*bold*``, bullets, ``_italics_``) untouched.
    Idempotent: guarding already-guarded text is a no-op."""
    if "~" not in text:
        return text
    spans = [m.span() for m in _CODE_SPAN_RE.finditer(text)]
    if not spans:
        return _guard_segment(text)
    # Slack pairs tildes across an inline code span, so the census has to see
    # the whole rendered line. Guarding each inter-span segment separately
    # would restart the count at every span and let a pair through. Mask the
    # span bytes instead — keeping their newlines, and so the line boundaries
    # — guard the masked text, then put the original bytes back. Substitution
    # is one codepoint for one, so the guarded text is the same length as the
    # input and the span offsets still hold.
    masked = list(text)
    for start, end in spans:
        for i in range(start, end):
            if masked[i] != "\n":
                masked[i] = _MASK_CHAR
    guarded = _guard_segment("".join(masked))
    out: list[str] = []
    last = 0
    for start, end in spans:
        out.append(guarded[last:start])
        out.append(text[start:end])
        last = end
    out.append(guarded[last:])
    return "".join(out)
