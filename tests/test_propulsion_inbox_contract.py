"""Contract test for the propulsion fragment's two-queue and archive semantics.

Issue #342 names two properties this fragment has to hold, and neither is
visible to `gc lint` or to any per-pack suite: they are properties of prose.

1. Ordering. Inbox processing is never conditional on the hook being empty.
   The defect this replaces read `3. If it returns no work, process inbox`,
   which linted clean forever while letting a steady hook queue starve mail.
2. Completion semantics. Reading clears unread; archiving happens only after
   the obligation is resolved or is represented by durable tracked work.

Both are one careless edit away from regressing — the shape of the regression
is a helpful-looking simplification ("archive when you're done reading"), not a
syntax error. So the load-bearing sentences are pinned here by exact text, and
the known-bad shapes are pinned as prohibitions. An edit that drops the archive
discipline, or that reinstates hook-empty gating, turns this suite red.

Prohibitions are asserted against the same source the positive assertions read,
so a rename of the fragment file fails collection rather than passing vacuously
(see `test_fragment_source_is_where_we_think_it_is`).

Mail retention wording is pinned too. Gas City's `mail.retention_ttl` purges
read *wisp-tier* messages only when set to a nonzero duration; empty or "0"
disables the purge, and main-tier messages are preserved either way
(`internal/config/config.go` MailConfig.RetentionTTLDuration,
`internal/mail/beadmail/beadmail.go` PurgeReadMessageWisps). Guidance that
tells a seat every read message is inherently temporary is factually wrong, so
the phrasings that assert it are prohibited.
"""

from __future__ import annotations

from pathlib import Path
import re

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
FRAGMENT = REPO_ROOT / "gastown" / "template-fragments" / "propulsion.template.md"

DEFINE = re.compile(
    r'\{\{\s*define\s+"(?P<name>[^"]+)"\s*\}\}(?P<body>.*?)\{\{\s*end\s*\}\}',
    re.DOTALL,
)

# The sentences the doctrine rests on. Each is a whole thought, not a keyword,
# so a reword that keeps the meaning is a deliberate edit to this list rather
# than an accident.
LOAD_BEARING = (
    # Two queues, and the inbox is a peer of the hook.
    "you have TWO work queues and they are PEERS",
    "Inbox processing is never conditional on the hook being empty.",
    "Neither queue is the fallback for the other.",
    # The completion rule, stated once, in both directions.
    "IT IS NOT DONE UNTIL IT IS ARCHIVED, AND IT IS NOT ARCHIVABLE UNTIL IT IS RESOLVED.",
    "ARCHIVE ONLY WHEN THE OBLIGATION IS RESOLVED, OR WHEN IT IS REPRESENTED BY > DURABLE TRACKED WORK",
    # Reading and archiving are different acts.
    "Reading clears the unread count.",
    "NEVER archive to clear a count.",
    # Retention, stated as the configurable behavior it is.
    "`mail.retention_ttl` purges read wisp-tier messages once it is set to a nonzero duration",
    "disables that purge entirely when it is zero or empty",
    "main-tier messages are preserved either way",
)

# Shapes that were wrong before, or would be wrong if reintroduced.
PROHIBITED = (
    # The starvation defect: inbox work gated on an empty hook.
    ("If it returns no work, **process inbox", "inbox gated on an empty hook"),
    ("If it returns no work, check mail", "inbox gated on an empty hook"),
    # Retention claims contradicting mail.retention_ttl.
    ("will not persist", "claims read mail is inherently temporary"),
    ("is not durable storage", "claims read mail is inherently temporary"),
    ("lifecycle-managed", "claims read mail is inherently temporary"),
    # Archiving sold as a way to reach a clean count.
    ("Read + archive is fine", "archiving offered as a substitute for resolving"),
)

# Roles whose startup guidance must reach the inbox unconditionally. The other
# propulsion roles (deacon, witness, refinery, polecat, dog) run patrol wisps or
# a scripted claim block and have no startup inbox step to gate.
INBOX_STARTUP_ROLES = ("propulsion-mayor", "propulsion-crew")


def fragment_text() -> str:
    return FRAGMENT.read_text(encoding="utf-8")


def fragment_blocks() -> dict[str, str]:
    return {m.group("name"): m.group("body") for m in DEFINE.finditer(fragment_text())}


def flat(text: str) -> str:
    """Collapse wrapping so a pinned sentence survives a reflow of the prose.

    The pins are about wording, not about where the 80th column lands; a
    contributor rewrapping a paragraph should not have to touch this file.
    """
    return re.sub(r"\s+", " ", text)


def test_fragment_source_is_where_we_think_it_is() -> None:
    """Without this, a moved or renamed fragment makes every check below vacuous."""
    assert FRAGMENT.is_file(), f"propulsion fragment not found at {FRAGMENT}"
    assert 'define "propulsion-base"' in fragment_text()


@pytest.mark.parametrize("sentence", LOAD_BEARING)
def test_base_fragment_keeps_load_bearing_sentence(sentence: str) -> None:
    assert sentence in flat(fragment_blocks()["propulsion-base"]), (
        "propulsion-base no longer carries a load-bearing sentence from #342. "
        "If this is intentional, change the sentence here in the same commit "
        f"and say why in the message: {sentence!r}"
    )


@pytest.mark.parametrize(("phrase", "why"), PROHIBITED)
def test_fragment_avoids_known_bad_phrasing(phrase: str, why: str) -> None:
    assert phrase not in flat(fragment_text()), f"{phrase!r} reintroduces: {why}"


@pytest.mark.parametrize("role", INBOX_STARTUP_ROLES)
def test_startup_reaches_inbox_without_an_empty_hook(role: str) -> None:
    """The step that reaches the inbox must not sit behind a hook-empty branch."""
    body = fragment_blocks()[role]
    startup = body.split("**Your startup behavior:**", 1)
    assert len(startup) == 2, f"{role} has no startup block to check"
    steps = startup[1]

    inbox_lines = [
        line
        for line in steps.splitlines()
        if re.search(r"\binbox\b|\bmail\b", line, re.IGNORECASE)
    ]
    assert inbox_lines, f"{role} startup never reaches the inbox"

    # Walk numbered steps so a conditional two lines above still counts as the
    # guard on the step it introduces.
    current_step: list[str] = []
    for line in steps.splitlines():
        if re.match(r"\s*\d+\.\s", line):
            current_step = [line]
        elif current_step:
            current_step.append(line)
        if not current_step:
            continue
        if not re.search(r"\binbox\b|\bmail\b", line, re.IGNORECASE):
            continue
        step_text = "\n".join(current_step)
        assert "no work" not in flat(step_text), (
            f"{role}: inbox processing is gated on an empty hook — this is the "
            f"#342 starvation defect.\n{step_text}"
        )


@pytest.mark.parametrize("role", INBOX_STARTUP_ROLES)
def test_inbox_startup_states_the_archive_precondition(role: str) -> None:
    body = fragment_blocks()[role]
    assert "resolved or represented by durable tracked work" in flat(body), (
        f"{role} states an inbox step without the archive precondition; the two "
        "have to travel together or a seat reads 'process the inbox' as "
        "'empty the inbox'."
    )


def test_every_role_composes_the_base_fragment() -> None:
    """No role may carry inbox guidance that skips the shared rule."""
    blocks = fragment_blocks()
    roles = [name for name in blocks if name != "propulsion-base"]
    assert roles, "no propulsion role fragments found"
    for role in roles:
        assert '{{ template "propulsion-base" . }}' in blocks[role], (
            f"{role} does not compose propulsion-base"
        )


def test_archive_rule_is_stated_once_in_the_base() -> None:
    """#342 asks for one concise rule; two phrasings is how they drift apart."""
    base = fragment_blocks()["propulsion-base"]
    assert flat(base).count("ARCHIVE ONLY WHEN THE OBLIGATION IS RESOLVED") == 1
