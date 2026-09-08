"""Inventory check: packs must not name a Beads subcommand refused by the proxy.

This mirrors gascity's `TestActivePacksDoNotIntroduceUntrackedProxyRefusals`
(`internal/builtinpacks/proxy_capabilities_test.go`), which walks the embedded
packs line by line for front-door commands that Beads v1.3.0-rc.1 refuses in
proxied-server mode. That gate walks all five bundled packs (`core`, `bd`,
`dolt`, `gastown`, `gascity`); this mirror covers the two whose content is
authored in this repository, because the other three are embedded from
gascity's own tree. The gate ships with its owning bead's work and is not on
gascity `origin/main` yet, so the path above resolves only in a branch that
carries it. That scanner keys each finding by
`pack/path:subcommand` and tolerates one only while an issue-backed exception
is in flight, so a pack line matching it here is a gap that goes red downstream
as soon as the exception is retired. Checking it on this side keeps the packs
clean without waiting for a module pin bump to report it.

The check is textual and per-line, exactly like the scanner: a prohibition
matches the same way an instruction does, so guidance that warns against a
refused subcommand must not spell it adjacent to the command name.

This file belongs in the repo root `tests/` tree, which is outside `embed.go`'s
`//go:embed all:gastown all:gascity`. Moving it under `gastown/tests/` or
`gascity/tests/` would place its own fixture strings inside the scanned
filesystem, where they would trip the check they implement.
"""

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PROMPT = ROOT / "gastown/agents/mayor/prompt.template.md"
PACK_DIRS = ("gastown", "gascity")

# Faithful mirror of the scanner's two patterns, including its leading
# delimiter class (backtick included, so fenced prose is not exempt). The
# command name is concatenated rather than spelled inline so this file does not
# itself read as an invocation to `test_no_bare_bd_commands.py`.
_SPACE = r"[\t\n\v\f\r ]"
_LEAD = "(?:^|[\t\n\v\f\r \"'`();|])"
_PREFIX = _LEAD + r"(?:gc" + _SPACE + r"+)?" + "bd" + _SPACE + r"+"
REFUSED_COMMAND = re.compile(_PREFIX + r"(doctor|backup|rename-prefix)\b")
REFUSED_WATCH = re.compile(_PREFIX + r"show\b[^\r\n]*--watch\b")

# The scanner skips comment lines; a pack does not execute prose.
COMMENT_PREFIXES = ("#", "//", "<!--")

# The remediation imperative must point at the surface that actually assigns a
# rig's prefix. routes.jsonl is generated: gascity's writeRoutesFile
# (cmd/gc/rig_beads.go) rebuilds it from the loaded city config and renames it
# over the target, so a hand edit there is discarded whole and never changes
# the prefix the rig's store mints. The block may still mention routes.jsonl
# descriptively, so pin the destination of the assignment rather than the mere
# absence of the generated path.
CONFLICTS_ANCHOR = "**Conflicts:**"
ASSIGNMENT_ANCHOR = "unique prefix"
CONFIG_FILE = "city.toml"
FILE_TOKEN = re.compile(r"\b([\w-]+\.(?:toml|jsonl))\b")

# Positional pinning alone is blind to regression by *addition*: it reads the
# first file token after the assignment clause, so a second, prescriptive
# routes.jsonl instruction appended behind it — or the descriptive clause
# swapped for a hand-edit one — leaves it green. The framing pin below covers
# that axis by counting the mentions and requiring the surviving one to be
# described as generated.
ARTIFACT_FILE = "routes.jsonl"
GENERATED_FRAMING = re.compile(r"\b(?:re)?generat\w*", re.IGNORECASE)
# A sentence ends at .!? followed by whitespace. The paths in this block put no
# whitespace after their dots ("city.toml", "{{ .CityRoot }}/.beads/..."), so
# they do not split; the trailing ")." of the parenthetical does.
SENTENCE_BREAK = re.compile(r"(?<=[.!?])\s")


def refused_command(line):
    """Return the refused subcommand named on this line, or None."""
    if REFUSED_WATCH.search(line):
        return "show --watch"
    match = REFUSED_COMMAND.search(line)
    return match.group(1) if match else None


def scan_packs():
    findings = []
    for pack in PACK_DIRS:
        pack_root = ROOT / pack
        assert pack_root.is_dir(), f"embedded pack {pack} is missing"
        # The working tree is the deliberate walk target, unlike the sibling
        # inventory guard in test_no_bare_bd_commands.py, which enumerates
        # tracked files. `//go:embed all:gastown all:gascity` reads the
        # filesystem at build time and never consults git, so an untracked pack
        # file really is embedded by a local build and really is in scope. The
        # cost is a local-only red when an untracked scratch file names a
        # refused subcommand; that is a true report about the local build, and
        # it keeps this file runnable from a plain copy of the tree.
        for path in sorted(pack_root.rglob("*")):
            if not path.is_file():
                continue
            try:
                text = path.read_text(encoding="utf-8")
            except (UnicodeDecodeError, OSError):
                continue
            for number, line in enumerate(text.split("\n"), 1):
                stripped = line.strip()
                if not stripped or stripped.startswith(COMMENT_PREFIXES):
                    continue
                command = refused_command(line)
                if command is not None:
                    findings.append(
                        f"{path.relative_to(ROOT)}:{number} names refused "
                        f"proxied command {command!r}: {stripped}"
                    )
    return findings


def test_packs_name_no_proxy_refused_subcommand():
    findings = scan_packs()
    assert not findings, "\n".join(
        ["packs name commands Beads refuses in proxied-server mode:"] + findings
    )


def test_collision_guidance_is_proxy_safe():
    text = PROMPT.read_text()
    assert "Prefix collisions are fatal" in text
    assert "do not run" in text
    assert "verified backup" in text
    assert "never delete or rewrite" in text


def conflicts_paragraph(text):
    """Return the Conflicts block as one whitespace-normalized line.

    Fails closed naming the anchor it could not find: a missing start anchor
    would yield an empty string and a missing end anchor would run to the end
    of the file, and either one turns the assertions below into checks that
    pass for the wrong reason. Backticks and line breaks are flattened so the
    pin measures the wording rather than the current wrap.
    """
    start = text.find(CONFLICTS_ANCHOR)
    assert start != -1, f"missing {CONFLICTS_ANCHOR!r} anchor in {PROMPT}"
    end = text.find("\n\n", start)
    assert end != -1, f"{CONFLICTS_ANCHOR!r} block runs to the end of {PROMPT}"
    return " ".join(text[start:end].replace("`", " ").split())


def test_collision_remedy_assigns_the_prefix_in_the_city_config():
    """The remedy must name the config that owns the prefix, not the artifact.

    Anchored to the assignment clause itself, so a routes.jsonl mention
    elsewhere in the block (it is the right file to *describe*) stays green and
    only the assignment target is pinned.
    """
    block = conflicts_paragraph(PROMPT.read_text())
    cursor = block.find(ASSIGNMENT_ANCHOR)
    assert cursor != -1, f"missing {ASSIGNMENT_ANCHOR!r} in the Conflicts block"
    target = FILE_TOKEN.search(block, cursor)
    assert target is not None, f"the prefix assignment names no file: {block[cursor:]}"
    assert target.group(1) == CONFIG_FILE, (
        f"the prefix assignment points at {target.group(1)}, not {CONFIG_FILE}; "
        f"routes.jsonl is generated from the city config: {block[cursor:]}"
    )


def sentence_containing(block, index):
    """Return the sentence of `block` that spans offset `index`.

    Scoped to one sentence rather than the whole block: this runbook already
    talks about regenerating and migrating elsewhere, so a block-wide search
    for the framing vocabulary would let an unrelated sentence vouch for a
    mention it does not describe.
    """
    start, end = 0, len(block)
    for boundary in SENTENCE_BREAK.finditer(block):
        if boundary.end() <= index:
            start = boundary.end()
        else:
            end = boundary.start()
            break
    return block[start:end]


def test_collision_remedy_keeps_the_generated_artifact_descriptive():
    """routes.jsonl may be described, never prescribed as an edit target.

    Complements the positional pin above, which only measures which file the
    assignment names *first*. Appending a hand-edit instruction after the
    pinned city.toml token, or swapping the descriptive parenthetical for a
    prescriptive clause, both restore the defect round 2 removed while leaving
    that pin green.

    Known residual, deliberately not pinned: a mention that is prescriptive and
    framed as generated at once ("hand-edit the generated routes.jsonl"). The
    obvious cure, banning edit verbs from the sentence, reddens ordinary
    correct rewordings such as "update its city.toml entry", which is the
    false-red failure mode this suite is already trying to avoid.
    """
    block = conflicts_paragraph(PROMPT.read_text())
    mentions = [m.start() for m in re.finditer(re.escape(ARTIFACT_FILE), block)]
    assert len(mentions) == 1, (
        f"{ARTIFACT_FILE} is named {len(mentions)} times in the Conflicts "
        f"block; the sole mention must be the generated-artifact clause: {block}"
    )
    clause = sentence_containing(block, mentions[0])
    assert ARTIFACT_FILE in clause, (
        f"sentence extraction lost the {ARTIFACT_FILE} mention, so the framing "
        f"check below would measure unrelated prose: {clause!r}"
    )
    assert GENERATED_FRAMING.search(clause) is not None, (
        f"the sentence naming {ARTIFACT_FILE} does not describe it as "
        f"generated, so the block reads as an instruction to edit it: {clause}"
    )
