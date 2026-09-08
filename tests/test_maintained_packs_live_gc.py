"""Every pack we maintain, stood up in a city and asked about by a real `gc`.

These five are the packs users actually install, and the ones we have committed
to keeping working. Each is imported on its own here, because each is imported
on its own by a user -- nobody installs "the maintained set". The whole point of
running them separately is that a break in one is attributed to that one.

What each pack gets:

* its declared surfaces, derived from its own directory rather than listed here,
  resolved through a running `gc` in a city that imports it;
* a `gc doctor` delta against a baseline city, asserted by set EQUALITY against
  what that pack is currently expected to add. Equality rather than emptiness
  because a legitimate finding exists (an adapter binary a pack cannot ship
  built), and equality rather than `assertFalse` on new items because a finding
  that DISAPPEARS is also news: it means either the pack stopped declaring
  something or `gc` stopped checking it.

The control lives in `test_the_doctor_delta_can_surface_a_finding` and every
delta assertion here depends on it. An empty or unchanged delta is also what a
doctor that stopped reporting produces.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from gc_live_city import (
    CANARY_BINDING,
    CANARY_CHECK,
    REPO_ROOT,
    attributable,
    declares_a_rig_scoped_session,
    declares_a_service,
    discover_agents,
    discover_command_words,
    discover_formulas,
    gc_output,
    gc_test_bin,  # noqa: F401 -- pytest fixture, used by name
    write_canary_pack,
    write_city,
)


# The packs this city owns the maintenance of. Adding a pack here is the whole
# cost of bringing it under live-gc coverage; everything below derives from the
# pack's own contents.
MAINTAINED_PACKS = (
    "oversight-rig",
    "pr-pipeline",
    "slack-channel",
    "slack-full",
    "slack-mini",
)

# Doctor findings each pack is currently expected to add to a city, with the
# reason it is legitimate. An entry here is a claim that a user installing the
# pack meets this message and that we have decided it is correct for them to.
# Anything not written down is a regression, and removing an entry without
# removing its cause is caught too, because the assertion is equality.
EXPECTED_DOCTOR_DELTA: dict[str, frozenset[str]] = {
    "oversight-rig": frozenset(),
    "pr-pipeline": frozenset(),
    "slack-channel": frozenset(),
    # Both of these are the correct messages for a fresh install, and both come
    # from checks only `slack-full` ships. `binaries`: the pack ships its Slack
    # adapter as source, and the user builds it during setup. `env`: the
    # adapter's credentials live in a config file the user writes during setup,
    # and it does not exist yet. `slack-channel` and `slack-mini` ship the same
    # unbuilt adapter and no check that notices, which is why their entries are
    # empty rather than matching.
    "slack-full": frozenset({"slack-full:binaries", "slack-full:env"}),
    "slack-mini": frozenset(),
}

# Findings whose presence is a property of the machine, not of the pack. These
# are set aside before the equality above and PRINTED whenever they are, so a
# run always shows what it stopped asserting on -- a silent subtraction and a
# check that stopped reporting read identically.
#
# Keep this set as small as the evidence forces. It is not a waiver list: the
# bar is that no import of the pack can decide the outcome, so the equality
# assertion could not be green on every machine at once.
HOST_DEPENDENT: dict[str, frozenset[str]] = {
    # slack-full/doctor/check-funnel.sh inspects the host's Tailscale Funnel
    # rules. With tailscale absent it prints a note and exits 0; with tailscale
    # present and no rule forwarding to the adapter port it exits 2. Both are
    # correct messages for the machine they ran on, and neither is caused by
    # importing the pack -- a developer's laptop and a CI runner cannot both
    # satisfy one recorded value.
    "slack-full": frozenset({"slack-full:funnel"}),
}


def pack_dir(pack: str) -> Path:
    return REPO_ROOT / pack


def wiring(pack: str) -> tuple[dict[str, Path], dict[str, Path]]:
    """City-scope and rig-scope bindings for one pack, as its manifest requires.

    Not a choice. A pack declaring a `[[service]]` is rejected at rig scope, and
    a pack declaring a rig-scoped `[[named_session]]` leaves `rig-pack-coverage`
    reported unless it is bound per rig. Both are read from the pack, so a pack
    that changes its manifest changes how this suite installs it.
    """
    directory = pack_dir(pack)
    has_service = declares_a_service(directory)
    needs_rig = declares_a_rig_scoped_session(directory)

    assert not (has_service and needs_rig), (
        f"{pack} declares both a [[service]] (city scope only) and a rig-scoped "
        "[[named_session]] (needs a rig binding). There is no wiring that "
        "satisfies both, so this suite cannot describe how to install it."
    )

    return {pack: directory}, ({pack: directory} if needs_rig else {})


@pytest.fixture(scope="session")
def canary_delta(tmp_path_factory, gc_test_bin: Path) -> frozenset[str]:  # noqa: F811
    """The control, run once: a pack that earns a finding on purpose.

    Every delta assertion in this file is a statement about a subtraction. If
    the subtraction cannot surface anything -- because doctor stopped reporting,
    because the fixture city fails to load before doctor reaches its packs,
    because the binary changed its output shape -- then every one of those
    statements holds vacuously and this suite goes green having measured
    nothing. This fixture is what makes the greens mean something.
    """
    root = tmp_path_factory.mktemp("canary")
    canary = write_canary_pack(root / "fixture")
    return frozenset(attributable(gc_test_bin, root / "city", {CANARY_BINDING: canary}))


def test_the_doctor_delta_can_surface_a_finding(canary_delta: frozenset[str]) -> None:
    assert CANARY_CHECK in canary_delta, (
        'a pack whose formula declares the deprecated `contract = "graph.v2"` '
        f"did not add {CANARY_CHECK!r} to the doctor delta, so every delta "
        f"asserted in this file proves nothing. Delta was: {sorted(canary_delta)}"
    )


@pytest.mark.parametrize("pack", MAINTAINED_PACKS)
def test_pack_adds_only_the_doctor_findings_we_have_accepted(
    pack: str, tmp_path: Path, gc_test_bin: Path, canary_delta: frozenset[str]  # noqa: F811
) -> None:
    imports, rig_imports = wiring(pack)
    found = attributable(gc_test_bin, tmp_path, imports, rig_imports)
    expected = EXPECTED_DOCTOR_DELTA[pack]

    host = HOST_DEPENDENT.get(pack, frozenset())
    for check in sorted(found & host):
        print(
            f"[host-dependent] {check} reported on this machine and is not "
            f"asserted on; see HOST_DEPENDENT in {Path(__file__).name}"
        )
    found -= host

    assert found == expected, (
        f"installing {pack} changes what gc doctor reports, against what this "
        f"suite records as accepted.\n"
        f"  new (a user installing {pack} now meets this): {sorted(found - expected)}\n"
        f"  gone (recorded as expected, no longer reported): {sorted(expected - found)}\n"
        f"Run `gc doctor` in a city importing {pack} to read the messages. A new "
        f"finding is a defect in the pack unless it is deliberate, in which case "
        f"it goes in EXPECTED_DOCTOR_DELTA with the reason. A finding that went "
        f"away is fixed work: delete the entry in the same change."
    )


@pytest.mark.parametrize("pack", MAINTAINED_PACKS)
def test_pack_registers_the_command_verbs_it_ships(
    pack: str, tmp_path: Path, gc_test_bin: Path  # noqa: F811
) -> None:
    """Pack commands are discovered by convention, so nothing declares them.

    That makes the surface easy to lose silently: a moved directory or a renamed
    leaf script drops a verb with no error anywhere. `gc <pack> ... --help` is
    where a user would notice, so it is where this asserts.
    """
    leaves = discover_command_words(pack_dir(pack))
    if not leaves:
        pytest.skip(f"{pack} ships no commands")

    imports, rig_imports = wiring(pack)
    workspace = write_city(tmp_path, imports, rig_imports)

    # One `--help` per level of nesting: pr-pipeline puts its leaves under `pr`,
    # the slack packs put theirs directly under the pack.
    by_parent: dict[tuple[str, ...], set[str]] = {}
    for words in leaves:
        by_parent.setdefault(words[:-1], set()).add(words[-1])

    for parent, expected in sorted(by_parent.items()):
        listed = set(gc_output(gc_test_bin, workspace, pack, *parent, "--help").split())
        missing = expected - listed
        assert not missing, (
            f"gc {pack} {' '.join(parent)} --help did not offer verbs the pack "
            f"ships: " + ", ".join(sorted(missing))
        )


@pytest.mark.parametrize("pack", MAINTAINED_PACKS)
def test_pack_formulas_resolve_through_a_city(
    pack: str, tmp_path: Path, gc_test_bin: Path  # noqa: F811
) -> None:
    expected = discover_formulas(pack_dir(pack))
    if not expected:
        pytest.skip(f"{pack} ships no formulas")

    imports, rig_imports = wiring(pack)
    workspace = write_city(tmp_path, imports, rig_imports)
    listed = set(gc_output(gc_test_bin, workspace, "formula", "list").split())

    missing = expected - listed
    assert not missing, (
        f"formulas {pack} ships did not resolve in a city that imports it: "
        + ", ".join(sorted(missing))
    )


@pytest.mark.parametrize("pack", MAINTAINED_PACKS)
def test_pack_agents_resolve_through_a_city(
    pack: str, tmp_path: Path, gc_test_bin: Path  # noqa: F811
) -> None:
    expected = discover_agents(pack_dir(pack))
    if not expected:
        pytest.skip(f"{pack} ships no agents")

    imports, rig_imports = wiring(pack)
    workspace = write_city(tmp_path, imports, rig_imports)
    listed = gc_output(gc_test_bin, workspace, "agent", "list")

    missing = {name for name in expected if name not in listed}
    assert not missing, (
        f"agent roles {pack} ships did not resolve in a city that imports it: "
        + ", ".join(sorted(missing))
        + f"\nOutput:\n{listed}"
    )
