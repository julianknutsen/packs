# Testing across the layer boundary

A patch says it does something, and it does. The test proves it. The live build
is unchanged.

This is the failure mode this document exists to stop, and it is the one that
makes a pull request untrustworthy even when every claim in it is true. What
follows is the standard for a fix in the dolt / beads / gascity / packs stack,
and the layer map you need to know which of those four repositories a defect
actually belongs to.

**Nothing enforces it yet.** This is a document a reviewer reads, not a gate a
formula runs: no `pr-pipeline` formula, review prompt or scorecard field checks
any of it as of 2026-08-17. Saying "the pipeline applies this standard" would be
the same overclaim the document is about. Wiring it into the review scorecard's
evidence category is open work.

## The rule

**A fix's test must fail on the unfixed live build and pass on the fixed live
build, with the fix as the only difference.** Everything below serves that
sentence.

## Two worked examples, both ours

`gastownhall/gascity` issues **#5333** (`gc start` treats a supervisor reload
timeout as fatal before the readiness check) and **#5324** (`gc supervisor stop`
can report success while launchd remains able to restart the supervisor). Both
are real bugs. Neither report, as written, establishes that fixing it fixes the
machine it was found on.

**#5333 has an executable that asserts the wrong thing.** Its reproducer drives
`registerCityWithSupervisor` directly and replaces five seams:
`ensureSupervisorRunningHook`, `reloadSupervisorHook`, `supervisorAliveHook`,
`waitForSupervisorCityHook`, `cityControllerURLHook`. The report says plainly why
it had to. The real path "can regenerate or install platform supervisor service
files before liveness checks on macOS," so the test stubbed exactly the boundary
the bug lives on. What it proves is a claim about the stub graph: given a
`reloadSupervisorHook` that returns 1 and writes a particular string to stderr,
the caller now treats it as an async start. Whether the *real* macOS reload
returns 1, and writes that string, on a genuine timeout is a separate claim, it
is the load-bearing one, and nothing in the report establishes it. If the real
path returns 2, or a different message, the fix is correct in the test and inert
in the build.

**#5324 asserts the right thing and has no executable.** Its stated
postcondition is about the world rather than a return value: `gc supervisor stop
--wait` "should leave the machine supervisor durably stopped, or fail if it
cannot prove that launchd will not restart it." That is the correct shape. Its
reproduction is a prose "best-effort reproduction shape," and its evidence was
withheld for a reason the reporter names: the logs "include machine-specific
paths and session identifiers."

Between them they name the two halves. A test can be runnable, or it can be
about the live system. We keep accepting the ones that are only the first.

## 1. Push the test to the outermost tier where the bug still reproduces

| Tier | What is real | What is fake |
|---|---|---|
| **T1** | the installed binary, the real platform service (launchd/systemd), a real store | nothing |
| **T2** | the real `gc` binary, real `bd`, real dolt, a throwaway city in a temp dir | the work, the repos |
| **T3** | in-process, real collaborators wired together | the environment |
| **T4** | in-process, seams replaced by hooks | the collaborators |

Both examples above sit at T4. That is the tier where a green result carries the
least information, and it is the tier the test hooks make easiest to reach.

**If a bug reproduces only at T4, you have not reproduced the bug. You have
reproduced a stub.** Move up until it stops reproducing; the last tier where it
still does is where the test belongs.

`test/acceptance/` and `test/qualification/` in `gastownhall/gascity` are the T2
tier and already exist. `test/acceptance/beads_cli_contract_test.go` is the shape
to copy. Reaching for a hook when a T2 harness already exists is the specific
move to stop.

For a pack, T2 means standing the pack up in a throwaway city and running the
real `gc` against the shipped pack directory, not a copy of it.
`tests/test_gc_role_prompt_integration.py` is the merged working harness to copy.
`tests/test_pack_composition_warnings.py` is a second one and is **not merged**
— it is open as pull request #332, so do not expect to find it on `main`.

## 2. Every stub carries its fidelity claim, in the pull request body

For each seam the reproducer replaces, state the real behavior it stands for and
the evidence they match. "The real `reloadSupervisorHook` exits 1 and writes
`reconcile did not finish before timeout` to stderr on macOS timeout, verified by
`<command>`" is a reviewable sentence. Its absence is where the
fixes-the-reproducer-not-the-build failure lives, every time.

A reviewer who cannot find that sentence has found the finding.

## 3. Assert the postcondition, not the report

A test that checks the function returned success is testing the report. A test
that checks launchd no longer holds a restartable job is testing the world.

## 4. Kill the fix, then kill the stub

Reverting the fix must turn the test red. That is necessary and it is weak: it
shows only that the stub graph is sensitive to the change. The second mutation is
the one that matters, and it has to be aimed correctly, because the obvious
version of it proves nothing.

Perturbing a stub toward a *plausible* alternative (a different exit code, a
reworded message) and finding the test still discriminates is not evidence of a
bad test. A fix that handles both exit codes while the unfixed build handles
neither will keep discriminating, and that is the fix being robust. Do not read
it as blindness.

Aim the mutation at the stub's *fidelity* instead. Move it to a state the real
component never produces, or delete a call the real component always makes. If
the test's verdict is unchanged, the stub is not load-bearing: the test would go
on passing against a stand-in that no longer resembles the thing it replaces,
which is the whole failure this document exists to catch. When a plausible-
alternative mutation *does* flip the result, that is a question and not a verdict
— either the fix is narrower than its claim or the stub was over-specified, and
the pull request should say which.

The same discipline applies to a test's own detector. If a check asserts the
absence of a condition, plant that condition deliberately in the same run and
require it to be reported. Otherwise a passing check and a detector that stopped
working look identical, and the check goes green forever after the next upstream
change.

## 5. For an environment-specific bug, ship a probe, not a city

Cities cannot be passed back and forth, and that should not end the conversation.
What travels is a **diagnostic the reporter runs**, emitting a scrubbed,
machine-produced transcript: versions of `gc`, `bd` and dolt, the platform service
state, and the exact command output at the failing step, with home paths,
hostnames and session identifiers redacted at the source.

That solves both halves of #5324 at once. The reporter wanted to give us the
evidence and could not do so safely. A probe that scrubs by construction gets a
real reading of a real environment and keeps their paths out of a public issue.
The bug report then contains an observation instead of a reproduction shape.

## The layer map

Four layers. Each row is what the layer owns, where its contract with the layer
above is written down, and what enforces it.

| Layer | Owns | Contract specified in | Enforced by |
|---|---|---|---|
| **dolt** | storage engine, SQL surface, version history | upstream | a pinned version in `deps.env` |
| **beads** (`bd`) | work records, claim/lease/fence, `--json` wire shapes, exit codes, `schema_version` | `engdocs/design/beads-dolt-contract-redesign.md` | nothing cross-repo |
| **gascity core** (`gc`) | cities, rigs, sessions, the supervisor, formulas, and the `bd` subprocess consumer (roughly 20 subcommands parsed into a fixed struct, with behavior keyed off exit codes and free-text error strings) | `engdocs/design/beads-gascity-contract-test-system.md` | Phase 0+1 only |
| **packs** | agents, commands, services, formulas, skills, hooks, template fragments, composed via `pack.toml` imports | `docs/reference/specs/pack-spec.md`, `engdocs/design/packv2/` | `gc lint` on 5 of 16 registry packs |

Paths in the third column are relative to a `gastownhall/gascity` checkout,
except the packs row's first entry, which is also in that repository.

**The boundary is specified in four places and gated end to end in none.** Drift
lives in the gap between a written contract and an executed one.
`engdocs/design/beads-gascity-contract-test-system.md` (Proposed, 2026-06-24)
diagnosed this for the `bd` to `gc` edge and catalogued **28 historical drift
incidents**, dominated by version-gated code that CI never exercises. Its Phase
0+1 landed; the cross-version matrix that was the actual gate did not.

### Which repository owns a defect

The layer map is not only architecture. It decides where a bug report goes, and
report-by-symptom routes it wrong often enough to be worth a checklist.

Three of the open issues on this tracker name artifacts that are not in this
repository:

| Issue | Names | Actually lives in |
|---|---|---|
| **#166** | `bd` pack, `assets/scripts/gc-beads-bd.sh` | `gastownhall/gascity`, `examples/bd/` |
| **#168** | `dolt` pack command group missing `stop` | `gastownhall/gascity`, `examples/bd/dolt/commands/` |
| **#300** | `{{.AgentBase}}` resolves to the pool template name | `gastownhall/gascity`, `internal/workdir` and the desired-state builder |

The first two are the same trap: `bd` and `dolt` are packs, and they ship inside
the `gascity` repository rather than this one. The third is subtler and is the
one worth internalizing. The pack's `work_dir` template is correct as written;
what is wrong is the identity `gc` binds when it expands that template for a pool
instance. **A defect in what a template expands to belongs to whoever expands
it.** Editing the pack there would produce a fix that is bespoke to one pack and
leaves the same bug in every other pack that uses the same variable.

The routing question, in order:

1. Does the file the report names exist in this repository? If not, it is not a
   packs issue, whatever the report calls the component.
2. Is the wrong value authored by the pack, or bound by `gc` when it reads the
   pack? Authored is ours; bound is core's.
3. Would the fix name a specific pack inside core? If yes, see the next section:
   it is a missing declaration, not a fix.

## The interaction effect, and how not to make core bespoke

A pack expresses a particular user's system. Core must serve it without learning
about it. The test for whether a fix crossed that line:

> **Does the fix add a branch that names a pack, or a capability the pack
> declares and core validates generically?**

The first is bespoke and accrues forever. The second is what `gc lint` and the
pack conformance matrix are for: the pack *declares* its requirement, core
*checks* the declaration, and the check is the same code for every pack. A pack
that needs something core cannot express is a request for a new declaration, not
a new special case.

This is also where reported breakage comes from. Derived 2026-08-17 from the CI
lint loop and `registry.toml`: **11 of the 16 registry packs never meet a running
`gc` at all**. Those eleven are `cass`, `contributing`, `discord`,
`gastown`, `github`, `oversight-rig`, `pr-pipeline`, `runtime-cloudflare`,
`slack-channel`, `slack-full` and `slack-mini`. The five the loop reaches are
`bmad`, `compound-engineering`, `gascity`, `gstack` and `superpowers`. Counting
the lint loop's entries overstates coverage by **two**: it has seven, and
`gascity/roles` and `profiler` are both linted and neither is a registry pack.
Issue **#307** reports this at ten.

**Read those five as lint, not as T2.** `gc lint` runs the real binary against
the pack, which is why it belongs in this count at all, but it reads the pack's
declarations — it never stands the pack up in a city and never starts an agent,
so it cannot see a `pre_start` whose `{{.Missing}}` survives template expansion
as literal text and reaches `sh -c`. That is the defect class this document is
about, and lint is structurally blind to it.

By the tier table's own T2 row, the merged coverage is **one pack family**:
`tests/test_gc_role_prompt_integration.py` writes a `pack.toml` and a `city.toml`
into a temp dir and runs `$GC_TEST_BIN` against them, covering `gascity/roles`
plus a parameterized city pack. Nothing else in the repository does. So the
honest reading of "5 of 16" is five linted and one stood up, and the gap between
those two numbers is where the reported breakage lives.

Several of the eleven have their own pytest or Go suites, which test the pack's
logic against the pack's own fixtures. Nothing stands them up against a gascity
that has moved since the pack was last touched, and `registry.toml` carries no
support-tier or last-validated-version field, so a user reading it cannot tell an
exercised pack from an unexercised one.

That is the same failure as #5333 at a different scale: a green suite about the
artifact's fixtures rather than about the system it runs in.

## What a reviewer asks

Five questions, in order. Any "no" is the review comment.

1. What tier is the test at, and does the bug reproduce at a higher one?
2. For each stub: what real behavior does it stand for, and what verified it?
3. Does the assertion name a state of the world, or a return value?
4. Does reverting the fix turn it red, and does breaking a stub's *fidelity*
   (a state the real component never produces) change the verdict?
5. If the bug is environment-specific, is there a probe the reporter can run?

## Refuting commands

Every number above is re-derivable. Set both first — the block is written to run
from any directory, and under `set -u` an unset one aborts it immediately:

```sh
GASCITY=/path/to/gastownhall/gascity        # a checkout of gastownhall/gascity
PACKS=/path/to/gastownhall/gascity-packs    # a checkout of gastownhall/gascity-packs

# the five stubbed seams in #5333
gh issue view 5333 --repo gastownhall/gascity --json body \
  | grep -oE '[a-zA-Z]+Hook = ' | sort -u

# the 28 drift incidents, and the design's status (still "Proposed")
sed -n '1,40p' "$GASCITY/engdocs/design/beads-gascity-contract-test-system.md"

# which packs CI actually lints against a running gc -- the loop body is
# `"$GC_BIN" lint "$pack"`, so grepping for a literal `gc lint` finds nothing
grep -n 'lint "\$pack"' -B4 "$PACKS/.github/workflows/ci.yml"

# registry packs, and which of them that loop covers -- the path must be
# absolute; a bare "registry.toml" reads whatever directory you happen to be in
python3 -c 'import sys,tomllib;print(sorted(p["name"] for p in tomllib.load(open(sys.argv[1],"rb"))["pack"]))' "$PACKS/registry.toml"

# the dolt command group's verbs, and the absent stop (issue #168)
ls "$GASCITY/examples/bd/dolt/commands/"

# where AgentBase is bound (issue #300)
grep -rn 'AgentBase:' "$GASCITY/internal/workdir/workdir.go"
```
