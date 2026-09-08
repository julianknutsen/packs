# Gastown Pack

Gastown is the domain-specific coding workflow pack. It provides the city
coordinator roles, rig worker roles, patrol formulas, and the pack-local dog
pool used for stuck-agent shutdown warrants.

## Import

```toml
[imports.gastown]
source = "../packs/gastown"
```

Use the pack as the workspace pack for city-scoped agents and as a rig pack for
rig-scoped agents.

## Composition

Gas City composes the builtin core pack (mechanical housekeeping orders)
through the explicit `includes` entries that `gc init` writes into
city.toml; this pack composes alongside it via `[imports.gastown]`. The
retired maintenance pack no longer exists: gastown's `mol-shutdown-dance`
and dog prompt fragments (`propulsion-dog`, `architecture`,
`following-mol`) are the only copies in play, and cross-pack agent name
collisions are hard errors rather than fallback resolutions.

Verify the composed recipe after changing imports:

```bash
gc formula show mol-shutdown-dance
```

The recipe must read warrant metadata from the claimed bead via
`$GC_BEAD_ID` and must not declare a required `warrant_id` var.

## Default Sling Formula

The polecat agent ships an agent-level default sling formula, so a plain
`gc sling <rig>/gastown.polecat "<text>"` compiles `mol-polecat-work`
(`method = "default-on-formula"`) instead of routing a bare bead.

That agent-level value outranks a city's `[agent_defaults]
default_sling_formula`: `Agent.EffectiveDefaultSlingFormula()` returns the
agent's own value before any inherited one, and `ApplyAgentDefaults` fills the
city default in only for agents that set neither their own nor an inherited pack
default. A city that deliberately routed polecat slings through its own default
formula is therefore shadowed when it upgrades to a gastown pack carrying this
knob.

The config-level override is a per-agent patch, which assigns the agent's own
value and so wins over the pack's:

```toml
[[patches.agent]]
dir = "<rig>"
name = "gastown.polecat"
default_sling_formula = "mol-your-formula"
```

Per sling, an explicit choice still short-circuits ahead of the default:
`--no-formula` routes a raw bead, `<formula> --formula` instantiates that formula
instead (`--formula` is a boolean that reinterprets the positional argument), and
`--on <formula>` attaches one to an existing bead. `[agent_defaults]` is the one
knob that cannot override the agent's own value.

## Dog Pool

Gastown owns `mol-shutdown-dance` and the dog agent that runs stuck-agent
warrants, including the dog's `wake_mode` and `work_dir` settings. In import
composition gastown's dog expands as the distinct `gastown.dog` agent; the
dolt pack ships its own separate dog for Dolt maintenance formulas, and the
two coexist under their binding-qualified names.

Gastown deliberately does not ship retired dog formulas for JSONL export or
stale-session reaping. The Gas City builtin core pack provides JSONL export,
stale-session and stale-data cleanup, and Dolt housekeeping as deterministic
exec orders.
