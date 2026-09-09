#!/usr/bin/env bash
set -euo pipefail

# Structural invariants for the mol-port-review formula (gci-rug / gci-7x4 D5).
# Mirrors the grep+tomllib assertion idiom of test_gastown_pack_assets.sh: the
# formula is prose-as-executable-steps, so the discipline it encodes IS the
# contract, and these assertions pin the parts that must not silently drift.

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
FORMULA="$ROOT/gastown/formulas/mol-port-review.toml"

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

need() {
    # need "<description>" <grep-args...>  — assert a pattern is present.
    local desc="$1"; shift
    grep -q "$@" "$FORMULA" || fail "$desc"
}

test_formula_parses_and_declares_v2() {
    [[ -f "$FORMULA" ]] || fail "missing mol-port-review formula"
    python3 - "$FORMULA" <<'PY'
import sys, tomllib
with open(sys.argv[1], "rb") as fh:
    d = tomllib.load(fh)
assert d.get("formula") == "mol-port-review", "formula name must be mol-port-review"
req = d.get("requires", {})
assert str(req.get("formula_compiler", "")).startswith(">=2"), \
    "must opt into the v2 compiler via [requires] formula_compiler >=2.0.0"
# Deprecated opt-ins must not be reintroduced (spec §5 canonical style).
assert "contract" not in d, "use [requires], not the deprecated contract key"
assert d.get("phase") in (None, ""), "phase=vapor must not be authored into new formulas"
# Reserved vars must never be declared (spec §1.4).
for reserved in ("convoy_id", "bead_id"):
    assert reserved not in d.get("vars", {}), f"reserved var {reserved} must not be declared"
# Provenance inputs are required, not defaulted.
for req_var in ("review_target", "source_rig", "source_ref"):
    v = d.get("vars", {}).get(req_var)
    assert isinstance(v, dict) and v.get("required") is True, f"{req_var} must be required"
    assert "default" not in v, f"required var {req_var} must not carry a default"
# The three-stage step DAG, in order, chained by needs.
ids = [s["id"] for s in d.get("steps", [])]
expected = ["load-context", "port", "branch-ready-halt",
            "owning-pl-review", "review-gate", "mayor-verify", "mayor-publish"]
assert ids == expected, f"unexpected step set/order: {ids}"
assert len(ids) == len(set(ids)), "duplicate step ids"
by_id = {s["id"]: s for s in d["steps"]}
for a, b in zip(expected, expected[1:]):
    assert b == "load-context" or a in by_id[b].get("needs", []), f"{b} must need {a}"
# Cross-agent routing: review stage → review_target, publish stage → publish_target.
for sid in ("owning-pl-review", "review-gate"):
    assert by_id[sid].get("assignee") == "{{review_target}}", f"{sid} must route to review_target"
for sid in ("mayor-verify", "mayor-publish"):
    assert by_id[sid].get("assignee") == "{{publish_target}}", f"{sid} must route to publish_target"
print("tomllib structural checks passed")
PY
}

test_stage1_halts_for_owning_pl_review() {
    need "stage 1 must halt with halt_reason=owning-PL-review" -F 'halt_reason=owning-PL-review'
    need "stage 1 must set review_target for the handoff" -F 'review_target={{review_target}}'
    need "stage 1 must record branch_ready" -F 'branch_ready=true'
    need "stage 1 must carry provenance (source_rig/source_ref)" -F 'source_rig={{source_rig}}'
    # Stage 1 stops at a pushed branch — the mayor publishes, not the porter.
    need "stage 1 explicitly halts short of PR/merge" -Fi 'no PR, no merge'
}

test_stage2_review_variants_present() {
    need "scoped-re-review variant" -Fi 'scoped re-review'
    need "scoped re-review is carried as review_scope=fix-diff" -F 'review_scope=fix-diff'
    need "pre-approved-mechanical-fixes variant" -Fi 'pre-approved mechanical'
    need "pre-approved-mechanical verdict value" -F 'pre-approved-mechanical'
    need "SEED-ERROR self-verification prompt" -F 'SEED-ERROR'
    need "reviewer re-verifies their own seeds against the live impl" -Fi 'seed'
    need "verdict is persisted for the publish gate" -F 'evidence.review_verdict'
    need "review discipline reuses the review-leg helper" -F '{{review_formula}}'
}

test_stage3_publish_gates_present() {
    need "diff-scope gate" -Fi 'diff-scope'
    need "secret-shape gate" -Fi 'secret-shape'
    need "squash-complete gate" -Fi 'squash-complete'
    # A failed publish gate must halt, never publish.
    need "publish gates gate the publish step" -F 'evidence.publish_gates'
    need "mayor squash-publishes to the canonical branch" -Fi 'squash-publish'
}

main() {
    test_formula_parses_and_declares_v2
    test_stage1_halts_for_owning_pl_review
    test_stage2_review_variants_present
    test_stage3_publish_gates_present
    echo "PASS: mol-port-review structural invariants"
}

main "$@"
