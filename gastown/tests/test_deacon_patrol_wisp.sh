#!/usr/bin/env bash
# Regression test for gc-fph55: deacon patrol wisps accumulating as duplicate
# OPEN molecules on fresh restart.
#
# Two layers:
#   1. Static guards on the real formula + prompt — assert the patrol-wisp
#      queries never use the invalid `--type=wisp` (matches nothing) and are
#      title-narrowed to this patrol loop, and that the Startup Protocol gates
#      its pour on an existing-wisp check (the gc-fph55 bootstrap fix).
#   2. Behavioural idempotency — drive a mirror of the Startup Protocol pour
#      decision against a stubbed `gc` ledger and assert it never pours a
#      duplicate when a patrol wisp already exists (and is a no-op on re-run).
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
FORMULA="$ROOT/gastown/formulas/mol-deacon-patrol.toml"
PROMPT="$ROOT/gastown/agents/deacon/prompt.template.md"

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

# ---------------------------------------------------------------------------
# Layer 1: static guards on the real artifacts
# ---------------------------------------------------------------------------
test_no_invalid_wisp_type_filter() {
    # `--type=wisp` is not a valid bd issue type; the query errors and matches
    # nothing, so the open-successor / current-wisp lookups silently return
    # empty and a duplicate is poured. Patrol wisp roots are issue_type=molecule.
    if grep -n -- '--type=wisp' "$FORMULA" "$PROMPT"; then
        fail "found invalid --type=wisp filter (patrol wisps are issue_type=molecule)"
    fi
}

test_patrol_wisp_queries_are_title_narrowed() {
    # Every molecule lookup that resolves a patrol wisp must narrow by title to
    # this patrol loop, so reconcile/idempotency logic never matches unrelated
    # molecule work that may be assigned to the deacon (gc-fph55 caveat).
    local untargeted
    untargeted=$(grep -hn -- '--type=molecule' "$FORMULA" "$PROMPT" \
        | grep -- 'gc bd list' \
        | grep -v -- '--title="mol-deacon-patrol"' || true)
    if [ -n "$untargeted" ]; then
        fail "molecule wisp query missing --title=\"mol-deacon-patrol\" narrow:
$untargeted"
    fi
}

test_startup_pour_is_guarded() {
    # The Startup Protocol must look for an already-queued OPEN patrol wisp and
    # skip the pour when one exists — otherwise a fresh restart re-pours a
    # duplicate (the core gc-fph55 bootstrap bug).
    grep -q 'EXISTING_WISP=' "$PROMPT" \
        || fail "Startup Protocol missing EXISTING_WISP open-successor check"
    grep -q 'status=open,in_progress' "$PROMPT" \
        || fail "EXISTING_WISP check must include open status (not in_progress only)"
    grep -q 'if \[ -z "\$EXISTING_WISP" \]; then' "$PROMPT" \
        || fail "Startup pour must be gated on an empty EXISTING_WISP"
}

test_assignee_is_unified() {
    # GC_ALIAS and GC_AGENT must not be mixed in the prompt's wisp lookups —
    # resolve one ASSIGNEE and use it everywhere (avoids GC_ALIAS/GC_AGENT skew).
    grep -q 'ASSIGNEE=${GC_ALIAS:-$GC_AGENT}' "$PROMPT" \
        || fail "prompt must resolve a single ASSIGNEE=\${GC_ALIAS:-\$GC_AGENT}"
    if grep -- 'gc bd list' "$PROMPT" | grep -- '--type=molecule' | grep -qE -- '--assignee="\$GC_(ALIAS|AGENT)"'; then
        fail "prompt molecule wisp queries must use --assignee=\"\$ASSIGNEE\", not raw GC_ALIAS/GC_AGENT"
    fi
}

test_title_match_is_exact() {
    # Belt-and-suspenders: --title= is a case-insensitive substring prefilter;
    # an exact jq select guards against substring false-matches.
    local q
    for q in "$FORMULA" "$PROMPT"; do
        if grep -- 'gc bd list' "$q" | grep -- '--title="mol-deacon-patrol"' \
            | grep -v -- 'select(.title=="mol-deacon-patrol")' | grep -q .; then
            fail "title-narrowed query in $q missing exact jq select(.title==...)"
        fi
    done
}

# ---------------------------------------------------------------------------
# Layer 2: behavioural idempotency of the Startup pour decision
# ---------------------------------------------------------------------------
# Stubs `gc` with a file-backed ledger of patrol wisps and runs a mirror of the
# Startup Protocol decision (Step 2 + Step 4 in the prompt — kept in sync by the
# static guards above). Asserts the decision converges to exactly one wisp and
# never pours a duplicate.
write_gc_stub() {
    local bin="$1"
    mkdir -p "$bin"
    cat >"$bin/gc" <<'SH'
#!/usr/bin/env sh
# Minimal gc-bd ledger stub. Ledger file = one wisp id per line (all "open").
LEDGER="$GC_TEST_LEDGER"
case "$1 $2 ${3:-}" in
  "bd list "*)
    # Emit the ledger as a JSON array of {"id": "..."} objects.
    if [ ! -s "$LEDGER" ]; then printf '[]'; exit 0; fi
    out=""
    while IFS= read -r id; do
      [ -n "$id" ] || continue
      out="$out{\"id\":\"$id\",\"title\":\"mol-deacon-patrol\"},"
    done < "$LEDGER"
    printf '[%s]' "${out%,}"
    ;;
  "bd mol wisp")
    n=$(( $(wc -l < "$LEDGER" 2>/dev/null || echo 0) + 1 ))
    id="gc-wisp-stub$n"
    printf '%s\n' "$id" >> "$LEDGER"
    printf '{"new_epic_id":"%s"}' "$id"
    ;;
  "bd mol burn")
    id="$4"
    grep -v -x "$id" "$LEDGER" > "$LEDGER.tmp" 2>/dev/null || :
    mv "$LEDGER.tmp" "$LEDGER" 2>/dev/null || :
    ;;
  "bd update "*) : ;;
  "mail inbox"*) : ;;
  *) printf '[]' ;;
esac
SH
    chmod +x "$bin/gc"
}

# Mirror of the prompt Startup Protocol pour decision (Step 2 + Step 4).
# The static guards (Layer 1) assert the real prompt keeps this shape.
startup_pour_decision() {
    ASSIGNEE=${GC_ALIAS:-$GC_AGENT}
    EXISTING_WISP=$(gc bd list --assignee="$ASSIGNEE" --status=open,in_progress --type=molecule --title="mol-deacon-patrol" --json | jq -r 'map(select(.title=="mol-deacon-patrol"))[0].id // empty')
    gc mail inbox
    if [ -z "$EXISTING_WISP" ]; then
        NEW_WISP=$(gc bd mol wisp mol-deacon-patrol --root-only --var binding_prefix=test. --json | jq -r '.new_epic_id')
        gc bd update "$NEW_WISP" --assignee="$ASSIGNEE"
    fi
}

ledger_count() { awk 'NF{c++} END{print c+0}' "$1" 2>/dev/null; }

test_startup_is_idempotent() {
    command -v jq >/dev/null 2>&1 || { echo "SKIP: jq not installed"; return 0; }
    local tmp bin ledger
    tmp=$(mktemp -d)
    bin="$tmp/bin"
    ledger="$tmp/ledger"
    : > "$ledger"
    write_gc_stub "$bin"

    # Empty ledger -> pours exactly one.
    ( export PATH="$bin:$PATH" GC_TEST_LEDGER="$ledger" GC_ALIAS=deacon; startup_pour_decision )
    [ "$(ledger_count "$ledger")" -eq 1 ] || fail "empty start should pour exactly 1, got $(ledger_count "$ledger")"

    # An open wisp already exists -> must NOT pour a duplicate.
    ( export PATH="$bin:$PATH" GC_TEST_LEDGER="$ledger" GC_ALIAS=deacon; startup_pour_decision )
    [ "$(ledger_count "$ledger")" -eq 1 ] || fail "existing wisp: must not pour duplicate, got $(ledger_count "$ledger")"

    # Idempotency-twice: a third run is still a no-op.
    ( export PATH="$bin:$PATH" GC_TEST_LEDGER="$ledger" GC_ALIAS=deacon; startup_pour_decision )
    [ "$(ledger_count "$ledger")" -eq 1 ] || fail "idempotency: third run should stay at 1, got $(ledger_count "$ledger")"
}

test_no_invalid_wisp_type_filter
test_patrol_wisp_queries_are_title_narrowed
test_startup_pour_is_guarded
test_assignee_is_unified
test_title_match_is_exact
test_startup_is_idempotent

echo "PASS: $(basename "${BASH_SOURCE[0]}")"
