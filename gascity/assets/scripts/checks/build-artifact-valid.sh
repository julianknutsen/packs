#!/usr/bin/env bash
set -euo pipefail

# Generic producer-stage build-artifact validation gate.
#
# The checked formula step names its artifact contract in step metadata:
#   gc.build.artifact_schema    - expected schema id (e.g. gc.build.requirements.v1)
#   gc.build.artifact_path_keys - comma-separated workflow-root metadata keys;
#                                 the first non-empty value is the artifact path
#
# The step bead (and the ralph control bead cloned from it) carries that
# metadata, so this script reads $GC_BEAD_ID, resolves the workflow root via
# gc.root_bead_id, resolves the artifact path, and validates the artifact with
# the shared base validator. All failures print machine-readable lines on
# stderr; the dispatcher records them in gc.attempt_log as repair context for
# the next bounded producer attempt. This gate never prompts.

fail() {
  echo "build-artifact-check: $*" >&2
  exit 1
}

retryable() {
  echo "build-artifact-check: RETRYABLE: $*" >&2
  exit 75
}

BEAD_ID="${GC_BEAD_ID:-}"
[ -n "$BEAD_ID" ] || fail "GC_BEAD_ID is required"
command -v gc >/dev/null 2>&1 || fail "gc is required on PATH"
command -v python3 >/dev/null 2>&1 || fail "python3 is required on PATH"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

metadata_value() {
  # metadata_value <json> <key> -> prints metadata[key] or empty
  printf '%s' "$1" | python3 -c '
import json
import sys

key = sys.argv[1]
try:
    data = json.load(sys.stdin)
except Exception:
    print("")
    raise SystemExit(0)
if isinstance(data, list):
    data = data[0] if data else {}
if not isinstance(data, dict):
    print("")
    raise SystemExit(0)
metadata = data.get("metadata") or {}
value = metadata.get(key, "") if isinstance(metadata, dict) else ""
print(value if isinstance(value, str) else "")
' "$2"
}

resolve_show_scope() {
  local store_ref scope_root rig_json resolved

  SHOW_COMMAND=(gc)
  store_ref=${GC_TRIGGER_BEAD_STORE_REF:-${GC_TRIGGER_WORK_STORE_REF:-}}
  if [ -n "${GC_CITY:-}" ]; then
    case "$store_ref" in
      city|city:*)
        SHOW_COMMAND=(gc --city "$GC_CITY")
        return 0
        ;;
      rig:*)
        resolved=${store_ref#rig:}
        case "$resolved" in
          ''|*[!A-Za-z0-9._-]*) retryable "invalid work store ref: $store_ref" ;;
        esac
        SHOW_COMMAND=(gc --city "$GC_CITY" --rig "$resolved")
        return 0
        ;;
      '') ;;
      *) retryable "unsupported work store ref: $store_ref" ;;
    esac
  fi

  # Controller checks already receive the durable Beads scope root on current
  # runtimes. Use that exact store instead of allowing the controller cwd to
  # select the city store for a rig-owned attempt.
  scope_root=${GC_BEADS_SCOPE_ROOT:-${GC_RIG_ROOT:-}}
  case "$scope_root" in
    /*)
      if [ -n "${GC_CITY:-}" ]; then
        SHOW_COMMAND=(gc --city "$GC_CITY" --rig "$scope_root")
      else
        SHOW_COMMAND=(gc --rig "$scope_root")
      fi
      return 0
      ;;
    '') ;;
    *) retryable "Beads scope root is not absolute: $scope_root" ;;
  esac

  [ -n "${GC_CITY:-}" ] || return 0
  if rig_json="$(gc --city "$GC_CITY" rig list --json 2>/dev/null)"; then
    resolved="$(printf '%s' "$rig_json" | python3 -c '
import json
import sys

bead_id = sys.argv[1]
prefix = bead_id.split("-", 1)[0]
try:
    data = json.load(sys.stdin)
except Exception:
    raise SystemExit(0)
rigs = data.get("rigs", []) if isinstance(data, dict) else []
matches = [r for r in rigs if isinstance(r, dict) and r.get("prefix") == prefix]
if len(matches) == 1:
    rig = matches[0]
    print("city" if rig.get("hq") is True else "rig:" + str(rig.get("name", "")))
' "$BEAD_ID")"
    case "$resolved" in
      city) SHOW_COMMAND=(gc --city "$GC_CITY") ;;
      rig:*) SHOW_COMMAND=(gc --city "$GC_CITY" --rig "${resolved#rig:}") ;;
    esac
  fi
}

show_bead_with_retry() {
  local bead_id=$1 attempts delay attempt output
  attempts=${GC_BUILD_ARTIFACT_SHOW_ATTEMPTS:-12}
  delay=${GC_BUILD_ARTIFACT_SHOW_RETRY_DELAY:-1}
  case "$attempts" in
    ''|*[!0-9]*|0) retryable "invalid GC_BUILD_ARTIFACT_SHOW_ATTEMPTS=$attempts" ;;
  esac

  attempt=1
  while [ "$attempt" -le "$attempts" ]; do
    if output="$("${SHOW_COMMAND[@]}" bd show "$bead_id" --json 2>/dev/null)"; then
      printf '%s' "$output"
      return 0
    fi
    if [ "$attempt" -lt "$attempts" ]; then
      sleep "$delay"
    fi
    attempt=$((attempt + 1))
  done
  return 75
}

resolve_show_scope
SHOW_JSON="$(show_bead_with_retry "$BEAD_ID")" ||
  retryable "gc bd show $BEAD_ID remained unreadable after ${GC_BUILD_ARTIFACT_SHOW_ATTEMPTS:-12} attempts; no artifact verdict was made"

SCHEMA="$(metadata_value "$SHOW_JSON" "gc.build.artifact_schema")"
PATH_KEYS="$(metadata_value "$SHOW_JSON" "gc.build.artifact_path_keys")"
[ -n "$SCHEMA" ] || fail "step metadata gc.build.artifact_schema is missing on $BEAD_ID"
[ -n "$PATH_KEYS" ] || fail "step metadata gc.build.artifact_path_keys is missing on $BEAD_ID"

ROOT_ID="$(metadata_value "$SHOW_JSON" "gc.root_bead_id")"
ROOT_JSON="$SHOW_JSON"
if [ -n "$ROOT_ID" ] && [ "$ROOT_ID" != "$BEAD_ID" ]; then
  ROOT_JSON="$(show_bead_with_retry "$ROOT_ID")" ||
    retryable "gc bd show $ROOT_ID remained unreadable after ${GC_BUILD_ARTIFACT_SHOW_ATTEMPTS:-12} attempts; no artifact verdict was made"
fi

ARTIFACT_PATH=""
RESOLVED_KEY=""
IFS=',' read -r -a KEYS <<<"$PATH_KEYS"
for key in "${KEYS[@]}"; do
  key="$(printf '%s' "$key" | tr -d '[:space:]')"
  [ -n "$key" ] || continue
  value="$(metadata_value "$ROOT_JSON" "$key")"
  if [ -n "$value" ]; then
    ARTIFACT_PATH="$value"
    RESOLVED_KEY="$key"
    break
  fi
done
[ -n "$ARTIFACT_PATH" ] || fail "no artifact path recorded on workflow root ${ROOT_ID:-$BEAD_ID}; tried metadata keys: $PATH_KEYS. The producing stage must record the resolved artifact path before closing."

case "$ARTIFACT_PATH" in
  /*) ;;
  *)
    # Formula artifact paths are rig-relative. A producer runs in a disposable
    # per-bead worktree, so GC_WORK_DIR points at the wrong place whenever the
    # runtime provides the durable rig root. Controller checks use
    # GC_BEADS_SCOPE_ROOT on some runtimes, while agent sessions use
    # GC_RIG_ROOT. Older controllers supply neither but execute the installed
    # check from <rig>/.gc/scripts/checks, which is another durable root
    # signal. Do not use that fallback for a source-tree script.
    ARTIFACT_ROOT="${GC_RIG_ROOT:-${GC_BEADS_SCOPE_ROOT:-${GC_DIR:-}}}"
    if [ -z "$ARTIFACT_ROOT" ]; then
      INSTALLED_RIG_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
      if [ -d "$INSTALLED_RIG_ROOT/.gc" ]; then
        ARTIFACT_ROOT="$INSTALLED_RIG_ROOT"
      fi
    fi
    if [ -n "$ARTIFACT_ROOT" ]; then
      ARTIFACT_PATH="$ARTIFACT_ROOT/$ARTIFACT_PATH"
    else
      [ -n "${GC_WORK_DIR:-}" ] || fail "artifact path $ARTIFACT_PATH from $RESOLVED_KEY is relative and no rig-root environment is set"
      ARTIFACT_PATH="$GC_WORK_DIR/$ARTIFACT_PATH"
    fi
    ;;
esac
[ -f "$ARTIFACT_PATH" ] || fail "artifact $ARTIFACT_PATH from $RESOLVED_KEY does not exist"

VALIDATOR=""
for candidate in \
  ${GC_WORK_DIR:+"$GC_WORK_DIR/gascity/assets/scripts/validate_build_artifact.py"} \
  "$SCRIPT_DIR/../validate_build_artifact.py"; do
  if [ -n "$candidate" ] && [ -f "$candidate" ]; then
    VALIDATOR="$candidate"
    break
  fi
done
[ -n "$VALIDATOR" ] || fail "validate_build_artifact.py not found beside $SCRIPT_DIR or under GC_WORK_DIR"

if OUTPUT="$(python3 "$VALIDATOR" --schema "$SCHEMA" --path "$ARTIFACT_PATH" 2>&1)"; then
  echo "build artifact valid: schema=$SCHEMA path=$ARTIFACT_PATH"
  exit 0
fi

echo "build-artifact-check: schema=$SCHEMA path=$ARTIFACT_PATH failed validation" >&2
printf '%s\n' "$OUTPUT" >&2
exit 1
