#!/bin/sh
set -eu

if [ -z "${GC_PACK_DIR:-}" ]; then
    echo "gc gastown witness-heartbeat-check: missing Gas City pack context" >&2
    exit 2
fi

exec "$GC_PACK_DIR/assets/scripts/witness-heartbeat-check.sh" "$@"
