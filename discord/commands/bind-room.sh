#!/bin/sh
set -eu

if [ -z "${GC_CITY_PATH:-}" ] || [ -z "${GC_PACK_DIR:-}" ]; then
  echo "gc discord bind-room: missing Gas City pack context" >&2
  exit 1
fi

exec python3 "$GC_PACK_DIR/scripts/discord_chat_bind.py" --kind room "$@"
