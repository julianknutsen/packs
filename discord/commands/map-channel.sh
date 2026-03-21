#!/bin/sh
set -eu

if [ -z "${GC_CITY_PATH:-}" ] || [ -z "${GC_PACK_DIR:-}" ]; then
  echo "gc discord map-channel: missing Gas City pack context" >&2
  exit 1
fi

exec python3 "$GC_PACK_DIR/scripts/discord_intake_map_channel.py" "$@"
