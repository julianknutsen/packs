#!/bin/sh
set -eu

city_root="${GC_CITY_ROOT:-${GC_CITY_PATH:-}}"
if [ -z "$city_root" ]; then
  city_root="."
fi

legacy_root="$city_root/.gc/services/discord-intake"
legacy_config="$legacy_root/data/config.json"
legacy_bot_token="$legacy_root/secrets/bot-token.txt"

if [ -f "$legacy_config" ] || [ -f "$legacy_bot_token" ]; then
  echo "legacy discord-intake state detected at $legacy_root"
  echo "Do not load both discord-intake and discord in the same workspace."
  echo "Migrate to the discord pack, then remove the old discord-intake include."
  exit 2
fi

echo "no legacy discord-intake state detected"
