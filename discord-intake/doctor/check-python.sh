#!/bin/sh
set -eu

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 not found"
  echo "Install Python 3.11 or newer to run the discord-intake services."
  exit 2
fi

python3 - <<'PY'
import sys
if sys.version_info < (3, 11):
    print(f"python3 is {sys.version.split()[0]}; need 3.11+")
    print("Install Python 3.11 or newer to run the discord-intake services.")
    raise SystemExit(2)
print(f"python3 {sys.version.split()[0]} available")
PY
