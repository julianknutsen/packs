#!/bin/sh
set -eu

if ! command -v jq >/dev/null 2>&1; then
  echo "jq not found"
  echo "Install or expose jq so the discord pack can read workflow metadata."
  exit 2
fi

echo "jq available"
