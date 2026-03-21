#!/bin/sh
set -eu

if ! command -v gc >/dev/null 2>&1; then
  echo "gc CLI not found"
  echo "Install or expose the gc binary so the discord pack can dispatch formulas."
  exit 2
fi

echo "gc CLI available"
