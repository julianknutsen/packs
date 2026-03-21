#!/bin/sh
set -eu

if ! command -v bd >/dev/null 2>&1; then
  echo "bd CLI not found"
  echo "Install or expose the bd binary so the discord pack can manage fix-workflow beads."
  exit 2
fi

echo "bd CLI available"
