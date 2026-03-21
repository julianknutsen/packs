#!/bin/sh
set -eu

if ! command -v git >/dev/null 2>&1; then
  echo "git not found"
  echo "Install or expose git so the discord pack can create and clean worktrees."
  exit 2
fi

echo "git available"
