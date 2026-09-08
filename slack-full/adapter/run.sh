#!/usr/bin/env bash
#
# run.sh — entrypoint for the gc-slack-adapter. This is the pack's
# [[service]] proxy_process command, and it also works run by hand in a
# dev checkout.
#
# WHY THE SERVICE COMMAND IS A SCRIPT, NOT THE BINARY: the adapter
# binary is a gitignored build artifact, but `gc import install`
# re-materializes the pack cache GIT-ONLY. When the service command
# points straight at the binary, every pack pin bump wipes it and
# strands the service until a human rebuilds by hand. This script is
# checked in, so it survives every materialization — and when the
# binary is missing it rebuilds it from the sources sitting next to it
# before exec'ing (self-heal, loud logs, idempotent: an existing binary
# is exec'd directly with no build).
#
# Reads secrets from a sourced env file. Default location:
#   ~/.config/gc-slack-adapter/env
# Override via GC_SLACK_ADAPTER_ENV. A missing default is a warning; a
# GC_SLACK_ADAPTER_ENV you set explicitly must exist or startup fails
# (see the env-file block below for why).
#
# Required env keys (in the file):
#   SLACK_WORKSPACE_ID      # T... id, find via Slack admin or auth.test API
#   SLACK_BOT_TOKEN         # xoxb-...
#   SLACK_SIGNING_SECRET    # signing secret from Slack app's Basic Information
#   GC_CITY_NAME            # gc city the adapter posts to (matches
#                           # [workspace].name in city.toml). No default —
#                           # adapter exits at startup if unset.
#
# Optional env keys:
#   LISTEN_PUBLIC           # default :8765 (Funnel exposes this; /slack/events)
#   LISTEN_INTERNAL         # default 127.0.0.1:8766 (localhost-only; /publish)
#   INTERNAL_CALLBACK_URL   # default http://127.0.0.1:8766
#   GC_API_BASE_URL         # default http://127.0.0.1:9443
#   ADAPTER_PROVIDER        # default slack
#   REGISTER_ON_START       # default true; set false to skip self-registration

set -euo pipefail

log() { echo "gc-slack-adapter run.sh: $*" >&2; }

# Zero-padded sort key for a dotted version, so 1.9 vs 1.25 compares
# numerically. Deliberately not `sort -V`: this script runs under BSD
# userlands (launchd) where that flag is not guaranteed.
version_key() {
  local trimmed major minor patch rest
  trimmed="${1%%[!0-9.]*}"
  IFS=. read -r major minor patch rest <<<"$trimmed"
  printf '%05d%05d%05d\n' \
    "$((10#0${major:-0}))" "$((10#0${minor:-0}))" "$((10#0${patch:-0}))"
}

bin_dir="$(cd "$(dirname "$0")" && pwd)"

# A defaulted env-file path and one the operator named explicitly are
# different cases and must not fail the same way. A missing default is
# ordinary — supervised deployments often inject the env another way.
# A missing GC_SLACK_ADAPTER_ENV is operator error, and continuing is
# unsafe: the adapter validates that its required keys are *present*,
# never where they came from, so it would boot against whatever Slack
# credentials happen to be in the ambient environment (a decommissioned
# workspace's token in a stale unit file, say) instead of exiting.
#
# SCOPE: this strict rule is service-only. scripts/slack_intake_common.py
# reads the same GC_SLACK_ADAPTER_ENV for the `slack_chat_*` commands and
# treats a missing path as "no env file" (this pack's own suites use that
# idiom), so it stays lenient there. Tightening the command path is a
# separate change — do not assume the variable means the same thing on
# both sides.
#
# ${HOME:-} / ${XDG_CONFIG_HOME:-}: supervisor environments may not set
# HOME, and set -u must not abort startup over a default we only need
# when GC_SLACK_ADAPTER_ENV is unset. Honors XDG_CONFIG_HOME like the
# README's manual-sourcing instructions do. With neither set, there is no
# usable default at all: the path would degenerate to the root-owned
# /.config/gc-slack-adapter/env, which the operator being handed it
# cannot create. Leave it empty so no message can offer that path.
if [[ -n "${XDG_CONFIG_HOME:-}" || -n "${HOME:-}" ]]; then
  default_env_file="${XDG_CONFIG_HOME:-${HOME:-}/.config}/gc-slack-adapter/env"
else
  default_env_file=""
fi
env_file="${GC_SLACK_ADAPTER_ENV:-$default_env_file}"
if [[ -n "$env_file" && -f "$env_file" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "$env_file"
  set +a
elif [[ -n "${GC_SLACK_ADAPTER_ENV:-}" ]]; then
  log "ERROR: GC_SLACK_ADAPTER_ENV=$env_file does not exist — refusing to start on ambient credentials"
  if [[ -n "$default_env_file" ]]; then
    log "manual fix: create $env_file, or unset GC_SLACK_ADAPTER_ENV to fall back to $default_env_file"
  else
    log "manual fix: create $env_file (unsetting GC_SLACK_ADAPTER_ENV would not help: no HOME or XDG_CONFIG_HOME is set, so there is no default env file path)"
  fi
  exit 1
else
  # Not fatal: supervised deployments may inject env another way, and
  # the adapter validates its required keys at startup with a precise
  # error.
  if [[ -n "$default_env_file" ]]; then
    log "WARNING: env file not found at $env_file"
  else
    log "WARNING: env file not found — no HOME or XDG_CONFIG_HOME is set, so there is no default env file path to read"
  fi
  log "continuing with the inherited environment (adapter exits at startup if SLACK_WORKSPACE_ID / SLACK_BOT_TOKEN / GC_CITY_NAME are unset)"
fi

adapter_bin="$bin_dir/gc-slack-adapter"

# Fast path (idempotent): a previously built binary — exec, no build.
if [[ -x "$adapter_bin" ]]; then
  exec "$adapter_bin" "$@"
fi

# Self-heal: rebuild the binary from the sources in this directory.
log "binary missing at $adapter_bin — rebuilding from source (pack cache was likely re-materialized git-only by 'gc import install')"

# `type -P` and not `command -v`: it forces a PATH search, so a shell
# function, alias or builtin named `go` cannot shadow the lookup. That is
# reachable here — an exported function propagates into this script, and
# the env file sourced above is arbitrary shell. `command -v` returns the
# bare name `go` for a function, which the normalization below would turn
# into a nonexistent <cwd>/go while suppressing the fallback search, so a
# real installed Go would go unfound behind a confusing build error.
go_bin="$(type -P go 2>/dev/null || true)"
if [[ -n "$go_bin" ]]; then
  # Still can be a relative path (a relative PATH entry), which would
  # stop resolving once the build cd's into $bin_dir — pin it to an
  # absolute path now.
  if [[ "$go_bin" != /* ]]; then
    go_bin="$(cd "$(dirname "$go_bin")" && pwd)/$(basename "$go_bin")"
  fi
else
  # Supervisor environments (launchd, systemd) often carry a minimal
  # PATH — frequently one without /usr/bin at all, which hides a distro
  # Go (`apt install golang-go`, `dnf install golang`) from `command -v`
  # even though it is installed. ${HOME:-} keeps set -u happy when HOME
  # is unset.
  for cand in /opt/homebrew/bin/go /usr/local/go/bin/go /usr/local/bin/go \
              /usr/bin/go /bin/go "${HOME:-}/go/bin/go"; do
    if [[ -x "$cand" ]]; then go_bin="$cand"; break; fi
  done
fi
if [[ -z "$go_bin" ]]; then
  log "ERROR: no Go toolchain found (checked PATH, /opt/homebrew/bin, /usr/local/go/bin, /usr/local/bin, /usr/bin, /bin, ~/go/bin)"
  log "manual fix: cd $bin_dir && go build -o gc-slack-adapter ."
  exit 1
fi

# Check the toolchain against go.mod's `go` directive before building.
# That directive is an exact patch floor, and CI installs precisely it
# (ci.yml uses go-version-file), so CI can never surface a too-old
# toolchain — the first host to hit it is a production supervisor, and
# it deserves a named remedy rather than a raw compiler error. Only
# hard-fail where Go cannot resolve this itself: GOTOOLCHAIN=local pins
# it to what we found, whereas the default (auto) legitimately fetches
# the required toolchain, and refusing that would strand hosts that
# build fine today. An unparseable version on either side skips the
# check — the compiler is still the backstop.
go_version="$("$go_bin" version 2>/dev/null || echo 'version unknown')"
need_go="$(sed -n 's/^go[[:space:]]\{1,\}\([0-9][0-9.]*\).*/\1/p' "$bin_dir/go.mod" 2>/dev/null | head -n1)"
have_go="$(printf '%s\n' "$go_version" | sed -n 's/^go version go\([0-9][0-9.]*\).*/\1/p')"
go_too_old=0
if [[ -n "$need_go" && -n "$have_go" ]] &&
   (( 10#$(version_key "$have_go") < 10#$(version_key "$need_go") )); then
  go_too_old=1
  if [[ "${GOTOOLCHAIN:-auto}" == "local" ]]; then
    log "ERROR: need Go >= $need_go, found $have_go at $go_bin (GOTOOLCHAIN=local pins this toolchain)"
    log "manual fix: install Go >= $need_go, unset GOTOOLCHAIN, or prebuild: cd $bin_dir && go build -o gc-slack-adapter ."
    exit 1
  fi
  log "WARNING: $go_bin is $have_go but go.mod needs >= $need_go — Go will try to fetch the required toolchain (needs network and a writable module cache)"
fi

# go build REQUIRES a build cache and can only locate one through
# GOCACHE, XDG_CACHE_HOME, or HOME. All three can be unset under
# launchd/systemd — the same reason ${HOME:-} is guarded above — and the
# build then dies with "build cache is required, but could not be
# located". Fill only that gap: when Go can find a cache on its own,
# leave it alone so rebuilds keep sharing the normal one.
if [[ -z "${GOCACHE:-}" && -z "${XDG_CACHE_HOME:-}" && -z "${HOME:-}" ]]; then
  export GOCACHE="${TMPDIR:-/tmp}/gc-slack-adapter-gocache"
  log "no HOME / XDG_CACHE_HOME / GOCACHE in the environment — building with GOCACHE=$GOCACHE"
fi

# GOPATH is the same problem one layer down, but it answers a different
# question and so needs its own guard: the module cache is located via
# GOMODCACHE or GOPATH, and GOPATH derives from HOME *alone*. Gating it
# on the build-cache condition above would strand exactly the operator
# who partially remediates by exporting GOCACHE after reading the
# previous error — HOME still unset, so the toolchain fetch dies on
# "module cache not found: neither GOMODCACHE nor GOPATH is set".
if [[ -z "${GOPATH:-}" && -z "${GOMODCACHE:-}" && -z "${HOME:-}" ]]; then
  export GOPATH="${TMPDIR:-/tmp}/gc-slack-adapter-gopath"
  log "no HOME / GOMODCACHE / GOPATH in the environment — building with GOPATH=$GOPATH"
fi

# Build to a PID-suffixed temp file and mv into place: concurrent
# supervisor restarts each build their own copy, the mv is atomic, and
# nothing ever execs a half-written binary.
tmp_bin="$adapter_bin.build.$$"
trap 'rm -f "$tmp_bin"' EXIT
log "building with $go_bin ($go_version)"
if ! (cd "$bin_dir" && "$go_bin" build -o "$tmp_bin" .); then
  log "ERROR: go build failed (compiler output above) — service cannot start"
  if (( go_too_old )); then
    log "likely cause: $go_bin is $have_go but go.mod needs >= $need_go — install Go >= $need_go or prebuild the binary"
  fi
  log "manual fix: cd $bin_dir && go build -o gc-slack-adapter ."
  exit 1
fi
mv -f "$tmp_bin" "$adapter_bin"
trap - EXIT
log "rebuilt $adapter_bin OK — starting adapter"
exec "$adapter_bin" "$@"
