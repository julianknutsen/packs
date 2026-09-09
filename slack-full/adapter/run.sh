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
# EXPECT A FLAP ON THE FIRST START AFTER A PIN BUMP — read this before
# debugging one. gc's supervisor gives a starting proxy_process 5
# seconds to accept on its socket and answer /healthz, then kills the
# process group and restarts on a 1s backoff, with no attempt cap. A
# COLD rebuild does not fit that window (measured on this tree: ~11s
# cold, ~0.7s warm), so the first start after a materialization wiped
# the binary is expected to be killed mid-build. It still converges:
# `go build` checkpoints per-package results in the build cache, so each
# retry resumes cheaper than the last until one finishes inside the
# window and publishes the binary. A few
#   service "slack" did not become ready before timeout
# lines right after an install are therefore normal and self-clearing.
# The same message repeating indefinitely is NOT — that means a step
# with no partial-progress checkpoint keeps restarting from zero, which
# is why the toolchain check below refuses to start a toolchain DOWNLOAD
# under the supervisor. To skip the flap entirely, prebuild:
#   cd adapter && go build -o gc-slack-adapter .
# The structural fix (a per-service readiness timeout, or building at
# `gc import install` time) belongs to gc itself, not to this pack.
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

# Are we running as a gc-supervised service, or by hand? The supervisor
# exports both of these into every proxy_process it starts, and nothing
# else in this pack sets them. Only the startup-window rule below cares:
# supervised means a hard 5s readiness deadline and an uncapped
# kill/restart loop, so work that cannot make partial progress must not
# be started here at all.
supervised=0
if [[ -n "${GC_SERVICE_NAME:-}" || -n "${GC_SERVICE_URL_PREFIX:-}" ]]; then
  supervised=1
fi

# A private, reusable scratch dir under TMPDIR for the Go caches below.
#
# PER-USER, because /tmp is world-writable: a fixed shared name like
# .../gc-slack-adapter-gocache can be pre-created by any other local
# user, and Go trusts whatever it finds in these caches — a pre-owned
# GOCACHE can serve object files into a binary that holds the Slack bot
# token and signing secret, and a pre-owned GOPATH is where a fetched
# toolchain is unpacked and re-exec'd. The uid suffix takes the name out
# of the shared namespace, `mkdir` (no -p, so it fails rather than
# accepts an existing dir) plus -O keeps us from adopting one somebody
# else got to first, and /tmp's sticky bit stops them swapping it out
# afterwards, since only the owner can unlink it.
#
# STABLE ACROSS RESTARTS, deliberately: warm rebuilds measured ~0.7s
# against ~11s cold, and it is exactly that reuse between supervisor
# restarts that lets the header's flap converge. A fresh mktemp dir per
# start would never converge on the HOME-less hosts this path serves, so
# it is only the fallback for the anomalous case where the stable name
# is unusable — availability over speed there, never as the normal path.
private_cache_dir() {
  # `$1` and not `$name` in dir=: one `local` expands every right-hand
  # side before it assigns any of them, so `$name` is still empty here.
  local name="$1" dir="${TMPDIR:-/tmp}/$1.${EUID}"
  if mkdir -m 700 "$dir" 2>/dev/null; then
    printf '%s\n' "$dir"
    return 0
  fi
  if [[ -d "$dir" && ! -L "$dir" && -O "$dir" ]]; then
    chmod 700 "$dir" 2>/dev/null || true
    printf '%s\n' "$dir"
    return 0
  fi
  if [[ -e "$dir" || -L "$dir" ]]; then
    log "WARNING: $dir exists but is not a directory this user owns — refusing to reuse it (another local user could be seeding the Go cache)"
  fi
  mktemp -d "${TMPDIR:-/tmp}/$name.XXXXXX" 2>/dev/null
}

# go build REQUIRES a build cache and can only locate one through
# GOCACHE, XDG_CACHE_HOME, or HOME. All three can be unset under
# launchd/systemd — the same reason ${HOME:-} is guarded above — and the
# build then dies with "build cache is required, but could not be
# located". Fill only that gap: when Go can find a cache on its own,
# leave it alone so rebuilds keep sharing the normal one.
if [[ -z "${GOCACHE:-}" && -z "${XDG_CACHE_HOME:-}" && -z "${HOME:-}" ]]; then
  gocache_dir="$(private_cache_dir gc-slack-adapter-gocache || true)"
  if [[ -n "$gocache_dir" ]]; then
    export GOCACHE="$gocache_dir"
    log "no HOME / XDG_CACHE_HOME / GOCACHE in the environment — building with GOCACHE=$GOCACHE"
  else
    # Leave GOCACHE unset rather than point Go at a path we could not
    # secure; the build's own "build cache is required" error is a
    # better diagnostic than a poisoned cache.
    log "WARNING: could not create a private build cache under ${TMPDIR:-/tmp} — go build will report the cache error itself"
  fi
fi

# GOPATH is the same problem one layer down, but it answers a different
# question and so needs its own guard: the module cache is located via
# GOMODCACHE or GOPATH, and GOPATH derives from HOME *alone*. Gating it
# on the build-cache condition above would strand exactly the operator
# who partially remediates by exporting GOCACHE after reading the
# previous error — HOME still unset, so the toolchain fetch dies on
# "module cache not found: neither GOMODCACHE nor GOPATH is set".
if [[ -z "${GOPATH:-}" && -z "${GOMODCACHE:-}" && -z "${HOME:-}" ]]; then
  gopath_dir="$(private_cache_dir gc-slack-adapter-gopath || true)"
  if [[ -n "$gopath_dir" ]]; then
    export GOPATH="$gopath_dir"
    log "no HOME / GOMODCACHE / GOPATH in the environment — building with GOPATH=$GOPATH"
  else
    log "WARNING: could not create a private module cache under ${TMPDIR:-/tmp} — go build will report the module cache error itself"
  fi
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
#
# `|| true` on the go.mod read: with a missing go.mod the sed exits 2,
# pipefail propagates it, and set -e would kill the script AT THE
# ASSIGNMENT with no message — the one silent exit in an otherwise
# loudly-logged script. An empty need_go already skips the version gate
# by design, and the compiler then produces the real diagnostic.
go_version="$("$go_bin" version 2>/dev/null || echo 'version unknown')"
need_go="$(sed -n 's/^go[[:space:]]\{1,\}\([0-9][0-9.]*\).*/\1/p' "$bin_dir/go.mod" 2>/dev/null | head -n1 || true)"
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
  # Supervised, and Go is about to resolve a newer toolchain. If that
  # means DOWNLOADING one, refuse rather than start it inside a 5s
  # window it cannot finish in. Unlike a compile, a toolchain fetch keeps
  # no partial progress: Go downloads it to a temp file and renames, so
  # every kill throws the whole transfer away and the next restart begins
  # again from zero. That turns the ordinary flap-then-converge described
  # in the header into a permanent loop that re-downloads tens of MB
  # every ~8s forever. Exiting instead makes each cycle cheap and legible
  # — an early exit takes the supervisor's exited-early path, which skips
  # the kill and records this script's own stderr as the reason. It does
  # NOT stop the restart loop (nothing in-repo can); it stops the loop
  # from burning the network while hiding its cause.
  #
  # But only when a download is really required. The toolchain may
  # already be in the module cache from an earlier hand-run, and that
  # build needs no network and DOES converge — refusing it would turn
  # the self-heal this script exists for back into a permanent outage.
  # Ask Go itself, offline, instead of guessing from version numbers: run
  # from the module directory so the go command performs its toolchain
  # switch, with GOPROXY=off so a missing toolchain fails instead of
  # fetching. rc=0 means the switch resolved from local caches. This runs
  # after the GOCACHE/GOPATH defaulting above on purpose — the switch
  # extracts into the module cache, so the probe has to see the same
  # environment the build will get. `go version` does no module work
  # beyond that switch, and GOPROXY=off forbids network access outright,
  # so the probe itself cannot spend the readiness window it is here to
  # protect — it either exec's a local toolchain or fails immediately.
  if (( supervised )); then
    if (cd "$bin_dir" && GOPROXY=off "$go_bin" version >/dev/null 2>&1); then
      log "WARNING: $go_bin is $have_go but go.mod needs >= $need_go — the required toolchain is already in the local module cache, so the build proceeds without a download"
    else
      log "ERROR: need Go >= $need_go, found $have_go at $go_bin, and GOTOOLCHAIN=${GOTOOLCHAIN:-auto} would have to fetch the required toolchain over the network (it is not in the local module cache)"
      log "refusing to start that download under the gc supervisor: a starting service has ~5s to become ready before it is killed and restarted, and a toolchain fetch cannot resume, so it would re-download from scratch on every restart forever"
      log "manual fix: install Go >= $need_go on this host, or prebuild once: cd $bin_dir && go build -o gc-slack-adapter ."
      log "(running this script by hand, outside the supervisor, still lets Go fetch the toolchain)"
      exit 1
    fi
  else
    log "WARNING: $go_bin is $have_go but go.mod needs >= $need_go — Go will try to fetch the required toolchain (needs network and a writable module cache)"
  fi
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
