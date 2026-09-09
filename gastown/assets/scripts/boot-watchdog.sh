#!/usr/bin/env bash
set -euo pipefail

message=${GASTOWN_BOOT_MESSAGE:-"Boot watchdog tick"}
last_error=""
targets=()

add_target() {
    local target=$1
    local existing

    [[ -n "$target" ]] || return 0
    for existing in "${targets[@]}"; do
        [[ "$existing" != "$target" ]] || return 0
    done
    targets+=("$target")
}

if [[ -n "${GASTOWN_BOOT_TARGET:-}" ]]; then
    add_target "$GASTOWN_BOOT_TARGET"
else
    if [[ -n "${GC_PACK_NAME:-}" ]]; then
        add_target "$GC_PACK_NAME.boot"
    fi
    add_target "boot"
fi

for target in "${targets[@]}"; do
    if output=$(gc session nudge "$target" "$message" 2>&1); then
        printf '%s\n' "$output"
        exit 0
    fi
    last_error="target $target failed: $output"
done

printf 'boot watchdog: unable to nudge boot target; set GASTOWN_BOOT_TARGET for non-standard bindings. %s\n' "$last_error" >&2
exit 1
