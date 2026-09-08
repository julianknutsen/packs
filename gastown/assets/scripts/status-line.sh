#!/bin/sh
# status-line.sh — tmux status-right helper for Gas Town agents.
# Usage: status-line.sh <agent-name> [city-path]
# Called by tmux every status-interval seconds via #(command).
# Always exits 0 — tmux must never see errors.
#
# Counts are cached with a short TTL so tmux's frequent refresh does not query
# Beads on every render across every agent. TTL override:
# GC_STATUSLINE_TTL (seconds). Cache directory override:
# GC_STATUSLINE_CACHE_DIR.

agent="$1"
city="${2:-${GC_CITY:-${GT_ROOT:-${GC_DIR:-}}}}"
[ -z "$agent" ] && exit 0

if [ -n "$city" ] && [ -d "$city" ]; then
    cd "$city" 2>/dev/null || true
fi

run_bounded() {
    if command -v timeout >/dev/null 2>&1; then
        timeout 2s "$@"
    elif command -v perl >/dev/null 2>&1; then
        # macOS ships no coreutils timeout. A plain alarm+exec is not enough:
        # the Go runtime swallows SIGALRM, so gc would run unbounded anyway.
        # Fork and SIGTERM (then SIGKILL) the child instead — Go honors TERM.
        perl -e '
            my $t = shift @ARGV;
            my $p = fork;
            if (!$p) { exec @ARGV or exit 127 }
            $SIG{ALRM} = sub { kill "TERM", $p; select undef, undef, undef, 0.5; kill "KILL", $p; waitpid $p, 0; exit 124 };
            alarm $t;
            waitpid $p, 0;
            exit($? >> 8);
        ' 2 "$@"
    else
        "$@"
    fi
}

json_array_count() {
    if ! command -v jq >/dev/null 2>&1; then
        printf '0'
        return 0
    fi

    n=$(run_bounded "$@" 2>/dev/null | jq 'if type == "array" then length else 0 end' 2>/dev/null || true)
    case "$n" in
        ''|*[!0-9]*) printf '0' ;;
        *) printf '%s' "$n" ;;
    esac
}

first_number() {
    n=$(run_bounded "$@" 2>/dev/null | awk '{print $1+0; exit}' || true)
    case "$n" in
        ''|*[!0-9]*) printf '0' ;;
        *) printf '%s' "$n" ;;
    esac
}

cache_mtime() {
    stat -c %Y "$1" 2>/dev/null || stat -f %m "$1" 2>/dev/null || printf '0'
}

is_number() {
    case "$1" in
        ''|*[!0-9]*) return 1 ;;
        *) return 0 ;;
    esac
}

cache_ttl="${GC_STATUSLINE_TTL:-30}"
is_number "$cache_ttl" || cache_ttl=30
if [ -n "${GC_STATUSLINE_CACHE_DIR:-}" ]; then
    cache_dir="$GC_STATUSLINE_CACHE_DIR"
    cache_private=0
else
    cache_base="${XDG_RUNTIME_DIR:-${TMPDIR:-/tmp}}"
    uid=$(id -u 2>/dev/null || printf 'unknown')
    cache_dir="$cache_base/gc-statusline-$uid"
    cache_private=1
fi
cache_city=$(pwd -P 2>/dev/null || pwd)
safe_agent=$(printf '%s' "$agent" | tr -c 'A-Za-z0-9._-' '_')
cache_key=$(printf '%s\n%s\n' "$cache_city" "$agent" | cksum | awk '{print $1}')
cache="$cache_dir/gc-statusline-${safe_agent}-${cache_key}.cache"

now=$(date +%s 2>/dev/null || printf '0')
mtime=$(cache_mtime "$cache")
if is_number "$now" && is_number "$mtime" && [ "$mtime" -gt 0 ] && [ "$((now - mtime))" -lt "$cache_ttl" ]; then
    read -r w m < "$cache" 2>/dev/null || true
    is_number "${w:-}" || w=0
    is_number "${m:-}" || m=0
else
    # Single-flight: only one render per agent may refresh; concurrent
    # renders serve the stale cache instead of piling more gc (store)
    # queries onto an already-slow store — stacked slow renders are the
    # exact feedback loop that makes the store slower.
    mkdir -p "$cache_dir" 2>/dev/null || true
    if [ "$cache_private" = 1 ]; then
        chmod 700 "$cache_dir" 2>/dev/null || true
    fi
    lock="$cache.lock"
    if mkdir "$lock" 2>/dev/null; then
        trap 'rmdir "$lock" 2>/dev/null' EXIT INT TERM

        # Preserve gc hook ready-work semantics while bounding tmux refreshes.
        w=$(json_array_count gc hook "$agent")

        # Preserve gc mail check unread/recipient-route semantics while caching.
        m=$(first_number gc mail check "$agent")

        printf '%s %s\n' "${w:-0}" "${m:-0}" > "$cache" 2>/dev/null || true
    else
        # Refresh already in flight: serve stale values. Break locks older
        # than 120s so a killed refresher cannot wedge the status line.
        lock_mtime=$(cache_mtime "$lock")
        if is_number "$lock_mtime" && [ "$lock_mtime" -gt 0 ] && [ "$((now - lock_mtime))" -gt 120 ]; then
            rmdir "$lock" 2>/dev/null || true
        fi
        read -r w m < "$cache" 2>/dev/null || true
        is_number "${w:-}" || w=0
        is_number "${m:-}" || m=0
    fi
fi

# Format: agent | hook-icon N | mail-icon N  (omit segments that are 0)
printf '%s' "$agent"
[ "${w:-0}" -gt 0 ] && printf ' | 🪝 %d' "$w"
[ "${m:-0}" -gt 0 ] && printf ' | 📬 %d' "$m"
exit 0
