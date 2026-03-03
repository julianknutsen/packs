#!/bin/sh
# cycle.sh — cycle between Gas City agent sessions in the same group.
# Usage: cycle.sh next|prev <current-session> <client-tty>
# Called via tmux run-shell from a keybinding.
#
# Grouping rules (activated by session name pattern):
#   Rig ops:       {rig}--witness ↔ {rig}--refinery ↔ {rig}--polecat-*  (per rig)
#   Rig crew:      {rig}--{name} members  (per rig)
#   Town group:    mayor ↔ deacon
#   Dog pool:      dog-1 ↔ dog-2 ↔ dog-3
#   Fallback:      all gc-* sessions (works for any pack)
#
# Session name format: gc-{city}-{agent}
#   City-scoped:  gc-mycity-mayor, gc-mycity-deacon, gc-mycity-dog-1
#   Rig-scoped:   gc-mycity-myrig--witness, gc-mycity-myrig--polecat-1

direction="$1"
current="$2"
client="$3"

[ -z "$direction" ] || [ -z "$current" ] || [ -z "$client" ] && exit 0

# Determine the group filter pattern based on session name conventions.
case "$current" in
    # Rig ops: witness ↔ refinery ↔ polecats in same rig.
    *--witness|*--refinery|*--polecat-*)
        rig="${current%%--*}"
        pattern="^${rig}--\(witness\|refinery\|polecat-\)"
        ;;
    # Other rig-scoped (crew, etc): cycle all same-rig agents.
    *--*)
        rig="${current%%--*}"
        pattern="^${rig}--"
        ;;
    # Town group: mayor ↔ deacon.
    *-mayor|*-deacon)
        city="${current%-mayor}"
        city="${city%-deacon}"
        pattern="^${city}-\(mayor\|deacon\)$"
        ;;
    # Dog pool: cycle between dog instances.
    *-dog-[0-9]*)
        prefix=$(printf '%s' "$current" | sed 's/dog-[0-9]*$/dog-/')
        pattern="^${prefix}[0-9]"
        ;;
    # Unknown — cycle all gc-* sessions as fallback.
    *)
        pattern="^gc-"
        ;;
esac

# Get target session: filter to same group, find current, pick next/prev.
target=$(tmux list-sessions -F '#{session_name}' 2>/dev/null \
    | grep "$pattern" \
    | sort \
    | awk -v cur="$current" -v dir="$direction" '
        { a[NR] = $0; if ($0 == cur) idx = NR }
        END {
            if (NR <= 1 || idx == 0) exit
            if (dir == "next") t = (idx % NR) + 1
            else t = ((idx - 2 + NR) % NR) + 1
            print a[t]
        }')

[ -z "$target" ] && exit 0
tmux switch-client -c "$client" -t "$target"
