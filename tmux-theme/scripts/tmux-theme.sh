#!/bin/sh
# tmux-theme.sh — Gas City status bar theme with colors.
# Usage: tmux-theme.sh <session> <agent> <config-dir>
#
# Applies a color theme based on the agent name. Uses consistent
# hashing so the same agent always gets the same color.
SESSION="$1" AGENT="$2" CONFIGDIR="$3"

# Socket-aware tmux command (uses GC_TMUX_SOCKET when set).
gcmux() { tmux ${GC_TMUX_SOCKET:+-L "$GC_TMUX_SOCKET"} "$@"; }

# ── Color palette (10 themes) ──────────────────────────────────────────
# Matches the Go DefaultPalette in internal/session/tmux/theme.go.
set -- \
    "#1e3a5f:#e0e0e0" \
    "#2d5a3d:#e0e0e0" \
    "#8b4513:#f5f5dc" \
    "#4a3050:#e0e0e0" \
    "#4a5568:#e0e0e0" \
    "#b33a00:#f5f5dc" \
    "#1a1a2e:#c0c0c0" \
    "#722f37:#f5f5dc" \
    "#0d5c63:#e0e0e0" \
    "#6d4c41:#f5f5dc"

# Consistent hash: cksum of agent name, mod palette size.
idx=$(printf '%s' "$AGENT" | cksum | awk "{print \$1 % $# + 1}")
eval "theme=\${$idx}"
bg="${theme%%:*}"
fg="${theme##*:}"

# ── Apply theme ─────────────────────────────────────────────────────────
gcmux set-option -t "$SESSION" status-style "bg=$bg,fg=$fg"
gcmux set-option -t "$SESSION" status-left-length 25
gcmux set-option -t "$SESSION" status-left " $AGENT "
gcmux set-option -t "$SESSION" status-right-length 80
gcmux set-option -t "$SESSION" status-interval 5
gcmux set-option -t "$SESSION" status-right "#($CONFIGDIR/scripts/status-line.sh $AGENT) %H:%M"
