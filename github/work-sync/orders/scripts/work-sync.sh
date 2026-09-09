#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
order_dir="${ORDER_DIR:-$(cd "$script_dir/.." && pwd -P)}"
# The opt-in Rig order and City ingress share the runtime from the same pinned
# repository checkout. Do not import the parent service pack into a Rig.
pack_root="$(cd "$order_dir/../.." && pwd -P)"

args=()
if [[ "${GC_GITHUB_WORK_SYNC_DRY_RUN:-}" == "1" ]]; then
  args+=(--dry-run)
fi

python3 "$pack_root/scripts/github_work_sync.py" "${args[@]}"
