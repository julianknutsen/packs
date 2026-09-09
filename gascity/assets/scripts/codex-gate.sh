#!/usr/bin/env bash
# gp-mj2q — Afik, 2026-09-06: "we should always use codex with astra";
# approved the isolated city profile and gate rollout (1788737744.207359).
# Install on citadel as assets/ops/mayor-tools/codex-gate.sh.
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  codex-gate.sh review --base REF -C REPO --output FILE [--model MODEL]
  codex-gate.sh exec PROMPT_FILE -C REPO --output FILE [--model MODEL]

Requires the city Codex profile (city.config.toml on Codex 0.153.3).
Writes the final answer to FILE and the execution transcript to FILE.log.
Exit: 0 = CLEAN, 1 = BLOCK, 2 = missing/invalid verdict or usage error;
other Codex failures retain their exit status. Commit before review.
USAGE
}
fail() { printf 'codex-gate: %s\n' "$*" >&2; exit 2; }
[[ $# -gt 0 ]] || { usage >&2; exit 2; }
mode=$1; shift
case "$mode" in
  -h|--help) usage; exit 0 ;;
  review|exec) ;;
  *) fail "unknown mode: $mode" ;;
esac
model=gpt-6-astra
repo= output= base= prompt_file=
if [[ "$mode" == exec ]]; then
  [[ $# -gt 0 && "$1" != -* ]] || fail 'exec requires PROMPT_FILE'
  prompt_file=$1; shift
fi
while [[ $# -gt 0 ]]; do
  case "$1" in
    --base|-C|--output|--model)
      [[ $# -ge 2 && -n "$2" ]] || fail "missing value for $1"
      case "$1" in
        --base) base=$2 ;;
        -C) repo=$2 ;;
        --output) output=$2 ;;
        --model) model=$2 ;;
      esac
      shift 2 ;;
    *) fail "unknown argument: $1" ;;
  esac
done
[[ -n "$repo" && -d "$repo" ]] || fail '-C must name an existing directory'
[[ -n "$output" ]] || fail '--output is required'
[[ "$model" != *'"'* && "$model" != *\\* && "$model" != *$'\n'* ]] || fail 'invalid model name'
contract='Read-only gate. Do not edit files. Report actionable findings with severity and file:line. End the final answer with exactly one standalone line: VERDICT: CLEAN if there are no critical or major findings, otherwise VERDICT: BLOCK. Do not put the verdict in a code fence or repeat it.'
args=(-p city -m "$model" -a never -s read-only -C "$repo")
# review has its own optional model setting; keep --model authoritative there too.
args+=(-c "review_model=\"$model\"" -c "developer_instructions=\"$contract\"")
if [[ "$mode" == review ]]; then
  [[ -n "$base" ]] || fail 'review requires --base REF'
  # Native `codex review` renders its own finding format and does not honor a
  # requested VERDICT line. Use exec with the same committed merge-base delta.
  base_commit=$(git -C "$repo" rev-parse --verify --end-of-options "${base}^{commit}") || fail 'invalid review base'
  merge_base=$(git -C "$repo" merge-base "$base_commit" HEAD) || fail 'no merge base with HEAD'
  head_commit=$(git -C "$repo" rev-parse --verify HEAD) || fail 'HEAD must resolve to a commit'
  worktree_status=$(git -C "$repo" status --porcelain --untracked-files=all) || fail 'cannot inspect worktree'
  [[ -z "$worktree_status" ]] || fail 'worktree has uncommitted or untracked changes; commit before review'
  if git -C "$repo" diff --quiet "$merge_base" "$head_commit"; then
    fail 'review requires a nonempty committed delta'
  else
    [[ $? -eq 1 ]] || fail 'cannot inspect review delta'
  fi
  prompt="QUICK code review of committed changes in this repository. Inspect git diff $merge_base $head_commit and the relevant surrounding code. Review only that delta; the immutable base and head are $merge_base and $head_commit. Report actionable correctness and regression findings."
else
  [[ -z "$base" ]] || fail '--base is only valid for review'
  [[ -f "$prompt_file" && -r "$prompt_file" && -s "$prompt_file" ]] || fail 'prompt file must be readable and nonempty'
  [[ ! "$prompt_file" -ef "$output" && ! "$prompt_file" -ef "$output.log" ]] || fail 'output must not overwrite the prompt'
  prompt=$(cat -- "$prompt_file")
fi
[[ -d "$(dirname -- "$output")" ]] || fail 'output directory must exist'
[[ ! -d "$output" ]] || fail 'output must name a file'
output="$(cd -- "$(dirname -- "$output")" && pwd)/$(basename -- "$output")"
answer=$(mktemp "${output}.answer.XXXXXX")
trap 'rm -f -- "$answer"' EXIT
# Clear any old answer so a failed run cannot leave a stale CLEAN result.
: > "$output" || fail 'cannot write output'
args+=(exec --skip-git-repo-check --json --output-last-message "$answer")
args+=(-- "$prompt"$'\n\n'"$contract")
result=0
codex "${args[@]}" < /dev/null > "$output.log" 2>&1 || result=$?
# Failed invocations never publish an answer that might look like CLEAN.
[[ "$result" -eq 0 ]] || exit "$result"
cat -- "$answer" > "$output"
# Read only the final answer, never echoed prompts or tool output in the log.
verdict=$(awk '
  NF {last=$0}
  {
    candidate=$0
    sub(/^ {0,3}/, "", candidate)
    if (candidate ~ /^```/ || candidate ~ /^~~~/) {
      marker=substr(candidate, 1, 1)
      tail=candidate
      if (marker == "`") sub(/^`+/, "", tail)
      else sub(/^~+/, "", tail)
      width=length(candidate)-length(tail)
      if (fence == "") {fence=marker; fence_width=width}
      else if (marker == fence && width >= fence_width && tail ~ /^[ \t]*$/) fence=""
    }
  }
  /^VERDICT:/ {count++; line=$0; in_fence=(fence != "")}
  END {if (count == 1 && !in_fence && line == last) print line}
' "$output")
case "$verdict" in
  'VERDICT: CLEAN') exit 0 ;;
  'VERDICT: BLOCK') exit 1 ;;
  *) fail "missing or invalid anchored verdict in $output (see $output.log)" ;;
esac
