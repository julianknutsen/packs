Show the RLM pack runtime state for the current city.

The report includes:
  - whether the runtime is installed
  - rlms package version
  - configured backend, model, base URL, and environment policy
  - Docker image availability for sandboxed execution
  - the newest run and the most recent failed run, when present

Usage:
  gc rlm status
  gc rlm status --json
