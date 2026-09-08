# gc gastown witness-heartbeat-check

Measure heartbeat freshness for every running or sleeping Gastown witness.

The command is read-only. It prints one TSV row per checked witness and exits
with:

- `0` when every checked witness is fresh (or none are eligible)
- `1` when it finds a stalled witness or a witness with no usable heartbeat
- `2` when configuration or session-roster errors prevent measurement

Set `GASTOWN_WITNESS_STALE_MIN` to override the default 90-minute window.
