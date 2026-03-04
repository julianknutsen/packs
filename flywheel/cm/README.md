# CM Pack

Persistent memory via [cass_memory_system](https://github.com/Dicklesworthstone/cass_memory_system).

## What this pack provides

- **MCP server**: `cass-memory` wired to `http://127.0.0.1:8766/mcp`
- **Skill**: `/recall` — query playbook for relevant rules before starting work
- **Skill**: `/reflect` — trigger reflection on recent sessions to extract lessons

## Prerequisites

Install and start the cass-memory server:

```bash
pip install cass-memory-system
cm init
cm serve --port 8766 &
```

Set the authentication token:

```bash
export CM_TOKEN="your-token-here"
```

Optional: set up nightly reflection via cron:

```bash
crontab -e
# Add: 0 3 * * * cm reflect --recent 20
```

**Note:** `cm` depends on `cass` being installed (it reads session data).

## Usage

Add to your `city.toml`:

```toml
[workspace]
includes = [
    "https://github.com/julianknutsen/packs/tree/main/flywheel/cm",
]
```
