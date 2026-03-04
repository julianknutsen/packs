# CASS Pack

Search past agent sessions via [coding_agent_session_search](https://github.com/Dicklesworthstone/coding_agent_session_search).

## What this pack provides

- **Skill**: `/search-sessions` — search past sessions for solutions, patterns, debugging history

## Prerequisites

Install the `cass` binary:

```bash
pip install coding-agent-session-search
```

Or build from source:

```bash
git clone https://github.com/Dicklesworthstone/coding_agent_session_search.git
cd coding_agent_session_search && pip install -e .
```

## Usage

Add to your `city.toml`:

```toml
[workspace]
includes = [
    "https://github.com/julianknutsen/packs/tree/main/flywheel/cass",
]
```
