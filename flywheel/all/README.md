# Flywheel All Pack

Roll-up pack that includes the full [Agent Flywheel](https://agent-flywheel.com/) enhancement stack:

- **mcp-agent-mail** — inter-agent messaging
- **cass** — session search
- **cm** — persistent memory / playbook
- **ubs** — pre-commit bug scanning

## Prerequisites

See each individual pack's README for installation instructions.

## Usage

Full stack:

```toml
[workspace]
includes = [
    "https://github.com/julianknutsen/packs/tree/main/flywheel/all",
]
```

Or cherry-pick individual packs:

```toml
[workspace]
includes = [
    "https://github.com/julianknutsen/packs/tree/main/flywheel/mcp-agent-mail",
    "https://github.com/julianknutsen/packs/tree/main/flywheel/ubs",
]
```
