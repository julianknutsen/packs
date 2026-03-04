# MCP Agent Mail Pack

Inter-agent messaging via [mcp-agent-mail](https://github.com/Dicklesworthstone/mcp_agent_mail).

## What this pack provides

- **MCP server**: `agent-mail` wired to `http://127.0.0.1:8765/mcp`
- **Skill**: `/coordinate` — register identity, send/receive messages, reserve files
- **Hook**: `UserPromptSubmit` — checks inbox on each prompt

## Prerequisites

Install and start the mcp-agent-mail server:

```bash
pip install mcp-agent-mail
mcp-agent-mail --port 8765 &
```

Set the authentication token:

```bash
export MCP_AGENT_MAIL_TOKEN="your-token-here"
```

Optionally install the `am` CLI alias:

```bash
alias am="curl -s -H 'Authorization: Bearer $MCP_AGENT_MAIL_TOKEN' http://127.0.0.1:8765"
```

## Usage

Add to your `city.toml`:

```toml
[workspace]
includes = [
    "https://github.com/julianknutsen/packs/tree/main/flywheel/mcp-agent-mail",
]
```
