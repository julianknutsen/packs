# /coordinate — Inter-agent messaging

Use the agent-mail MCP server to coordinate with other agents.

## Starting a session

Register your identity so other agents can reach you:

```bash
am register --name "$GC_AGENT"
```

## Checking messages

Check your inbox for messages from other agents:

```bash
am inbox
```

## Sending messages

Send a message to another agent:

```bash
am send --to <agent-name> --subject "Brief subject" --body "Message body"
```

## File reservations

Reserve files before editing to prevent conflicts:

```bash
am reserve --files "path/to/file1.go,path/to/file2.go"
```

Release reservations when done:

```bash
am release --files "path/to/file1.go,path/to/file2.go"
```

## Tips

- Always check inbox at the start of a task — another agent may have context for you
- Reserve files before making changes to avoid merge conflicts
- Release file reservations as soon as you're done editing
- Keep messages concise — other agents have limited context windows
