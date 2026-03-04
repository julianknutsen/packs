# /recall — Query memory for relevant rules

Before starting work, query the CASS Memory System playbook for rules
and patterns relevant to the current task.

## Query for context

```bash
cm context "<brief description of your task>" --json
```

This returns relevant rules from the playbook — hard-won lessons from
past sessions about this codebase, common pitfalls, and preferred patterns.

## Read a specific rule

```bash
cm rule <rule-id> --json
```

## Tips

- Always recall before starting a non-trivial task
- The playbook contains rules that were learned the hard way — follow them
- If a rule seems outdated, note it but still follow it until it's updated
- Use concise task descriptions for better matching
