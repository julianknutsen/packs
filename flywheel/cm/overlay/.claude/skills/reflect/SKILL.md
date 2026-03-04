# /reflect — Trigger memory reflection

After completing work or a debugging session, trigger reflection to
extract lessons learned into the CASS Memory System playbook.

## Record an outcome

After finishing a task, record what happened:

```bash
cm outcome --session "$GC_SESSION" --result "success|failure|partial" --notes "Brief description of what was learned"
```

## Trigger reflection

Run reflection to analyze recent sessions and extract new rules:

```bash
cm reflect --recent 5 --json
```

## Tips

- Reflect after difficult debugging sessions — those are the most valuable lessons
- Reflection is expensive (analyzes multiple sessions), don't run it after every task
- The system automatically extracts rules; you just need to trigger it
- Nightly cron reflection is recommended: `0 3 * * * cm reflect --recent 20`
