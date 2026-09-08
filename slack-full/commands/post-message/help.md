Post a structured workflow-status payload (milestone | progress | rollup)
to a Slack channel, rendered via Block Kit.

This is the agent-driven status-projection surface. Unlike 'gc slack
publish' (binding-resolved) and 'gc slack reply-current' (inbound-
anchored), post-message bypasses extmsg bindings and posts directly to
the configured channel using SLACK_BOT_TOKEN.

Payload kinds:
  milestone  Headline + summary + optional label/value fields.
  progress   Headline + unicode progress bar from {current,total}.
  rollup     Headline + bulleted list from {items:[{label,value}]}.

Examples:
  gc slack post-message --channel C0123 --kind milestone \\
    --payload '{"title":"Polecat 7 reached green","summary":"Done"}'

  gc slack post-message --channel C0123 --kind progress \\
    --payload '{"title":"Convoy 12","progress":{"current":3,"total":5}}'

Pass --update <ts> with a previously-returned message ts to edit the
post in place (Slack chat.update).

Not covered by the formatting guard: the text send commands
(reply-current, publish, publish-to-channel, upload --initial-comment,
delegate) neutralize tildes that would accidentally pair into Slack
strikethrough. post-message does not — summary, field values, and
rollup items are rendered as mrkdwn section text by the Go Block Kit
renderer, downstream of the Python guard, so "~$58.5k … ~$16.5k" in a
payload still strikes through. Titles are plain_text and unaffected.
Until gp-x5bdy decides that surface, write "approx." or wrap the
figures in a code span when a payload carries tilde-as-approximately.

Routes to: gc-slack-cli post-message
