Post a Discord message using the workspace bot token.

Examples:
  gc discord post-message --request-id dc-123-fix --body "Started work"
  gc discord post-message --channel-id 223456789012345678 --body-file ./message.txt
  gc discord post-message --channel-id 223456789012345678 --thread-id 323456789012345678 --body "Update"

Flags:
  --request-id <id>     load the target channel from a saved intake request
  --channel-id <id>     parent channel id if no request id is provided
  --thread-id <id>      optional thread id to post into
  --body <text>         inline message body
  --body-file <path>    read the message body from a file

Use `--request-id` from formulas so the message lands back in the original
conversation.
