Create a pull request using a workspace-owned GitHub App installation. Prefer
a separately scoped delivery identity; the least-privilege intake App has only
Pull requests read permission.

Example:
  gc github create-pr owner/repo \
    --github-app-identity delivery \
    --base main \
    --head fix-42 \
    --title "fix: correct widget behavior" \
    --body-file /tmp/pr.md

Arguments:
  <repository> owner/repo

Flags:
  --github-app-identity <id> separately scoped delivery App identity
  --installation-id <id>    override the identity's installation id
  --base <branch>           base branch for the PR
  --head <branch>           head branch for the PR
  --title <text>            PR title
  --body <text>             inline PR body
  --body-file <path>        read PR body from file
