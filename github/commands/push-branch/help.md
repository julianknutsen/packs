Push the current git HEAD to a named GitHub branch using the workspace-owned
GitHub App installation. Prefer a separately scoped delivery identity; the
least-privilege intake App does not have Contents write permission.

Example:
  gc github push-branch owner/repo \
    --github-app-identity delivery \
    --branch fix-42

Arguments:
  <repository> owner/repo

Flags:
  --github-app-identity <id> separately scoped delivery App identity
  --installation-id <id>    override the identity's installation id
  --branch <name>           branch name to create or update
  --ref <spec>              source ref to push (default: HEAD)
