# GitHub Intake Pack

Workspace-hosted GitHub slash-command intake for Gas City.

This pack keeps `gastown-hosted` generic. It runs the GitHub-facing service
inside the workspace and exports it through the normal published-service path:

- `github-webhook` is the public webhook endpoint GitHub calls
- `github-admin` is the tenant-visible setup and status surface
- both services share `.gc/services/github-intake/`

The current slice ships:

- GitHub App manifest/bootstrap hosted by the workspace service
- webhook signature validation
- durable receipt and request persistence
- issue-only `/gc fix ...` command parsing with multiline context support
- per-issue idempotency for `/gc fix`
- write/admin permission verification through the imported GitHub App
- rig bead creation plus `gc sling <target> <bead> --on <formula>` dispatch
- pack commands for issue comments, authenticated branch push, and PR creation
- `mol-github-fix-issue` workflow for TDD bugfixes
- issue-only `/gc fix` routing for this phase; other `/gc` commands are intentionally ignored

The new `/gc fix` path does not post an immediate ack comment. The workflow
itself comments when work starts and when the PR is ready for review.

If a dispatched workflow gets wedged and you need to retry the same issue
before cancel/retry automation exists, release the intake lock manually:

```bash
gc github-intake release-workflow owner/repo 42
```

## Include It

```toml
[packs.github-intake]
source = "https://github.com/julianknutsen/packs.git"
ref = "main"
path = "github-intake"

[workspace]
includes = ["github-intake"]
```

## Publication

This pack expects helper-backed published services. After the workspace starts,
`gc service list` should show:

- `github-webhook` with public publication
- `github-admin` with tenant publication

Open the tenant-visible `github-admin` URL to register the GitHub App from the
hosted manifest helper.

## Repository Mapping

After app bootstrap, map repositories to slash-command targets:

```bash
gc github-intake map-repo owner/repo rig/polecat \
  --fix-formula mol-github-fix-issue
```

That stores dispatch config locally under `.gc/services/github-intake/data/`.

## Manual App Import

If the manifest flow is not suitable, you can import an existing app:

```bash
gc github-intake import-app \
  --app-id 123456 \
  --client-id Iv1.example \
  --webhook-secret "$GITHUB_WEBHOOK_SECRET" \
  --private-key-file ./github-app.private-key.pem
```

## Inspect Status

```bash
gc github-intake status
gc github-intake status --json
```

## Workflow Helpers

The pack also exposes helper commands the workflow can call directly:

```bash
gc github-intake comment-issue owner/repo 42 --installation-id 123 --body "hello"
gc github-intake push-branch owner/repo --installation-id 123 --branch fix-42
gc github-intake create-pr owner/repo --installation-id 123 --base main --head fix-42 --title "fix: widget"
```
