# GitHub Intake Pack

Workspace-hosted GitHub comment and event intake for Gas City.

This pack keeps `gastown-hosted` generic. It runs the GitHub-facing service
inside the workspace and exports it through the normal published-service path:

- `github-webhook` is the public webhook endpoint GitHub calls
- `github-admin` is the tenant-visible setup and status surface
- both services share `.gc/services/github/`

The current slice ships:

- GitHub App manifest/bootstrap hosted by the workspace service
- env-backed or identity-resolved GitHub App secrets for service runtimes
- webhook signature validation
- durable receipt and request persistence
- checked-in event rules loaded from `config/github-intake/rules.toml`
- repo-scoped addressed-message intake such as `@mayor <request>`
- issue-only `/gc fix ...` command parsing with multiline context support
- per-issue idempotency for `/gc fix`
- write/admin permission verification through the imported GitHub App
- bugflow source-bead creation plus immediate bugflow router scan
- pack commands for issue comments, authenticated branch push, and PR creation
- `/gc fix` routing into the workflows-pack `mol-bug-report-flow-v2` pipeline
- issue-only `/gc fix` routing for this phase; other `/gc` commands are intentionally ignored

The `/gc fix` path posts a queued-in-bugflow acknowledgement after the bugflow
source bead and router scan succeed. Bugflow snapshots the issue and comments,
then owns investigation, classification, implementation gates, PR review, CI,
and final issue updates.

If a dispatched workflow gets wedged and you need to retry the same issue
before cancel/retry automation exists, release the intake lock manually:

```bash
gc github release-workflow owner/repo 42
```

## Import It

```toml
# pack.toml
[imports.github]
source = "../packs/github"
```

## Bidirectional Work Sync

Work sync is opt-in per Rig. The City-level `github` import owns ingress,
credentials and the existing GitHub service data directory; it deliberately
does **not** register a City-wide work-sync order. Import the service-free
`github/work-sync` subpack on each participating Rig:

```toml
# city.toml
[[rigs]]
name = "product"

[rigs.imports.github_work_sync]
source = "../packs/github/work-sync"
```

Bind that Rig's repository path in `.gc/site.toml`, as for any native Rig.
For remote imports, use
`https://github.com/gastownhall/gascity-packs.git//github/work-sync`, run
`gc import install`, and pin ingress and work-sync to the **same accepted
repository commit** in `packs.lock`. The subpack reuses
`github/scripts/github_work_sync.py` and the intake helpers from that checkout;
it does not copy credentials, start a second service, or import the City-only
service pack into a Rig.

The one `work-sync` order uses Gas City's existing five-minute cooldown,
tracking and history. Two importing Rigs get independent scoped instances.
Webhook actions with `route_from_repo = true` invoke this same order. Always
select the exact Rig for a manual run:

```sh
gc order list
gc order run work-sync --rig product
gc order history work-sync --rig product
```

Provide these non-secret runtime settings to the controller/order environment:

- `GC_WORK_SYNC_POLICY_BIN`: absolute path to the work-sync policy provider executable.
- `GC_WORK_SYNC_POLICY_ROOT`: absolute path to the policy contract root.
- `GC_GITHUB_WORK_SYNC_DRY_RUN=1`: optional additional read-only guard.

The runner reads the configured provider’s `--json work-sync runtime-contract` and uses
the pure `work-sync plan` command. The canonical contract's
`live_mutations = false` always forces dry-run, even if the environment flag
is absent. Dry-run performs authenticated reads and reports planned operation
counts, but does not apply Beads or GitHub changes. Supply the same supported
GitHub App identity resolver/routing configuration as ingress; periodic runs
mint short-lived installation tokens through the existing intake helpers.
Do not configure a PAT or widen the intake App to delivery permissions.

The runtime owns Issues/Projects v2 projection, reconciliation and receipts; the configured provider supplies policy and a pure plan, not another worker or store.
It reads the exact Rig via native `gc beads snapshot` and applies incoming
changes through whole-row `gc beads update-cas`. A native snapshot/CAS-capable
Gas City build is required; comment imports additionally require transactional
comment support in the selected Beads provider. Unsupported capabilities,
unknown/mismatched routes, schema drift, identity collisions and concurrent
edits fail closed. There is no direct database fallback. The experimental
Beads proxied-server mode is not required.

The runtime accepts exactly one current managed-block contract from the policy
provider. A legacy or unknown marker fails before planning or writing; the Pack
does not contain a compatibility migration writer.

Stable node IDs bind Issues and Projects items to Beads; replay receipts stay
under the target City's existing `.gc/services/github/data` root. Cross-City
webhook dispatch must not inherit the ingress City's receipt location.
Writes require readback, ambiguous
transport outcomes are reconciled before retry, and conflicts prevent unsafe
overwrite. A successful reconciliation requires a final zero-operation plan.
Receipts distinguish the projected Bead revision from the revision proven by
an incoming CAS; importing a request does not imply it was projected back.
Output exposes counts and reason codes, not issue bodies or credentials.

Run the native composition regression against a candidate Gas City binary:

```sh
GC_TEST_BIN=/absolute/path/to/gc \
  python3 -m unittest discover -s github/tests -p test_github_work_sync_order.py -v
```

It imports the actual packs into two temporary file-store Rigs, checks that
ingress creates no Rig-less reconciler, and exercises real order dispatch,
per-Rig history and cooldown. Only the external Python runner is substituted;
the test starts no supervisor and contacts neither GitHub nor a live store.

## Publication

This pack expects helper-backed published services. After the workspace starts,
`gc service list` should show:

- `github-webhook` with public publication
- `github-admin` with tenant publication

Open the tenant-visible `github-admin` URL to register the GitHub App from the
hosted manifest helper.

The generated intake manifest follows the least-privilege work-sync contour:
repository metadata read, Issues write, Pull requests read (required for the
subscribed webhook), organization Projects write, and organization Issue Types
and Issue Fields read. It does not request Contents, Actions, Administration,
or Workflows. Branch push and PR creation therefore require a separately
scoped delivery identity instead of broadening the organization intake App.

## Bugflow Routing

`/gc fix` now uses the workflows-pack bugflow router. Configure repository
routing with the shared workflows routing config:

```bash
gc workflows pr-review config set-repo owner/repo \
  --city /abs/path/to/city \
  --rig <rig> \
  --base-branch main
```

The legacy `gc github map-repo ... --fix-formula mol-github-fix-issue`
command still records old direct-fix mappings in
`.gc/services/github/data/`, but `/gc fix` no longer requires those
mappings.

## Event Rules

City-owned event rules live at `config/github-intake/rules.toml` by default
or at `$GC_GITHUB_INTAKE_RULES_FILE` when set. Rules match exact dotted
GitHub payload fields and run ordered actions synchronously. The first action
type is `order`, which runs a checked-in Gas City order with GitHub event
context in environment variables:

```toml
version = 1

[[rule]]
id = "pr-review-on-needs-review-label"
event = "pull_request"

[rule.match]
action = "labeled"
label.name = "status/needs-review"
pull_request.state = "open"

[[rule.action]]
type = "order"
name = "pr-review-request"
github_app_token_env = "GH_TOKEN"
```

For organization ingress that owns repositories in more than one registered
City, declare the exact owning City and Rig once in the repo topology and opt
the action into topology routing:

```toml
[[repo]]
full_name = "owner/repo"
city = "product-city"
rig = "product"

[[rule.action]]
type = "order"
name = "work-sync"
route_from_repo = true
```

That action runs as
`gc --city product-city order run work-sync --rig product`. Unknown repositories
and incomplete City/Rig mappings fail closed before a subprocess starts; there
is no fallback queue or repository-local work store. Rules without
`route_from_repo = true` retain their existing local-City behavior.

Rules ignore events sent by the configured GitHub App bot by default. Set
`allow_self = true` on a rule when bot-authored label changes are intentional
triggers, such as a GitHub Action adding `status/needs-triage`.

The order receives fields such as `GC_GITHUB_PR_URL`, `GC_GITHUB_ISSUE_URL`,
`GC_GITHUB_REPO`, `GC_GITHUB_ITEM_NUMBER`, and
`GC_GITHUB_EVENT_PAYLOAD_FILE`. When
`github_app_token_env` is set, the service mints a short-lived installation
token for the webhook installation and injects it only into that order run.

## Addressed Messages

The same rules file can define repo-scoped comment addresses. Addressed
messages use the webhook edge for `issue_comment.created`: the webhook creates
or reuses the source bead and kicks the addressed router in the background. The
handling session posts the acknowledgement as the addressed profile before
acting. The `github-addressed-message-router` cooldown order keeps running as
the reconciliation path for missed comments or interrupted dispatch.

```toml
version = 1

[[repo]]
full_name = "owner/repo"
city = "product-city"
rig = "product"
authorized_users = ["alice", "bob"]
installation_id = "123456"

[[repo.address]]
address = "@mayor"
pool = "mayor"
profile = "mayor"
github_app_identity = "mayor"
installation_id = "234567"
formula = "github-addressed-message"
ack = true
```

Only configured addresses are considered. The sender login must appear in the
repo-level `authorized_users` snapshot. Bot comments are ignored. Empty
mentions such as `@mayor` create no work and receive an error reply when the
profile GitHub App can comment. When a comment contains a configured address,
addressed message intake owns that comment; the legacy `/gc` parser is not also
run for the same webhook delivery.

Valid addressed comments create or reuse a source bead keyed by
`github-comment:<repo-id>:<comment-id>:<address>`. The webhook does not post
the accepted-request ack. Instead, it stores the addressed profile's
`github_app_identity`, profile `installation_id`, and `ack` setting on the
source bead. The addressed router then slings the configured formula to
`<rig>/<pool>`; that formula tells the handling session to post the ack exactly
once with `gc github comment-issue --github-app-identity ...` before
doing request side effects. The address-level `installation_id` is the profile
App installation for that repository owner; it may differ from the repo-level
intake App installation used for webhook scans. The addressed router records
`addressed.workflow_root`, and closes the source bead. The repo-level `rig`
setting maps a GitHub path to a local Gas City rig name. If omitted,
`owner/repo` falls back to the derived rig `github-owner-repo`; with
`rig = "product"`, the example dispatch target is `product/mayor`. Duplicate
webhook deliveries and backup scans converge on the same source key.

The backup scan uses `gh` to inspect issues and PRs updated in the last seven
days, including closed items. It mints a GitHub App installation token from the repo
`installation_id` before calling `gh`; repos without an installation id are
skipped by the backup scan until the app is installed/configured. Edited
comments are skipped by the scan so comment edits do not create new concierge
work.

## Runtime Secrets

The service supports manual App import into state, environment-backed
credentials, and identity-resolved credentials. Production deployments should
prefer an identity resolver so restarts and secret rotation converge without
manual import. Set `GITHUB_INTAKE_APP_IDENTITY` to the intake App identity and
`GITHUB_INTAKE_IDENTITY_RESOLVER` to a command that returns the v1 resolver JSON
document. The service syncs that identity on startup, the webhook path retries
the sync before returning a missing-secret error, and the
`github-intake-sync-app` cooldown order refreshes the persisted config every
five minutes.

Manual environment keys are still supported:

```text
GITHUB_APP_ID
GITHUB_INSTALLATION_ID
GITHUB_APP_PRIVATE_KEY_PEM
GITHUB_WEBHOOK_SECRET
GITHUB_APP_SLUG
GITHUB_APP_NAME
GITHUB_APP_HTML_URL
GITHUB_APP_CLIENT_ID
GITHUB_APP_CLIENT_SECRET
```

Addressed profile replies load their own GitHub App bundle from
`repo.address.github_app_identity`. The identity is a deployment-local key,
not a secret-store path; it must match
`[A-Za-z0-9][A-Za-z0-9._:-]{0,127}`. When an address sets that field,
configure `GITHUB_INTAKE_IDENTITY_RESOLVER` to a command that accepts the
identity as its only argument and writes a v1 resolver JSON object to stdout.
The full contract and local-file quick start live in
[`docs/github-app-identity.md`](docs/github-app-identity.md).

The write side is symmetric: set `GITHUB_INTAKE_IDENTITY_PUBLISHER` to a
command that accepts the identity as its only argument and reads the v1
identity JSON from stdin. After the manifest onboarding callback or a manual
`/v0/github/app/import`, the service runs the publisher so new credentials
land in your secret store instead of staying only in the local service
config. With no publisher configured the hook is a no-op, and publish
failures are logged without breaking credential capture. A portable
file-backed pair ships in `examples/`: `file_identity_publisher.py` writes
`$GITHUB_INTAKE_IDENTITY_DIR/<identity>.json` (0600, merge-on-republish) and
`file_identity_resolver.py` reads it back — a full onboarding round-trip with
no secret store required.

To sync the intake App from the configured resolver immediately:

```bash
gc github sync-app
```

## Manual App Import

If the manifest flow is not suitable, you can import an existing app:

```bash
gc github import-app \
  --app-id 123456 \
  --client-id Iv1.example \
  --webhook-secret "$GITHUB_WEBHOOK_SECRET" \
  --private-key-file ./github-app.private-key.pem
```

## Inspect Status

```bash
gc github status
gc github status --json
```

## Workflow Helpers

The pack also exposes helper commands the workflow can call directly:

```bash
gc github comment-issue owner/repo 42 --installation-id 123 --body "hello"
gc github comment-issue owner/repo 42 \
  --installation-id 234 \
  --github-app-identity mayor \
  --body "hello from the addressed profile"
gc github push-branch owner/repo --installation-id 123 --branch fix-42
gc github create-pr owner/repo --installation-id 123 --base main --head fix-42 --title "fix: widget"
```
