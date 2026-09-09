# GitHub Work Sync Rig Import

This service-free subpack opts one Rig into the native `work-sync` order.
Import `github` once at City scope for ingress and this subpack only on
participating Rigs. Both must use the same accepted repository commit.

See the parent [work-sync setup and contract](../README.md#bidirectional-work-sync)
for runtime requirements, dry-run, exact routing and verification. There is
one shared implementation and no additional scheduler, service or task store.

Each run mints a new installation token for exactly the configured repository
with the canonical runtime contract's `token_permissions`. Before using it,
the existing App helper verifies the returned permissions, an expiry within
one hour, and the effective repository list. An inherited token is never scope
proof and is not reused. Missing or unverifiable scope fails closed, including
in dry-run mode. No token or App key is written into work-sync receipts.
The API must be an HTTPS origin without userinfo, query, or fragment; this is
checked before JWT signing. Token mint, scope readback, and work-sync REST/GraphQL
requests reject all redirects, so credentials never follow a redirected URL.
