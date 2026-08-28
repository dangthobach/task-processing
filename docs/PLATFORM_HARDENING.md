# Platform hardening

## Identity and RBAC

HTTP handlers now consume an identity Provider boundary. The bundled
HeaderProvider is development-only and is enabled only with
TASK_DEV_HEADER_IDENTITY=true. The included OIDC/JWT provider is enabled with
`TASK_OIDC_ISSUER` and `TASK_OIDC_AUDIENCE`; it verifies issuer, signature and
audience, then maps `tenant_id` (or `TASK_OIDC_TENANT_CLAIM`) and `sub` to the
platform subject. Permissions are resolved from the database on every request,
not trusted from a role claim.

Migration 011 adds tenant-scoped users and roles, global permission catalogue,
user_roles and role_permissions. Platform administrators can configure them
through /api/v1/rbac/users, /api/v1/rbac/roles and /api/v1/rbac/permissions,
including replacement endpoints for user-role and role-permission mappings.
Route policy requires effective permissions such as control:write or job:read;
platform:admin remains the explicit break-glass permission.

## Payload protection

Set TASK_PAYLOAD_MASTER_KEY to a base64 encoded 32-byte key and optionally
TASK_PAYLOAD_KEY_REF. New job payloads are encrypted with AES-256-GCM; the
project and job-definition identifiers are authenticated additional data.
Workers decrypt only after a run is leased. Without a configured provider,
payloads remain plaintext for local compatibility. A cloud KMS adapter only
needs to implement the small kms.Protector interface. Queue-backend connection
configuration uses the same provider, is authenticated to its backend ID, and
stores its key reference so the worker can refresh adapters by row version.

## Operations data

Migration 010 adds structured job logs, filtered by run and level through
GET /api/v1/job-runs/{id}/logs. It also adds Prometheus queue-age, heartbeat
and outbox-failure metrics, plus starter alert rules in deploy/prometheus.
Retention policies run in bounded worker chunks, configured through
`WORKER_RETENTION_INTERVAL_SECONDS`, so cleanup cannot hold long transactions
on the runtime hot path.

## Workflows

Migration 010 introduces workflow definition, node, edge and runtime tables.
The workflow package validates topology deterministically and rejects cycles,
duplicate nodes and dangling edges. Terminal state changes now write a durable
workflow-dispatch outbox in the same transaction; reconciliation repairs a
crash window and drains it, so downstream nodes do not become stuck.

Workflow runs are versioned operational resources. `POST
/api/v1/workflow-runs/{id}/cancel` requires `If-Match` and runs under a
workflow-row fence. It cancels only at a node boundary: queued/pending work
and its dispatch signal are invalidated atomically, while a currently running
handler returns `409 WORKFLOW_RUN_ACTIVE` instead of being abandoned.

`POST /api/v1/workflow-runs/{id}/retry` accepts a failed, cancelled, or
manually-paused run and also requires `If-Match`. It creates one idempotent
child run linked by `retry_of_run_id`, copying the source's immutable node and
graph snapshot rather than rewriting its history or reading a later definition.

Workflow definitions now choose a snapshot failure policy: `FAIL_FAST`
cancels undispatched branches, `CONTINUE` evaluates failure edges, and
`MANUAL_INTERVENTION` blocks pending nodes in `AWAITING_INTERVENTION`. Edges
support `ON_SUCCESS`, `ON_FAILURE`, and `ALWAYS`. Every incoming edge must
match (the established DAG join rule); a terminal node with a non-matching
edge is recorded as `SKIPPED`. Expression predicates and compensation remain
disabled until handlers have a durable output/compensation contract.
