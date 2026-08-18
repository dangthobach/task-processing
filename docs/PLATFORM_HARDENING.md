# Platform hardening

## Identity and RBAC

HTTP handlers now consume an identity Provider boundary. The bundled
HeaderProvider is development-only and is enabled only with
TASK_DEV_HEADER_IDENTITY=true. Production must inject an OIDC/JWT Provider
which maps verified claims to subject and tenant; permissions are resolved from
the database on every request, not trusted from a role claim.

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
needs to implement the small kms.Protector interface.

## Operations data

Migration 010 adds structured job logs, filtered by run and level through
GET /api/v1/job-runs/{id}/logs. It also adds Prometheus queue-age, heartbeat
and outbox-failure metrics, plus starter alert rules in deploy/prometheus.
Retention policies and a partitioned job-log table are introduced in the same
migration; scheduling the purge executor is the next operational deployment
step.

## Workflows

Migration 010 introduces workflow definition, node, edge and runtime tables.
The workflow package validates topology deterministically and rejects cycles,
duplicate nodes and dangling edges. Dispatching ready DAG nodes is intentionally
kept out of the worker claim loop until workflow CRUD and run orchestration are
exposed as a dedicated API.
