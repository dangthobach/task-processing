# API contract and integration status

## Common HTTP contract

Every JSON success response uses:

```json
{"data": {}, "meta": {"request_id": "uuid", "trace_id": "w3c-trace-id", "timestamp": "RFC3339"}}
```

Every JSON failure uses `application/problem+json`:

```json
{"error": {"code": "...", "title": "...", "status": 409, "detail": "...", "retryable": false}, "meta": {"request_id": "...", "trace_id": "...", "timestamp": "..."}}
```

`/metrics`, `/` and the SSE endpoint are protocol exceptions. All responses include
`X-Request-ID` and `X-Trace-ID`; W3C `traceparent` is accepted and propagated.

State-changing endpoints for an existing entity require `If-Match: "<version>"`.
Read the API `version` or `ETag` first. It is backed by `row_version` in the
database, deliberately separate from semantic handler/worker versions. A
stale/missing precondition returns `412`/`428`.

## React integration coverage

| API group | Endpoints | React client |
|---|---|---|
| Health | `GET /healthz` | yes |
| Job submission and lifecycle | run, bulk, list/detail, attempts, cancel, retry | yes |
| Batch observability | list/detail, items, attempts, structured logs | yes |
| Operations | DLQ/list+replay, workers, scheduler logs, audit logs | yes |
| Realtime | durable SSE `/events` with cursor | yes |
| Control-plane creation | queue, retry policy, function definition, job definition, schedule | yes |
| Control-plane management | list/detail/patch/soft-delete/restore for all five aggregates | yes |
| Control-plane lifecycle | queue pause/resume/drain; schedule pause/resume | yes, from management UI |
| Platform configuration | queue backend CRUD/restore, RBAC user/role/mapping CRUD/restore | API client ready |

For each of queues, retry-policies, function-definitions, job-definitions and schedules:

- GET /api/v1/{aggregate} lists up to 200 active records. Add
  include_deleted=true to view restorable records.
- GET /api/v1/{aggregate}/{id} returns a record and its ETag.
- PATCH /api/v1/{aggregate}/{id}, DELETE /api/v1/{aggregate}/{id} and
  POST /api/v1/{aggregate}/{id}/restore require If-Match.

Patch fields are per-aggregate whitelisted. Immutable ownership and execution
references cannot be edited; dependent records, inactive backends/policies and
restore-name conflicts return a deterministic problem response. Each successful
update/delete/restore writes its audit record and audit outbox event in the same
database transaction.

## Audit and tracing

Externally initiated mutations write a central `audit_logs` record with actor,
tenant, resource, before/after snapshots for state changes, request ID and trace ID. The migration adds a
row_version column and database trigger to all mutable control-plane and execution
aggregates, so any database update advances the entity version, including worker
state transitions.

Creation events use a null `before_data`; state transitions record their previous
status/version and resulting status/version. Future document-update endpoints
must use the same transaction to load the prior document and write its full
before/after snapshot.

## System transitions and retention

Worker job-run and batch status transitions are captured by database triggers
with actor `system:transition-engine`. The same transaction inserts an
`audit_outbox_events` row; the worker pump publishes it to durable
`realtime_events`, where the existing SSE endpoint delivers it as
`audit.system_transition`. Active worker spans are written to `audit_logs.trace_id`
and task attempt records.

`tenants`, `projects`, queue backends, retry policies, queues, function/job
definitions and schedules support soft deletion through `deleted_at` and
`deleted_by`. Runtime executions, attempts, batches, DLQ, audit and outbox
records are append-only evidence and must never be soft-deleted.

## Platform configuration and RBAC

`queue_backends` is a platform aggregate. Its encrypted configuration is write-only:
the API exposes neither ciphertext nor plaintext. `GET/PATCH/DELETE /api/v1/queue-backends/{id}`
and `POST /api/v1/queue-backends/{id}/restore` require platform administration;
all mutations except creation require `If-Match`.

RBAC follows the same contract for `/api/v1/rbac/users/{id}` and
`/api/v1/rbac/roles/{id}`. Replacing user roles or role permissions also requires
the parent version in `If-Match`; the server validates every referenced ID before
replacing the mapping and advances that parent version transactionally. Use
`include_deleted=true` to locate records eligible for restore.

These mutations write to `platform_audit_logs` and one matching
`platform_audit_outbox_events` record in the exact database transaction as the
aggregate change. Platform administrators can inspect tenant-scoped records through
`GET /api/v1/platform-audit-logs`; it includes request and trace IDs.

The worker sends both project and platform audit outboxes to `TASK_AUDIT_SINK_URL`
using at-least-once HTTP delivery. Platform events have `scope: "PLATFORM"` and
`tenant_id` (not `project_id`); sinks must deduplicate with `X-Audit-Event-ID`.
Claims are fenced, failures use bounded exponential backoff, and a poison event is
marked expired after 20 attempts while its immutable audit log remains queryable.
