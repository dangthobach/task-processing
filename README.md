# Task Processing Platform

Durable task-processing foundation based on the supplied BRD. `gocron/v2` is used only by the scheduler; PostgreSQL owns job state, retry timing, leases, outbox publication, DLQ and history.

## Run locally

```powershell
docker compose up -d postgres
$env:DATABASE_URL = 'postgres://task:task@localhost:5432/task_processing?sslmode=disable'
go run ./cmd/migrate
go run ./cmd/bootstrap
go run ./cmd/api
# In separate terminals:
go run ./cmd/worker
go run ./cmd/scheduler
```

## React control-plane UI

The React/Vite console lives in [`frontend/`](frontend/). It is a responsive operations UI for runs, batches, DLQ, worker fleet, scheduler logs, audit history, realtime events, and creation flows for jobs, queues, definitions and schedules.

```powershell
cd frontend
npm install
npm run dev
```

Open `http://localhost:5173`. The Vite proxy sends `/api` and `/metrics` to the Go API at `http://localhost:8080`. Use the Connection panel to supply `tenant_id`, `project_id`, actor and role. Production build: `npm run build`.

The common HTTP envelope, distributed tracing, audit and optimistic-locking rules are in [docs/API_CONTRACT.md](docs/API_CONTRACT.md).
The exact backend-to-React endpoint mapping is in [docs/API_FE_MAPPING.md](docs/API_FE_MAPPING.md).
The portable queue semantics and PostgreSQL adapter are in [docs/QUEUE_BACKENDS.md](docs/QUEUE_BACKENDS.md).

The API listens on `:8080`; open `http://localhost:8080/` for the small operational dashboard. Development identity headers are required for protected routes:

```text
X-Actor-ID: alice
X-Tenant-ID: <tenant UUID>
X-Role: admin | operator | developer | viewer
```

`bootstrap` prints local tenant/project/queue IDs. Register `example.echo` through `POST /api/v1/function-definitions`, then create a job definition through `POST /api/v1/job-definitions`; the worker binary exposes that compiled handler. Production authentication is deliberately an interface boundary: replace the development header authentication in `internal/httpapi/api.go` with OIDC validation before deployment.

## Guarantees and boundaries

- Submission creates `job_runs` and `outbox_events` in one database transaction.
- Dispatcher publishes the outbox by transitioning runs to `QUEUED`; workers claim with short `FOR UPDATE SKIP LOCKED` transactions and leases.
- Delivery is at-least-once. Handlers must deduplicate external side effects using `Execution.RunID` / `IdempotencyKey`.
- Retry, DLQ, priority, pause/drain and lease recovery belong to the worker/queue layer, never to gocron's wait queue.
- A unique `(schedule_id, scheduled_for)` constraint protects scheduled occurrences across scheduler failover.
- Each handler receives a typed `job.ExecutionContext` with tenant/project/queue/definition/run IDs and its cancellation deadline. Middleware is composed around this handler contract.
- Tenant, project, queue and function concurrency limits are enforced at attempt-start with PostgreSQL transaction advisory locks, so quotas apply across worker replicas. Defaults are 20/20/10/5 respectively; tune them in the corresponding database records.

## Included API

`POST /api/v1/job-definitions/{id}/run`, `POST /api/v1/job-runs:bulk`, run history/detail/cancel/retry, attempts, queues create/pause/resume/drain, schedules create/pause/resume, DLQ list/replay, worker health, audit logs and durable SSE at `GET /api/v1/events?project_id=<uuid>&after_id=<event-id>`. Prometheus metrics are at unauthenticated `GET /metrics`.

Handlers can report progress during work with `ctx.ReportProgress(percent, message)`. Progress and state changes are persisted to `realtime_events`, so SSE clients can reconnect from their last event ID.

## Batch handlers

Set a job definition's `execution_mode` to `BATCH` and configure `batch_size` (2-1000). Compatible queued runs are atomically claimed into a `job_batch`; each run remains an independent item, with its own attempt, retry and DLQ decision. Register the compiled batch function with `registry.RegisterBatch`:

```go
func(ctx *job.BatchExecutionContext) ([]job.BatchItemResult, error)
```

The result must contain exactly one entry per `ctx.Batch.Items`. Successful items are completed; only failed items enter retry/DLQ. Use `ctx.ReportProgress(processed, message)` and `ctx.Log(level, message, fields)` for durable batch monitoring. Query `GET /api/v1/job-batches?project_id=...`, then `/items` and `/logs` for transparent item-level history.

Set `OTEL_EXPORTER_OTLP_ENDPOINT` to enable OTLP/HTTP tracing for API, worker and scheduler; otherwise tracing is a no-op for local development.
