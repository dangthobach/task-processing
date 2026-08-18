# Backend API to React mapping

All JSON endpoints return the common envelope defined in `API_CONTRACT.md`.
`frontend/src/api.ts` unwraps `data` and converts `error` into `ApiError`.

| Backend endpoint | React API method | UI consumer | Notes |
|---|---|---|---|
| `GET /healthz` | `health()` | connection health | public |
| `POST /api/v1/job-definitions/{id}/run` | `createRun()` | Submit work | project in body |
| `POST /api/v1/job-runs:bulk` | `bulk()` | Submit work | project in body |
| `GET /api/v1/job-runs`, `/{id}`, `/{id}/attempts` | `runs()`, `run()`, `attempts()` | Overview, runs drawer | `version` returned on runs |
| `POST /api/v1/job-runs/{id}/cancel`, `/retry` | `cancel()`, `retry()` | Runs page | requires `If-Match` |
| `GET /api/v1/job-batches`, detail/items/attempts/logs | `batches()`, `batch*()` | Batches page/drawer | read-only |
| `GET /api/v1/dlq`, `POST /api/v1/dlq/{id}/replay` | `dlq()`, `replayDlq()` | Operations | replay requires `If-Match` |
| `GET /api/v1/workers`, `/scheduler-logs`, `/audit-logs` | matching methods | Operations | audit includes system actor rows |
| `GET /api/v1/events` (SSE) | `events()` + `parseSse()` | Live events | protocol exception to JSON envelope |
| `POST /api/v1/queues`, function/job definitions, schedules | `createQueue()`, `createFunction()`, `createDefinition()`, `createSchedule()` | Create workspace | project in body |
| Queue/schedule state endpoints | `queueAction()`, `scheduleAction()` | no screen yet | requires version; backend list/detail endpoints are missing |

`/metrics` is intentionally not called by the React console: Prometheus uses its
own text exposition format and should be scraped by Prometheus, not rendered as
the API source of truth.
