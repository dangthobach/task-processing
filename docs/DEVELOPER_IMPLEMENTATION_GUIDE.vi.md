# Hướng dẫn bắt đầu triển khai

Tài liệu này là điểm vào cho developer mới. Đọc theo thứ tự: entrypoint →
luồng runtime → vị trí triển khai feature → cách kiểm chứng.

## 1. Bản đồ repository

| Khu vực | Vai trò | Bắt đầu đọc tại |
|---|---|---|
| `cmd/api` | HTTP control-plane/data-plane trên `:8080` | `cmd/api/main.go` |
| `cmd/worker` | Dispatch, execute handler, retry, lease recovery, outbox | `cmd/worker/main.go`, `internal/worker/worker.go` |
| `cmd/scheduler` | HA schedule planner; `gocron` chỉ là wake-up | `cmd/scheduler/main.go`, `internal/scheduler/scheduler.go` |
| `cmd/migrate` | Migration checksummed/ledger repair | `cmd/migrate/main.go` |
| `cmd/bootstrap` | Seed tenant/project/RBAC local | `cmd/bootstrap/main.go` |
| `internal/httpapi` | Router, auth, HTTP envelope, DTO | `internal/httpapi/api.go` |
| `internal/application` | Use case/business validation; HTTP không chứa mutation SQL | `internal/application/controlplane`, `tasks`, `scheduling`, `workflow` |
| `internal/persistence/postgres` | Transactional state machine và SQL PostgreSQL | `store.go`, `queue.go`, `batch.go` |
| `internal/queuebackend` | Contract và adapter Postgres/Redis Streams/JetStream | `backend.go` |
| `internal/domain/job` | Typed handler context, retry/payload/batch contract | `types.go`, `retry.go` |
| `internal/registry` | Đăng ký handler + version/capability | `registry.go` |
| `frontend` | React/Vite operations UI | `frontend/src/app.tsx` |
| `migrations` | Schema theo version tăng dần | `migrations/*.sql` |

## 2. Chạy local theo đúng thứ tự

Mọi binary dùng chung `DATABASE_URL`. Với database local hiện có:

```powershell
$env:DATABASE_URL = 'postgres://postgres:postgres@localhost:5432/task_processing?sslmode=disable'
$env:TASK_DEV_HEADER_IDENTITY = 'true'
go run ./cmd/migrate
go run ./cmd/bootstrap # chỉ chạy một lần trên database trống
```

Mở ba terminal khác nhau:

```powershell
go run ./cmd/api       # http://localhost:8080
go run ./cmd/worker    # handler + outbox + recovery; metrics :9090
go run ./cmd/scheduler # schedule HA/planner
```

UI:

```powershell
cd frontend
npm install
npm run dev            # http://localhost:5173
```

`bootstrap` in ra `tenant_id`/`project_id`; nhập vào Connection settings của
UI cùng actor `admin`, role `admin`. Không chạy `bootstrap` lặp lại vì nó tạo
identity dữ liệu local mới.

## 3. Luồng chạy chính

```text
React / HTTP client
  -> cmd/api/main.go
  -> internal/httpapi (identity, request_id, W3C trace, RBAC)
  -> internal/application (validation/use case)
  -> internal/persistence/postgres (job_runs + outbox_events cùng transaction)
  -> worker dispatchOutbox
  -> QueueBackend (Postgres | Redis Streams | JetStream)
  -> worker reserve/start attempt + handler registry
  -> CompleteSuccess / CompleteFailure
  -> attempts, logs, audit, realtime_events, retry/DLQ
  -> SSE/UI/metrics
```

Điểm quan trọng: PostgreSQL là source of truth. Redis/NATS chỉ mang routing
metadata; không chứa payload hay trạng thái quyết định. Worker chỉ ack broker
sau khi PostgreSQL đã persist transition terminal/retry.

## 4. Tạo một handler mới

1. Đăng ký handler trong `cmd/worker/main.go` (sau này chuyển registration
   này sang package service của bạn):

```go
err := reg.RegisterVersion("payments.capture", "v1", func(ctx *job.ExecutionContext) error {
    // ctx.Execution có RunID, IdempotencyKey, Payload, deadline, trace context.
    // Mọi side effect ngoài hệ thống phải deduplicate bằng IdempotencyKey/RunID.
    return nil
})
```

2. Khởi động/restart worker. `Worker.Run` publish capability vào
   `worker_function_capabilities`; control plane sẽ không tạo Job Definition
   nếu chưa có worker ONLINE tương thích key/version/mode.
3. Tạo Function Definition (`function_key=payments.capture`, `version=v1`),
   Queue, Retry Policy và Job Definition trên UI/API.
4. Submit run tại Job Definition → Run now. Theo dõi Job runs, Attempts, Logs
   và worker log.

Không làm I/O blocking hay unbounded retry trong middleware/handler. Để lỗi
trả về; retry class/policy và persistence do platform quản lý.

## 5. Batch handler

Chỉ dùng BATCH với PostgreSQL backend. Đăng ký bằng
`RegisterBatchVersion`; handler phải trả đúng một result cho mỗi item.

```go
reg.RegisterBatchVersion("payments.batch_capture", "v1",
  func(ctx *job.BatchExecutionContext) ([]job.BatchItemResult, error) {
      results := make([]job.BatchItemResult, 0, len(ctx.Batch.Items))
      for i, item := range ctx.Batch.Items {
          // xử lý item.Run.Payload
          results = append(results, job.BatchItemResult{ItemID: item.ItemID, Success: true})
          if err := ctx.ReportProgress(i+1, "processed"); err != nil { return nil, err }
      }
      return results, nil
  })
```

Batch có `lease_token` fencing. Khi lease bị mất, context handler bị cancel;
không cố ghi progress/result tiếp vì trạng thái stale sẽ bị từ chối.

## 6. Schedule

`cmd/scheduler/main.go` gọi `scheduler.Service.Run`. Service lấy PostgreSQL
advisory lock leader; mỗi 5 giây reconcile cấu hình động. `gocron` chỉ đánh
thức planner. Occurrence được canonicalized bằng `(schedule_id, scheduled_for)`;
planner submit trước rồi mới advance cursor để failover không mất job.

Khi bổ sung schedule semantics, bắt đầu tại:

- `internal/application/scheduling/planner.go`: pure decision/misfire.
- `internal/persistence/postgres/schedule_planner.go`: cursor/state.
- `internal/scheduler/scheduler.go`: reconciliation/wakeup runtime.

## 7. Thêm/sửa control-plane aggregate

Luôn đi theo trình tự này:

1. Tạo migration (table có `row_version`, `created_at`, `updated_at`; entity
   cấu hình có `deleted_at`, `deleted_by` nếu cho phép soft delete).
2. Thêm baseline fingerprint trong `internal/migration/baseline.go`.
3. Thêm static resource spec/validation/dependency guard trong
   `internal/application/controlplane/resources.go` hoặc service chuyên biệt.
4. Mutation phải dùng `row_version` trong `WHERE`, snapshot before/after, và
   ghi `audit_logs` + `audit_outbox_events` trong cùng transaction.
5. HTTP chỉ parse DTO, auth và map error; không đặt SQL mutation ở handler.
6. Cập nhật UI/API mapping và test optimistic-lock, soft-delete, restore
   conflict, dependency race.

Runtime evidence (`job_runs`, attempts, batches, DLQ, outbox, audit) là
append-only: không soft-delete.

## 8. Migration an toàn

Tạo file `NNN_description.sql`, không sửa migration đã có ledger. Chạy:

```powershell
go run ./cmd/migrate
```

Nếu DDL từng chạy thủ công nhưng ledger thiếu, dùng repair có verifier thay vì
insert history thủ công:

```powershell
$env:MIGRATION_REPAIR_LEDGER_THROUGH = '26'
go run ./cmd/migrate
```

## 9. Test trước khi bàn giao

```powershell
go test -race ./...
$env:TEST_DATABASE_URL = $env:DATABASE_URL
go test ./internal/migration ./internal/persistence/postgres -count=1 -v
cd frontend; npm run build
```

Khi sửa Redis/NATS, chạy thêm `TEST_REDIS_URL`/`TEST_NATS_URL` để cover ack,
nack, redelivery và fencing tại `internal/queuebackend/external_delivery_integration_test.go`.

## 10. Khi debug job không chạy

1. Kiểm tra worker `ONLINE` và capability đúng function key/version/mode.
2. Kiểm tra Job Definition/Queue đang ACTIVE, queue không PAUSED/DRAINING.
3. Xem `job_runs.status`: `ENQUEUE_PENDING` là outbox; `QUEUED` chờ claim;
   `RESERVED/RUNNING` kiểm tra lease/worker; `RETRY_WAIT` chờ promotion.
4. Xem Attempts/Logs và `/metrics` (`task_outbox_failures_total`, queue age,
   heartbeat).
5. Không update trực tiếp `job_runs`; dùng API retry/cancel/DLQ replay để giữ
   dispatch generation, audit và outbox đúng.

Tài liệu liên quan: [API contract](API_CONTRACT.md), [queue backends](QUEUE_BACKENDS.md),
[middleware runtime](MIDDLEWARE_RUNTIME.md), [operator guide](USER_OPERATIONS_GUIDE.vi.md).
