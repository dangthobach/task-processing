# Hướng dẫn vận hành Task Processing Platform

Tài liệu này dành cho môi trường local/development và mô tả chính xác các màn hình React hiện có. API và UI dùng common envelope, optimistic locking, audit log và event stream bền vững.

## 1. Kết nối workspace local

Mở `http://localhost:5173`, chọn biểu tượng **Connection settings**, sau đó nhập:

| Trường | Giá trị local hiện tại | Ý nghĩa |
|---|---|---|
| API base URL | Để trống | Vite proxy chuyển `/api` sang `http://localhost:8080`. Chỉ điền `http://localhost:8080` khi không dùng proxy. |
| Actor ID | `admin` | Subject đã được bootstrap trong database. |
| Tenant UUID | `be8281b5-ecb8-4f10-b828-ea4a2c8c7280` | Ranh giới tenant. Không dùng tenant của môi trường khác. |
| Project UUID | `98bae494-7725-438f-9491-c8d530e038a0` | Workspace/project để xem và tạo dữ liệu. |
| Role | `admin` | Quyền local đầy đủ. |

Nhấn **Save connection**. Chấm xanh ở sidebar nghĩa là health endpoint trả về thành công. Development identity gửi `X-Actor-ID`, `X-Tenant-ID` và `X-Role`; không dùng cơ chế này ở production, nơi OIDC và dynamic RBAC phải được bật.

## 2. Mô hình vận hành

```text
Function → Queue + Retry Policy → Job Definition → Job Run
                                      ↓
                                Schedule / Workflow
                                      ↓
Outbox → Queue backend → Worker → Attempt / Log / Event → UI
                                      ↓
                             Retry hoặc DLQ nếu thất bại
```

- **Function Definition** là hợp đồng một handler đã có trong binary worker, ví dụ `example.echo`.
- **Queue** quyết định nhận việc và giới hạn concurrency.
- **Retry Policy** quyết định số lần thử và thời gian thử lại.
- **Job Definition** ghép Function, Queue và Retry Policy thành loại công việc có thể submit.
- **Job Run** là một lần thực thi logic; **Attempt** là mỗi lần worker chạy run đó.
- **Outbox** đảm bảo submit và ý định dispatch được commit cùng transaction; worker là bên publish/claim.

## 3. Quy trình nhanh để trải nghiệm trên UI

Đã có sẵn dữ liệu demo:

| Thành phần | Giá trị |
|---|---|
| Queue | `99ae1bfa-ca14-42e7-86bb-aaf427ec26d8` |
| Function key | `example.echo` |
| Job Definition | `26ba2b11-b928-4e88-95c3-b1b3d10cb877` |
| Demo run | `9fd655a1-d29f-4121-96f1-604e75ad05ea` (`SUCCEEDED`) |

1. Vào **Create**.
2. Trong **Submit work**, để `Single run`.
3. Dán Job Definition UUID ở trên.
4. Dán payload:

   ```json
   {"message":"Hello from UI"}
   ```

5. Nên nhập idempotency key duy nhất, ví dụ `ui-demo-001`.
6. Nhấn **Submit job**.
7. Vào **Job runs**, nhấn Refresh. Bấm một dòng để xem drawer chứa attempts và progress.
8. Vào **Live events** để thấy `job.started`, `job.succeeded` và audit/system events.

Không dùng lại một idempotency key cho payload khác: server sẽ trả run đầu tiên thay vì tạo run mới.

## 4. Tạo cấu hình mới

Thao tác theo đúng thứ tự sau trong màn **Create**.

### 4.1 Queue

Tại **Create queue**, nhập tên duy nhất và concurrency. Bắt đầu với concurrency `5` hoặc `10`.

- Queue mới ở trạng thái `ACTIVE`.
- Chỉ gán `BATCH` Job Definition cho PostgreSQL queue backend; external Redis/NATS chưa hỗ trợ batch transport.

### 4.2 Retry Policy

Tại **Create retry policy**, nhập tên và chọn strategy. Sau khi tạo, lấy UUID từ **Control plane → Retry policies** để dùng khi tạo definition qua API hoặc UI mở rộng.

### 4.3 Function và Job Definition

Tại **Register execution**:

1. Nhập Function key đã đăng ký trong worker. Local binary hiện có `example.echo` và `example.batch_echo`.
2. Đặt version, ví dụ `v1`, rồi nhấn **Register function**.
3. UI điền Function UUID sau khi thành công.
4. Điền Queue UUID, Definition name, chọn execution mode và nhấn **Create definition**.

Một Function Definition không tự tạo code. Nếu Function key không được đăng ký trong worker đang chạy, run sẽ không thực thi được; cần thêm handler vào registry, build/restart worker rồi mới tạo definition.

### 4.4 Schedule

Tại **Create schedule**, chọn Job Definition UUID và schedule type. Cron được scheduler evaluate theo timezone; gocron chỉ là wake-up signal, PostgreSQL cursor là source of truth. Xem Scheduler log trong **Operations** nếu schedule không tạo run như mong đợi.

## 5. Ý nghĩa enum và dropdown

### Role

| Enum | Dùng khi |
|---|---|
| `admin` | Quản trị đầy đủ: control plane, vận hành, RBAC ở local. |
| `developer` | Tạo Function/Job Definition, submit job; không nên dùng cho vận hành nhạy cảm. |
| `operator` | Theo dõi, cancel/retry/replay theo permission được cấp. |
| `viewer` | Chỉ xem. |

Quyền thực tế production lấy từ RBAC permissions trong database; role header chỉ dùng cho development UI.

### Execution mode

| Enum | Ý nghĩa | Khi chọn |
|---|---|---|
| `SINGLE` | Một run gọi một handler. | Dùng mặc định; phù hợp `example.echo`. |
| `BATCH` | Nhiều run tương thích được gom vào một batch handler. | Chỉ dùng với `example.batch_echo` hoặc batch handler đã đăng ký; cần PostgreSQL backend. |

### Retry strategy

| Enum | Ý nghĩa |
|---|---|
| `FIXED` | Mỗi retry dùng cùng delay. Dùng cho lỗi tạm thời có thời gian hồi phục ổn định. |
| `EXPONENTIAL` | Delay tăng theo multiplier, có max delay và jitter. Dùng mặc định để tránh retry storm. |

### Rate-limit scope

| Enum | `target_id` | Phạm vi |
|---|---|---|
| `PROJECT` | Để trống | Tất cả run trong project. |
| `QUEUE` | Queue UUID | Chỉ run đi vào queue đó. |
| `FUNCTION` | Function UUID | Tất cả Job Definition dùng function đó. |

`capacity` là số token tối đa, `refill_tokens` là token nạp thêm sau mỗi `refill_period_ms`. Ví dụ `100 / 100 / 1000` cho tối đa xấp xỉ 100 start/giây.

API hiện hỗ trợ `enforcement_point` là `WORKER_START` hoặc `SUBMISSION`. Form UI hiện mặc định `WORKER_START`; để dùng `SUBMISSION`, tạo/patch policy qua API cho tới khi UI được bổ sung dropdown này.

### Schedule type và status

| Enum | Ý nghĩa |
|---|---|
| `CRON` | Tạo occurrence lặp theo cron expression/timezone. |
| `ONE_TIME` | Chạy một lần tại `run_at` RFC3339. |
| `ACTIVE` | Có thể evaluate/claim. |
| `PAUSED` | Giữ cấu hình, không tạo occurrence mới. |
| `DISABLED` | Không nhận việc/cần bật lại trước khi dùng. |

## 6. Trạng thái run, batch và queue

### Job Run

| Status | Ý nghĩa | Hành động vận hành |
|---|---|---|
| `ENQUEUE_PENDING` | DB đã ghi run/outbox, chưa publish transport. | Kiểm tra worker/outbox nếu kéo dài. |
| `QUEUED` | Có thể được worker claim. | Bình thường. |
| `RESERVED` | Worker đã giữ quyền lease, chưa bắt đầu handler. | Tự recovery khi lease hết hạn. |
| `RUNNING` | Handler đang chạy. | Xem attempt/log/progress; chỉ cancel nếu cần. |
| `RETRY_WAIT` | Attempt lỗi nhưng còn retry; chờ `available_at`. | Kiểm tra retry policy nếu kéo dài. |
| `SUCCEEDED` | Hoàn thành terminal. | Chỉ xem audit/log. |
| `DEAD_LETTER` | Hết retry hoặc lỗi không retry. | Xem **Operations → Dead letter queue**, sửa nguyên nhân rồi Replay. |
| `CANCELLED` | Bị huỷ trước terminal. | Có thể Retry nếu policy vận hành cho phép. |

### Queue

| Status | Hành vi |
|---|---|
| `ACTIVE` | Nhận và xử lý job mới. |
| `PAUSED` | Giữ queued job, worker không bắt đầu thêm job. |
| `DRAINING` | Không nhận submit mới; chờ công việc đang có hoàn tất. |
| `DISABLED` | Không nhận work. |

### Batch

- `RESERVED`/`RUNNING`: batch handler đang sở hữu nhóm item bằng lease token
  (fenced ownership). Khi lease hết hạn, recovery requeue item và handler cũ
  không thể ghi progress/kết quả muộn.
- Thành công/thất bại hiển thị ở từng item: một item fail có thể retry/DLQ độc lập, không che giấu item thành công.
- Bấm batch trong **Batches** để xem progress, items, attempts và structured logs.

## 7. Control Plane và optimistic locking

Màn **Control plane** quản lý Queue, Retry Policy, Rate Limit, Function, Job Definition và Schedule.

1. Chọn aggregate ở tab ngang.
2. Bấm record để xem ID và `version`.
3. Dán JSON patch chỉ chứa field được phép, ví dụ:

   ```json
   {"max_concurrency": 5}
   ```

4. Nhấn **Save patch**. UI tự gửi version bằng `If-Match`.
5. Nếu nhận `PRECONDITION_FAILED`, refresh record, kiểm tra thay đổi của người khác và thử lại.

Soft delete không xóa evidence runtime. Bật **Deleted** để xem record đã xoá và nhấn **Restore**. Restore có thể bị từ chối nếu name/key đã được record active khác sử dụng hoặc dependency không còn hợp lệ.

## 8. Vận hành và chẩn đoán

- **Overview**: số run active, batch, DLQ và worker online.
- **Job runs**: retry/cancel, click row xem attempt và lỗi.
- **Operations → DLQ**: chỉ replay sau khi đã sửa payload/config/handler gây lỗi.
- **Operations → Worker fleet**: worker phải `ONLINE` và heartbeat mới.
- **Operations → Scheduler log**: xem cron validation/evaluation và lỗi schedule.
- **Operations → Audit trail**: ai thay đổi control-plane và các system transition.
- **Live events**: source-of-truth realtime có thể reconnect; dùng để theo dõi trạng thái lúc submit.

Nếu run kẹt `ENQUEUE_PENDING`, kiểm tra worker đang chạy và `task_outbox_failures_total` ở `http://localhost:9090/metrics`. Nếu run `RESERVED`/`RUNNING` vượt lease, worker recovery sẽ xử lý; kiểm tra `task_maintenance_failures_total` và worker log trước khi can thiệp database.

## 9. Phạm vi UI hiện tại và công việc tiếp theo theo BRD

### Đã có để vận hành thử

- Job lifecycle, retry, DLQ, batch monitor, logs, audit, durable SSE.
- Queue/function/job/schedule/retry/rate-limit control-plane CRUD với soft-delete/restore cho các aggregate đã expose.
- Workflow API/runtime snapshot và downstream dispatch; Redis Streams/NATS JetStream adapter; KMS local provider.

### Ưu tiên triển khai tiếp

1. **Hoàn thiện UI control-plane**: picker thay UUID thủ công, dropdown cho Retry Policy/Queue/Function, form JSON Schema, schedule cron preview, rate-limit `enforcement_point`, Queue Backend management và workflow DAG designer.
2. **Tách application service cho CRUD legacy**: di chuyển transaction SQL còn trong HTTP handlers vào aggregate services và tăng PostgreSQL concurrency/integration coverage.
3. **External audit sink & KMS production**: Kafka/NATS/SIEM sink có claim fence/backoff, cloud KMS/Vault provider, key rotation/re-encryption.
4. **Batch transport protocol**: chỉ sau khi có contract Redis/NATS cho batch mới cho phép BATCH trên external backend.
5. **Retention/partition operations**: tạo partition trước hạn, archive/purge monitoring và retention management UI.
6. **Workflow operations UI**: graph versioning, run detail, node retry/cancel, compensation/failure policy nếu BRD yêu cầu.
7. **Observability production**: dashboards, alert routing, SLO cho queue latency/outbox/worker heartbeat, OIDC/JWKS integration tests.

## 10. An toàn local

- Không dùng `task/task` hay Header Identity cho môi trường production.
- Không sửa trực tiếp `job_runs`/`job_attempts` để vận hành thường ngày; dùng API cancel/retry/DLQ replay để giữ audit/outbox/fencing đúng.
- Không đăng ký Function key chưa có handler trong worker.
- Dùng idempotency key cho mọi submit có side effect bên ngoài.
# Bổ sung Control Plane và vận hành production

## Picker, schema, cron và workflow

- Tại **Create**, các quan hệ Function, Queue, Retry Policy và Job Definition dùng picker. Chọn theo tên rồi kiểm tra ID rút gọn; không cần tự chép UUID.
- **Input JSON Schema** là schema JSON Schema của payload. Nhấn Register function chỉ khi JSON hợp lệ; API vẫn validate schema khi submit để không thể bypass UI.
- **Schedule job** có `Preview next 5`. Kết quả được backend tính bằng cùng parser với Schedule Planner. Bật `Six-field cron` nếu biểu thức có giây.
- **Workflow DAG designer**: thêm step, gán Job Definition cho từng step, sau đó `Connect steps`. Step key phải duy nhất. Backend kiểm tra node thiếu, self-edge và cycle trước khi ghi transaction.
- **Workflow operations**: chọn Workflow để xem từng run và trạng thái từng node. `Cancel safely` chỉ hủy tại ranh giới giữa các node: các node/job chưa chạy và dispatch signal bị hủy cùng transaction. Nếu node đang `RUNNING`, UI trả `WORKFLOW_RUN_ACTIVE`; chờ handler kết thúc rồi refresh/thử lại, không có thao tác nào cắt ngang handler đang chạy.
- Với run `FAILED`, `CANCELLED` hoặc `AWAITING_INTERVENTION`, dùng **Retry workflow**. Hệ thống tạo một run con mới (không sửa lịch sử run cũ), giữ nguyên node/graph snapshot của run nguồn. Nhấn lại sau timeout sẽ trả cùng run con thay vì tạo execution trùng lặp.
- Khi tạo workflow, chọn **Failure policy**: `Fail fast` dừng các nhánh chưa dispatch; `Continue` cho phép các cạnh `On failure`; `Manual intervention` chuyển run sang `AWAITING_INTERVENTION` và chặn các node đang chờ. Mỗi cạnh có `On success`, `On failure`, hoặc `Always`; mọi cạnh đi vào một step phải thỏa, cạnh không thỏa sẽ làm step `SKIPPED`. Condition expression và compensation chưa được mở để không suy diễn kết quả handler không tồn tại trong runtime contract.

## Retention, audit và SLO

- Form **Retention & partitions** tạo policy theo resource. Sửa, disable, soft-delete hoặc restore ở Control Plane → Retention. Worker xóa theo chunk nên không giữ transaction lớn.
- Alert rules ở `deploy/prometheus/task-processing-alerts.yml`; import `deploy/grafana/task-processing-dashboard.json` vào Grafana. SLO vận hành khuyến nghị: queue oldest age < 300s, heartbeat age < 30s và audit sink failures = 0 trong 15 phút.
- Set `TASK_AUDIT_SINK_URL` (và tùy chọn `TASK_AUDIT_SINK_TOKEN`) trên worker để forward audit event. Sink nhận `X-Audit-Event-ID` và phải deduplicate theo ID; delivery là at-least-once với lease/backoff, tối đa 20 lần rồi expiry rõ ràng trong outbox. Audit cấp project có `scope: "PROJECT"` và `project_id`; audit RBAC/Queue Backend có `scope: "PLATFORM"` và `tenant_id`.

## Vault Transit và key rotation

Sử dụng `TASK_KMS_PROVIDER=vault`, `TASK_VAULT_ADDR`, `TASK_VAULT_TOKEN`, `TASK_VAULT_TRANSIT_KEY`; tùy chọn `TASK_VAULT_TRANSIT_MOUNT` (mặc định `transit`) và `TASK_VAULT_NAMESPACE`. Provider có deadline 10 giây; lỗi KMS trả về lỗi nghiệp vụ, không làm treo worker loop.

Sau khi Vault rotate key, chạy chiến dịch bounded:

```powershell
$env:DATABASE_URL='postgres://task:task@localhost:5432/task_processing?sslmode=disable'
$env:KMS_ROTATE_RESET='true' # chỉ một lần, bắt đầu campaign mới
$env:KMS_ROTATE_LIMIT='100'
go run ./cmd/kms-rotate
```

Sau đó bỏ `KMS_ROTATE_RESET` và lặp lệnh cho tới khi output là `rewrapped 0`. Cả payload job và cấu hình encrypted backend đều dùng conditional write, nên cập nhật cạnh tranh không bị ghi đè.
