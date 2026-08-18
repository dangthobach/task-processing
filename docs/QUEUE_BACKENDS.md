# Queue backend contract

`internal/queuebackend.Backend` implements the BRD semantic contract:
enqueue (single/batch), reserve, acknowledge, negatively acknowledge, depth,
health and explicit capabilities. `Message` always carries the stable
control-plane run/project/queue IDs; handlers remain at-least-once and must use
the idempotency key supplied by the execution context.

Available adapters are `POSTGRES`, `REDIS_STREAMS`, and `JETSTREAM`.
PostgreSQL supports priority, delay, DLQ, pause and atomic enqueue batches
using the durable `job_runs` state machine. Redis Streams uses an `XREADGROUP`
consumer group and JetStream uses a durable pull consumer with explicit ack.
Both external adapters carry only routing IDs, take PostgreSQL dispatch
ownership before starting a handler, and acknowledge their transport only after
PostgreSQL persists success/retry/DLQ. Control-plane data and execution history
remain in PostgreSQL for every backend.

Configure external consumers on the worker with either or both groups below.
The worker always starts the PostgreSQL adapter; an unavailable configured
external service fails worker startup instead of silently leaving messages
undelivered.

```text
TASK_REDIS_STREAMS_URL=redis://localhost:6379/0
TASK_REDIS_STREAMS_STREAM=task.dispatch        # optional
TASK_REDIS_STREAMS_GROUP=task-workers          # optional

TASK_NATS_URL=nats://localhost:4222
TASK_NATS_STREAM=TASK_DISPATCH                 # optional
TASK_NATS_SUBJECT=task.dispatch                # optional
TASK_NATS_CONSUMER=task-workers                # optional
```

`BATCH` execution remains PostgreSQL-only. Configure a batch definition on a
queue without an external backend until a batch transport protocol is added.
