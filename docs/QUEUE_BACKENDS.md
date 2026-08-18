# Queue backend contract

`internal/queuebackend.Backend` implements the BRD semantic contract:
enqueue (single/batch), reserve, acknowledge, negatively acknowledge, depth,
health and explicit capabilities. `Message` always carries the stable
control-plane run/project/queue IDs; handlers remain at-least-once and must use
the idempotency key supplied by the execution context.

The available adapter is `POSTGRES`. It supports priority, delay, DLQ, pause and
atomic enqueue batches using the existing durable `job_runs` state machine. The
worker outbox pump resolves each queue's `backend_type` (`POSTGRES` when no
backend is assigned), calls `Backend.Enqueue`, and marks the outbox row only
after success. Unknown/unhealthy backends retain the durable row and receive
bounded retry backoff. Control-plane data and execution history remain in
PostgreSQL for every backend.

Redis Streams and NATS JetStream adapters must register the same `Backend`
contract, declare only their real capabilities, and never bypass the control
plane's transactional outbox/audit path. Their delivery consumers are not yet
enabled: the worker reserve loop remains PostgreSQL-only until those adapters
are supplied with consumer-group/ack implementations.
