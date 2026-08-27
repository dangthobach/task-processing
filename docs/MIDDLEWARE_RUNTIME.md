# Middleware runtime contract

Middleware is executed only after a worker has acquired a durable PostgreSQL
lease and before the compiled handler is invoked. It is not an HTTP middleware
chain and it must never own queue state. Queue transitions, retry timing,
acknowledgement and terminal writes remain in the PostgreSQL state machine.

## Fast-path rules

- A middleware must not perform database, broker, HTTP, filesystem or KMS I/O.
- It must not sleep, wait for a semaphore, spin, or start a goroutine per task.
- It must honor `ctx.Done()` and return an explicit classified error.
- It must not retry handlers. Retrying inside middleware creates duplicate side
  effects and fights the durable retry/DLQ policy.
- It must not mutate a shared handler context. Timeout derives a copied context
  only when it narrows the deadline.

The worker composes the stack once per function key and caches the wrapped
handler. This avoids closure/slice allocation for every attempt. Middleware is
therefore startup-scoped: do not mutate `Worker.Middleware` while `Run` is
active.

## Built-in policies

`Recover` is always outermost and converts a handler panic into `PANIC`.

`DeadlineGuard` rejects a task that has less than its configured useful budget
left with `TIMEOUT/DEADLINE_BUDGET_EXHAUSTED`. It is a guard, not a timeout
replacement: the worker's job-definition timeout and lease cancellation are
still authoritative.

`CircuitBreaker` is per-process and keyed by `function_key@function_version`.
It uses atomics and `sync.Map`, opens after eligible consecutive failures,
allows a bounded half-open probe set, and returns
`DEPENDENCY_UNAVAILABLE/CIRCUIT_OPEN` while open. It deliberately fails open
when its configured key limit is reached; metadata cardinality must not become
an availability outage. It is not distributed: PostgreSQL rate limits and
concurrency limits remain cross-replica controls.

`Bulkhead` is also per-process and keyed by function/version. It uses a CAS
counter and never waits. When full it returns `RATE_LIMITED/BULKHEAD_FULL`; the
durable retry policy decides whether and when to retry. Set it no higher than
the function's global PostgreSQL concurrency if it is enabled.

`Timeout(limit)` is available for programmatic stacks and only narrows a
deadline. In normal deployments prefer the job definition timeout so the
policy is captured in the immutable execution snapshot.

Custom error classifiers run once after handler completion. They must be pure
and non-blocking. A classifier panic is contained and the default
classification is used. Explicit `job.ClassifiedError` remains authoritative.
`Worker.MiddlewareFor` and `Worker.ClassifiersFor` allow a compiled worker to
apply an additional stack or classifier list per `function_key` and version;
they are evaluated only when a wrapped handler is first cached (middleware)
or after a handler returns (classification).

## Configuration

All controls are opt-in. Defaults preserve existing worker behavior.

```text
WORKER_MIN_EXECUTION_BUDGET_MS=25
WORKER_CIRCUIT_BREAKER_FAILURES=5
WORKER_CIRCUIT_BREAKER_OPEN_MS=30000
WORKER_CIRCUIT_BREAKER_HALF_OPEN_MAX=1
WORKER_CIRCUIT_BREAKER_MAX_KEYS=10000
WORKER_BULKHEAD_LIMIT=8
WORKER_BULKHEAD_MAX_KEYS=10000
```

Start conservatively. A breaker configured for a function with intermittent
validation errors must use `TripOn` to exclude them; the default already
excludes validation, authorization, permanent, cancelled and rate-limited
errors. Observe `CIRCUIT_OPEN`, `BULKHEAD_FULL`, lease recovery, retry age and
DLQ depth before increasing concurrency.

## Ordering and failure semantics

The production order is `Recover -> DeadlineGuard -> CircuitBreaker ->
Bulkhead -> handler`. An expired task does not consume breaker/bulkhead state;
an open circuit does not consume bulkhead capacity. A lost lease cancels the
handler with a cause and suppresses terminal writes from that stale execution.

Progress reporting and durable job logs are outside this stack. Worker start,
completion and failure logs use bounded one-second persistence contexts so a
slow log store cannot permanently block task completion. Handlers should rate
limit their own progress calls; a future coalescing reporter can be introduced
without changing the handler interface.

## Capacity guidance

The fast path is O(1): one cached function lookup plus atomic loads/CAS. It has
no contention on a global mutex. `sync.Map` is accessed only by function key;
the key bound prevents unbounded memory. The expensive operations remain
outside middleware: lease state transitions, rate-limit admission, payload
decrypt, durable logs and broker acknowledgements.

Load-test with representative handler latency, not synthetic no-op handlers.
Measure p99 start-attempt latency, handler duration, database pool wait,
breaker-open rate, bulkhead rejection rate, lease-renew success, retry age and
DLQ ingress. Raise worker concurrency only while the PostgreSQL transaction
latency and queue age stay within the target SLO.
