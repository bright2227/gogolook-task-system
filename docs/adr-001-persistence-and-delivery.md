# ADR 001: PostgreSQL task decisions with an outbox and RabbitMQ

**Status:** Accepted and implemented

## Context

Task status, retry schedules, attempt history, dead-letter metadata, and replay receipts must agree after crashes. Publishing directly to RabbitMQ from inside a database transaction cannot make the database and broker one atomic resource. The broker also delivers at least once, so workers and relay must tolerate duplicates.

## Decision

PostgreSQL is the source of truth for task state and delivery decisions. A submission transaction stores the pending task, `TaskSubmitted` event, and work outbox row together. Retry and terminal-failure decisions likewise update task state, append events, finalize attempt/failure records, and enqueue the next work or DLQ message in one transaction.

The relay reserves only due outbox rows, publishes persistent messages with mandatory routing and publisher confirms outside the database transaction, then marks a confirmed row published. A crash after publish confirmation but before that mark can produce a duplicate; stable event IDs, execution-generation checks, terminal-state checks, and lease tokens make task-state application idempotent. External side effects still need a stable business idempotency key, supplied as the task ID to `Processor.Execute`.

Each scheduled execution has an execution ID. Retry changes execution ID, returns the task to `pending`, clears its lease, increments the current automatic `retry_count`, and sets `not_before`. The relay publishes the retry directly to the work queue only when its outbox row becomes due. No RabbitMQ TTL is applied on top of PostgreSQL scheduling. The default policy allows three automatic retries at 1, 10, and 60 seconds. Attempt count is lifetime total and includes lease-recovery starts; `replay_cycle` tracks operator replay separately.

Permanent processor errors and retry-budget exhaustion become `completed` with an error. A transaction records the final attempt, dead-letter metadata, `TaskDeadLettered` event, and DLQ outbox message. The broker envelope carries a stable message ID and structured failure details. Malformed envelopes and unknown task IDs lack a task transaction; the worker quarantines them to the persistent DLQ with mandatory routing and publisher confirmation before acknowledging the source. If quarantine publication fails, the source delivery remains unacknowledged.

Storage, transaction, active-lease, not-due, and shutdown interruptions are not processor failures. They do not spend retry budget. The source delivery remains recoverable; lease expiry can permit another start under the same execution ID, with a new lease token fencing a late worker. A stale execution generation is acknowledged as a safe no-op after PostgreSQL confirms it is stale.

Manual DLQ operation runs through `/taskctl` inside an existing worker pod; there is no admin HTTP API. Inspection and bounded message-ID scans use a dedicated AMQP channel and leave scanned messages unacknowledged, so closing the channel requeues them. RabbitMQ may change ordering or redelivery flags during this process. Replay validates the durable DLQ record and current terminal task generation, then transactionally creates an operator/reason receipt, starts a new pending execution, and enqueues its outbox message. The CLI acknowledges the selected DLQ message only after that transaction commits. The receipt is checked before current-state validation so a retry after a crash safely recognizes the committed replay; conflicting audit input is rejected. Unknown and malformed messages are inspect-only and require an explicit repair process outside this CLI.

The four lifecycle states remain `pending`, `processing`, `completed`, and `canceled`. Retry scheduling returns to `pending`; DLQ is not a fifth task status. Cancellation is final, clears a lease, and races with completion by transaction commit order.

The polling relay is the only implemented outbox publisher. CDC is an optional future replacement that must preserve this outbox identity, due-time, and durable-confirm-before-checkpoint contract. WAL capture alone does not honor `not_before`, so a CDC implementation needs a due-time scheduler or a configured delay tier. Polling and CDC must not publish the same rows concurrently.

## Consequences

- PostgreSQL transactions make retry/DLQ decisions and their next outbox messages atomic.
- Broker publication remains at-least-once; duplicates are expected and handled through IDs, state checks, and replay receipts.
- Operational counts and failure explanations come from durable database records; logs are supplementary and may be sent to any stdout log backend.
- A bounded DLQ scan preserves messages but can change order/redelivery flags, and a bounded scan cannot prove absence beyond its scanned prefix.
- The integration tests use real PostgreSQL and RabbitMQ containers. They are implemented but have not been compiled or run in the current environment because dependencies are unavailable offline and Docker access is blocked.
