# Task lifecycle and terms

## Lifecycle rules

Submission creates a task in `pending` with an initial execution ID. A worker may claim the current execution as `processing`, which increments lifetime `Attempts`, records an attempt, and assigns a time-limited lease token. Only the matching lease holder can finish that execution.

A successful execution becomes `completed` with a result. A permanent processor failure or retryable failure after the retry budget becomes `completed` with an error and a durable dead-letter record. A retryable failure with budget remaining returns the task to `pending`, clears its lease, increments the current cycle's `RetryCount`, assigns a new execution ID, and records its `NotBefore` time. The worker ACKs the input only after the transaction storing that decision and its next outbox row commits.

Defaults allow three automatic retries after the first execution, scheduled after 1 second, 10 seconds, and 60 seconds. A worker timeout is retryable unless explicitly classified permanent. Shutdown, database failure, transaction rollback, active lease, and not-yet-due delivery do not spend retry budget. The current delivery remains recoverable. Lease recovery reuses the execution ID but starts another lifetime attempt; the expired lease token cannot overwrite a newer owner.

Manual replay is allowed only for a known terminal failure that still matches its DLQ execution. It stores an operator/reason receipt, increments `ReplayCycle`, resets the current `RetryCount`, clears the terminal error, assigns a new execution ID, and returns the task to `pending`. It does not erase lifetime attempts or earlier failure history. A repeated replay with matching audit details returns the existing receipt; conflicting details are rejected. DLQ delivery is not another task state.

The system has exactly four server-managed states: `pending`, `processing`, `completed`, and `canceled`. Cancellation is final and clears a lease. Repeating cancellation is idempotent. A completed task cannot be canceled. If cancellation and completion race, the first committed transaction determines the outcome.

## Terms

| Term | Meaning |
| --- | --- |
| Task | Work request, JSON payload, four-state lifecycle, execution identity, and eventual result or error. |
| Event | Immutable versioned fact such as `TaskSubmitted`, retry scheduled, `TaskCompleted`, or `TaskDeadLettered`. |
| Outbox | Durable message created in the same transaction as the task decision and published by the relay after it becomes due. |
| Relay | Reserves due outbox rows, publishes outside the database transaction, waits for broker confirmation, then marks rows published. |
| Execution ID | Generation identity for one scheduled execution; fences messages from prior retry/replay generations. |
| Lease | Time-limited ownership token for one worker start; fences stale workers and supports crash recovery. |
| Attempt | One successful lease grant and processor start. `Attempts` is lifetime total, including lease recovery and replay. |
| Retry count | Automatic retries scheduled in the current replay cycle, separate from lifetime attempts. |
| Replay cycle | Operator-initiated retry generation; each accepted replay increments it without rewriting prior history. |
| `not_before` | Earliest time the current pending execution may start. The relay only publishes due outbox rows. |
| Dead-letter record | Durable reason and diagnostic metadata for a permanent/exhausted known task or quarantined invalid/unknown message. It does not add a task state. |
| Replay receipt | Transactional audit record keyed by stable DLQ message ID, used to make operator replay idempotent. |

## Boundary notes

The domain package owns task transitions and uses only the Go standard library. Application use cases coordinate transitions through repository and processor ports. HTTP DTOs and RabbitMQ envelopes are adapter contracts, not domain entities.

`domain.Snapshot` is a persistence projection containing fields such as version, lease, execution ID, retry count, replay cycle, and due time. `usecase.TaskResponse` is a public application projection; it does not expose lease tokens or event history. PostgreSQL is the source of truth. RabbitMQ transport is at-least-once, so a duplicate broker delivery is expected.

The processor receives the stable task ID as an idempotency key. Execution IDs intentionally change on automatic retry and replay; external side effects must deduplicate by task/business effect or otherwise tolerate repeated execution. Database lease and execution fencing protect task state but cannot reverse an already performed external effect.
