# Task lifecycle and terms

## Lifecycle rules

Tasks are independent work requests. Submission creates a `pending` task. A worker may claim it as `processing`, increasing its attempt count and receiving a time-limited lease. A matching lease holder can finish it as `completed` with exactly one result or error. An expired lease can be reclaimed with a new token; the new token prevents the former worker from overwriting the newer attempt.

Cancellation is final and clears any lease. Repeating cancellation is an idempotent no-op. A completed task cannot be canceled. When cancellation and completion race, the database transaction that commits first determines the outcome. The system has four statuses: `pending`, `processing`, `completed`, and `canceled`.

## Terms

| Term | Meaning |
| --- | --- |
| Task | A work request, JSON payload, lifecycle status, and eventual outcome. |
| Event | An immutable versioned fact, such as `TaskSubmitted` or `TaskCompleted`. |
| Outbox | A durable record created with `TaskSubmitted` and retried until broker confirmation is recorded. |
| Relay | Claims outbox rows briefly, publishes outside the database transaction, and marks confirmed deliveries. |
| Lease | A time-limited ownership token for processing, used for crash recovery and fencing. |
| Poison message | An invalid envelope or unknown task that cannot succeed and is sent to the dead-letter queue. |

## Boundary notes

The task package owns lifecycle state and transition rules. It uses only the Go standard library and does not depend on HTTP, PostgreSQL, RabbitMQ, or the application package. The application coordinates these rules and storage through ports. The HTTP contract and broker envelope are adapter concerns; neither is a domain type.

`task.Snapshot` is the storage adapter's persistence projection. It includes fields needed to restore and safely update state, including version and lease data. It is not the response exposed to use cases or HTTP clients. The application maps snapshots to `app.TaskResponse`, which contains the task ID, payload, status, outcome, timestamps, and attempt count, without the persistence version, lease token, or event history.

PostgreSQL is the source of truth for task status. RabbitMQ provides at-least-once transport, so duplicate delivery is expected and safe. Worker execution and state changes are separate: a consumer claims the task, performs work, and commits the outcome under the current lease.
