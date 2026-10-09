# Domain and context map

## Task lifecycle

`Task` owns the lifecycle and accepts only valid transitions. Submission creates `pending` at version 0. A worker atomically claims a task as `processing`, increments its attempt count, and records a time-limited lease. A matching lease holder may finish it as `completed` with exactly one result or error. Expired leases can be reclaimed with a new token; the token fences out an older worker after reclamation. Cancellation is final and clears any lease. Repeating cancellation is an idempotent no-op, while cancellation after completion is a conflict.

| Term | Meaning |
| --- | --- |
| Task | Work request, payload, lifecycle status, and outcome. |
| Event | Immutable versioned fact such as `TaskSubmitted` or `TaskCompleted`. |
| Outbox | Durable record created with `TaskSubmitted`, retried until broker confirmation is recorded. |
| Relay | Claims outbox rows briefly, publishes outside the database transaction, and marks confirmed deliveries. |
| Lease | Time-bounded ownership token for processing, used for crash recovery and fencing. |
| Poison message | Invalid envelope or unknown task that cannot succeed; sent to the dead-letter queue. |

## Strategic boundary

The **Task Processing** bounded context owns the terms and rules above. Its core
capability is coordinating independent work, cancellation, and recovery without
allowing duplicate or stale executions to corrupt task state. Reliable delivery
is a supporting capability inside this context; HTTP, PostgreSQL, and RabbitMQ
are adapters rather than separate business contexts. Authentication and task
dependencies are outside the assignment's scope.

Clients consume the published HTTP contract. The broker envelope is the
integration contract between the relay and workers; it carries task and event
identities, while task state stays in PostgreSQL. Neither adapter contributes
types to the domain model.

## Component relationships

```text
HTTP client -> HTTP API -> Task application -> Task domain
                                  |                 |
                                  v                 v
                          PostgreSQL adapter <- task events/outbox
                                  |
                              Relay -> RabbitMQ -> Worker -> Task application
```

The domain has no infrastructure dependencies. Application services coordinate domain transitions through repository/store ports. PostgreSQL atomically stores task state, event history, and outbox rows. RabbitMQ provides at-least-once transport; it does not own task status. Worker execution and state changes are deliberately separate, so delivery can be repeated safely.
