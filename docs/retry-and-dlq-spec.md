# Retry, failure records, and dead-letter replay

**Status: Implemented in source; core race tests and vet pass.** PostgreSQL/RabbitMQ integration cases are written, but remain uncompiled and unrun in this workspace because third-party modules/checksums are unavailable and Docker access is denied. CDC is a future option and is not implemented. This is the detailed behavior and operational specification for `gogolook-task-system`.

## Purpose and boundaries

Bound automatic retries for retryable failures, keep durable diagnostics that can explain each decision, and let an operator inspect or replay a dead-lettered task safely. PostgreSQL remains the source of truth for task state and retry decisions. RabbitMQ remains an at-least-once transport. The worker image bundles `/taskctl`, an operator CLI that runs inside an existing worker pod; this design adds no HTTP administration API and no separate admin service.

Keep the four existing task states: `pending`, `processing`, `completed`, and `canceled`. A scheduled retry returns a task to `pending`; a permanent failure or exhausted retry policy completes it with an error. DLQ status is delivery/attempt metadata, not a fifth task status. The existing `Task`, `Consumer.Process`, repository, `Relay`, and outbox remain the main application path and are extended to carry execution identity and retry decisions.

Do not hold a database transaction open during processor execution, network publishing, or RabbitMQ delivery acknowledgement. Do not publish directly to RabbitMQ from inside a database transaction. A worker acknowledges its source delivery only after the durable database transaction recording the outcome and any next outbox event has committed. The relay publishes outside the transaction and records broker confirmation afterward.

The AMQP session owns channel cleanup. Its defer is installed beside channel creation and closes the channel before waiting for worker completion. Delivery handlers return errors to terminate the session rather than closing shared channels themselves. Session failure cancels further intake and requeues unacknowledged deliveries; already-started processors retain the parent context and may commit their outcomes. Parent shutdown cancels processors and leaves unfinished leases recoverable.

## Execution identity and counters

The task entity counts `Attempts` each time `Task.Start` grants a processing lease. A lease recovery can therefore increase this count even when a prior worker was interrupted. Submission creates a task, `TaskSubmitted` event, and work outbox row in one transaction. Retry, terminal failure, and replay similarly commit their decisions with the corresponding outbox row.

Work envelopes require version `1` and UUID `event_id`, `task_id`, and `execution_id`, with a maximum encoded size of 1 KiB. Unknown fields, trailing JSON values, missing IDs, and unsupported versions are quarantined. Broker delivery IDs are transport metadata; task/execution IDs are the application fencing contract. The HTTP API never accepts status updates.

## Durable records and diagnostics

Keep `Task.Attempts` as the lifetime count of processor starts, including lease recovery starts and starts after a manual replay. Never reset or rewrite this history. Add a separate `retry_count` for automatic retries in the current execution cycle and a `replay_cycle` counter for operator initiated replay cycles. A manual replay increments `replay_cycle` and starts that cycle with `retry_count = 0`.

Give every scheduled execution a new `execution_id`. Put `task_id`, `event_id`, and `execution_id` in the broker envelope. The task row stores the current `execution_id`, `retry_count`, `replay_cycle`, and `not_before`. The `execution_id` fences stale messages: a message whose ID differs from the task's current execution is acknowledged as stale without starting work. A matching message can reclaim an expired lease; the existing lease token continues to fence `Finish` so an old worker cannot overwrite a newer owner.

`task_attempts` stores one row for each successful lease grant, unique by lease token and `(task_id, attempt)`. The row is finalized when the attempt ends, and finalized history is immutable. It includes task ID, lifetime attempt number, execution ID, retry/replay counters, worker/pod identity, start/end timestamps, outcome, category, code, and private error detail. Outcomes include `succeeded`, `retry_scheduled`, `dead_lettered`, `canceled`, `interrupted`, and `superseded`. Lease recovery closes any previous open attempt as interrupted; a late worker cannot rewrite that outcome.

`task_dead_letters` stores durable known-task failures with stable DLQ message ID, source event and execution IDs, retry/replay/lifetime counts, worker identity, category/code/detail, failure time, original bytes, SHA-256 hash, and truncation flag. `task_replay_receipts` stores one audited replay decision per DLQ message ID, including operator, reason, request fingerprint, newly scheduled execution ID, and commit time. Retry outbox rows also retain structured failure metadata. `task_events` stores versioned transition facts; operational detail is persisted separately rather than reconstructed from log text.

Known-task failure captures are capped at 64 KiB; malformed/unknown broker captures are capped at 1 MiB. Hashes cover the full original bytes even when capture is truncated. Known failure detail is normalized to valid UTF-8, removes NUL bytes, and is bounded to 4 KiB. Poison-message detail is capped at 512 bytes. Permanent error codes accept letters, digits, `_`, `-`, and `.`, up to 128 bytes; invalid codes become `permanent_error`. Public task errors remain `task execution failed`. Processor implementations must avoid secrets in their diagnostic errors; truncation is not redaction.

Worker logs emit structured JSON to stdout and identify task/event/execution IDs at delivery boundaries. Detailed attempt classifications and replay audit are stored in PostgreSQL. A log backend such as Loki or ELK is optional; no retry count or replay audit depends on parsing logs.

## Failure policy

Classify outcomes at the application boundary, rather than treating every non-`ErrNotFound` error alike.

| Outcome | Task and retry behavior | Delivery behavior |
| --- | --- | --- |
| Processor succeeds | Commit `completed` with result and attempt/event record. | Acknowledge after commit. |
| Retryable execution failure | If budget remains, commit `pending`, clear lease, increment automatic retry count, set next `execution_id` and `not_before`, append attempt/event metadata, and insert its outbox row. If budget is exhausted, commit `completed` with error and a DLQ outbox record. | Acknowledge only after the decision transaction commits. |
| Permanent processor failure | Commit `completed` with error and a DLQ outbox record. | Acknowledge after commit. |
| Timeout | Retryable by default, subject to the same budget. A specific processor policy may classify a timeout as permanent. | Follow the retryable/permanent rule above. |
| Database unavailable, transaction fails, or worker is shutting down | Do not spend automatic retry budget and do not claim a committed retry/DLQ decision. Leave the original delivery unacknowledged; connection closure returns it for redelivery. Lease recovery remains responsible for work interrupted after a lease was granted. | No acknowledgement. Reconnect with bounded backoff. |
| Active lease / duplicate delivery | Do not count as an execution failure and do not consume retry budget. An active lease does not prove that its owner is alive or holds another durable delivery. Wait for a terminal/stale state or lease expiry, then recheck ownership. | Keep the delivery recoverable while waiting; do not acknowledge solely because a lease is active. |
| Stale execution ID | Do not invoke the processor or mutate task outcome. Record/measure the stale delivery if useful. | Acknowledge after the database confirms it is stale. |
| Malformed envelope or unknown task | Quarantine the raw bounded envelope and reason for operator inspection. Never auto-retry it. | Publish a persistent DLQ envelope directly with publisher confirmation before acknowledging the source. Such deliveries have no valid application decision transaction. |

Ordinary processor errors, including execution deadlines, are retryable. A processor returns `usecase.Permanent(code, detail, cause)` to skip automatic retries. Transient storage failures before a task decision commits are transport interruptions. The default retry policy is **three automatic retries after the initial execution**, scheduled after **1 second, 10 seconds, and 60 seconds**. After the fourth failed execution, mark the task `completed` with a generic error and enqueue it for DLQ delivery. Backoff is measured from each recorded failure decision time, not from the original submission.

`TASK_RETRY_DELAYS` configures a non-decreasing list of at most ten durations, each from 1 ms to 1 hour. `none` disables automatic retries; an unset value selects `1s,10s,60s`. The worker captures `POD_NAME` as its identity, falling back to its hostname. Consumer reconnect backoff uses jitter with a 30-second upper bound and does not affect the execution retry budget. Historical retry-queue settings are accepted for configuration compatibility; the current topology declares only work and DLQ queues. Application retries use the database deadline and publish directly to work.

The automatic retry counter increments only in the same transaction that schedules the next execution and inserts its outbox event. A failed database transaction changes neither counter nor task status. `Task.Attempts` still increments on every successful `Start`, including a lease reclaim; the retry counter does not. A replay begins a new replay cycle and automatic retry budget while retaining all lifetime attempt rows.

## Atomic worker decision and outbox flow

`Consumer.Process` validates all three UUIDs and claims the task in a short transaction: lock the task, compare execution identity, check `not_before`, grant a lease, start an attempt row, and save the transition. It then calls `Processor.Execute(ctx, taskID, payload)` outside the transaction with a timeout. In a second short transaction, check the lease token and execution identity again, commit success or classify failure, finalize the attempt, and insert retry or DLQ records/outbox when required. Commit before acknowledging the source message. A parent context cancellation leaves the attempt open for recovery; it does not schedule a business retry.

The outbox should distinguish the destination (`work` or `dead-letter`) and retain `available_at`. Every scheduled work/retry/DLQ publication gets a distinct immutable event/outbox ID, while `task_id` remains the stable business idempotency key. Preserve causation IDs across retries and replay so an operator can trace the chain. The relay only publishes due rows, uses persistent messages and publisher confirms, and marks a row published after confirmation. It must tolerate a crash after confirm and before marking; duplicate deliveries are filtered by execution ID and current task state.

With the polling relay, retries publish directly to the work queue when due; no additional TTL delay is applied after `available_at`. RabbitMQ delay tiers are a separate option for CDC publishing.

The first delivery uses a new execution ID and is due immediately. A retry decision generates a new execution ID, sets `not_before` to the selected backoff deadline, and inserts its outbox row with `available_at = not_before`. A worker must check that the received ID is still current and that the task is due before executing it. If an early copy arrives because of clock skew or duplicate publication, it must not start early; transactionally ensure a due-time outbox publication exists before acknowledging that copy, or keep it unacknowledged for a bounded delay. Do not silently acknowledge the only durable work item before another scheduled publication exists.

Lease recovery is distinct from retry policy. If a worker dies after starting an execution but before recording its result, the same execution ID may be redelivered and reclaimed after the lease expires. This recovery does not consume automatic retry budget. A real external processor must use a stable key derived from task ID and logical effect, such as `taskID + ":charge"`, across retries and replay. The processor contract supplies that task ID. Database fencing cannot undo an external effect or establish exactly-once execution.

## DLQ record and operator workflow

For known tasks that fail permanently or exhaust automatic retries, the task state transition, final attempt record, `TaskDeadLettered` event, and DLQ outbox row commit together. The outbox relay publishes a persistent DLQ message containing a stable `message_id`, task ID, execution/event IDs, reason/category, retry/replay counts, and a bounded failure summary. Publish confirmation precedes marking the outbox row delivered. This avoids acknowledging a task failure without a durable DLQ publication path.

Malformed envelopes and unknown task IDs do not have a valid task row to update. Preserve the bounded raw body and full-body hash, retain a valid UUID broker message ID when supplied (otherwise generate a DLQ ID), and include a reason such as `malformed_envelope` or `unknown_task`. Publish these to the durable DLQ with `mandatory=true` and publisher confirms; only then acknowledge the source delivery. A publish failure leaves the source unacknowledged. A crash after quarantine publish confirmation but before source ACK can produce a duplicate DLQ message; there is no transactional deduplication record for unknown tasks.

The worker image bundles `/taskctl`. Operators run it inside an existing worker pod, for example:

```sh
kubectl exec POD -c worker -- /taskctl dlq inspect --limit 20
kubectl exec POD -c worker -- /taskctl dlq replay --message-id MESSAGE_ID --operator OPERATOR --reason "fixed upstream dependency"
```

The CLI uses the worker's existing database and RabbitMQ configuration. Inspection emits JSON lines for messages and a final scan summary, reporting IDs, reason, counts, failure summary, hash, truncation flag, and a bounded preview. It never acknowledges or removes inspected messages. The default inspection limit is 20, replay scan limit is 1,000, and command timeout is 30 seconds. `--limit`/`--scan-limit` accept 1 through 1,000; `--timeout` must be positive. Replay requires a UUID message ID, `--operator` of 1–128 bytes, and `--reason` of 1–512 bytes after rejecting blank inputs.

RabbitMQ does not provide random access to a queue by `message_id`. The CLI therefore performs a bounded `basic.get` scan on a dedicated channel with manual acknowledgements. Unmatched messages remain unacknowledged and are requeued when that channel closes. Broker inspection can change redelivery flags and queue order even though it preserves messages. Bound both message count and total scan time, report that a scan is partial when the bound is reached, and do not claim that an item is absent merely because it was not in the scanned prefix. Coordinate simultaneous replay operations with a PostgreSQL advisory lock or equivalent. DLQ consumers remain separate from normal workers, so normal worker activity does not drain the DLQ.

For a known task, replay accepts only a task that is still `completed` with an error and whose terminal execution matches the selected DLQ record. It must reject canceled tasks, successful tasks, a different active execution, and malformed/unresolved records. In one transaction, create a replay receipt unique by DLQ message ID, storing the request fingerprint, append an operator/reason audit event, increment `replay_cycle`, reset only the current automatic `retry_count`, clear the prior terminal error, set `pending`, assign a new execution ID, and insert the work outbox row. Preserve old attempt/failure records. After commit, acknowledge the DLQ message. If the process crashes after commit but before ack, a repeated replay sees the committed receipt first, verifies the same request, acknowledges the message, and reports that the replay was already scheduled. A different request for the same message ID is rejected.

Malformed and unknown-task messages are inspect-only. This version provides no repair command and never creates a missing task or bypasses cancellation/success checks. Resolve their cause through an explicitly reviewed data repair or a new ordinary submission; keep the original capture for investigation. A successful known-task replay starts a new budget but retains lifetime attempts and all previous failure/receipt records.

## Schema and upgrade

Migrations are ordered date filenames:

1. `internal/migrations/20261009130909_initial.sql` creates the original task/event/outbox schema.
2. `internal/migrations/20261009130910_retry_dlq.sql` adds execution identity, retry deadlines/counters, outbox destinations/failure metadata, attempts, dead letters, and replay receipts.

A fresh Compose PostgreSQL volume applies both automatically in lexical order. Initialization scripts do not run again for an existing volume. To upgrade an existing schema, stop server, worker, and relay before applying the second migration:

```sh
docker compose -f deployments/docker-compose.yml stop server worker relay
docker compose -f deployments/docker-compose.yml exec -T postgres \
  psql -v ON_ERROR_STOP=1 -U task -d tasks \
  < internal/migrations/20261009130910_retry_dlq.sql
docker compose -f deployments/docker-compose.yml up -d --build server worker relay
```

Legacy tasks receive `execution_id = task_id` and `not_before = created_at`. The migration reissues previously published legacy outbox work for pending/processing tasks using the new envelope; its NULL execution-ID predicate prevents repeated reissues when rerun. Completed/canceled tasks are preserved. Existing active leases receive an open attempt row identified as `migration`, using the recorded update time. Historical attempts before this upgrade cannot be reconstructed and are not fabricated.

Already queued legacy envelopes lack version/execution identity and are quarantined; the reissued outbox provides a valid replacement for unfinished work. Stop old processes before migration so they cannot consume or republish during the change. Default queues are `gogolook.tasks`, `gogolook.tasks.retry`, and `gogolook.tasks.dlq`; `TASK_QUEUE`, `RETRY_QUEUE`, and `DLQ_QUEUE` can retain existing queue names. Preserve the desired database and broker volumes explicitly when changing the Compose project name, which changes default volume names. Existing body-only DLQ messages remain inspect-only.

## CDC as a later relay option

CDC is optional and is not part of the initial retry/DLQ implementation. It may replace the polling relay as the outbox publisher, but it must preserve the same outbox event IDs, destination routing, durable-confirm-before-checkpoint rule, duplicate tolerance, and task/execution idempotency contract. Disable polling for rows owned by the CDC publisher; running both against the same rows creates avoidable duplicate publication.

WAL observation does not enforce `available_at`. A CDC connector sees an outbox insert when it is committed, including future-scheduled retries. Respect `not_before` with a separate due-time scheduler or publish immediately to configured RabbitMQ delay tiers whose TTL/DLX behavior matches the persisted deadline. Do not assume a CDC offset or WAL timestamp delays delivery until `available_at`.

Advance a CDC checkpoint only after the destination has durably confirmed the event or after a durable quarantine record has been committed for an unpublishable outbox row. A poison event encountered before broker publication must not be skipped by advancing the offset first. Keep the outbox as the durable handoff contract so CDC and polling implementations can be compared or replaced without changing task semantics.

## Verification and remaining validation

Core race tests verify policy, rollback, fencing, cancellation, replay receipts, UTF-8-safe diagnostics, and stable processor task identity. `TestPostgresAndRabbitMQ` and `TestRetryDLQAndWorkerCLI` provide real PostgreSQL/RabbitMQ integration scenarios for the main delivery and operator paths. They remain uncompiled/unrun here; the following acceptance behaviors require a Docker-enabled environment:

- A retryable failure schedules exactly three retries at the configured deadlines, then completes with an error and reaches the DLQ; repeated delivery does not increase `retry_count` twice for one decision.
- A permanent failure reaches completed-error/DLQ without consuming retry budget; a timeout follows the configured retry policy.
- Database outage or transaction rollback leaves the original delivery recoverable and does not spend retry budget or create a partial task/event/outbox decision.
- Crash after retry/DLQ transaction commit but before source acknowledgement is safe: redelivery does not create another retry decision or duplicate task attempt record, and the outbox still publishes.
- Duplicate, stale, early, and expired-lease messages are fenced by execution ID, `not_before`, and lease token; they cannot start early or overwrite a newer result.
- Lifetime attempts include every actual start and lease recovery; manual replay starts a new cycle without erasing previous attempts or failure records.
- DLQ messages retain stable identity and reason metadata; malformed and unknown messages preserve bounded raw body/hash, and failed DLQ publication leaves the source message unacknowledged.
- Inspection does not remove DLQ messages; redelivery flags and order may change during a broker scan. Bounded scans report partial results and requeue unmatched messages. Replay validates current task state, records operator/reason, and acknowledges only after commit.
- A crash after replay commit but before DLQ ack followed by the same replay request creates one new execution/outbox event and safely acknowledges the duplicate command. Conflicting replay requests for one DLQ message are rejected.
- The processor receives the stable task ID across retries. A production processor must implement and validate external-effect idempotency; the sample processor only waits and returns a string.
- CDC, if implemented later, does not execute future work before `not_before`, does not run concurrently with polling for the same outbox rows, and does not advance its checkpoint before durable publish or quarantine.

Run `make test-core`, `go vet ./...`, `go build ./...`, and `go test -tags=integration -v ./tests/integration` with dependencies installed and a working Docker daemon. Unit tests alone cannot establish SQL locking/constraints, broker confirms, acknowledgement, requeue, or worker-image CLI behavior. Migration upgrade/re-run and command timeout/advisory-lock behavior still need runtime validation, in addition to the authored integration scenarios. CI is configured for full tests/build and Testcontainers; no CI run is claimed here.
