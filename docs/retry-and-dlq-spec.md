# Retry, failure records, and dead-letter replay

**Status: Specified; not implemented.** This document describes a proposed change to the original `vsx-task-system`. The repository currently has no retry counter, typed retry policy, failure-record store, DLQ inspection/replay CLI, or CDC relay. The commands and schema concepts below are design examples, not commands or schema that can be used today.

## Purpose and boundaries

Bound automatic retries for retryable failures, keep durable diagnostics that can explain each decision, and let an operator inspect or replay a dead-lettered task safely. PostgreSQL remains the source of truth for task state and retry decisions. RabbitMQ remains an at-least-once transport. The worker image will eventually bundle an operator CLI that runs inside an existing worker pod; this design adds no HTTP administration API and no separate admin service.

Keep the four existing task states: `pending`, `processing`, `completed`, and `canceled`. A scheduled retry returns a task to `pending`; a permanent failure or exhausted retry policy completes it with an error. DLQ status is delivery/attempt metadata, not a fifth task status. The existing `Task`, `Consumer.Process`, repository, `Relay`, and outbox remain the main application path and are extended to carry execution identity and retry decisions.

Do not hold a database transaction open during processor execution, network publishing, or RabbitMQ delivery acknowledgement. Do not publish directly to RabbitMQ from inside a database transaction. A worker acknowledges its source delivery only after the durable database transaction recording the outcome and any next outbox event has committed. The relay publishes outside the transaction and records broker confirmation afterward.

## Current behavior

The task entity counts `Attempts` each time `Task.Start` grants a processing lease. A lease recovery can therefore increase this count even when a prior worker was interrupted. Task events currently contain only ID, task ID, version, name, and timestamp. Submission creates one outbox row; the relay publishes it to the work queue with publisher confirms.

The RabbitMQ worker currently sends every processing error other than `ErrNotFound` to one TTL retry queue. The envelope contains only `event_id` and `task_id`, so retries have no durable count and can cycle indefinitely. Malformed envelopes and unknown task IDs are copied to the DLQ as body-only messages. Their reason is written to worker logs but not included in the DLQ record. There is no replay command. The sample `Consumer.Process` turns business execution failures into a completed error; a future implementation must classify retryable and permanent outcomes explicitly.

## Proposed counters and records

Keep `Task.Attempts` as the lifetime count of processor starts, including lease recovery starts and starts after a manual replay. Never reset or rewrite this history. Add a separate `retry_count` for automatic retries in the current execution cycle and a `replay_cycle` counter for operator initiated replay cycles. A manual replay increments `replay_cycle` and starts that cycle with `retry_count = 0`.

Give every scheduled execution a new `execution_id`. Put `task_id`, `event_id`, and `execution_id` in the broker envelope. The task row stores the current `execution_id`, `retry_count`, `replay_cycle`, and `not_before`. The `execution_id` fences stale messages: a message whose ID differs from the task's current execution is acknowledged as stale without starting work. A matching message can reclaim an expired lease; the existing lease token continues to fence `Finish` so an old worker cannot overwrite a newer owner.

Persist structured attempt and failure information in PostgreSQL. The practical proposal is a `task_attempts` table, with one row for each successful lease grant; the row is finalized when the attempt ends and completed history is immutable. Rows carry task ID, lifetime attempt number, execution ID, lease token, worker/pod identity, start/end timestamps, outcome category, bounded error code/message, and retry decision. Add a `dead_letter_records` table for durable known-task failure and replay audit metadata, including a stable DLQ message ID, source event/execution IDs, reason, bounded raw-envelope capture, payload hash, truncation flag, timestamps, and replay receipt. Use structured database fields for the retry and replay decisions; do not parse log text to recover counters or replay state.

Keep raw captures bounded. The current task envelope limit is 1 KiB; store at most that amount for an envelope plus its hash and a `truncated` flag. Store a bounded normalized error code and message separately. Avoid storing credentials or unbounded processor output in failure records.

Worker logs remain useful operational output: emit structured JSON to stdout with task ID, event ID, execution ID, attempt, category, and decision. A log backend such as Loki or ELK is optional. PostgreSQL attempt/failure records are authoritative, and no retry count or replay audit depends on parsing logs.

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
| Malformed envelope or unknown task | Quarantine the raw bounded envelope and reason for operator inspection. Never auto-retry it. | For a known task, persist the quarantine decision and outbox atomically. For a message with no matching task row, publish a persistent DLQ message with reason metadata and wait for broker confirmation before acknowledging the source. |

Use typed retryable/permanent processor results or an equivalent explicit policy. Transient storage failures before a task decision is committed are transport interruptions, not completed task failures. The default retry policy is **three automatic retries after the initial execution**, scheduled after **1 second, 10 seconds, and 60 seconds**. After the fourth failed execution, mark the task `completed` with its error and enqueue it for DLQ delivery. These are proposed defaults and should be configuration constants or validated configuration, not hidden magic values.

The automatic retry counter increments only in the same transaction that schedules the next execution and inserts its outbox event. A failed database transaction changes neither counter nor task status. `Task.Attempts` still increments on every successful `Start`, including a lease reclaim; the retry counter does not. A replay begins a new replay cycle and automatic retry budget while retaining all lifetime attempt rows.

## Atomic worker decision and outbox flow

Extend the processor request and message envelope with `execution_id` and the causation/event identity needed to validate the delivery. In one PostgreSQL transaction, lock the task, compare the incoming execution ID with the current one, check `not_before`, apply the domain transition, append event and attempt/failure metadata, and insert the next outbox row when scheduling retry or DLQ delivery. Commit before acknowledging the source message.

The outbox should distinguish the destination (`work` or `dead-letter`) and retain `available_at`. Every scheduled work/retry/DLQ publication gets a distinct immutable event/outbox ID, while `task_id` remains the stable business idempotency key. Preserve causation IDs across retries and replay so an operator can trace the chain. The relay only publishes due rows, uses persistent messages and publisher confirms, and marks a row published after confirmation. It must tolerate a crash after confirm and before marking; duplicate deliveries are filtered by execution ID and current task state.

With the polling relay, retries publish directly to the work queue when due; no additional TTL delay is applied after `available_at`. RabbitMQ delay tiers are a separate option for CDC publishing.

The first delivery uses a new execution ID and is due immediately. A retry decision generates a new execution ID, sets `not_before` to the selected backoff deadline, and inserts its outbox row with `available_at = not_before`. A worker must check that the received ID is still current and that the task is due before executing it. If an early copy arrives because of clock skew or duplicate publication, it must not start early; transactionally ensure a due-time outbox publication exists before acknowledging that copy, or keep it unacknowledged for a bounded delay. Do not silently acknowledge the only durable work item before another scheduled publication exists.

Lease recovery is distinct from retry policy. If a worker dies after starting an execution but before recording its result, the same execution ID may be redelivered and reclaimed after the lease expires. This recovery does not consume automatic retry budget. If an external side effect can run before the worker records a result, that side effect must accept a stable business idempotency key derived from task ID and logical effect; automatic retries and replay must not change that key merely because execution ID changes or otherwise tolerate duplicate execution; database fencing cannot undo an external effect.

## DLQ record and operator workflow

For known tasks that fail permanently or exhaust automatic retries, the task state transition, final attempt record, `TaskDeadLettered` event, and DLQ outbox row commit together. The outbox relay publishes a persistent DLQ message containing a stable `message_id`, task ID, execution/event IDs, reason/category, retry/replay counts, and a bounded failure summary. Publish confirmation precedes marking the outbox row delivered. This avoids acknowledging a task failure without a durable DLQ publication path.

Malformed envelopes and unknown task IDs do not have a valid task row to update. Preserve the raw bounded body, hash, original broker message ID when available, and a reason such as `malformed_envelope` or `unknown_task`. Publish these to the durable DLQ with `mandatory=true` and publisher confirms; only then acknowledge the source delivery. A publish failure leaves the source unacknowledged. Do not erase or overwrite the original body during repair.

Bundle a future `/taskctl` executable in the existing worker image. Operators run it inside an existing worker pod, for example:

```sh
kubectl exec POD -c worker -- /taskctl dlq inspect --limit 20
kubectl exec POD -c worker -- /taskctl dlq replay --message-id MESSAGE_ID --operator OPERATOR --reason "fixed upstream dependency"
```

These are proposed command shapes only; `/taskctl` does not exist yet. The CLI uses the worker's existing database and RabbitMQ configuration. It does not expose an HTTP endpoint or require a new service. Inspection is non-destructive and reports message ID, task/execution/event IDs when present, reason, recorded attempts, failure summary, and whether the body was truncated. Inspection must not acknowledge or remove records.

RabbitMQ does not provide random access to a queue by `message_id`. The CLI therefore performs a bounded `basic.get` scan on a dedicated channel with manual acknowledgements. Unmatched messages remain unacknowledged and are requeued when that channel closes. Broker inspection can change redelivery flags and queue order even though it preserves messages. Bound both message count and total scan time, report that a scan is partial when the bound is reached, and do not claim that an item is absent merely because it was not in the scanned prefix. Coordinate simultaneous replay operations with a PostgreSQL advisory lock or equivalent. DLQ consumers remain separate from normal workers, so normal worker activity does not drain the DLQ.

For a known task, replay accepts only a task that is still `completed` with an error and whose terminal execution matches the selected DLQ record. It must reject canceled tasks, successful tasks, a different active execution, and malformed/unresolved records. In one transaction, create a replay receipt unique by DLQ message ID, storing the request fingerprint, append an operator/reason audit event, increment `replay_cycle`, reset only the current automatic `retry_count`, clear the prior terminal error, set `pending`, assign a new execution ID, and insert the work outbox row. Preserve old attempt/failure records. After commit, acknowledge the DLQ message. If the process crashes after commit but before ack, a repeated replay sees the committed receipt first, verifies the same request, acknowledges the message, and reports that the replay was already scheduled. A different request for the same message ID is rejected.

Do not automatically replay malformed or unknown-task messages. Their replay requires an explicit repair operation that validates the corrected envelope and identifies a valid eligible task; the original raw capture remains immutable and the repair/replay is audited. If a task does not exist, do not create a task or mutate state implicitly. The operator must first resolve the underlying data/task through the ordinary application workflow, then use the explicit repair path. Never bypass cancellation or successful-completion checks.

## CDC as a later relay option

CDC is optional and is not part of the initial retry/DLQ implementation. It may replace the polling relay as the outbox publisher, but it must preserve the same outbox event IDs, destination routing, durable-confirm-before-checkpoint rule, duplicate tolerance, and task/execution idempotency contract. Disable polling for rows owned by the CDC publisher; running both against the same rows creates avoidable duplicate publication.

WAL observation does not enforce `available_at`. A CDC connector sees an outbox insert when it is committed, including future-scheduled retries. Respect `not_before` with a separate due-time scheduler or publish immediately to configured RabbitMQ delay tiers whose TTL/DLX behavior matches the persisted deadline. Do not assume a CDC offset or WAL timestamp delays delivery until `available_at`.

Advance a CDC checkpoint only after the destination has durably confirmed the event or after a durable quarantine record has been committed for an unpublishable outbox row. A poison event encountered before broker publication must not be skipped by advancing the offset first. Keep the outbox as the durable handoff contract so CDC and polling implementations can be compared or replaced without changing task semantics.

## Verification required before implementation is considered complete

Add PostgreSQL/RabbitMQ integration coverage for the following behaviors:

- A retryable failure schedules exactly three retries at the configured deadlines, then completes with an error and reaches the DLQ; repeated delivery does not increase `retry_count` twice for one decision.
- A permanent failure reaches completed-error/DLQ without consuming retry budget; a timeout follows the configured retry policy.
- Database outage or transaction rollback leaves the original delivery recoverable and does not spend retry budget or create a partial task/event/outbox decision.
- Crash after retry/DLQ transaction commit but before source acknowledgement is safe: redelivery does not create another retry decision or duplicate task attempt record, and the outbox still publishes.
- Duplicate, stale, early, and expired-lease messages are fenced by execution ID, `not_before`, and lease token; they cannot start early or overwrite a newer result.
- Lifetime attempts include every actual start and lease recovery; manual replay starts a new cycle without erasing previous attempts or failure records.
- DLQ messages retain stable identity and reason metadata; malformed and unknown messages preserve bounded raw body/hash, and failed DLQ publication leaves the source message unacknowledged.
- Inspection does not remove DLQ messages; redelivery flags and order may change during a broker scan. Bounded scans report partial results and requeue unmatched messages. Replay validates current task state, records operator/reason, and acknowledges only after commit.
- A crash after replay commit but before DLQ ack followed by the same replay request creates one new execution/outbox event and safely acknowledges the duplicate command. Conflicting replay requests for one DLQ message are rejected.
- External processor effects use or document an idempotency key so lease recovery and at-least-once redelivery cannot silently duplicate non-idempotent effects.
- CDC, if implemented later, does not execute future work before `not_before`, does not run concurrently with polling for the same outbox rows, and does not advance its checkpoint before durable publish or quarantine.

Run the real PostgreSQL/RabbitMQ Testcontainers integration suite in CI for these cases. Unit tests alone cannot establish confirm, acknowledgement, TTL/DLX, requeue, or replay behavior.
