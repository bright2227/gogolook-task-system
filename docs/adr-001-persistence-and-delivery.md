# ADR 001: PostgreSQL state with an outbox and RabbitMQ delivery

This document describes the current implementation. The [retry and DLQ specification](retry-and-dlq-spec.md) defines the planned bounded retries and manual replay workflow; those changes remain pending implementation.

**Status:** Accepted

Task state and its event history need a single durable commit boundary. Submission therefore writes the pending task, `TaskSubmitted` event, and outbox record in one PostgreSQL transaction. A relay publishes outbox entries with RabbitMQ publisher confirms, then marks them published. A crash between publish and marking can produce a duplicate, which the worker handles by checking current task state under a database transaction. This protects task state; external processor effects need their own idempotency mechanism.

Workers claim tasks with expiring lease tokens. A restarted worker can reclaim an expired lease; a previous worker cannot finish after a new token is installed. Cancellation is terminal and clears the lease. If cancellation commits before completion, completion is discarded. A completion committed first makes cancellation a conflict. An execution timeout is shorter than its lease so normal timed-out work can persist its error outcome.

Transient broker or storage failures retain/retry delivery. Malformed envelopes and unknown task IDs are dead-lettered. A task execution failure is persisted as `completed` with an error outcome; it is not confused with the transport's retry policy.
