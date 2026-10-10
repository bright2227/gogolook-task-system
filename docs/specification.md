# Specification

The [original assignment](test.md) requires a REST CRUD API with a task name,
integer status, unit tests and Docker support. This document describes the
implemented contract. The application adds persistent storage, asynchronous
processing, a required payload, pagination and optimistic concurrency.

Processing controls status automatically. POST and PUT accept an optional
`status` of 0 or 1, validate it, and ignore it. Clients edit task content through
`name` and `payload`. The [architecture](architecture.md) explains delivery and
recovery; the [README](../README.md) covers setup and configuration.

## Task representation

| Field | Type | Meaning |
| --- | --- | --- |
| `id` | string | UUID generated at creation, stored in lower case. |
| `name` | string | Task name; defaults to an empty string. |
| `payload` | string | Processing input, including empty or non-JSON text. |
| `status` | integer | 0 incomplete; 1 completed, including terminal failures. |
| `is_running` | boolean | A consumer holds an execution claim. |
| `is_submitted` | boolean | Publication was confirmed and marked, or a consumer claimed the task. |
| `version` | integer | Revision used for edits, claims and outcomes; starts at 1. |
| `result` | string or null | Payload length in Unicode code points after successful execution. |
| `error` | string or null | Terminal failure diagnostic, at most 4096 UTF-8 bytes. |
| `created_time` | string | UTC creation timestamp. |
| `updated_time` | string | UTC timestamp of the latest change. |

Timestamps have microsecond precision. Retry count and the next eligible
execution time are stored internally and are not exposed in the response.

## HTTP API

| Method and path | Request | Success |
| --- | --- | --- |
| `POST /tasks` | Required string `payload`; optional string `name` and integer `status`. | 200 with the task, `Location` and `ETag`. |
| `GET /tasks/{id}` | Task UUID. | 200 with the task and `ETag`. |
| `PUT /tasks/{id}` | Required strings `name` and `payload`; optional integer `status` and `If-Match` header. | 200 with the task and `ETag`. |
| `DELETE /tasks/{id}` | Task UUID; optional `If-Match` header. | 204 with no body. |
| `GET /tasks` | Optional `page_size` and `cursor`. | 200 with `items` and `next_cursor`. |

Example create request:

```json
{"name":"example","payload":"hello"}
```

A newly persisted task has `status=0`, `is_running=false`, `version=1` and no
outcome. Publication and execution can advance its state before later reads.
After successful processing, `status=1`, `is_running=false`, `result="5"` and
`error=null`.

POST first persists the task. A later publication failure still returns the
persisted task; the sweeper recovers it. A storage failure returns 500.

### Validation and errors

POST and PUT require one JSON object and a raw body of at most 8 KiB. Invalid
UTF-8 bytes are rejected before JSON decoding. A name is valid UTF-8 without
NUL and at most 256 bytes; a payload follows the same rules with a 1024-byte
limit. These limits count bytes, while execution counts Unicode code points.
Empty names and payloads are valid.

POST ignores unknown fields; PUT rejects them. A supplied `status` must be the integer 0 or 1;
strings, null and other values are invalid.

| Code | Meaning |
| --- | --- |
| 400 | Invalid pagination parameters or cursor. |
| 404 | Task not found or path ID is not a UUID. |
| 409 | Task is running or the supplied version is stale. |
| 413 | Request body exceeds 8 KiB. |
| 422 | Invalid JSON, fields, name, payload, status or `If-Match`. |
| 500 | Unexpected storage or application failure; response message is generic. |

Task error responses use `{"message":"..."}`.

### Versions and preconditions

POST, GET-by-ID and PUT return a quoted version, for example `ETag: "3"`.
PUT and DELETE accept one `If-Match` value containing a double-quoted positive
integer. Wildcards, weak tags, lists, unquoted values and repeated headers
return 422. A stale valid version returns 409.

Without `If-Match`, the operation locks the current row and skips the client
version comparison. The running-task restriction still applies. The repository
uses the locked row's version for the final write.

Edits, execution claims, outcomes, retries, releases and early-message handoffs
increment the version. Publication marking and rejection backoff refresh
`updated_time` without changing it. The version therefore guards content and
execution changes rather than every response field. Execution can make an
ETag stale between a read and an edit; read again before retrying.

### Editing and deletion

PUT replaces both name and payload. A running task returns 409 even when the
content is identical. Otherwise:

- Identical content is a no-op and preserves the version.
- A name-only edit preserves the outcome, submission and retry state.
- A changed payload returns the task to pending, clears the outcome and retry
  count, and makes it eligible for another publication by the sweeper.
- The task ID and creation timestamp remain stable.

DELETE removes the row under its version and running-state checks. A queued
message for a deleted task is skipped and acknowledged by the consumer.

### Pagination

`page_size` defaults to 20 and accepts values from 1 to 100. Tasks are ordered
by creation time descending, then ID descending. Pass the returned opaque
`next_cursor` as `cursor`; `next_cursor=null` marks the last page.

Pagination uses a timestamp-and-ID cursor rather than offsets. `offset`, `limit`,
repeated `page_size` or `cursor` parameters, and malformed cursors return 400.
Other query parameters are ignored. Pages are not a snapshot across concurrent
creates or deletes.

## Task lifecycle

| Internal state | `status` | `is_running` | Outcome |
| --- | --- | --- | --- |
| Pending | 0 | false | None; waiting for publication, execution or retry. |
| Processing | 0 | true | None; execution is claimed. |
| Completed | 1 | false | A result or terminal error. |

A consumer claims a due pending task, waits three seconds and records the
payload length. Claims and outcomes use version checks. Missing, completed or
already-running tasks are skipped when delivered again.

Network failures, per-task timeouts and stale claims share the configured retry
budget. Other failures, including a recovered panic, complete the task with an
error. A graceful shutdown releases interrupted work without spending a retry.
A payload edit reopens completed work and resets the retry budget.

## Operational endpoints

| Path | Purpose |
| --- | --- |
| `GET /health/live` | Process liveness. |
| `GET /health/ready` | PostgreSQL readiness; does not require RabbitMQ. |
| `GET /metrics` | Request and storage-failure counters. |
| `GET /openapi.json` | Machine-readable API contract. |
| `GET /docs` | Swagger UI. |
