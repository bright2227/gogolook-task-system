# Gogolook Task System

A Go asynchronous task service using Clean Architecture and a four-state lifecycle: `pending`, `processing`, `completed`, and `canceled`. Delivery adapters call application-owned input interfaces with request DTOs; use cases validate inputs, apply task rules, and return response DTOs. PostgreSQL and RabbitMQ implement the application's output ports, and `cmd` packages wire concrete implementations together.

The HTTP API stores a task with its submission event and outbox record in one PostgreSQL transaction. A relay publishes due outbox messages to durable RabbitMQ queues with publisher confirms. Workers execute asynchronously (the sample processor takes three seconds), retry transient execution failures within a bounded budget, and send permanent or exhausted failures to the DLQ through the same transactional outbox. PostgreSQL remains the source of truth. Execution IDs and leases fence stale work. Real processors receive the stable task ID and must make external effects idempotent because execution can succeed before its result is saved.

## Retry and DLQ specification

The [detailed retry and DLQ specification](docs/retry-and-dlq-spec.md) defines failure classification, bounded execution retries, durable failure records, task-state transitions, atomic outbox delivery, and manual investigation and replay.

The four improvements are implemented: failure classification, bounded automatic execution retries, persisted attempt/failure diagnostics, and manual DLQ inspection/replay. The default is three retries after the initial attempt, delayed by **1s, 10s, and 60s**. Set `TASK_RETRY_DELAYS` to a validated comma-separated duration list, or `none` to disable automatic retries. Database outages and worker interruption preserve the delivery without consuming this budget. Public failed-task responses use a generic error; detailed causes stay in operational records.

Manual replay uses `/taskctl`, bundled in the worker image and executed inside the existing worker pod. It uses the pod's database and RabbitMQ configuration; no administration HTTP API is added. A replay commits an audit receipt and a new work outbox record before acknowledging the DLQ message. Repeating the same request is idempotent.

Retry counts, attempt outcomes, and replay receipts belong in PostgreSQL. They must survive process restarts and commit with the task and outbox changes. Structured JSON application logs help troubleshooting. A separate log aggregation platform is optional; logs are not the source of retry decisions or replay state.

The [architecture](docs/architecture.md), [lifecycle](docs/domain.md), and [requirement mapping](docs/requirements.md) explain the implementation. The retry/DLQ specification records transaction boundaries, failure policies, operator commands, upgrade steps, and an optional future CDC handoff. CDC is not implemented.

The layout follows the original client project's layer names:

```text
cmd/                         # server, worker, relay, taskctl, load runner
internal/
  configs/                   # environment configuration
  domain/                    # task entity and lifecycle rules
  usecase/                   # operations, DTOs, input/output interfaces
  delivery/
    http/                    # HTTP handlers and presentation
    rabbitmq/                # broker transport and worker delivery
  repository/task/
    postgres/                # durable storage and outbox
    memory/                  # transactional test double
  infrastructure/processor/  # simulated task execution
  migrations/                # SQL schema
deployments/                 # Dockerfile and docker-compose.yml
tests/                       # architecture and Testcontainers checks
```

Repository implementations live under `repository`; their interfaces stay in `usecase` so dependencies point inward. HTTP delivery uses Go's `net/http` package.

## Run the stack

Requires Docker Compose. The stack publishes the API at `http://localhost:8080`, PostgreSQL on `5432`, and RabbitMQ on `5672` (`15672` management UI). A fresh PostgreSQL volume runs both migrations in filename order on its first start.

For an existing database, stop application processes before applying migrations. Apply [20261009130909_initial.sql](internal/migrations/20261009130909_initial.sql) if the original schema is absent, then [20261009130910_retry_dlq.sql](internal/migrations/20261009130910_retry_dlq.sql). The latter backfills execution identity and active attempt records and reissues legacy outbox work for unresolved tasks. See the [upgrade procedure](docs/retry-and-dlq-spec.md#schema-and-upgrade) before reusing existing volumes or queues.

```sh
docker compose -f deployments/docker-compose.yml up --build --scale worker=2
```

Submit, inspect, and cancel tasks:

```sh
curl -i -H 'Content-Type: application/json' \
  -d '{"payload":{"message":"hello"}}' http://localhost:8080/tasks
curl http://localhost:8080/tasks/TASK_ID
curl -X POST http://localhost:8080/tasks/TASK_ID:cancel
```

Creation returns `202 Accepted`. Cancellation is idempotent while canceled; completed tasks return `409 Conflict` when cancellation is requested. A cancellation racing with execution wins if it commits first, and a worker's stale lease cannot overwrite it. Payloads must be JSON no larger than 64 KiB. See the [architecture guide](docs/architecture.md), [task lifecycle and terms](docs/domain.md), [design decision](docs/adr-001-persistence-and-delivery.md), and [requirement mapping](docs/requirements.md).

## Inspect and replay the DLQ

Run inside an existing worker pod:

```sh
kubectl exec POD -c worker -- /taskctl dlq inspect --limit 20
kubectl exec POD -c worker -- /taskctl dlq replay \
  --message-id MESSAGE_UUID --operator YOUR_NAME --reason "fixed upstream dependency"
```

For the Compose stack, use `docker compose -f deployments/docker-compose.yml exec worker /taskctl dlq inspect --limit 20`. Scans default to a 30-second deadline and are bounded to 1,000 messages; inspection preserves messages but may change queue order and redelivery flags. Malformed and unknown-task messages are inspect-only. A repair command is not provided.

## Develop and validate

Install the Go module dependencies with `make deps`. The default Go suite, race detector, vet, and build are available through `make test`, `make race`, `make vet`, and `make build`. The integration suite uses Testcontainers to start isolated real PostgreSQL and RabbitMQ containers:

```sh
go test -tags=integration -v ./tests/integration
```

It requires a working Docker daemon and the Go module dependencies. In the current development environment Docker socket access is denied and dependency checksums/modules are unavailable, so full builds and the integration suite remain uncompiled and unrun here. CI is configured to run them on a Docker-enabled GitHub-hosted runner.

`make test-core` runs race-enabled tests for the entity, use cases, HTTP adapter, and dependency boundaries, and compiles the memory and processor adapters. These checks and vet pass without third-party dependencies. The architecture test rejects outward imports from the entity/use-case layers and direct domain imports from the HTTP adapter. Compose configuration validation also passes; this does not verify running containers.

The Compose stack is intended for longer manual runs. Once it is running, a concurrent create-and-poll load run can be started with `make load`; tune duration, concurrency, and poll interval via `go run ./cmd/load -h`.
