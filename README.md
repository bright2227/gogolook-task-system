# VSX Task System

A small asynchronous task service built around a four-state lifecycle: `pending`, `processing`, `completed`, and `canceled`. The HTTP API stores a task with its submission event and outbox record in one PostgreSQL transaction. A relay publishes outbox messages to durable RabbitMQ queues with publisher confirms; workers execute each job asynchronously (the sample processor takes three seconds) and persist the terminal outcome. PostgreSQL remains the source of truth, so duplicate messages are safe and processing leases allow recovery after worker loss.

## Run the stack

Requires Docker Compose. The stack publishes the API at `http://localhost:8080`, PostgreSQL on `5432`, and RabbitMQ on `5672` (`15672` management UI). The Compose database is initialized from the migration on its first start.

The migration mount runs only when PostgreSQL initializes a new data volume. To apply the initial schema to an existing empty database, run `docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U task -d tasks < migrations/001_initial.sql`. Future schema changes need new versioned migrations.

```sh
docker compose up --build --scale worker=2
```

Submit, inspect, and cancel tasks:

```sh
curl -i -H 'Content-Type: application/json' \
  -d '{"payload":{"message":"hello"}}' http://localhost:8080/tasks
curl http://localhost:8080/tasks/TASK_ID
curl -X POST http://localhost:8080/tasks/TASK_ID:cancel
```

Creation returns `202 Accepted`. Cancellation is idempotent while canceled; completed tasks return `409 Conflict` when cancellation is requested. A cancellation racing with execution wins if it commits first, and a worker's stale lease cannot overwrite it. Payloads must be JSON no larger than 64 KiB. See [domain notes](docs/domain.md), [design decision](docs/adr-001-persistence-and-delivery.md), and [requirement mapping](docs/requirements.md).

## Develop and validate

Install the Go module dependencies with `make deps`. The default Go suite, race detector, vet, and build are available through `make test`, `make race`, `make vet`, and `make build`. The integration suite uses Testcontainers to start isolated real PostgreSQL and RabbitMQ containers:

```sh
go test -tags=integration -v ./tests/integration
```

It requires a working Docker daemon and the Go module dependencies. In the current development environment Docker socket access is denied and dependencies are not locally cached, so full builds and the integration suite have not been verified here. CI is configured to run them on a Docker-enabled GitHub-hosted runner. Core unit tests, race checks, and vet pass for the domain, application, and HTTP packages; Compose configuration validation also passes. The Compose stack is intended for longer manual runs. Once it is running, a concurrent create-and-poll load run can be started with `make load`; tune duration, concurrency, and poll interval via `go run ./cmd/load -h`.
