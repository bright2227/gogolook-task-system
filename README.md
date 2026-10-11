# Gogolook Task System

A REST task API in Go with PostgreSQL storage and RabbitMQ processing. Tasks
support create, read, update and delete, cursor pagination, and version checks.
A consumer waits three seconds, then records the payload's Unicode character
count as the result. A sweeper recovers failed deliveries and schedules retries.

## Documentation

- [Original assignment](docs/test.md): the interviewer's requirements, unchanged.
- [Specification](docs/specification.md): the implemented API, validation,
  task lifecycle and assumptions.
- [Architecture](docs/architecture.md): components, data flow, recovery,
  design trade-offs.

## Run

Docker with Compose v2 runs the complete stack. Local Go commands require
Go 1.25, matching the module, Dockerfile and CI configuration.

```sh
make up      # PostgreSQL, RabbitMQ, API server, sweeper and consumer
make down    # stop the stack; preserve named data volumes
make load    # create tasks for 30 seconds and poll for completion
```

The API listens on `http://localhost:8080`. Swagger UI is at
`http://localhost:8080/docs`; the OpenAPI document is at
`http://localhost:8080/openapi.json`.

```sh
# Create a task. Copy its id and ETag from the response.
curl -i -H 'Content-Type: application/json' \
  -d '{"name":"example","payload":"hello"}' http://localhost:8080/tasks

# Read the current task and ETag; execution changes the version.
curl -i http://localhost:8080/tasks/TASK_ID

# Replace name and payload, using the latest ETag.
curl -i -X PUT -H 'Content-Type: application/json' -H 'If-Match: "3"' \
  -d '{"name":"edited","payload":"new text"}' http://localhost:8080/tasks/TASK_ID

# Delete using the latest ETag.
curl -i -X DELETE -H 'If-Match: "4"' http://localhost:8080/tasks/TASK_ID

# List the first page; use next_cursor as cursor to fetch another page.
curl 'http://localhost:8080/tasks?page_size=20'
```

POST and PUT require a nonblank `name` (at most 256 UTF-8 bytes) and a string
`payload`; empty payloads are valid. Missing, null, empty or whitespace-only
names return 422. The load CLI supplies a name by default; use `-name` to set it.

PUT and DELETE return 409 while a task is running or the supplied version is
stale. `If-Match` is optional; omitting it operates on the current row under a
lock. A changed payload starts a new execution. `status` is managed by processing;
an optional request value of 0 or 1 is validated and ignored. See the
[API contract](docs/specification.md#http-api) for details.

## Configuration

Compose supplies service URLs. These defaults apply when running commands
directly; configuration is read from environment variables.

| Variable | Default | Purpose |
| --- | --- | --- |
| `DATABASE_URL` | `postgres://postgres:postgres@localhost:5432/gogolook_tasks?sslmode=disable` | PostgreSQL connection. |
| `DB_MAX_OPEN_CONNS` / `DB_MAX_IDLE_CONNS` | `4` / `1` | Database pool size per process. |
| `HTTP_ADDR` | `:8080` | API listen address. |
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` | RabbitMQ connection. |
| `TASK_QUEUE` | `tasks` | Queue name and routing key. |
| `CONSUMERS` | `4` | Parallel executions per consumer process. |
| `PROCESS_TIMEOUT` | `10s` | Deadline for one execution. |
| `TASK_QUEUE_CHANNELS` | `8` | Maximum concurrent API publications. |
| `SWEEPER_BATCH` | `8` | Tasks per sweep, from 1 to 1024. |
| `SWEEPER_INTERVAL` | `1s` | Sweep interval and reconnect delay. |
| `TASK_RETRY_DELAYS` | `1s,10s,60s` | Shared retry budget; `none` disables retries. |
| `TASK_STALE_AFTER` | `5m` | Recover running claims older than this; at least `2*PROCESS_TIMEOUT+10s`. |
| `TASK_REPUBLISH_AFTER` | `0` | Optional recovery of submitted tasks whose broker messages were lost; 0 disables it. |

Run one sweeper. Consumers can scale with `CONSUMER_REPLICAS=3 make up`;
use the same `TASK_RETRY_DELAYS` for consumers and the sweeper. When enabled,
`TASK_REPUBLISH_AFTER` must exceed the longest queue wait plus execution time.
The server starts with PostgreSQL and connects to RabbitMQ lazily.
Sweeper and consumer sessions redeclare broker topology when reconnecting.
Lost messages still require optional republishing; see [recovery](docs/architecture.md#failure-and-recovery).

## Tests

```sh
make race
make vet
make build

make test-resources-up
make integration TEST=TestHTTPAndBatchProcessing
make integration TEST=TestBrokerSessionsRestoreMissingTopology
make test-resources-down
```

`make integration` runs one named test with a 60-second cap against warm
PostgreSQL and RabbitMQ fixtures. Each case gets a fresh database and unique
queues. List available cases with:

```sh
go test -tags=integration -list '^Test' ./tests/integration
```

Without `INTEGRATION_POSTGRES_URL` and `INTEGRATION_RABBITMQ_URL`, integration
tests start disposable containers through Testcontainers. CI runs unit tests
with the race detector, vet, build, and every integration case individually.

## Development data

Every service applies the embedded migration at startup. Migration checksums
reject changes to an already-applied schema. To reset a disposable local
environment, delete both database and broker volumes, then start again:

```sh
docker compose -f deployments/docker-compose.yml down -v
make up
```

This removes stored tasks and queued messages.
