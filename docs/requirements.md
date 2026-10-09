# Assignment requirement mapping

| Requirement | Implementation | Verification |
| --- | --- | --- |
| Four persisted statuses and cancellation rules | `internal/task`, PostgreSQL migration, HTTP API | `internal/task` and HTTP API tests |
| Asynchronous three-second work | sample runtime processor and worker | Compose stack and load runner |
| Concurrent workers, duplicate delivery, crash recovery | RabbitMQ consumer, transactionally claimed leases | app/domain tests; integration suite |
| Task, event, and outbox atomicity | PostgreSQL repository transaction | Testcontainers integration suite |
| Confirmed publishing and poison-message handling | RabbitMQ adapter and relay | Testcontainers integration suite |
| Isolated real database and broker tests | build-tagged Testcontainers suite | `go test -tags=integration ./tests/integration` |
| Long-lived deployment and throughput exercise | Dockerfile, Compose, `cmd/load` | `docker compose up --build --scale worker=2`; `make load` |
| Domain glossary, context map, rationale | `docs/domain.md`, ADR 001 | Documentation review |
