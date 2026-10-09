# Assignment requirement mapping

**VERIFIED (core only)** means the named unit tests ran successfully in this workspace without external services. **PENDING (container)** means the Testcontainers case is implemented but remains uncompiled and unrun here because Go dependencies are unavailable and Docker access is denied.

| Requirement | Implementation | Verification | Status |
| --- | --- | --- | --- |
| Four lifecycle states; cancellation is final and completed cancellation conflicts | `internal/task`, HTTP API, PostgreSQL migration | `TestLifecycleCancellationAndFencedLeaseRecovery`, `TestCompletedTaskCannotBeCanceled`, `TestTaskHTTPFlowAndValidation` | VERIFIED (core only) |
| Submit, retrieve, and cancel through HTTP | HTTP API | `TestTaskHTTPFlowAndValidation` | VERIFIED (core only) |
| Business failures persist as completed error outcomes; shutdown leaves a leased task recoverable | Consumer application service | `TestConsumerPersistsBusinessErrorAndLeavesInterruptedWorkRecoverable` | VERIFIED (core only) |
| Relay releases failed publishes and marks confirmed publishes | Relay application service | `TestRelayReleasesFailedPublishAndMarksConfirmedPublish` | VERIFIED (core only) |
| Task, event, and outbox commit together or roll back together | PostgreSQL repository transaction | `TestPostgresAndRabbitMQ` submission and rollback phases | PENDING (container) |
| Confirmed outbox publishing; broker outage releases a lease and later succeeds | Relay and RabbitMQ adapter | `TestPostgresAndRabbitMQ` broker outage/recovery phase | PENDING (container) |
| A closed database causes queued retry; recovery completes once | RabbitMQ consumer and PostgreSQL repository | `TestPostgresAndRabbitMQ` closed-database retry phase | PENDING (container) |
| Three-second asynchronous processor, duplicate delivery, and concurrent workers | Processor adapter and RabbitMQ consumer | `TestPostgresAndRabbitMQ` timed completion, duplicate publish, and two-job barrier phases | PENDING (container) |
| Pending cancellation prevents execution; completed cancellation returns conflict | HTTP API and task application | `TestPostgresAndRabbitMQ` PostgreSQL-backed HTTP cancellation and queued delivery phase | PENDING (container) |
| Poison messages reach the dead-letter queue; worker shutdown requeues and lease recovery completes | RabbitMQ adapter and task application | `TestPostgresAndRabbitMQ` poison and shutdown redelivery phases | PENDING (container) |
| Isolated real PostgreSQL and RabbitMQ integration suite | Testcontainers | `go test -tags=integration -v ./tests/integration` | PENDING (container) |
| Long-lived deployment and load exercise | Dockerfile, Compose, `cmd/load` | `docker compose up --build --scale worker=2`; `make load` | PENDING (container) |
| Task lifecycle glossary, architecture boundaries, and design rationale | `docs/domain.md`, `docs/architecture.md`, ADR 001 | Documentation review | VERIFIED |
| Inward dependencies and independent HTTP input boundary | Application input ports/DTOs and HTTP presenter | `TestDependencyRule`, `TestHTTPTranslatesRequestsAndPresentsResponses`, `TestHTTPMapsApplicationErrorsAndRetainsBodyValidation` | VERIFIED (core only) |
