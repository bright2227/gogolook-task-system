.PHONY: deps test test-core race vet build integration test-resources-up test-resources-down up down load

export GOTOOLCHAIN=local

deps:
	go mod tidy

test:
	go test ./...

test-core:
	go test -race ./internal/domain ./internal/migrations ./internal/usecase ./internal/repository/task ./internal/delivery ./internal/delivery/http/... ./internal/delivery/amqp ./internal/infrastructure/rabbitmq ./tests/architecture

race:
	go test -race ./...

vet:
	go vet ./...

build:
	go build ./...

up:
	docker compose -f deployments/docker-compose.yml up --build --remove-orphans

down:
	docker compose -f deployments/docker-compose.yml down --remove-orphans

load:
	go run ./cmd/cli -url http://localhost:8080 -duration 30s -concurrency 8

# Start once; run named tests independently without repeated cold startup.
test-resources-up:
	docker compose -p gogolook-tests -f deployments/docker-compose.test.yml up -d --wait --wait-timeout 60

test-resources-down:
	docker compose -p gogolook-tests -f deployments/docker-compose.test.yml down --timeout 5

integration:
	@printf '%s\n' '$(TEST)' | grep -Eq '^Test[A-Za-z0-9_]+$$' || (echo 'Set TEST to one exact integration test name'; exit 2)
	@go test -tags=integration -list '^$(TEST)$$' ./tests/integration | grep -Fxq '$(TEST)' || (echo 'Integration test does not exist: $(TEST)'; exit 2)
	INTEGRATION_POSTGRES_URL='postgres://task:task@localhost:15432/tasks?sslmode=disable' INTEGRATION_RABBITMQ_URL='amqp://task:task@localhost:15672/' go test -tags=integration -race -run '^$(TEST)$$' -count=1 -timeout 60s -v ./tests/integration
