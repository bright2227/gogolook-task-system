.PHONY: deps test test-core race vet build integration up down load

deps:
	go mod tidy

test:
	go test ./...

test-core:
	go test -race ./internal/domain ./internal/usecase ./internal/repository/task/memory ./internal/delivery/http ./internal/infrastructure/processor ./tests/architecture

race:
	go test -race ./...

vet:
	go vet ./...

build:
	go build ./...

integration:
	go test -tags=integration ./tests/integration -v

up:
	docker compose -f deployments/docker-compose.yml up --build --scale worker=2

down:
	docker compose -f deployments/docker-compose.yml down

load:
	go run ./cmd/load -url http://localhost:8080 -duration 30s -concurrency 8
