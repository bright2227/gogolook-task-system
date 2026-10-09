.PHONY: deps test test-core race vet build integration up down load

deps:
	go mod tidy

test:
	go test ./...

test-core:
	go test -race ./internal/task ./internal/app ./internal/adapter/memory ./internal/adapter/httpapi ./internal/adapter/processor ./tests/architecture

race:
	go test -race ./...

vet:
	go vet ./...

build:
	go build ./...

integration:
	go test -tags=integration ./tests/integration -v

up:
	docker compose up --build --scale worker=2

down:
	docker compose down

load:
	go run ./cmd/load -url http://localhost:8080 -duration 30s -concurrency 8
