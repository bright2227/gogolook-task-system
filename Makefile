.PHONY: deps test race vet build integration up down load

deps:
	go mod tidy

test:
	go test ./...

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
