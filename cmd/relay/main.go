package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bright2227/vsx-task-system/internal/adapter/postgres"
	"github.com/bright2227/vsx-task-system/internal/adapter/rabbitmq"
	"github.com/bright2227/vsx-task-system/internal/app"
	runtimecfg "github.com/bright2227/vsx-task-system/internal/runtime"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	cfg, err := runtimecfg.Load()
	if err != nil {
		slog.Error("invalid configuration", "error", err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	startup, cancel := context.WithTimeout(ctx, 5*time.Second)
	store, err := postgres.Open(startup, cfg.DatabaseURL)
	cancel()
	if err != nil {
		slog.Error("database unavailable", "error", err)
		os.Exit(1)
	}
	defer store.Close()
	broker := rabbitmq.New(cfg.Rabbit)
	defer broker.Close()
	relay := app.Relay{Outbox: store, Publisher: broker, Batch: cfg.RelayBatch}
	ticker := time.NewTicker(cfg.RelayInterval)
	defer ticker.Stop()
	slog.Info("outbox relay started", "batch", cfg.RelayBatch, "interval", cfg.RelayInterval.String())
	for ctx.Err() == nil {
		run, cancel := context.WithTimeout(ctx, 10*time.Second)
		n, runErr := relay.Once(run)
		cancel()
		if runErr != nil && !errors.Is(runErr, context.Canceled) {
			slog.Error("relay batch failed", "error", runErr)
		} else if n > 0 {
			slog.Info("outbox events published", "count", n)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
