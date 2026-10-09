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
	adapterprocessor "github.com/bright2227/vsx-task-system/internal/adapter/processor"
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
	consumer := app.Consumer{Store: store, Clock: time.Now, Processor: adapterprocessor.SimulatedProcessor{}, Lease: cfg.Lease, Timeout: cfg.ProcessTimeout}
	slog.Info("worker started", "workers", cfg.Workers)
	err = broker.Consume(ctx, cfg.Workers, consumer)
	if err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("worker stopped", "error", err)
		os.Exit(1)
	}
}
