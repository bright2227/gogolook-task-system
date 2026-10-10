package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bright2227/gogolook-task-system/internal/configs"
	httpapi "github.com/bright2227/gogolook-task-system/internal/delivery/http"
	"github.com/bright2227/gogolook-task-system/internal/repository/task"
	"github.com/bright2227/gogolook-task-system/internal/usecase"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if err := run(); err != nil {
		slog.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := configs.LoadConfig()
	if err != nil {
		slog.Error("invalid configuration", "error", err)
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	startup, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	postgres := configs.NewPostgresResourcesManager(cfg)
	if err := postgres.Initialization(startup); err != nil {
		slog.Error("resource initialization failed", "error", err)
		return err
	}
	defer postgres.Close()
	db := postgres.DB
	cancel()
	// The server does not need RabbitMQ to start: broker resources dial lazily,
	// and sweeper/consumer declare the topology. A publication that fails
	// before then is logged by Submit and retried by the sweeper.
	queue := configs.NewSharedTaskQueueResources(cfg.Rabbit, cfg.QueueChannels)
	defer queue.Close()
	repo := task.NewTaskRepository(db)
	taskUseCase := usecase.NewTaskUseCase(repo, queue, time.Now)
	api := httpapi.NewServer(taskUseCase, func(ctx context.Context) error { return db.PingContext(ctx) })
	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: api, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}
	serveErr := make(chan error, 1)
	go func() { slog.Info("http server listening", "addr", cfg.HTTPAddr); serveErr <- srv.ListenAndServe() }()
	select {
	case err = <-serveErr:
		if !errors.Is(err, http.ErrServerClosed) {
			slog.Error("http server failed", "error", err)
			return err
		}
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err = srv.Shutdown(shutdown); err != nil {
			slog.Error("http shutdown failed", "error", err)
			_ = srv.Close()
		}
		if e := <-serveErr; e != nil && !errors.Is(e, http.ErrServerClosed) {
			slog.Error("http server failed", "error", e)
			return e
		}
	}
	return nil
}
