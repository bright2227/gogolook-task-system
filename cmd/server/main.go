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

	"github.com/bright2227/vsx-task-system/internal/configs"
	"github.com/bright2227/vsx-task-system/internal/delivery/http"
	"github.com/bright2227/vsx-task-system/internal/repository/task/postgres"
	"github.com/bright2227/vsx-task-system/internal/usecase"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	cfg, err := configs.Load()
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
	api := &httpapi.API{Tasks: usecase.Tasks{Store: store, Clock: time.Now}, Ready: func(ctx context.Context) error { return store.DB.PingContext(ctx) }}
	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: api.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}
	serveErr := make(chan error, 1)
	go func() { slog.Info("http server listening", "addr", cfg.HTTPAddr); serveErr <- srv.ListenAndServe() }()
	select {
	case err = <-serveErr:
		if !errors.Is(err, http.ErrServerClosed) {
			slog.Error("http server failed", "error", err)
			os.Exit(1)
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
			os.Exit(1)
		}
	}
}
