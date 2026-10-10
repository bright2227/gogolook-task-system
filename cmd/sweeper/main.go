package main

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bright2227/gogolook-task-system/internal/configs"
	"github.com/bright2227/gogolook-task-system/internal/delivery"
	"github.com/bright2227/gogolook-task-system/internal/infrastructure/rabbitmq"
	"github.com/bright2227/gogolook-task-system/internal/repository/task"
	"github.com/bright2227/gogolook-task-system/internal/usecase"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if err := run(); err != nil {
		slog.Error("sweeper stopped", "error", err)
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
	startup, cancelStartup := context.WithTimeout(ctx, 60*time.Second)
	defer cancelStartup()
	postgres := configs.NewPostgresResourcesManager(cfg)
	if err := postgres.Initialization(startup); err != nil {
		slog.Error("resource initialization failed", "error", err)
		return err
	}
	defer postgres.Close()
	db := postgres.DB
	rabbit := configs.NewRabbitMQResourcesManager(cfg.Rabbit)
	if err := rabbit.Initialization(startup); err != nil {
		return err
	}
	defer rabbit.Close()
	initialSetup := true
	for ctx.Err() == nil {
		setup, cancelSetup := startup, cancelStartup
		if !initialSetup {
			setup, cancelSetup = context.WithTimeout(ctx, 10*time.Second)
		}
		conn, sessionErr := rabbit.Client.Connection()
		var taskUseCase *usecase.TaskUseCase
		if sessionErr != nil {
			sessionErr = rabbitmq.TransportError(sessionErr)
		} else {
			taskUseCase, sessionErr = newSweeperUseCase(setup, db, conn, rabbit.Client.Queue(), cfg)
		}
		cancelSetup()
		if sessionErr != nil && initialSetup {
			return sessionErr
		}
		if sessionErr == nil {
			if initialSetup {
				slog.Info("submission recovery started", "batch", cfg.SweeperBatch, "interval", cfg.SweeperInterval.String())
			}
			initialSetup = false
			sessionErr = runSweeperSession(ctx, conn, taskUseCase, cfg.SweeperInterval)
		}
		_ = rabbit.Close()
		if ctx.Err() != nil {
			return nil
		}
		if !delivery.IsNetworkError(sessionErr) && !errors.Is(sessionErr, context.DeadlineExceeded) {
			return sessionErr
		}
		slog.Error("sweeper broker session failed", "error", sessionErr)
		timer := time.NewTimer(cfg.SweeperInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
	return nil
}

func runSweeperSession(ctx context.Context, conn *amqp.Connection, taskUseCase *usecase.TaskUseCase, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		batch, cancelBatch := context.WithTimeout(ctx, 10*time.Second)
		stopOperation := rabbitmq.CloseConnectionOnCancel(batch, conn)
		n, err := taskUseCase.PublishPending(batch)
		stopOperation()
		cancelBatch()
		if err != nil {
			return err
		}
		if n > 0 {
			slog.Info("task IDs published", "count", n)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func newSweeperUseCase(ctx context.Context, db *sql.DB, conn *amqp.Connection, queue string, cfg configs.Config) (*usecase.TaskUseCase, error) {
	stopInitialization := rabbitmq.CloseConnectionOnCancel(ctx, conn)
	defer stopInitialization()
	channel, err := conn.Channel()
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, rabbitmq.TransportError(err)
	}
	// A replacement broker may have lost its exchange, queue and bindings.
	if err := rabbitmq.RunTopology(channel, queue); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, rabbitmq.TransportError(err)
	}
	taskQueue := rabbitmq.NewTaskQueue(channel, queue)
	if err := taskQueue.Initialization(); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	taskUseCase := usecase.NewTaskUseCase(task.NewTaskRepository(db), taskQueue, time.Now)
	taskUseCase.SweeperBatch = cfg.SweeperBatch
	taskUseCase.RepublishAfter = cfg.RepublishAfter
	taskUseCase.StaleAfter = cfg.StaleAfter
	// Stale releases spend the consumers' retry budget.
	taskUseCase.RetryDelays = cfg.RetryDelays
	return taskUseCase, nil
}
