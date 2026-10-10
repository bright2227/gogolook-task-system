package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bright2227/gogolook-task-system/internal/configs"
	"github.com/bright2227/gogolook-task-system/internal/delivery"
	deliveryamqp "github.com/bright2227/gogolook-task-system/internal/delivery/amqp"
	"github.com/bright2227/gogolook-task-system/internal/infrastructure/rabbitmq"
	"github.com/bright2227/gogolook-task-system/internal/repository/task"
	"github.com/bright2227/gogolook-task-system/internal/usecase"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if err := run(); err != nil {
		slog.Error("consumer stopped", "error", err)
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
	// Startup releases nothing, so consumers may overlap or scale out. A claim
	// left running by a gone consumer is never taken over by a delivery, even a
	// redelivered one; the sweeper releases it once stale, spending
	// one retry (TASK_RETRY_DELAYS), so a crash-looping task terminates.
	// Graceful shutdown releases its claims itself and spends nothing.
	taskUseCase := usecase.NewTaskUseCase(task.NewTaskRepository(db), nil, time.Now)
	taskUseCase.Timeout = cfg.ProcessTimeout
	taskUseCase.Parallelism = cfg.Consumers
	// The source simulates each task's processing with a three-second delay.
	work := usecase.Delay(3 * time.Second)
	initialSetup := true
	for ctx.Err() == nil {
		setup, cancelSetup := startup, cancelStartup
		if !initialSetup {
			setup, cancelSetup = context.WithTimeout(ctx, 10*time.Second)
		}
		conn, sessionErr := rabbit.Client.Connection()
		var consumer *deliveryamqp.Consumer
		if sessionErr != nil {
			sessionErr = rabbitmq.TransportError(sessionErr)
		} else {
			consumer, sessionErr = newConsumer(setup, conn, rabbit.Client.Queue(), cfg.Consumers, taskUseCase, work, cfg.RetryDelays)
		}
		cancelSetup()
		if sessionErr != nil && initialSetup {
			return sessionErr
		}
		if sessionErr == nil {
			if initialSetup {
				slog.Info("consumer started", "consumers", cfg.Consumers)
			}
			initialSetup = false
			stopOperation := rabbitmq.CloseConnectionOnCancel(ctx, conn)
			for ctx.Err() == nil {
				if sessionErr = consumer.ConsumeBatch(ctx); sessionErr != nil {
					break
				}
			}
			stopOperation()
		}
		_ = rabbit.Close()
		if ctx.Err() != nil {
			return nil
		}
		var processing *deliveryamqp.ProcessingError
		if errors.As(sessionErr, &processing) {
			return sessionErr
		}
		if !delivery.IsNetworkError(sessionErr) && !errors.Is(sessionErr, context.DeadlineExceeded) {
			return sessionErr
		}
		slog.Error("consumer broker session failed", "error", sessionErr)
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
	return nil
}

func newConsumer(ctx context.Context, conn *amqp.Connection, queue string, prefetch int, taskUseCase *usecase.TaskUseCase, work usecase.Work, retryDelays []time.Duration) (*deliveryamqp.Consumer, error) {
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
	consumer := deliveryamqp.NewConsumer(channel, queue, prefetch, taskUseCase, work, retryDelays)
	if err := consumer.Initialization(); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	return consumer, nil
}
