package configs

import (
	"context"
	"fmt"
	"time"

	"github.com/bright2227/gogolook-task-system/internal/infrastructure/rabbitmq"
)

type RabbitMQResourcesManager struct {
	Client         *rabbitmq.Client
	cleanupTimeout time.Duration
}

// NewRabbitMQResourcesManager creates the resource manager without I/O.
func NewRabbitMQResourcesManager(cfg rabbitmq.Config) *RabbitMQResourcesManager {
	cleanupTimeout := cfg.DialTimeout
	if cleanupTimeout <= 0 {
		cleanupTimeout = rabbitmq.DefaultDialTimeout
	}
	return &RabbitMQResourcesManager{Client: rabbitmq.New(cfg), cleanupTimeout: cleanupTimeout}
}

// Initialization opens the connection and runs the configured topology.
func (r *RabbitMQResourcesManager) Initialization(ctx context.Context) (err error) {
	defer func() {
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		if err != nil {
			_ = r.Close()
		}
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	conn, err := r.Client.Connection()
	if err != nil {
		return fmt.Errorf("initialize RabbitMQ connection: %w", rabbitmq.TransportError(err))
	}
	stop := rabbitmq.CloseConnectionOnCancel(ctx, conn)
	defer stop()
	if err := ctx.Err(); err != nil {
		return err
	}
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("initialize RabbitMQ channel: %w", rabbitmq.TransportError(err))
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), r.cleanupTimeout)
		defer cancel()
		stop := rabbitmq.CloseConnectionOnCancel(cleanup, conn)
		closeErr := ch.Close()
		stop()
		if err == nil {
			if cleanup.Err() != nil {
				err = fmt.Errorf("close RabbitMQ topology channel: %w", cleanup.Err())
			} else if closeErr != nil {
				err = fmt.Errorf("close RabbitMQ topology channel: %w", rabbitmq.TransportError(closeErr))
			}
		}
	}()
	if err := rabbitmq.RunTopology(ch, r.Client.Queue()); err != nil {
		return fmt.Errorf("initialize RabbitMQ topology: %w", rabbitmq.TransportError(err))
	}
	return nil
}

func (r *RabbitMQResourcesManager) Close() error {
	return r.Client.Close()
}
