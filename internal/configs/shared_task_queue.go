package configs

import (
	"context"

	"github.com/bright2227/gogolook-task-system/internal/domain"
	"github.com/bright2227/gogolook-task-system/internal/infrastructure/rabbitmq"
	"github.com/bright2227/gogolook-task-system/internal/usecase"
	amqp "github.com/rabbitmq/amqp091-go"
)

// SharedTaskQueueResources owns the HTTP server's lazy broker connection and
// replaces its channel pool after reconnection. Close follows handler drain.
type SharedTaskQueueResources struct {
	Client      *rabbitmq.Client
	size        int
	sessionGate chan struct{}
	connection  *amqp.Connection
	queue       *rabbitmq.SharedTaskQueue
}

var _ usecase.TaskQueue = (*SharedTaskQueueResources)(nil)

func NewSharedTaskQueueResources(cfg rabbitmq.Config, size int) *SharedTaskQueueResources {
	return &SharedTaskQueueResources{Client: rabbitmq.New(cfg), size: size, sessionGate: make(chan struct{}, 1)}
}

func (r *SharedTaskQueueResources) session(ctx context.Context) (*rabbitmq.SharedTaskQueue, error) {
	select {
	case r.sessionGate <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-r.sessionGate }()
	connection, err := r.Client.ConnectionContext(ctx)
	if err != nil {
		return nil, rabbitmq.TransportError(err)
	}
	if r.connection != connection {
		// Client replaces only closed connections, so old-channel cleanup is local.
		if r.queue != nil {
			_ = r.queue.Close()
		}
		r.connection = connection
		r.queue = rabbitmq.NewSharedTaskQueue(connection, r.Client.Queue(), r.size, func(ctx context.Context) func() {
			return rabbitmq.CloseConnectionOnCancel(ctx, connection)
		})
	}
	return r.queue, nil
}

func (r *SharedTaskQueueResources) Enqueue(ctx context.Context, id domain.ID) (usecase.Publication, error) {
	queue, err := r.session(ctx)
	if err != nil {
		return usecase.Publication{}, err
	}
	return queue.Enqueue(ctx, id)
}

func (r *SharedTaskQueueResources) Close() error {
	r.sessionGate <- struct{}{}
	defer func() { <-r.sessionGate }()
	err := r.Client.Close()
	if r.queue != nil {
		if closeErr := r.queue.Close(); err == nil {
			err = closeErr
		}
	}
	return err
}
