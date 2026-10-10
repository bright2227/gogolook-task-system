// Package amqp is the inbound RabbitMQ delivery adapter: it receives task ID
// batches and drives TaskUseCase.ProcessTasks, as the HTTP adapter drives requests.
package amqp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/bright2227/gogolook-task-system/internal/domain"
	"github.com/bright2227/gogolook-task-system/internal/infrastructure/rabbitmq"
	"github.com/bright2227/gogolook-task-system/internal/usecase"
	amqp091 "github.com/rabbitmq/amqp091-go"
)

type Consumer struct {
	channel     *amqp091.Channel
	queue       string
	prefetch    int
	closes      <-chan *amqp091.Error
	deliveries  <-chan amqp091.Delivery
	taskUseCase *usecase.TaskUseCase
	work        usecase.Work
	retryDelays []time.Duration
}

// NewConsumer takes prefetch, the unacknowledged delivery limit and batch cap.
// One batch is one round of parallel work, so callers pass the use case's
// Parallelism: a delivered message then waits unacknowledged for at most one
// batch, which ProcessTimeout bounds.
func NewConsumer(channel *amqp091.Channel, queue string, prefetch int, taskUseCase *usecase.TaskUseCase, work usecase.Work, retryDelays []time.Duration) *Consumer {
	return &Consumer{channel: channel, queue: queue, prefetch: prefetch, taskUseCase: taskUseCase, work: work, retryDelays: retryDelays}
}

// ProcessingError identifies business processing that failed before its outcome
// was committed. The runtime stops; closing its channel requeues the batch,
// whose running claims the sweeper releases once stale (a redelivery skips them).
type ProcessingError struct{ Err error }

func (e *ProcessingError) Error() string { return e.Err.Error() }
func (e *ProcessingError) Unwrap() error { return e.Err }

// deliveryBatch contains delivery metadata only. Its tag belongs to the consumer
// channel that received it; processing and ACK complete before another receive.
type deliveryBatch struct {
	IDs         []domain.ID
	DeliveryTag uint64
}

func decode(body []byte) (domain.ID, error) {
	taskID, err := domain.ParseID(string(body))
	if err != nil {
		return "", fmt.Errorf("invalid task ID: %w", err)
	}
	return taskID, nil
}

// Initialization configures prefetch on the supplied channel. The caller owns
// channel creation and connection lifetime; Consumer only returns errors.
func (c *Consumer) Initialization() error {
	if c.channel == nil {
		return errors.New("consumer channel is required")
	}
	if c.taskUseCase == nil {
		return errors.New("consumer task use case is required")
	}
	if c.prefetch < 1 {
		return errors.New("consumer prefetch must be positive")
	}
	if err := c.channel.Qos(c.prefetch, 0, false); err != nil {
		return rabbitmq.TransportError(err)
	}
	c.closes = c.channel.NotifyClose(make(chan *amqp091.Error, 1))
	return nil
}

// ConsumeBatch receives one batch, commits its task outcomes, then acknowledges
// it. Processing failure leaves the batch unacknowledged; ACK failures remain separate.
func (c *Consumer) ConsumeBatch(ctx context.Context) error {
	batch, err := c.receive(ctx)
	if err != nil {
		return err
	}
	return c.settle(ctx, batch, c.ack)
}

// settle is the process→ACK step; ack is a parameter so the order is testable.
func (c *Consumer) settle(ctx context.Context, batch deliveryBatch, ack func(uint64) error) error {
	if err := c.taskUseCase.ProcessTasks(ctx, batch.IDs, c.work, c.retryDelays); err != nil {
		return &ProcessingError{Err: err}
	}
	return ack(batch.DeliveryTag)
}

// receive reads one batch without acknowledging valid deliveries.
func (c *Consumer) receive(ctx context.Context) (deliveryBatch, error) {
	if c.channel == nil || c.closes == nil {
		return deliveryBatch{}, errors.New("consumer is not initialized")
	}
	if c.deliveries == nil {
		var err error
		c.deliveries, err = c.channel.Consume(c.queue, "gogolook-consumer-"+string(domain.NewID()), false, false, false, false, nil)
		if err != nil {
			return deliveryBatch{}, rabbitmq.TransportError(err)
		}
	}
	select {
	case <-ctx.Done():
		return deliveryBatch{}, ctx.Err()
	case closeErr, ok := <-c.closes:
		if ok && closeErr != nil {
			return deliveryBatch{}, rabbitmq.TransportError(closeErr)
		}
		return deliveryBatch{}, rabbitmq.TransportError(io.EOF)
	case d, ok := <-c.deliveries:
		if !ok {
			// The client sends the close reason before closing deliveries.
			// Preserve permission/protocol failures instead of retrying EOF.
			select {
			case closeErr := <-c.closes:
				if closeErr != nil {
					return deliveryBatch{}, rabbitmq.TransportError(closeErr)
				}
			default:
			}
			return deliveryBatch{}, rabbitmq.TransportError(io.EOF)
		}
		deliveries, err := collectBatch(ctx, c.deliveries, d, c.prefetch)
		if err != nil {
			return deliveryBatch{}, err
		}
		return decodeBatch(deliveries)
	}
}

func collectBatch(ctx context.Context, deliveries <-chan amqp091.Delivery, first amqp091.Delivery, limit int) ([]amqp091.Delivery, error) {
	batch := make([]amqp091.Delivery, 0, limit)
	batch = append(batch, first)
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()
	for len(batch) < limit {
		select {
		case d, ok := <-deliveries:
			if !ok {
				return batch, nil
			}
			batch = append(batch, d)
		case <-timer.C:
			return batch, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return batch, nil
}

// decodeBatch rejects malformed messages individually and leaves valid messages
// unacknowledged. Only plain IDs and the highest valid delivery tag leave here.
func decodeBatch(deliveries []amqp091.Delivery) (deliveryBatch, error) {
	batch := deliveryBatch{IDs: make([]domain.ID, 0, len(deliveries))}
	for _, message := range deliveries {
		id, err := decode(message.Body)
		if err != nil {
			if err := message.Nack(false, false); err != nil {
				return deliveryBatch{}, rabbitmq.TransportError(err)
			}
			continue
		}
		batch.IDs = append(batch.IDs, id)
		if message.DeliveryTag > batch.DeliveryTag {
			batch.DeliveryTag = message.DeliveryTag
		}
	}
	return batch, nil
}

// ack acknowledges all outstanding deliveries through deliveryTag on this
// consumer's channel. A zero tag represents a batch with no valid deliveries.
func (c *Consumer) ack(deliveryTag uint64) error {
	if deliveryTag == 0 {
		return nil
	}
	if c.channel == nil || c.closes == nil || c.deliveries == nil {
		return errors.New("consumer is not initialized")
	}
	return rabbitmq.TransportError(c.channel.Ack(deliveryTag, true))
}
