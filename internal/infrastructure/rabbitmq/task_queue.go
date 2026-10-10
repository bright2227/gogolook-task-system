package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/bright2227/gogolook-task-system/internal/domain"
	"github.com/bright2227/gogolook-task-system/internal/usecase"
	amqp "github.com/rabbitmq/amqp091-go"
)

func taskIDBody(id domain.ID) (domain.ID, []byte, error) {
	taskID, err := domain.ParseID(string(id))
	if err != nil {
		return "", nil, fmt.Errorf("invalid task ID: %w", err)
	}
	return taskID, []byte(taskID), nil
}

// TaskQueue implements usecase.TaskQueue with mandatory routing and broker
// confirms. It is owned by one goroutine. Publishing is assumed much faster than
// consumption, so it reuses one confirm channel without a pool or synchronization.
type TaskQueue struct {
	queue   string
	channel *amqp.Channel
	// Returns belong to this AMQP channel; only one publication is outstanding.
	returns <-chan amqp.Return
	closes  <-chan *amqp.Error
}

var _ usecase.TaskQueue = (*TaskQueue)(nil)

func NewTaskQueue(channel *amqp.Channel, queue string) *TaskQueue {
	return &TaskQueue{channel: channel, queue: queue}
}

// Initialization enables confirms on the supplied channel. Its owner opens
// channels and controls connection lifetime; TaskQueue only returns errors.
func (p *TaskQueue) Initialization() error {
	if p.channel == nil {
		return errors.New("task queue channel is required")
	}
	if err := p.channel.Confirm(false); err != nil {
		return TransportError(err)
	}
	p.returns = p.channel.NotifyReturn(make(chan amqp.Return, 1))
	p.closes = p.channel.NotifyClose(make(chan *amqp.Error, 1))
	return nil
}

// Enqueue reports message failures in Publication. Context or transport failures
// return an error so the caller stops publishing on this channel.
// ctx belongs to this publication, not the channel or connection lifetime.
// Its owner supplies cancellation and decides whether to close/restart resources.
func (p *TaskQueue) Enqueue(ctx context.Context, id domain.ID) (usecase.Publication, error) {
	if err := ctx.Err(); err != nil {
		return usecase.Publication{}, err
	}
	if p == nil || p.channel == nil || p.closes == nil {
		return usecase.Publication{}, errors.New("task queue is not initialized")
	}
	taskID, body, err := taskIDBody(id)
	if err != nil {
		return usecase.Publication{ID: id, Err: err}, nil
	}
	confirmation, err := p.channel.PublishWithDeferredConfirm(DefaultExchange, p.queue, true, false, amqp.Publishing{
		ContentType: "text/plain", DeliveryMode: amqp.Persistent,
		MessageId: string(taskID), Timestamp: time.Now().UTC(), Body: body,
	})
	if err != nil {
		return usecase.Publication{}, TransportError(err)
	}
	returns := p.returns
	done := confirmation.Done()
	var returnedErr error
	for {
		select {
		case <-ctx.Done():
			return usecase.Publication{}, ctx.Err()
		case closed, ok := <-p.closes:
			if ok && closed != nil {
				return usecase.Publication{}, TransportError(closed)
			}
			return usecase.Publication{}, TransportError(io.EOF)
		case returned, ok := <-returns:
			if !ok {
				returns = nil // Wait for NotifyClose to supply the shutdown reason.
				continue
			}
			returnedErr = fmt.Errorf("message returned by broker: %s", returned.ReplyText)
			// Finish this confirmation before reusing the channel for another ID.
		case <-done:
			if !confirmation.Acked() {
				if p.channel.IsClosed() {
					done = nil // Shutdown also completes pending confirmations as NACKs.
					continue
				}
				return usecase.Publication{ID: id, Err: errors.New("publisher nack")}, nil
			}
			// A return is delivered before its ACK, but select may choose the ACK
			// first when both are ready. Consume that buffered return before success.
			select {
			case returned, ok := <-returns:
				if ok {
					returnedErr = fmt.Errorf("message returned by broker: %s", returned.ReplyText)
				}
			default:
			}
			return usecase.Publication{ID: id, Err: returnedErr}, nil
		}
	}
}
