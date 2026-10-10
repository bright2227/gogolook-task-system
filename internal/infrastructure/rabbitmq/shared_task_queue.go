package rabbitmq

import (
	"context"
	"errors"

	"github.com/bright2227/gogolook-task-system/internal/domain"
	"github.com/bright2227/gogolook-task-system/internal/usecase"
	amqp "github.com/rabbitmq/amqp091-go"
)

// PooledChannel holds one AMQP channel and its notification subscriptions.
// Only the borrower reads these notifications. A borrower must settle all
// outstanding work before returning successfully, or return an error to discard
// the channel. Consumers must cancel their subscriptions before reuse.
type PooledChannel struct {
	Channel *amqp.Channel
	Returns <-chan amqp.Return
	Closes  <-chan *amqp.Error // Channel.NotifyClose; ends with this channel.
}

// ChannelPool lends channels on a connection supplied by its owner. It knows
// nothing about tasks, destinations or reconnection. initialize optionally sets
// channel policy (for example, publisher confirms) once for each new channel.
//
// Contexts bound waiting for capacity and the borrower's notification waits.
// AMQP channel setup, writes and close are synchronous: the connection owner
// must bound them, using CloseConnectionOnCancel at its operation/session scope.
type ChannelPool struct {
	connection *amqp.Connection
	idle       chan *PooledChannel
	permits    chan struct{}
	initialize func(*amqp.Channel) error
}

func NewChannelPool(connection *amqp.Connection, size int, initialize func(*amqp.Channel) error) *ChannelPool {
	size = max(size, 1)
	return &ChannelPool{
		connection: connection,
		idle:       make(chan *PooledChannel, size),
		permits:    make(chan struct{}, size),
		initialize: initialize,
	}
}

// WithChannel borrows an actual channel, runs operation, then returns or
// discards it. Capacity is reserved and released entirely inside the pool;
// callers never receive a nil channel representing an opening reservation.
func (p *ChannelPool) WithChannel(ctx context.Context, operation func(*PooledChannel) error) error {
	return p.withChannel(ctx, nil, operation)
}

func (p *ChannelPool) withChannel(ctx context.Context, begin BeginChannelOperation, operation func(*PooledChannel) error) error {
	select {
	case p.permits <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-p.permits }()
	// Both select cases may be ready; cancellation must win before setup.
	if err := ctx.Err(); err != nil {
		return err
	}
	var stop func()
	if begin != nil {
		stop = begin(ctx)
	}
	defer func() {
		if stop != nil {
			stop()
		}
	}()
	channel, err := p.acquire()
	if err != nil {
		return err
	}
	// Until the operation finishes successfully, the channel is unsafe to reuse.
	// This also discards it if the borrower panics and its caller recovers.
	reusable := false
	defer func() {
		if reusable {
			// Join the owner's cancellation callback before another caller can
			// borrow this channel. Keep the guard active during failed-call close.
			if stop != nil {
				stop()
				stop = nil
			}
			if !channel.Channel.IsClosed() {
				p.idle <- channel
				return
			}
		}
		_ = channel.Channel.Close()
	}()
	// Setup can finish after cancellation; do not dispatch canceled work.
	if err := ctx.Err(); err != nil {
		return err
	}
	err = operation(channel)
	reusable = err == nil
	return err
}

func (p *ChannelPool) acquire() (*PooledChannel, error) {
	for {
		select {
		case channel := <-p.idle:
			if !channel.Channel.IsClosed() {
				return channel, nil
			}
		default:
			return p.open()
		}
	}
}

func (p *ChannelPool) open() (*PooledChannel, error) {
	if p.connection == nil {
		return nil, errors.New("channel pool connection is required")
	}
	channel, err := p.connection.Channel()
	if err != nil {
		return nil, TransportError(err)
	}
	pooled := &PooledChannel{
		Channel: channel,
		Returns: channel.NotifyReturn(make(chan amqp.Return, 1)),
		Closes:  channel.NotifyClose(make(chan *amqp.Error, 1)),
	}
	if p.initialize != nil {
		if err := p.initialize(channel); err != nil {
			_ = channel.Close()
			return nil, TransportError(err)
		}
	}
	return pooled, nil
}

// Close drains idle channels after the owner stops borrowers and closes the
// connection. It neither closes the connection nor coordinates session shutdown.
func (p *ChannelPool) Close() error {
	var err error
	for {
		select {
		case channel := <-p.idle:
			if closeErr := channel.Channel.Close(); err == nil && !errors.Is(closeErr, amqp.ErrClosed) {
				err = closeErr
			}
		default:
			return err
		}
	}
}

const DefaultQueueChannels = 8

// BeginChannelOperation is supplied by the connection owner. It starts after
// capacity is acquired, bounds channel cleanup, and ends before channel reuse.
type BeginChannelOperation func(context.Context) func()

// SharedTaskQueue publishes task IDs through a generic channel pool. The upper
// layer supplies a connection and replaces the whole queue on reconnection.
// ChannelPool stores channels and notifications, never TaskQueue adapters.
type SharedTaskQueue struct {
	begin    BeginChannelOperation
	channels *ChannelPool
	queue    string
}

var _ usecase.TaskQueue = (*SharedTaskQueue)(nil)

func NewSharedTaskQueue(connection *amqp.Connection, queue string, size int, begin BeginChannelOperation) *SharedTaskQueue {
	channels := NewChannelPool(connection, size, func(channel *amqp.Channel) error {
		return channel.Confirm(false)
	})
	return &SharedTaskQueue{channels: channels, queue: queue, begin: begin}
}

func (q *SharedTaskQueue) Enqueue(ctx context.Context, id domain.ID) (usecase.Publication, error) {
	// Reject invalid IDs without borrowing a channel or calling the broker.
	if _, _, err := taskIDBody(id); err != nil {
		return usecase.Publication{ID: id, Err: err}, nil
	}
	var publication usecase.Publication
	err := q.channels.withChannel(ctx, q.begin, func(channel *PooledChannel) error {
		// The adapter lasts for this call. Notification subscriptions belong to
		// the channel and are reused; Initialization must not register them again.
		publisher := &TaskQueue{
			queue: q.queue, channel: channel.Channel,
			returns: channel.Returns, closes: channel.Closes,
		}
		var err error
		publication, err = publisher.Enqueue(ctx, id)
		return err
	})
	return publication, err
}

func (q *SharedTaskQueue) Close() error { return q.channels.Close() }
