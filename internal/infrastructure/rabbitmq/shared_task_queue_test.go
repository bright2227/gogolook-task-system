package rabbitmq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bright2227/gogolook-task-system/internal/domain"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestChannelPoolCanceledWaitDoesNotRunBorrower(t *testing.T) {
	pool := NewChannelPool(nil, 1, nil)
	pool.permits <- struct{}{}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	called := false
	err := pool.WithChannel(ctx, func(*PooledChannel) error {
		called = true
		return nil
	})
	if !errors.Is(err, context.DeadlineExceeded) || called || len(pool.permits) != 1 {
		t.Fatalf("exhausted pool: err=%v, borrower called=%v, permits=%d", err, called, len(pool.permits))
	}
	<-pool.permits
	if err := pool.WithChannel(context.Background(), func(*PooledChannel) error {
		t.Fatal("borrower ran after failed channel setup")
		return nil
	}); err == nil || len(pool.permits) != 0 {
		t.Fatalf("failed setup: err=%v, permits=%d", err, len(pool.permits))
	}
}

func TestSharedTaskQueueInvalidIDDoesNotCallBroker(t *testing.T) {
	queue := NewSharedTaskQueue(nil, DefaultQueue, 1, nil)
	publication, err := queue.Enqueue(context.Background(), "invalid")
	if err != nil || publication.ID != domain.ID("invalid") || publication.Err == nil || len(queue.channels.permits) != 0 {
		t.Fatalf("invalid publication=%+v, err=%v", publication, err)
	}
}

func TestChannelPoolAlreadyCanceledWithAvailableCapacity(t *testing.T) {
	pool := NewChannelPool(nil, 1, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for range 128 {
		err := pool.WithChannel(ctx, func(*PooledChannel) error {
			t.Fatal("already-canceled borrower ran")
			return nil
		})
		if !errors.Is(err, context.Canceled) || len(pool.permits) != 0 {
			t.Fatalf("already-canceled borrow=%v, permits=%d", err, len(pool.permits))
		}
	}
}

func TestChannelPoolFinishesOwnerGuardBeforeChannelReuse(t *testing.T) {
	pool := NewChannelPool(nil, 2, nil)
	// The callback performs no protocol calls; only reuse ordering is exercised.
	pool.idle <- &PooledChannel{Channel: new(amqp.Channel)}
	stopping := make(chan struct{})
	finishStop := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- pool.withChannel(context.Background(), func(context.Context) func() {
			return func() {
				close(stopping)
				<-finishStop
			}
		}, func(*PooledChannel) error { return nil })
	}()
	<-stopping
	idleBeforeStop := len(pool.idle)
	close(finishStop)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if idleBeforeStop != 0 || len(pool.idle) != 1 || len(pool.permits) != 0 {
		t.Fatalf("idle before guard stopped=%d, idle after=%d, permits=%d", idleBeforeStop, len(pool.idle), len(pool.permits))
	}
}
