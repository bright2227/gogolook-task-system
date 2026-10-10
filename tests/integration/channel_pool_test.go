//go:build integration

package integration

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bright2227/gogolook-task-system/internal/domain"
	"github.com/bright2227/gogolook-task-system/internal/infrastructure/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestSharedTaskQueueChannelOwnershipAndReturns(t *testing.T) {
	s := newIntegrationStack(t)
	fixture := s.client(t, "integration.channel-pool")
	conn, err := fixture.broker.Connection()
	if err != nil {
		t.Fatal(err)
	}
	connectionCloses := conn.NotifyClose(make(chan *amqp.Error, 1))
	pool := rabbitmq.NewChannelPool(conn, 1, nil)
	t.Cleanup(func() {
		_ = fixture.broker.Close()
		_ = pool.Close()
	})
	var first *amqp.Channel
	if err := pool.WithChannel(s.ctx, func(channel *rabbitmq.PooledChannel) error {
		first = channel.Channel
		_, err := channel.Channel.QueueInspect(fixture.broker.Queue())
		return err
	}); err != nil {
		t.Fatal(err)
	}
	// A borrower can perform arbitrary channel operations. Successful calls
	// reuse the channel; a channel exception is discarded without closing conn.
	if err := pool.WithChannel(s.ctx, func(channel *rabbitmq.PooledChannel) error {
		if channel.Channel != first {
			t.Fatal("successful borrow did not reuse its channel")
		}
		_, err := channel.Channel.QueueInspect(s.queue("integration.channel-pool.missing"))
		select {
		case closeErr, ok := <-channel.Closes:
			if !ok || closeErr == nil || closeErr.Code != 404 {
				t.Fatalf("channel close notification=%v, open=%v; want AMQP 404", closeErr, ok)
			}
		case <-s.ctx.Done():
			t.Fatal("channel failure did not notify its borrower")
		}
		return err
	}); err == nil {
		t.Fatal("missing queue inspection unexpectedly succeeded")
	}
	if conn.IsClosed() {
		t.Fatal("channel exception closed its connection")
	}
	select {
	case closeErr := <-connectionCloses:
		t.Fatalf("channel exception notified the connection owner: %v", closeErr)
	default:
	}
	borrowed := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- pool.WithChannel(s.ctx, func(channel *rabbitmq.PooledChannel) error {
			if channel.Channel == first {
				return errors.New("failed channel was reused")
			}
			close(borrowed)
			select {
			case <-release:
				return nil
			case <-s.ctx.Done():
				return s.ctx.Err()
			}
		})
	}()
	select {
	case <-borrowed:
	case err := <-done:
		t.Fatalf("replacement borrow failed: %v", err)
	case <-s.ctx.Done():
		t.Fatal(s.ctx.Err())
	}
	waiting, cancelWaiting := context.WithTimeout(s.ctx, 50*time.Millisecond)
	err = pool.WithChannel(waiting, func(*rabbitmq.PooledChannel) error {
		return errors.New("exhausted pool ran another borrower")
	})
	cancelWaiting()
	close(release)
	if activeErr := <-done; activeErr != nil {
		t.Fatal(activeErr)
	}
	if !errors.Is(err, context.DeadlineExceeded) || conn.IsClosed() {
		t.Fatalf("capacity wait=%v, connection closed=%v", err, conn.IsClosed())
	}

	// Repeated mandatory returns must not accumulate subscriptions or leak
	// notification state into later successful publications on reused channels.
	queueName := s.queue("integration.channel-pool.restored")
	queue := rabbitmq.NewSharedTaskQueue(conn, queueName, 2, nil)
	t.Cleanup(func() {
		_ = fixture.broker.Close()
		_ = queue.Close()
	})
	for range 4 {
		publication, err := queue.Enqueue(s.ctx, domain.NewID())
		if err != nil || publication.Err == nil || !strings.Contains(publication.Err.Error(), "NO_ROUTE") {
			t.Fatalf("unroutable publication=%+v,%v", publication, err)
		}
	}
	if err := pool.WithChannel(s.ctx, func(channel *rabbitmq.PooledChannel) error {
		return rabbitmq.RunTopology(channel.Channel, queueName)
	}); err != nil {
		t.Fatal(err)
	}
	s.cleanupQueue(t, queueName)
	const publications = 12
	var wg sync.WaitGroup
	failures := make(chan error, publications)
	for range publications {
		wg.Go(func() {
			publication, err := queue.Enqueue(s.ctx, domain.NewID())
			if err != nil {
				failures <- err
			} else if publication.Err != nil {
				failures <- publication.Err
			}
		})
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	if messages := inspectQueue(s, queueName); messages != publications {
		t.Fatalf("queued messages=%d, want %d", messages, publications)
	}
}

func TestSharedTaskQueueUsesOwnerCancellation(t *testing.T) {
	s := newIntegrationStack(t)
	fixture := s.client(t, "integration.channel-pool-cancel")
	for _, tc := range []struct {
		name          string
		class, method uint16
		warm          bool
	}{
		{name: "channel open", class: 20, method: 10},
		{name: "confirm setup", class: 85, method: 10},
		{name: "publication", class: 60, method: 40, warm: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proxy := newMethodBlockingProxy(t, s.brokerURL, tc.class, tc.method)
			client := rabbitmq.New(rabbitmq.Config{URL: proxy.URL, DialTimeout: time.Second})
			conn, err := client.ConnectionContext(s.ctx)
			if err != nil {
				t.Fatal(err)
			}
			queue := rabbitmq.NewSharedTaskQueue(conn, fixture.broker.Queue(), 1, nil)
			t.Cleanup(func() {
				_ = client.Close()
				_ = queue.Close()
			})
			if tc.warm {
				if publication, err := queue.Enqueue(s.ctx, domain.NewID()); err != nil || publication.Err != nil {
					t.Fatalf("warm publication=%+v,%v", publication, err)
				}
			}
			proxy.blocked.Store(true)
			ctx, cancel := context.WithTimeout(s.ctx, 300*time.Millisecond)
			defer cancel()
			// The owner supplies the guard and observes connection closure.
			// Neither ChannelPool nor the task adapter installs one.
			closes := conn.NotifyClose(make(chan *amqp.Error, 1))
			stopConnection := rabbitmq.CloseConnectionOnCancel(ctx, conn)
			defer stopConnection()
			done := make(chan error, 1)
			go func() {
				_, err := queue.Enqueue(ctx, domain.NewID())
				done <- err
			}()
			select {
			case <-proxy.started:
			case err := <-done:
				t.Fatalf("operation stopped before the blocked method: %v", err)
			case <-time.After(time.Second):
				t.Fatal("operation did not reach the blocked method")
			}
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("blocked operation unexpectedly succeeded")
				}
			case <-time.After(time.Second):
				t.Fatal("owner cancellation did not release the channel operation")
			}
			stopConnection()
			select {
			case <-closes:
			case <-time.After(time.Second):
				t.Fatal("connection owner did not receive its close notification")
			}
			if !conn.IsClosed() {
				t.Fatal("owner cancellation left the connection open")
			}
		})
	}
}

func TestChannelPoolCancellationAfterSetup(t *testing.T) {
	s := newIntegrationStack(t)
	fixture := s.client(t, "integration.channel-pool-setup-cancel")
	conn, err := fixture.broker.Connection()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	var opened *amqp.Channel
	pool := rabbitmq.NewChannelPool(conn, 1, func(channel *amqp.Channel) error {
		opened = channel
		cancel()
		return nil
	})
	t.Cleanup(func() { _ = pool.Close() })
	err = pool.WithChannel(ctx, func(*rabbitmq.PooledChannel) error {
		t.Error("borrower ran after setup canceled its context")
		return nil
	})
	if !errors.Is(err, context.Canceled) || opened == nil || !opened.IsClosed() || conn.IsClosed() {
		t.Fatalf("setup cancellation=%v, channel=%v, connection closed=%v", err, opened, conn.IsClosed())
	}
}
