//go:build integration

package integration

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bright2227/gogolook-task-system/internal/delivery"
	"github.com/bright2227/gogolook-task-system/internal/domain"
	"github.com/bright2227/gogolook-task-system/internal/infrastructure/rabbitmq"
	"github.com/bright2227/gogolook-task-system/internal/usecase"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestPublisherRejectsUnroutableMessageAndReconnects(t *testing.T) {
	s := newIntegrationStack(t)
	client := s.client(t, "integration.publisher")
	queue := s.queue("integration.publisher.tasks")
	conn, err := amqp.DialConfig(s.brokerURL, amqp.Config{Dial: amqp.DefaultDial(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseDeadline(time.Now().Add(time.Second))
	stop := rabbitmq.CloseConnectionOnCancel(s.ctx, conn)
	defer stop()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	id := domain.NewID()
	if publication, err := client.Enqueue(s.ctx, id); err != nil || publication.ID != id || publication.Err != nil {
		t.Fatalf("publication=%+v, err=%v", publication, err)
	}
	if publication, err := client.Enqueue(s.ctx, "invalid"); err != nil || publication.ID != "invalid" || publication.Err == nil {
		t.Fatalf("invalid ID publication=%+v, err=%v", publication, err)
	}
	if err := ch.QueueUnbind(queue, queue, rabbitmq.DefaultExchange, nil); err != nil {
		t.Fatal(err)
	}
	// Exercise both select orderings when the return and confirmation are ready.
	for i := 0; i < 32; i++ {
		id := domain.NewID()
		publication, err := client.Enqueue(s.ctx, id)
		if err != nil || publication.ID != id || publication.Err == nil || !strings.Contains(publication.Err.Error(), "message returned by broker") || errors.Is(publication.Err, domain.ErrNetwork) {
			t.Fatalf("mandatory return %d=%+v, err=%v", i, publication, err)
		}
	}
	// The use case sees the same message-local results through its port.
	var port usecase.TaskQueue = client.TaskQueue
	ids := []domain.ID{domain.NewID(), domain.NewID(), domain.NewID()}
	for i, id := range ids {
		publication, err := port.Enqueue(s.ctx, id)
		if err != nil || publication.ID != id || publication.Err == nil || !strings.Contains(publication.Err.Error(), "message returned by broker") {
			t.Fatalf("unroutable port publication %d=%+v, err=%v", i, publication, err)
		}
	}
	if err := ch.QueueBind(queue, queue, rabbitmq.DefaultExchange, false, nil); err != nil {
		t.Fatal(err)
	}
	// Mandatory returns leave the initialized channel usable after routing is repaired.
	if publication, err := client.Enqueue(s.ctx, domain.NewID()); err != nil || publication.Err != nil {
		t.Fatalf("publish after routing repair=%+v, err=%v", publication, err)
	}
	ctx, cancel := context.WithCancel(s.ctx)
	cancel()
	if publication, err := client.Enqueue(ctx, domain.NewID()); !errors.Is(err, context.Canceled) || publication.ID != "" || publication.Err != nil {
		t.Fatalf("canceled publish=%+v, err=%v", publication, err)
	}
	if publication, err := client.Enqueue(s.ctx, domain.NewID()); err != nil || publication.Err != nil {
		t.Fatalf("publish after cancellation=%+v, err=%v", publication, err)
	}
	// Connection recovery belongs to the caller. A closed connection must fail
	// without a hidden dial; explicitly inject and initialize its replacement.
	previous, err := client.broker.Connection()
	if err != nil {
		t.Fatal(err)
	}
	_ = previous.CloseDeadline(time.Now())
	if publication, err := client.Enqueue(s.ctx, domain.NewID()); !errors.Is(err, domain.ErrNetwork) || publication.ID != "" || publication.Err != nil {
		t.Fatalf("closed connection publication=%+v, err=%v", publication, err)
	}
	if publication, err := port.Enqueue(s.ctx, id); !errors.Is(err, domain.ErrNetwork) || publication.ID != "" || publication.Err != nil {
		t.Fatalf("closed connection port publication=%+v, err=%v", publication, err)
	}
	replacement, err := client.broker.Connection()
	if err != nil {
		t.Fatal(err)
	}
	stopReplacement := rabbitmq.CloseConnectionOnCancel(s.ctx, replacement)
	t.Cleanup(stopReplacement)
	channel, err := replacement.Channel()
	if err != nil {
		t.Fatal(err)
	}
	client.TaskQueue = rabbitmq.NewTaskQueue(channel, queue)
	if err := client.TaskQueue.Initialization(); err != nil {
		t.Fatal(err)
	}
	if publication, err := client.Enqueue(s.ctx, domain.NewID()); err != nil || publication.Err != nil {
		t.Fatalf("explicit reconnection=%+v, err=%v", publication, err)
	}
	assertQueueCount(t, s.ctx, s, queue, 4)
}

// blockedPublishConn uses the real broker for setup, then writes into an unread
// pipe. Deadlines and Close reach both sockets so cancellation can release Write.
type blockedPublishConn struct {
	net.Conn
	blocked net.Conn
	sink    net.Conn
	block   atomic.Bool
	started chan struct{}
}

func (c *blockedPublishConn) Write(body []byte) (int, error) {
	if !c.block.Load() {
		return c.Conn.Write(body)
	}
	select {
	case c.started <- struct{}{}:
	default:
	}
	return c.blocked.Write(body)
}
func (c *blockedPublishConn) SetDeadline(deadline time.Time) error {
	return errors.Join(c.Conn.SetDeadline(deadline), c.blocked.SetDeadline(deadline))
}
func (c *blockedPublishConn) SetWriteDeadline(deadline time.Time) error {
	return errors.Join(c.Conn.SetWriteDeadline(deadline), c.blocked.SetWriteDeadline(deadline))
}
func (c *blockedPublishConn) Close() error {
	return errors.Join(c.Conn.Close(), c.blocked.Close(), c.sink.Close())
}

func TestPublisherDeadlineReleasesBlockedSocketWrite(t *testing.T) {
	s := newIntegrationStack(t)
	s.client(t, "integration.blocked-publisher") // Declare its isolated topology.
	var transport *blockedPublishConn
	conn, err := amqp.DialConfig(s.brokerURL, amqp.Config{Dial: func(network, address string) (net.Conn, error) {
		raw, err := amqp.DefaultDial(time.Second)(network, address)
		if err != nil {
			return nil, err
		}
		blocked, sink := net.Pipe()
		transport = &blockedPublishConn{Conn: raw, blocked: blocked, sink: sink, started: make(chan struct{}, 1)}
		return transport, nil
	}})
	if transport != nil {
		t.Cleanup(func() { _ = transport.Close() })
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.CloseDeadline(time.Now()) })
	stopFixture := rabbitmq.CloseConnectionOnCancel(s.ctx, conn)
	defer stopFixture()
	channel, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	p := rabbitmq.NewTaskQueue(channel, s.queue("integration.blocked-publisher.tasks"))
	if err := p.Initialization(); err != nil {
		t.Fatal(err)
	}
	transport.block.Store(true)
	ctx, cancel := context.WithTimeout(s.ctx, 50*time.Millisecond)
	defer cancel()
	// The connection owner chooses to abort the socket on this operation's
	// deadline; TaskQueue itself only returns the resulting error.
	stopConnection := rabbitmq.CloseConnectionOnCancel(ctx, conn)
	defer stopConnection()
	started := time.Now()
	done := make(chan error, 1)
	go func() {
		publication, err := p.Enqueue(ctx, domain.NewID())
		if publication.ID != "" || publication.Err != nil {
			done <- errors.New("blocked publication returned a message-local result")
			return
		}
		done <- err
	}()
	select {
	case err := <-done:
		// The owner's deadline either wins the wait or aborts the socket first.
		if !errors.Is(err, context.DeadlineExceeded) && !delivery.IsNetworkError(err) {
			t.Fatalf("blocked publication must honor its caller deadline: %v", err)
		}
	case <-time.After(time.Second):
		_ = transport.Close()
		t.Fatal("publication remained blocked after its caller deadline")
	}
	// Join the owner's asynchronous cancellation cleanup before asserting
	// resource state, just as the command does after Enqueue returns.
	stopConnection()
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("blocked publication took %v", elapsed)
	}
	select {
	case <-transport.started:
	default:
		t.Fatal("test never blocked a socket write")
	}
	if !conn.IsClosed() {
		t.Fatal("deadline did not force the AMQP connection closed")
	}
}

// A message lost by the broker (queue purged or deleted) is recovered by the
// optional safety net: the sweeper republishes a task still pending and
// unclaimed after RepublishAfter, and a consumer completes it.
func TestSweeperRepublishesLostMessage(t *testing.T) {
	s := newIntegrationStack(t)
	client := s.client(t, "integration.expired")
	queue := s.queue("integration.expired.tasks")
	created := submitTask(t, s.ctx, usecase.NewTaskUseCase(s.repo, sweeperOnly, time.Now), "expired")
	now := time.Now().UTC().Truncate(time.Microsecond)
	publication := newPublishingTaskUseCase(s, client, 8)
	publication.Clock = func() time.Time { return now }
	now = now.Add(publication.PublishGrace)
	if n, err := publication.PublishPending(s.ctx); n != 1 || err != nil {
		t.Fatalf("sweeper publication=%d,%v", n, err)
	}
	assertQueueCount(t, s.ctx, s, queue, 1)
	// Purging simulates a message lost by the broker.
	conn, err := amqp.DialConfig(s.brokerURL, amqp.Config{Dial: amqp.DefaultDial(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseDeadline(time.Now().Add(time.Second))
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	if purged, err := ch.QueuePurge(queue, false); err != nil || purged != 1 {
		t.Fatalf("purge=%d,%v", purged, err)
	}
	if n, err := publication.PublishPending(s.ctx); n != 0 || err != nil {
		t.Fatalf("republished before RepublishAfter=%d,%v", n, err)
	}
	now = now.Add(publication.RepublishAfter)
	if n, err := publication.PublishPending(s.ctx); n != 1 || err != nil {
		t.Fatalf("republication=%d,%v", n, err)
	}
	assertQueueCount(t, s.ctx, s, queue, 1)
	stored, err := s.repo.Get(s.ctx, created.ID)
	if err != nil || !stored.IsSubmitted || stored.Version != created.Version || !stored.UpdatedAt.Equal(now) {
		t.Fatalf("republished task=%+v,%v", stored, err)
	}
	worker := usecase.NewTaskUseCase(s.repo, nil, time.Now)
	consumer := startBatchConsumer(t, s.ctx, client, worker, usecase.Delay(time.Millisecond), retryDelays, nil)
	if state := waitForStatus(t, s.ctx, worker, created.ID, domain.Completed); state.Result == nil || *state.Result != "7" {
		t.Fatalf("completion=%+v", state)
	}
	if err := consumer.Stop(); err != nil {
		t.Fatal(err)
	}
}

// A message delivered before its task is due (clock skew between publisher and
// consumer) is ACKed and the submitted task handed back to the sweeper with a
// Version bump, which publishes it again once due, with the submitted-task
// safety net disabled.
func TestNotDueDeliveryIsRepublishedWhenDue(t *testing.T) {
	s := newIntegrationStack(t)
	client := s.client(t, "integration.notdue")
	queue := s.queue("integration.notdue.tasks")
	created := submitTask(t, s.ctx, usecase.NewTaskUseCase(s.repo, sweeperOnly, time.Now), "not due")
	due := time.Now().Add(2 * time.Second)
	// A retry scheduled in the future that a skewed publisher already published.
	err := s.repo.UpdateByID(s.ctx, created.ID, created.Version, func(task *domain.Task) (bool, error) {
		now := time.Now()
		if _, err := task.Start(now); err != nil {
			return false, err
		}
		if _, err := task.Retry(now, due); err != nil {
			return false, err
		}
		task.MarkSubmitted(now)
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	scheduled, err := s.repo.Get(s.ctx, created.ID)
	if err != nil || !scheduled.IsSubmitted {
		t.Fatalf("scheduled=%+v,%v", scheduled, err)
	}
	if publication, err := client.Enqueue(s.ctx, created.ID); err != nil || publication.Err != nil {
		t.Fatalf("early publication=%+v,%v", publication, err)
	}
	executions := atomic.Int32{}
	worker := usecase.NewTaskUseCase(s.repo, nil, time.Now)
	consumer := startBatchConsumer(t, s.ctx, client, worker, func(context.Context) error { executions.Add(1); return nil }, retryDelays, nil)
	deadline := time.Now().Add(time.Second)
	for {
		stored, err := s.repo.Get(s.ctx, created.ID)
		if err != nil {
			t.Fatal(err)
		}
		if !stored.IsSubmitted {
			if stored.Status != domain.Pending || stored.Version != scheduled.Version+1 || executions.Load() != 0 {
				t.Fatalf("handed back=%+v executions=%d", stored, executions.Load())
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("not-due task stayed submitted=%+v", stored)
		}
		time.Sleep(20 * time.Millisecond)
	}
	waitForQueueDrained(t, s.ctx, s, queue)
	publication := newPublishingTaskUseCase(s, client, 8)
	publication.RepublishAfter = 0
	if n, err := publication.PublishPending(s.ctx); n != 0 || err != nil {
		t.Fatalf("published before due=%d,%v", n, err)
	}
	time.Sleep(time.Until(due))
	if n, err := publication.PublishPending(s.ctx); n != 1 || err != nil {
		t.Fatalf("publication when due=%d,%v", n, err)
	}
	if state := waitForStatus(t, s.ctx, worker, created.ID, domain.Completed); state.Result == nil || *state.Result != "7" || executions.Load() != 1 {
		t.Fatalf("completion=%+v executions=%d", state, executions.Load())
	}
	if err := consumer.Stop(); err != nil {
		t.Fatal(err)
	}
}
