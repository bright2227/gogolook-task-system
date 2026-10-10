//go:build integration

package integration

import (
	"context"
	"database/sql/driver"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bright2227/gogolook-task-system/internal/usecase/port"

	deliveryamqp "github.com/bright2227/gogolook-task-system/internal/delivery/amqp"
	"github.com/bright2227/gogolook-task-system/internal/domain"
	"github.com/bright2227/gogolook-task-system/internal/usecase"
)

type failedOutcomeRepository struct {
	port.TaskRepository
	updates int
}

func (r *failedOutcomeRepository) UpdateByIDs(ctx context.Context, ids []domain.ID, update func([]*domain.Task) (bool, error)) error {
	r.updates++
	if r.updates == 2 {
		return driver.ErrBadConn
	}
	return r.TaskRepository.UpdateByIDs(ctx, ids, update)
}

func TestConsumerDatabaseNetworkFailureStopsBeforeAbandonedWorkIsAcknowledged(t *testing.T) {
	s := newIntegrationStack(t)
	client := s.client(t, "integration.database-failure")
	svc := newPublishingTaskUseCase(s, client, 8)
	created := submitTask(t, s.ctx, svc, "recoverable execution")
	if count, err := svc.PublishPending(s.ctx); err != nil || count != 0 || !created.IsSubmitted {
		t.Fatalf("initial publication=%d,%v", count, err)
	}
	executions := 0
	var execute usecase.Work = func(context.Context) error {
		executions++
		return nil
	}
	failed := &failedOutcomeRepository{TaskRepository: s.repo}
	consumer := client.consumer(t, s.ctx, usecase.NewTaskUseCase(failed, nil, time.Now), execute, retryDelays)
	// A failed outcome commit must stop this consumer, not reconnect and ACK a
	// redelivery whose persisted running flag still belongs to the failed run.
	ctx, cancel := context.WithTimeout(s.ctx, 3*time.Second)
	consumeErr := consumer.ConsumeBatch(ctx)
	cancel()
	var processingError *deliveryamqp.ProcessingError
	if !errors.Is(consumeErr, driver.ErrBadConn) || !errors.As(consumeErr, &processingError) {
		t.Fatalf("database network error did not stop consumer: %v", consumeErr)
	}
	client.restartConnection(t, s.ctx)
	if failed.updates != 2 || executions != 1 {
		t.Fatalf("consumer continued after failed commit: updates=%d executions=%d", failed.updates, executions)
	}
	state, err := s.repo.Get(s.ctx, created.ID)
	if err != nil || state.Status != domain.Processing || !state.IsRunning || state.Result != nil || state.Error != nil {
		t.Fatalf("failed outcome state=%+v,%v", state, err)
	}
	assertQueueCount(t, s.ctx, s, s.queue("integration.database-failure.tasks"), 1)
	// A redelivery never takes a running claim over; the sweeper releases it
	// once stale, spending one retry (here without backoff).
	ageClaims(t, s, created.ID)
	if n, err := s.repo.ReleaseStale(s.ctx, staleAfter, []time.Duration{0}); n != 1 || err != nil {
		t.Fatalf("stale release=%d,%v", n, err)
	}

	// The previous consumer has returned and released its broker channel, so
	// the replacement receives the message redelivered and claims the task.
	ctx, cancel = context.WithTimeout(s.ctx, 3*time.Second)
	defer cancel()
	checked := &committedOutcomeRepository{TaskRepository: s.repo, afterCommit: func(ctx context.Context, ids []domain.ID) error {
		return requireCommittedOutcomes(ctx, s.repo, ids)
	}}
	processed := make(chan struct{}, 1)
	settled := 0
	running := startBatchConsumer(t, ctx, client, usecase.NewTaskUseCase(checked, nil, time.Now), execute, retryDelays, func(ctx context.Context) error {
		if err := requireCommittedOutcomes(ctx, s.repo, []domain.ID{created.ID}); err != nil {
			return err
		}
		settled++
		processed <- struct{}{}
		return nil
	})
	select {
	case <-processed:
	case <-ctx.Done():
		t.Fatalf("replacement consumer did not finish: %v", ctx.Err())
	}
	waitForQueueDrained(t, ctx, s, s.queue("integration.database-failure.tasks"))
	if err := running.Stop(); err != nil {
		t.Fatalf("replacement consumer=%v", err)
	}
	state, err = s.repo.Get(s.ctx, created.ID)
	if err != nil || state.Status != domain.Completed || state.IsRunning || state.Version <= created.Version || state.Result == nil || *state.Result != "21" || state.Error != nil || state.RetryCount != 1 {
		t.Fatalf("recovered completion=%+v,%v", state, err)
	}
	if settled != 1 || checked.commits != 1 || executions != 2 {
		t.Fatalf("recovery batches=%d commits=%d executions=%d", settled, checked.commits, executions)
	}
	assertQueueCount(t, s.ctx, s, s.queue("integration.database-failure.tasks"), 0)
}

// A channel/session deadline is returned to the caller. It must not implicitly
// destroy another publisher or channel sharing the same connection.
func TestConsumerContextFailureLeavesConnectionLifetimeToCaller(t *testing.T) {
	s := newIntegrationStack(t)
	client := s.client(t, "integration.context-lifetime")
	conn, err := client.broker.Connection()
	if err != nil {
		t.Fatal(err)
	}
	consumer := client.consumer(t, s.ctx, usecase.NewTaskUseCase(s.repo, nil, time.Now), usecase.Delay(time.Millisecond), retryDelays)
	ctx, cancel := context.WithTimeout(s.ctx, 50*time.Millisecond)
	defer cancel()
	err = consumer.ConsumeBatch(ctx)
	var processingError *deliveryamqp.ProcessingError
	if !errors.Is(err, context.DeadlineExceeded) || errors.As(err, &processingError) || conn.IsClosed() {
		t.Fatalf("channel context must return without closing connection: err=%v closed=%v", err, conn.IsClosed())
	}
	if publication, err := client.Enqueue(s.ctx, domain.NewID()); err != nil || publication.Err != nil {
		t.Fatalf("shared publisher lost its connection: publication=%+v err=%v", publication, err)
	}
	// The upper owner can now independently choose to retire the connection.
	if err := client.broker.Close(); err != nil || !conn.IsClosed() {
		t.Fatalf("owner close: err=%v closed=%v", err, conn.IsClosed())
	}
}

// A consumer that dies holding a claim closes its channel without an ACK, so
// RabbitMQ redelivers the message. The redelivery does not prove that the
// claim's holder is gone, so the next consumer skips the running row and ACKs.
// Once the claim is stale the sweeper releases it (one retry), republishes it
// when due, and another consumer completes it; the dead holder's late outcome
// is discarded.
func TestRedeliveryLeavesCrashedClaimToStaleRelease(t *testing.T) {
	s := newIntegrationStack(t)
	client := s.client(t, "integration.crashed")
	svc := newPublishingTaskUseCase(s, client, 8)
	created := submitTask(t, s.ctx, svc, "crashed")
	claimed := make(chan struct{})
	resume := make(chan struct{})
	resumeCrashed := sync.OnceFunc(func() { close(resume) })
	defer resumeCrashed()
	crashed := client.consumer(t, s.ctx, usecase.NewTaskUseCase(s.repo, nil, time.Now), func(ctx context.Context) error {
		close(claimed)
		select {
		case <-resume:
		case <-ctx.Done():
		}
		return ctx.Err()
	}, retryDelays)
	crashedDone := make(chan error, 1)
	go func() { crashedDone <- crashed.ConsumeBatch(s.ctx) }()
	select {
	case <-claimed:
	case <-time.After(5 * time.Second):
		t.Fatal("first consumer did not claim the task")
	}
	// The holder dies: its connection closes without an ACK.
	if err := client.broker.Close(); err != nil {
		t.Fatal(err)
	}
	held, err := s.repo.Get(s.ctx, created.ID)
	if err != nil || !held.IsRunning {
		t.Fatalf("crashed claim=%+v,%v", held, err)
	}
	batches := make(chan struct{}, 1)
	var executions atomic.Int32
	replacement := startBatchConsumer(t, s.ctx, client, usecase.NewTaskUseCase(s.repo, nil, time.Now), func(ctx context.Context) error {
		executions.Add(1)
		return ctx.Err()
	}, retryDelays, func(context.Context) error {
		select {
		case batches <- struct{}{}:
		default:
		}
		return nil
	})
	select {
	case <-batches:
	case <-time.After(10 * time.Second):
		t.Fatal("redelivered message was not processed")
	}
	waitForQueueDrained(t, s.ctx, s, s.queue("integration.crashed.tasks"))
	if state, err := s.repo.Get(s.ctx, created.ID); err != nil || !state.IsRunning || state.Version != held.Version || executions.Load() != 0 {
		t.Fatalf("redelivery took the claim over=%+v,%v executions=%d", state, err, executions.Load())
	}
	// The claim goes stale; the sweeper (on a live connection) releases it,
	// then republishes it once its one-second retry delay has passed.
	ageClaims(t, s, created.ID)
	sweeper := newPublishingTaskUseCase(s, s.process(t, client), 8)
	deadline := time.Now().Add(15 * time.Second)
	var state *domain.Task
	for {
		if _, err := sweeper.PublishPending(s.ctx); err != nil {
			t.Fatal(err)
		}
		if state, err = s.repo.Get(s.ctx, created.ID); err != nil {
			t.Fatal(err)
		}
		if state.Finished() || time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if state.Status != domain.Completed || state.Result == nil || *state.Result != "7" || state.RetryCount != 1 || executions.Load() != 1 {
		t.Fatalf("recovered=%+v executions=%d", state, executions.Load())
	}
	// The dead holder's late outcome no longer matches the row's version.
	resumeCrashed()
	select {
	case err := <-crashedDone:
		if !errors.Is(err, domain.ErrNetwork) {
			t.Fatalf("crashed consumer ACK on a closed connection=%v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("crashed consumer did not return")
	}
	if after, err := s.repo.Get(s.ctx, created.ID); err != nil || after.Version != state.Version {
		t.Fatalf("late outcome overwrote the recovery=%+v,%v", after, err)
	}
	waitForQueueDrained(t, s.ctx, s, s.queue("integration.crashed.tasks"))
	if err := replacement.Stop(); err != nil {
		t.Fatal(err)
	}
}

// ageClaims stamps running claims one hour behind the database clock.
func ageClaims(t *testing.T, s integrationStack, ids ...domain.ID) {
	t.Helper()
	for _, id := range ids {
		if _, err := s.db.ExecContext(s.ctx, `UPDATE tasks SET created_at=LEAST(created_at,now()-interval '1 hour'),updated_at=now()-interval '1 hour' WHERE id=$1 AND is_running`, id); err != nil {
			t.Fatal(err)
		}
	}
}

// The sweeper releases a running claim not updated for StaleAfter (its
// consumer is gone) and republishes it once due (here at once: the sweeper's
// clock runs an hour ahead); a fresh running claim is left alone even when the sweeper's clock
// runs ahead.
func TestSweeperReleasesOnlyStaleRunningClaims(t *testing.T) {
	s := newIntegrationStack(t)
	client := s.client(t, "integration.stale")
	submitter := usecase.NewTaskUseCase(s.repo, sweeperOnly, time.Now)
	stale := submitTask(t, s.ctx, submitter, "stale")
	fresh := submitTask(t, s.ctx, submitter, "fresh")
	for _, item := range []*domain.Task{stale, fresh} {
		if err := s.repo.UpdateByID(s.ctx, item.ID, item.Version, func(t *domain.Task) (bool, error) { return t.Start(time.Now()) }); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.db.ExecContext(s.ctx, `UPDATE tasks SET created_at=created_at-interval '1 hour',updated_at=updated_at-interval '1 hour',next_retry=next_retry-interval '1 hour' WHERE id=$1`, stale.ID); err != nil {
		t.Fatal(err)
	}
	live, err := s.repo.Get(s.ctx, fresh.ID)
	if err != nil {
		t.Fatal(err)
	}
	publication := newPublishingTaskUseCase(s, client, 8)
	// Staleness is judged by the database clock: a sweeper clock an hour
	// ahead must not release the fresh claim.
	publication.Clock = func() time.Time { return time.Now().Add(time.Hour) }
	if n, err := publication.PublishPending(s.ctx); n != 1 || err != nil {
		t.Fatalf("stale sweep=%d,%v", n, err)
	}
	released, err := s.repo.Get(s.ctx, stale.ID)
	if err != nil || released.Status != domain.Pending || released.IsRunning || !released.IsSubmitted || released.RetryCount != 1 || released.Version != 3 {
		t.Fatalf("released=%+v,%v", released, err)
	}
	if state, err := s.repo.Get(s.ctx, fresh.ID); err != nil || !state.IsRunning || state.RetryCount != 0 || state.Version != live.Version {
		t.Fatalf("fresh claim released=%+v,%v", state, err)
	}
	assertQueueCount(t, s.ctx, s, s.queue("integration.stale.tasks"), 1)
}

// Consumers may overlap (scale-out or rolling restart): starting another one
// releases nothing, so the first keeps its live claim and both make progress.
func TestSecondConsumerLeavesLiveClaimAlone(t *testing.T) {
	s := newIntegrationStack(t)
	client := s.client(t, "integration.two-consumers")
	svc := newPublishingTaskUseCase(s, client, 8)
	held := submitTask(t, s.ctx, svc, "held")
	started := make(chan struct{})
	release := make(chan struct{})
	releaseHeld := sync.OnceFunc(func() { close(release) })
	defer releaseHeld()
	signalStarted := sync.OnceFunc(func() { close(started) })
	// Prefetch 1 keeps the busy first consumer from buffering the next message.
	busy := usecase.NewTaskUseCase(s.repo, nil, time.Now)
	busy.Parallelism = 1
	first := startBatchConsumer(t, s.ctx, client, busy, func(ctx context.Context) error {
		signalStarted()
		select {
		case <-release:
		case <-ctx.Done():
		}
		return ctx.Err()
	}, retryDelays, nil)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("first consumer did not claim the task")
	}
	claimed, err := s.repo.Get(s.ctx, held.ID)
	if err != nil || !claimed.IsRunning {
		t.Fatalf("first claim=%+v,%v", claimed, err)
	}
	processed := make(chan struct{}, 1)
	// The second consumer is a separate process with its own connection.
	peer := s.process(t, client)
	second := startBatchConsumer(t, s.ctx, peer, usecase.NewTaskUseCase(s.repo, nil, time.Now), func(ctx context.Context) error { return ctx.Err() }, retryDelays, func(ctx context.Context) error {
		select {
		case processed <- struct{}{}:
		case <-ctx.Done():
		}
		return nil
	})
	other := submitTask(t, s.ctx, svc, "other")
	select {
	case <-processed:
	case <-time.After(10 * time.Second):
		t.Fatal("second consumer did not process its task")
	}
	if state := waitForStatus(t, s.ctx, svc, other.ID, domain.Completed); state.RetryCount != 0 {
		t.Fatalf("second consumer task=%+v", state)
	}
	if state, err := s.repo.Get(s.ctx, held.ID); err != nil || !state.IsRunning || state.Version != claimed.Version || state.RetryCount != 0 {
		t.Fatalf("live claim released=%+v,%v", state, err)
	}
	releaseHeld()
	if state := waitForStatus(t, s.ctx, svc, held.ID, domain.Completed); state.RetryCount != 0 || state.Result == nil || *state.Result != "4" {
		t.Fatalf("first consumer task=%+v", state)
	}
	if err := first.Stop(); err != nil {
		t.Fatal(err)
	}
	// Stopping the first process (and its connection) leaves the second
	// consuming; publish through the second process's connection.
	third := submitTask(t, s.ctx, newPublishingTaskUseCase(s, peer, 8), "third")
	if !third.IsSubmitted {
		t.Fatalf("third submission=%+v", third)
	}
	if state := waitForStatus(t, s.ctx, svc, third.ID, domain.Completed); state.RetryCount != 0 {
		t.Fatalf("second consumer task after first stop=%+v", state)
	}
	if err := second.Stop(); err != nil {
		t.Fatal(err)
	}
}
