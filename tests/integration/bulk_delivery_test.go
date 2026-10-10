//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bright2227/gogolook-task-system/internal/usecase/port"

	deliveryamqp "github.com/bright2227/gogolook-task-system/internal/delivery/amqp"
	"github.com/bright2227/gogolook-task-system/internal/domain"
	"github.com/bright2227/gogolook-task-system/internal/usecase"
)

// committedOutcomeRepository observes the consumer's process step: afterCommit
// runs once an UpdateByIDs call that ended a claim (a locked row running before
// the callback and not after) has committed, which ProcessTasks does for its
// outcome write before any ACK. A claim commit leaves the row running and is
// not counted.
type committedOutcomeRepository struct {
	port.TaskRepository
	afterCommit func(context.Context, []domain.ID) error
	commits     int
}

func (r *committedOutcomeRepository) UpdateByIDs(ctx context.Context, ids []domain.ID, update func([]*domain.Task) (bool, error)) error {
	outcome := false
	err := r.TaskRepository.UpdateByIDs(ctx, ids, func(items []*domain.Task) (bool, error) {
		running := make([]bool, len(items))
		for i, item := range items {
			running[i] = item.IsRunning
		}
		changed, err := update(items)
		for i, item := range items {
			outcome = outcome || (running[i] && !item.IsRunning)
		}
		return changed, err
	})
	if err != nil || !outcome {
		return err
	}
	r.commits++
	return r.afterCommit(ctx, ids)
}

func requireCommittedOutcomes(ctx context.Context, repo port.TaskRepository, ids []domain.ID) error {
	for _, id := range ids {
		item, err := repo.Get(ctx, id)
		if err != nil || item.Status != domain.Completed || item.Result == nil {
			return fmt.Errorf("ACK before committed outcome: %+v, %v", item, err)
		}
	}
	return nil
}

func TestBulkCommitBeforeAckAndRedelivery(t *testing.T) {
	s := newIntegrationStack(t)
	client := s.client(t, "integration.bulk")
	svc := newPublishingTaskUseCase(s, client, 2)
	first := submitTask(t, s.ctx, svc, "first")
	second := submitTask(t, s.ctx, svc, "second")
	// Submit published both; the sweeper has nothing left.
	if n, err := svc.PublishPending(s.ctx); n != 0 || err != nil || !first.IsSubmitted || !second.IsSubmitted {
		t.Fatalf("publish=%d,%v", n, err)
	}
	// PostgreSQL rejects the outcome write: the entire batch must roll back and
	// RabbitMQ must receive no ACK, leaving both messages to be redelivered.
	_, err := s.db.ExecContext(s.ctx, `CREATE FUNCTION reject_completion() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.status = 1 THEN RAISE EXCEPTION 'outcome commit unavailable'; END IF; RETURN NEW; END $$; CREATE TRIGGER reject_completion BEFORE UPDATE ON tasks FOR EACH ROW EXECUTE FUNCTION reject_completion()`)
	if err != nil {
		t.Fatal(err)
	}
	// executeTasks runs claimed tasks concurrently.
	var executions atomic.Int32
	var execute usecase.Work = func(context.Context) error {
		executions.Add(1)
		return nil
	}
	consumer := client.consumer(t, s.ctx, usecase.NewTaskUseCase(s.repo, nil, time.Now), execute, retryDelays)
	var processingError *deliveryamqp.ProcessingError
	if err := consumer.ConsumeBatch(s.ctx); err == nil || !strings.Contains(err.Error(), "outcome commit unavailable") || !errors.As(err, &processingError) {
		t.Fatalf("failed commit=%v", err)
	}
	client.restartConnection(t, s.ctx)
	for _, id := range []domain.ID{first.ID, second.ID} {
		item, err := s.repo.Get(s.ctx, id)
		if err != nil || item.Status != domain.Processing || !item.IsRunning || item.Result != nil {
			t.Fatalf("partial outcome committed: %+v,%v", item, err)
		}
	}
	assertQueueCount(t, s.ctx, s, s.queue("integration.bulk.tasks"), 2)
	if _, err = s.db.ExecContext(s.ctx, `DROP TRIGGER reject_completion ON tasks; DROP FUNCTION reject_completion()`); err != nil {
		t.Fatal(err)
	}
	// A redelivery never takes a running claim over: the sweeper releases the
	// stranded claims once stale, spending one retry (here without backoff).
	ageClaims(t, s, first.ID, second.ID)
	if n, err := s.repo.ReleaseStale(s.ctx, staleAfter, []time.Duration{0}); n != 2 || err != nil {
		t.Fatalf("stale release=%d,%v", n, err)
	}
	// The redelivered messages claim the released tasks. Their commit
	// succeeds but ACK is lost. A further redelivery must inspect the committed
	// task rows and avoid repeating their executor calls.
	checked := &committedOutcomeRepository{TaskRepository: s.repo, afterCommit: func(ctx context.Context, ids []domain.ID) error {
		if err := requireCommittedOutcomes(ctx, s.repo, ids); err != nil {
			return err
		}
		// Processing succeeded, but the real consumer cannot ACK this connection.
		return client.broker.Close()
	}}
	svc = usecase.NewTaskUseCase(checked, nil, time.Now)
	consumer = client.consumer(t, s.ctx, svc, execute, retryDelays)
	processingError = nil
	if err := consumer.ConsumeBatch(s.ctx); !errors.Is(err, domain.ErrNetwork) || errors.As(err, &processingError) {
		t.Fatalf("lost ACK=%v", err)
	}
	client.restartConnection(t, s.ctx)
	if checked.commits != 1 || executions.Load() != 4 {
		t.Fatalf("outcome commits=%d executions=%d", checked.commits, executions.Load())
	}
	assertQueueCount(t, s.ctx, s, s.queue("integration.bulk.tasks"), 2)
	checked.afterCommit = func(context.Context, []domain.ID) error {
		return errors.New("redelivery committed another outcome")
	}
	processed := make(chan struct{}, 1)
	settled := 0
	running := startBatchConsumer(t, s.ctx, client, svc, execute, retryDelays, func(ctx context.Context) error {
		if err := requireCommittedOutcomes(ctx, s.repo, []domain.ID{first.ID, second.ID}); err != nil {
			return err
		}
		settled++
		processed <- struct{}{}
		return nil
	})
	select {
	case <-processed:
	case <-time.After(time.Second):
		t.Fatal("redelivered batch did not finish")
	}
	waitForQueueDrained(t, s.ctx, s, s.queue("integration.bulk.tasks"))
	if err := running.Stop(); err != nil {
		t.Fatalf("consumer stop=%v", err)
	}
	if settled != 1 || checked.commits != 1 || executions.Load() != 4 {
		t.Fatalf("redelivery repeated completed work: batches=%d commits=%d executions=%d", settled, checked.commits, executions.Load())
	}
	// The owner's connection is closed: unacknowledged deliveries would requeue.
	assertQueueCount(t, s.ctx, s, s.queue("integration.bulk.tasks"), 0)
	for _, id := range []domain.ID{first.ID, second.ID} {
		if item, err := s.repo.Get(s.ctx, id); err != nil || item.RetryCount != 1 {
			t.Fatalf("recovered state=%+v,%v", item, err)
		}
	}
}
