package usecase_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bright2227/vsx-task-system/internal/domain"
	"github.com/bright2227/vsx-task-system/internal/repository/task/memory"
	"github.com/bright2227/vsx-task-system/internal/usecase"
)

type processor func(context.Context, domain.Payload) (string, error)

func (p processor) Execute(ctx context.Context, in domain.Payload) (string, error) { return p(ctx, in) }

func TestSubmitAndConsumerPersistLifecycle(t *testing.T) {
	now := time.Now().UTC()
	store := memory.New()
	clock := func() time.Time { now = now.Add(time.Millisecond); return now }
	service := usecase.Tasks{Store: store, Clock: clock}
	created, err := service.Submit(context.Background(), usecase.SubmitTaskRequest{Payload: []byte(`{"amount":7}`)})
	if err != nil {
		t.Fatal(err)
	}
	if created.Status != string(domain.Pending) || len(store.Events) != 1 || len(store.Outbox) != 1 {
		t.Fatalf("submit did not persist event/outbox: state=%+v events=%d outbox=%d", created, len(store.Events), len(store.Outbox))
	}
	var calls atomic.Int32
	consumer := usecase.Consumer{Store: store, Clock: clock, Lease: time.Second, Timeout: time.Second, Processor: processor(func(ctx context.Context, p domain.Payload) (string, error) { calls.Add(1); return `{"ok":true}`, nil })}
	if err := consumer.Process(context.Background(), usecase.ProcessTaskRequest{ID: created.ID}); err != nil {
		t.Fatal(err)
	}
	final, err := service.Get(context.Background(), usecase.TaskRequest{ID: created.ID})
	if err != nil || final.Status != string(domain.Completed) || final.Attempts != 1 || final.Result == nil || calls.Load() != 1 {
		t.Fatalf("final=%+v calls=%d err=%v", final, calls.Load(), err)
	}
	if len(store.Events) != 3 {
		t.Fatalf("expected submitted, started, completed events; got %d", len(store.Events))
	}
}

func TestStoreTransactionRollsBackWhenCallbackFails(t *testing.T) {
	store := memory.New()
	p, _ := domain.NewPayload([]byte(`{}`))
	tsk, _ := domain.Submit(p, time.Now())
	want := errors.New("simulated database failure")
	err := store.Within(context.Background(), func(r usecase.Repository) error {
		if err := r.Insert(context.Background(), tsk); err != nil {
			return err
		}
		return want
	})
	if !errors.Is(err, want) {
		t.Fatalf("Within error=%v", err)
	}
	if _, err = store.Get(context.Background(), tsk.Snapshot().ID); !errors.Is(err, usecase.ErrNotFound) {
		t.Fatalf("rolled-back task lookup error=%v", err)
	}
}

func TestConsumerPersistsBusinessErrorAndLeavesInterruptedWorkRecoverable(t *testing.T) {
	store := memory.New()
	service := usecase.Tasks{Store: store, Clock: time.Now}
	created, err := service.Submit(context.Background(), usecase.SubmitTaskRequest{Payload: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	consumer := usecase.Consumer{Store: store, Clock: time.Now, Lease: time.Second, Timeout: time.Second, Processor: processor(func(context.Context, domain.Payload) (string, error) {
		return "", errors.New("private processor detail")
	})}
	if err = consumer.Process(context.Background(), usecase.ProcessTaskRequest{ID: created.ID}); err != nil {
		t.Fatal(err)
	}
	failed, err := service.Get(context.Background(), usecase.TaskRequest{ID: created.ID})
	if err != nil || failed.Status != string(domain.Completed) || failed.Error == nil || *failed.Error != "task execution failed" {
		t.Fatalf("business failure state=%+v err=%v", failed, err)
	}

	interrupted, err := service.Submit(context.Background(), usecase.SubmitTaskRequest{Payload: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	blocking := usecase.Consumer{Store: store, Clock: time.Now, Lease: time.Second, Timeout: time.Second, Processor: processor(func(ctx context.Context, _ domain.Payload) (string, error) {
		close(started)
		<-ctx.Done()
		return "", ctx.Err()
	})}
	done := make(chan error, 1)
	go func() { done <- blocking.Process(ctx, usecase.ProcessTaskRequest{ID: interrupted.ID}) }()
	<-started
	cancel()
	if err = <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("interrupted Process error=%v", err)
	}
	stored, err := store.Get(context.Background(), domain.ID(interrupted.ID))
	if err != nil {
		t.Fatal(err)
	}
	recoverable := stored.Snapshot()
	if recoverable.Status != domain.Processing || recoverable.LeaseUntil == nil {
		t.Fatalf("interrupted work state=%+v err=%v", recoverable, err)
	}
}

func TestUseCasesValidateInputsAndReturnZeroResponseOnInsertFailure(t *testing.T) {
	store := memory.New()
	service := usecase.Tasks{Store: store, Clock: time.Now}
	if _, err := service.Submit(context.Background(), usecase.SubmitTaskRequest{Payload: []byte(`{`)}); !errors.Is(err, usecase.ErrInvalidPayload) {
		t.Fatalf("invalid payload error=%v", err)
	}
	if _, err := service.Get(context.Background(), usecase.TaskRequest{ID: "bad"}); !errors.Is(err, usecase.ErrInvalidID) {
		t.Fatalf("invalid get ID error=%v", err)
	}
	if _, err := service.Cancel(context.Background(), usecase.TaskRequest{ID: "bad"}); !errors.Is(err, usecase.ErrInvalidID) {
		t.Fatalf("invalid cancel ID error=%v", err)
	}
	consumer := usecase.Consumer{Store: store, Clock: time.Now, Processor: processor(func(context.Context, domain.Payload) (string, error) { return "", nil })}
	if err := consumer.Process(context.Background(), usecase.ProcessTaskRequest{ID: "bad"}); !errors.Is(err, usecase.ErrInvalidID) {
		t.Fatalf("invalid process ID error=%v", err)
	}
	if _, err := service.Submit(context.Background(), usecase.SubmitTaskRequest{Payload: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	// A duplicate event or storage failure is injected through a Store wrapper
	// below; this confirms Submit never returns an uncommitted generated ID.
	failing := failInsertStore{Store: store, err: errors.New("insert failed")}
	created, err := (usecase.Tasks{Store: failing, Clock: time.Now}).Submit(context.Background(), usecase.SubmitTaskRequest{Payload: []byte(`{}`)})
	if err == nil || created.ID != "" || created.Payload != nil || created.Status != "" || created.Result != nil || created.Error != nil || !created.CreatedAt.IsZero() || !created.UpdatedAt.IsZero() || created.Attempts != 0 {
		t.Fatalf("failed submit response=%+v err=%v", created, err)
	}
}

type failInsertStore struct {
	usecase.Store
	err error
}

func (s failInsertStore) Within(ctx context.Context, fn func(usecase.Repository) error) error {
	return s.Store.Within(ctx, func(r usecase.Repository) error { return fn(failInsertRepository{Repository: r, err: s.err}) })
}

type failInsertRepository struct {
	usecase.Repository
	err error
}

func (r failInsertRepository) Insert(context.Context, *domain.Task) error { return r.err }

func TestTaskResponseDoesNotShareMutableValues(t *testing.T) {
	store := memory.New()
	service := usecase.Tasks{Store: store, Clock: time.Now}
	created, err := service.Submit(context.Background(), usecase.SubmitTaskRequest{Payload: []byte(`{"x":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	created.Payload[0] = '['
	again, err := service.Get(context.Background(), usecase.TaskRequest{ID: created.ID})
	if err != nil || string(again.Payload) != `{"x":1}` {
		t.Fatalf("response payload mutated stored task: payload=%s err=%v", again.Payload, err)
	}
	if created.Result != nil {
		t.Fatal("pending response unexpectedly has result")
	}
}
