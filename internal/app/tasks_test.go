package app_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bright2227/vsx-task-system/internal/adapter/memory"
	"github.com/bright2227/vsx-task-system/internal/app"
	"github.com/bright2227/vsx-task-system/internal/task"
)

type processor func(context.Context, task.Payload) (string, error)

func (p processor) Execute(ctx context.Context, in task.Payload) (string, error) { return p(ctx, in) }

func TestSubmitAndConsumerPersistLifecycle(t *testing.T) {
	now := time.Now().UTC()
	store := memory.New()
	clock := func() time.Time { now = now.Add(time.Millisecond); return now }
	service := app.Tasks{Store: store, Clock: clock}
	created, err := service.Submit(context.Background(), app.SubmitTaskRequest{Payload: []byte(`{"amount":7}`)})
	if err != nil {
		t.Fatal(err)
	}
	if created.Status != string(task.Pending) || len(store.Events) != 1 || len(store.Outbox) != 1 {
		t.Fatalf("submit did not persist event/outbox: state=%+v events=%d outbox=%d", created, len(store.Events), len(store.Outbox))
	}
	var calls atomic.Int32
	consumer := app.Consumer{Store: store, Clock: clock, Lease: time.Second, Timeout: time.Second, Processor: processor(func(ctx context.Context, p task.Payload) (string, error) { calls.Add(1); return `{"ok":true}`, nil })}
	if err := consumer.Process(context.Background(), app.ProcessTaskRequest{ID: created.ID}); err != nil {
		t.Fatal(err)
	}
	final, err := service.Get(context.Background(), app.TaskRequest{ID: created.ID})
	if err != nil || final.Status != string(task.Completed) || final.Attempts != 1 || final.Result == nil || calls.Load() != 1 {
		t.Fatalf("final=%+v calls=%d err=%v", final, calls.Load(), err)
	}
	if len(store.Events) != 3 {
		t.Fatalf("expected submitted, started, completed events; got %d", len(store.Events))
	}
}

func TestStoreTransactionRollsBackWhenCallbackFails(t *testing.T) {
	store := memory.New()
	p, _ := task.NewPayload([]byte(`{}`))
	tsk, _ := task.Submit(p, time.Now())
	want := errors.New("simulated database failure")
	err := store.Within(context.Background(), func(r app.Repository) error {
		if err := r.Insert(context.Background(), tsk); err != nil {
			return err
		}
		return want
	})
	if !errors.Is(err, want) {
		t.Fatalf("Within error=%v", err)
	}
	if _, err = store.Get(context.Background(), tsk.Snapshot().ID); !errors.Is(err, app.ErrNotFound) {
		t.Fatalf("rolled-back task lookup error=%v", err)
	}
}

func TestConsumerPersistsBusinessErrorAndLeavesInterruptedWorkRecoverable(t *testing.T) {
	store := memory.New()
	service := app.Tasks{Store: store, Clock: time.Now}
	created, err := service.Submit(context.Background(), app.SubmitTaskRequest{Payload: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	consumer := app.Consumer{Store: store, Clock: time.Now, Lease: time.Second, Timeout: time.Second, Processor: processor(func(context.Context, task.Payload) (string, error) { return "", errors.New("private processor detail") })}
	if err = consumer.Process(context.Background(), app.ProcessTaskRequest{ID: created.ID}); err != nil {
		t.Fatal(err)
	}
	failed, err := service.Get(context.Background(), app.TaskRequest{ID: created.ID})
	if err != nil || failed.Status != string(task.Completed) || failed.Error == nil || *failed.Error != "task execution failed" {
		t.Fatalf("business failure state=%+v err=%v", failed, err)
	}

	interrupted, err := service.Submit(context.Background(), app.SubmitTaskRequest{Payload: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	blocking := app.Consumer{Store: store, Clock: time.Now, Lease: time.Second, Timeout: time.Second, Processor: processor(func(ctx context.Context, _ task.Payload) (string, error) {
		close(started)
		<-ctx.Done()
		return "", ctx.Err()
	})}
	done := make(chan error, 1)
	go func() { done <- blocking.Process(ctx, app.ProcessTaskRequest{ID: interrupted.ID}) }()
	<-started
	cancel()
	if err = <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("interrupted Process error=%v", err)
	}
	stored, err := store.Get(context.Background(), task.ID(interrupted.ID))
	if err != nil {
		t.Fatal(err)
	}
	recoverable := stored.Snapshot()
	if recoverable.Status != task.Processing || recoverable.LeaseUntil == nil {
		t.Fatalf("interrupted work state=%+v err=%v", recoverable, err)
	}
}

func TestUseCasesValidateInputsAndReturnZeroResponseOnInsertFailure(t *testing.T) {
	store := memory.New()
	service := app.Tasks{Store: store, Clock: time.Now}
	if _, err := service.Submit(context.Background(), app.SubmitTaskRequest{Payload: []byte(`{`)}); !errors.Is(err, app.ErrInvalidPayload) {
		t.Fatalf("invalid payload error=%v", err)
	}
	if _, err := service.Get(context.Background(), app.TaskRequest{ID: "bad"}); !errors.Is(err, app.ErrInvalidID) {
		t.Fatalf("invalid get ID error=%v", err)
	}
	if _, err := service.Cancel(context.Background(), app.TaskRequest{ID: "bad"}); !errors.Is(err, app.ErrInvalidID) {
		t.Fatalf("invalid cancel ID error=%v", err)
	}
	consumer := app.Consumer{Store: store, Clock: time.Now, Processor: processor(func(context.Context, task.Payload) (string, error) { return "", nil })}
	if err := consumer.Process(context.Background(), app.ProcessTaskRequest{ID: "bad"}); !errors.Is(err, app.ErrInvalidID) {
		t.Fatalf("invalid process ID error=%v", err)
	}
	if _, err := service.Submit(context.Background(), app.SubmitTaskRequest{Payload: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	// A duplicate event or storage failure is injected through a Store wrapper
	// below; this confirms Submit never returns an uncommitted generated ID.
	failing := failInsertStore{Store: store, err: errors.New("insert failed")}
	created, err := (app.Tasks{Store: failing, Clock: time.Now}).Submit(context.Background(), app.SubmitTaskRequest{Payload: []byte(`{}`)})
	if err == nil || created.ID != "" || created.Payload != nil || created.Status != "" || created.Result != nil || created.Error != nil || !created.CreatedAt.IsZero() || !created.UpdatedAt.IsZero() || created.Attempts != 0 {
		t.Fatalf("failed submit response=%+v err=%v", created, err)
	}
}

type failInsertStore struct {
	app.Store
	err error
}

func (s failInsertStore) Within(ctx context.Context, fn func(app.Repository) error) error {
	return s.Store.Within(ctx, func(r app.Repository) error { return fn(failInsertRepository{Repository: r, err: s.err}) })
}

type failInsertRepository struct {
	app.Repository
	err error
}

func (r failInsertRepository) Insert(context.Context, *task.Task) error { return r.err }

func TestTaskResponseDoesNotShareMutableValues(t *testing.T) {
	store := memory.New()
	service := app.Tasks{Store: store, Clock: time.Now}
	created, err := service.Submit(context.Background(), app.SubmitTaskRequest{Payload: []byte(`{"x":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	created.Payload[0] = '['
	again, err := service.Get(context.Background(), app.TaskRequest{ID: created.ID})
	if err != nil || string(again.Payload) != `{"x":1}` {
		t.Fatalf("response payload mutated stored task: payload=%s err=%v", again.Payload, err)
	}
	if created.Result != nil {
		t.Fatal("pending response unexpectedly has result")
	}
}
