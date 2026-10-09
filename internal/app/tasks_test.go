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
	payload, _ := task.NewPayload([]byte(`{"amount":7}`))
	service := app.Tasks{Store: store, Clock: clock}
	created, err := service.Submit(context.Background(), payload)
	if err != nil {
		t.Fatal(err)
	}
	if created.Status != task.Pending || len(store.Events) != 1 || len(store.Outbox) != 1 {
		t.Fatalf("submit did not persist event/outbox: state=%+v events=%d outbox=%d", created, len(store.Events), len(store.Outbox))
	}
	var calls atomic.Int32
	consumer := app.Consumer{Store: store, Clock: clock, Lease: time.Second, Timeout: time.Second, Processor: processor(func(ctx context.Context, p task.Payload) (string, error) { calls.Add(1); return `{"ok":true}`, nil })}
	if err := consumer.Process(context.Background(), created.ID); err != nil {
		t.Fatal(err)
	}
	final, err := service.Get(context.Background(), created.ID)
	if err != nil || final.Status != task.Completed || final.Attempts != 1 || final.Result == nil || calls.Load() != 1 {
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
	payload, _ := task.NewPayload([]byte(`{}`))
	service := app.Tasks{Store: store, Clock: time.Now}
	created, err := service.Submit(context.Background(), payload)
	if err != nil {
		t.Fatal(err)
	}
	consumer := app.Consumer{Store: store, Clock: time.Now, Lease: time.Second, Timeout: time.Second, Processor: processor(func(context.Context, task.Payload) (string, error) { return "", errors.New("private processor detail") })}
	if err = consumer.Process(context.Background(), created.ID); err != nil {
		t.Fatal(err)
	}
	failed, err := service.Get(context.Background(), created.ID)
	if err != nil || failed.Status != task.Completed || failed.Error == nil || *failed.Error != "task execution failed" {
		t.Fatalf("business failure state=%+v err=%v", failed, err)
	}

	interrupted, err := service.Submit(context.Background(), payload)
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
	go func() { done <- blocking.Process(ctx, interrupted.ID) }()
	<-started
	cancel()
	if err = <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("interrupted Process error=%v", err)
	}
	recoverable, err := service.Get(context.Background(), interrupted.ID)
	if err != nil || recoverable.Status != task.Processing || recoverable.LeaseUntil == nil {
		t.Fatalf("interrupted work state=%+v err=%v", recoverable, err)
	}
}
