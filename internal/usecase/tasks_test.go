package usecase_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/bright2227/gogolook-task-system/internal/domain"
	"github.com/bright2227/gogolook-task-system/internal/repository/task/memory"
	"github.com/bright2227/gogolook-task-system/internal/usecase"
)

func requestFor(store *memory.Store, id string) usecase.ProcessTaskRequest {
	parsed, _ := domain.ParseID(id)
	task, err := store.Get(context.Background(), parsed)
	if err != nil {
		panic(err)
	}
	snap := task.Snapshot()
	var event domain.ID
	for i := len(store.Events) - 1; i >= 0; i-- {
		if store.Events[i].TaskID == parsed && store.Events[i].ExecutionID == snap.ExecutionID {
			event = store.Events[i].ID
			break
		}
	}
	return usecase.ProcessTaskRequest{ID: id, EventID: string(event), ExecutionID: string(snap.ExecutionID), Original: snap.Payload.JSON()}
}

type processor func(context.Context, domain.ID, domain.Payload) (string, error)

func (p processor) Execute(ctx context.Context, id domain.ID, in domain.Payload) (string, error) {
	return p(ctx, id, in)
}

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
	var processorTaskID domain.ID
	consumer := usecase.Consumer{Store: store, Clock: clock, Lease: time.Second, Timeout: time.Second, Processor: processor(func(ctx context.Context, id domain.ID, p domain.Payload) (string, error) {
		processorTaskID = id
		calls.Add(1)
		return `{"ok":true}`, nil
	})}
	if err := consumer.Process(context.Background(), requestFor(store, created.ID)); err != nil {
		t.Fatal(err)
	}
	final, err := service.Get(context.Background(), usecase.TaskRequest{ID: created.ID})
	if err != nil || final.Status != string(domain.Completed) || final.Attempts != 1 || final.Result == nil || calls.Load() != 1 {
		t.Fatalf("final=%+v calls=%d err=%v", final, calls.Load(), err)
	}
	if string(processorTaskID) != created.ID {
		t.Fatalf("processor idempotency key=%s, want stable task ID %s", processorTaskID, created.ID)
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
	consumer := usecase.Consumer{Store: store, Clock: time.Now, Lease: time.Second, Timeout: time.Second, RetryDelays: []time.Duration{}, Processor: processor(func(context.Context, domain.ID, domain.Payload) (string, error) {
		return "", errors.New("private processor detail")
	})}
	if err = consumer.Process(context.Background(), requestFor(store, created.ID)); err != nil {
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
	blocking := usecase.Consumer{Store: store, Clock: time.Now, Lease: time.Second, Timeout: time.Second, Processor: processor(func(ctx context.Context, _ domain.ID, _ domain.Payload) (string, error) {
		close(started)
		<-ctx.Done()
		return "", ctx.Err()
	})}
	done := make(chan error, 1)
	go func() { done <- blocking.Process(ctx, requestFor(store, interrupted.ID)) }()
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
	consumer := usecase.Consumer{Store: store, Clock: time.Now, Processor: processor(func(context.Context, domain.ID, domain.Payload) (string, error) { return "", nil })}
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

func TestRetryPersistsDelayedWorkAndDeadLettersAfterBudget(t *testing.T) {
	store := memory.New()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	service := usecase.Tasks{Store: store, Clock: clock}
	created, err := service.Submit(context.Background(), usecase.SubmitTaskRequest{Payload: []byte(`{"x":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	consumer := usecase.Consumer{Store: store, Clock: clock, Lease: time.Minute, Timeout: time.Second, WorkerID: "worker-a", RetryDelays: []time.Duration{time.Second}, Processor: processor(func(context.Context, domain.ID, domain.Payload) (string, error) {
		return "", errors.New("private upstream detail")
	})}
	firstReq := requestFor(store, created.ID)
	firstExec := firstReq.ExecutionID
	if err = consumer.Process(context.Background(), firstReq); err != nil {
		t.Fatal(err)
	}
	pending, err := service.Get(context.Background(), usecase.TaskRequest{ID: created.ID})
	if err != nil || pending.Status != string(domain.Pending) || pending.RetryCount != 1 || pending.Attempts != 1 {
		t.Fatalf("first retry state=%+v err=%v", pending, err)
	}
	if len(store.Outbox) != 2 || store.Outbox[1].Destination != usecase.DestinationWork || store.Outbox[1].ExecutionID == domain.ID(firstExec) || !store.Outbox[1].AvailableAt.Equal(now.Add(time.Second)) {
		t.Fatalf("retry outbox=%+v", store.Outbox)
	}
	if err = consumer.Process(context.Background(), requestFor(store, created.ID)); !errors.Is(err, usecase.ErrNotDue) {
		t.Fatalf("early delivery error=%v", err)
	}
	if err = consumer.Process(context.Background(), firstReq); err != nil {
		t.Fatalf("stale old generation should be safe no-op: %v", err)
	}
	now = now.Add(time.Second)
	secondReq := requestFor(store, created.ID)
	if secondReq.ExecutionID == firstExec {
		t.Fatal("retry did not fence old execution")
	}
	secondReq.Original = bytes.Repeat([]byte("x"), 65537)
	if err = consumer.Process(context.Background(), secondReq); err != nil {
		t.Fatal(err)
	}
	failed, err := service.Get(context.Background(), usecase.TaskRequest{ID: created.ID})
	if err != nil || failed.Status != string(domain.Completed) || failed.Error == nil || *failed.Error != "task execution failed" || failed.RetryCount != 1 || failed.Attempts != 2 {
		t.Fatalf("exhausted state=%+v err=%v", failed, err)
	}
	if len(store.DeadLetters) != 1 || len(store.Outbox) != 3 {
		t.Fatalf("DLQ/outbox records: dlq=%d outbox=%+v", len(store.DeadLetters), store.Outbox)
	}
	var dlq usecase.DeadLetterRecord
	for _, dlq = range store.DeadLetters {
	}
	hash := sha256.Sum256(secondReq.Original)
	if dlq.MessageID != string(store.Outbox[2].EventID) || store.Outbox[2].Destination != usecase.DestinationDeadLetter || store.Outbox[2].Failure == nil || store.Outbox[2].Failure.Detail != "private upstream detail" || dlq.Detail != "private upstream detail" || dlq.SourceEventID != domain.ID(secondReq.EventID) || dlq.WorkerID != "worker-a" || !dlq.OriginalTruncated || len(dlq.Original) != 65536 || dlq.OriginalHash != hex.EncodeToString(hash[:]) {
		t.Fatalf("DLQ diagnostics/outbox mismatch: record=%+v message=%+v", dlq, store.Outbox[2])
	}
	if len(store.Attempts) != 2 || store.Attempts[0].Outcome != "retry_scheduled" || store.Attempts[0].Category != "retryable" || store.Attempts[0].Code != "processor_error" || store.Attempts[1].Outcome != "dead_lettered" || store.Attempts[1].Category != "exhausted" || store.Attempts[1].Code != "retries_exhausted" {
		t.Fatalf("attempt history=%+v", store.Attempts)
	}
}

func TestPermanentFailureAndReplayReceiptAreAtomicAndIdempotent(t *testing.T) {
	store := memory.New()
	now := time.Now().UTC()
	svc := usecase.Tasks{Store: store, Clock: func() time.Time { return now }}
	created, err := svc.Submit(context.Background(), usecase.SubmitTaskRequest{Payload: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	c := usecase.Consumer{Store: store, Clock: func() time.Time { return now }, Lease: time.Minute, Timeout: time.Second, WorkerID: "worker-b", Processor: processor(func(context.Context, domain.ID, domain.Payload) (string, error) {
		return "", usecase.Permanent("invalid_input", "cannot process", errors.New("detail"))
	})}
	if err = c.Process(context.Background(), requestFor(store, created.ID)); err != nil {
		t.Fatal(err)
	}
	var dlq usecase.DeadLetterRecord
	for _, dlq = range store.DeadLetters {
	}
	if dlq.Category != "permanent" || dlq.Code != "invalid_input" || dlq.Attempts != 1 || store.Attempts[0].Category != "permanent" || store.Attempts[0].Code != "invalid_input" {
		t.Fatalf("permanent record=%+v", dlq)
	}
	req := usecase.ReplayRequest{MessageID: dlq.MessageID, Operator: "operator", Reason: "corrected dependency"}
	type result struct {
		response usecase.ReplayResponse
		err      error
	}
	results := make(chan result, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); v, e := svc.Replay(context.Background(), req); results <- result{v, e} }()
	}
	wg.Wait()
	close(results)
	var first usecase.ReplayResponse
	count := 0
	already := 0
	for got := range results {
		if got.err != nil {
			t.Fatal(got.err)
		}
		count++
		if got.response.AlreadyReplayed {
			already++
		}
		if first.ExecutionID == "" {
			first = got.response
		} else if first.ExecutionID != got.response.ExecutionID {
			t.Fatalf("replay executions differ: %+v %+v", first, got.response)
		}
	}
	if count != 2 || already != 1 || len(store.Receipts) != 1 || len(store.Outbox) != 3 {
		t.Fatalf("replay results count=%d already=%d receipt=%d outbox=%d", count, already, len(store.Receipts), len(store.Outbox))
	}
	replayed, err := svc.Get(context.Background(), usecase.TaskRequest{ID: created.ID})
	if err != nil || replayed.Status != string(domain.Pending) || replayed.RetryCount != 0 || replayed.ReplayCycle != 1 || replayed.Attempts != 1 {
		t.Fatalf("replay task=%+v err=%v", replayed, err)
	}
	if _, err = svc.Replay(context.Background(), usecase.ReplayRequest{MessageID: req.MessageID, Operator: "other", Reason: req.Reason}); !errors.Is(err, usecase.ErrReplayConflict) {
		t.Fatalf("changed replay fingerprint error=%v", err)
	}
}

func TestExpiredLeaseRecoveryPreservesAttemptHistoryAndFencesLateWorker(t *testing.T) {
	store := memory.New()
	now := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	clock := func() time.Time { return now }
	svc := usecase.Tasks{Store: store, Clock: clock}
	created, err := svc.Submit(context.Background(), usecase.SubmitTaskRequest{Payload: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	first := usecase.Consumer{Store: store, Clock: clock, Lease: time.Second, Timeout: time.Minute, WorkerID: "first", RetryDelays: []time.Duration{}, Processor: processor(func(context.Context, domain.ID, domain.Payload) (string, error) {
		close(started)
		<-release
		return "late result", nil
	})}
	firstDone := make(chan error, 1)
	go func() { firstDone <- first.Process(context.Background(), requestFor(store, created.ID)) }()
	<-started
	now = now.Add(2 * time.Second)
	second := usecase.Consumer{Store: store, Clock: clock, Lease: time.Second, Timeout: time.Second, WorkerID: "recovery", Processor: processor(func(context.Context, domain.ID, domain.Payload) (string, error) { return "recovered", nil })}
	if err = second.Process(context.Background(), requestFor(store, created.ID)); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err = <-firstDone; err != nil {
		t.Fatal(err)
	}
	final, err := svc.Get(context.Background(), usecase.TaskRequest{ID: created.ID})
	if err != nil || final.Status != string(domain.Completed) || final.Result == nil || *final.Result != "recovered" || final.Attempts != 2 {
		t.Fatalf("final=%+v err=%v", final, err)
	}
	if len(store.Attempts) != 2 || store.Attempts[0].Outcome != "interrupted" || store.Attempts[1].Outcome != "succeeded" {
		t.Fatalf("attempts=%+v", store.Attempts)
	}
}

func TestFailureOutboxRollbackLeavesInputUnacknowledgeable(t *testing.T) {
	store := memory.New()
	now := time.Now().UTC()
	service := usecase.Tasks{Store: store, Clock: func() time.Time { return now }}
	created, err := service.Submit(context.Background(), usecase.SubmitTaskRequest{Payload: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	consumer := usecase.Consumer{Store: failEnqueueStore{Store: store}, Clock: func() time.Time { return now }, Lease: time.Minute, Timeout: time.Second, RetryDelays: []time.Duration{time.Second}, Processor: processor(func(context.Context, domain.ID, domain.Payload) (string, error) { return "", errors.New("transient") })}
	if err = consumer.Process(context.Background(), requestFor(store, created.ID)); err == nil {
		t.Fatal("durability failure returned nil and would ACK the input")
	}
	stored, err := store.Get(context.Background(), domain.ID(created.ID))
	if err != nil {
		t.Fatal(err)
	}
	snap := stored.Snapshot()
	if snap.Status != domain.Processing || snap.LeaseToken == "" || snap.RetryCount != 0 || len(store.Outbox) != 1 || len(store.DeadLetters) != 0 || len(store.Attempts) != 1 || store.Attempts[0].FinishedAt != nil {
		t.Fatalf("failure decision partially committed: state=%+v outbox=%d dlq=%d attempts=%+v", snap, len(store.Outbox), len(store.DeadLetters), store.Attempts)
	}
}

type failEnqueueStore struct{ usecase.Store }

func (s failEnqueueStore) Within(ctx context.Context, fn func(usecase.Repository) error) error {
	return s.Store.Within(ctx, func(r usecase.Repository) error { return fn(failEnqueueRepository{Repository: r}) })
}

type failEnqueueRepository struct{ usecase.Repository }

func (f failEnqueueRepository) Enqueue(context.Context, usecase.Message) error {
	return errors.New("outbox insert failed")
}

func TestLateSuccessCannotOverwriteCancellationOrAttemptOutcome(t *testing.T) {
	store := memory.New()
	service := usecase.Tasks{Store: store, Clock: time.Now}
	created, err := service.Submit(context.Background(), usecase.SubmitTaskRequest{Payload: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	consumer := usecase.Consumer{Store: store, Clock: time.Now, Lease: time.Minute, Timeout: time.Minute, Processor: processor(func(context.Context, domain.ID, domain.Payload) (string, error) {
		close(started)
		<-release
		return "late", nil
	})}
	done := make(chan error, 1)
	go func() { done <- consumer.Process(context.Background(), requestFor(store, created.ID)) }()
	<-started
	if _, err = service.Cancel(context.Background(), usecase.TaskRequest{ID: created.ID}); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	final, err := service.Get(context.Background(), usecase.TaskRequest{ID: created.ID})
	if err != nil || final.Status != string(domain.Canceled) || final.Result != nil {
		t.Fatalf("late result overwrote cancel: %+v err=%v", final, err)
	}
	if len(store.Attempts) != 1 || store.Attempts[0].Outcome != "canceled" {
		t.Fatalf("late finalization overwrote attempt: %+v", store.Attempts)
	}
}

func TestNilRetryDelaysUseDefaultOneTenSixtySeconds(t *testing.T) {
	store := memory.New()
	now := time.Date(2026, 4, 5, 6, 7, 8, 0, time.UTC)
	clock := func() time.Time { return now }
	service := usecase.Tasks{Store: store, Clock: clock}
	created, err := service.Submit(context.Background(), usecase.SubmitTaskRequest{Payload: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	consumer := usecase.Consumer{Store: store, Clock: clock, Lease: time.Minute, Timeout: time.Second, WorkerID: "default-delay", RetryDelays: nil, Processor: processor(func(context.Context, domain.ID, domain.Payload) (string, error) { return "", errors.New("retry") })}
	for i := 0; i < 4; i++ {
		if i > 0 {
			now = store.Outbox[len(store.Outbox)-1].AvailableAt
		}
		if err = consumer.Process(context.Background(), requestFor(store, created.ID)); err != nil {
			t.Fatalf("attempt %d: %v", i+1, err)
		}
	}
	state, err := service.Get(context.Background(), usecase.TaskRequest{ID: created.ID})
	if err != nil || state.Status != string(domain.Completed) || state.Attempts != 4 || state.RetryCount != 3 {
		t.Fatalf("default retries state=%+v err=%v", state, err)
	}
	want := []time.Duration{time.Second, 10 * time.Second, time.Minute}
	for i, d := range want {
		if got := store.Outbox[i+1].AvailableAt.Sub(store.Events[(i+1)*2].OccurredAt); got != d {
			t.Fatalf("retry %d delay=%v want %v", i+1, got, d)
		}
	}
}

func TestWorkerTimeoutIsRetryableUnlessParentShutsDown(t *testing.T) {
	store := memory.New()
	created, err := (usecase.Tasks{Store: store, Clock: time.Now}).Submit(context.Background(), usecase.SubmitTaskRequest{Payload: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	consumer := usecase.Consumer{Store: store, Clock: time.Now, Lease: time.Minute, Timeout: time.Millisecond, RetryDelays: []time.Duration{time.Second}, Processor: processor(func(context.Context, domain.ID, domain.Payload) (string, error) {
		time.Sleep(5 * time.Millisecond)
		return "ignored", nil
	})}
	if err = consumer.Process(context.Background(), requestFor(store, created.ID)); err != nil {
		t.Fatal(err)
	}
	state, err := (usecase.Tasks{Store: store, Clock: time.Now}).Get(context.Background(), usecase.TaskRequest{ID: created.ID})
	if err != nil || state.Status != string(domain.Pending) || state.RetryCount != 1 || len(store.Outbox) != 2 {
		t.Fatalf("timeout was not retried: state=%+v outbox=%d err=%v", state, len(store.Outbox), err)
	}
}

func TestPrivateFailureDiagnosticsAreBoundedAndValidForPostgres(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	tasks := usecase.Tasks{Store: store, Clock: time.Now}
	created, err := tasks.Submit(ctx, usecase.SubmitTaskRequest{Payload: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	consumer := usecase.Consumer{Store: store, Clock: time.Now, Lease: time.Second, Timeout: time.Second, Processor: processor(func(context.Context, domain.ID, domain.Payload) (string, error) {
		return "", usecase.Permanent("invalid code", "private\x00"+strings.Repeat("界", 1400), nil)
	})}
	if err = consumer.Process(ctx, requestFor(store, created.ID)); err != nil {
		t.Fatal(err)
	}
	if len(store.DeadLetters) != 1 {
		t.Fatalf("dead-letter records=%d", len(store.DeadLetters))
	}
	for _, failure := range store.DeadLetters {
		if !utf8.ValidString(failure.Detail) || strings.ContainsRune(failure.Detail, 0) || len(failure.Detail) > 4096 || failure.Code != "permanent_error" {
			t.Fatalf("unsafe database diagnostics: code=%q bytes=%d valid=%v", failure.Code, len(failure.Detail), utf8.ValidString(failure.Detail))
		}
		if _, err = tasks.Replay(ctx, usecase.ReplayRequest{MessageID: failure.MessageID, Operator: "operator\x00", Reason: "repair"}); err == nil {
			t.Fatal("NUL in replay audit accepted")
		}
	}
	public, err := tasks.Get(ctx, usecase.TaskRequest{ID: created.ID})
	if err != nil || public.Error == nil || *public.Error != "task execution failed" {
		t.Fatalf("private details leaked into public outcome: %+v err=%v", public, err)
	}
}
