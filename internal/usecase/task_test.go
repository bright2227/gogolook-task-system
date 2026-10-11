package usecase

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/bright2227/gogolook-task-system/internal/usecase/port"

	"github.com/bright2227/gogolook-task-system/internal/domain"
	"github.com/bright2227/gogolook-task-system/internal/repository/task"
)

// retryDelays mirrors the source's default network retry policy.
var retryDelays = []time.Duration{time.Second, 10 * time.Second, time.Minute}

func TestSubmitRejectsInvalidNamesBeforeStorageOrPublication(t *testing.T) {
	repo := task.NewMockTaskRepository()
	publications := 0
	queue := enqueueFunc(func(_ context.Context, id domain.ID) (Publication, error) {
		publications++
		return Publication{ID: id}, nil
	})
	svc := NewTaskUseCase(repo, queue, time.Now)
	for _, name := range []string{"", " \t\n", "\u00a0\u3000", "before\x00after", string([]byte{0xff}), strings.Repeat("a", domain.MaxNameBytes+1)} {
		created, err := svc.Submit(context.Background(), name, []byte("payload"))
		if created != nil || !errors.Is(err, domain.ErrName) {
			t.Errorf("name %q: task=%+v err=%v", name, created, err)
		}
	}
	items, _, err := svc.List(context.Background(), 100, time.Time{}, "")
	if err != nil || len(items) != 0 || publications != 0 {
		t.Fatalf("invalid names stored=%d published=%d err=%v", len(items), publications, err)
	}
}

func TestLegacyUnnamedTaskCanProcessAndBeRenamed(t *testing.T) {
	repo := task.NewMockTaskRepository()
	payload, _ := domain.NewPayload([]byte("hello"))
	legacy, err := domain.Submit(payload, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Insert(context.Background(), legacy); err != nil {
		t.Fatal(err)
	}
	svc := NewTaskUseCase(repo, accepted, time.Now)
	if err := svc.ProcessTasks(context.Background(), []domain.ID{legacy.ID}, Delay(0), nil); err != nil {
		t.Fatal(err)
	}
	stored, err := svc.Get(context.Background(), string(legacy.ID))
	if err != nil || stored.Name != "" || !stored.Finished() || stored.Result == nil || *stored.Result != "5" {
		t.Fatalf("legacy task=%+v err=%v", stored, err)
	}
	if _, err := svc.Update(context.Background(), string(legacy.ID), "", []byte("hello"), 0); !errors.Is(err, domain.ErrName) {
		t.Fatalf("unnamed edit=%v", err)
	}
	updated, err := svc.Update(context.Background(), string(legacy.ID), "renamed", []byte("hello"), 0)
	if err != nil || updated.Name != "renamed" || !updated.Finished() {
		t.Fatalf("renamed legacy task=%+v err=%v", updated, err)
	}
}

func TestExecuteCountsUnicodePayloadCharacters(t *testing.T) {
	svc := NewTaskUseCase(task.NewMockTaskRepository(), accepted, time.Now)
	for _, tc := range []struct{ payload, want string }{
		{"", "0"}, {"plain non-JSON", "14"}, {"你好🙂", "3"}, {"e\u0301", "2"},
	} {
		payload, err := domain.NewPayload([]byte(tc.payload))
		if err != nil {
			t.Fatal(err)
		}
		result, err := svc.execute(context.Background(), payload, func(ctx context.Context) error { return ctx.Err() })
		if err != nil || result != tc.want {
			t.Fatalf("payload %q result=%q want=%q err=%v", tc.payload, result, tc.want, err)
		}
	}
}

func TestSubmitProcessAndTerminalPrivateError(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	repo := task.NewMockTaskRepository()
	svc := NewTaskUseCase(repo, accepted, clock)
	created, err := svc.Submit(context.Background(), "test task", []byte(`{"x":1}`))
	if err != nil {
		t.Fatal(err)
	}
	var execution Work = func(context.Context) error {
		return errors.New("internal credential detail")
	}
	snap := created
	req := snap.ID
	now = now.Add(time.Second)
	if err = svc.ProcessTasks(context.Background(), []domain.ID{req}, execution, retryDelays); err != nil {
		t.Fatal(err)
	}
	stored, err := svc.Get(context.Background(), string(snap.ID))
	if err != nil {
		t.Fatal(err)
	}
	state := stored
	if state.Status != domain.Completed || state.Error == nil || *state.Error != "internal credential detail" || state.Result != nil || state.Payload.String() != `{"x":1}` {
		t.Fatalf("state=%+v", state)
	}
}

func TestListKeysetPagesAndPaginationValidation(t *testing.T) {
	now := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	svc := NewTaskUseCase(task.NewMockTaskRepository(), accepted, func() time.Time { return now })
	for range 3 {
		if _, err := svc.Submit(context.Background(), "test task", []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
	}
	first, more, err := svc.List(context.Background(), 2, time.Time{}, "")
	if err != nil || !more || len(first) != 2 {
		t.Fatalf("page=%d more=%v err=%v", len(first), more, err)
	}
	second, more, err := svc.List(context.Background(), 2, first[1].CreatedAt, first[1].ID)
	if err != nil || more || len(second) != 1 {
		t.Fatalf("page=%d more=%v err=%v", len(second), more, err)
	}
	empty, more, err := svc.List(context.Background(), 20, second[0].CreatedAt, second[0].ID)
	if err != nil || more || empty == nil || len(empty) != 0 {
		t.Fatalf("empty=%v more=%v err=%v", empty, more, err)
	}
	if _, _, err = svc.List(context.Background(), 0, time.Time{}, ""); !errors.Is(err, domain.ErrPagination) {
		t.Fatalf("invalid page size err=%v", err)
	}
	if _, _, err = svc.List(context.Background(), 10, now, ""); !errors.Is(err, domain.ErrPagination) {
		t.Fatalf("unpaired cursor err=%v", err)
	}
}

func TestOnlyNetworkFailuresRetryWithinBudget(t *testing.T) {
	for _, tc := range []struct {
		name  string
		err   error
		retry bool
	}{
		{"network", errors.Join(domain.ErrNetwork, syscall.ECONNRESET), true},
		{"permission", syscall.EACCES, false}, {"input", errors.New("bad input"), false},
		{"unmarked", errors.New("connection reset"), false},
		{"network deadline", errors.Join(domain.ErrNetwork, context.DeadlineExceeded), true},
		{"task timeout", context.DeadlineExceeded, true},
		{"canceled", context.Canceled, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now().UTC()
			repo := task.NewMockTaskRepository()
			svc := NewTaskUseCase(repo, accepted, func() time.Time { return now })
			retryDelays := []time.Duration{time.Second, time.Minute}
			created, err := svc.Submit(context.Background(), "test task", []byte("text"))
			if err != nil {
				t.Fatal(err)
			}
			var execution Work = func(context.Context) error { return tc.err }
			attempts := 1
			if tc.retry {
				attempts = 3
			}
			for i := 0; i < attempts; i++ {
				if err := svc.ProcessTasks(context.Background(), []domain.ID{created.ID}, execution, retryDelays); err != nil {
					t.Fatal(err)
				}
				state, _ := repo.Get(context.Background(), created.ID)
				if tc.retry && i < 2 {
					if state.Status != domain.Pending || state.IsSubmitted || state.RetryCount != i+1 {
						t.Fatalf("retry=%+v", state)
					}
					now = state.NextRetry
				} else if state.Status != domain.Completed || state.Error == nil || !strings.Contains(*state.Error, tc.err.Error()) {
					t.Fatalf("terminal=%+v", state)
				}
			}
		})
	}
}

func TestBatchClaimsCurrentRowsAndRejectsEditsWhileRunning(t *testing.T) {
	ctx := context.Background()
	repo := task.NewMockTaskRepository()
	svc := NewTaskUseCase(repo, accepted, time.Now)
	first, _ := svc.Submit(ctx, "first", []byte("first"))
	second, _ := svc.Submit(ctx, "second", []byte("second"))
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var execution Work = func(ctx context.Context) error {
		started <- struct{}{}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	done := make(chan error, 1)
	go func() {
		done <- svc.ProcessTasks(ctx, []domain.ID{first.ID, second.ID, first.ID, domain.NewID()}, execution, retryDelays)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("batch did not start")
	}
	for _, item := range []*domain.Task{first, second} {
		claimed, err := repo.Get(ctx, item.ID)
		if err != nil || !claimed.IsRunning || claimed.Status != domain.Processing {
			close(release)
			t.Fatalf("batch did not claim task=%+v err=%v", claimed, err)
		}
	}
	active := first.ID
	if _, err := svc.Update(ctx, string(active), "edited", []byte("new"), 0); !errors.Is(err, domain.ErrBusy) {
		close(release)
		t.Fatalf("running PUT=%v", err)
	}
	if err := svc.Delete(ctx, string(active), 0); !errors.Is(err, domain.ErrBusy) {
		close(release)
		t.Fatalf("running DELETE=%v", err)
	}
	// A duplicate delivery during an active claim must not execute another copy.
	if err := svc.ProcessTasks(ctx, []domain.ID{active}, execution, retryDelays); err != nil {
		close(release)
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	for _, expected := range []struct {
		task   *domain.Task
		result string
	}{{first, "5"}, {second, "6"}} {
		state, err := repo.Get(ctx, expected.task.ID)
		if err != nil || state.Status != domain.Completed || state.IsRunning || state.Result == nil || *state.Result != expected.result || state.Version != 3 {
			t.Fatalf("completion=%+v,%v", state, err)
		}
	}
}

func TestExecutionTimeoutRetriesThenPersistsError(t *testing.T) {
	repo := task.NewMockTaskRepository()
	svc := NewTaskUseCase(repo, accepted, time.Now)
	svc.Timeout = 10 * time.Millisecond
	var execution Work = func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err() // Work contract: a done context is never reported as success.
	}
	created, _ := svc.Submit(context.Background(), "test task", []byte("text"))
	// The per-task deadline is transient: it spends the retry budget first.
	oneRetry := []time.Duration{0}
	if err := svc.ProcessTasks(context.Background(), []domain.ID{created.ID}, execution, oneRetry); err != nil {
		t.Fatal(err)
	}
	state, _ := repo.Get(context.Background(), created.ID)
	if state.Status != domain.Pending || state.IsSubmitted || state.RetryCount != 1 || state.Error != nil {
		t.Fatalf("first timeout=%+v", state)
	}
	if err := svc.ProcessTasks(context.Background(), []domain.ID{created.ID}, execution, oneRetry); err != nil {
		t.Fatal(err)
	}
	state, _ = repo.Get(context.Background(), created.ID)
	if state.Status != domain.Completed || state.Error == nil || !strings.Contains(*state.Error, context.DeadlineExceeded.Error()) || state.RetryCount != 1 {
		t.Fatalf("exhausted timeout=%+v", state)
	}
}

// A panic in Work completes only that task with an error; the batch's other
// tasks finish and ProcessTasks returns normally.
func TestWorkPanicCompletesOnlyThatTask(t *testing.T) {
	ctx := context.Background()
	repo := task.NewMockTaskRepository()
	svc := NewTaskUseCase(repo, accepted, time.Now)
	poison, _ := svc.Submit(ctx, "test task", []byte("poison"))
	healthy, _ := svc.Submit(ctx, "test task", []byte("text"))
	// Work only sees ctx, so the first call (Parallelism 1) panics.
	var calls atomic.Int64
	svc.Parallelism = 1
	var work Work = func(ctx context.Context) error {
		if calls.Add(1) == 1 {
			panic("boom")
		}
		return ctx.Err()
	}
	if err := svc.ProcessTasks(ctx, []domain.ID{poison.ID, healthy.ID}, work, retryDelays); err != nil {
		t.Fatal(err)
	}
	var panicked, finished int
	for _, id := range []domain.ID{poison.ID, healthy.ID} {
		state, _ := repo.Get(ctx, id)
		switch {
		case state.Status == domain.Completed && state.Error != nil && *state.Error == "task work panicked: boom" && state.RetryCount == 0:
			panicked++
		case state.Status == domain.Completed && state.Result != nil && state.Error == nil:
			finished++
		default:
			t.Fatalf("state=%+v", state)
		}
	}
	if panicked != 1 || finished != 1 {
		t.Fatalf("panicked=%d finished=%d", panicked, finished)
	}
}

// failCommitRepository fails the outcome commit (the second batch update).
type failCommitRepository struct {
	port.TaskRepository
	calls int
}

func (r *failCommitRepository) UpdateByIDs(ctx context.Context, ids []domain.ID, fn func([]*domain.Task) (bool, error)) error {
	if r.calls++; r.calls == 2 {
		return errors.New("commit unavailable")
	}
	return r.TaskRepository.UpdateByIDs(ctx, ids, fn)
}

func TestShutdownReleasesInterruptedClaimsAndKeepsFinishedWork(t *testing.T) {
	for _, commitFails := range []bool{false, true} {
		t.Run(map[bool]string{false: "commit", true: "commit fails"}[commitFails], func(t *testing.T) {
			base := task.NewMockTaskRepository()
			var repo port.TaskRepository = base
			if commitFails {
				repo = &failCommitRepository{TaskRepository: base}
			}
			svc := NewTaskUseCase(repo, accepted, time.Now)
			svc.Parallelism = 2
			finished, _ := svc.Submit(context.Background(), "test task", []byte("done"))
			interrupted, _ := svc.Submit(context.Background(), "test task", []byte("text"))
			ctx, cancel := context.WithCancel(context.Background())
			// The first execution finishes; the second runs until shutdown.
			var calls atomic.Int64
			started := make(chan struct{})
			var work Work = func(workCtx context.Context) error {
				if calls.Add(1) == 1 {
					return nil
				}
				close(started)
				<-workCtx.Done()
				return workCtx.Err()
			}
			done := make(chan error, 1)
			go func() { done <- svc.ProcessTasks(ctx, []domain.ID{finished.ID, interrupted.ID}, work, retryDelays) }()
			<-started
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatalf("shutdown=%v", err)
			}
			var completed, released *domain.Task
			for _, id := range []domain.ID{finished.ID, interrupted.ID} {
				state, err := base.Get(context.Background(), id)
				if err != nil {
					t.Fatal(err)
				}
				if state.Status == domain.Completed {
					completed = state
				} else {
					released = state
				}
			}
			if commitFails {
				if completed != nil || released.Status != domain.Processing {
					t.Fatalf("failed commit changed claims: completed=%+v other=%+v", completed, released)
				}
				return
			}
			if completed == nil || completed.Result == nil || completed.Error != nil {
				t.Fatalf("finished work lost: %+v", completed)
			}
			if released == nil || released.Status != domain.Pending || released.IsRunning || released.IsSubmitted || released.RetryCount != 0 || released.Result != nil || released.Error != nil || released.Version != 3 {
				t.Fatalf("interrupted claim not released: %+v", released)
			}
		})
	}
}

type testPublisher struct {
	err          error
	operationErr error
	stopAfter    int
	calls        int
	last         domain.ID
}

func (p *testPublisher) Enqueue(_ context.Context, id domain.ID) (Publication, error) {
	p.calls++
	p.last = id
	if p.operationErr != nil && (p.stopAfter == 0 || p.calls == p.stopAfter) {
		return Publication{}, p.operationErr
	}
	return Publication{ID: id, Err: p.err}, nil
}

type publisherFunc func(context.Context, domain.ID) error

func (p publisherFunc) Enqueue(ctx context.Context, id domain.ID) (Publication, error) {
	return Publication{ID: id, Err: p(ctx, id)}, nil
}

// accepted confirms every publication.
var accepted TaskQueue = enqueueFunc(func(_ context.Context, id domain.ID) (Publication, error) { return Publication{ID: id}, nil })

// newPublisherTask inserts an unsubmitted task created before PublishGrace, as
// left behind when the Submit fast path failed.
func newPublisherTask(t *testing.T, repo *task.MockTaskRepository, now time.Time) *domain.Task {
	t.Helper()
	payload, _ := domain.NewPayload([]byte("text"))
	created, err := domain.Submit(payload, now.Add(-time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Insert(context.Background(), created); err != nil {
		t.Fatal(err)
	}
	return created
}
func TestSubmissionPersistsBeforePublishThenMarksSubmit(t *testing.T) {
	repo := task.NewMockTaskRepository()
	now := time.Date(2050, 1, 1, 0, 0, 0, 0, time.UTC)
	queue := publisherFunc(func(ctx context.Context, id domain.ID) error {
		stored, err := repo.Get(ctx, id)
		if err != nil || stored.Status != domain.Pending || stored.IsSubmitted || stored.Payload.String() != "" {
			t.Fatalf("before publication=%+v,%v", stored, err)
		}
		return nil
	})
	svc := NewTaskUseCase(repo, queue, func() time.Time { return now })
	svc.RepublishAfter = republishAfter
	svc.StaleAfter = staleAfter
	created, err := svc.Submit(context.Background(), "test task", []byte(""))
	if err != nil || !created.IsSubmitted || created.Version != 1 {
		t.Fatalf("submission=%+v,%v", created, err)
	}
	stored, err := repo.Get(context.Background(), created.ID)
	if err != nil || !stored.IsSubmitted || stored.Version != 1 {
		t.Fatalf("published task=%+v,%v", stored, err)
	}
	now = now.Add(svc.PublishGrace)
	if count, err := svc.PublishPending(context.Background()); err != nil || count != 0 {
		t.Fatalf("sweeper republished a fresh submission=%d,%v", count, err)
	}
}

func TestSubmitSucceedsUnsubmittedWhenFastPathFails(t *testing.T) {
	for _, mode := range []string{"operation", "message", "conflict"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			now := time.Date(2050, 1, 1, 0, 0, 0, 0, time.UTC)
			repo := task.NewMockTaskRepository()
			queue := enqueueFunc(func(ctx context.Context, id domain.ID) (Publication, error) {
				switch mode {
				case "operation":
					return Publication{}, domain.ErrNetwork
				case "message":
					return Publication{ID: id, Err: errors.New("message returned by broker")}, nil
				}
				// A consumer claims the task before the marker.
				return Publication{ID: id}, repoClaim(ctx, repo, id, now)
			})
			svc := NewTaskUseCase(repo, queue, func() time.Time { return now })
			created, err := svc.Submit(ctx, "name", []byte("text"))
			// A skipped marker returns the current row: here, the consumer's claim.
			claimed := mode == "conflict"
			if err != nil || created == nil || created.IsSubmitted != claimed || created.IsRunning != claimed || created.Version != map[bool]int64{false: 1, true: 2}[claimed] || created.Name != "name" {
				t.Fatalf("submission=%+v,%v", created, err)
			}
			stored, err := repo.Get(ctx, created.ID)
			// A claim marks the task submitted itself.
			if err != nil || stored.IsSubmitted != (mode == "conflict") || stored.Error != nil || (mode == "conflict") != stored.IsRunning {
				t.Fatalf("stored=%+v,%v", stored, err)
			}
		})
	}
}

// getFailRepository fails reads, so Submit falls back to its inserted snapshot.
type getFailRepository struct{ port.TaskRepository }

func (getFailRepository) Get(context.Context, domain.ID) (*domain.Task, error) {
	return nil, errors.New("read unavailable")
}

func TestSubmitSkippedMarkerFallsBackToInsertedSnapshot(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2050, 1, 1, 0, 0, 0, 0, time.UTC)
	base := task.NewMockTaskRepository()
	svc := NewTaskUseCase(getFailRepository{base}, enqueueFunc(func(ctx context.Context, id domain.ID) (Publication, error) {
		return Publication{ID: id}, base.DeleteByID(ctx, id, 1)
	}), func() time.Time { return now })
	created, err := svc.Submit(ctx, "name", []byte("text"))
	if err != nil || created == nil || created.IsSubmitted || created.Version != 1 {
		t.Fatalf("submission=%+v,%v", created, err)
	}
}

func repoClaim(ctx context.Context, repo *task.MockTaskRepository, id domain.ID, now time.Time) error {
	return repo.UpdateByID(ctx, id, 1, func(t *domain.Task) (bool, error) { return t.Start(now) })
}

func TestSweeperLeavesNewTasksToFastPathDuringGrace(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2050, 1, 1, 0, 0, 0, 0, time.UTC)
	repo := task.NewMockTaskRepository()
	svc := NewTaskUseCase(repo, enqueueFunc(func(context.Context, domain.ID) (Publication, error) {
		return Publication{}, domain.ErrNetwork
	}), func() time.Time { return now })
	svc.RepublishAfter = republishAfter
	svc.StaleAfter = staleAfter
	created, err := svc.Submit(ctx, "test task", []byte("text"))
	if err != nil || created.IsSubmitted {
		t.Fatalf("submission=%+v,%v", created, err)
	}
	svc.queue = accepted
	now = now.Add(svc.PublishGrace - time.Microsecond)
	if count, err := svc.PublishPending(ctx); err != nil || count != 0 {
		t.Fatalf("within grace=%d,%v", count, err)
	}
	now = now.Add(time.Microsecond)
	if count, err := svc.PublishPending(ctx); err != nil || count != 1 {
		t.Fatalf("after grace=%d,%v", count, err)
	}
}

func TestSweeperPublishesDueRetryImmediately(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2050, 1, 1, 0, 0, 0, 0, time.UTC)
	repo := task.NewMockTaskRepository()
	svc := NewTaskUseCase(repo, accepted, func() time.Time { return now })
	svc.RepublishAfter = republishAfter
	svc.StaleAfter = staleAfter
	created, err := svc.Submit(ctx, "test task", []byte("text"))
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.ProcessTasks(ctx, []domain.ID{created.ID}, func(context.Context) error { return domain.ErrNetwork }, []time.Duration{time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Millisecond)
	if count, err := svc.PublishPending(ctx); err != nil || count != 1 {
		t.Fatalf("due retry=%d,%v", count, err)
	}
}

func TestSweeperRepublishesExpiredSubmissionAfterRepublishAfter(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2050, 1, 1, 0, 0, 0, 0, time.UTC)
	now := start
	repo := task.NewMockTaskRepository()
	calls := 0
	svc := NewTaskUseCase(repo, enqueueFunc(func(_ context.Context, id domain.ID) (Publication, error) {
		calls++
		return Publication{ID: id}, nil
	}), func() time.Time { return now })
	svc.RepublishAfter = republishAfter
	svc.StaleAfter = staleAfter
	created, err := svc.Submit(ctx, "test task", []byte("text"))
	if err != nil || !created.IsSubmitted {
		t.Fatalf("submission=%+v,%v", created, err)
	}
	now = start.Add(svc.RepublishAfter - time.Microsecond)
	if count, err := svc.PublishPending(ctx); err != nil || count != 0 {
		t.Fatalf("before RepublishAfter=%d,%v", count, err)
	}
	now = start.Add(svc.RepublishAfter)
	if count, err := svc.PublishPending(ctx); err != nil || count != 1 || calls != 2 {
		t.Fatalf("republication=%d,%v calls=%d", count, err, calls)
	}
	stored, err := repo.Get(ctx, created.ID)
	if err != nil || !stored.IsSubmitted || stored.Version != 1 || !stored.UpdatedAt.Equal(now) {
		t.Fatalf("republished=%+v,%v", stored, err)
	}
	if count, err := svc.PublishPending(ctx); err != nil || count != 0 {
		t.Fatalf("republished again immediately=%d,%v", count, err)
	}
}

// A zero RepublishAfter disables the submitted-task safety net: a submitted,
// pending task is never republished, however old.
func TestSweeperWithoutRepublishAfterLeavesSubmittedTasks(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2050, 1, 1, 0, 0, 0, 0, time.UTC)
	repo := task.NewMockTaskRepository()
	calls := 0
	svc := NewTaskUseCase(repo, enqueueFunc(func(_ context.Context, id domain.ID) (Publication, error) {
		calls++
		return Publication{ID: id}, nil
	}), func() time.Time { return now })
	svc.StaleAfter = staleAfter
	created, err := svc.Submit(ctx, "test task", []byte("text"))
	if err != nil || !created.IsSubmitted {
		t.Fatalf("submission=%+v,%v", created, err)
	}
	now = now.Add(24 * time.Hour)
	if count, err := svc.PublishPending(ctx); err != nil || count != 0 || calls != 1 {
		t.Fatalf("republished with RepublishAfter disabled=%d,%v calls=%d", count, err, calls)
	}
}

// A message delivered before its task is due (clock skew) is ACKed, so the
// task is handed back to the sweeper unsubmitted, with a Version bump, and
// republished once due; an old copy for an unsubmitted task bumps Version too.
func TestNotDueDeliveryHandsSubmittedTaskBackToSweeper(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2050, 1, 1, 0, 0, 0, 0, time.UTC)
	repo := task.NewMockTaskRepository()
	calls := 0
	svc := NewTaskUseCase(repo, enqueueFunc(func(_ context.Context, id domain.ID) (Publication, error) {
		calls++
		return Publication{ID: id}, nil
	}), func() time.Time { return now })
	svc.StaleAfter = staleAfter
	created, err := svc.Submit(ctx, "test task", []byte("text"))
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.ProcessTasks(ctx, []domain.ID{created.ID}, func(context.Context) error { return domain.ErrNetwork }, []time.Duration{time.Minute}); err != nil {
		t.Fatal(err)
	}
	retried, _ := repo.Get(ctx, created.ID)
	// A skewed publisher published the retry early and marked it submitted.
	if err := repo.UpdateByID(ctx, created.ID, retried.Version, func(t *domain.Task) (bool, error) {
		t.MarkSubmitted(now)
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	executions := 0
	work := func(context.Context) error { executions++; return nil }
	if err := svc.ProcessTasks(ctx, []domain.ID{created.ID}, work, nil); err != nil {
		t.Fatal(err)
	}
	handedBack, _ := repo.Get(ctx, created.ID)
	if executions != 0 || handedBack.IsSubmitted || handedBack.Status != domain.Pending || handedBack.Version != retried.Version+1 || !handedBack.UpdatedAt.Equal(now) {
		t.Fatalf("not-due submitted task=%+v executions=%d", handedBack, executions)
	}
	now = now.Add(time.Second)
	if err := svc.ProcessTasks(ctx, []domain.ID{created.ID}, work, nil); err != nil {
		t.Fatal(err)
	}
	if again, _ := repo.Get(ctx, created.ID); executions != 0 || again.IsSubmitted || again.Version != handedBack.Version+1 || !again.NextRetry.Equal(handedBack.NextRetry) {
		t.Fatalf("not-due unsubmitted task=%+v", again)
	}
	if count, err := svc.PublishPending(ctx); err != nil || count != 0 {
		t.Fatalf("published before due=%d,%v", count, err)
	}
	now = handedBack.NextRetry
	if count, err := svc.PublishPending(ctx); err != nil || count != 1 || calls != 2 {
		t.Fatalf("republication when due=%d,%v calls=%d", count, err, calls)
	}
	if republished, _ := repo.Get(ctx, created.ID); !republished.IsSubmitted || republished.Version != handedBack.Version+1 {
		t.Fatalf("republished=%+v", republished)
	}
}

// The sweeper lists a task its clock finds due and publishes it; the message
// reaches a consumer whose clock finds it not due before the publication
// marker is written. The marker must then conflict, so the task stays
// unsubmitted and the sweeper publishes it again when due.
func TestNotDueDeliveryBeforeMarkerLeavesTaskUnsubmitted(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2050, 1, 1, 0, 0, 0, 0, time.UTC)
	repo := task.NewMockTaskRepository()
	created := newPublisherTask(t, repo, now)
	// The consumer's clock runs behind the task's due time.
	consumer := NewTaskUseCase(repo, nil, func() time.Time { return created.NextRetry.Add(-time.Second) })
	executions := 0
	work := func(context.Context) error { executions++; return nil }
	publisher := publisherFunc(func(ctx context.Context, id domain.ID) error {
		return consumer.ProcessTasks(ctx, []domain.ID{id}, work, nil)
	})
	publication := newPublishingTaskUseCase(repo, publisher, 1)
	publication.Clock = func() time.Time { return now }
	if count, err := publication.PublishPending(ctx); err != nil || count != 0 {
		t.Fatalf("raced marker=%d,%v", count, err)
	}
	stored, _ := repo.Get(ctx, created.ID)
	if executions != 0 || stored.IsSubmitted || stored.Status != domain.Pending || stored.Version != created.Version+1 {
		t.Fatalf("marker after an early delivery=%+v executions=%d", stored, executions)
	}
	publication.queue = accepted
	if count, err := publication.PublishPending(ctx); err != nil || count != 1 {
		t.Fatalf("sweeper did not publish again=%d,%v", count, err)
	}
}

func TestSweeperSkipsSubmittedTasksPastPending(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2050, 1, 1, 0, 0, 0, 0, time.UTC)
	repo := task.NewMockTaskRepository()
	svc := NewTaskUseCase(repo, accepted, func() time.Time { return now })
	svc.RepublishAfter = republishAfter
	svc.StaleAfter = 2 * time.Hour // The running claim stays live here.
	var ids []domain.ID
	for range 2 {
		created, err := svc.Submit(ctx, "test task", []byte("text"))
		if err != nil || !created.IsSubmitted {
			t.Fatalf("submission=%+v,%v", created, err)
		}
		ids = append(ids, created.ID)
	}
	// Completed and running.
	if err := svc.ProcessTasks(ctx, ids[:1], func(ctx context.Context) error { return ctx.Err() }, nil); err != nil {
		t.Fatal(err)
	}
	if err := repoClaim(ctx, repo, ids[1], now); err != nil {
		t.Fatal(err)
	}
	calls := 0
	svc.queue = enqueueFunc(func(_ context.Context, id domain.ID) (Publication, error) { calls++; return Publication{ID: id}, nil })
	now = now.Add(time.Hour)
	if count, err := svc.PublishPending(ctx); err != nil || count != 0 || calls != 0 {
		t.Fatalf("swept non-pending submissions=%d,%v calls=%d", count, err, calls)
	}
}

type deletingSnapshotRepository struct{ *task.MockTaskRepository }

func (r *deletingSnapshotRepository) ListPublishable(ctx context.Context, q port.PublishableQuery) ([]*domain.Task, error) {
	tasks, err := r.MockTaskRepository.ListPublishable(ctx, q)
	if err != nil || len(tasks) == 0 {
		return tasks, err
	}
	return tasks, r.DeleteByID(ctx, tasks[0].ID, tasks[0].Version)
}

func (*deletingSnapshotRepository) Get(context.Context, domain.ID) (*domain.Task, error) {
	return nil, errors.New("publisher must use listed snapshots")
}

func TestPublisherHandlesTasksDeletedAfterListing(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	base := task.NewMockTaskRepository()
	newPublisherTask(t, base, now)
	newPublisherTask(t, base, now)
	publisher := &testPublisher{}
	svc := NewTaskUseCase(&deletingSnapshotRepository{base}, publisher, func() time.Time { return now })
	svc.RepublishAfter = republishAfter
	svc.StaleAfter = staleAfter
	if count, err := svc.PublishPending(ctx); err != nil || count != 1 || publisher.calls != 2 {
		t.Fatalf("remaining publication=%d,%v; calls=%d", count, err, publisher.calls)
	}
	remaining, err := base.List(ctx, 3, time.Time{}, "")
	if err != nil || len(remaining) != 1 || !remaining[0].IsSubmitted {
		t.Fatalf("deleted snapshot was restored or remaining task was not marked: %+v err=%v", remaining, err)
	}
}

func TestPublisherPreservesEditsAndDeletesDuringConfirmation(t *testing.T) {
	for _, operation := range []string{"edit", "delete"} {
		t.Run(operation, func(t *testing.T) {
			ctx := context.Background()
			now := time.Now().UTC()
			repo := task.NewMockTaskRepository()
			created := newPublisherTask(t, repo, now)
			publisher := publisherFunc(func(ctx context.Context, id domain.ID) error {
				if operation == "delete" {
					return repo.DeleteByID(ctx, id, created.Version)
				}
				payload, _ := domain.NewPayload([]byte("updated payload"))
				return repo.UpdateByID(ctx, id, created.Version, func(item *domain.Task) (bool, error) {
					return item.Edit("updated name", payload, now.Add(time.Second))
				})
			})
			svc := newPublishingTaskUseCase(repo, publisher, 1)
			svc.Clock = func() time.Time { return now }
			if count, err := svc.PublishPending(ctx); err != nil || count != 0 {
				t.Fatalf("obsolete snapshot was marked: count=%d err=%v", count, err)
			}
			stored, err := repo.Get(ctx, created.ID)
			if operation == "delete" {
				if !errors.Is(err, domain.ErrNotFound) {
					t.Fatalf("deleted task was restored: %+v err=%v", stored, err)
				}
			} else if err != nil || stored.Version != 2 || stored.Name != "updated name" || stored.Payload.String() != "updated payload" || stored.IsSubmitted {
				t.Fatalf("obsolete confirmation changed new revision: %+v err=%v", stored, err)
			}
		})
	}
}

func TestPublisherPublishesBatchInRepositoryOrder(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	repo := task.NewMockTaskRepository()
	for i := 0; i < 3; i++ {
		newPublisherTask(t, repo, now)
	}
	snapshots, err := repo.ListPublishable(ctx, port.PublishableQuery{Limit: 3, Now: now, CreatedBefore: now})
	if err != nil {
		t.Fatal(err)
	}
	expected := make([]domain.ID, len(snapshots))
	for i, snapshot := range snapshots {
		expected[i] = snapshot.ID
	}
	var published []domain.ID
	messageErr := errors.New("message returned by broker")
	publisher := publisherFunc(func(ctx context.Context, id domain.ID) error {
		if len(published) > 0 {
			previous, err := repo.Get(ctx, published[len(published)-1])
			if err != nil || previous.Error != nil || previous.IsSubmitted != (previous.ID != expected[1]) {
				t.Fatalf("previous publication was not recorded before the next: %+v, %v", previous, err)
			}
		}
		published = append(published, id)
		if id == expected[1] {
			return messageErr
		}
		return nil
	})
	publication := newPublishingTaskUseCase(repo, publisher, 3)
	publication.Clock = func() time.Time { return now }
	count, err := publication.PublishPending(ctx)
	if count != 2 || err != nil || !slices.Equal(published, expected) {
		t.Fatalf("published=%v, want ordered %v; count=%d err=%v", published, expected, count, err)
	}
	for i, id := range expected {
		stored, err := repo.Get(ctx, id)
		// A rejected message stays pending and unsubmitted for the next sweep.
		if err != nil || stored.IsSubmitted != (i != 1) || stored.Error != nil || stored.Status != domain.Pending {
			t.Fatalf("publication marker for %s: %+v, %v", id, stored, err)
		}
	}
}

func TestPublisherOperationFailureStopsWithoutChangingRemainingTasks(t *testing.T) {
	for _, mode := range []string{"network", "setup"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			now := time.Now().UTC().Truncate(time.Microsecond)
			repo := task.NewMockTaskRepository()
			for range 3 {
				newPublisherTask(t, repo, now)
			}
			snapshots, err := repo.ListPublishable(ctx, port.PublishableQuery{Limit: 3, Now: now, CreatedBefore: now})
			if err != nil {
				t.Fatal(err)
			}
			want := errors.New("publisher setup failed")
			if mode == "network" {
				want = domain.ErrNetwork
			}
			publisher := &testPublisher{operationErr: want, stopAfter: 2}
			svc := NewTaskUseCase(repo, publisher, func() time.Time { return now })
			svc.RepublishAfter = republishAfter
			svc.StaleAfter = staleAfter
			count, err := svc.PublishPending(ctx)
			completed := 1
			if count != completed || err == nil || !errors.Is(err, want) || publisher.calls != 2 {
				t.Fatalf("operation failure: count=%d calls=%d err=%v", count, publisher.calls, err)
			}
			for i, snapshot := range snapshots {
				stored, err := repo.Get(ctx, snapshot.ID)
				changed := snapshot.UpdatedAt
				if i < completed {
					changed = now
				}
				if err != nil || stored.IsSubmitted != (i < completed) || stored.Status != domain.Pending || stored.Error != nil || stored.RetryCount != 0 || !stored.NextRetry.Equal(snapshot.NextRetry) || !stored.UpdatedAt.Equal(changed) {
					t.Fatalf("operation error changed task: %+v, %v", stored, err)
				}
			}
		})
	}
}

func TestPublicationFailurePreservesCompletedResultAfterFailedMarker(t *testing.T) {
	for _, completeDuringPublish := range []bool{false, true} {
		name := "already completed"
		if completeDuringPublish {
			name = "completes during recovery publication"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			now := time.Now().UTC()
			base := task.NewMockTaskRepository()
			created := newPublisherTask(t, base, now)
			repo := &failMarkerRepository{base, true}
			publication := newPublishingTaskUseCase(repo, &testPublisher{}, 1)
			publication.Clock = func() time.Time { return now }
			if _, err := publication.PublishPending(ctx); err == nil {
				t.Fatal("expected failure recording original confirmed publication")
			}
			repo.fail = false
			svc := NewTaskUseCase(base, accepted, func() time.Time { return now })
			var execution Work = func(ctx context.Context) error { return ctx.Err() }
			complete := func() {
				t.Helper()
				if err := svc.ProcessTasks(ctx, []domain.ID{created.ID}, execution, retryDelays); err != nil {
					t.Fatal(err)
				}
			}
			if !completeDuringPublish {
				complete()
			}
			publishErr := errors.New("access refused")
			publication.queue = publisherFunc(func(context.Context, domain.ID) error {
				if completeDuringPublish {
					complete()
				}
				return publishErr
			})
			if n, err := publication.PublishPending(ctx); n != 0 || err != nil {
				t.Fatalf("recovery=%d,%v", n, err)
			}
			stored, err := base.Get(ctx, created.ID)
			// The claim marked the task submitted; a completed task is never swept.
			if err != nil || stored.Status != domain.Completed || stored.Result == nil || *stored.Result != "4" || stored.Error != nil || !stored.IsSubmitted {
				t.Fatalf("successful execution changed after publication failure: %+v, %v", stored, err)
			}
			publisher := &testPublisher{}
			publication.queue = publisher
			if n, err := publication.PublishPending(ctx); n != 0 || err != nil || publisher.calls != 0 {
				t.Fatalf("completed task was swept=%d,%v calls=%d", n, err, publisher.calls)
			}
		})
	}
}
func TestPublicationFailuresStayPendingForTheNextSweep(t *testing.T) {
	for _, tc := range []struct {
		name     string
		err      error
		terminal bool
	}{
		{"network", errors.Join(domain.ErrNetwork, errors.New("connection reset")), false},
		{"permission", errors.New("access refused"), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now().UTC()
			repo := task.NewMockTaskRepository()
			created := newPublisherTask(t, repo, now)
			publisher := &testPublisher{}
			if tc.terminal {
				publisher.err = tc.err
			} else {
				publisher.operationErr = tc.err
			}
			publication := newPublishingTaskUseCase(repo, publisher, 1)
			publication.Clock = func() time.Time { return now }
			n, err := publication.PublishPending(context.Background())
			if n != 0 || (tc.terminal && err != nil) || (!tc.terminal && !errors.Is(err, tc.err)) || publisher.calls != 1 {
				t.Fatalf("publication=%d,%v", n, err)
			}
			stored, _ := repo.Get(context.Background(), created.ID)
			if stored.IsSubmitted {
				t.Fatal("marked a failed publication")
			}
			if tc.terminal {
				// Rejections may be transient: a sweep after the backoff
				// publishes again.
				if stored.Status != domain.Pending || stored.Error != nil || !stored.NextRetry.Equal(now.Add(publication.PublishRejectBackoff).Truncate(time.Microsecond)) {
					t.Fatalf("rejected=%+v", stored)
				}
				publisher.err = nil
				now = now.Add(publication.PublishRejectBackoff)
				n, err = publication.PublishPending(context.Background())
				stored, _ = repo.Get(context.Background(), created.ID)
				if n != 1 || err != nil || publisher.calls != 2 || !stored.IsSubmitted {
					t.Fatalf("rejected publication was not retried: %d,%v,%+v", n, err, stored)
				}
			} else {
				if stored.Status != domain.Pending || stored.Error != nil || !stored.NextRetry.Equal(created.NextRetry) || !stored.UpdatedAt.Equal(created.UpdatedAt) {
					t.Fatalf("network=%+v", stored)
				}
				publisher.operationErr = nil
				n, err = publication.PublishPending(context.Background())
				stored, _ = repo.Get(context.Background(), created.ID)
				if n != 1 || err != nil || !stored.IsSubmitted {
					t.Fatalf("recovery=%d,%v,%+v", n, err, stored)
				}
			}
		})
	}
}

type failMarkerRepository struct {
	port.TaskRepository
	fail bool
}

func (r *failMarkerRepository) UpdateByID(ctx context.Context, id domain.ID, version int64, fn func(*domain.Task) (bool, error)) error {
	if r.fail {
		return errors.New("marker unavailable")
	}
	return r.TaskRepository.UpdateByID(ctx, id, version, fn)
}
func TestConfirmedPublishWithFailedMarkerRemainsRecoverable(t *testing.T) {
	now := time.Now().UTC()
	base := task.NewMockTaskRepository()
	created := newPublisherTask(t, base, now)
	repo := &failMarkerRepository{base, true}
	publisher := &testPublisher{}
	publication := newPublishingTaskUseCase(repo, publisher, 1)
	publication.Clock = func() time.Time { return now }
	n, err := publication.PublishPending(context.Background())
	if n != 0 || err == nil || publisher.calls != 1 {
		t.Fatalf("marker=%d,%v", n, err)
	}
	stored, _ := base.Get(context.Background(), created.ID)
	if stored.IsSubmitted || stored.Error != nil {
		t.Fatalf("marker=%+v", stored)
	}
	repo.fail = false
	n, err = publication.PublishPending(context.Background())
	stored, _ = base.Get(context.Background(), created.ID)
	if n != 1 || err != nil || publisher.calls != 2 || !stored.IsSubmitted {
		t.Fatalf("recovery=%d,%v,%+v", n, err, stored)
	}
}

func TestPublisherStopsBeforeNextTaskWhenMarkerFails(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	base := task.NewMockTaskRepository()
	for range 2 {
		newPublisherTask(t, base, now)
	}
	snapshots, err := base.ListPublishable(ctx, port.PublishableQuery{Limit: 2, Now: now, CreatedBefore: now})
	if err != nil {
		t.Fatal(err)
	}
	publisher := &testPublisher{}
	publication := newPublishingTaskUseCase(&failMarkerRepository{base, true}, publisher, 2)
	publication.Clock = func() time.Time { return now }
	if count, err := publication.PublishPending(ctx); count != 0 || err == nil || publisher.calls != 1 || publisher.last != snapshots[0].ID {
		t.Fatalf("marker failure did not stop publication: count=%d calls=%d last=%s err=%v", count, publisher.calls, publisher.last, err)
	}
	for _, snapshot := range snapshots {
		stored, err := base.Get(ctx, snapshot.ID)
		if err != nil || stored.IsSubmitted || stored.Error != nil || stored.Status != domain.Pending {
			t.Fatalf("marker failure changed recoverable task: %+v, %v", stored, err)
		}
	}
}
func TestPublishConfirmationDoesNotEraseConcurrentExecutionRetry(t *testing.T) {
	now := time.Now().UTC()
	repo := task.NewMockTaskRepository()
	svc := NewTaskUseCase(repo, accepted, func() time.Time { return now })
	var execution Work = func(context.Context) error { return domain.ErrNetwork }
	created := newPublisherTask(t, repo, now)
	publisher := publisherFunc(func(ctx context.Context, id domain.ID) error {
		return svc.ProcessTasks(ctx, []domain.ID{id}, execution, retryDelays)
	})
	publication := newPublishingTaskUseCase(repo, publisher, 1)
	publication.Clock = func() time.Time { return now }
	if count, err := publication.PublishPending(context.Background()); err != nil || count != 0 {
		t.Fatalf("superseded publication=%d,%v", count, err)
	}
	stored, _ := repo.Get(context.Background(), created.ID)
	if stored.Status != domain.Pending || stored.IsSubmitted || stored.RetryCount != 1 {
		t.Fatalf("retry publication lost: %+v", stored)
	}
}

func newPublishingTaskUseCase(repo port.TaskRepository, queue TaskQueue, batch int) *TaskUseCase {
	svc := NewTaskUseCase(repo, queue, time.Now)
	svc.SweeperBatch = batch
	svc.RepublishAfter = republishAfter
	svc.StaleAfter = staleAfter
	svc.RetryDelays = retryDelays
	return svc
}

// republishAfter enables the optional submitted-task safety net (disabled by
// default via TASK_REPUBLISH_AFTER=0).
const republishAfter = 45 * time.Second

// staleAfter is shorter than the TASK_STALE_AFTER default to keep tests quick;
// it is still 2*Timeout+10s for the default 10s Timeout.
const staleAfter = 30 * time.Second

func TestPublishPendingValidatesRepublishAfterAndStaleAfter(t *testing.T) {
	svc := NewTaskUseCase(task.NewMockTaskRepository(), accepted, time.Now)
	svc.StaleAfter = staleAfter
	if _, err := svc.PublishPending(context.Background()); err != nil {
		t.Fatalf("rejected a disabled RepublishAfter: %v", err)
	}
	svc.RepublishAfter = -time.Second
	if _, err := svc.PublishPending(context.Background()); err == nil {
		t.Fatal("accepted a negative RepublishAfter")
	}
	svc.RepublishAfter, svc.StaleAfter = republishAfter, 0
	if _, err := svc.PublishPending(context.Background()); err == nil {
		t.Fatal("accepted a missing StaleAfter")
	}
	svc.StaleAfter = -time.Second
	if _, err := svc.PublishPending(context.Background()); err == nil {
		t.Fatal("accepted a negative StaleAfter")
	}
	svc.StaleAfter, svc.PublishRejectBackoff = staleAfter, 0
	if _, err := svc.PublishPending(context.Background()); err == nil {
		t.Fatal("accepted a missing PublishRejectBackoff")
	}
	svc.PublishRejectBackoff, svc.RetryDelays = time.Minute, []time.Duration{-time.Second}
	if _, err := svc.PublishPending(context.Background()); err == nil {
		t.Fatal("accepted a negative retry delay")
	}
}

// failReleaseRepository fails the stale release.
type failReleaseRepository struct{ port.TaskRepository }

func (failReleaseRepository) ReleaseStale(context.Context, time.Duration, []time.Duration) (int64, error) {
	return 0, errors.New("release unavailable")
}

// A failed stale release is logged; the sweep still publishes.
func TestReleaseStaleFailureDoesNotAbortSweep(t *testing.T) {
	now := time.Now().UTC()
	repo := task.NewMockTaskRepository()
	created := newPublisherTask(t, repo, now)
	publisher := &testPublisher{}
	svc := newPublishingTaskUseCase(failReleaseRepository{repo}, publisher, 8)
	svc.Clock = func() time.Time { return now }
	if count, err := svc.PublishPending(context.Background()); err != nil || count != 1 || publisher.last != created.ID {
		t.Fatalf("sweep after failed release=%d,%v", count, err)
	}
}

// A rejected publication is postponed by PublishRejectBackoff: other due tasks
// take the sweep slots meanwhile, and it is listed again after the backoff.
func TestRejectedPublicationBacksOff(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	repo := task.NewMockTaskRepository()
	rejected := newPublisherTask(t, repo, now.Add(-time.Second)) // First in order.
	var published []domain.ID
	publisher := publisherFunc(func(_ context.Context, id domain.ID) error {
		published = append(published, id)
		if id == rejected.ID {
			return errors.New("message returned by broker")
		}
		return nil
	})
	svc := newPublishingTaskUseCase(repo, publisher, 1)
	svc.Clock = func() time.Time { return now }
	if count, err := svc.PublishPending(ctx); err != nil || count != 0 || !slices.Equal(published, []domain.ID{rejected.ID}) {
		t.Fatalf("rejection=%d,%v published=%v", count, err, published)
	}
	postponed, _ := repo.Get(ctx, rejected.ID)
	if postponed.IsSubmitted || postponed.Version != rejected.Version || !postponed.NextRetry.Equal(now.Add(svc.PublishRejectBackoff)) || !postponed.UpdatedAt.Equal(now) {
		t.Fatalf("postponed=%+v", postponed)
	}
	// A task due later than the rejected one now takes the single slot.
	other := newPublisherTask(t, repo, now)
	now = now.Add(svc.PublishRejectBackoff - time.Microsecond)
	for range 2 {
		if _, err := svc.PublishPending(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if !slices.Equal(published, []domain.ID{rejected.ID, other.ID}) {
		t.Fatalf("published during backoff=%v", published)
	}
	now = now.Add(time.Microsecond)
	if count, err := svc.PublishPending(ctx); err != nil || count != 0 || !slices.Equal(published, []domain.ID{rejected.ID, other.ID, rejected.ID}) {
		t.Fatalf("after backoff=%d,%v published=%v", count, err, published)
	}
}

// The sweeper releases running claims not updated for StaleAfter, spending one
// retry with its backoff, and republishes them once due; fresh claims stay.
func TestSweeperReleasesOnlyStaleClaimsAndRepublishesThem(t *testing.T) {
	ctx := context.Background()
	// The repository judges staleness by its own clock (the mock's time.Now()),
	// not svc.Clock, so claims are stamped relative to the real time.
	start := time.Now().UTC()
	repo := task.NewMockTaskRepository()
	stale := newPublisherTask(t, repo, start.Add(-staleAfter))
	fresh := newPublisherTask(t, repo, start.Add(-staleAfter))
	if err := repoClaim(ctx, repo, stale.ID, start.Add(-staleAfter-time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := repoClaim(ctx, repo, fresh.ID, start); err != nil {
		t.Fatal(err)
	}
	publisher := &testPublisher{}
	svc := newPublishingTaskUseCase(repo, publisher, 8)
	// A sweeper clock far ahead does not release the fresh claim.
	svc.Clock = func() time.Time { return start.Add(time.Hour) }
	if count, err := svc.PublishPending(ctx); err != nil || count != 1 || publisher.calls != 1 || publisher.last != stale.ID {
		t.Fatalf("stale sweep=%d,%v calls=%d", count, err, publisher.calls)
	}
	released, _ := repo.Get(ctx, stale.ID)
	if released.Status != domain.Pending || released.IsRunning || !released.IsSubmitted || released.RetryCount != 1 || released.Version != 3 {
		t.Fatalf("released=%+v", released)
	}
	if live, _ := repo.Get(ctx, fresh.ID); !live.IsRunning || live.RetryCount != 0 || live.Version != 2 {
		t.Fatalf("fresh claim released=%+v", live)
	}
}

func TestEditAndDeleteWithoutIfMatchFollowBackgroundChanges(t *testing.T) {
	ctx := context.Background()
	repo := task.NewMockTaskRepository()
	svc := NewTaskUseCase(repo, accepted, time.Now)
	created, err := svc.Submit(ctx, "name", []byte("text"))
	if err != nil {
		t.Fatal(err)
	}
	// A background claim and outcome change Version after the client read it.
	if err := svc.ProcessTasks(ctx, []domain.ID{created.ID}, func(ctx context.Context) error { return ctx.Err() }, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Update(ctx, string(created.ID), "renamed", []byte("text"), created.Version); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("stale If-Match PUT=%v", err)
	}
	edited, err := svc.Update(ctx, string(created.ID), "renamed", []byte("text"), 0)
	if err != nil || edited.Name != "renamed" || edited.Version != 4 {
		t.Fatalf("PUT without If-Match=%+v,%v", edited, err)
	}
	if err := svc.Delete(ctx, string(created.ID), created.Version); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("stale If-Match DELETE=%v", err)
	}
	if err := svc.Delete(ctx, string(created.ID), 0); err != nil {
		t.Fatalf("DELETE without If-Match=%v", err)
	}
	if _, err := svc.Get(ctx, string(created.ID)); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("deleted get=%v", err)
	}
}

type enqueueFunc func(context.Context, domain.ID) (Publication, error)

func (f enqueueFunc) Enqueue(ctx context.Context, id domain.ID) (Publication, error) {
	return f(ctx, id)
}

// afterListRepository calls after once the sweeper batch has been listed.
type afterListRepository struct {
	port.TaskRepository
	after func()
}

func (r *afterListRepository) ListPublishable(ctx context.Context, q port.PublishableQuery) ([]*domain.Task, error) {
	tasks, err := r.TaskRepository.ListPublishable(ctx, q)
	r.after()
	return tasks, err
}

func TestPublishPendingSeparatesMessageAndOperationErrors(t *testing.T) {
	messageErr := errors.New("unroutable message")
	operationErr := errors.New("channel closed")
	for _, mode := range []string{"success", "message", "operation", "publisher deadline", "canceled before", "expired before"} {
		t.Run(mode, func(t *testing.T) {
			now := time.Now().UTC()
			base := task.NewMockTaskRepository()
			created := newPublisherTask(t, base, now)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var repo port.TaskRepository = base
			var wantError error
			switch mode {
			case "operation":
				wantError = operationErr
			case "publisher deadline":
				wantError = context.DeadlineExceeded
			case "canceled before":
				// Cancel between listing and Enqueue: the queue reports cancellation.
				repo, wantError = &afterListRepository{base, cancel}, context.Canceled
			case "expired before":
				var stop context.CancelFunc
				ctx, stop = context.WithDeadline(ctx, time.Now().Add(-time.Second))
				defer stop()
				wantError = context.DeadlineExceeded
			}
			calls := 0
			var queue TaskQueue = enqueueFunc(func(gotCtx context.Context, gotID domain.ID) (Publication, error) {
				calls++
				if err := gotCtx.Err(); err != nil {
					return Publication{}, err // TaskQueue context contract.
				}
				if gotCtx != ctx || gotID != created.ID {
					t.Fatalf("queue received context=%v id=%s, want context=%v id=%s", gotCtx, gotID, ctx, created.ID)
				}
				switch mode {
				case "message":
					return Publication{ID: gotID, Err: messageErr}, nil
				case "operation":
					return Publication{}, operationErr
				case "publisher deadline":
					return Publication{}, context.DeadlineExceeded
				}
				return Publication{ID: gotID}, nil
			})
			svc := newPublishingTaskUseCase(repo, queue, 1)
			svc.Clock = func() time.Time { return now }
			count, err := svc.PublishPending(ctx)
			wantCalls := 1
			if mode == "expired before" {
				wantCalls = 0
			}
			if calls != wantCalls {
				t.Fatalf("queue calls=%d, want %d", calls, wantCalls)
			}
			stored, _ := base.Get(context.Background(), created.ID)
			if wantError != nil {
				if count != 0 || err == nil || !errors.Is(err, wantError) || stored.IsSubmitted || stored.Error != nil {
					t.Fatalf("operation failure count=%d err=%v stored=%+v, want error=%v", count, err, stored, wantError)
				}
				return
			}
			if err != nil || (mode == "message" && (count != 0 || stored.Error != nil || stored.IsSubmitted || stored.Status != domain.Pending)) || (mode == "success" && (count != 1 || !stored.IsSubmitted)) {
				t.Fatalf("message publication count=%d err=%v stored=%+v", count, err, stored)
			}
		})
	}
}

func TestDelayHonorsTimeoutAndCancellation(t *testing.T) {
	if err := Delay(time.Millisecond)(context.Background()); err != nil {
		t.Fatal(err)
	}
	work := Delay(time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := work(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout = %v", err)
	}
	ctx, stop := context.WithCancel(context.Background())
	stop()
	if err := work(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation = %v", err)
	}
}

func TestProcessTasksLimitsConcurrencyAndJoins(t *testing.T) {
	ctx := context.Background()
	repo := task.NewMockTaskRepository()
	svc := NewTaskUseCase(repo, accepted, time.Now)
	svc.Parallelism = 2
	var ids []domain.ID
	for range 4 {
		created, err := svc.Submit(ctx, "test task", []byte("text"))
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, created.ID)
	}
	started := make(chan struct{}, len(ids))
	release := make(chan struct{})
	var active, peak, calls atomic.Int64
	work := func(context.Context) error {
		current := active.Add(1)
		defer active.Add(-1)
		for previous := peak.Load(); current > previous && !peak.CompareAndSwap(previous, current); previous = peak.Load() {
		}
		calls.Add(1)
		started <- struct{}{}
		<-release
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- svc.ProcessTasks(ctx, ids, work, nil) }()
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			close(release)
			t.Fatal("did not start two executions")
		}
	}
	select {
	case <-started:
		close(release)
		t.Fatal("exceeded concurrency limit")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if peak.Load() != 2 || active.Load() != 0 || calls.Load() != int64(len(ids)) {
		t.Fatalf("peak=%d active=%d calls=%d", peak.Load(), active.Load(), calls.Load())
	}
	for _, id := range ids {
		if stored, err := repo.Get(ctx, id); err != nil || stored.Status != domain.Completed || stored.IsRunning {
			t.Fatalf("task not completed after join: %+v, %v", stored, err)
		}
	}
}
