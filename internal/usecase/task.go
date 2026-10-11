package usecase

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/bright2227/gogolook-task-system/internal/usecase/port"

	"github.com/bright2227/gogolook-task-system/internal/domain"
)

// TaskQueue enqueues task IDs for execution. Err in Publication affects that
// message only; a returned error is an operation failure that stops the batch.
// Context contract: Enqueue checks ctx itself and returns ctx.Err() when ctx is
// done before or while it publishes, so callers do not re-check ctx. A caller
// that also closes the connection on cancellation may instead receive that
// transport error; such a caller checks its own ctx to tell shutdown apart.
type TaskQueue interface {
	Enqueue(context.Context, domain.ID) (Publication, error)
}
type Publication struct {
	ID  domain.ID
	Err error
}

// TaskUseCase holds the repository, the task queue, the clock and static
// configuration only. Execution work and the consumer's retry policy are
// per-call arguments of ProcessTasks.
type TaskUseCase struct {
	repo         port.TaskRepository
	queue        TaskQueue
	Clock        func() time.Time
	Timeout      time.Duration
	SweeperBatch int
	// PublishGrace leaves new tasks to the Submit fast path before the sweeper
	// publishes them.
	PublishGrace time.Duration
	// RepublishAfter republishes submitted tasks still pending and unclaimed
	// and unchanged for this long: a last-resort safety net for messages lost
	// by the broker (queue deleted or purged). Zero disables it. When enabled it
	// must exceed the longest time a message can wait in the queue plus one
	// batch, or live messages are duplicated.
	RepublishAfter time.Duration
	// Parallelism bounds concurrent executions within one consumed batch.
	Parallelism int
	// StaleAfter releases running claims not updated for this long, measured
	// by the database clock. It must exceed the longest execution of one batch
	// plus its outcome commit and clock skew between consumer hosts (which
	// stamp claims) and the database host. It has no default: the sweeper
	// sets TASK_STALE_AFTER.
	StaleAfter time.Duration
	// RetryDelays is used only by PublishPending's stale release: an
	// interrupted execution spends the same retry budget, with the same
	// backoff, as a network failure in ProcessTasks, so the sweeper is
	// configured with the consumers' TASK_RETRY_DELAYS.
	RetryDelays []time.Duration
	// PublishRejectBackoff postpones a task whose publication the broker
	// rejected, so it does not take the first sweep slots again meanwhile.
	PublishRejectBackoff time.Duration
}

// NewTaskUseCase accepts a nil queue for callers that never call Submit or
// PublishPending.
func NewTaskUseCase(repository port.TaskRepository, queue TaskQueue, clock func() time.Time) *TaskUseCase {
	return &TaskUseCase{repo: repository, queue: queue, Clock: clock, Timeout: 10 * time.Second, SweeperBatch: 8, PublishGrace: 5 * time.Second, Parallelism: 4, PublishRejectBackoff: time.Minute}
}

// Work performs one task's processing before execute computes its result.
//
// Context contract — callers rely on it and do not re-check ctx:
//   - ctx carries the per-task deadline (TaskUseCase.Timeout) and process
//     shutdown. Work must stop promptly when ctx is done.
//   - Once ctx is done, Work must return ctx.Err(), even if its own processing
//     finished at the same moment. It must never return nil after ctx is done:
//     nil is committed as a successful outcome.
//   - Other failures are returned as errors. Wrap transport failures with
//     domain.ErrNetwork. Network errors and the per-task deadline
//     (context.DeadlineExceeded) spend the retry budget; every other error
//     completes the task with it. An error caused by process shutdown releases
//     the claim instead (see ProcessTasks).
//   - A panic is recovered and completes that task with a "task work
//     panicked" error; the other tasks of the batch are unaffected.
type Work func(context.Context) error

func (s *TaskUseCase) Submit(ctx context.Context, name string, raw []byte) (*domain.Task, error) {
	if err := domain.ValidateName(name); err != nil {
		return nil, err
	}
	p, err := domain.NewPayload(raw)
	if err != nil {
		return nil, err
	}
	t, err := domain.Submit(p, s.Clock())
	if err != nil {
		return nil, err
	}
	t.Name = name
	if err = s.repo.Insert(ctx, t); err != nil {
		return nil, err
	}
	// Fast path: the row is persisted, so any publication or marker failure is
	// left to the PublishPending sweeper and Submit still succeeds.
	publication, err := s.queue.Enqueue(ctx, t.ID)
	if err == nil {
		err = publication.Err
	}
	var marked *domain.Task
	if err == nil {
		err = s.repo.UpdateByID(ctx, t.ID, t.Version, func(current *domain.Task) (bool, error) {
			marked = current
			current.MarkSubmitted(s.Clock())
			return true, nil
		})
	}
	if errors.Is(err, domain.ErrConflict) || errors.Is(err, domain.ErrNotFound) {
		slog.Info("publication marker skipped", "id", t.ID, "reason", "task changed before publication marker, e.g. claimed by a consumer", "error", err)
		// Return the current row; the inserted snapshot only if it cannot be read.
		if current, getErr := s.repo.Get(ctx, t.ID); getErr == nil {
			return current, nil
		}
		return t, nil
	}
	if err != nil {
		slog.Warn("task publication deferred to sweeper", "id", t.ID, "error", err)
		return t, nil
	}
	return marked, nil
}
func (s *TaskUseCase) Get(ctx context.Context, rawID string) (*domain.Task, error) {
	id, err := domain.ParseID(rawID)
	if err != nil {
		return nil, err
	}
	return s.repo.Get(ctx, id)
}
func (s *TaskUseCase) List(ctx context.Context, pageSize int, before time.Time, beforeID domain.ID) ([]*domain.Task, bool, error) {
	if pageSize < 1 || pageSize > 100 || before.IsZero() != (beforeID == "") {
		return nil, false, domain.ErrPagination
	}
	if beforeID != "" {
		if _, err := domain.ParseID(string(beforeID)); err != nil {
			return nil, false, domain.ErrPagination
		}
	}
	items, err := s.repo.List(ctx, pageSize+1, before, beforeID)
	if err != nil {
		return nil, false, err
	}
	hasMore := len(items) > pageSize
	if hasMore {
		items = items[:pageSize]
	}
	return items, hasMore, nil
}

// expectedVersion=0 (no If-Match) edits the current row under its lock, so a
// background claim or outcome commit is never a conflict; a running task is
// still ErrBusy. A nonzero expectedVersion must match the locked row.
func (s *TaskUseCase) Update(ctx context.Context, rawID, name string, raw []byte, expectedVersion int64) (*domain.Task, error) {
	id, err := domain.ParseID(rawID)
	if err != nil {
		return nil, err
	}
	payload, err := domain.NewPayload(raw)
	if err != nil {
		return nil, err
	}
	var out *domain.Task
	err = s.repo.UpdateByID(ctx, id, expectedVersion, func(t *domain.Task) (bool, error) {
		changed, err := t.Edit(name, payload, s.Clock())
		out = t
		return changed, err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
func (s *TaskUseCase) Delete(ctx context.Context, rawID string, expectedVersion int64) error {
	id, err := domain.ParseID(rawID)
	if err != nil {
		return err
	}
	return s.repo.DeleteByID(ctx, id, expectedVersion)
}

// ProcessTasks uses two database batches with execution between them.
// IDs are validated at delivery boundaries; current rows control claim/completion.
// A running row is always skipped: a message, even a redelivered one, does not
// prove that the claim's holder is gone (see docs/adr-001). Only the sweeper's
// stale release recovers the claims of crashed consumers.
// When ctx is done after execution (shutdown), outcomes are still committed
// under a short detached context: finished work is kept and claims interrupted
// by the shutdown are released without spending a budget. It then returns
// ctx.Err() so the batch is not acknowledged; if that commit fails, the
// sweeper releases the claims once stale.
func (s *TaskUseCase) ProcessTasks(ctx context.Context, taskIDs []domain.ID, work Work, retryDelays []time.Duration) error {
	if s.Timeout <= 0 || work == nil {
		return errors.New("invalid execution configuration")
	}
	ids := make([]domain.ID, 0, len(taskIDs))
	seen := make(map[domain.ID]struct{}, len(taskIDs))
	for _, id := range taskIDs {
		if _, exists := seen[id]; !exists {
			ids = append(ids, id)
			seen[id] = struct{}{}
		}
	}
	if len(ids) == 0 {
		return nil
	}
	var processingTasks []*domain.Task
	err := s.repo.UpdateByIDs(ctx, ids, func(currentTasks []*domain.Task) (bool, error) {
		processingTasks = make([]*domain.Task, 0, len(currentTasks))
		changed := false
		for _, task := range currentTasks {
			now := s.Clock()
			started, err := task.Start(now)
			if errors.Is(err, domain.ErrNotDue) {
				// The message is ACKed, so the task goes back to the sweeper.
				// Unsubmit always bumps Version, so the publication marker of
				// this (early) message conflicts if it is still to be written.
				task.Unsubmit(now)
				changed = true
				continue
			}
			if err != nil {
				return false, err
			}
			changed = changed || started
			if started {
				processingTask, err := domain.Restore(*task)
				if err != nil {
					return false, err
				}
				processingTasks = append(processingTasks, processingTask)
			}
		}
		if len(currentTasks) < len(ids) {
			slog.Warn("missing task IDs in consumer batch", "requested", len(ids), "found", len(currentTasks))
		}
		return changed, nil
	})
	if err != nil || len(processingTasks) == 0 {
		return err
	}
	results := s.executeTasks(ctx, processingTasks, work)
	outcomes := make(map[domain.ID]executionResult, len(results))
	for _, result := range results {
		outcomes[result.ID] = result
	}
	commitCtx, stopped := ctx, ctx.Err()
	if stopped != nil {
		var cancel context.CancelFunc
		commitCtx, cancel = context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
	}
	err = s.repo.UpdateByIDs(commitCtx, ids, func(currentTasks []*domain.Task) (bool, error) {
		changed := false
		for _, task := range currentTasks {
			o, ok := outcomes[task.ID]
			if !ok || task.Status != domain.Processing || !task.IsRunning || task.Version != o.Version {
				continue
			}
			now := s.Clock()
			var updated bool
			var err error
			if stopped != nil && o.Err != nil && errors.Is(o.Err, stopped) {
				updated = task.Release(now)
			} else if o.Err == nil {
				updated, err = task.Finish(now, &o.Result, nil)
			} else {
				if retryable(o.Err) && task.RetryCount < len(retryDelays) {
					updated, err = task.Retry(now, now.Add(retryDelays[task.RetryCount]))
				} else {
					detail := privateError(o.Err.Error())
					updated, err = task.Finish(now, nil, &detail)
				}
			}
			if err != nil {
				return false, err
			}
			changed = changed || updated
		}
		return changed, nil
	})
	if stopped != nil {
		return stopped
	}
	return err
}

// retryable reports transient execution failures: transport errors and the
// per-task deadline. Shutdown cancellation is handled before this (Release).
func retryable(err error) bool {
	if errors.Is(err, context.Canceled) {
		return false
	}
	return errors.Is(err, domain.ErrNetwork) || errors.Is(err, context.DeadlineExceeded)
}

func privateError(detail string) string {
	detail = strings.ToValidUTF8(strings.TrimSpace(strings.ReplaceAll(detail, "\x00", "�")), "�")
	if detail == "" {
		return "task execution failed"
	}
	if len(detail) > 4096 {
		detail = detail[:4096]
		for !utf8.ValidString(detail) {
			detail = detail[:len(detail)-1]
		}
	}
	return detail
}

// executionResult captures a claimed revision before its outcome is persisted.
type executionResult struct {
	ID      domain.ID
	Version int64
	Result  string
	Err     error
}

// Delay returns Work that waits d. It follows the Work context contract: the
// timer and ctx.Done may be ready together, so both branches return ctx.Err().
func Delay(d time.Duration) Work {
	return func(ctx context.Context) error {
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return ctx.Err()
		}
	}
}

// execute owns the task's deadline and Unicode payload-length result.
func (s *TaskUseCase) execute(ctx context.Context, payload domain.Payload, work Work) (string, error) {
	if s.Timeout <= 0 || work == nil {
		return "", errors.New("invalid execution configuration")
	}
	workCtx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()
	if err := runWork(workCtx, work); err != nil {
		return "", err
	}
	return strconv.Itoa(utf8.RuneCountInString(payload.String())), nil
}

// runWork turns a panic in work into this task's error, so it completes that
// task (a non-retryable failure) and the process and the batch survive.
func runWork(ctx context.Context, work Work) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("task work panicked: %v", r)
		}
	}()
	return work(ctx)
}

// executeTasks runs at most Parallelism tasks at once and joins all of them
// before returning, so every claimed task has a result for the outcome commit.
func (s *TaskUseCase) executeTasks(ctx context.Context, tasks []*domain.Task, work Work) []executionResult {
	results := make([]executionResult, len(tasks))
	workers := min(max(s.Parallelism, 1), len(tasks))
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Go(func() {
			for i := worker; i < len(tasks); i += workers {
				task := tasks[i]
				result, err := s.execute(ctx, task.Payload, work)
				results[i] = executionResult{ID: task.ID, Version: task.Version, Result: result, Err: err}
			}
		})
	}
	wg.Wait()
	return results
}

// PublishPending is the recovery sweeper; the primary publication happens in
// Submit. It first releases running claims not updated for StaleAfter (their
// consumer is gone), spending one retry from RetryDelays each; a failed release
// is logged and the sweep goes on. It then publishes due unsubmitted tasks (new
// ones only after PublishGrace), and, when RepublishAfter is set, republishes
// submitted tasks still pending and unclaimed after it, whose message the
// broker may have lost. A task whose message the broker rejects is postponed
// by PublishRejectBackoff. It runs in one sweeper goroutine and
// intentionally has no concurrency.
func (s *TaskUseCase) PublishPending(ctx context.Context) (int, error) {
	if s.SweeperBatch < 1 || s.SweeperBatch > 1024 || s.PublishGrace <= 0 || s.RepublishAfter < 0 || s.StaleAfter <= 0 || s.PublishRejectBackoff <= 0 || slices.ContainsFunc(s.RetryDelays, func(d time.Duration) bool { return d < 0 }) {
		return 0, errors.New("invalid submission recovery configuration")
	}
	now := s.Clock()
	released, err := s.repo.ReleaseStale(ctx, s.StaleAfter, s.RetryDelays)
	if err != nil {
		slog.Error("stale running claims not released; publishing anyway", "error", err)
	} else if released > 0 {
		slog.Warn("stale running claims released", "count", released)
	}
	query := port.PublishableQuery{Limit: s.SweeperBatch, Now: now, CreatedBefore: now.Add(-s.PublishGrace)}
	if s.RepublishAfter > 0 {
		query.SubmittedBefore = now.Add(-s.RepublishAfter)
	}
	tasks, err := s.repo.ListPublishable(ctx, query)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, task := range tasks {
		id := task.ID
		publication, err := s.queue.Enqueue(ctx, id)
		if err != nil {
			return count, err
		}
		if publication.ID != id {
			return count, errors.New("invalid publication result ID")
		}
		if publication.Err != nil {
			// A NACK or mandatory return can be transient (e.g. the queue is
			// being redeclared), so the task stays unsubmitted; it is postponed
			// so that it does not take the first slots of every sweep.
			slog.Warn("publication rejected; retrying after backoff", "id", id, "backoff", s.PublishRejectBackoff.String(), "error", publication.Err)
			err = s.repo.UpdateByID(ctx, id, task.Version, func(t *domain.Task) (bool, error) {
				at := s.Clock()
				t.Postpone(at, at.Add(s.PublishRejectBackoff))
				return true, nil
			})
		} else {
			err = s.repo.UpdateByID(ctx, id, task.Version, func(t *domain.Task) (bool, error) {
				t.MarkSubmitted(s.Clock())
				return true, nil
			})
		}
		if errors.Is(err, domain.ErrConflict) || errors.Is(err, domain.ErrNotFound) {
			slog.Info("publication marker skipped", "id", id, "reason", "task changed before publication marker, e.g. claimed by a consumer", "error", err)
			continue
		}
		if err != nil {
			return count, err
		}
		if publication.Err == nil {
			count++
		}
	}
	return count, nil
}
