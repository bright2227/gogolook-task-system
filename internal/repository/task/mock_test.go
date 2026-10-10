package task

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bright2227/gogolook-task-system/internal/usecase/port"

	"github.com/bright2227/gogolook-task-system/internal/domain"
)

func newMockTask(t *testing.T, repo *MockTaskRepository) *domain.Task {
	t.Helper()
	payload, err := domain.NewPayload([]byte(`{"n":1}`))
	if err != nil {
		t.Fatal(err)
	}
	task, err := domain.Submit(payload, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if err = repo.Insert(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	return task
}

func TestUpdateByIDRollsBackCallbackError(t *testing.T) {
	repo := NewMockTaskRepository()
	created := newMockTask(t, repo)
	before := created
	want := errors.New("transaction failed")
	err := repo.UpdateByID(context.Background(), before.ID, before.Version, func(task *domain.Task) (bool, error) {
		changed, e := task.Start(time.Now())
		if e != nil || !changed {
			t.Fatalf("start=%v err=%v", changed, e)
		}
		return true, want
	})
	if !errors.Is(err, want) {
		t.Fatalf("UpdateByID err=%v", err)
	}
	after, err := repo.Get(context.Background(), before.ID)
	if err != nil {
		t.Fatal(err)
	}
	snap := after
	if snap.Status != domain.Pending || snap.Error != nil {
		t.Fatalf("rollback leaked state: snap=%+v", snap)
	}
	err = repo.UpdateByID(context.Background(), before.ID, before.Version, func(task *domain.Task) (bool, error) {
		task.Name = "invalid\x00name"
		return true, nil
	})
	if !errors.Is(err, domain.ErrName) {
		t.Fatalf("invalid mutation err=%v", err)
	}
	after, err = repo.Get(context.Background(), before.ID)
	if err != nil || after.Name != before.Name || after.Version != before.Version {
		t.Fatalf("invalid mutation persisted: task=%+v err=%v", after, err)
	}
	err = repo.UpdateByID(context.Background(), before.ID, before.Version, func(task *domain.Task) (bool, error) {
		task.ID = domain.NewID()
		return true, nil
	})
	if !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("changed identity err=%v", err)
	}
	after, err = repo.Get(context.Background(), before.ID)
	if err != nil || after.ID != before.ID || after.Status != before.Status {
		t.Fatalf("changed identity persisted: task=%+v err=%v", after, err)
	}
}

func TestUpdateByIDNoChangeDoesNotPersistCallbackMutation(t *testing.T) {
	repo := NewMockTaskRepository()
	created := newMockTask(t, repo)
	before := created
	err := repo.UpdateByID(context.Background(), before.ID, before.Version, func(task *domain.Task) (bool, error) {
		_, err := task.Start(time.Now())
		task.ID = domain.NewID()
		return false, err
	})
	if err != nil {
		t.Fatal(err)
	}
	after, err := repo.Get(context.Background(), before.ID)
	if err != nil {
		t.Fatal(err)
	}
	if snap := after; snap.Status != domain.Pending {
		t.Fatalf("no-change callback persisted: %+v", snap)
	}
}

func TestUpdateByIDHonorsCanceledContext(t *testing.T) {
	repo := NewMockTaskRepository()
	created := newMockTask(t, repo)
	before := created
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := repo.UpdateByID(ctx, before.ID, before.Version, func(task *domain.Task) (bool, error) {
		t.Fatal("callback ran with canceled context")
		return false, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("UpdateByID err=%v", err)
	}
	after, err := repo.Get(context.Background(), before.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Status != before.Status {
		t.Fatal("canceled update changed task")
	}
	for _, changed := range []bool{true, false} {
		ctx, cancel := context.WithCancel(context.Background())
		err = repo.UpdateByID(ctx, before.ID, before.Version, func(task *domain.Task) (bool, error) {
			_, startErr := task.Start(time.Now())
			cancel()
			return changed, startErr
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation after callback changed=%v err=%v", changed, err)
		}
		after, err = repo.Get(context.Background(), before.ID)
		if err != nil || after.Status != before.Status || after.IsRunning != before.IsRunning {
			t.Fatalf("canceled callback persisted: task=%+v err=%v", after, err)
		}
	}
}

func TestBatchUpdateRollsBackAllRows(t *testing.T) {
	repo := NewMockTaskRepository()
	first := newMockTask(t, repo)
	second := newMockTask(t, repo)
	err := repo.UpdateByIDs(context.Background(), []domain.ID{first.ID, second.ID, first.ID, domain.NewID()}, func(items []*domain.Task) (bool, error) {
		if len(items) != 2 {
			t.Fatalf("unique existing rows=%d", len(items))
		}
		for _, item := range items {
			_, _ = item.Start(time.Now())
		}
		return true, errors.New("rollback")
	})
	if err == nil {
		t.Fatal("missing rollback error")
	}
	for _, id := range []domain.ID{first.ID, second.ID} {
		item, _ := repo.Get(context.Background(), id)
		if item.Status != domain.Pending {
			t.Fatalf("batch mutation persisted: %+v", item)
		}
	}
}
func TestListUsesStableKeysetOrderingAndReturnsDetachedTasks(t *testing.T) {
	repo := NewMockTaskRepository()
	base := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	for _, id := range []domain.ID{"00000000-0000-4000-8000-000000000001", "00000000-0000-4000-8000-000000000002", "00000000-0000-4000-8000-000000000003"} {
		payload, _ := domain.NewPayload([]byte(`{}`))
		item, err := domain.Submit(payload, base)
		if err != nil {
			t.Fatal(err)
		}
		item.ID = id
		item.CreatedAt = base
		item.UpdatedAt = base
		if err = repo.Insert(context.Background(), item); err != nil {
			t.Fatal(err)
		}
	}
	items, err := repo.List(context.Background(), 2, time.Time{}, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 || items[0].ID != "00000000-0000-4000-8000-000000000003" || items[1].ID != "00000000-0000-4000-8000-000000000002" {
		t.Fatalf("first page=%v", []domain.ID{items[0].ID, items[1].ID})
	}
	items[0].Status = domain.Completed
	second, err := repo.List(context.Background(), 2, base, items[1].ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 1 || second[0].ID != "00000000-0000-4000-8000-000000000001" || second[0].Status != domain.Pending {
		t.Fatalf("second page=%+v", second)
	}
	empty, err := repo.List(context.Background(), 10, base.Add(-time.Second), "00000000-0000-4000-8000-000000000003")
	if err != nil || empty == nil || len(empty) != 0 {
		t.Fatalf("empty page=%v err=%v", empty, err)
	}
	const firstID domain.ID = "00000000-0000-4000-8000-000000000001"
	result := "original result"
	if err := repo.UpdateByID(context.Background(), firstID, 1, func(item *domain.Task) (bool, error) {
		if _, err := item.Start(base); err != nil {
			return false, err
		}
		return item.Finish(base, &result, nil)
	}); err != nil {
		t.Fatal(err)
	}
	unpublished, err := repo.ListPublishable(context.Background(), port.PublishableQuery{Limit: 2, Now: base, CreatedBefore: base})
	if err != nil || len(unpublished) != 2 || unpublished[0].ID != "00000000-0000-4000-8000-000000000002" || unpublished[1].ID != "00000000-0000-4000-8000-000000000003" {
		t.Fatalf("unpublished snapshot order=%+v err=%v", unpublished, err)
	}
	unpublished[0].Name = "changed snapshot"
	stored, err := repo.Get(context.Background(), unpublished[0].ID)
	if err != nil || stored.Name != "" {
		t.Fatalf("unpublished snapshot mutation leaked: %+v err=%v", stored, err)
	}
}

func TestUpdateByIDRejectsStaleVersion(t *testing.T) {
	repo := NewMockTaskRepository()
	item := newMockTask(t, repo)
	ctx := context.Background()
	if err := repo.UpdateByID(ctx, item.ID, item.Version, func(task *domain.Task) (bool, error) { return task.Edit("edited", task.Payload, time.Now()) }); err != nil {
		t.Fatal(err)
	}
	called := false
	err := repo.UpdateByID(ctx, item.ID, item.Version, func(*domain.Task) (bool, error) { called = true; return true, nil })
	if !errors.Is(err, domain.ErrConflict) || called {
		t.Fatalf("stale callback ran=%v err=%v", called, err)
	}
	if err := repo.DeleteByID(ctx, item.ID, item.Version); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("stale delete=%v", err)
	}
	current, _ := repo.Get(ctx, item.ID)
	if err := repo.DeleteByID(ctx, item.ID, current.Version); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Get(ctx, item.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("deleted get=%v", err)
	}
	called = false
	err = repo.UpdateByID(ctx, item.ID, current.Version, func(*domain.Task) (bool, error) { called = true; return true, nil })
	if !errors.Is(err, domain.ErrNotFound) || called {
		t.Fatalf("missing callback ran=%v err=%v", called, err)
	}
}

func TestReleaseStaleSpendsRetriesThenAbandons(t *testing.T) {
	repo := NewMockTaskRepository()
	item := newMockTask(t, repo)
	fresh := newMockTask(t, repo)
	ctx := context.Background()
	// The mock compares against time.Now(), standing in for the database clock;
	// the live claim is stamped ahead of it so it never goes stale here.
	delays := []time.Duration{time.Millisecond, time.Millisecond}
	if err := repo.UpdateByID(ctx, fresh.ID, fresh.Version, func(task *domain.Task) (bool, error) { return task.Start(time.Now().Add(time.Hour)) }); err != nil {
		t.Fatal(err)
	}
	for attempt := 1; attempt <= len(delays)+1; attempt++ {
		time.Sleep(2 * time.Millisecond) // Past the previous backoff.
		current, _ := repo.Get(ctx, item.ID)
		if err := repo.UpdateByID(ctx, item.ID, current.Version, func(task *domain.Task) (bool, error) { return task.Start(time.Now()) }); err != nil {
			t.Fatal(err)
		}
		claimed, _ := repo.Get(ctx, item.ID)
		// A claim updated within staleAfter is live and stays running.
		if n, err := repo.ReleaseStale(ctx, time.Hour, delays); n != 0 || err != nil {
			t.Fatalf("fresh release %d=%d,%v", attempt, n, err)
		}
		time.Sleep(2 * time.Millisecond)
		if n, err := repo.ReleaseStale(ctx, time.Millisecond, delays); n != 1 || err != nil {
			t.Fatalf("release %d=%d,%v", attempt, n, err)
		}
		released, _ := repo.Get(ctx, item.ID)
		if released.IsRunning || released.RetryCount != min(attempt, len(delays)) || released.Version != current.Version+2 || !released.UpdatedAt.After(claimed.UpdatedAt) {
			t.Fatalf("release %d=%+v", attempt, released)
		}
		if attempt <= len(delays) && (released.Status != domain.Pending || released.IsSubmitted || released.Error != nil || !released.NextRetry.Equal(released.UpdatedAt.Add(time.Millisecond))) {
			t.Fatalf("release %d did not return to pending=%+v", attempt, released)
		}
		if attempt > len(delays) && (!released.Finished() || !released.IsSubmitted || released.Result != nil || released.Error == nil || *released.Error != "abandoned after 3 attempts; the last execution was interrupted") {
			t.Fatalf("release %d did not abandon=%+v", attempt, released)
		}
	}
	if live, _ := repo.Get(ctx, fresh.ID); !live.IsRunning || live.RetryCount != 0 {
		t.Fatalf("live claim released=%+v", live)
	}
	if _, err := repo.ReleaseStale(ctx, 0, delays); err == nil {
		t.Fatal("accepted a non-positive stale threshold")
	}
}

func TestVersionZeroLocksCurrentRowWithoutComparison(t *testing.T) {
	repo := NewMockTaskRepository()
	item := newMockTask(t, repo)
	ctx := context.Background()
	if err := repo.UpdateByID(ctx, item.ID, 0, func(task *domain.Task) (bool, error) { return task.Start(time.Now()) }); err != nil {
		t.Fatal(err)
	}
	if err := repo.DeleteByID(ctx, item.ID, 0); !errors.Is(err, domain.ErrBusy) {
		t.Fatalf("running delete=%v", err)
	}
	result := "done"
	if err := repo.UpdateByID(ctx, item.ID, 0, func(task *domain.Task) (bool, error) { return task.Finish(time.Now(), &result, nil) }); err != nil {
		t.Fatal(err)
	}
	if err := repo.DeleteByID(ctx, item.ID, item.Version); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("stale delete=%v", err)
	}
	if err := repo.DeleteByID(ctx, item.ID, 0); err != nil {
		t.Fatal(err)
	}
}
