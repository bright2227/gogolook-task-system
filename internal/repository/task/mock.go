// Package task provides a transactionally faithful test double, never runtime storage.
package task

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/bright2227/gogolook-task-system/internal/usecase/port"

	"github.com/bright2227/gogolook-task-system/internal/domain"
)

type MockTaskRepository struct {
	mu    sync.Mutex
	tasks map[domain.ID]domain.Task
}

var _ port.TaskRepository = (*MockTaskRepository)(nil)

func NewMockTaskRepository() *MockTaskRepository {
	return &MockTaskRepository{tasks: make(map[domain.ID]domain.Task)}
}
func (s *MockTaskRepository) Get(ctx context.Context, id domain.ID) (*domain.Task, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	stored, ok := s.tasks[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return domain.Restore(stored)
}
func (s *MockTaskRepository) List(ctx context.Context, pageSize int, before time.Time, beforeID domain.ID) ([]*domain.Task, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validPage(pageSize, before, beforeID); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	items := make([]*domain.Task, 0, len(s.tasks))
	for _, stored := range s.tasks {
		item, err := domain.Restore(stored)
		if err != nil {
			return nil, err
		}
		if !before.IsZero() && (item.CreatedAt.After(before) || (item.CreatedAt.Equal(before) && item.ID >= beforeID)) {
			continue
		}
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].CreatedAt.Equal(items[j].CreatedAt) {
			return items[i].ID > items[j].ID
		}
		return items[i].CreatedAt.After(items[j].CreatedAt)
	})
	if len(items) > pageSize {
		items = items[:pageSize]
	}
	return items, nil
}
func (s *MockTaskRepository) Insert(ctx context.Context, t *domain.Task) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	copy, err := domain.Restore(*t)
	if err != nil {
		return err
	}
	if _, ok := s.tasks[t.ID]; ok {
		return domain.ErrConflict
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.tasks[t.ID] = *copy
	return nil
}
func (s *MockTaskRepository) UpdateByID(ctx context.Context, id domain.ID, version int64, update func(*domain.Task) (bool, error)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	stored, ok := s.tasks[id]
	if !ok {
		return domain.ErrNotFound
	}
	item, err := domain.Restore(stored)
	if err != nil {
		return err
	}
	if version != 0 && item.Version != version {
		return domain.ErrConflict
	}
	changed, err := update(item)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !changed {
		return nil
	}
	if item.ID != id {
		return domain.ErrConflict
	}
	copy, err := domain.Restore(*item)
	if err != nil {
		return err
	}
	s.tasks[id] = *copy
	return nil
}
func (s *MockTaskRepository) UpdateByIDs(ctx context.Context, ids []domain.ID, update func([]*domain.Task) (bool, error)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	items := make([]*domain.Task, 0, len(ids))
	seen := make(map[domain.ID]bool, len(ids))
	for _, id := range ids {
		if seen[id] {
			continue
		}
		seen[id] = true
		if stored, ok := s.tasks[id]; ok {
			copy, err := domain.Restore(stored)
			if err != nil {
				return err
			}
			items = append(items, copy)
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	changed, err := update(items)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !changed {
		return nil
	}
	copies := make([]*domain.Task, len(items))
	for i, item := range items {
		copies[i], err = domain.Restore(*item)
		if err != nil {
			return err
		}
	}
	for _, copy := range copies {
		s.tasks[copy.ID] = *copy
	}
	return nil
}
func (s *MockTaskRepository) ListPublishable(ctx context.Context, q port.PublishableQuery) ([]*domain.Task, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	limit := q.Limit
	if limit < 1 || limit > 1024 {
		return nil, domain.ErrConflict
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	items := make([]*domain.Task, 0, len(s.tasks))
	for _, t := range s.tasks {
		unsubmitted := !t.IsSubmitted && !t.NextRetry.After(q.Now) && (t.RetryCount > 0 || !t.CreatedAt.After(q.CreatedBefore))
		expired := !q.SubmittedBefore.IsZero() && t.IsSubmitted && !t.UpdatedAt.After(q.SubmittedBefore)
		if t.Error == nil && t.Status == domain.Pending && !t.IsRunning && (unsubmitted || expired) {
			copy, err := domain.Restore(t)
			if err != nil {
				return nil, err
			}
			items = append(items, copy)
		}
	}
	sort.Slice(items, func(i, j int) bool {
		if !items[i].NextRetry.Equal(items[j].NextRetry) {
			return items[i].NextRetry.Before(items[j].NextRetry)
		}
		if !items[i].CreatedAt.Equal(items[j].CreatedAt) {
			return items[i].CreatedAt.Before(items[j].CreatedAt)
		}
		return items[i].ID < items[j].ID
	})
	if len(items) > limit {
		items = items[:limit]
	}
	return items, nil
}

func (s *MockTaskRepository) DeleteByID(ctx context.Context, id domain.ID, version int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.tasks[id]
	if !ok {
		return domain.ErrNotFound
	}
	if version != 0 && item.Version != version {
		return domain.ErrConflict
	}
	if item.IsRunning {
		return domain.ErrBusy
	}
	delete(s.tasks, id)
	return nil
}

// ReleaseStale compares against time.Now(), standing in for the database clock,
// and applies domain.Task.Interrupt like the PostgreSQL repository.
func (s *MockTaskRepository) ReleaseStale(ctx context.Context, staleAfter time.Duration, retryDelays []time.Duration) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if staleAfter <= 0 {
		return 0, fmt.Errorf("invalid stale threshold: %s", staleAfter)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC().Truncate(time.Microsecond)
	staleBefore := now.Add(-staleAfter)
	var released int64
	for id, item := range s.tasks {
		if !item.IsRunning || item.UpdatedAt.After(staleBefore) {
			continue
		}
		item.Interrupt(now, retryDelays)
		s.tasks[id] = item
		released++
	}
	return released, nil
}
