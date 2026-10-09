// Package memory provides a transactionally faithful test double, never runtime storage.
package memory

import (
	"context"
	"sync"

	"github.com/bright2227/vsx-task-system/internal/domain"
	"github.com/bright2227/vsx-task-system/internal/usecase"
)

type Store struct {
	mu     sync.Mutex
	tasks  map[domain.ID]domain.Snapshot
	Events []domain.Event
	Outbox []usecase.Message
}

func New() *Store { return &Store{tasks: make(map[domain.ID]domain.Snapshot)} }
func (s *Store) Get(ctx context.Context, id domain.ID) (*domain.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (&repo{tasks: s.tasks}).Get(ctx, id)
}
func (s *Store) Within(ctx context.Context, fn func(usecase.Repository) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := &repo{tasks: make(map[domain.ID]domain.Snapshot), knownEvents: make(map[domain.ID]bool)}
	for _, event := range s.Events {
		r.knownEvents[event.ID] = true
	}
	for id, t := range s.tasks {
		r.tasks[id] = t
	}
	if err := fn(r); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.tasks = r.tasks
	s.Events = append(s.Events, r.events...)
	s.Outbox = append(s.Outbox, r.outbox...)
	return nil
}

type repo struct {
	tasks       map[domain.ID]domain.Snapshot
	events      []domain.Event
	outbox      []usecase.Message
	knownEvents map[domain.ID]bool
}

func (r *repo) Get(ctx context.Context, id domain.ID) (*domain.Task, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s, ok := r.tasks[id]
	if !ok {
		return nil, usecase.ErrNotFound
	}
	return domain.Restore(s)
}
func (r *repo) record(t *domain.Task) {
	for _, e := range t.Events() {
		if r.knownEvents[e.ID] {
			continue
		}
		r.knownEvents[e.ID] = true
		r.events = append(r.events, e)
		if e.Name == "TaskSubmitted" {
			r.outbox = append(r.outbox, usecase.Message{EventID: e.ID, TaskID: e.TaskID})
		}
	}
}
func (r *repo) Insert(ctx context.Context, t *domain.Task) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s := t.Snapshot()
	if _, ok := r.tasks[s.ID]; ok {
		return usecase.ErrConflict
	}
	r.tasks[s.ID] = s
	r.record(t)
	return nil
}
func (r *repo) Save(ctx context.Context, t *domain.Task, version int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s := t.Snapshot()
	old, ok := r.tasks[s.ID]
	if !ok {
		return usecase.ErrNotFound
	}
	if old.Version != version {
		return usecase.ErrConflict
	}
	r.tasks[s.ID] = s
	r.record(t)
	return nil
}
