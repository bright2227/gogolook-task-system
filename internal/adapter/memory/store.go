// Package memory provides a transactionally faithful test double, never runtime storage.
package memory

import (
	"context"
	"sync"

	"github.com/bright2227/vsx-task-system/internal/app"
	"github.com/bright2227/vsx-task-system/internal/task"
)

type Store struct {
	mu     sync.Mutex
	tasks  map[task.ID]task.Snapshot
	Events []task.Event
	Outbox []app.Message
}

func New() *Store { return &Store{tasks: make(map[task.ID]task.Snapshot)} }
func (s *Store) Get(ctx context.Context, id task.ID) (*task.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (&repo{tasks: s.tasks}).Get(ctx, id)
}
func (s *Store) Within(ctx context.Context, fn func(app.Repository) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := &repo{tasks: make(map[task.ID]task.Snapshot), knownEvents: make(map[task.ID]bool)}
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
	tasks       map[task.ID]task.Snapshot
	events      []task.Event
	outbox      []app.Message
	knownEvents map[task.ID]bool
}

func (r *repo) Get(ctx context.Context, id task.ID) (*task.Task, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s, ok := r.tasks[id]
	if !ok {
		return nil, app.ErrNotFound
	}
	return task.Restore(s)
}
func (r *repo) record(t *task.Task) {
	for _, e := range t.Events() {
		if r.knownEvents[e.ID] {
			continue
		}
		r.knownEvents[e.ID] = true
		r.events = append(r.events, e)
		if e.Name == "TaskSubmitted" {
			r.outbox = append(r.outbox, app.Message{EventID: e.ID, TaskID: e.TaskID})
		}
	}
}
func (r *repo) Insert(ctx context.Context, t *task.Task) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s := t.Snapshot()
	if _, ok := r.tasks[s.ID]; ok {
		return app.ErrConflict
	}
	r.tasks[s.ID] = s
	r.record(t)
	return nil
}
func (r *repo) Save(ctx context.Context, t *task.Task, version int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s := t.Snapshot()
	old, ok := r.tasks[s.ID]
	if !ok {
		return app.ErrNotFound
	}
	if old.Version != version {
		return app.ErrConflict
	}
	r.tasks[s.ID] = s
	r.record(t)
	return nil
}
