// Package memory provides a transactionally faithful test double, never runtime storage.
package memory

import (
	"context"
	"sync"

	"github.com/bright2227/gogolook-task-system/internal/domain"
	"github.com/bright2227/gogolook-task-system/internal/usecase"
)

type Store struct {
	mu          sync.Mutex
	tasks       map[domain.ID]domain.Snapshot
	Events      []domain.Event
	Outbox      []usecase.Message
	Attempts    []usecase.AttemptRecord
	DeadLetters map[string]usecase.DeadLetterRecord
	Receipts    map[string]usecase.ReplayReceipt
}

func New() *Store {
	return &Store{tasks: make(map[domain.ID]domain.Snapshot), DeadLetters: make(map[string]usecase.DeadLetterRecord), Receipts: make(map[string]usecase.ReplayReceipt)}
}
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
	attempts := make([]usecase.AttemptRecord, len(s.Attempts))
	for i, a := range s.Attempts {
		attempts[i] = cloneAttempt(a)
	}
	r := &repo{tasks: make(map[domain.ID]domain.Snapshot), knownEvents: make(map[domain.ID]bool), deadLetters: copyDLQ(s.DeadLetters), receipts: copyReceipts(s.Receipts), attempts: attempts}
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
	for _, m := range r.outbox {
		s.Outbox = append(s.Outbox, cloneMessage(m))
	}
	s.Attempts = r.attempts
	s.DeadLetters = r.deadLetters
	s.Receipts = r.receipts
	return nil
}

type repo struct {
	tasks       map[domain.ID]domain.Snapshot
	events      []domain.Event
	outbox      []usecase.Message
	knownEvents map[domain.ID]bool
	attempts    []usecase.AttemptRecord
	deadLetters map[string]usecase.DeadLetterRecord
	receipts    map[string]usecase.ReplayReceipt
}

func copyDLQ(in map[string]usecase.DeadLetterRecord) map[string]usecase.DeadLetterRecord {
	out := make(map[string]usecase.DeadLetterRecord, len(in))
	for k, v := range in {
		v.Original = append([]byte(nil), v.Original...)
		out[k] = v
	}
	return out
}
func copyReceipts(in map[string]usecase.ReplayReceipt) map[string]usecase.ReplayReceipt {
	out := make(map[string]usecase.ReplayReceipt, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
func cloneAttempt(a usecase.AttemptRecord) usecase.AttemptRecord {
	if a.FinishedAt != nil {
		v := *a.FinishedAt
		a.FinishedAt = &v
	}
	return a
}
func cloneMessage(m usecase.Message) usecase.Message {
	if m.Failure != nil {
		f := *m.Failure
		f.Original = append([]byte(nil), f.Original...)
		m.Failure = &f
	}
	return m
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
			r.outbox = append(r.outbox, usecase.Message{EventID: e.ID, TaskID: e.TaskID, ExecutionID: e.ExecutionID, Destination: usecase.DestinationWork, AvailableAt: e.AvailableAt})
		}
	}
}

func (r *repo) Enqueue(ctx context.Context, m usecase.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.outbox = append(r.outbox, cloneMessage(m))
	return nil
}
func (r *repo) StartAttempt(ctx context.Context, a usecase.AttemptRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	now := a.StartedAt
	for i := range r.attempts {
		old := &r.attempts[i]
		if old.TaskID == a.TaskID && old.FinishedAt == nil {
			old.FinishedAt = &now
			old.Outcome = "interrupted"
			old.Error = "lease recovered"
		}
	}
	for _, old := range r.attempts {
		if old.LeaseToken == a.LeaseToken {
			return usecase.ErrConflict
		}
	}
	r.attempts = append(r.attempts, a)
	return nil
}
func (r *repo) FinalizeAttempt(ctx context.Context, a usecase.AttemptRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for i := range r.attempts {
		if r.attempts[i].LeaseToken == a.LeaseToken {
			if r.attempts[i].FinishedAt != nil {
				return nil
			}
			r.attempts[i] = a
			return nil
		}
	}
	return usecase.ErrNotFound
}
func (r *repo) AddDeadLetter(ctx context.Context, d usecase.DeadLetterRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, ok := r.deadLetters[d.MessageID]; ok {
		return usecase.ErrConflict
	}
	d.Original = append([]byte(nil), d.Original...)
	r.deadLetters[d.MessageID] = d
	return nil
}
func (r *repo) GetDeadLetter(ctx context.Context, id string) (usecase.DeadLetterRecord, error) {
	if err := ctx.Err(); err != nil {
		return usecase.DeadLetterRecord{}, err
	}
	d, ok := r.deadLetters[id]
	if !ok {
		return d, usecase.ErrNotFound
	}
	d.Original = append([]byte(nil), d.Original...)
	return d, nil
}
func (r *repo) GetReplayReceipt(ctx context.Context, id string) (usecase.ReplayReceipt, error) {
	if err := ctx.Err(); err != nil {
		return usecase.ReplayReceipt{}, err
	}
	v, ok := r.receipts[id]
	if !ok {
		return v, usecase.ErrNotFound
	}
	return v, nil
}
func (r *repo) InsertReplayReceipt(ctx context.Context, v usecase.ReplayReceipt) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, ok := r.receipts[v.MessageID]; ok {
		return usecase.ErrAlreadyReplayed
	}
	r.receipts[v.MessageID] = v
	return nil
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
