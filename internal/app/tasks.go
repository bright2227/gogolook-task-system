package app

import (
	"context"
	"time"

	"github.com/bright2227/vsx-task-system/internal/task"
)

type Tasks struct {
	Store Store
	Clock Clock
}

func (s Tasks) Submit(ctx context.Context, p task.Payload) (task.Snapshot, error) {
	t, err := task.Submit(p, s.Clock())
	if err != nil {
		return task.Snapshot{}, err
	}
	err = s.Store.Within(ctx, func(r Repository) error { return r.Insert(ctx, t) })
	return t.Snapshot(), err
}
func (s Tasks) Get(ctx context.Context, id task.ID) (task.Snapshot, error) {
	t, err := s.Store.Get(ctx, id)
	if err != nil {
		return task.Snapshot{}, err
	}
	return t.Snapshot(), nil
}
func (s Tasks) Cancel(ctx context.Context, id task.ID) (task.Snapshot, error) {
	var out task.Snapshot
	err := s.Store.Within(ctx, func(r Repository) error {
		t, err := r.Get(ctx, id)
		if err != nil {
			return err
		}
		version := t.Snapshot().Version
		changed, err := t.Cancel(s.Clock())
		if err != nil {
			return err
		}
		if changed {
			if err = r.Save(ctx, t, version); err != nil {
				return err
			}
		}
		out = t.Snapshot()
		return nil
	})
	return out, err
}

type Consumer struct {
	Store     Store
	Clock     Clock
	Processor Processor
	Lease     time.Duration
	Timeout   time.Duration
}

func (c Consumer) Process(ctx context.Context, id task.ID) error {
	token := task.NewID()
	var payload task.Payload
	started := false
	err := c.Store.Within(ctx, func(r Repository) error {
		t, err := r.Get(ctx, id)
		if err != nil {
			return err
		}
		s := t.Snapshot()
		started, err = t.Start(token, c.Clock(), c.Lease)
		if err != nil || !started {
			return err
		}
		payload = s.Payload
		return r.Save(ctx, t, s.Version)
	})
	if err != nil || !started {
		return err
	}
	workCtx, cancel := context.WithTimeout(ctx, c.Timeout)
	defer cancel()
	result, execErr := c.Processor.Execute(workCtx, payload)
	// Shutdown causes redelivery. Business failure/timeout becomes a completed
	// error outcome, retaining the assignment's four-status lifecycle.
	if ctx.Err() != nil {
		return ctx.Err()
	}
	var resultPtr, failurePtr *string
	if execErr == nil {
		resultPtr = &result
	} else {
		failure := "task execution failed"
		failurePtr = &failure
	}
	return c.Store.Within(ctx, func(r Repository) error {
		t, err := r.Get(ctx, id)
		if err != nil {
			return err
		}
		version := t.Snapshot().Version
		changed, err := t.Finish(token, c.Clock(), resultPtr, failurePtr)
		if err != nil || !changed {
			return err
		}
		return r.Save(ctx, t, version)
	})
}
