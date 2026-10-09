package usecase

import (
	"context"
	"errors"
	"time"

	"github.com/bright2227/vsx-task-system/internal/domain"
)

type Tasks struct {
	Store Store
	Clock Clock
}

var _ TaskUseCases = Tasks{}

func (s Tasks) Submit(ctx context.Context, req SubmitTaskRequest) (TaskResponse, error) {
	p, err := domain.NewPayload(req.Payload)
	if err != nil {
		return TaskResponse{}, ErrInvalidPayload
	}
	t, err := domain.Submit(p, s.Clock())
	if err != nil {
		return TaskResponse{}, mapTaskError(err)
	}
	if err = s.Store.Within(ctx, func(r Repository) error { return r.Insert(ctx, t) }); err != nil {
		return TaskResponse{}, mapTaskError(err)
	}
	return response(t.Snapshot()), nil
}

func (s Tasks) Get(ctx context.Context, req TaskRequest) (TaskResponse, error) {
	id, err := parseID(req.ID)
	if err != nil {
		return TaskResponse{}, err
	}
	t, err := s.Store.Get(ctx, id)
	if err != nil {
		return TaskResponse{}, mapTaskError(err)
	}
	return response(t.Snapshot()), nil
}

func (s Tasks) Cancel(ctx context.Context, req TaskRequest) (TaskResponse, error) {
	id, err := parseID(req.ID)
	if err != nil {
		return TaskResponse{}, err
	}
	var out TaskResponse
	err = s.Store.Within(ctx, func(r Repository) error {
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
		out = response(t.Snapshot())
		return nil
	})
	if err != nil {
		return TaskResponse{}, mapTaskError(err)
	}
	return out, nil
}

func parseID(raw string) (domain.ID, error) {
	id, err := domain.ParseID(raw)
	if err != nil {
		return "", ErrInvalidID
	}
	return id, nil
}

func response(s domain.Snapshot) TaskResponse {
	out := TaskResponse{
		ID: string(s.ID), Payload: append([]byte(nil), s.Payload.JSON()...), Status: string(s.Status),
		CreatedAt: s.CreatedAt, UpdatedAt: s.UpdatedAt, Attempts: s.Attempts,
	}
	if s.Result != nil {
		v := *s.Result
		out.Result = &v
	}
	if s.Error != nil {
		v := *s.Error
		out.Error = &v
	}
	return out
}

func mapTaskError(err error) error {
	switch {
	case errors.Is(err, domain.ErrPayload):
		return ErrInvalidPayload
	case errors.Is(err, domain.ErrCompleted):
		return ErrCompleted
	case errors.Is(err, domain.ErrBusy):
		return ErrBusy
	case errors.Is(err, ErrNotFound), errors.Is(err, ErrConflict):
		return err
	default:
		return err
	}
}

type Consumer struct {
	Store     Store
	Clock     Clock
	Processor Processor
	Lease     time.Duration
	Timeout   time.Duration
}

var _ TaskProcessor = Consumer{}

func (c Consumer) Process(ctx context.Context, req ProcessTaskRequest) error {
	id, err := parseID(req.ID)
	if err != nil {
		return err
	}
	token := domain.NewID()
	var payload domain.Payload
	started := false
	err = c.Store.Within(ctx, func(r Repository) error {
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
		return mapTaskError(err)
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
	err = c.Store.Within(ctx, func(r Repository) error {
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
	return mapTaskError(err)
}
