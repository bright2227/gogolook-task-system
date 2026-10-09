package usecase

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/bright2227/gogolook-task-system/internal/domain"
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
		t, e := r.Get(ctx, id)
		if e != nil {
			return e
		}
		before := t.Snapshot()
		changed, e := t.Cancel(s.Clock())
		if e != nil {
			return e
		}
		if changed {
			if before.LeaseToken != "" {
				now := s.Clock()
				if e = r.FinalizeAttempt(ctx, AttemptRecord{TaskID: id, ExecutionID: before.ExecutionID, LeaseToken: before.LeaseToken, FinishedAt: &now, Outcome: "canceled"}); e != nil {
					return e
				}
			}
			if e = r.Save(ctx, t, before.Version); e != nil {
				return e
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
	out := TaskResponse{ID: string(s.ID), Payload: append([]byte(nil), s.Payload.JSON()...), Status: string(s.Status), CreatedAt: s.CreatedAt, UpdatedAt: s.UpdatedAt, Attempts: s.Attempts, RetryCount: s.RetryCount, ReplayCycle: s.ReplayCycle, NotBefore: s.NotBefore}
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
	case errors.Is(err, domain.ErrNotDue):
		return ErrNotDue
	default:
		return err
	}
}

type Consumer struct {
	Store       Store
	Clock       Clock
	Processor   Processor
	Lease       time.Duration
	Timeout     time.Duration
	WorkerID    string
	RetryDelays []time.Duration
}

var _ TaskProcessor = Consumer{}
var defaultRetryDelays = []time.Duration{time.Second, 10 * time.Second, time.Minute}

func (c Consumer) Process(ctx context.Context, req ProcessTaskRequest) error {
	taskID, err := parseID(req.ID)
	if err != nil {
		return err
	}
	eventID, err := parseID(req.EventID)
	if err != nil {
		return err
	}
	executionID, err := parseID(req.ExecutionID)
	if err != nil {
		return err
	}
	var payload domain.Payload
	var token domain.ID
	var started bool
	var attempt AttemptRecord
	err = c.Store.Within(ctx, func(r Repository) error {
		t, e := r.Get(ctx, taskID)
		if e != nil {
			return e
		}
		snap := t.Snapshot()
		if snap.ExecutionID != executionID {
			return nil
		} // durable newer generation makes this delivery stale.
		if snap.Status == domain.Completed || snap.Status == domain.Canceled {
			return nil
		}
		token = domain.NewID()
		started, e = t.Start(token, c.Clock(), c.Lease)
		if e != nil || !started {
			return e
		}
		claimed := t.Snapshot()
		payload = claimed.Payload
		attempt = AttemptRecord{TaskID: taskID, ExecutionID: executionID, LeaseToken: token, Attempt: claimed.Attempts, RetryCount: claimed.RetryCount, ReplayCycle: claimed.ReplayCycle, StartedAt: claimed.UpdatedAt, WorkerID: c.WorkerID}
		if e = r.StartAttempt(ctx, attempt); e != nil {
			return e
		}
		return r.Save(ctx, t, snap.Version)
	})
	if err != nil {
		return mapTaskError(err)
	}
	if !started {
		return nil
	}
	workCtx, cancel := context.WithTimeout(ctx, c.Timeout)
	defer cancel()
	result, workErr := c.Processor.Execute(workCtx, taskID, payload)
	if ctx.Err() != nil {
		return ctx.Err()
	} // interruption leaves lease for recovery.
	if workErr == nil && errors.Is(workCtx.Err(), context.DeadlineExceeded) {
		workErr = context.DeadlineExceeded
	}
	if workErr == nil {
		return c.finishSuccess(ctx, taskID, token, result, attempt)
	}
	var permanent *PermanentProcessorError
	permanentFailure := errors.As(workErr, &permanent)
	delays := c.RetryDelays
	if delays == nil {
		delays = defaultRetryDelays
	}
	return c.finishFailure(ctx, req, taskID, eventID, executionID, token, attempt, workErr, permanentFailure, permanent, delays)
}

func (c Consumer) finishSuccess(ctx context.Context, id, token domain.ID, result string, attempt AttemptRecord) error {
	now := c.Clock()
	return c.Store.Within(ctx, func(r Repository) error {
		t, e := r.Get(ctx, id)
		if e != nil {
			return e
		}
		before := t.Snapshot()
		value := result
		changed, e := t.Finish(token, now, &value, nil)
		if e != nil {
			return e
		}
		finished := attempt
		finished.FinishedAt = &now
		finished.Outcome = "succeeded"
		if !changed {
			finished.Outcome = "superseded"
			return r.FinalizeAttempt(ctx, finished)
		}
		if e = r.Save(ctx, t, before.Version); e != nil {
			return e
		}
		return r.FinalizeAttempt(ctx, finished)
	})
}

func (c Consumer) finishFailure(ctx context.Context, req ProcessTaskRequest, id, eventID, executionID, token domain.ID, attempt AttemptRecord, workErr error, isPermanent bool, permanent *PermanentProcessorError, delays []time.Duration) error {
	now := c.Clock()
	detail := safeDetail(workErr.Error())
	category, code := "retryable", "processor_error"
	if isPermanent {
		category = "permanent"
		code = permanent.Code
		if !validFailureCode(code) {
			code = "permanent_error"
		}
	}
	return c.Store.Within(ctx, func(r Repository) error {
		t, e := r.Get(ctx, id)
		if e != nil {
			return e
		}
		before := t.Snapshot()
		if before.ExecutionID != executionID || before.Status != domain.Processing || before.LeaseToken != token {
			finished := attempt
			finished.FinishedAt = &now
			finished.Outcome = "superseded"
			finished.Error = detail
			return r.FinalizeAttempt(ctx, finished)
		}
		original := req.Original
		if len(original) == 0 {
			original = before.Payload.JSON()
		}
		sum := sha256.Sum256(original)
		stored := append([]byte(nil), original...)
		truncated := len(stored) > 65536
		if truncated {
			stored = stored[:65536]
		}
		workerID := boundedUTF8(c.WorkerID, 256)
		record := FailureRecord{Category: category, Code: code, Detail: detail, WorkerID: workerID, RetryCount: before.RetryCount, ReplayCycle: before.ReplayCycle, Attempts: before.Attempts, FailedAt: now, SourceEventID: eventID, Original: stored, OriginalHash: hex.EncodeToString(sum[:]), OriginalTruncated: truncated}
		if !isPermanent && before.RetryCount < len(delays) {
			notBefore := now.Add(delays[before.RetryCount])
			newExecution := domain.NewID()
			df := domainFailure(record)
			changed, e := t.Retry(token, newExecution, now, notBefore, &df)
			if e != nil {
				return e
			}
			if !changed {
				return ErrConflict
			}
			if e = r.Save(ctx, t, before.Version); e != nil {
				return e
			}
			finished := attempt
			finished.FinishedAt = &now
			finished.Outcome = "retry_scheduled"
			finished.Error = detail
			finished.Category = record.Category
			finished.Code = record.Code
			if e = r.FinalizeAttempt(ctx, finished); e != nil {
				return e
			}
			return enqueueLatest(ctx, r, t, DestinationWork, notBefore, &record)
		}
		if !isPermanent {
			record.Category = "exhausted"
			record.Code = "retries_exhausted"
		}
		df := domainFailure(record)
		changed, e := t.Fail(token, now, &df)
		if e != nil {
			return e
		}
		if !changed {
			return ErrConflict
		}
		if e = r.Save(ctx, t, before.Version); e != nil {
			return e
		}
		finished := attempt
		finished.FinishedAt = &now
		finished.Outcome = "dead_lettered"
		finished.Error = detail
		finished.Category = record.Category
		finished.Code = record.Code
		if e = r.FinalizeAttempt(ctx, finished); e != nil {
			return e
		}
		event := lastEvent(t)
		dlq := DeadLetterRecord{MessageID: string(event.ID), EventID: event.ID, SourceEventID: eventID, TaskID: id, ExecutionID: executionID, RetryCount: record.RetryCount, ReplayCycle: record.ReplayCycle, Attempts: record.Attempts, FailedAt: now, Category: record.Category, Code: record.Code, Detail: record.Detail, WorkerID: record.WorkerID, Original: record.Original, OriginalHash: record.OriginalHash, OriginalTruncated: record.OriginalTruncated}
		if e = r.AddDeadLetter(ctx, dlq); e != nil {
			return e
		}
		return enqueueLatest(ctx, r, t, DestinationDeadLetter, now, &record)
	})
}

func enqueueLatest(ctx context.Context, r Repository, t *domain.Task, d Destination, at time.Time, f *FailureRecord) error {
	e := lastEvent(t)
	msg := Message{EventID: e.ID, TaskID: e.TaskID, ExecutionID: e.ExecutionID, Destination: d, AvailableAt: at, Failure: f}
	return r.Enqueue(ctx, msg)
}
func lastEvent(t *domain.Task) domain.Event { events := t.Events(); return events[len(events)-1] }
func domainFailure(f FailureRecord) domain.Failure {
	return domain.Failure{Category: f.Category, Code: f.Code, Detail: f.Detail, WorkerID: f.WorkerID, Original: append([]byte(nil), f.Original...), OriginalHash: f.OriginalHash, OriginalTruncated: f.OriginalTruncated}
}
func safeDetail(s string) string {
	s = boundedUTF8(strings.ReplaceAll(strings.TrimSpace(s), "\x00", "�"), 4096)
	if len(s) > 4096 {
		s = boundedUTF8(s, 4096)
	}
	if s == "" {
		return "task execution failed"
	}
	return s
}

func boundedUTF8(s string, limit int) string {
	s = strings.ToValidUTF8(s, "�")
	if len(s) > limit {
		s = s[:limit]
		for !utf8.ValidString(s) {
			s = s[:len(s)-1]
		}
	}
	return s
}
func validFailureCode(code string) bool {
	if code == "" || len(code) > 128 {
		return false
	}
	for _, r := range code {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' || r == '.') {
			return false
		}
	}
	return true
}

func (s Tasks) Replay(ctx context.Context, req ReplayRequest) (ReplayResponse, error) {
	if strings.TrimSpace(req.MessageID) == "" || strings.TrimSpace(req.Operator) == "" || strings.TrimSpace(req.Reason) == "" || len(req.Operator) > 256 || len(req.Reason) > 4096 || !utf8.ValidString(req.Operator) || !utf8.ValidString(req.Reason) || strings.ContainsRune(req.Operator, 0) || strings.ContainsRune(req.Reason, 0) {
		return ReplayResponse{}, ErrConflict
	}
	fingerprint := fmt.Sprintf("%x", sha256.Sum256([]byte(req.Operator+"\x00"+req.Reason)))
	for retry := 0; retry < 2; retry++ {
		var out ReplayResponse
		err := s.Store.Within(ctx, func(r Repository) error {
			receipt, e := r.GetReplayReceipt(ctx, req.MessageID)
			if e == nil {
				if receipt.Fingerprint != fingerprint {
					return ErrReplayConflict
				}
				out = ReplayResponse{TaskID: string(receipt.TaskID), ExecutionID: string(receipt.ExecutionID), AlreadyReplayed: true}
				return nil
			}
			if !errors.Is(e, ErrNotFound) {
				return e
			}
			dlq, e := r.GetDeadLetter(ctx, req.MessageID)
			if e != nil {
				return e
			}
			t, e := r.Get(ctx, dlq.TaskID)
			if e != nil {
				return e
			}
			// A concurrent replay can commit while this transaction waits for the task row.
			// Recheck the durable receipt before applying terminal-state validation.
			if receipt, receiptErr := r.GetReplayReceipt(ctx, req.MessageID); receiptErr == nil {
				if receipt.Fingerprint != fingerprint {
					return ErrReplayConflict
				}
				out = ReplayResponse{TaskID: string(receipt.TaskID), ExecutionID: string(receipt.ExecutionID), AlreadyReplayed: true}
				return nil
			} else if !errors.Is(receiptErr, ErrNotFound) {
				return receiptErr
			}
			before := t.Snapshot()
			if before.ExecutionID != dlq.ExecutionID || before.ReplayCycle != dlq.ReplayCycle {
				return ErrConflict
			}
			newExecution := domain.NewID()
			now := s.Clock()
			if e = t.Replay(newExecution, now); e != nil {
				return e
			}
			if e = r.Save(ctx, t, before.Version); e != nil {
				return e
			}
			receipt = ReplayReceipt{MessageID: req.MessageID, Fingerprint: fingerprint, Operator: req.Operator, Reason: req.Reason, TaskID: dlq.TaskID, ExecutionID: newExecution, ReplayedAt: now}
			if e = r.InsertReplayReceipt(ctx, receipt); e != nil {
				return e
			}
			if e = enqueueLatest(ctx, r, t, DestinationWork, now, nil); e != nil {
				return e
			}
			out = ReplayResponse{TaskID: string(dlq.TaskID), ExecutionID: string(newExecution)}
			return nil
		})
		if errors.Is(err, ErrAlreadyReplayed) {
			continue
		}
		if err != nil {
			return ReplayResponse{}, mapTaskError(err)
		}
		return out, nil
	}
	return ReplayResponse{}, ErrAlreadyReplayed
}
