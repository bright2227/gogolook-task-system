package usecase

import (
	"context"
	"errors"
	"time"

	"github.com/bright2227/gogolook-task-system/internal/domain"
)

var (
	ErrInvalidID       = errors.New("invalid task ID")
	ErrInvalidPayload  = errors.New("payload must be valid JSON of at most 65536 bytes")
	ErrCompleted       = errors.New("completed tasks cannot be canceled")
	ErrBusy            = errors.New("task execution lease is still held")
	ErrNotDue          = errors.New("task execution is not due")
	ErrNotFound        = errors.New("task not found")
	ErrConflict        = errors.New("concurrent task update")
	ErrAlreadyReplayed = errors.New("dead-letter message already replayed")
	ErrReplayConflict  = errors.New("dead-letter message replayed with different audit details")
)

// Input ports use transport-neutral DTOs and never expose persistence state.
type SubmitTaskRequest struct{ Payload []byte }
type TaskRequest struct{ ID string }
type ProcessTaskRequest struct {
	ID          string
	EventID     string
	ExecutionID string
	Original    []byte
}
type TaskResponse struct {
	ID          string
	Payload     []byte
	Status      string
	Result      *string
	Error       *string
	CreatedAt   time.Time
	UpdatedAt   time.Time
	Attempts    int
	RetryCount  int
	ReplayCycle int
	NotBefore   time.Time
}
type ReplayRequest struct{ MessageID, Operator, Reason string }
type ReplayResponse struct {
	TaskID          string
	ExecutionID     string
	AlreadyReplayed bool
}

type TaskUseCases interface {
	Submit(context.Context, SubmitTaskRequest) (TaskResponse, error)
	Get(context.Context, TaskRequest) (TaskResponse, error)
	Cancel(context.Context, TaskRequest) (TaskResponse, error)
}
type TaskProcessor interface {
	Process(context.Context, ProcessTaskRequest) error
}
type TaskProcessorFunc func(context.Context, ProcessTaskRequest) error

func (f TaskProcessorFunc) Process(ctx context.Context, req ProcessTaskRequest) error {
	return f(ctx, req)
}

type Repository interface {
	Get(context.Context, domain.ID) (*domain.Task, error)
	Insert(context.Context, *domain.Task) error
	Save(context.Context, *domain.Task, int64) error
	Enqueue(context.Context, Message) error
	StartAttempt(context.Context, AttemptRecord) error
	FinalizeAttempt(context.Context, AttemptRecord) error
	AddDeadLetter(context.Context, DeadLetterRecord) error
	GetDeadLetter(context.Context, string) (DeadLetterRecord, error)
	GetReplayReceipt(context.Context, string) (ReplayReceipt, error)
	InsertReplayReceipt(context.Context, ReplayReceipt) error
}
type Store interface {
	Get(context.Context, domain.ID) (*domain.Task, error)
	Within(context.Context, func(Repository) error) error
}
type Processor interface {
	// Execute receives the stable task ID so external effects can use a durable
	// idempotency key across retries and replay cycles.
	Execute(context.Context, domain.ID, domain.Payload) (string, error)
}
type Clock func() time.Time

type Destination string

const (
	DestinationWork       Destination = "work"
	DestinationDeadLetter Destination = "dead-letter"
)

type Message struct {
	EventID     domain.ID      `json:"event_id"`
	TaskID      domain.ID      `json:"task_id"`
	ExecutionID domain.ID      `json:"execution_id"`
	Token       domain.ID      `json:"token"`
	Destination Destination    `json:"destination"`
	AvailableAt time.Time      `json:"available_at"`
	Failure     *FailureRecord `json:"failure,omitempty"`
}
type FailureRecord struct {
	Category          string    `json:"category"`
	Code              string    `json:"code"`
	Detail            string    `json:"detail"`
	WorkerID          string    `json:"worker_id"`
	RetryCount        int       `json:"retry_count"`
	ReplayCycle       int       `json:"replay_cycle"`
	Attempts          int       `json:"attempts"`
	FailedAt          time.Time `json:"failed_at"`
	SourceEventID     domain.ID `json:"source_event_id"`
	Original          []byte    `json:"original,omitempty"`
	OriginalHash      string    `json:"original_hash,omitempty"`
	OriginalTruncated bool      `json:"original_truncated,omitempty"`
}
type AttemptRecord struct {
	TaskID      domain.ID
	ExecutionID domain.ID
	LeaseToken  domain.ID
	Attempt     int
	RetryCount  int
	ReplayCycle int
	StartedAt   time.Time
	FinishedAt  *time.Time
	Outcome     string
	Error       string
	Category    string
	Code        string
	WorkerID    string
}
type DeadLetterRecord struct {
	MessageID         string
	EventID           domain.ID
	SourceEventID     domain.ID
	TaskID            domain.ID
	ExecutionID       domain.ID
	RetryCount        int
	ReplayCycle       int
	Attempts          int
	FailedAt          time.Time
	Category          string
	Code              string
	Detail            string
	WorkerID          string
	Original          []byte
	OriginalHash      string
	OriginalTruncated bool
}
type ReplayReceipt struct {
	MessageID   string
	Fingerprint string
	Operator    string
	Reason      string
	TaskID      domain.ID
	ExecutionID domain.ID
	ReplayedAt  time.Time
}
type Outbox interface {
	Reserve(context.Context, int) ([]Message, error)
	Published(context.Context, Message) error
	Release(context.Context, Message) error
}
type Publisher interface {
	Publish(context.Context, Message) error
}

// PermanentProcessorError marks business failures that must not be retried.
type PermanentProcessorError struct {
	Code, Detail string
	Err          error
}

func (e *PermanentProcessorError) Error() string {
	if e.Detail != "" {
		return e.Detail
	}
	if e.Err != nil {
		return e.Err.Error()
	}
	return "permanent task failure"
}
func (e *PermanentProcessorError) Unwrap() error { return e.Err }
func Permanent(code, detail string, err error) error {
	return &PermanentProcessorError{Code: code, Detail: detail, Err: err}
}
