package app

import (
	"context"
	"errors"
	"time"

	"github.com/bright2227/vsx-task-system/internal/task"
)

var (
	ErrInvalidID      = errors.New("invalid task ID")
	ErrInvalidPayload = errors.New("payload must be valid JSON of at most 65536 bytes")
	ErrCompleted      = errors.New("completed tasks cannot be canceled")
	ErrBusy           = errors.New("task execution lease is still held")
	ErrNotFound       = errors.New("task not found")
	ErrConflict       = errors.New("concurrent task update")
)

// Input ports use transport-neutral DTOs and never expose persistence state.
type SubmitTaskRequest struct{ Payload []byte }
type TaskRequest struct{ ID string }
type ProcessTaskRequest struct{ ID string }
type TaskResponse struct {
	ID        string
	Payload   []byte
	Status    string
	Result    *string
	Error     *string
	CreatedAt time.Time
	UpdatedAt time.Time
	Attempts  int
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
	Get(context.Context, task.ID) (*task.Task, error)
	Insert(context.Context, *task.Task) error
	Save(context.Context, *task.Task, int64) error
}
type Store interface {
	Get(context.Context, task.ID) (*task.Task, error)
	Within(context.Context, func(Repository) error) error
}
type Processor interface {
	Execute(context.Context, task.Payload) (string, error)
}
type Clock func() time.Time

type Message struct {
	EventID task.ID
	TaskID  task.ID
	Token   task.ID
}
type Outbox interface {
	Reserve(context.Context, int) ([]Message, error)
	Published(context.Context, Message) error
	Release(context.Context, Message) error
}
type Publisher interface {
	Publish(context.Context, Message) error
}
