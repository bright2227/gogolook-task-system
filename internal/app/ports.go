package app

import (
	"context"
	"errors"
	"time"

	"github.com/bright2227/vsx-task-system/internal/task"
)

var (
	ErrNotFound = errors.New("task not found")
	ErrConflict = errors.New("concurrent task update")
)

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
