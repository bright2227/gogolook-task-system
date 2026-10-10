// Package port declares the use-case layer's storage port. It is a separate
// package so repository implementations and their test double can implement
// it without the use cases importing them (and without an import cycle from
// use-case tests that use the in-memory double).
package port

import (
	"context"
	"time"

	"github.com/bright2227/gogolook-task-system/internal/domain"
)

// TaskRepository is task storage only; messaging has its own ports and adapters.
type TaskRepository interface {
	Get(context.Context, domain.ID) (*domain.Task, error)
	List(context.Context, int, time.Time, domain.ID) ([]*domain.Task, error)
	Insert(context.Context, *domain.Task) error
	// UpdateByID and DeleteByID lock the row; version 0 skips the version
	// comparison, any other version must match the locked row.
	UpdateByID(context.Context, domain.ID, int64, func(*domain.Task) (bool, error)) error
	// UpdateByIDs locks existing rows in ID order and commits the batch atomically.
	// Missing IDs are omitted.
	UpdateByIDs(context.Context, []domain.ID, func([]*domain.Task) (bool, error)) error
	ListPublishable(context.Context, PublishableQuery) ([]*domain.Task, error)
	DeleteByID(context.Context, domain.ID, int64) error
	// ReleaseStale applies domain.Task.Interrupt to every running claim not
	// updated for staleAfter by the storage clock (the database's now(), not the
	// caller's): each spends one retry from retryDelays with its backoff, and
	// past the budget the task completes with an abandonment error. Rows locked
	// by a concurrent commit are skipped until the next call.
	ReleaseStale(ctx context.Context, staleAfter time.Duration, retryDelays []time.Duration) (int64, error)
}

// PublishableQuery selects pending, unclaimed tasks without an error that are
// either unsubmitted and due (new tasks only after CreatedBefore, leaving them
// to the fast path), or submitted and unchanged since SubmittedBefore (message
// may have been lost by the broker). A zero SubmittedBefore disables the
// submitted condition.
type PublishableQuery struct {
	Limit                               int
	Now, CreatedBefore, SubmittedBefore time.Time
}
