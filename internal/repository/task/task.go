package task

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/bright2227/gogolook-task-system/internal/domain"
	"github.com/bright2227/gogolook-task-system/internal/usecase/port"
)

// TaskRepository holds PostgreSQL task storage only.
type TaskRepository struct {
	db *sql.DB
}

func NewTaskRepository(db *sql.DB) *TaskRepository {
	return &TaskRepository{db: db}
}

const selectTaskColumns = `SELECT id,payload,status,created_at,updated_at,result,error,is_submitted,retry_count,next_retry,name,version,is_running FROM tasks`

type scanner interface{ Scan(...any) error }

// The status column stores 0 (incomplete) or 1 (completed); processing is
// derived from is_running. These two functions are the only mapping.
func statusColumn(s domain.Status) int {
	if s == domain.Completed {
		return 1
	}
	return 0
}
func domainStatus(column int, running bool) domain.Status {
	switch {
	case column == 1:
		return domain.Completed
	case running:
		return domain.Processing
	default:
		return domain.Pending
	}
}

func read(row scanner) (*domain.Task, error) {
	var t domain.Task
	var raw []byte
	var status int
	var result, failure sql.NullString
	if err := row.Scan(&t.ID, &raw, &status, &t.CreatedAt, &t.UpdatedAt, &result, &failure, &t.IsSubmitted, &t.RetryCount, &t.NextRetry, &t.Name, &t.Version, &t.IsRunning); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domain.ErrNotFound
		}
		return nil, err
	}
	t.Status = domainStatus(status, t.IsRunning)
	var err error
	t.Payload, err = domain.NewPayload(raw)
	if err != nil {
		return nil, err
	}
	if result.Valid {
		t.Result = &result.String
	}
	if failure.Valid {
		t.Error = &failure.String
	}
	return domain.Restore(t)
}
func (s *TaskRepository) Get(ctx context.Context, id domain.ID) (*domain.Task, error) {
	return read(s.db.QueryRowContext(ctx, selectTaskColumns+` WHERE id=$1`, id))
}
func validPage(size int, before time.Time, id domain.ID) error {
	if size < 1 || size > 101 || before.IsZero() != (id == "") {
		return domain.ErrPagination
	}
	if id != "" {
		if _, err := domain.ParseID(string(id)); err != nil {
			return domain.ErrPagination
		}
	}
	return nil
}
func (s *TaskRepository) List(ctx context.Context, size int, before time.Time, id domain.ID) ([]*domain.Task, error) {
	if err := validPage(size, before, id); err != nil {
		return nil, err
	}
	query, args := selectTaskColumns+` ORDER BY created_at DESC,id DESC LIMIT $1`, []any{size}
	if !before.IsZero() {
		query = selectTaskColumns + ` WHERE (created_at,id)<($1,$2) ORDER BY created_at DESC,id DESC LIMIT $3`
		args = []any{before, id, size}
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]*domain.Task, 0, size)
	for rows.Next() {
		item, err := read(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}
func (s *TaskRepository) Insert(ctx context.Context, t *domain.Task) error {
	copy, err := domain.Restore(*t)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO tasks(id,payload,status,created_at,updated_at,result,error,is_submitted,retry_count,next_retry,name,version,is_running) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`, copy.ID, copy.Payload.String(), statusColumn(copy.Status), copy.CreatedAt, copy.UpdatedAt, copy.Result, copy.Error, copy.IsSubmitted, copy.RetryCount, copy.NextRetry, copy.Name, copy.Version, copy.IsRunning)
	return err
}
func (s *TaskRepository) UpdateByID(ctx context.Context, id domain.ID, version int64, update func(*domain.Task) (bool, error)) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	item, err := read(tx.QueryRowContext(ctx, selectTaskColumns+` WHERE id=$1 FOR UPDATE`, id))
	if err != nil {
		return err
	}
	if version != 0 && item.Version != version {
		return domain.ErrConflict
	}
	locked := item.Version
	changed, err := update(item)
	if err != nil {
		return err
	}
	if changed {
		if item.ID != id {
			return domain.ErrConflict
		}
		if err := writeTask(ctx, tx, item, locked); err != nil {
			return err
		}
	}
	return tx.Commit()
}
func (s *TaskRepository) UpdateByIDs(ctx context.Context, ids []domain.ID, update func([]*domain.Task) (bool, error)) error {
	if len(ids) == 0 {
		return ctx.Err()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	placeholders, args := make([]string, len(ids)), make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}
	rows, err := tx.QueryContext(ctx, selectTaskColumns+` WHERE id IN (`+strings.Join(placeholders, ",")+`) ORDER BY id FOR UPDATE`, args...)
	if err != nil {
		return err
	}
	items := make([]*domain.Task, 0, len(ids))
	for rows.Next() {
		item, err := read(rows)
		if err != nil {
			rows.Close()
			return err
		}
		items = append(items, item)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err = rows.Close(); err != nil {
		return err
	}
	versions := make(map[domain.ID]int64, len(items))
	for _, item := range items {
		versions[item.ID] = item.Version
	}
	changed, err := update(items)
	if err != nil {
		return err
	}
	if changed {
		for _, item := range items {
			if err := writeTask(ctx, tx, item, versions[item.ID]); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

// writeTask persists validated callback changes under the locked content version.
func writeTask(ctx context.Context, tx *sql.Tx, item *domain.Task, version int64) error {
	item, err := domain.Restore(*item)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE tasks SET status=$2,updated_at=$3,result=$4,error=$5,is_submitted=$6,retry_count=$7,next_retry=$8,name=$9,payload=$10,version=$11,is_running=$12 WHERE id=$1 AND version=$13`, item.ID, statusColumn(item.Status), item.UpdatedAt, item.Result, item.Error, item.IsSubmitted, item.RetryCount, item.NextRetry, item.Name, item.Payload.String(), item.Version, item.IsRunning, version)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return domain.ErrConflict
	}
	return nil
}

func (s *TaskRepository) ListPublishable(ctx context.Context, q port.PublishableQuery) ([]*domain.Task, error) {
	if q.Limit < 1 || q.Limit > 1024 {
		return nil, fmt.Errorf("invalid submission batch: %d", q.Limit)
	}
	// A zero SubmittedBefore disables the submitted condition.
	var submittedBefore *time.Time
	if !q.SubmittedBefore.IsZero() {
		submittedBefore = &q.SubmittedBefore
	}
	rows, err := s.db.QueryContext(ctx, selectTaskColumns+` WHERE error IS NULL AND status=0 AND is_running=false AND (
		(is_submitted=false AND next_retry<=$1 AND (retry_count>0 OR created_at<=$2))
		OR ($3::timestamptz IS NOT NULL AND is_submitted=true AND updated_at<=$3)
	) ORDER BY next_retry,created_at,id LIMIT $4`, q.Now, q.CreatedBefore, submittedBefore, q.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]*domain.Task, 0, q.Limit)
	for rows.Next() {
		item, err := read(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

var _ port.TaskRepository = (*TaskRepository)(nil)

func (s *TaskRepository) DeleteByID(ctx context.Context, id domain.ID, version int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	item, err := read(tx.QueryRowContext(ctx, selectTaskColumns+` WHERE id=$1 FOR UPDATE`, id))
	if err != nil {
		return err
	}
	if version != 0 && item.Version != version {
		return domain.ErrConflict
	}
	if item.IsRunning {
		return domain.ErrBusy
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM tasks WHERE id=$1 AND version=$2 AND is_running=false`, id, item.Version)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return domain.ErrConflict
	}
	return tx.Commit()
}

// ReleaseStale is the sweeper's time-based release of claims whose consumer is
// gone. A live claim is never stale: the claim and outcome commits both set
// updated_at, and one execution is bounded by the process timeout. The cutoff
// and the release time use the database clock (now(), the transaction start),
// so sweeper clock skew does not matter; claims are still stamped by the
// consumer clock, and staleAfter's margin absorbs consumer-to-database skew.
// Stale rows are locked with SKIP LOCKED, so a claim being committed right now
// is left for the next sweep, and domain.Task.Interrupt decides each outcome.
func (s *TaskRepository) ReleaseStale(ctx context.Context, staleAfter time.Duration, retryDelays []time.Duration) (int64, error) {
	if staleAfter <= 0 {
		return 0, fmt.Errorf("invalid stale threshold: %s", staleAfter)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	var now time.Time
	if err := tx.QueryRowContext(ctx, `SELECT now()`).Scan(&now); err != nil {
		return 0, err
	}
	rows, err := tx.QueryContext(ctx, selectTaskColumns+` WHERE is_running=true AND updated_at<=now()-make_interval(secs => $1) ORDER BY id FOR UPDATE SKIP LOCKED`, staleAfter.Seconds())
	if err != nil {
		return 0, err
	}
	var items []*domain.Task
	for rows.Next() {
		item, err := read(rows)
		if err != nil {
			rows.Close()
			return 0, err
		}
		items = append(items, item)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	if err = rows.Close(); err != nil {
		return 0, err
	}
	for _, item := range items {
		locked := item.Version
		item.Interrupt(now, retryDelays)
		if err := writeTask(ctx, tx, item, locked); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return int64(len(items)), nil
}
