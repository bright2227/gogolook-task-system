package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/bright2227/vsx-task-system/internal/app"
	"github.com/bright2227/vsx-task-system/internal/task"
	_ "github.com/lib/pq"
)

type Store struct{ DB *sql.DB }

func Open(ctx context.Context, url string) (*Store, error) {
	db, err := sql.Open("postgres", url)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(32)
	db.SetMaxIdleConns(8)
	db.SetConnMaxLifetime(30 * time.Minute)
	if err = db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db}, nil
}
func (s *Store) Close() error { return s.DB.Close() }
func (s *Store) Get(ctx context.Context, id task.ID) (*task.Task, error) {
	return read(s.DB.QueryRowContext(ctx, selectTask, id))
}
func (s *Store) Within(ctx context.Context, fn func(app.Repository) error) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = fn(repository{tx}); err != nil {
		return err
	}
	return tx.Commit()
}

const selectTask = `SELECT id,payload,status,created_at,updated_at,version,attempts,result,error,lease_token,lease_until FROM tasks WHERE id=$1`

type scanner interface{ Scan(...any) error }

func read(row scanner) (*task.Task, error) {
	var s task.Snapshot
	var raw []byte
	var result, failure, token sql.NullString
	var lease sql.NullTime
	err := row.Scan(&s.ID, &raw, &s.Status, &s.CreatedAt, &s.UpdatedAt, &s.Version, &s.Attempts, &result, &failure, &token, &lease)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, app.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	s.Payload, err = task.NewPayload(raw)
	if err != nil {
		return nil, err
	}
	if result.Valid {
		s.Result = &result.String
	}
	if failure.Valid {
		s.Error = &failure.String
	}
	if token.Valid {
		s.LeaseToken = task.ID(token.String)
	}
	if lease.Valid {
		s.LeaseUntil = &lease.Time
	}
	return task.Restore(s)
}

type repository struct{ tx *sql.Tx }

func (r repository) Get(ctx context.Context, id task.ID) (*task.Task, error) {
	return read(r.tx.QueryRowContext(ctx, selectTask+` FOR UPDATE`, id))
}
func nullableToken(id task.ID) any {
	if id == "" {
		return nil
	}
	return string(id)
}
func (r repository) Insert(ctx context.Context, t *task.Task) error {
	s := t.Snapshot()
	_, err := r.tx.ExecContext(ctx, `INSERT INTO tasks (id,payload,status,created_at,updated_at,version,attempts) VALUES ($1,$2,$3,$4,$5,$6,$7)`, s.ID, s.Payload.String(), s.Status, s.CreatedAt, s.UpdatedAt, s.Version, s.Attempts)
	if err != nil {
		return err
	}
	return r.events(ctx, t)
}
func (r repository) Save(ctx context.Context, t *task.Task, expected int64) error {
	s := t.Snapshot()
	res, err := r.tx.ExecContext(ctx, `UPDATE tasks SET status=$2,updated_at=$3,version=$4,attempts=$5,result=$6,error=$7,lease_token=$8,lease_until=$9 WHERE id=$1 AND version=$10`, s.ID, s.Status, s.UpdatedAt, s.Version, s.Attempts, s.Result, s.Error, nullableToken(s.LeaseToken), s.LeaseUntil, expected)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return app.ErrConflict
	}
	return r.events(ctx, t)
}
func (r repository) events(ctx context.Context, t *task.Task) error {
	for _, e := range t.Events() {
		_, err := r.tx.ExecContext(ctx, `INSERT INTO task_events (event_id,task_id,version,name,occurred_at) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (event_id) DO NOTHING`, e.ID, e.TaskID, e.Version, e.Name, e.OccurredAt)
		if err != nil {
			return err
		}
		if e.Name == "TaskSubmitted" {
			_, err = r.tx.ExecContext(ctx, `INSERT INTO outbox (event_id,task_id,available_at) VALUES ($1,$2,$3) ON CONFLICT (event_id) DO NOTHING`, e.ID, e.TaskID, e.OccurredAt)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Reserve leases a bounded batch in one statement. Network I/O never holds row locks.
func (s *Store) Reserve(ctx context.Context, limit int) ([]app.Message, error) {
	if limit < 1 || limit > 1024 {
		return nil, fmt.Errorf("invalid outbox batch: %d", limit)
	}
	token := task.NewID()
	rows, err := s.DB.QueryContext(ctx, `WITH candidates AS (
	 SELECT event_id FROM outbox WHERE published_at IS NULL AND available_at<=now()
	 AND (lease_until IS NULL OR lease_until<=now()) ORDER BY available_at,event_id
	 LIMIT $1 FOR UPDATE SKIP LOCKED
	) UPDATE outbox o SET lease_token=$2,lease_until=now()+interval '30 seconds'
	FROM candidates c WHERE o.event_id=c.event_id RETURNING o.event_id,o.task_id`, limit, token)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var messages []app.Message
	for rows.Next() {
		m := app.Message{Token: token}
		if err = rows.Scan(&m.EventID, &m.TaskID); err != nil {
			return nil, err
		}
		messages = append(messages, m)
	}
	return messages, rows.Err()
}
func (s *Store) Published(ctx context.Context, m app.Message) error {
	r, err := s.DB.ExecContext(ctx, `UPDATE outbox SET published_at=now(),lease_token=NULL,lease_until=NULL WHERE event_id=$1 AND lease_token=$2 AND published_at IS NULL`, m.EventID, m.Token)
	if err != nil {
		return err
	}
	n, err := r.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return app.ErrConflict
	}
	return nil
}
func (s *Store) Release(ctx context.Context, m app.Message) error {
	_, err := s.DB.ExecContext(ctx, `UPDATE outbox SET available_at=now()+interval '1 second',lease_token=NULL,lease_until=NULL WHERE event_id=$1 AND lease_token=$2 AND published_at IS NULL`, m.EventID, m.Token)
	return err
}
