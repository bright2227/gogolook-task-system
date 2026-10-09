package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/bright2227/gogolook-task-system/internal/domain"
	"github.com/bright2227/gogolook-task-system/internal/usecase"
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
func (s *Store) Get(ctx context.Context, id domain.ID) (*domain.Task, error) {
	return read(s.DB.QueryRowContext(ctx, selectTask, id))
}
func (s *Store) Within(ctx context.Context, fn func(usecase.Repository) error) error {
	tx, err := s.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = fn(repository{tx}); err != nil {
		return err
	}
	return tx.Commit()
}

const selectTask = `SELECT id,payload,status,created_at,updated_at,version,attempts,result,error,lease_token,lease_until,execution_id,retry_count,replay_cycle,not_before FROM tasks WHERE id=$1`

type scanner interface{ Scan(...any) error }

func read(row scanner) (*domain.Task, error) {
	var s domain.Snapshot
	var raw []byte
	var result, failure, token sql.NullString
	var lease sql.NullTime
	err := row.Scan(&s.ID, &raw, &s.Status, &s.CreatedAt, &s.UpdatedAt, &s.Version, &s.Attempts, &result, &failure, &token, &lease, &s.ExecutionID, &s.RetryCount, &s.ReplayCycle, &s.NotBefore)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, usecase.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	s.Payload, err = domain.NewPayload(raw)
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
		s.LeaseToken = domain.ID(token.String)
	}
	if lease.Valid {
		s.LeaseUntil = &lease.Time
	}
	return domain.Restore(s)
}

type repository struct{ tx *sql.Tx }

func (r repository) Get(ctx context.Context, id domain.ID) (*domain.Task, error) {
	return read(r.tx.QueryRowContext(ctx, selectTask+` FOR UPDATE`, id))
}
func nullableToken(id domain.ID) any {
	if id == "" {
		return nil
	}
	return string(id)
}
func (r repository) Insert(ctx context.Context, t *domain.Task) error {
	s := t.Snapshot()
	_, err := r.tx.ExecContext(ctx, `INSERT INTO tasks (id,payload,status,created_at,updated_at,version,attempts,execution_id,retry_count,replay_cycle,not_before) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, s.ID, s.Payload.String(), s.Status, s.CreatedAt, s.UpdatedAt, s.Version, s.Attempts, s.ExecutionID, s.RetryCount, s.ReplayCycle, s.NotBefore)
	if err != nil {
		return err
	}
	return r.events(ctx, t)
}
func (r repository) Save(ctx context.Context, t *domain.Task, expected int64) error {
	s := t.Snapshot()
	res, err := r.tx.ExecContext(ctx, `UPDATE tasks SET status=$2,updated_at=$3,version=$4,attempts=$5,result=$6,error=$7,lease_token=$8,lease_until=$9,execution_id=$10,retry_count=$11,replay_cycle=$12,not_before=$13 WHERE id=$1 AND version=$14`, s.ID, s.Status, s.UpdatedAt, s.Version, s.Attempts, s.Result, s.Error, nullableToken(s.LeaseToken), s.LeaseUntil, s.ExecutionID, s.RetryCount, s.ReplayCycle, s.NotBefore, expected)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return usecase.ErrConflict
	}
	return r.events(ctx, t)
}
func (r repository) events(ctx context.Context, t *domain.Task) error {
	for _, e := range t.Events() {
		_, err := r.tx.ExecContext(ctx, `INSERT INTO task_events (event_id,task_id,version,name,occurred_at) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (event_id) DO NOTHING`, e.ID, e.TaskID, e.Version, e.Name, e.OccurredAt)
		if err != nil {
			return err
		}
		if e.Name == "TaskSubmitted" {
			_, err = r.tx.ExecContext(ctx, `INSERT INTO outbox (event_id,task_id,execution_id,destination,available_at) VALUES ($1,$2,$3,'work',$4) ON CONFLICT (event_id) DO NOTHING`, e.ID, e.TaskID, e.ExecutionID, e.AvailableAt)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (r repository) Enqueue(ctx context.Context, m usecase.Message) error {
	var failure any
	if m.Failure != nil {
		encoded, err := json.Marshal(m.Failure)
		if err != nil {
			return err
		}
		failure = string(encoded)
	}
	_, err := r.tx.ExecContext(ctx, `INSERT INTO outbox(event_id,task_id,execution_id,destination,available_at,failure) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(event_id) DO NOTHING`, m.EventID, m.TaskID, m.ExecutionID, string(m.Destination), m.AvailableAt, failure)
	return err
}
func (r repository) StartAttempt(ctx context.Context, a usecase.AttemptRecord) error {
	_, err := r.tx.ExecContext(ctx, `UPDATE task_attempts SET finished_at=$2,outcome='interrupted',error='lease recovered' WHERE task_id=$1 AND finished_at IS NULL`, a.TaskID, a.StartedAt)
	if err != nil {
		return err
	}
	_, err = r.tx.ExecContext(ctx, `INSERT INTO task_attempts(lease_token,task_id,execution_id,attempt,retry_count,replay_cycle,worker_id,started_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, a.LeaseToken, a.TaskID, a.ExecutionID, a.Attempt, a.RetryCount, a.ReplayCycle, a.WorkerID, a.StartedAt)
	return err
}
func (r repository) FinalizeAttempt(ctx context.Context, a usecase.AttemptRecord) error {
	res, err := r.tx.ExecContext(ctx, `UPDATE task_attempts SET finished_at=$2,outcome=$3,error=$4,category=$5,code=$6 WHERE lease_token=$1 AND finished_at IS NULL`, a.LeaseToken, a.FinishedAt, a.Outcome, nullableString(a.Error), nullableString(a.Category), nullableString(a.Code))
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		var exists bool
		if err = r.tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM task_attempts WHERE lease_token=$1)`, a.LeaseToken).Scan(&exists); err != nil {
			return err
		}
		if exists {
			return nil
		} // a prior cancel/recovery already finalized it; never overwrite that outcome.
		return usecase.ErrNotFound
	}
	return nil
}
func nullableString(s string) any {
	if s == "" {
		return nil
	}
	return s
}
func (r repository) AddDeadLetter(ctx context.Context, d usecase.DeadLetterRecord) error {
	_, err := r.tx.ExecContext(ctx, `INSERT INTO task_dead_letters(message_id,source_event_id,task_id,execution_id,retry_count,replay_cycle,attempts,failed_at,category,code,detail,worker_id,original_payload,original_hash,original_truncated) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) ON CONFLICT(message_id) DO NOTHING`, d.MessageID, d.SourceEventID, d.TaskID, d.ExecutionID, d.RetryCount, d.ReplayCycle, d.Attempts, d.FailedAt, d.Category, d.Code, d.Detail, d.WorkerID, d.Original, d.OriginalHash, d.OriginalTruncated)
	return err
}
func (r repository) GetDeadLetter(ctx context.Context, id string) (usecase.DeadLetterRecord, error) {
	var d usecase.DeadLetterRecord
	err := r.tx.QueryRowContext(ctx, `SELECT message_id,source_event_id,task_id,execution_id,retry_count,replay_cycle,attempts,failed_at,category,code,detail,worker_id,original_payload,original_hash,original_truncated FROM task_dead_letters WHERE message_id=$1 FOR UPDATE`, id).Scan(&d.MessageID, &d.SourceEventID, &d.TaskID, &d.ExecutionID, &d.RetryCount, &d.ReplayCycle, &d.Attempts, &d.FailedAt, &d.Category, &d.Code, &d.Detail, &d.WorkerID, &d.Original, &d.OriginalHash, &d.OriginalTruncated)
	if errors.Is(err, sql.ErrNoRows) {
		return d, usecase.ErrNotFound
	}
	if err == nil {
		d.EventID = domain.ID(d.MessageID)
	}
	return d, err
}
func (r repository) GetReplayReceipt(ctx context.Context, id string) (usecase.ReplayReceipt, error) {
	var v usecase.ReplayReceipt
	err := r.tx.QueryRowContext(ctx, `SELECT message_id,fingerprint,operator,reason,task_id,execution_id,replayed_at FROM task_replay_receipts WHERE message_id=$1`, id).Scan(&v.MessageID, &v.Fingerprint, &v.Operator, &v.Reason, &v.TaskID, &v.ExecutionID, &v.ReplayedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return v, usecase.ErrNotFound
	}
	return v, err
}
func (r repository) InsertReplayReceipt(ctx context.Context, v usecase.ReplayReceipt) error {
	res, err := r.tx.ExecContext(ctx, `INSERT INTO task_replay_receipts(message_id,fingerprint,operator,reason,task_id,execution_id,replayed_at) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(message_id) DO NOTHING`, v.MessageID, v.Fingerprint, v.Operator, v.Reason, v.TaskID, v.ExecutionID, v.ReplayedAt)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err == nil && n != 1 {
		return usecase.ErrAlreadyReplayed
	}
	return err
}

// Reserve leases a bounded batch in one statement. Network I/O never holds row locks.
func (s *Store) Reserve(ctx context.Context, limit int) ([]usecase.Message, error) {
	if limit < 1 || limit > 1024 {
		return nil, fmt.Errorf("invalid outbox batch: %d", limit)
	}
	token := domain.NewID()
	rows, err := s.DB.QueryContext(ctx, `WITH candidates AS (
	 SELECT event_id FROM outbox WHERE published_at IS NULL AND available_at<=now()
	 AND (lease_until IS NULL OR lease_until<=now()) ORDER BY available_at,event_id
	 LIMIT $1 FOR UPDATE SKIP LOCKED
	) UPDATE outbox o SET lease_token=$2,lease_until=now()+interval '30 seconds'
	FROM candidates c WHERE o.event_id=c.event_id RETURNING o.event_id,o.task_id,o.execution_id,o.destination,o.available_at,o.failure`, limit, token)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var messages []usecase.Message
	for rows.Next() {
		m := usecase.Message{Token: token}
		var destination string
		var raw []byte
		if err = rows.Scan(&m.EventID, &m.TaskID, &m.ExecutionID, &destination, &m.AvailableAt, &raw); err != nil {
			return nil, err
		}
		m.Destination = usecase.Destination(destination)
		if len(raw) > 0 {
			if err = json.Unmarshal(raw, &m.Failure); err != nil {
				return nil, err
			}
		}
		messages = append(messages, m)
	}
	return messages, rows.Err()
}
func (s *Store) Published(ctx context.Context, m usecase.Message) error {
	r, err := s.DB.ExecContext(ctx, `UPDATE outbox SET published_at=now(),lease_token=NULL,lease_until=NULL WHERE event_id=$1 AND lease_token=$2 AND published_at IS NULL`, m.EventID, m.Token)
	if err != nil {
		return err
	}
	n, err := r.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return usecase.ErrConflict
	}
	return nil
}
func (s *Store) Release(ctx context.Context, m usecase.Message) error {
	_, err := s.DB.ExecContext(ctx, `UPDATE outbox SET available_at=now()+interval '1 second',lease_token=NULL,lease_until=NULL WHERE event_id=$1 AND lease_token=$2 AND published_at IS NULL`, m.EventID, m.Token)
	return err
}
