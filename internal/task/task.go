// Package task defines the task-processing bounded context. It has no adapters.
package task

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

var (
	ErrPayload   = errors.New("payload must be valid JSON of at most 65536 bytes")
	ErrCompleted = errors.New("completed tasks cannot be canceled")
	ErrBusy      = errors.New("task execution lease is still held")
)

type ID string

func NewID() ID {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	s := hex.EncodeToString(b[:])
	return ID(s[:8] + "-" + s[8:12] + "-" + s[12:16] + "-" + s[16:20] + "-" + s[20:])
}

func ParseID(s string) (ID, error) {
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return "", errors.New("invalid task ID")
	}
	raw := s[:8] + s[9:13] + s[14:18] + s[19:23] + s[24:]
	if _, err := hex.DecodeString(raw); err != nil {
		return "", errors.New("invalid task ID")
	}
	return ID(strings.ToLower(s)), nil
}

// Payload is immutable. JSON returns a copy, so callers cannot alter it.
type Payload struct{ encoded string }

func NewPayload(raw []byte) (Payload, error) {
	if len(raw) > 65536 || !utf8.Valid(raw) || !json.Valid(raw) {
		return Payload{}, ErrPayload
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, raw); err != nil {
		return Payload{}, ErrPayload
	}
	return Payload{compact.String()}, nil
}
func (p Payload) JSON() json.RawMessage { return json.RawMessage(p.encoded) }
func (p Payload) String() string        { return p.encoded }

type Status string

const (
	Pending    Status = "pending"
	Processing Status = "processing"
	Completed  Status = "completed"
	Canceled   Status = "canceled"
)

type Event struct {
	ID         ID
	TaskID     ID
	Version    int64
	Name       string
	OccurredAt time.Time
}

// Snapshot is the explicit persistence contract, not a writable domain entity.
type Snapshot struct {
	ID                   ID
	Payload              Payload
	Status               Status
	CreatedAt, UpdatedAt time.Time
	Version              int64
	Attempts             int
	Result, Error        *string
	LeaseToken           ID
	LeaseUntil           *time.Time
}

type Task struct {
	state  Snapshot
	events []Event
}

func Submit(payload Payload, now time.Time) (*Task, error) {
	if payload.encoded == "" {
		return nil, ErrPayload
	}
	if now.IsZero() {
		return nil, errors.New("submission time is required")
	}
	t := &Task{state: Snapshot{ID: NewID(), Payload: payload, Status: Pending, CreatedAt: now.UTC(), UpdatedAt: now.UTC()}}
	t.record("TaskSubmitted")
	return t, nil
}

func Restore(s Snapshot) (*Task, error) {
	if _, err := ParseID(string(s.ID)); err != nil {
		return nil, err
	}
	if s.Payload.encoded == "" || s.CreatedAt.IsZero() || s.UpdatedAt.Before(s.CreatedAt) || s.Version < 0 || s.Attempts < 0 {
		return nil, errors.New("invalid task snapshot")
	}
	switch s.Status {
	case Pending, Processing, Completed, Canceled:
	default:
		return nil, fmt.Errorf("invalid task status: %s", s.Status)
	}
	if s.Status == Processing {
		if s.LeaseToken == "" || s.LeaseUntil == nil {
			return nil, errors.New("processing requires a lease")
		}
		if _, err := ParseID(string(s.LeaseToken)); err != nil {
			return nil, err
		}
	} else if s.LeaseToken != "" || s.LeaseUntil != nil {
		return nil, errors.New("only processing tasks hold leases")
	}
	if s.Status == Completed {
		if (s.Result == nil) == (s.Error == nil) {
			return nil, errors.New("completed requires exactly one outcome")
		}
	} else if s.Result != nil || s.Error != nil {
		return nil, errors.New("nonterminal task has an outcome")
	}
	return &Task{state: clone(s)}, nil
}

func clone(s Snapshot) Snapshot {
	if s.Result != nil {
		v := *s.Result
		s.Result = &v
	}
	if s.Error != nil {
		v := *s.Error
		s.Error = &v
	}
	if s.LeaseUntil != nil {
		v := *s.LeaseUntil
		s.LeaseUntil = &v
	}
	return s
}
func (t *Task) Snapshot() Snapshot { return clone(t.state) }
func (t *Task) Events() []Event    { return append([]Event(nil), t.events...) }
func (t *Task) record(name string) {
	t.events = append(t.events, Event{NewID(), t.state.ID, t.state.Version, name, t.state.UpdatedAt})
}
func (t *Task) changed(name string, now time.Time) {
	if now.Before(t.state.UpdatedAt) {
		now = t.state.UpdatedAt
	}
	t.state.UpdatedAt = now.UTC()
	t.state.Version++
	t.record(name)
}

func (t *Task) Cancel(now time.Time) (bool, error) {
	if t.state.Status == Completed {
		return false, ErrCompleted
	}
	if t.state.Status == Canceled {
		return false, nil
	}
	t.state.Status = Canceled
	t.state.LeaseToken = ""
	t.state.LeaseUntil = nil
	t.changed("TaskCanceled", now)
	return true, nil
}

// Start grants a new fenced lease. Expired execution can be reclaimed.
func (t *Task) Start(token ID, now time.Time, duration time.Duration) (bool, error) {
	if t.state.Status == Canceled || t.state.Status == Completed {
		return false, nil
	}
	if duration <= 0 || now.IsZero() {
		return false, errors.New("invalid execution lease")
	}
	if _, err := ParseID(string(token)); err != nil {
		return false, err
	}
	if now.Before(t.state.UpdatedAt) {
		now = t.state.UpdatedAt
	}
	if t.state.Status == Processing && t.state.LeaseUntil.After(now) {
		return false, ErrBusy
	}
	until := now.Add(duration).UTC()
	t.state.Status = Processing
	t.state.LeaseToken = token
	t.state.LeaseUntil = &until
	t.state.Attempts++
	t.changed("TaskStarted", now)
	return true, nil
}

// Finish cannot overwrite cancellation or a newer owner's result.
func (t *Task) Finish(token ID, now time.Time, result, failure *string) (bool, error) {
	if t.state.Status != Processing || t.state.LeaseToken != token {
		return false, nil
	}
	if (result == nil) == (failure == nil) {
		return false, errors.New("provide exactly one outcome")
	}
	t.state.Status = Completed
	if result != nil {
		v := *result
		t.state.Result = &v
	}
	if failure != nil {
		v := *failure
		t.state.Error = &v
	}
	t.state.LeaseToken = ""
	t.state.LeaseUntil = nil
	t.changed("TaskCompleted", now)
	return true, nil
}
