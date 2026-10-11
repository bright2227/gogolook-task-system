// Package domain contains task lifecycle rules, independent of infrastructure.
package domain

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
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
		return "", ErrInvalidID
	}
	raw := s[:8] + s[9:13] + s[14:18] + s[19:23] + s[24:]
	if _, err := hex.DecodeString(raw); err != nil {
		return "", ErrInvalidID
	}
	return ID(strings.ToLower(s)), nil
}

type Payload struct {
	encoded string
	valid   bool
}

// MaxPayloadBytes bounds a payload's UTF-8 encoding.
const MaxPayloadBytes = 1024

func NewPayload(raw []byte) (Payload, error) {
	// PostgreSQL text cannot store NUL, so it is rejected like in names.
	if len(raw) > MaxPayloadBytes || !utf8.Valid(raw) || bytes.IndexByte(raw, 0) >= 0 {
		return Payload{}, ErrPayload
	}
	return Payload{encoded: string(raw), valid: true}, nil
}
func (p Payload) String() string { return p.encoded }

type Status string

const (
	Pending    Status = "pending"
	Processing Status = "processing"
	Completed  Status = "completed"
)

// Task tracks editable content and the current execution claim.
type Task struct {
	ID                   ID
	Name                 string
	Payload              Payload
	Version              int64
	IsRunning            bool
	Status               Status
	CreatedAt, UpdatedAt time.Time
	Result, Error        *string
	IsSubmitted          bool
	// RetryCount counts executions that ended without an outcome: network
	// failures, per-task deadlines and interrupted (stale) claims share it.
	RetryCount int
	NextRetry  time.Time
}

func Submit(payload Payload, now time.Time) (*Task, error) {
	if !payload.valid {
		return nil, ErrPayload
	}
	if now.IsZero() {
		return nil, errors.New("submission time is required")
	}
	now = taskTime(now)
	return &Task{ID: NewID(), Payload: payload, Version: 1, Status: Pending, CreatedAt: now, UpdatedAt: now, NextRetry: now}, nil
}

// PostgreSQL persists microseconds. Truncating before persistence keeps due
// times from rounding into the future and gives every repository the same keys.
func taskTime(t time.Time) time.Time { return t.UTC().Truncate(time.Microsecond) }

// MaxNameBytes bounds a name's UTF-8 encoding.
const MaxNameBytes = 256

func validName(name string) bool {
	return len(name) <= MaxNameBytes && utf8.ValidString(name) && !strings.ContainsRune(name, '\x00')
}

// ValidateName requires nonblank names for new submissions and edits. Restore
// still accepts unnamed tasks persisted before names became required.
func ValidateName(name string) error {
	if !validName(name) || strings.TrimSpace(name) == "" {
		return ErrName
	}
	return nil
}

// Restore validates and detaches a persisted task, including nullable values.
func Restore(s Task) (*Task, error) {
	if _, err := ParseID(string(s.ID)); err != nil {
		return nil, err
	}
	if !validName(s.Name) {
		return nil, ErrName
	}
	if !s.Payload.valid || s.Version < 1 || s.CreatedAt.IsZero() || s.UpdatedAt.Before(s.CreatedAt) || s.RetryCount < 0 {
		return nil, errors.New("invalid task")
	}
	switch s.Status {
	case Pending, Processing, Completed:
	default:
		return nil, fmt.Errorf("invalid task status: %s", s.Status)
	}
	if s.IsRunning != (s.Status == Processing) || (s.Status == Completed) != (s.Result != nil || s.Error != nil) {
		return nil, errors.New("invalid task state")
	}
	s.CreatedAt = taskTime(s.CreatedAt)
	s.UpdatedAt = taskTime(s.UpdatedAt)
	s.NextRetry = taskTime(s.NextRetry)
	if s.Result != nil {
		value := *s.Result
		s.Result = &value
	}
	if s.Error != nil {
		value := *s.Error
		s.Error = &value
	}
	return &s, nil
}

// revise records a change visible to API clients: Version is the whole-row
// revision used for ETags and every optimistic-concurrency check.
func (t *Task) revise(now time.Time) {
	t.Version++
	t.changed(now)
}

func (t *Task) changed(now time.Time) {
	now = taskTime(now)
	if now.Before(t.UpdatedAt) {
		now = t.UpdatedAt
	}
	t.UpdatedAt = now
}

func (t *Task) Edit(name string, payload Payload, now time.Time) (bool, error) {
	if t.IsRunning {
		return false, ErrBusy
	}
	if err := ValidateName(name); err != nil {
		return false, err
	}
	if !payload.valid {
		return false, ErrPayload
	}
	payloadChanged := payload != t.Payload
	if name == t.Name && !payloadChanged {
		return false, nil
	}
	if now.IsZero() {
		return false, errors.New("edit time is required")
	}
	t.Name, t.Payload = name, payload
	if payloadChanged {
		t.Status = Pending
		t.IsSubmitted = false
		t.Result, t.Error = nil, nil
		t.RetryCount = 0
		t.NextRetry = taskTime(now)
	}
	t.revise(now)
	return true, nil
}

// Finished reports that a final outcome is recorded and no automatic
// transition will change it.
func (t *Task) Finished() bool { return t.Status == Completed }

// Start atomically claims pending work.
// A claim proves the message was published, so it also marks the task submitted.
func (t *Task) Start(now time.Time) (bool, error) {
	if t.Status != Pending || t.IsRunning {
		return false, nil
	}
	if now.Before(t.NextRetry) {
		return false, ErrNotDue
	}
	t.Status = Processing
	t.IsRunning = true
	t.IsSubmitted = true
	t.revise(now)
	return true, nil
}

func (t *Task) Finish(now time.Time, result, failure *string) (bool, error) {
	if t.Status != Processing || !t.IsRunning {
		return false, nil
	}
	if (result == nil) == (failure == nil) {
		return false, errors.New("provide exactly one outcome")
	}
	t.Status = Completed
	t.IsRunning = false
	t.Result, t.Error = result, failure
	t.revise(now)
	return true, nil
}

func (t *Task) Retry(now, nextRetry time.Time) (bool, error) {
	if t.Status != Processing || !t.IsRunning {
		return false, nil
	}
	if now.IsZero() || nextRetry.Before(now) {
		return false, errors.New("invalid retry time")
	}
	t.Status = Pending
	t.IsRunning = false
	t.IsSubmitted = false
	t.RetryCount++
	t.NextRetry = taskTime(nextRetry)
	t.Result, t.Error = nil, nil
	t.revise(now)
	return true, nil
}

// Release returns a claim interrupted by graceful shutdown to due, unsubmitted
// pending work without spending any budget, so the sweeper republishes it.
func (t *Task) Release(now time.Time) bool {
	if t.Status != Processing || !t.IsRunning {
		return false
	}
	t.Status = Pending
	t.IsRunning = false
	t.IsSubmitted = false
	t.NextRetry = taskTime(now)
	t.revise(now)
	return true
}

// Interrupt releases a claim whose holder is gone: the sweeper found it stale.
// The interrupted execution spends the same retry budget as a network failure:
// within len(retryDelays) the task returns to unsubmitted pending work due
// after retryDelays[RetryCount]; past it the task completes as abandoned. It
// does nothing and returns false when the task is not running.
func (t *Task) Interrupt(now time.Time, retryDelays []time.Duration) (abandoned bool) {
	if t.Status != Processing || !t.IsRunning {
		return false
	}
	t.IsRunning = false
	if t.RetryCount < len(retryDelays) {
		t.Status = Pending
		t.IsSubmitted = false
		t.NextRetry = taskTime(now.Add(retryDelays[t.RetryCount]))
		t.RetryCount++
	} else {
		failure := fmt.Sprintf("abandoned after %d attempts; the last execution was interrupted", t.RetryCount+1)
		t.Status, t.Error = Completed, &failure
		abandoned = true
	}
	t.revise(now)
	return abandoned
}

// MarkSubmitted records a confirmed publication. Callers update through the
// listed Version, so any claim, retry or edit since listing is a conflict.
// It always refreshes UpdatedAt, so a republished message is not swept again
// before RepublishAfter. It keeps Version: publication must not invalidate a
// client's ETag.
func (t *Task) MarkSubmitted(now time.Time) {
	t.IsSubmitted = true
	t.changed(now)
}

// Postpone backs off a task whose publication the broker rejected, so it does
// not take the first sweep slots again before until. Callers update through the
// listed Version. For an unsubmitted task it moves NextRetry to until; a
// submitted task (a republication) already backs off for RepublishAfter through
// UpdatedAt, and its NextRetry stays so a surviving message is not early. Like
// MarkSubmitted it refreshes UpdatedAt and keeps Version.
func (t *Task) Postpone(now, until time.Time) {
	if !t.IsSubmitted {
		t.NextRetry = taskTime(until)
	}
	t.changed(now)
}

// Unsubmit hands a task back to the sweeper: a message arrived before the task
// was due (clock skew, or an old copy after a Retry) and is being ACKed, so the
// sweeper must publish again when due. It bumps Version even when the task is
// already unsubmitted: a publication marker for that message, written later
// through the listed Version, then conflicts and is skipped instead of marking
// the task submitted with no message left.
func (t *Task) Unsubmit(now time.Time) {
	t.IsSubmitted = false
	t.revise(now)
}
