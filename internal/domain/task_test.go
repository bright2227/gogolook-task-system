package domain

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestPayloadPreservesSourceStrings(t *testing.T) {
	for _, text := range []string{"", "not JSON", " { \"x\" : 1 } ", "你好🙂", strings.Repeat("a", MaxPayloadBytes)} {
		p, err := NewPayload([]byte(text))
		if err != nil || p.String() != text {
			t.Fatalf("payload changed: %q, %v", p.String(), err)
		}
		task, err := Submit(p, time.Now())
		if err != nil || task.Payload.String() != text {
			t.Fatalf("submit: %v", err)
		}
		if _, err := Restore(*task); err != nil {
			t.Fatal(err)
		}
	}
	for _, raw := range [][]byte{{0xff}, []byte("a\x00b"), []byte(strings.Repeat("a", MaxPayloadBytes+1))} {
		if _, err := NewPayload(raw); !errors.Is(err, ErrPayload) {
			t.Fatalf("payload %q: %v", raw, err)
		}
	}
	if _, err := Submit(Payload{}, time.Now()); !errors.Is(err, ErrPayload) {
		t.Fatalf("uninitialized payload: %v", err)
	}
}

func TestLifecycleClaimsOnceAndFinishes(t *testing.T) {
	now := time.Now().UTC()
	p, _ := NewPayload([]byte("test"))
	task, _ := Submit(p, now)
	if ok, err := task.Start(now); !ok || err != nil {
		t.Fatalf("start=%v,%v", ok, err)
	}
	if ok, err := task.Start(now); ok || err != nil {
		t.Fatalf("duplicate claim=%v,%v", ok, err)
	}
	if !task.IsRunning || task.Status != Processing || !task.IsSubmitted {
		t.Fatalf("claimed task=%+v", task)
	}
	result := "4"
	if ok, err := task.Finish(now, &result, nil); !ok || err != nil || task.Status != Completed || task.IsRunning || !task.IsSubmitted {
		t.Fatalf("finish=%+v,%v", task, err)
	}
	if ok, err := task.Start(now); ok || err != nil {
		t.Fatalf("completed claim=%v,%v", ok, err)
	}
	if !task.Finished() {
		t.Fatalf("completed task not finished=%+v", task)
	}
}
func TestRetryReturnsToUnsubmittedPendingUntilDue(t *testing.T) {
	now := time.Now().UTC()
	p, _ := NewPayload([]byte("test"))
	task, _ := Submit(p, now)
	_, _ = task.Start(now)
	if ok, err := task.Retry(now, now.Add(time.Second)); !ok || err != nil {
		t.Fatalf("retry=%v,%v", ok, err)
	}
	if task.Status != Pending || task.IsRunning || task.IsSubmitted || task.RetryCount != 1 || task.Version != 3 {
		t.Fatalf("retry=%+v", task)
	}
	if _, err := task.Start(now); !errors.Is(err, ErrNotDue) {
		t.Fatalf("early start=%v", err)
	}
	if ok, err := task.Start(now.Add(time.Second)); !ok || err != nil {
		t.Fatalf("due start=%v,%v", ok, err)
	}
}

func TestReleaseReturnsClaimToDuePendingWithoutSpendingBudget(t *testing.T) {
	now := time.Now().UTC()
	p, _ := NewPayload([]byte("test"))
	task, _ := Submit(p, now)
	if task.Release(now) {
		t.Fatal("released an unclaimed task")
	}
	_, _ = task.Start(now)
	task.RetryCount = 1
	later := now.Add(time.Second)
	if !task.Release(later) {
		t.Fatal("release refused a claim")
	}
	if task.Status != Pending || task.IsRunning || task.IsSubmitted || task.RetryCount != 1 || task.Result != nil || task.Error != nil || task.Version != 3 || !task.NextRetry.Equal(later.Truncate(time.Microsecond)) {
		t.Fatalf("release=%+v", task)
	}
	if ok, err := task.Start(later); !ok || err != nil {
		t.Fatalf("released task not due=%v,%v", ok, err)
	}
}

// An interrupted execution spends the network retry budget with its backoff,
// then completes as abandoned.
func TestInterruptSpendsRetryBudgetThenAbandons(t *testing.T) {
	now := time.Now().UTC()
	p, _ := NewPayload([]byte("test"))
	task, _ := Submit(p, now)
	delays := []time.Duration{time.Second, time.Minute}
	if task.Interrupt(now, delays) || task.Version != 1 || task.RetryCount != 0 {
		t.Fatalf("interrupted an unclaimed task=%+v", task)
	}
	for attempt := 1; attempt <= 3; attempt++ {
		if ok, err := task.Start(task.NextRetry); !ok || err != nil {
			t.Fatalf("start %d=%v,%v", attempt, ok, err)
		}
		version := task.Version
		now = task.NextRetry.Add(time.Second)
		abandoned := task.Interrupt(now, delays)
		if task.IsRunning || task.Version != version+1 || !task.UpdatedAt.Equal(now.Truncate(time.Microsecond)) {
			t.Fatalf("interrupt %d=%+v", attempt, task)
		}
		if attempt <= 2 && (abandoned || task.Status != Pending || task.IsSubmitted || task.Error != nil || task.RetryCount != attempt || !task.NextRetry.Equal(now.Add(delays[attempt-1]).Truncate(time.Microsecond))) {
			t.Fatalf("interrupt %d did not retry=%v,%+v", attempt, abandoned, task)
		}
		if attempt == 3 && (!abandoned || !task.Finished() || !task.IsSubmitted || task.RetryCount != 2 || task.Result != nil || task.Error == nil || *task.Error != "abandoned after 3 attempts; the last execution was interrupted") {
			t.Fatalf("interrupt %d did not abandon=%v,%+v", attempt, abandoned, task)
		}
		if _, err := Restore(*task); err != nil {
			t.Fatalf("interrupt %d state invalid: %v", attempt, err)
		}
	}
	// Without retries the first interruption abandons.
	other, _ := Submit(p, now)
	_, _ = other.Start(now)
	if !other.Interrupt(now, nil) || *other.Error != "abandoned after 1 attempts; the last execution was interrupted" {
		t.Fatalf("no budget=%+v", other)
	}
}

func TestTaskTimestampsMatchPersistencePrecision(t *testing.T) {
	now := time.Date(2026, 5, 12, 9, 30, 0, 123456789, time.FixedZone("offset", 8*60*60))
	want := now.UTC().Truncate(time.Microsecond)
	p, _ := NewPayload([]byte("test"))
	task, err := Submit(p, now)
	if err != nil {
		t.Fatal(err)
	}
	for name, got := range map[string]time.Time{"created": task.CreatedAt, "updated": task.UpdatedAt, "due": task.NextRetry} {
		if got != want {
			t.Fatalf("%s time=%s want=%s", name, got.Format(time.RFC3339Nano), want.Format(time.RFC3339Nano))
		}
	}
	// Restoration also covers timestamps assigned by repository callbacks.
	state := *task
	state.CreatedAt, state.UpdatedAt, state.NextRetry = now, now, now.Add(time.Second)
	restored, err := Restore(state)
	if err != nil {
		t.Fatal(err)
	}
	if restored.CreatedAt != want || restored.UpdatedAt != want || restored.NextRetry != want.Add(time.Second) {
		t.Fatalf("restored timestamps=%+v", restored)
	}
	if ok, err := task.Start(now); !ok || err != nil {
		t.Fatalf("submission is not immediately runnable: %v,%v", ok, err)
	}
}

func TestLifecycleTimestampsKeepPrecisionAndNeverGoBackwards(t *testing.T) {
	now := time.Date(2026, 5, 12, 9, 30, 0, 123456789, time.UTC)
	want := now.Truncate(time.Microsecond)
	p, _ := NewPayload([]byte("test"))
	task, _ := Submit(p, now)
	if ok, err := task.Start(now.Add(time.Nanosecond)); !ok || err != nil || task.UpdatedAt != want {
		t.Fatalf("start=%v,%v updated=%s", ok, err, task.UpdatedAt)
	}
	nextRetry := now.Add(time.Second)
	if ok, err := task.Retry(now.Add(time.Nanosecond), nextRetry); !ok || err != nil {
		t.Fatalf("retry=%v,%v", ok, err)
	}
	if task.UpdatedAt != want || task.NextRetry != nextRetry.Truncate(time.Microsecond) {
		t.Fatalf("retry timestamps=%+v", task)
	}
	if ok, err := task.Start(task.NextRetry.Add(-time.Microsecond)); ok || !errors.Is(err, ErrNotDue) {
		t.Fatalf("retry ran before its stored due time: %v,%v", ok, err)
	}
	if ok, err := task.Start(task.NextRetry); !ok || err != nil {
		t.Fatalf("retry skipped at its stored due time: %v,%v", ok, err)
	}
	updated := task.UpdatedAt
	result := "4"
	if ok, err := task.Finish(now, &result, nil); !ok || err != nil || task.UpdatedAt != updated {
		t.Fatalf("finish moved time backwards: %v,%v updated=%s", ok, err, task.UpdatedAt)
	}
	if ok, err := task.Edit("renamed", p, now.Add(2*time.Second)); !ok || err != nil || task.UpdatedAt != want.Add(2*time.Second) {
		t.Fatalf("edit=%v,%v updated=%s", ok, err, task.UpdatedAt)
	}
}

func TestEditPreservesNameOnlyOutcomeAndResetsChangedPayload(t *testing.T) {
	now := time.Date(2026, 10, 10, 1, 0, 0, 123456789, time.UTC)
	payload, _ := NewPayload([]byte("original"))
	task, _ := Submit(payload, now)
	if task.Version != 1 || task.IsRunning || task.Name != "" {
		t.Fatalf("submission defaults=%+v", task)
	}
	_, _ = task.Start(now)
	result := "8"
	_, _ = task.Finish(now, &result, nil)
	task.RetryCount = 2
	due := task.NextRetry
	created := task.CreatedAt
	if changed, err := task.Edit("renamed", payload, now.Add(time.Second)); !changed || err != nil {
		t.Fatalf("name edit=%v,%v", changed, err)
	}
	if task.Version != 4 || task.Name != "renamed" || task.Status != Completed || task.Result == nil || *task.Result != result || task.Error != nil || !task.IsSubmitted || task.RetryCount != 2 || task.NextRetry != due || task.CreatedAt != created {
		t.Fatalf("name edit changed execution state=%+v", task)
	}
	unchanged := *task
	if changed, err := task.Edit(task.Name, payload, now.Add(2*time.Second)); changed || err != nil || *task != unchanged {
		t.Fatalf("identical content changed task=%+v,%v", task, err)
	}
	failure := "old failure"
	task.Result, task.Error = nil, &failure
	newPayload, _ := NewPayload([]byte("new content"))
	editTime := now.Add(3 * time.Second)
	if changed, err := task.Edit("new name", newPayload, editTime); !changed || err != nil {
		t.Fatalf("payload edit=%v,%v", changed, err)
	}
	if task.Version != 5 || task.Name != "new name" || task.Payload != newPayload || task.Status != Pending || task.IsRunning || task.IsSubmitted || task.Result != nil || task.Error != nil || task.RetryCount != 0 || task.NextRetry != editTime.Truncate(time.Microsecond) || task.CreatedAt != created {
		t.Fatalf("payload edit did not reset execution=%+v", task)
	}
}

func TestEditRejectsRunningAndInvalidContentWithoutMutation(t *testing.T) {
	now := time.Now().UTC()
	payload, _ := NewPayload([]byte("original"))
	task, _ := Submit(payload, now)
	before := *task
	for _, tc := range []struct {
		name    string
		payload Payload
		err     error
	}{
		{"", payload, ErrName},
		{" \t\n\u3000", payload, ErrName},
		{string([]byte{0xff}), payload, ErrName},
		{"before\x00after", payload, ErrName},
		{"\x00", payload, ErrName},
		{strings.Repeat("a", MaxNameBytes+1), payload, ErrName},
		{"new name", Payload{}, ErrPayload},
	} {
		if changed, err := task.Edit(tc.name, tc.payload, now); changed || !errors.Is(err, tc.err) || *task != before {
			t.Fatalf("invalid edit changed task=%+v,%v", task, err)
		}
	}
	_, _ = task.Start(now)
	before = *task
	if changed, err := task.Edit(task.Name, task.Payload, now); changed || !errors.Is(err, ErrBusy) || *task != before {
		t.Fatalf("running task accepted edit=%+v,%v", task, err)
	}
}

func TestRestoreValidatesVersionNameAndRunningState(t *testing.T) {
	payload, _ := NewPayload([]byte("original"))
	task, _ := Submit(payload, time.Now())
	for _, name := range []string{string([]byte{0xff}), "before\x00after", "\x00", strings.Repeat("é", MaxNameBytes/2) + "a"} {
		invalid := *task
		invalid.Name = name
		if _, err := Restore(invalid); !errors.Is(err, ErrName) {
			t.Fatalf("restored invalid name %q: %v", name, err)
		}
	}
	failure := "failed"
	for _, mutate := range []func(*Task){
		func(t *Task) { t.Version = 0 },
		func(t *Task) { t.RetryCount = -1 },
		func(t *Task) { t.IsRunning = true },
		func(t *Task) { t.Status = Processing },
		func(t *Task) { t.Status = Completed },
		func(t *Task) { t.Error = &failure },
		func(t *Task) { t.Status = "canceled" },
	} {
		invalid := *task
		mutate(&invalid)
		if _, err := Restore(invalid); err == nil {
			t.Fatalf("restored invalid task=%+v", invalid)
		}
	}
	for _, mutate := range []func(*Task){
		func(t *Task) { t.Status, t.IsRunning = Processing, true },
		func(t *Task) { t.Status, t.Error = Completed, &failure },
	} {
		state := *task
		mutate(&state)
		if _, err := Restore(state); err != nil {
			t.Fatalf("valid task=%+v: %v", state, err)
		}
	}
}

func TestMarkSubmittedRefreshesUpdatedAtWithoutVersion(t *testing.T) {
	p, _ := NewPayload([]byte("text"))
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	task, _ := Submit(p, now)
	for i := 1; i <= 2; i++ {
		at := now.Add(time.Duration(i) * time.Minute)
		if task.MarkSubmitted(at); !task.IsSubmitted || task.Version != 1 || !task.UpdatedAt.Equal(at) {
			t.Fatalf("mark %d=%+v", i, task)
		}
	}
}

// Unsubmit bumps Version even for an unsubmitted task, so a publication
// marker for an early message, written through the listed Version, conflicts.
func TestUnsubmitBumpsVersion(t *testing.T) {
	p, _ := NewPayload([]byte("text"))
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	task, _ := Submit(p, now)
	task.MarkSubmitted(now)
	at := now.Add(time.Minute)
	if task.Unsubmit(at); task.IsSubmitted || task.Version != 2 || task.Status != Pending || !task.UpdatedAt.Equal(at) {
		t.Fatalf("unsubmit=%+v", task)
	}
	if task.Unsubmit(now); task.IsSubmitted || task.Version != 3 || !task.UpdatedAt.Equal(at) {
		t.Fatalf("unsubmit of an unsubmitted task=%+v", task)
	}
}

func TestPostponeMovesUnsubmittedDueTimeWithoutVersion(t *testing.T) {
	p, _ := NewPayload([]byte("text"))
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	task, _ := Submit(p, now)
	at, until := now.Add(time.Second), now.Add(time.Minute)
	if task.Postpone(at, until); task.Version != 1 || task.IsSubmitted || !task.NextRetry.Equal(until) || !task.UpdatedAt.Equal(at) {
		t.Fatalf("postpone=%+v", task)
	}
	if _, err := task.Start(at); !errors.Is(err, ErrNotDue) {
		t.Fatalf("postponed start=%v", err)
	}
	// A submitted task backs off through UpdatedAt only.
	task.MarkSubmitted(at)
	due := task.NextRetry
	if task.Postpone(until, until.Add(time.Hour)); task.Version != 1 || !task.NextRetry.Equal(due) || !task.UpdatedAt.Equal(until) {
		t.Fatalf("postpone submitted=%+v", task)
	}
}

func TestNameLimit(t *testing.T) {
	p, _ := NewPayload([]byte("text"))
	task, _ := Submit(p, time.Now())
	// A 2-byte rune keeps a 256-byte name valid UTF-8 at the limit.
	if changed, err := task.Edit(strings.Repeat("é", MaxNameBytes/2), p, time.Now()); !changed || err != nil {
		t.Fatalf("256-byte name=%v,%v", changed, err)
	}
	if _, err := task.Edit(strings.Repeat("a", MaxNameBytes+1), p, time.Now()); !errors.Is(err, ErrName) {
		t.Fatalf("257-byte name=%v", err)
	}
}
