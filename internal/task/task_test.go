package task

import (
	"errors"
	"testing"
	"time"
)

func TestPayloadValidationAndCopy(t *testing.T) {
	for _, raw := range [][]byte{nil, []byte("{"), []byte{0xff}, make([]byte, 65537)} {
		if _, err := NewPayload(raw); !errors.Is(err, ErrPayload) {
			t.Fatalf("NewPayload(%d bytes) err=%v", len(raw), err)
		}
	}
	p, err := NewPayload([]byte(" { \"x\" : 1 } "))
	if err != nil || p.String() != `{"x":1}` {
		t.Fatalf("payload=%q err=%v", p.String(), err)
	}
	copy := p.JSON()
	copy[2] = 'z'
	if p.String() != `{"x":1}` {
		t.Fatal("JSON exposed mutable payload")
	}
}

func TestLifecycleCancellationAndFencedLeaseRecovery(t *testing.T) {
	now := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	p, _ := NewPayload([]byte(`{"work":true}`))
	task, err := Submit(p, now)
	if err != nil {
		t.Fatal(err)
	}
	if s := task.Snapshot(); s.Status != Pending || s.Version != 0 {
		t.Fatalf("initial state: %+v", s)
	}
	first, second := NewID(), NewID()
	if ok, err := task.Start(first, now, time.Second); err != nil || !ok {
		t.Fatalf("Start: %v %v", ok, err)
	}
	if ok, err := task.Start(second, now.Add(500*time.Millisecond), time.Second); ok || !errors.Is(err, ErrBusy) {
		t.Fatalf("overlap: %v %v", ok, err)
	}
	if ok, err := task.Start(second, now.Add(time.Second), time.Second); err != nil || !ok {
		t.Fatalf("expired lease recovery: %v %v", ok, err)
	}
	result := "stale"
	if ok, err := task.Finish(first, now.Add(2*time.Second), &result, nil); err != nil || ok {
		t.Fatalf("stale finish: %v %v", ok, err)
	}
	if changed, err := task.Cancel(now.Add(2 * time.Second)); err != nil || !changed {
		t.Fatalf("cancel: %v %v", changed, err)
	}
	result = "too late"
	if ok, err := task.Finish(second, now.Add(3*time.Second), &result, nil); err != nil || ok {
		t.Fatalf("finish after cancel: %v %v", ok, err)
	}
	if s := task.Snapshot(); s.Status != Canceled || s.Attempts != 2 || s.Result != nil {
		t.Fatalf("final state: %+v", s)
	}
	if _, err := task.Cancel(now.Add(4 * time.Second)); err != nil {
		t.Fatal(err)
	}
}

func TestCompletedTaskCannotBeCanceled(t *testing.T) {
	now := time.Now().UTC()
	p, _ := NewPayload([]byte(`{}`))
	tsk, _ := Submit(p, now)
	token := NewID()
	_, _ = tsk.Start(token, now, time.Second)
	result := "done"
	_, err := tsk.Finish(token, now.Add(time.Second), &result, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tsk.Cancel(now.Add(2 * time.Second)); !errors.Is(err, ErrCompleted) {
		t.Fatalf("Cancel error=%v", err)
	}
	if tsk.Snapshot().Status != Completed {
		t.Fatal("completed task changed state")
	}
}
