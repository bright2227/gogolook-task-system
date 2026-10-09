package rabbitmq

import (
	"strings"
	"testing"
	"time"

	"github.com/bright2227/vsx-task-system/internal/domain"
)

func TestDecodeValidTaskMessage(t *testing.T) {
	taskID := domain.NewID()
	eventID := domain.NewID()
	m, err := decode([]byte(`{"event_id":"` + string(eventID) + `","task_id":"` + string(taskID) + `"}`))
	if err != nil {
		t.Fatal(err)
	}
	if m.TaskID != taskID || m.EventID != eventID {
		t.Fatalf("decoded message = %#v", m)
	}
}

func TestDecodeNormalizesIDs(t *testing.T) {
	taskID := "00000000-0000-4000-8000-000000000001"
	m, err := decode([]byte(`{"task_id":"` + strings.ToUpper(taskID) + `"}`))
	if err != nil {
		t.Fatal(err)
	}
	if string(m.TaskID) != taskID {
		t.Fatalf("task ID = %q, want normalized %q", m.TaskID, taskID)
	}
}

func TestDecodeRejectsPoisonMessages(t *testing.T) {
	for _, body := range []string{
		`not json`,
		`{"task_id":"not-a-uuid"}`,
		`{"task_id":"00000000-0000-4000-8000-000000000001","unexpected":true}`,
		`{"task_id":"00000000-0000-4000-8000-000000000001"} {}`,
		`{"task_id":"00000000-0000-4000-8000-000000000001","padding":"` + strings.Repeat("x", 1024) + `"}`,
	} {
		t.Run(body, func(t *testing.T) {
			if _, err := decode([]byte(body)); err == nil {
				t.Fatal("decode accepted malformed message")
			}
		})
	}
}

func TestRetryDelayNormalizationBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   time.Duration
		want time.Duration
	}{
		{name: "sub-millisecond defaults", in: time.Nanosecond, want: 2 * time.Second},
		{name: "minimum accepted", in: time.Millisecond, want: time.Millisecond},
		{name: "maximum accepted", in: time.Duration(2147483647) * time.Millisecond, want: time.Duration(2147483647) * time.Millisecond},
		{name: "above int32 milliseconds defaults", in: time.Duration(2147483648) * time.Millisecond, want: 2 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := (Config{RetryDelay: tc.in}).normalized().RetryDelay; got != tc.want {
				t.Fatalf("normalized delay = %s, want %s", got, tc.want)
			}
		})
	}
}
