package rabbitmq

import (
	"strings"
	"testing"

	"github.com/bright2227/vsx-task-system/internal/task"
)

func TestDecodeValidTaskMessage(t *testing.T) {
	taskID := task.NewID()
	eventID := task.NewID()
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
