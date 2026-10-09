package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/bright2227/gogolook-task-system/internal/domain"
	"github.com/bright2227/gogolook-task-system/internal/usecase"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestDecodeValidTaskMessage(t *testing.T) {
	taskID := domain.NewID()
	eventID := domain.NewID()
	executionID := domain.NewID()
	m, err := decode([]byte(`{"version":1,"event_id":"` + string(eventID) + `","task_id":"` + string(taskID) + `","execution_id":"` + string(executionID) + `"}`))
	if err != nil {
		t.Fatal(err)
	}
	if m.TaskID != taskID || m.EventID != eventID || m.ExecutionID != executionID {
		t.Fatalf("decoded message = %#v", m)
	}
}

func TestDecodeNormalizesIDs(t *testing.T) {
	taskID := "00000000-0000-4000-8000-000000000001"
	m, err := decode([]byte(`{"version":1,"task_id":"` + strings.ToUpper(taskID) + `","event_id":"00000000-0000-4000-8000-000000000002","execution_id":"00000000-0000-4000-8000-000000000003"}`))
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
		`{"version":1,"task_id":"not-a-uuid","event_id":"00000000-0000-4000-8000-000000000002","execution_id":"00000000-0000-4000-8000-000000000003"}`,
		`{"version":1,"task_id":"00000000-0000-4000-8000-000000000001","event_id":"00000000-0000-4000-8000-000000000002","execution_id":"00000000-0000-4000-8000-000000000003","unexpected":true}`,
		`{"version":1,"task_id":"00000000-0000-4000-8000-000000000001","event_id":"00000000-0000-4000-8000-000000000002","execution_id":"00000000-0000-4000-8000-000000000003"} {}`,
		`{"version":1,"task_id":"00000000-0000-4000-8000-000000000001","event_id":"00000000-0000-4000-8000-000000000002","execution_id":"00000000-0000-4000-8000-000000000003","padding":"` + strings.Repeat("x", 1024) + `"}`,
		`{"version":2,"task_id":"00000000-0000-4000-8000-000000000001","event_id":"00000000-0000-4000-8000-000000000002","execution_id":"00000000-0000-4000-8000-000000000003"}`,
	} {
		t.Run(body, func(t *testing.T) {
			if _, err := decode([]byte(body)); err == nil {
				t.Fatal("decode accepted malformed message")
			}
		})
	}
}

func TestMessageRequiresAndEncodesExecutionIdentity(t *testing.T) {
	m := usecase.Message{
		EventID: domain.NewID(), TaskID: domain.NewID(), ExecutionID: domain.NewID(),
		Destination: usecase.DestinationWork,
	}
	body, err := message(m)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decode(body)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.TaskID != m.TaskID || decoded.EventID != m.EventID || decoded.ExecutionID != m.ExecutionID {
		t.Fatalf("decoded message = %#v, want task/event/execution IDs %s/%s/%s", decoded, m.TaskID, m.EventID, m.ExecutionID)
	}
	m.ExecutionID = ""
	if _, err = message(m); err == nil {
		t.Fatal("message accepted without execution ID")
	}
}

type countingAcknowledger struct {
	calls    int
	tag      uint64
	multiple bool
}

func (a *countingAcknowledger) Ack(tag uint64, multiple bool) error {
	a.calls++
	a.tag = tag
	a.multiple = multiple
	return nil
}

func (*countingAcknowledger) Nack(uint64, bool, bool) error { return nil }
func (*countingAcknowledger) Reject(uint64, bool) error     { return nil }

func TestHandleAcknowledgesOnlyCommittedDecision(t *testing.T) {
	messageBody, err := message(usecase.Message{
		EventID: domain.NewID(), TaskID: domain.NewID(), ExecutionID: domain.NewID(),
	})
	if err != nil {
		t.Fatal(err)
	}
	called := 0
	processor := usecase.TaskProcessorFunc(func(context.Context, usecase.ProcessTaskRequest) error {
		called++
		return usecase.ErrBusy
	})
	client := New(Config{})
	acknowledger := &countingAcknowledger{}
	delivery := amqp.Delivery{Acknowledger: acknowledger, DeliveryTag: 11, Body: messageBody}
	err = client.handle(context.Background(), delivery, processor)
	if !errors.Is(err, usecase.ErrBusy) {
		t.Fatalf("handle error = %v, want ErrBusy", err)
	}
	if called != 1 || acknowledger.calls != 0 {
		t.Fatalf("processor calls/acks = %d/%d, want 1/0", called, acknowledger.calls)
	}

	acknowledger = &countingAcknowledger{}
	delivery.Acknowledger = acknowledger
	processor = usecase.TaskProcessorFunc(func(context.Context, usecase.ProcessTaskRequest) error { return nil })
	if err := client.handle(context.Background(), delivery, processor); err != nil {
		t.Fatalf("handle committed decision: %v", err)
	}
	if acknowledger.calls != 1 || acknowledger.tag != 11 || acknowledger.multiple {
		t.Fatalf("acknowledgements = %+v, want one single-message ack for tag 11", acknowledger)
	}
}

func TestParseFailureEnvelope(t *testing.T) {
	messageID := domain.NewID()
	taskID := domain.NewID()
	eventID := domain.NewID()
	executionID := domain.NewID()
	sourceEventID := domain.NewID()
	failedAt := time.Now().UTC()
	body, err := json.Marshal(FailureEnvelope{
		Version: FailureEnvelopeVersion, MessageID: string(messageID), TaskID: taskID,
		EventID: eventID, ExecutionID: executionID, FailedAt: failedAt,
		Failure: usecase.FailureRecord{
			Category: "execution", Code: "exhausted", Detail: "retry budget exhausted",
			RetryCount: 3, ReplayCycle: 2, Attempts: 7, FailedAt: failedAt, SourceEventID: sourceEventID,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseFailureEnvelope(body)
	if err != nil {
		t.Fatal(err)
	}
	if got.MessageID != string(messageID) || got.TaskID != taskID || got.ExecutionID != executionID {
		t.Fatalf("parsed failure envelope = %#v", got)
	}
	if got.Failure.RetryCount != 3 || got.Failure.ReplayCycle != 2 || got.Failure.Attempts != 7 || got.Failure.SourceEventID != sourceEventID {
		t.Fatalf("failure metadata was not preserved: %+v", got.Failure)
	}

	workMessage := usecase.Message{
		EventID: eventID, TaskID: taskID, ExecutionID: executionID,
		Destination: usecase.DestinationDeadLetter, AvailableAt: failedAt,
		Failure: &got.Failure,
	}
	published, err := failureEnvelope(workMessage)
	if err != nil {
		t.Fatal(err)
	}
	if published.MessageID != string(eventID) || published.Failure.SourceEventID != sourceEventID || published.Failure.Attempts != 7 || !published.FailedAt.Equal(failedAt) {
		t.Fatalf("publisher dropped failure metadata: %+v", published)
	}
	if _, err := ParseFailureEnvelope([]byte(`{"version":1,"message_id":"bad","failure":{"category":"x","code":"y"}}`)); err == nil {
		t.Fatal("accepted invalid failure message ID")
	}
}

func TestReconnectJitterIsBoundedByBaseDelay(t *testing.T) {
	delay := 4 * time.Second
	for i := 0; i < 100; i++ {
		got := jitter(delay)
		if got < delay/2 || got >= delay {
			t.Fatalf("jitter(%s) = %s, want [%s,%s)", delay, got, delay/2, delay)
		}
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
