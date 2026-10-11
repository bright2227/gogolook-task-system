package amqp

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/bright2227/gogolook-task-system/internal/domain"
	"github.com/bright2227/gogolook-task-system/internal/repository/task"
	"github.com/bright2227/gogolook-task-system/internal/usecase"
	amqp091 "github.com/rabbitmq/amqp091-go"
)

func TestDecodeValidTaskIDBody(t *testing.T) {
	taskID := domain.NewID()
	got, err := decode([]byte(taskID))
	if err != nil {
		t.Fatal(err)
	}
	if got != taskID {
		t.Fatalf("decoded task ID = %q want %q", got, taskID)
	}
}

func TestDecodeNormalizesIDs(t *testing.T) {
	taskID := "abcdefab-1234-4abc-8def-abcdefabcdef"
	got, err := decode([]byte(strings.ToUpper(taskID)))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != taskID {
		t.Fatalf("task ID = %q, want normalized %q", got, taskID)
	}
}

func TestDecodeRejectsNonTaskIDs(t *testing.T) {
	for _, body := range []string{"", "not a UUID", `{"task_id":"00000000-0000-4000-8000-000000000001"}`, "00000000-0000-4000-8000-000000000001\n"} {
		t.Run(body, func(t *testing.T) {
			if _, err := decode([]byte(body)); err == nil {
				t.Fatal("decode accepted invalid task ID body")
			}
		})
	}
}

type countingAcknowledger struct {
	calls    int
	nacks    int
	tag      uint64
	multiple bool
	requeue  bool
	failure  error
}

func (a *countingAcknowledger) Ack(tag uint64, multiple bool) error {
	a.calls++
	a.tag = tag
	a.multiple = multiple
	return a.failure
}

func (a *countingAcknowledger) Nack(tag uint64, multiple, requeue bool) error {
	a.nacks++
	a.tag = tag
	a.multiple = multiple
	a.requeue = requeue
	return a.failure
}
func (*countingAcknowledger) Reject(uint64, bool) error { return nil }

func TestConsumerReceivesOneBatchWithoutAcknowledging(t *testing.T) {
	ack := &countingAcknowledger{}
	id := domain.ID("abcdefab-1234-4abc-8def-abcdefabcdef")
	const prefetch = 4
	deliveries := make(chan amqp091.Delivery, 2*prefetch)
	for tag := 1; tag <= 2*prefetch; tag++ {
		deliveries <- amqp091.Delivery{Acknowledger: ack, DeliveryTag: uint64(tag), Body: []byte(strings.ToUpper(string(id)))}
	}
	close(deliveries)
	c := &Consumer{channel: &amqp091.Channel{}, prefetch: prefetch, closes: make(chan *amqp091.Error), deliveries: deliveries}
	for batchNumber := 1; batchNumber <= 2; batchNumber++ {
		batch, err := c.receive(context.Background())
		if err != nil || len(batch.IDs) != prefetch || batch.DeliveryTag != uint64(batchNumber*prefetch) || ack.calls != 0 || ack.nacks != 0 {
			t.Fatalf("batch=%+v err=%v ack=%+v", batch, err, ack)
		}
		for _, got := range batch.IDs {
			if got != id {
				t.Fatalf("normalized task ID=%q want %q", got, id)
			}
		}
	}
}

func TestBatchRejectsMalformedDelivery(t *testing.T) {
	ack := &countingAcknowledger{}
	batch, err := decodeBatch([]amqp091.Delivery{{Acknowledger: ack, DeliveryTag: 7, Body: []byte("not a UUID")}})
	if err != nil || len(batch.IDs) != 0 || batch.DeliveryTag != 0 || ack.nacks != 1 || ack.calls != 0 || ack.tag != 7 || ack.requeue || ack.multiple {
		t.Fatalf("batch=%+v err=%v ack=%+v", batch, err, ack)
	}
}

func TestBatchKeepsUnknownIDsForRepositoryDecision(t *testing.T) {
	ack := &countingAcknowledger{}
	id := domain.NewID()
	batch, err := decodeBatch([]amqp091.Delivery{{Acknowledger: ack, DeliveryTag: 9, Body: []byte(id)}})
	if err != nil || len(batch.IDs) != 1 || batch.IDs[0] != id || batch.DeliveryTag != 9 || ack.calls != 0 || ack.nacks != 0 {
		t.Fatalf("batch=%+v err=%v ack=%+v", batch, err, ack)
	}
}

func TestBatchUsesHighestValidTag(t *testing.T) {
	ack := &countingAcknowledger{}
	deliveries := []amqp091.Delivery{
		{Acknowledger: ack, DeliveryTag: 9, Body: []byte(domain.NewID())},
		{Acknowledger: ack, DeliveryTag: 7, Redelivered: true, Body: []byte(domain.NewID())},
		{Acknowledger: ack, DeliveryTag: 11, Body: []byte("invalid")},
	}
	batch, err := decodeBatch(deliveries)
	if err != nil || len(batch.IDs) != 2 || batch.DeliveryTag != 9 || ack.calls != 0 || ack.nacks != 1 || ack.tag != 11 || ack.multiple || ack.requeue {
		t.Fatalf("decoded batch=%+v err=%v ack=%+v", batch, err, ack)
	}
}

func TestBatchReturnsNackTransportFailure(t *testing.T) {
	ack := &countingAcknowledger{failure: io.EOF}
	_, err := decodeBatch([]amqp091.Delivery{{Acknowledger: ack, DeliveryTag: 1, Body: []byte("invalid")}})
	if !errors.Is(err, domain.ErrNetwork) || ack.nacks != 1 || ack.calls != 0 {
		t.Fatalf("NACK transport failure: err=%v ack=%+v", err, ack)
	}
}

func TestConsumerAckRequiresInitialization(t *testing.T) {
	c := NewConsumer(nil, "tasks", 1, nil, nil, nil)
	if err := c.ack(0); err != nil {
		t.Fatalf("empty batch ACK=%v", err)
	}
	if err := c.ack(1); err == nil {
		t.Fatal("ACK accepted an uninitialized consumer")
	}
}

func TestConsumerReceivePreservesPermanentCloseReason(t *testing.T) {
	deliveries := make(chan amqp091.Delivery)
	close(deliveries)
	closes := make(chan *amqp091.Error, 1)
	closes <- &amqp091.Error{Code: 406, Reason: "precondition failed"}
	c := &Consumer{channel: &amqp091.Channel{}, closes: closes, deliveries: deliveries}
	_, err := c.receive(context.Background())
	var brokerError *amqp091.Error
	if !errors.As(err, &brokerError) || brokerError.Code != 406 || errors.Is(err, domain.ErrNetwork) {
		t.Fatalf("permanent channel failure=%v", err)
	}
}

func TestConsumerReceiveClosedStreamReturnsNetworkError(t *testing.T) {
	deliveries := make(chan amqp091.Delivery)
	close(deliveries)
	c := &Consumer{channel: &amqp091.Channel{}, closes: make(chan *amqp091.Error), deliveries: deliveries}
	if _, err := c.receive(context.Background()); !errors.Is(err, domain.ErrNetwork) {
		t.Fatalf("closed stream=%v", err)
	}
}

func TestConsumerReceiveRequiresInitialization(t *testing.T) {
	c := NewConsumer(nil, "tasks", 1, nil, nil, nil)
	if _, err := c.receive(context.Background()); err == nil {
		t.Fatal("receive accepted an uninitialized consumer")
	}
}

// retryDelays mirrors the source's default network retry policy.
var retryDelays = []time.Duration{time.Second, 10 * time.Second, time.Minute}

func TestConsumeCommitsOutcomesBeforeAcknowledgment(t *testing.T) {
	for _, acknowledgmentErr := range []error{nil, errors.New("acknowledgment failed")} {
		ctx := context.Background()
		repo := task.NewMockTaskRepository()
		svc := usecase.NewTaskUseCase(repo, acceptedQueue{}, time.Now)
		created, err := svc.Submit(ctx, "test task", []byte("你好🙂"))
		if err != nil {
			t.Fatal(err)
		}
		acknowledged := false
		c := NewConsumer(nil, "tasks", 1, svc, func(ctx context.Context) error { return ctx.Err() }, retryDelays)
		err = c.settle(ctx, deliveryBatch{IDs: []domain.ID{created.ID}, DeliveryTag: 42}, func(tag uint64) error {
			if tag != 42 {
				t.Fatalf("acknowledgment tag=%d", tag)
			}
			stored, err := repo.Get(ctx, created.ID)
			if err != nil || stored.Status != domain.Completed || stored.IsRunning || stored.Result == nil || *stored.Result != "3" || stored.Error != nil {
				t.Fatalf("acknowledgment preceded committed outcome: %+v err=%v", stored, err)
			}
			acknowledged = true
			return acknowledgmentErr
		})
		var processing *ProcessingError
		if !acknowledged || err != acknowledgmentErr || errors.As(err, &processing) {
			t.Fatalf("acknowledgment called=%v err=%v want=%v", acknowledged, err, acknowledgmentErr)
		}
	}
}

func TestConsumeProcessingFailureSkipsAcknowledgment(t *testing.T) {
	ctx := context.Background()
	repo := task.NewMockTaskRepository()
	svc := usecase.NewTaskUseCase(repo, acceptedQueue{}, time.Now)
	created, err := svc.Submit(ctx, "test task", []byte("text"))
	if err != nil {
		t.Fatal(err)
	}
	svc.Timeout = 0 // ProcessTasks rejects the configuration before claiming.
	c := NewConsumer(nil, "tasks", 1, svc, func(ctx context.Context) error { return ctx.Err() }, retryDelays)
	err = c.settle(ctx, deliveryBatch{IDs: []domain.ID{created.ID}, DeliveryTag: 7}, func(uint64) error {
		t.Fatal("processing failure was acknowledged")
		return nil
	})
	var processing *ProcessingError
	if !errors.As(err, &processing) || processing.Unwrap() == nil {
		t.Fatalf("processing failure=%v", err)
	}
	if stored, err := repo.Get(ctx, created.ID); err != nil || stored.Status != domain.Pending || stored.IsRunning {
		t.Fatalf("failed processing changed task: %+v err=%v", stored, err)
	}
	if err := NewConsumer(new(amqp091.Channel), "tasks", 1, nil, nil, nil).Initialization(); err == nil {
		t.Fatal("initialization accepted a missing task use case")
	}
	if err := NewConsumer(new(amqp091.Channel), "tasks", 0, svc, nil, nil).Initialization(); err == nil {
		t.Fatal("initialization accepted a nonpositive prefetch")
	}
}

// acceptedQueue confirms every Submit fast-path publication.
type acceptedQueue struct{}

func (acceptedQueue) Enqueue(_ context.Context, id domain.ID) (usecase.Publication, error) {
	return usecase.Publication{ID: id}, nil
}
