package rabbitmq

import (
	"context"
	"errors"
	"testing"

	"github.com/bright2227/gogolook-task-system/internal/domain"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestTaskQueueRequiresInitializationAndHonorsCanceledContext(t *testing.T) {
	taskQueue := NewTaskQueue(nil, "tasks")
	if err := taskQueue.Initialization(); err == nil {
		t.Fatal("initialization accepted a missing connection")
	}
	if publication, err := taskQueue.Enqueue(context.Background(), domain.NewID()); err == nil || publication.ID != "" || publication.Err != nil {
		t.Fatal("publication accepted an uninitialized task queue")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if publication, err := taskQueue.Enqueue(ctx, domain.NewID()); !errors.Is(err, context.Canceled) || publication.ID != "" || publication.Err != nil {
		t.Fatalf("canceled publication=%+v, err=%v", publication, err)
	}
	// Invalid IDs are local failures and do not need an AMQP operation.
	taskQueue.channel = new(amqp.Channel)
	taskQueue.closes = make(chan *amqp.Error)
	if publication, err := taskQueue.Enqueue(context.Background(), "invalid"); err != nil || publication.ID != "invalid" || publication.Err == nil {
		t.Fatalf("invalid ID publication=%+v, err=%v", publication, err)
	}
}

func TestTaskIDBodyValidatesAndEncodesPlainText(t *testing.T) {
	id := domain.NewID()
	gotID, body, err := taskIDBody(id)
	if err != nil {
		t.Fatal(err)
	}
	if gotID != id || string(body) != string(id) {
		t.Fatalf("task body = %q, id = %q; want plain UUID %q", body, gotID, id)
	}
	if _, _, err = taskIDBody("invalid"); err == nil {
		t.Fatal("accepted invalid task ID for publication")
	}
}
