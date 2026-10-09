package usecase_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/bright2227/vsx-task-system/internal/domain"
	"github.com/bright2227/vsx-task-system/internal/usecase"
)

type outboxStub struct {
	mu                                   sync.Mutex
	message                              usecase.Message
	reserveErr, publishedErr, releaseErr error
	reserved, marked, released           int
}

func (o *outboxStub) Reserve(context.Context, int) ([]usecase.Message, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.reserved++
	if o.reserveErr != nil {
		return nil, o.reserveErr
	}
	return []usecase.Message{o.message}, nil
}
func (o *outboxStub) Published(context.Context, usecase.Message) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.marked++
	return o.publishedErr
}
func (o *outboxStub) Release(context.Context, usecase.Message) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.released++
	return o.releaseErr
}

type publisherStub struct {
	err   error
	calls int
}

func (p *publisherStub) Publish(context.Context, usecase.Message) error { p.calls++; return p.err }

func TestRelayReleasesFailedPublishAndMarksConfirmedPublish(t *testing.T) {
	message := usecase.Message{EventID: domain.NewID(), TaskID: domain.NewID()}
	t.Run("publish failure releases lease", func(t *testing.T) {
		want := errors.New("broker unavailable")
		outbox := &outboxStub{message: message}
		publisher := &publisherStub{err: want}
		count, err := (usecase.Relay{Outbox: outbox, Publisher: publisher, Batch: 1}).Once(context.Background())
		if count != 0 || !errors.Is(err, want) || publisher.calls != 1 || outbox.marked != 0 || outbox.released != 1 {
			t.Fatalf("count=%d err=%v publisher=%+v outbox=%+v", count, err, publisher, outbox)
		}
	})
	t.Run("confirmed publish marks outbox", func(t *testing.T) {
		outbox := &outboxStub{message: message}
		publisher := &publisherStub{}
		count, err := (usecase.Relay{Outbox: outbox, Publisher: publisher, Batch: 1}).Once(context.Background())
		if count != 1 || err != nil || publisher.calls != 1 || outbox.marked != 1 || outbox.released != 0 {
			t.Fatalf("count=%d err=%v publisher=%+v outbox=%+v", count, err, publisher, outbox)
		}
	})
}
