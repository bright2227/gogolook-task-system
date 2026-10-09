package usecase

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

type Relay struct {
	Outbox    Outbox
	Publisher Publisher
	Batch     int
}

func (r Relay) Once(ctx context.Context) (int, error) {
	if r.Batch < 1 {
		return 0, errors.New("relay batch must be positive")
	}
	msgs, err := r.Outbox.Reserve(ctx, r.Batch)
	if err != nil {
		return 0, err
	}
	var count atomic.Int64
	var wg sync.WaitGroup
	var mu sync.Mutex
	var failures []error
	for _, m := range msgs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := r.Publisher.Publish(ctx, m)
			if err == nil {
				err = r.Outbox.Published(ctx, m)
			}
			if err == nil {
				count.Add(1)
				return
			}
			// If marking failed after a successful publish, a duplicate is safe.
			err = errors.Join(err, r.Outbox.Release(ctx, m))
			mu.Lock()
			failures = append(failures, err)
			mu.Unlock()
		}()
	}
	wg.Wait()
	return int(count.Load()), errors.Join(failures...)
}
