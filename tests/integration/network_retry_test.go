//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/bright2227/gogolook-task-system/internal/usecase/port"

	delivery "github.com/bright2227/gogolook-task-system/internal/delivery"
	httpapi "github.com/bright2227/gogolook-task-system/internal/delivery/http"
	"github.com/bright2227/gogolook-task-system/internal/domain"
	"github.com/bright2227/gogolook-task-system/internal/usecase"
)

// claimRecordingRepository records the delivered IDs that ProcessTasks tries
// to claim: the UpdateByIDs call whose locked rows are not running yet.
type claimRecordingRepository struct {
	port.TaskRepository
	record func([]domain.ID)
}

func (r *claimRecordingRepository) UpdateByIDs(ctx context.Context, ids []domain.ID, update func([]*domain.Task) (bool, error)) error {
	claim := true
	return r.TaskRepository.UpdateByIDs(ctx, ids, func(items []*domain.Task) (bool, error) {
		for _, item := range items {
			claim = claim && !item.IsRunning
		}
		if claim {
			r.record(ids)
		}
		return update(items)
	})
}

func TestNetworkFailuresRetryThenFailWithoutDLQ(t *testing.T) {
	s := newIntegrationStack(t)
	ctx := s.ctx
	client := s.client(t, "integration.network-retry")
	svc := newPublishingTaskUseCase(s, client, 16)
	svc.Timeout = time.Second
	retryDelays := []time.Duration{30 * time.Millisecond, 60 * time.Millisecond, 90 * time.Millisecond}
	created := submitTask(t, ctx, svc, `{"job":"network-retry"}`)
	if !created.IsSubmitted {
		t.Fatalf("initial publication=%+v", created)
	}

	networkErr := &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	var calls atomic.Int32
	var mu sync.Mutex
	var deliveries []domain.ID
	var execute usecase.Work = func(context.Context) error {
		calls.Add(1)
		return delivery.NetworkError(networkErr)
	}
	recorded := &claimRecordingRepository{TaskRepository: s.repo, record: func(ids []domain.ID) {
		mu.Lock()
		deliveries = append(deliveries, ids...)
		mu.Unlock()
	}}
	worker := usecase.NewTaskUseCase(recorded, nil, time.Now)
	worker.Timeout = svc.Timeout
	consumer := startBatchConsumer(t, ctx, client, worker, execute, retryDelays, nil)
	publisherCtx, stopPublisher := context.WithCancel(ctx)
	publisherDone := make(chan struct{})
	go func() {
		defer close(publisherDone)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-publisherCtx.Done():
				return
			case <-ticker.C:
				_, _ = newPublishingTaskUseCase(s, client, 16).PublishPending(publisherCtx)
			}
		}
	}()
	state := waitForStatus(t, ctx, svc, created.ID, domain.Completed)
	stopPublisher()
	<-publisherDone
	if err := consumer.Stop(); err != nil {
		t.Fatalf("consumer stop: %v", err)
	}
	if state.RetryCount != 3 || state.Error == nil {
		t.Fatalf("network failure terminal state=%+v", state)
	}
	if calls.Load() != 4 {
		t.Fatalf("executor calls=%d, want 4", calls.Load())
	}
	mu.Lock()
	if len(deliveries) != 4 {
		mu.Unlock()
		t.Fatalf("broker deliveries=%d, want 4", len(deliveries))
	}
	for _, id := range deliveries {
		if id != created.ID {
			mu.Unlock()
			t.Fatalf("broker delivery ID=%s, want task ID=%s", id, created.ID)
		}
	}
	mu.Unlock()
	if !strings.Contains(*state.Error, "connection refused") {
		t.Fatalf("stored error lost network details: %q", *state.Error)
	}
	var storedError string
	if err := s.db.QueryRowContext(ctx, `SELECT error FROM tasks WHERE id=$1`, created.ID).Scan(&storedError); err != nil {
		t.Fatal(err)
	}
	if storedError != *state.Error {
		t.Fatalf("database error=%q task error=%q", storedError, *state.Error)
	}
	assertQueueCount(t, ctx, s, s.queue("integration.network-retry.tasks"), 0)

	// The task response returns the stored string error, as in VSX.
	server := httptest.NewServer(httpapi.NewServer(svc, func(ctx context.Context) error { return s.db.PingContext(ctx) }))
	defer server.Close()
	resp, err := http.Get(server.URL + "/tasks/" + string(created.ID))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var dto map[string]any
	if err = json.NewDecoder(resp.Body).Decode(&dto); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || dto["error"] != storedError {
		t.Fatalf("task DTO status=%d error=%#v", resp.StatusCode, dto["error"])
	}
}

func TestPermissionAndInputFailuresAreTerminal(t *testing.T) {
	s := newIntegrationStack(t)
	ctx := s.ctx
	var syntaxErr error
	var decoded map[string]any
	if err := json.Unmarshal([]byte(`{"broken":`), &decoded); err != nil {
		syntaxErr = err
	} else {
		t.Fatal("invalid JSON unexpectedly decoded")
	}
	wrappedPermission := delivery.NetworkError(&net.OpError{Op: "dial", Net: "tcp", Err: os.ErrPermission})
	cases := []struct {
		name string
		err  error
	}{
		{name: "permission", err: errors.Join(errors.New("credential access"), os.ErrPermission)},
		{name: "wrapped network permission", err: wrappedPermission},
		{name: "JSON syntax", err: syntaxErr},
		{name: "bad input", err: errors.New("upstream rejected the supplied account")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc := usecase.NewTaskUseCase(s.repo, sweeperOnly, time.Now)
			svc.Timeout = time.Second
			retryDelays := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 30 * time.Millisecond}
			created := submitTask(t, ctx, svc, `{"case":"`+tc.name+`"}`)
			var calls atomic.Int32
			var execute usecase.Work = func(context.Context) error {
				calls.Add(1)
				return tc.err
			}
			if err := svc.ProcessTasks(ctx, []domain.ID{created.ID}, execute, retryDelays); err != nil {
				t.Fatal(err)
			}
			state, err := svc.Get(ctx, string(created.ID))
			if err != nil {
				t.Fatal(err)
			}
			if state.Status != domain.Completed || state.RetryCount != 0 || state.Error == nil {
				t.Fatalf("terminal state=%+v", state)
			}
			if calls.Load() != 1 {
				t.Fatalf("executor calls=%d, want 1", calls.Load())
			}
			if strings.TrimSpace(*state.Error) == "" || *state.Error == "task execution failed" {
				t.Fatalf("task error lacks private detail: %q", *state.Error)
			}
			var persisted string
			if err := s.db.QueryRowContext(ctx, `SELECT error FROM tasks WHERE id=$1`, state.ID).Scan(&persisted); err != nil {
				t.Fatal(err)
			}
			if persisted != *state.Error {
				t.Fatalf("stored error=%q task error=%q", persisted, *state.Error)
			}
		})
	}
}
