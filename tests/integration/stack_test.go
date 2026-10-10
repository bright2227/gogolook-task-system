//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	httpapi "github.com/bright2227/gogolook-task-system/internal/delivery/http"
	"github.com/bright2227/gogolook-task-system/internal/domain"
	"github.com/bright2227/gogolook-task-system/internal/usecase"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestHTTPAndBatchProcessing(t *testing.T) {
	s := newIntegrationStack(t)
	client := s.client(t, "integration.source")
	svc := newPublishingTaskUseCase(s, client, 8)
	execute := usecase.Delay(time.Millisecond)
	server := httptest.NewServer(httpapi.NewServer(svc, func(ctx context.Context) error { return s.db.PingContext(ctx) }))
	defer server.Close()
	var ids []domain.ID
	for _, text := range []string{"", "plain non-JSON", "你好🙂", strings.Repeat("a", 1024)} {
		encoded, _ := json.Marshal(map[string]string{"name": "source task", "payload": text})
		resp, err := http.Post(server.URL+"/tasks", "application/json", strings.NewReader(string(encoded)))
		if err != nil {
			t.Fatal(err)
		}
		var dto struct {
			ID          domain.ID `json:"id"`
			Payload     string    `json:"payload"`
			Name        string    `json:"name"`
			Version     int64     `json:"version"`
			IsRunning   bool      `json:"is_running"`
			Status      int       `json:"status"`
			IsSubmitted bool      `json:"is_submitted"`
			Created     time.Time `json:"created_time"`
			Updated     time.Time `json:"updated_time"`
		}
		err = json.NewDecoder(resp.Body).Decode(&dto)
		resp.Body.Close()
		if err != nil || resp.StatusCode != 200 || dto.Name != "source task" || dto.Version != 1 || dto.IsRunning || dto.Payload != text || dto.Status != 0 || !dto.IsSubmitted || dto.Created.IsZero() || dto.Updated.IsZero() {
			t.Fatalf("source response=%+v status=%d err=%v", dto, resp.StatusCode, err)
		}
		ids = append(ids, dto.ID)
	}
	// POST published each task; the sweeper has nothing left to publish.
	assertQueueCount(t, s.ctx, s, s.queue("integration.source.tasks"), len(ids))
	if count, err := svc.PublishPending(s.ctx); err != nil || count != 0 {
		t.Fatalf("publisher batch=%d,%v", count, err)
	}
	for _, id := range ids {
		state, err := svc.Get(s.ctx, string(id))
		if err != nil || !state.IsSubmitted {
			t.Fatalf("publication marker=%+v,%v", state, err)
		}
	}
	// Source exchange and content contract also allow an original Python producer.
	conn, err := amqp.Dial(s.brokerURL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	if err = ch.ExchangeDeclarePassive("direct", "direct", true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	var deliveries int
	for deliveries < 4 {
		d, ok, err := ch.Get(s.queue("integration.source.tasks"), false)
		if err != nil || !ok {
			t.Fatalf("source message missing: %v", err)
		}
		_, idErr := domain.ParseID(string(d.Body))
		if d.ContentType != "text/plain" || idErr != nil {
			t.Fatalf("message contract: type=%s body=%s", d.ContentType, d.Body)
		}
		if err = d.Nack(false, true); err != nil {
			t.Fatal(err)
		}
		deliveries++
	}
	// Requeued messages may be returned again, so inspect identity without
	// assuming queue order after NACK: all work still executes from current rows.
	consumer := startBatchConsumer(t, s.ctx, client, svc, execute, retryDelays, nil)
	results := []string{"0", "14", "3", "1024"}
	for i, id := range ids {
		state := waitForStatus(t, s.ctx, svc, id, domain.Completed)
		if state.Result == nil || *state.Result != results[i] || state.Error != nil {
			t.Fatalf("completion=%+v", state)
		}
		response, err := http.Get(server.URL + "/tasks/" + string(id))
		if err != nil {
			t.Fatal(err)
		}
		var dto struct {
			Status      *int    `json:"status"`
			IsSubmitted *bool   `json:"is_submitted"`
			Result      *string `json:"result"`
			Error       *string `json:"error"`
		}
		err = json.NewDecoder(response.Body).Decode(&dto)
		_ = response.Body.Close()
		if err != nil || response.StatusCode != http.StatusOK || dto.Status == nil || *dto.Status != 1 || dto.IsSubmitted == nil || !*dto.IsSubmitted || dto.Result == nil || *dto.Result != results[i] || dto.Error != nil {
			t.Fatalf("completed HTTP task=%+v status=%d err=%v", dto, response.StatusCode, err)
		}
	}
	if err := consumer.Stop(); err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodDelete, server.URL+"/tasks/"+string(ids[0]), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("completed deletion=%d", resp.StatusCode)
	}
	if _, err := svc.Get(s.ctx, string(ids[0])); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("deleted task remains accessible: %v", err)
	}
}

// A running row is skipped by every delivery: the batch executes only the
// pending task, and running rows reject edits and deletes.
func TestBatchRunningGuardsAndSkipsRunningClaims(t *testing.T) {
	s := newPostgresStack(t, true)
	svc := usecase.NewTaskUseCase(s.repo, sweeperOnly, time.Now)
	first := submitTask(t, s.ctx, svc, "first")
	second := submitTask(t, s.ctx, svc, "second")
	// Another consumer holds first; this batch must leave its claim alone.
	if err := s.repo.UpdateByID(s.ctx, first.ID, first.Version, func(t *domain.Task) (bool, error) { return t.Start(time.Now()) }); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	releaseWork := sync.OnceFunc(func() { close(release) })
	defer releaseWork()
	var execute usecase.Work = func(ctx context.Context) error {
		started <- struct{}{}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	done := make(chan error, 1)
	go func() {
		done <- svc.ProcessTasks(s.ctx, []domain.ID{first.ID, second.ID, domain.NewID()}, execute, retryDelays)
	}()
	for i := 0; i < 1; i++ {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("batch did not start")
		}
	}
	// Version 0 (no If-Match) locks the current row without comparing versions;
	// the running flag must still reject the mutation.
	for _, item := range []*domain.Task{first, second} {
		if _, err := svc.Update(s.ctx, string(item.ID), "edited", []byte("edited"), 0); !errors.Is(err, domain.ErrBusy) {
			t.Fatalf("running edit was accepted: %v", err)
		}
		if err := svc.Delete(s.ctx, string(item.ID), 0); !errors.Is(err, domain.ErrBusy) {
			t.Fatalf("running delete was accepted: %v", err)
		}
	}
	releaseWork()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	a, _ := s.repo.Get(s.ctx, first.ID)
	b, _ := s.repo.Get(s.ctx, second.ID)
	if a.Status != domain.Processing || !a.IsRunning || a.Version != first.Version+1 || a.RetryCount != 0 || b.Status != domain.Completed || b.IsRunning || b.Result == nil || *b.Result != "6" {
		t.Fatalf("states=%+v/%+v", a, b)
	}
	if _, err := svc.Update(s.ctx, string(first.ID), "edited", []byte("edited"), 0); !errors.Is(err, domain.ErrBusy) {
		t.Fatalf("held claim edit=%v", err)
	}
	// The background claim and outcome advanced Version: a stale If-Match
	// conflicts, while no If-Match edits or deletes the current row.
	if _, err := svc.Update(s.ctx, string(second.ID), "edited", []byte("edited"), second.Version); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("stale If-Match edit=%v", err)
	}
	if edited, err := svc.Update(s.ctx, string(second.ID), "edited", []byte("edited"), 0); err != nil || edited.Name != "edited" || edited.Version != b.Version+1 || edited.Status != domain.Pending {
		t.Fatalf("edit without If-Match=%+v,%v", edited, err)
	}
	if err := svc.Delete(s.ctx, string(second.ID), second.Version); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("stale If-Match delete=%v", err)
	}
	if err := svc.Delete(s.ctx, string(second.ID), 0); err != nil {
		t.Fatalf("delete without If-Match=%v", err)
	}
	// Batch callback failure cannot persist a partial transition.
	third := submitTask(t, s.ctx, svc, "third")
	fourth := submitTask(t, s.ctx, svc, "fourth")
	err := s.repo.UpdateByIDs(s.ctx, []domain.ID{third.ID, fourth.ID}, func(items []*domain.Task) (bool, error) {
		for _, item := range items {
			_, _ = item.Start(time.Now())
		}
		return true, errors.New("rollback")
	})
	if err == nil {
		t.Fatal("expected rollback error")
	}
	for _, id := range []domain.ID{third.ID, fourth.ID} {
		state, _ := s.repo.Get(s.ctx, id)
		if state.Status != domain.Pending || state.IsRunning {
			t.Fatalf("partial batch persisted=%+v", state)
		}
	}
}

func TestUnsubmittedRecoveryHandlesTimestampTies(t *testing.T) {
	s := newIntegrationStack(t)
	client := s.client(t, "integration.recovery")
	now := time.Now().UTC()
	svc := usecase.NewTaskUseCase(s.repo, sweeperOnly, func() time.Time { return now })
	var ids []domain.ID
	for i := 0; i < 5; i++ {
		created := submitTask(t, s.ctx, svc, "recovery")
		ids = append(ids, created.ID)
	}
	publication := newPublishingTaskUseCase(s, client, 2)
	publication.Clock = func() time.Time { return now.Add(publication.PublishGrace) }
	total := 0
	for i := 0; i < 4; i++ {
		n, err := publication.PublishPending(s.ctx)
		if err != nil {
			t.Fatal(err)
		}
		total += n
	}
	if total != 5 {
		t.Fatalf("recovered tied timestamps=%d", total)
	}
	for _, id := range ids {
		state, _ := s.repo.Get(s.ctx, id)
		if !state.IsSubmitted {
			t.Fatalf("submitted=%v for %s", state.IsSubmitted, id)
		}
	}
	var execute usecase.Work = func(context.Context) error { return nil }
	consumer := startBatchConsumer(t, s.ctx, client, svc, execute, retryDelays, nil)
	for _, id := range ids {
		waitForStatus(t, s.ctx, svc, id, domain.Completed)
	}
	if err := consumer.Stop(); err != nil {
		t.Fatal(err)
	}
}

// A task whose claim keeps going stale spends one network retry per release
// and completes as abandoned once the retry budget is exhausted. Only claims
// not updated for staleAfter by the database clock are released, whatever the
// caller's clock says. Rows are released by domain.Task.Interrupt in Go.
func TestReleaseStaleAbandonsCrashLoopingTask(t *testing.T) {
	s := newPostgresStack(t, true)
	svc := usecase.NewTaskUseCase(s.repo, sweeperOnly, time.Now)
	created := submitTask(t, s.ctx, svc, "poison")
	delays := []time.Duration{0, 0}
	for attempt := 1; attempt <= len(delays)+1; attempt++ {
		current, err := s.repo.Get(s.ctx, created.ID)
		if err != nil {
			t.Fatal(err)
		}
		// Stamp the claim one hour behind the database clock.
		if err := s.repo.UpdateByID(s.ctx, created.ID, current.Version, func(t *domain.Task) (bool, error) { return t.Start(t.NextRetry) }); err != nil {
			t.Fatal(err)
		}
		if _, err := s.db.ExecContext(s.ctx, `UPDATE tasks SET created_at=LEAST(created_at,now()-interval '1 hour'),updated_at=now()-interval '1 hour' WHERE id=$1`, created.ID); err != nil {
			t.Fatal(err)
		}
		claimed, err := s.repo.Get(s.ctx, created.ID)
		if err != nil {
			t.Fatal(err)
		}
		if n, err := s.repo.ReleaseStale(s.ctx, time.Hour+time.Minute, delays); n != 0 || err != nil {
			t.Fatalf("fresh claim %d released=%d,%v", attempt, n, err)
		}
		if n, err := s.repo.ReleaseStale(s.ctx, 59*time.Minute+30*time.Second, delays); n != 1 || err != nil {
			t.Fatalf("recover %d=%d,%v", attempt, n, err)
		}
		state, err := s.repo.Get(s.ctx, created.ID)
		if err != nil || state.IsRunning || state.RetryCount != min(attempt, len(delays)) || state.Version != current.Version+2 || !state.UpdatedAt.After(claimed.UpdatedAt.Add(59*time.Minute)) {
			t.Fatalf("recover %d=%+v,%v", attempt, state, err)
		}
		if attempt <= len(delays) && (state.Status != domain.Pending || state.IsSubmitted || state.Error != nil || !state.NextRetry.Equal(state.UpdatedAt)) {
			t.Fatalf("recover %d did not return to pending=%+v", attempt, state)
		}
		if attempt > len(delays) && (!state.Finished() || !state.IsSubmitted || state.Result != nil || state.Error == nil || *state.Error != "abandoned after 3 attempts; the last execution was interrupted") {
			t.Fatalf("recover %d did not abandon=%+v", attempt, state)
		}
	}
	var status int
	if err := s.db.QueryRowContext(s.ctx, `SELECT status FROM tasks WHERE id=$1`, created.ID).Scan(&status); err != nil || status != 1 {
		t.Fatalf("stored status=%d,%v", status, err)
	}
}

// ReleaseStale skips a stale row locked by a concurrent transaction (e.g. an
// outcome commit in flight) and releases it on a later call.
func TestReleaseStaleSkipsLockedRows(t *testing.T) {
	s := newPostgresStack(t, true)
	svc := usecase.NewTaskUseCase(s.repo, sweeperOnly, time.Now)
	created := submitTask(t, s.ctx, svc, "locked")
	if err := s.repo.UpdateByID(s.ctx, created.ID, created.Version, func(t *domain.Task) (bool, error) { return t.Start(time.Now()) }); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(s.ctx, `UPDATE tasks SET created_at=now()-interval '1 hour',updated_at=now()-interval '1 hour' WHERE id=$1`, created.ID); err != nil {
		t.Fatal(err)
	}
	tx, err := s.db.BeginTx(s.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(s.ctx, `SELECT 1 FROM tasks WHERE id=$1 FOR UPDATE`, created.ID); err != nil {
		t.Fatal(err)
	}
	if n, err := s.repo.ReleaseStale(s.ctx, staleAfter, retryDelays); n != 0 || err != nil {
		t.Fatalf("locked row released=%d,%v", n, err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if n, err := s.repo.ReleaseStale(s.ctx, staleAfter, retryDelays); n != 1 || err != nil {
		t.Fatalf("unlocked row=%d,%v", n, err)
	}
}

// Graceful shutdown commits under a detached context: finished work is kept
// and the interrupted claim returns to pending without spending any budget.
func TestShutdownReleasesInterruptedClaimWithoutSpendingBudget(t *testing.T) {
	s := newPostgresStack(t, true)
	svc := usecase.NewTaskUseCase(s.repo, sweeperOnly, time.Now)
	svc.Parallelism = 2
	first := submitTask(t, s.ctx, svc, "first")
	second := submitTask(t, s.ctx, svc, "second")
	var calls atomic.Int32
	started := make(chan struct{})
	var execute usecase.Work = func(ctx context.Context) error {
		if calls.Add(1) == 1 {
			return nil
		}
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}
	ctx, cancel := context.WithCancel(s.ctx)
	done := make(chan error, 1)
	go func() { done <- svc.ProcessTasks(ctx, []domain.ID{first.ID, second.ID}, execute, retryDelays) }()
	<-started
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown=%v", err)
	}
	completed, released := 0, 0
	for _, id := range []domain.ID{first.ID, second.ID} {
		state, err := s.repo.Get(s.ctx, id)
		switch {
		case err != nil:
			t.Fatal(err)
		case state.Status == domain.Completed && state.Result != nil:
			completed++
		case state.Status == domain.Pending && !state.IsRunning && !state.IsSubmitted && state.RetryCount == 0 && state.Error == nil && state.Version == 3:
			released++
		default:
			t.Fatalf("shutdown state=%+v", state)
		}
	}
	if completed != 1 || released != 1 {
		t.Fatalf("completed=%d released=%d", completed, released)
	}
	if n, err := s.repo.ReleaseStale(s.ctx, time.Microsecond, retryDelays); n != 0 || err != nil {
		t.Fatalf("released claims left for recovery=%d,%v", n, err)
	}
}
