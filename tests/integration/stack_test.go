//go:build integration

package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bright2227/vsx-task-system/internal/delivery/http"
	"github.com/bright2227/vsx-task-system/internal/delivery/rabbitmq"
	"github.com/bright2227/vsx-task-system/internal/domain"
	processorapp "github.com/bright2227/vsx-task-system/internal/infrastructure/processor"
	"github.com/bright2227/vsx-task-system/internal/repository/task/postgres"
	"github.com/bright2227/vsx-task-system/internal/usecase"
	_ "github.com/lib/pq"
	"github.com/rabbitmq/amqp091-go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestPostgresAndRabbitMQ(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	pg, err := testcontainers.Run(ctx, "postgres:16-alpine",
		testcontainers.WithEnv(map[string]string{"POSTGRES_USER": "task", "POSTGRES_PASSWORD": "task", "POSTGRES_DB": "tasks"}),
		testcontainers.WithExposedPorts("5432/tcp"), testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(time.Minute), wait.ForListeningPort("5432/tcp")))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(pg) })
	pgHost, err := pg.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	pgPort, err := pg.MappedPort(ctx, "5432/tcp")
	if err != nil {
		t.Fatal(err)
	}
	databaseURL := fmt.Sprintf("postgres://task:task@%s:%s/tasks?sslmode=disable", pgHost, pgPort.Port())
	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err = db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	migration, err := os.ReadFile(filepath.Join("..", "..", "internal", "migrations", "001_initial.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, string(migration)); err != nil {
		t.Fatal(err)
	}
	var tables int
	if err = db.QueryRowContext(ctx, `SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_name IN ('tasks','task_events','outbox')`).Scan(&tables); err != nil || tables != 3 {
		t.Fatalf("migration tables=%d err=%v", tables, err)
	}
	pgStore := &postgres.Store{DB: db}

	rabbit, err := testcontainers.Run(ctx, "rabbitmq:3.13-alpine",
		testcontainers.WithEnv(map[string]string{"RABBITMQ_DEFAULT_USER": "task", "RABBITMQ_DEFAULT_PASS": "task"}),
		testcontainers.WithExposedPorts("5672/tcp"), testcontainers.WithWaitStrategy(wait.ForListeningPort("5672/tcp").WithStartupTimeout(time.Minute)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(rabbit) })
	host, err := rabbit.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := rabbit.MappedPort(ctx, "5672/tcp")
	if err != nil {
		t.Fatal(err)
	}
	// Exercise the production transaction and outbox publisher against both services.
	client := rabbitmq.New(rabbitmq.Config{URL: fmt.Sprintf("amqp://task:task@%s:%s/", host, port.Port()), Queue: "integration.tasks", RetryQueue: "integration.retry", DeadQueue: "integration.dead", RetryDelay: 100 * time.Millisecond})
	t.Cleanup(func() { _ = client.Close() })
	payload, _ := domain.NewPayload([]byte(`{"job":"integration"}`))
	created, err := (usecase.Tasks{Store: pgStore, Clock: time.Now}).Submit(ctx, usecase.SubmitTaskRequest{Payload: payload.JSON()})
	if err != nil {
		t.Fatal(err)
	}
	count, err := (usecase.Relay{Outbox: pgStore, Publisher: client, Batch: 16}).Once(ctx)
	if err != nil || count != 1 {
		t.Fatalf("relay published=%d err=%v", count, err)
	}
	var eventCount, outboxCount int
	if err = db.QueryRowContext(ctx, `SELECT count(*) FROM task_events WHERE task_id=$1`, created.ID).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRowContext(ctx, `SELECT count(*) FROM outbox WHERE task_id=$1 AND published_at IS NOT NULL`, created.ID).Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}
	if eventCount != 1 || outboxCount != 1 {
		t.Fatalf("transactional submit events=%d published outbox=%d", eventCount, outboxCount)
	}
	consumer := usecase.Consumer{Store: pgStore, Clock: time.Now, Lease: 10 * time.Second, Timeout: 5 * time.Second, Processor: processorapp.SimulatedProcessor{}}
	workStarted := time.Now()
	workPhase, stopWorkPhase := context.WithTimeout(ctx, 30*time.Second)
	defer stopWorkPhase()
	worker := startConsumer(t, ctx, client, 2, func(c context.Context, id domain.ID) error {
		return consumer.Process(c, usecase.ProcessTaskRequest{ID: string(id)})
	})
	deadline := time.NewTicker(20 * time.Millisecond)
	defer deadline.Stop()
	for {
		state, getErr := (usecase.Tasks{Store: pgStore, Clock: time.Now}).Get(workPhase, usecase.TaskRequest{ID: string(created.ID)})
		if getErr != nil {
			t.Fatal(getErr)
		}
		if state.Status == string(domain.Completed) {
			if elapsed := time.Since(workStarted); elapsed < 3*time.Second {
				t.Fatalf("sample work completed too early: %s", elapsed)
			}
			if state.Result == nil || *state.Result != "processed" {
				t.Fatalf("sample work result=%+v", state)
			}
			break
		}
		select {
		case <-deadline.C:
		case <-workPhase.Done():
			t.Fatal(workPhase.Err())
		case <-worker.done:
			t.Fatalf("worker exited before completing: %v", worker.err)
		}
	}
	if err = worker.Stop(); err != nil {
		t.Fatalf("worker stop: %v", err)
	}
	// Two queued jobs must both enter processing before either processor is released.
	concurrentIDs := make([]domain.ID, 2)
	for i := range concurrentIDs {
		p, _ := domain.NewPayload([]byte(`{"job":"parallel"}`))
		created, submitErr := (usecase.Tasks{Store: pgStore, Clock: time.Now}).Submit(ctx, usecase.SubmitTaskRequest{Payload: p.JSON()})
		if submitErr != nil {
			t.Fatal(submitErr)
		}
		concurrentIDs[i] = domain.ID(created.ID)
	}
	count, err = (usecase.Relay{Outbox: pgStore, Publisher: client, Batch: 16}).Once(ctx)
	if err != nil || count != 2 {
		t.Fatalf("relay parallel jobs=%d err=%v", count, err)
	}
	barrier := &barrierProcessor{started: make(chan struct{}, 2), release: make(chan struct{})}
	parallelPhase, stopParallelPhase := context.WithTimeout(ctx, 30*time.Second)
	defer stopParallelPhase()
	parallelConsumer := usecase.Consumer{Store: pgStore, Clock: time.Now, Lease: 10 * time.Second, Timeout: 5 * time.Second, Processor: barrier}
	parallel := startConsumer(t, ctx, client, 2, func(c context.Context, id domain.ID) error {
		return parallelConsumer.Process(c, usecase.ProcessTaskRequest{ID: string(id)})
	})
	for i := 0; i < 2; i++ {
		select {
		case <-barrier.started:
		case <-parallelPhase.Done():
			close(barrier.release)
			t.Fatal(parallelPhase.Err())
		case <-parallel.done:
			close(barrier.release)
			t.Fatalf("parallel worker exited: %v", parallel.err)
		}
	}
	close(barrier.release)
	for _, id := range concurrentIDs {
		for {
			state, getErr := (usecase.Tasks{Store: pgStore, Clock: time.Now}).Get(parallelPhase, usecase.TaskRequest{ID: string(id)})
			if getErr != nil {
				t.Fatal(getErr)
			}
			if state.Status == string(domain.Completed) {
				break
			}
			select {
			case <-time.After(20 * time.Millisecond):
			case <-parallelPhase.Done():
				t.Fatal(parallelPhase.Err())
			}
		}
	}
	if err = parallel.Stop(); err != nil {
		t.Fatalf("parallel worker stop: %v", err)
	}
	// A broker-level duplicate delivery after completion remains a no-op.
	if err = client.Publish(ctx, usecase.Message{EventID: domain.NewID(), TaskID: domain.ID(created.ID)}); err != nil {
		t.Fatal(err)
	}
	duplicateSeen := make(chan struct{}, 1)
	duplicateWorker := startConsumer(t, ctx, client, 1, func(c context.Context, id domain.ID) error {
		err := consumer.Process(c, usecase.ProcessTaskRequest{ID: string(id)})
		duplicateSeen <- struct{}{}
		return err
	})
	select {
	case <-duplicateSeen:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case <-duplicateWorker.done:
		t.Fatalf("duplicate consumer exited: %v", duplicateWorker.err)
	}
	if err = duplicateWorker.Stop(); err != nil {
		t.Fatalf("duplicate consumer stop: %v", err)
	}
	state, err := (usecase.Tasks{Store: pgStore, Clock: time.Now}).Get(ctx, usecase.TaskRequest{ID: string(created.ID)})
	if err != nil || state.Status != string(domain.Completed) || state.Attempts != 1 || state.Result == nil || *state.Result != "processed" {
		t.Fatalf("worker result=%+v err=%v", state, err)
	}
	if err = db.QueryRowContext(ctx, `SELECT count(*) FROM task_events WHERE task_id=$1`, created.ID).Scan(&eventCount); err != nil || eventCount != 3 {
		t.Fatalf("event history=%d err=%v", eventCount, err)
	}

	rollbackPayload, _ := domain.NewPayload([]byte(`{"job":"rollback"}`))
	rollbackTask, _ := domain.Submit(rollbackPayload, time.Now())
	wantFailure := errors.New("force rollback")
	if err = pgStore.Within(ctx, func(r usecase.Repository) error {
		if e := r.Insert(ctx, rollbackTask); e != nil {
			return e
		}
		return wantFailure
	}); !errors.Is(err, wantFailure) {
		t.Fatalf("rollback callback error=%v", err)
	}
	var rolledBack int
	if err = db.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM tasks WHERE id=$1)+(SELECT count(*) FROM task_events WHERE task_id=$1)+(SELECT count(*) FROM outbox WHERE task_id=$1)`, rollbackTask.Snapshot().ID).Scan(&rolledBack); err != nil || rolledBack != 0 {
		t.Fatalf("rollback left %d rows: %v", rolledBack, err)
	}

	cancelPayload, _ := domain.NewPayload([]byte(`{}`))
	cancelTask, err := (usecase.Tasks{Store: pgStore, Clock: time.Now}).Submit(ctx, usecase.SubmitTaskRequest{Payload: cancelPayload.JSON()})
	if err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	cancelConsumer := usecase.Consumer{Store: pgStore, Clock: time.Now, Lease: 10 * time.Second, Timeout: 5 * time.Second, Processor: blockingProcessor{started, release}}
	procDone := make(chan error, 1)
	go func() { procDone <- cancelConsumer.Process(ctx, usecase.ProcessTaskRequest{ID: string(cancelTask.ID)}) }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, err = (usecase.Tasks{Store: pgStore, Clock: time.Now}).Cancel(ctx, usecase.TaskRequest{ID: string(cancelTask.ID)}); err != nil {
		unblock()
		t.Fatal(err)
	}
	unblock()
	if err = <-procDone; err != nil {
		t.Fatal(err)
	}
	canceled, err := (usecase.Tasks{Store: pgStore, Clock: time.Now}).Get(ctx, usecase.TaskRequest{ID: string(cancelTask.ID)})
	if err != nil || canceled.Status != string(domain.Canceled) {
		t.Fatalf("cancellation state=%+v err=%v", canceled, err)
	}

	leaseTask, err := (usecase.Tasks{Store: pgStore, Clock: time.Now}).Submit(ctx, usecase.SubmitTaskRequest{Payload: cancelPayload.JSON()})
	if err != nil {
		t.Fatal(err)
	}
	firstStarted, firstRelease := make(chan struct{}), make(chan struct{})
	var firstReleaseOnce sync.Once
	allowStaleFinish := func() { firstReleaseOnce.Do(func() { close(firstRelease) }) }
	t.Cleanup(allowStaleFinish)
	var executions int
	var execMu sync.Mutex
	baseTime := time.Now().UTC()
	var clockCalls atomic.Int64
	leaseClock := func() time.Time {
		n := clockCalls.Add(1)
		if n == 1 {
			return baseTime
		}
		return baseTime.Add(60*time.Millisecond + time.Duration(n-1)*time.Millisecond)
	}
	leaseProcessor := integrationProcessorFunc(func(context.Context, domain.Payload) (string, error) {
		execMu.Lock()
		executions++
		n := executions
		execMu.Unlock()
		if n == 1 {
			close(firstStarted)
			<-firstRelease
			return "stale", nil
		}
		return "new owner", nil
	})
	firstConsumer := usecase.Consumer{Store: pgStore, Clock: leaseClock, Lease: 60 * time.Millisecond, Timeout: time.Second, Processor: leaseProcessor}
	firstDone := make(chan error, 1)
	go func() { firstDone <- firstConsumer.Process(ctx, usecase.ProcessTaskRequest{ID: string(leaseTask.ID)}) }()
	select {
	case <-firstStarted:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	secondConsumer := usecase.Consumer{Store: pgStore, Clock: leaseClock, Lease: time.Second, Timeout: time.Second, Processor: leaseProcessor}
	if err = secondConsumer.Process(ctx, usecase.ProcessTaskRequest{ID: string(leaseTask.ID)}); err != nil {
		allowStaleFinish()
		t.Fatal(err)
	}
	allowStaleFinish()
	if err = <-firstDone; err != nil {
		t.Fatal(err)
	}
	recovered, err := (usecase.Tasks{Store: pgStore, Clock: time.Now}).Get(ctx, usecase.TaskRequest{ID: string(leaseTask.ID)})
	if err != nil || recovered.Status != string(domain.Completed) || recovered.Attempts != 2 || recovered.Result == nil || *recovered.Result != "new owner" {
		t.Fatalf("lease recovery=%+v err=%v", recovered, err)
	}

	conn, err := amqp091.Dial(fmt.Sprintf("amqp://task:task@%s:%s/", host, port.Port()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	if err = ch.Publish("", "integration.tasks", false, false, amqp091.Publishing{ContentType: "application/json", Body: []byte(`{"task_id":`)}); err != nil {
		t.Fatal(err)
	}
	poisonWorker := startConsumer(t, ctx, client, 1, func(context.Context, domain.ID) error { return errors.New("poison handler must not run") })
	poisonPhase, stopPoisonPhase := context.WithTimeout(ctx, 30*time.Second)
	defer stopPoisonPhase()
	for {
		info, inspectErr := ch.QueueInspect("integration.dead")
		if inspectErr == nil && info.Messages > 0 {
			break
		}
		select {
		case <-time.After(25 * time.Millisecond):
		case <-poisonPhase.Done():
			t.Fatal(poisonPhase.Err())
		case <-poisonWorker.done:
			t.Fatalf("poison consumer exited: %v", poisonWorker.err)
		}
	}
	if err = poisonWorker.Stop(); err != nil {
		t.Fatalf("poison consumer stop: %v", err)
	}

	api := &httpapi.API{Tasks: usecase.Tasks{Store: pgStore, Clock: time.Now}, Ready: func(ctx context.Context) error { return db.PingContext(ctx) }}
	server := httptest.NewServer(api.Handler())
	defer server.Close()
	apiClient := queueClient(host, port.Port(), "integration.api")
	t.Cleanup(func() { _ = apiClient.Close() })
	submitHTTP := func() domain.ID {
		t.Helper()
		resp, e := http.Post(server.URL+"/tasks", "application/json", strings.NewReader(`{"payload":{"http":true}}`))
		if e != nil {
			t.Fatal(e)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("HTTP submit status %d", resp.StatusCode)
		}
		var body struct {
			ID domain.ID `json:"id"`
		}
		if e = json.NewDecoder(resp.Body).Decode(&body); e != nil || body.ID == "" {
			t.Fatalf("HTTP create response: %+v %v", body, e)
		}
		return body.ID
	}
	cancelHTTP := func(id domain.ID) *http.Response {
		t.Helper()
		req, e := http.NewRequest(http.MethodPost, server.URL+"/tasks/"+string(id)+":cancel", nil)
		if e != nil {
			t.Fatal(e)
		}
		resp, e := http.DefaultClient.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		return resp
	}
	completedHTTP := submitHTTP()
	instant := usecase.Consumer{Store: pgStore, Clock: time.Now, Lease: time.Second, Timeout: time.Second, Processor: integrationProcessorFunc(func(context.Context, domain.Payload) (string, error) { return "complete", nil })}
	if err = instant.Process(ctx, usecase.ProcessTaskRequest{ID: string(completedHTTP)}); err != nil {
		t.Fatal(err)
	}
	response := cancelHTTP(completedHTTP)
	response.Body.Close()
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("completed HTTP cancel status=%d", response.StatusCode)
	}
	pendingHTTP := submitHTTP()
	response = cancelHTTP(pendingHTTP)
	if response.StatusCode != http.StatusOK {
		response.Body.Close()
		t.Fatalf("pending HTTP cancel status=%d", response.StatusCode)
	}
	response.Body.Close()
	count, err = (usecase.Relay{Outbox: pgStore, Publisher: apiClient, Batch: 32}).Once(ctx)
	if err != nil || count < 2 {
		t.Fatalf("relay API tasks=%d err=%v", count, err)
	}
	seen := make(chan struct{}, count)
	apiPhase, stopAPIPhase := context.WithTimeout(ctx, 30*time.Second)
	defer stopAPIPhase()
	apiWorker := startConsumer(t, ctx, apiClient, 2, func(c context.Context, id domain.ID) error {
		err := instant.Process(c, usecase.ProcessTaskRequest{ID: string(id)})
		seen <- struct{}{}
		return err
	})
	for i := 0; i < count; i++ {
		select {
		case <-seen:
		case <-apiPhase.Done():
			t.Fatal(apiPhase.Err())
		case <-apiWorker.done:
			t.Fatalf("API delivery worker exited: %v", apiWorker.err)
		}
	}
	if err = apiWorker.Stop(); err != nil {
		t.Fatalf("API worker stop: %v", err)
	}
	if err = apiClient.Close(); err != nil {
		t.Fatalf("API broker close: %v", err)
	}
	pendingState, err := (usecase.Tasks{Store: pgStore, Clock: time.Now}).Get(ctx, usecase.TaskRequest{ID: string(pendingHTTP)})
	if err != nil || pendingState.Status != string(domain.Canceled) || pendingState.Attempts != 0 {
		t.Fatalf("pending cancel state=%+v err=%v", pendingState, err)
	}

	// A transient storage failure is retried through RabbitMQ, then completed after the store recovers.
	retryPayload, _ := domain.NewPayload([]byte(`{"job":"storage-retry"}`))
	retryTask, err := (usecase.Tasks{Store: pgStore, Clock: time.Now}).Submit(ctx, usecase.SubmitTaskRequest{Payload: retryPayload.JSON()})
	if err != nil {
		t.Fatal(err)
	}
	retryClient := queueClient(host, port.Port(), "integration.storage")
	t.Cleanup(func() { _ = retryClient.Close() })
	count, err = (usecase.Relay{Outbox: pgStore, Publisher: retryClient, Batch: 16}).Once(ctx)
	if err != nil || count != 1 {
		t.Fatalf("retry relay count=%d err=%v", count, err)
	}
	closedDB, err := sql.Open("postgres", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if err = closedDB.Close(); err != nil {
		t.Fatal(err)
	}
	closedStore := &postgres.Store{DB: closedDB}
	observedClosedStore := &signalStore{inner: closedStore, failed: make(chan struct{}, 1)}
	switchable := &switchStore{current: observedClosedStore}
	retryConsumer := usecase.Consumer{Store: switchable, Clock: time.Now, Lease: 10 * time.Second, Timeout: 5 * time.Second, Processor: integrationProcessorFunc(func(context.Context, domain.Payload) (string, error) { return "recovered", nil })}
	seenFailure := make(chan domain.ID, 1)
	retryWorker := startConsumer(t, ctx, retryClient, 1, func(c context.Context, id domain.ID) error {
		seenFailure <- id
		return retryConsumer.Process(c, usecase.ProcessTaskRequest{ID: string(id)})
	})
	retryPhase, stopRetryPhase := context.WithTimeout(ctx, 30*time.Second)
	defer stopRetryPhase()
	select {
	case id := <-seenFailure:
		if string(id) != retryTask.ID {
			t.Fatalf("storage failure received unexpected task %s, wanted %s", id, retryTask.ID)
		}
	case <-retryPhase.Done():
		t.Fatal(retryPhase.Err())
	case <-retryWorker.done:
		t.Fatalf("retry worker exited: %v", retryWorker.err)
	}
	select {
	case <-observedClosedStore.failed:
		switchable.Set(pgStore)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case <-retryWorker.done:
		t.Fatalf("retry worker exited: %v", retryWorker.err)
	}
	retryState := waitForStatus(t, ctx, usecase.Tasks{Store: pgStore, Clock: time.Now}, domain.ID(retryTask.ID), domain.Completed)
	if retryState.Attempts != 1 || retryState.Result == nil || *retryState.Result != "recovered" {
		t.Fatalf("storage retry result=%+v", retryState)
	}
	if err = retryWorker.Stop(); err != nil {
		t.Fatalf("retry worker stop: %v", err)
	}
	if err = retryClient.Close(); err != nil {
		t.Fatalf("storage retry client close: %v", err)
	}

	// A broker outage releases the claimed outbox lease; making it due again permits confirmed delivery.
	failurePayload, _ := domain.NewPayload([]byte(`{"job":"broker-retry"}`))
	failureTask, err := (usecase.Tasks{Store: pgStore, Clock: time.Now}).Submit(ctx, usecase.SubmitTaskRequest{Payload: failurePayload.JSON()})
	if err != nil {
		t.Fatal(err)
	}
	invalidBroker := rabbitmq.New(rabbitmq.Config{URL: "amqp://task:task@127.0.0.1:1/", DialTimeout: 50 * time.Millisecond})
	count, relayErr := (usecase.Relay{Outbox: pgStore, Publisher: invalidBroker, Batch: 1}).Once(ctx)
	if relayErr == nil || count != 0 {
		t.Fatalf("unavailable broker relay count=%d err=%v", count, relayErr)
	}
	var published sql.NullTime
	var leaseToken, leaseUntil sql.NullString
	if err = db.QueryRowContext(ctx, `SELECT published_at,lease_token,lease_until FROM outbox WHERE task_id=$1`, failureTask.ID).Scan(&published, &leaseToken, &leaseUntil); err != nil || published.Valid || leaseToken.Valid || leaseUntil.Valid {
		t.Fatalf("failed publish outbox published=%v lease=%v/%v err=%v", published, leaseToken, leaseUntil, err)
	}
	if _, err = db.ExecContext(ctx, `UPDATE outbox SET available_at=now() WHERE task_id=$1`, failureTask.ID); err != nil {
		t.Fatal(err)
	}
	failureClient := queueClient(host, port.Port(), "integration.failure")
	t.Cleanup(func() { _ = failureClient.Close() })
	count, err = (usecase.Relay{Outbox: pgStore, Publisher: failureClient, Batch: 1}).Once(ctx)
	if err != nil || count != 1 {
		t.Fatalf("recovered relay count=%d err=%v", count, err)
	}
	recoveryWorker := startConsumer(t, ctx, failureClient, 1, func(c context.Context, id domain.ID) error {
		return consumer.Process(c, usecase.ProcessTaskRequest{ID: string(id)})
	})
	recoveredFailure := waitForStatus(t, ctx, usecase.Tasks{Store: pgStore, Clock: time.Now}, domain.ID(failureTask.ID), domain.Completed)
	if recoveredFailure.Attempts != 1 || recoveredFailure.Result == nil || *recoveredFailure.Result != "processed" {
		t.Fatalf("recovered broker task=%+v", recoveredFailure)
	}
	if err = recoveryWorker.Stop(); err != nil {
		t.Fatalf("recovered task worker stop: %v", err)
	}
	if err = failureClient.Close(); err != nil {
		t.Fatalf("failure retry client close: %v", err)
	}

	// Closing a worker connection returns its unacked delivery. A new lease owner reclaims it.
	shutdownPayload, _ := domain.NewPayload([]byte(`{"job":"worker-restart"}`))
	shutdownTask, err := (usecase.Tasks{Store: pgStore, Clock: time.Now}).Submit(ctx, usecase.SubmitTaskRequest{Payload: shutdownPayload.JSON()})
	if err != nil {
		t.Fatal(err)
	}
	shutdownClient := queueClient(host, port.Port(), "integration.shutdown")
	t.Cleanup(func() { _ = shutdownClient.Close() })
	count, err = (usecase.Relay{Outbox: pgStore, Publisher: shutdownClient, Batch: 16}).Once(ctx)
	if err != nil || count != 1 {
		t.Fatalf("shutdown relay count=%d err=%v", count, err)
	}
	firstTime := time.Now().UTC()
	firstStarted := make(chan struct{}, 1)
	shutdownConsumer := usecase.Consumer{Store: pgStore, Clock: func() time.Time { return firstTime }, Lease: 100 * time.Millisecond, Timeout: 5 * time.Second, Processor: integrationProcessorFunc(func(c context.Context, _ domain.Payload) (string, error) {
		firstStarted <- struct{}{}
		<-c.Done()
		return "", c.Err()
	})}
	shutdownWorker := startConsumer(t, ctx, shutdownClient, 1, func(c context.Context, id domain.ID) error {
		return shutdownConsumer.Process(c, usecase.ProcessTaskRequest{ID: string(id)})
	})
	shutdownPhase, stopShutdownPhase := context.WithTimeout(ctx, 30*time.Second)
	defer stopShutdownPhase()
	select {
	case <-firstStarted:
	case <-shutdownPhase.Done():
		t.Fatal(shutdownPhase.Err())
	case <-shutdownWorker.done:
		t.Fatalf("shutdown worker exited: %v", shutdownWorker.err)
	}
	if err = shutdownWorker.Stop(); err != nil {
		t.Fatalf("shutdown worker stop: %v", err)
	}
	if err = shutdownClient.Close(); err != nil {
		t.Fatalf("closing interrupted broker connection: %v", err)
	}
	interrupted, err := (usecase.Tasks{Store: pgStore, Clock: time.Now}).Get(ctx, usecase.TaskRequest{ID: string(shutdownTask.ID)})
	if err != nil || interrupted.Status != string(domain.Processing) || interrupted.Attempts != 1 {
		t.Fatalf("interrupted task=%+v err=%v", interrupted, err)
	}
	replacementClient := queueClient(host, port.Port(), "integration.shutdown")
	t.Cleanup(func() { _ = replacementClient.Close() })
	replacementConsumer := usecase.Consumer{Store: pgStore, Clock: func() time.Time { return firstTime.Add(101 * time.Millisecond) }, Lease: time.Second, Timeout: 5 * time.Second, Processor: integrationProcessorFunc(func(context.Context, domain.Payload) (string, error) { return "replacement", nil })}
	replacementWorker := startConsumer(t, ctx, replacementClient, 1, func(c context.Context, id domain.ID) error {
		return replacementConsumer.Process(c, usecase.ProcessTaskRequest{ID: string(id)})
	})
	restarted := waitForStatus(t, ctx, usecase.Tasks{Store: pgStore, Clock: time.Now}, domain.ID(shutdownTask.ID), domain.Completed)
	if restarted.Attempts != 2 || restarted.Result == nil || *restarted.Result != "replacement" {
		t.Fatalf("restarted worker result=%+v", restarted)
	}
	if err = replacementWorker.Stop(); err != nil {
		t.Fatalf("replacement worker stop: %v", err)
	}
	if err = replacementClient.Close(); err != nil {
		t.Fatalf("replacement client close: %v", err)
	}

	_ = db.Close()
	if _, err = pgStore.Get(ctx, domain.ID(created.ID)); err == nil {
		t.Fatal("database read unexpectedly succeeded after close")
	}
}

type integrationProcessorFunc func(context.Context, domain.Payload) (string, error)

func (f integrationProcessorFunc) Execute(ctx context.Context, p domain.Payload) (string, error) {
	return f(ctx, p)
}

func queueClient(host, port, prefix string) *rabbitmq.Client {
	return rabbitmq.New(rabbitmq.Config{URL: fmt.Sprintf("amqp://task:task@%s:%s/", host, port), Queue: prefix + ".tasks", RetryQueue: prefix + ".retry", DeadQueue: prefix + ".dead", RetryDelay: 100 * time.Millisecond})
}

type blockingProcessor struct {
	started chan struct{}
	release chan struct{}
}

type runningConsumer struct {
	cancel   context.CancelFunc
	done     chan struct{}
	stopOnce sync.Once
	err      error
}

func startConsumer(t *testing.T, parent context.Context, client *rabbitmq.Client, workers int, handler func(context.Context, domain.ID) error) *runningConsumer {
	t.Helper()
	ctx, cancel := context.WithCancel(parent)
	r := &runningConsumer{cancel: cancel, done: make(chan struct{})}
	processor := usecase.TaskProcessorFunc(func(c context.Context, req usecase.ProcessTaskRequest) error { return handler(c, domain.ID(req.ID)) })
	go func() { r.err = client.Consume(ctx, workers, processor); close(r.done) }()
	t.Cleanup(func() { _ = r.Stop() })
	return r
}

func waitForStatus(t *testing.T, parent context.Context, tasks usecase.Tasks, id domain.ID, want domain.Status) domain.Snapshot {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		state, err := tasks.Get(ctx, usecase.TaskRequest{ID: string(id)})
		if err != nil {
			t.Fatal(err)
		}
		if state.Status == string(want) {
			stored, storeErr := tasks.Store.Get(ctx, id)
			if storeErr != nil {
				t.Fatal(storeErr)
			}
			return stored.Snapshot()
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("task %s did not reach %s (last status %s): %v", id, want, state.Status, ctx.Err())
		}
	}
}

type signalStore struct {
	inner  usecase.Store
	failed chan struct{}
	once   sync.Once
}

func (s *signalStore) signal() { s.once.Do(func() { s.failed <- struct{}{} }) }
func (s *signalStore) Get(ctx context.Context, id domain.ID) (*domain.Task, error) {
	v, err := s.inner.Get(ctx, id)
	if err != nil {
		s.signal()
	}
	return v, err
}
func (s *signalStore) Within(ctx context.Context, fn func(usecase.Repository) error) error {
	err := s.inner.Within(ctx, fn)
	if err != nil {
		s.signal()
	}
	return err
}

type switchStore struct {
	mu      sync.RWMutex
	current usecase.Store
}

func (s *switchStore) Set(v usecase.Store) { s.mu.Lock(); s.current = v; s.mu.Unlock() }
func (s *switchStore) get() usecase.Store  { s.mu.RLock(); defer s.mu.RUnlock(); return s.current }
func (s *switchStore) Get(ctx context.Context, id domain.ID) (*domain.Task, error) {
	return s.get().Get(ctx, id)
}
func (s *switchStore) Within(ctx context.Context, fn func(usecase.Repository) error) error {
	return s.get().Within(ctx, fn)
}

func (r *runningConsumer) Stop() error {
	r.stopOnce.Do(r.cancel)
	select {
	case <-r.done:
		if errors.Is(r.err, context.Canceled) {
			return nil
		}
		return r.err
	case <-time.After(10 * time.Second):
		return errors.New("consumer goroutine did not stop")
	}
}

type barrierProcessor struct {
	started chan struct{}
	release chan struct{}
}

func (p *barrierProcessor) Execute(ctx context.Context, _ domain.Payload) (string, error) {
	p.started <- struct{}{}
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-p.release:
		return "parallel", nil
	}
}

func (p blockingProcessor) Execute(context.Context, domain.Payload) (string, error) {
	close(p.started)
	<-p.release
	return "late", nil
}
