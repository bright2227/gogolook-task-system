//go:build integration

package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bright2227/gogolook-task-system/internal/delivery/rabbitmq"
	"github.com/bright2227/gogolook-task-system/internal/domain"
	"github.com/bright2227/gogolook-task-system/internal/repository/task/postgres"
	"github.com/bright2227/gogolook-task-system/internal/usecase"
	_ "github.com/lib/pq"
	"github.com/rabbitmq/amqp091-go"
	"github.com/testcontainers/testcontainers-go"
	tcnetwork "github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestRetryDLQAndWorkerCLI drives retry decisions through real PostgreSQL and
// RabbitMQ, then runs the operator CLI inside the worker image on the same
// container network as those services.
func TestRetryDLQAndWorkerCLI(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	network, err := tcnetwork.New(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = network.Remove(ctx) })

	pg, err := testcontainers.Run(ctx, "postgres:16-alpine",
		testcontainers.WithEnv(map[string]string{"POSTGRES_USER": "task", "POSTGRES_PASSWORD": "task", "POSTGRES_DB": "tasks"}),
		testcontainers.WithExposedPorts("5432/tcp"),
		tcnetwork.WithNetwork([]string{"pg"}, network),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(time.Minute), wait.ForListeningPort("5432/tcp")))
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
	migrationDir := filepath.Join("..", "..", "internal", "migrations")
	migrations, err := os.ReadDir(migrationDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range migrations {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		migration, readErr := os.ReadFile(filepath.Join(migrationDir, entry.Name()))
		if readErr != nil {
			t.Fatal(readErr)
		}
		if _, err = db.ExecContext(ctx, string(migration)); err != nil {
			t.Fatalf("apply migration %s: %v", entry.Name(), err)
		}
	}
	store := &postgres.Store{DB: db}

	rabbit, err := testcontainers.Run(ctx, "rabbitmq:3.13-alpine",
		testcontainers.WithEnv(map[string]string{"RABBITMQ_DEFAULT_USER": "task", "RABBITMQ_DEFAULT_PASS": "task"}),
		testcontainers.WithExposedPorts("5672/tcp"),
		tcnetwork.WithNetwork([]string{"rabbitmq"}, network),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("5672/tcp").WithStartupTimeout(time.Minute)))
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
	brokerURL := fmt.Sprintf("amqp://task:task@%s:%s/", host, port.Port())
	const queue, retryQueue, deadQueue = "integration.retry-dlq.tasks", "integration.retry-dlq.retry", "integration.retry-dlq.dead"
	client := rabbitmq.New(rabbitmq.Config{URL: brokerURL, Queue: queue, RetryQueue: retryQueue, DeadQueue: deadQueue, RetryDelay: 100 * time.Millisecond})
	t.Cleanup(func() { _ = client.Close() })

	// Four failed processor starts (initial plus three retries) produce exactly
	// three durable retry events at their configured due offsets and one DLQ row.
	payload, _ := domain.NewPayload([]byte(`{"effect":"idempotency-key-is-task-id"}`))
	task, err := (usecase.Tasks{Store: store, Clock: time.Now}).Submit(ctx, usecase.SubmitTaskRequest{Payload: payload.JSON()})
	if err != nil {
		t.Fatal(err)
	}
	initialDelivery := currentProcessRequest(ctx, t, store, db, domain.ID(task.ID))
	var retryProcessorStarts atomic.Int32
	var processorTaskIDs []domain.ID
	failing := usecase.Consumer{
		Store: store, Clock: time.Now, Lease: 5 * time.Second, Timeout: time.Second,
		WorkerID:    "integration-retry-worker",
		RetryDelays: []time.Duration{40 * time.Millisecond, 80 * time.Millisecond, 120 * time.Millisecond},
		Processor: integrationProcessorFunc(func(_ context.Context, taskID domain.ID, _ domain.Payload) (string, error) {
			processorTaskIDs = append(processorTaskIDs, taskID)
			if retryProcessorStarts.Add(1) == 1 {
				return "", context.DeadlineExceeded
			}
			return "", errors.New("temporary upstream unavailable")
		}),
	}
	if _, err = (usecase.Relay{Outbox: store, Publisher: client, Batch: 16}).Once(ctx); err != nil {
		t.Fatal(err)
	}
	worker := startConsumer(t, ctx, client, 1, func(c context.Context, req usecase.ProcessTaskRequest) error {
		return failing.Process(c, req)
	})
	relayDone := make(chan struct{})
	relayCtx, stopRelay := context.WithCancel(ctx)
	go func() {
		defer close(relayDone)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-relayCtx.Done():
				return
			case <-ticker.C:
				_, _ = (usecase.Relay{Outbox: store, Publisher: client, Batch: 16}).Once(relayCtx)
			}
		}
	}()
	state := waitForStatus(t, ctx, usecase.Tasks{Store: store, Clock: time.Now}, domain.ID(task.ID), domain.Completed)
	if state.Error == nil || state.Attempts != 4 || state.RetryCount != 3 || state.ReplayCycle != 0 {
		t.Fatalf("bounded retry terminal state=%+v", state)
	}
	waitForQueue(t, ctx, host, port.Port(), deadQueue, 1)
	if err = worker.Stop(); err != nil {
		t.Fatal(err)
	}
	stopRelay()
	<-relayDone
	if err = client.Close(); err != nil {
		t.Fatal(err)
	}
	assertAttemptHistory(t, ctx, db, domain.ID(task.ID), 4, 3)
	assertRetryDeadLetter(t, ctx, db, domain.ID(task.ID), []time.Duration{40 * time.Millisecond, 80 * time.Millisecond, 120 * time.Millisecond})
	var distinctExecutions, stableTaskIDs int
	if err = db.QueryRowContext(ctx, `SELECT count(DISTINCT execution_id),count(DISTINCT task_id) FROM task_attempts WHERE task_id=$1`, task.ID).Scan(&distinctExecutions, &stableTaskIDs); err != nil {
		t.Fatal(err)
	}
	if distinctExecutions != 4 || stableTaskIDs != 1 {
		t.Fatalf("retry execution identities distinct=%d task ids=%d", distinctExecutions, stableTaskIDs)
	}
	if len(processorTaskIDs) != 4 {
		t.Fatalf("processor received %d task keys for %d attempts", len(processorTaskIDs), state.Attempts)
	}
	for _, processorTaskID := range processorTaskIDs {
		if processorTaskID != domain.ID(task.ID) {
			t.Fatalf("processor business idempotency key changed across retry: %s", processorTaskID)
		}
	}
	// Redelivering the original, now-stale generation is acknowledged as stale;
	// it must not create a fifth processor attempt or another retry decision.
	if err = client.Publish(ctx, usecase.Message{EventID: domain.ID(initialDelivery.EventID), TaskID: domain.ID(task.ID), ExecutionID: domain.ID(initialDelivery.ExecutionID), Destination: usecase.DestinationWork}); err != nil {
		t.Fatal(err)
	}
	staleSeen := make(chan struct{}, 1)
	staleWorker := startConsumer(t, ctx, client, 1, func(c context.Context, req usecase.ProcessTaskRequest) error {
		processErr := failing.Process(c, req)
		select {
		case staleSeen <- struct{}{}:
		default:
		}
		return processErr
	})
	select {
	case <-staleSeen:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err = staleWorker.Stop(); err != nil {
		t.Fatal(err)
	}
	assertAttemptHistory(t, ctx, db, domain.ID(task.ID), 4, 3)

	// A failed transaction that reaches the retry decision rolls back the retry
	// counter and retry outbox row together. After the lease expires, the same
	// execution can be recovered and scheduled once.
	rollbackPayload, _ := domain.NewPayload([]byte(`{"kind":"transaction-rollback"}`))
	rollbackTask, err := (usecase.Tasks{Store: store, Clock: time.Now}).Submit(ctx, usecase.SubmitTaskRequest{Payload: rollbackPayload.JSON()})
	if err != nil {
		t.Fatal(err)
	}
	rollbackDelivery := currentProcessRequest(ctx, t, store, db, domain.ID(rollbackTask.ID))
	base := time.Now().UTC()
	rollbackStore := &failNthWithinStore{inner: store, failAt: 2, failure: errors.New("forced retry transaction rollback")}
	rollbackConsumer := usecase.Consumer{
		Store: rollbackStore, Clock: func() time.Time { return base }, Lease: 100 * time.Millisecond, Timeout: time.Second,
		RetryDelays: []time.Duration{25 * time.Millisecond},
		Processor: integrationProcessorFunc(func(context.Context, domain.ID, domain.Payload) (string, error) {
			return "", errors.New("temporary failure")
		}),
	}
	if err = rollbackConsumer.Process(ctx, rollbackDelivery); err == nil {
		t.Fatal("retry transaction failure unexpectedly returned success")
	}
	var retryCount, retryEvents int
	var rollbackStatus string
	if err = db.QueryRowContext(ctx, `SELECT retry_count,status FROM tasks WHERE id=$1`, rollbackTask.ID).Scan(&retryCount, &rollbackStatus); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRowContext(ctx, `SELECT count(*) FROM task_events WHERE task_id=$1 AND name='TaskRetryScheduled'`, rollbackTask.ID).Scan(&retryEvents); err != nil {
		t.Fatal(err)
	}
	if retryCount != 0 || retryEvents != 0 || rollbackStatus != string(domain.Processing) {
		t.Fatalf("rolled back retry decision left retry=%d events=%d status=%s", retryCount, retryEvents, rollbackStatus)
	}
	recoveredRollback := usecase.Consumer{
		Store: store, Clock: func() time.Time { return base.Add(101 * time.Millisecond) }, Lease: time.Second, Timeout: time.Second,
		RetryDelays: []time.Duration{25 * time.Millisecond},
		Processor: integrationProcessorFunc(func(context.Context, domain.ID, domain.Payload) (string, error) {
			return "", errors.New("temporary failure")
		}),
	}
	if err = recoveredRollback.Process(ctx, rollbackDelivery); err != nil {
		t.Fatal(err)
	}
	var recoveredAttempts int
	if err = db.QueryRowContext(ctx, `SELECT attempts,retry_count FROM tasks WHERE id=$1`, rollbackTask.ID).Scan(&recoveredAttempts, &retryCount); err != nil {
		t.Fatal(err)
	}
	if recoveredAttempts != 2 || retryCount != 1 {
		t.Fatalf("lease recovery after transaction rollback attempts=%d retry=%d", recoveredAttempts, retryCount)
	}
	if _, err = db.ExecContext(ctx, `UPDATE outbox SET published_at=COALESCE(published_at,now()),lease_token=NULL,lease_until=NULL WHERE task_id=$1`, rollbackTask.ID); err != nil {
		t.Fatal(err)
	}

	// An active lease does not authorize acknowledging a duplicate delivery.
	// The busy copy is requeued when its channel closes, then becomes a harmless
	// stale duplicate after the current lease owner commits success.
	activeClient := queueClient(host, port.Port(), "integration.active-lease")
	t.Cleanup(func() { _ = activeClient.Close() })
	activePayload, _ := domain.NewPayload([]byte(`{"kind":"active-lease"}`))
	activeTask, err := (usecase.Tasks{Store: store, Clock: time.Now}).Submit(ctx, usecase.SubmitTaskRequest{Payload: activePayload.JSON()})
	if err != nil {
		t.Fatal(err)
	}
	if n, err := (usecase.Relay{Outbox: store, Publisher: activeClient, Batch: 16}).Once(ctx); err != nil || n != 1 {
		t.Fatalf("active-lease initial relay=%d err=%v", n, err)
	}
	processorStarted, releaseProcessor := make(chan struct{}), make(chan struct{})
	var releaseProcessorOnce sync.Once
	unblockProcessor := func() { releaseProcessorOnce.Do(func() { close(releaseProcessor) }) }
	t.Cleanup(unblockProcessor)
	var processStarts atomic.Int32
	activeConsumer := usecase.Consumer{
		Store: store, Clock: time.Now, Lease: 5 * time.Second, Timeout: 4 * time.Second,
		Processor: integrationProcessorFunc(func(context.Context, domain.ID, domain.Payload) (string, error) {
			if processStarts.Add(1) == 1 {
				close(processorStarted)
			}
			<-releaseProcessor
			return "lease owner completed", nil
		}),
	}
	var deliveriesSeen atomic.Int32
	busySeen := make(chan struct{}, 1)
	activeWorker := startConsumer(t, ctx, activeClient, 2, func(c context.Context, req usecase.ProcessTaskRequest) error {
		deliveriesSeen.Add(1)
		processErr := activeConsumer.Process(c, req)
		if errors.Is(processErr, usecase.ErrBusy) {
			select {
			case busySeen <- struct{}{}:
			default:
			}
		}
		return processErr
	})
	select {
	case <-processorStarted:
	case <-ctx.Done():
		unblockProcessor()
		t.Fatal(ctx.Err())
	}
	activeRequest := currentProcessRequest(ctx, t, store, db, domain.ID(activeTask.ID))
	if err = activeClient.Publish(ctx, usecase.Message{EventID: domain.ID(activeRequest.EventID), TaskID: domain.ID(activeRequest.ID), ExecutionID: domain.ID(activeRequest.ExecutionID), Destination: usecase.DestinationWork}); err != nil {
		unblockProcessor()
		t.Fatal(err)
	}
	select {
	case <-busySeen:
	case <-ctx.Done():
		unblockProcessor()
		t.Fatal(ctx.Err())
	}
	waitForQueue(t, ctx, host, port.Port(), "integration.active-lease.tasks", 2)
	unblockProcessor()
	activeState := waitForStatus(t, ctx, usecase.Tasks{Store: store, Clock: time.Now}, domain.ID(activeTask.ID), domain.Completed)
	if activeState.Attempts != 1 || activeState.Result == nil || *activeState.Result != "lease owner completed" {
		t.Fatalf("active lease result=%+v", activeState)
	}
	waitForAtomicCount(t, ctx, &deliveriesSeen, 4)
	if err = activeWorker.Stop(); err != nil {
		t.Fatal(err)
	}
	assertQueueCount(t, ctx, host, port.Port(), "integration.active-lease.tasks", 0)
	if processStarts.Load() != 1 {
		t.Fatalf("active or stale duplicate invoked processor %d times", processStarts.Load())
	}

	// Simulate a worker process dying after the retry transaction commits but
	// before it acknowledges the source delivery. Redelivery of that old event
	// is stale, and the outbox retry produces only one additional attempt.
	crashPayload, _ := domain.NewPayload([]byte(`{"kind":"commit-before-ack"}`))
	crashTask, err := (usecase.Tasks{Store: store, Clock: time.Now}).Submit(ctx, usecase.SubmitTaskRequest{Payload: crashPayload.JSON()})
	if err != nil {
		t.Fatal(err)
	}
	crashClient := queueClient(host, port.Port(), "integration.commit-before-ack")
	t.Cleanup(func() { _ = crashClient.Close() })
	if _, err = (usecase.Relay{Outbox: store, Publisher: crashClient, Batch: 16}).Once(ctx); err != nil {
		t.Fatal(err)
	}
	crashConsumer := usecase.Consumer{
		Store: store, Clock: time.Now, Lease: 5 * time.Second, Timeout: time.Second,
		RetryDelays: []time.Duration{time.Second},
		Processor: integrationProcessorFunc(func(context.Context, domain.ID, domain.Payload) (string, error) {
			return "", errors.New("retry after commit")
		}),
	}
	var firstDecision atomic.Bool
	crashObserved := make(chan struct{}, 1)
	crashWorker := startConsumer(t, ctx, crashClient, 1, func(c context.Context, req usecase.ProcessTaskRequest) error {
		processErr := crashConsumer.Process(c, req)
		if processErr == nil && firstDecision.CompareAndSwap(false, true) {
			crashObserved <- struct{}{}
			return errors.New("simulated process exit after commit before acknowledgement")
		}
		return processErr
	})
	crashRelayCtx, stopCrashRelay := context.WithCancel(ctx)
	crashRelayDone := make(chan struct{})
	go func() {
		defer close(crashRelayDone)
		ticker := time.NewTicker(15 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-crashRelayCtx.Done():
				return
			case <-ticker.C:
				_, _ = (usecase.Relay{Outbox: store, Publisher: crashClient, Batch: 16}).Once(crashRelayCtx)
			}
		}
	}()
	select {
	case <-crashObserved:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	crashTerminal := waitForStatus(t, ctx, usecase.Tasks{Store: store, Clock: time.Now}, domain.ID(crashTask.ID), domain.Completed)
	if crashTerminal.Attempts != 2 || crashTerminal.RetryCount != 1 || crashTerminal.Error == nil {
		t.Fatalf("commit-before-ack replay state=%+v", crashTerminal)
	}
	waitForQueue(t, ctx, host, port.Port(), "integration.commit-before-ack.dead", 1)
	if err = crashWorker.Stop(); err != nil {
		t.Fatal(err)
	}
	stopCrashRelay()
	<-crashRelayDone
	assertAttemptHistory(t, ctx, db, domain.ID(crashTask.ID), 2, 1)

	// A permanent failure skips the retry budget and reaches the DLQ on its
	// first execution. The stable database DLQ id is then replayed from /taskctl.
	permanentPayload, _ := domain.NewPayload([]byte(`{"kind":"permanent"}`))
	permanentTask, err := (usecase.Tasks{Store: store, Clock: time.Now}).Submit(ctx, usecase.SubmitTaskRequest{Payload: permanentPayload.JSON()})
	if err != nil {
		t.Fatal(err)
	}
	permanentWorker := usecase.Consumer{
		Store: store, Clock: time.Now, Lease: 5 * time.Second, Timeout: time.Second, WorkerID: "integration-permanent-worker",
		RetryDelays: []time.Duration{40 * time.Millisecond, 80 * time.Millisecond},
		Processor: integrationProcessorFunc(func(context.Context, domain.ID, domain.Payload) (string, error) {
			return "", usecase.Permanent("bad_input", "upstream rejected this task", nil)
		}),
	}
	permanentDelivery := currentProcessRequest(ctx, t, store, db, domain.ID(permanentTask.ID))
	if err = permanentWorker.Process(ctx, permanentDelivery); err != nil {
		t.Fatal(err)
	}
	permanentState, err := (usecase.Tasks{Store: store, Clock: time.Now}).Get(ctx, usecase.TaskRequest{ID: permanentTask.ID})
	if err != nil || permanentState.Status != string(domain.Completed) || permanentState.Attempts != 1 || permanentState.RetryCount != 0 || permanentState.Error == nil {
		t.Fatalf("permanent failure state=%+v err=%v", permanentState, err)
	}
	if n, err := (usecase.Relay{Outbox: store, Publisher: client, Batch: 16}).Once(ctx); err != nil || n != 2 {
		// The task's original submission event is still unpublished because this
		// test drives the permanent consumer directly. Relay sends both that work
		// event and the terminal DLQ event; the worker later observes the work
		// event as stale and safely acknowledges it.
		t.Fatalf("publish permanent submission and DLQ events count=%d err=%v", n, err)
	}
	waitForQueue(t, ctx, host, port.Port(), deadQueue, 2)
	var messageID string
	if err = db.QueryRowContext(ctx, `SELECT message_id FROM task_dead_letters WHERE task_id=$1`, permanentTask.ID).Scan(&messageID); err != nil {
		t.Fatal(err)
	}

	// Build and start the actual worker image in the same isolated network. The
	// CLI runs through Container.Exec in that worker, using its DB/Rabbit config.
	service := "worker"
	workerImage, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			FromDockerfile: testcontainers.FromDockerfile{
				Context: filepath.Join("..", ".."), Dockerfile: "deployments/Dockerfile",
				BuildArgs: map[string]*string{"SERVICE": &service},
			},
			Env: map[string]string{
				"DATABASE_URL": "postgres://task:task@pg:5432/tasks?sslmode=disable",
				"RABBITMQ_URL": "amqp://task:task@rabbitmq:5672/",
				"TASK_QUEUE":   queue, "RETRY_QUEUE": retryQueue, "DLQ_QUEUE": deadQueue,
				"WORKERS": "1", "TASK_RETRY_DELAYS": "none", "PROCESS_TIMEOUT": "1s", "TASK_LEASE": "5s",
			},
			Networks:       []string{network.Name},
			NetworkAliases: map[string][]string{network.Name: {"worker"}},
			WaitingFor:     wait.ForLog("worker started").WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(workerImage) })

	inspectStatus, inspectOutput := execWorker(t, ctx, workerImage, "/taskctl", "dlq", "inspect", "--limit", "20")
	if inspectStatus != 0 || !strings.Contains(inspectOutput, messageID) || !strings.Contains(inspectOutput, string(permanentTask.ID)) || !strings.Contains(inspectOutput, "bad_input") {
		t.Fatalf("taskctl inspect exit=%d output=%q", inspectStatus, inspectOutput)
	}
	assertQueueCount(t, ctx, host, port.Port(), deadQueue, 2) // inspection requeues every scanned delivery.
	operator, reason := "oncall@example.test", "upstream dependency is fixed"
	replayStatus, replayOutput := execWorker(t, ctx, workerImage, "/taskctl", "dlq", "replay", "--message-id", messageID, "--operator", operator, "--reason", reason)
	if replayStatus != 0 || !strings.Contains(replayOutput, `"already_replayed":false`) {
		t.Fatalf("taskctl replay exit=%d output=%q", replayStatus, replayOutput)
	}
	var replayCycle, receiptCount, replayOutboxCount int
	if err = db.QueryRowContext(ctx, `SELECT replay_cycle FROM tasks WHERE id=$1`, permanentTask.ID).Scan(&replayCycle); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRowContext(ctx, `SELECT count(*) FROM task_replay_receipts WHERE message_id=$1`, messageID).Scan(&receiptCount); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRowContext(ctx, `SELECT count(*) FROM outbox WHERE task_id=$1 AND destination='work' AND execution_id=(SELECT execution_id FROM tasks WHERE id=$1)`, permanentTask.ID).Scan(&replayOutboxCount); err != nil {
		t.Fatal(err)
	}
	if replayCycle != 1 || receiptCount != 1 || replayOutboxCount != 1 {
		t.Fatalf("replay audit cycle=%d receipt=%d replay work events=%d", replayCycle, receiptCount, replayOutboxCount)
	}
	var attempts, retries int
	if err = db.QueryRowContext(ctx, `SELECT attempts,retry_count FROM tasks WHERE id=$1`, permanentTask.ID).Scan(&attempts, &retries); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || retries != 0 {
		t.Fatalf("replay rewrote lifetime attempts or retry budget: attempts=%d retries=%d", attempts, retries)
	}
	var auditOperator, auditReason string
	if err = db.QueryRowContext(ctx, `SELECT operator,reason FROM task_replay_receipts WHERE message_id=$1`, messageID).Scan(&auditOperator, &auditReason); err != nil {
		t.Fatal(err)
	}
	if auditOperator != operator || auditReason != reason {
		t.Fatalf("replay operator audit=%q/%q", auditOperator, auditReason)
	}

	// Simulate taskctl committing its replay receipt and exiting before it can
	// ACK the broker delivery. Channel close requeues the same message; the next
	// CLI invocation sees the receipt first, reports already_replayed, and ACKs.
	var retryMessageID string
	if err = db.QueryRowContext(ctx, `SELECT message_id FROM task_dead_letters WHERE task_id=$1`, task.ID).Scan(&retryMessageID); err != nil {
		t.Fatal(err)
	}
	var replayBody []byte
	crashScan, crashErr := client.ScanDeadLetters(ctx, 100, retryMessageID, func(item rabbitmq.DeadLetterItem) error {
		replayBody = append([]byte(nil), item.Body...)
		_, replayErr := (usecase.Tasks{Store: store, Clock: time.Now}).Replay(ctx, usecase.ReplayRequest{MessageID: retryMessageID, Operator: operator, Reason: reason})
		if replayErr != nil {
			return replayErr
		}
		return errors.New("simulated taskctl crash after receipt commit before ACK")
	})
	if crashErr == nil || !crashScan.Found || len(replayBody) == 0 {
		t.Fatalf("replay crash injection result=%+v err=%v body=%d", crashScan, crashErr, len(replayBody))
	}
	if err = client.Close(); err != nil {
		t.Fatal(err)
	}
	retryReplayStatus, retryReplayOutput := execWorker(t, ctx, workerImage, "/taskctl", "dlq", "replay", "--message-id", retryMessageID, "--operator", operator, "--reason", reason)
	if retryReplayStatus != 0 || !strings.Contains(retryReplayOutput, `"already_replayed":true`) {
		t.Fatalf("recovered taskctl replay exit=%d output=%q", retryReplayStatus, retryReplayOutput)
	}
	var retryCycle, retryAttempts, retryBudget, retryReceipts int
	if err = db.QueryRowContext(ctx, `SELECT replay_cycle,attempts,retry_count FROM tasks WHERE id=$1`, task.ID).Scan(&retryCycle, &retryAttempts, &retryBudget); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRowContext(ctx, `SELECT count(*) FROM task_replay_receipts WHERE message_id=$1`, retryMessageID).Scan(&retryReceipts); err != nil {
		t.Fatal(err)
	}
	if retryCycle != 1 || retryAttempts != 4 || retryBudget != 0 || retryReceipts != 1 {
		t.Fatalf("recovered replay audit cycle=%d attempts=%d retry=%d receipts=%d", retryCycle, retryAttempts, retryBudget, retryReceipts)
	}
	// A conflicting duplicate replay is rejected without ACK; the original
	// request can still safely consume the duplicate delivery afterward.
	if err = publishDeadLetterBody(ctx, brokerURL, deadQueue, retryMessageID, replayBody); err != nil {
		t.Fatal(err)
	}
	conflictStatus, conflictOutput := execWorker(t, ctx, workerImage, "/taskctl", "dlq", "replay", "--message-id", retryMessageID, "--operator", operator, "--reason", "different operator decision")
	if conflictStatus == 0 {
		t.Fatalf("conflicting replay unexpectedly succeeded: %q", conflictOutput)
	}
	assertQueueCount(t, ctx, host, port.Port(), deadQueue, 1)
	repeatStatus, repeatOutput := execWorker(t, ctx, workerImage, "/taskctl", "dlq", "replay", "--message-id", retryMessageID, "--operator", operator, "--reason", reason)
	if repeatStatus != 0 || !strings.Contains(repeatOutput, `"already_replayed":true`) {
		t.Fatalf("idempotent duplicate replay exit=%d output=%q", repeatStatus, repeatOutput)
	}
	assertQueueCount(t, ctx, host, port.Port(), deadQueue, 0)

	// Malformed envelopes remain inspectable but cannot be replayed implicitly.
	malformedID := string(domain.NewID())
	malformedBody, err := json.Marshal(rabbitmq.FailureEnvelope{
		Version: rabbitmq.FailureEnvelopeVersion, MessageID: malformedID, FailedAt: time.Now().UTC(),
		Failure: usecase.FailureRecord{Category: "malformed_envelope", Code: "malformed_envelope", Detail: "raw body requires explicit repair"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = publishDeadLetterBody(ctx, brokerURL, deadQueue, malformedID, malformedBody); err != nil {
		t.Fatal(err)
	}
	malformedStatus, malformedOutput := execWorker(t, ctx, workerImage, "/taskctl", "dlq", "replay", "--message-id", malformedID, "--operator", operator, "--reason", reason)
	if malformedStatus == 0 {
		t.Fatalf("malformed DLQ replay unexpectedly succeeded: %q", malformedOutput)
	}
	assertQueueCount(t, ctx, host, port.Port(), deadQueue, 1)
}

func assertAttemptHistory(t *testing.T, ctx context.Context, db *sql.DB, id domain.ID, wantAttempts, wantRetries int) {
	t.Helper()
	var attempts, finished, retryScheduled, deadLettered int
	if err := db.QueryRowContext(ctx, `SELECT count(*),count(finished_at),count(*) FILTER (WHERE outcome='retry_scheduled'),count(*) FILTER (WHERE outcome='dead_lettered') FROM task_attempts WHERE task_id=$1`, id).Scan(&attempts, &finished, &retryScheduled, &deadLettered); err != nil {
		t.Fatal(err)
	}
	if attempts != wantAttempts || finished != wantAttempts || retryScheduled != wantRetries || deadLettered != 1 {
		t.Fatalf("attempt records for %s: rows=%d finished=%d retry=%d dead=%d", id, attempts, finished, retryScheduled, deadLettered)
	}
}

func assertRetryDeadLetter(t *testing.T, ctx context.Context, db *sql.DB, id domain.ID, delays []time.Duration) {
	t.Helper()
	rows, err := db.QueryContext(ctx, `SELECT e.occurred_at,o.available_at FROM task_events e JOIN outbox o ON o.event_id=e.event_id WHERE e.task_id=$1 AND e.name='TaskRetryScheduled' ORDER BY e.occurred_at`, id)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got int
	for rows.Next() {
		var scheduledAt, dueAt time.Time
		if err = rows.Scan(&scheduledAt, &dueAt); err != nil {
			t.Fatal(err)
		}
		if got >= len(delays) || dueAt.Sub(scheduledAt) != delays[got] {
			t.Fatalf("retry %d deadline delta=%s, want %s", got+1, dueAt.Sub(scheduledAt), delays[min(got, len(delays)-1)])
		}
		got++
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	if got != len(delays) {
		t.Fatalf("scheduled retry decisions=%d, want %d", got, len(delays))
	}
	var destination, category, code string
	var retryCount, attempts int
	if err = db.QueryRowContext(ctx, `SELECT o.destination,d.category,d.code,d.retry_count,d.attempts FROM outbox o JOIN task_dead_letters d ON d.message_id=o.event_id::text WHERE o.task_id=$1 AND o.destination='dead-letter'`, id).Scan(&destination, &category, &code, &retryCount, &attempts); err != nil {
		t.Fatal(err)
	}
	if destination != string(usecase.DestinationDeadLetter) || category != "exhausted" || code != "retries_exhausted" || retryCount != len(delays) || attempts != len(delays)+1 {
		t.Fatalf("exhaustion diagnostics destination=%s category=%s code=%s retries=%d attempts=%d", destination, category, code, retryCount, attempts)
	}
}

func waitForQueue(t *testing.T, parent context.Context, host, port, queue string, count int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if assertQueueAtLeast(ctx, host, port, queue, count) {
			return
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("queue %s did not reach %d messages: %v", queue, count, ctx.Err())
		}
	}
}

func assertQueueCount(t *testing.T, ctx context.Context, host, port, queue string, want int) {
	t.Helper()
	if got := inspectQueueCount(t, ctx, host, port, queue); got != want {
		t.Fatalf("queue %s messages=%d, want %d", queue, got, want)
	}
}

func assertQueueAtLeast(ctx context.Context, host, port, queue string, want int) bool {
	return inspectQueueCount(nil, ctx, host, port, queue) >= want
}

func inspectQueueCount(t *testing.T, ctx context.Context, host, port, queue string) int {
	conn, err := amqp091.Dial(fmt.Sprintf("amqp://task:task@%s:%s/", host, port))
	if err != nil {
		if t != nil {
			t.Fatal(err)
		}
		return -1
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		if t != nil {
			t.Fatal(err)
		}
		return -1
	}
	defer ch.Close()
	info, err := ch.QueueInspect(queue)
	if err != nil {
		if t != nil {
			t.Fatal(err)
		}
		return -1
	}
	return info.Messages
}

func execWorker(t *testing.T, ctx context.Context, worker testcontainers.Container, args ...string) (int, string) {
	t.Helper()
	status, output, err := worker.Exec(ctx, args)
	if err != nil {
		t.Fatalf("worker Exec %v: %v", args, err)
	}
	body, err := io.ReadAll(output)
	if err != nil {
		t.Fatalf("read worker Exec %v output: %v", args, err)
	}
	return status, string(body)
}

func publishDeadLetterBody(ctx context.Context, url, queue, messageID string, body []byte) error {
	conn, err := amqp091.Dial(url)
	if err != nil {
		return err
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	if _, err = ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		return err
	}
	return ch.PublishWithContext(ctx, "", queue, true, false, amqp091.Publishing{
		ContentType: "application/json", DeliveryMode: amqp091.Persistent,
		MessageId: messageID, Body: append([]byte(nil), body...),
	})
}

type failNthWithinStore struct {
	inner   usecase.Store
	failAt  int
	failure error
	calls   int
}

func waitForAtomicCount(t *testing.T, parent context.Context, count *atomic.Int32, want int32) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for count.Load() < want {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("observed %d deliveries, want %d: %v", count.Load(), want, ctx.Err())
		}
	}
}

func (s *failNthWithinStore) Get(ctx context.Context, id domain.ID) (*domain.Task, error) {
	return s.inner.Get(ctx, id)
}

func (s *failNthWithinStore) Within(ctx context.Context, fn func(usecase.Repository) error) error {
	s.calls++
	if s.calls != s.failAt {
		return s.inner.Within(ctx, fn)
	}
	return s.inner.Within(ctx, func(r usecase.Repository) error {
		if err := fn(r); err != nil {
			return err
		}
		return s.failure
	})
}
