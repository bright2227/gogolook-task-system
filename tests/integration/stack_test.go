//go:build integration

package integration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bright2227/vsx-task-system/internal/adapter/postgres"
	"github.com/bright2227/vsx-task-system/internal/adapter/rabbitmq"
	"github.com/bright2227/vsx-task-system/internal/app"
	runtimeapp "github.com/bright2227/vsx-task-system/internal/runtime"
	"github.com/bright2227/vsx-task-system/internal/task"
	_ "github.com/lib/pq"
	"github.com/rabbitmq/amqp091-go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestPostgresAndRabbitMQ(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
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
	db, err := sql.Open("postgres", fmt.Sprintf("postgres://task:task@%s:%s/tasks?sslmode=disable", pgHost, pgPort.Port()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err = db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	migration, err := os.ReadFile(filepath.Join("..", "..", "migrations", "001_initial.sql"))
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
	client := rabbitmq.New(rabbitmq.Config{URL: fmt.Sprintf("amqp://task:task@%s:%s/", host, port.Port()), Queue: "integration.tasks", RetryQueue: "integration.retry", DeadQueue: "integration.dead"})
	t.Cleanup(func() { _ = client.Close() })
	payload, _ := task.NewPayload([]byte(`{"job":"integration"}`))
	created, err := (app.Tasks{Store: pgStore, Clock: time.Now}).Submit(ctx, payload)
	if err != nil {
		t.Fatal(err)
	}
	count, err := (app.Relay{Outbox: pgStore, Publisher: client, Batch: 16}).Once(ctx)
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
	consumer := app.Consumer{Store: pgStore, Clock: time.Now, Lease: 10 * time.Second, Timeout: 5 * time.Second, Processor: runtimeapp.SimulatedProcessor{}}
	workStarted := time.Now()
	workerCtx, stopWorker := context.WithCancel(ctx)
	workerErr := make(chan error, 1)
	go func() {
		workerErr <- client.Consume(workerCtx, 2, func(c context.Context, id task.ID) error { return consumer.Process(c, id) })
	}()
	deadline := time.NewTicker(20 * time.Millisecond)
	defer deadline.Stop()
	for {
		state, getErr := (app.Tasks{Store: pgStore, Clock: time.Now}).Get(ctx, created.ID)
		if getErr != nil {
			stopWorker()
			t.Fatal(getErr)
		}
		if state.Status == task.Completed {
			if elapsed := time.Since(workStarted); elapsed < 3*time.Second {
				stopWorker()
				t.Fatalf("sample work completed too early: %s", elapsed)
			}
			if state.Result == nil || *state.Result != "processed" {
				stopWorker()
				t.Fatalf("sample work result=%+v", state)
			}
			break
		}
		select {
		case <-deadline.C:
		case <-ctx.Done():
			stopWorker()
			t.Fatal(ctx.Err())
		case e := <-workerErr:
			stopWorker()
			t.Fatalf("worker exited before completing: %v", e)
		}
	}
	stopWorker()
	select {
	case <-workerErr:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop")
	}
	// Two queued jobs must both enter processing before either processor is released.
	concurrentIDs := make([]task.ID, 2)
	for i := range concurrentIDs {
		p, _ := task.NewPayload([]byte(`{"job":"parallel"}`))
		created, submitErr := (app.Tasks{Store: pgStore, Clock: time.Now}).Submit(ctx, p)
		if submitErr != nil {
			t.Fatal(submitErr)
		}
		concurrentIDs[i] = created.ID
	}
	count, err = (app.Relay{Outbox: pgStore, Publisher: client, Batch: 16}).Once(ctx)
	if err != nil || count != 2 {
		t.Fatalf("relay parallel jobs=%d err=%v", count, err)
	}
	barrier := &barrierProcessor{started: make(chan struct{}, 2), release: make(chan struct{})}
	parallelConsumer := app.Consumer{Store: pgStore, Clock: time.Now, Lease: 10 * time.Second, Timeout: 5 * time.Second, Processor: barrier}
	parallelCtx, stopParallel := context.WithCancel(ctx)
	parallelDone := make(chan error, 1)
	go func() {
		parallelDone <- client.Consume(parallelCtx, 2, func(c context.Context, id task.ID) error { return parallelConsumer.Process(c, id) })
	}()
	for i := 0; i < 2; i++ {
		select {
		case <-barrier.started:
		case <-ctx.Done():
			close(barrier.release)
			stopParallel()
			t.Fatal(ctx.Err())
		case e := <-parallelDone:
			close(barrier.release)
			stopParallel()
			t.Fatalf("parallel worker exited: %v", e)
		}
	}
	close(barrier.release)
	for _, id := range concurrentIDs {
		for {
			state, getErr := (app.Tasks{Store: pgStore, Clock: time.Now}).Get(ctx, id)
			if getErr != nil {
				stopParallel()
				t.Fatal(getErr)
			}
			if state.Status == task.Completed {
				break
			}
			select {
			case <-time.After(20 * time.Millisecond):
			case <-ctx.Done():
				stopParallel()
				t.Fatal(ctx.Err())
			}
		}
	}
	stopParallel()
	select {
	case <-parallelDone:
	case <-time.After(5 * time.Second):
		t.Fatal("parallel worker did not stop")
	}
	// A repeated delivery after completion remains a harmless no-op.
	if err = consumer.Process(ctx, created.ID); err != nil {
		t.Fatal(err)
	}
	state, err := (app.Tasks{Store: pgStore, Clock: time.Now}).Get(ctx, created.ID)
	if err != nil || state.Status != task.Completed || state.Attempts != 1 {
		t.Fatalf("worker result=%+v err=%v", state, err)
	}
	if err = db.QueryRowContext(ctx, `SELECT count(*) FROM task_events WHERE task_id=$1`, created.ID).Scan(&eventCount); err != nil || eventCount != 3 {
		t.Fatalf("event history=%d err=%v", eventCount, err)
	}

	rollbackPayload, _ := task.NewPayload([]byte(`{"job":"rollback"}`))
	rollbackTask, _ := task.Submit(rollbackPayload, time.Now())
	wantFailure := errors.New("force rollback")
	if err = pgStore.Within(ctx, func(r app.Repository) error {
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

	cancelPayload, _ := task.NewPayload([]byte(`{}`))
	cancelTask, err := (app.Tasks{Store: pgStore, Clock: time.Now}).Submit(ctx, cancelPayload)
	if err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	cancelConsumer := app.Consumer{Store: pgStore, Clock: time.Now, Lease: 10 * time.Second, Timeout: 5 * time.Second, Processor: blockingProcessor{started, release}}
	procDone := make(chan error, 1)
	go func() { procDone <- cancelConsumer.Process(ctx, cancelTask.ID) }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, err = (app.Tasks{Store: pgStore, Clock: time.Now}).Cancel(ctx, cancelTask.ID); err != nil {
		close(release)
		t.Fatal(err)
	}
	close(release)
	if err = <-procDone; err != nil {
		t.Fatal(err)
	}
	canceled, err := (app.Tasks{Store: pgStore, Clock: time.Now}).Get(ctx, cancelTask.ID)
	if err != nil || canceled.Status != task.Canceled {
		t.Fatalf("cancellation state=%+v err=%v", canceled, err)
	}

	leaseTask, err := (app.Tasks{Store: pgStore, Clock: time.Now}).Submit(ctx, cancelPayload)
	if err != nil {
		t.Fatal(err)
	}
	firstStarted, firstRelease := make(chan struct{}), make(chan struct{})
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
	leaseProcessor := integrationProcessorFunc(func(context.Context, task.Payload) (string, error) {
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
	firstConsumer := app.Consumer{Store: pgStore, Clock: leaseClock, Lease: 60 * time.Millisecond, Timeout: time.Second, Processor: leaseProcessor}
	firstDone := make(chan error, 1)
	go func() { firstDone <- firstConsumer.Process(ctx, leaseTask.ID) }()
	select {
	case <-firstStarted:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	secondConsumer := app.Consumer{Store: pgStore, Clock: leaseClock, Lease: time.Second, Timeout: time.Second, Processor: leaseProcessor}
	if err = secondConsumer.Process(ctx, leaseTask.ID); err != nil {
		close(firstRelease)
		t.Fatal(err)
	}
	close(firstRelease)
	if err = <-firstDone; err != nil {
		t.Fatal(err)
	}
	recovered, err := (app.Tasks{Store: pgStore, Clock: time.Now}).Get(ctx, leaseTask.ID)
	if err != nil || recovered.Status != task.Completed || recovered.Attempts != 2 || recovered.Result == nil || *recovered.Result != "new owner" {
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
	poisonCtx, stopPoison := context.WithCancel(ctx)
	poisonDone := make(chan error, 1)
	go func() {
		poisonDone <- client.Consume(poisonCtx, 1, func(context.Context, task.ID) error { return errors.New("poison handler must not run") })
	}()
	for {
		info, inspectErr := ch.QueueInspect("integration.dead")
		if inspectErr == nil && info.Messages > 0 {
			break
		}
		select {
		case <-time.After(25 * time.Millisecond):
		case <-ctx.Done():
			stopPoison()
			t.Fatal(ctx.Err())
		case e := <-poisonDone:
			stopPoison()
			t.Fatalf("poison consumer exited: %v", e)
		}
	}
	stopPoison()
	select {
	case <-poisonDone:
	case <-time.After(5 * time.Second):
		t.Fatal("poison consumer did not stop")
	}

	invalidBroker := rabbitmq.New(rabbitmq.Config{URL: "amqp://task:task@127.0.0.1:1/", DialTimeout: 50 * time.Millisecond})
	if err = invalidBroker.Publish(ctx, app.Message{TaskID: task.NewID(), EventID: task.NewID()}); err == nil {
		t.Fatal("publish unexpectedly succeeded without a broker")
	}
	_ = db.Close()
	if _, err = pgStore.Get(ctx, created.ID); err == nil {
		t.Fatal("database read unexpectedly succeeded after close")
	}
}

type integrationProcessorFunc func(context.Context, task.Payload) (string, error)

func (f integrationProcessorFunc) Execute(ctx context.Context, p task.Payload) (string, error) {
	return f(ctx, p)
}

type blockingProcessor struct {
	started chan struct{}
	release chan struct{}
}

type barrierProcessor struct {
	started chan struct{}
	release chan struct{}
}

func (p *barrierProcessor) Execute(ctx context.Context, _ task.Payload) (string, error) {
	p.started <- struct{}{}
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-p.release:
		return "parallel", nil
	}
}

func (p blockingProcessor) Execute(context.Context, task.Payload) (string, error) {
	close(p.started)
	<-p.release
	return "late", nil
}
