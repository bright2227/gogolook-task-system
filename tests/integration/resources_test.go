//go:build integration

package integration

import (
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bright2227/gogolook-task-system/internal/configs"
	"github.com/bright2227/gogolook-task-system/internal/domain"
	"github.com/bright2227/gogolook-task-system/internal/infrastructure/rabbitmq"
	"github.com/bright2227/gogolook-task-system/internal/repository/task"
	"github.com/bright2227/gogolook-task-system/internal/usecase"
)

func TestSeparateResourcesManagersInitializeAndClose(t *testing.T) {
	suite.rabbitOnce.Do(initializeSuiteRabbit)
	if suite.rabbitErr != nil {
		t.Fatal(suite.rabbitErr)
	}
	// Start resources before the per-test deadline, matching newIntegrationStack.
	s := newPostgresStack(t, false)
	s.brokerURL = suite.brokerURL
	workQueue := s.queue("integration.resources.tasks")
	postgres := configs.NewPostgresResourcesManager(configs.Config{DatabaseURL: s.dbURL, DBMaxOpenConns: 4, DBMaxIdleConns: 2})
	var taskTable sql.NullString
	if err := s.db.QueryRowContext(s.ctx, `SELECT to_regclass('public.tasks')`).Scan(&taskTable); err != nil || taskTable.Valid || postgres.DB != nil {
		t.Fatalf("constructor initialized database or schema: table=%v, db=%v, err=%v", taskTable, postgres.DB, err)
	}
	if err := postgres.Close(); err != nil {
		t.Fatalf("close PostgreSQL before initialization: %v", err)
	}
	if err := postgres.Initialization(s.ctx); err != nil {
		t.Fatalf("initialize PostgreSQL resources: %v", err)
	}
	t.Cleanup(func() { _ = postgres.Close() })
	pool := postgres.DB
	if err := postgres.Initialization(s.ctx); err != nil || postgres.DB != pool {
		t.Fatalf("repeated initialization replaced live pool: %v", err)
	}
	// Server wiring: no topology at startup; the shared queue dials lazily.
	shared := configs.NewSharedTaskQueueResources(rabbitmq.Config{URL: s.brokerURL, Queue: workQueue, DialTimeout: time.Second}, 2)
	t.Cleanup(func() { _ = shared.Close() })
	svc := usecase.NewTaskUseCase(task.NewTaskRepository(postgres.DB), shared, time.Now)
	early, err := svc.Submit(s.ctx, "", []byte(`{"source":"before-topology"}`))
	if err != nil || early.Status != domain.Pending || early.IsSubmitted {
		t.Fatalf("submission before topology=%+v,%v; want pending, unsubmitted, for the sweeper", early, err)
	}
	broker := configs.NewRabbitMQResourcesManager(rabbitmq.Config{URL: s.brokerURL, Queue: workQueue, DialTimeout: time.Second})
	if err := broker.Close(); err != nil {
		t.Fatalf("close RabbitMQ before initialization: %v", err)
	}
	if err := broker.Initialization(s.ctx); err != nil {
		t.Fatalf("initialize RabbitMQ resources: %v", err)
	}
	s.cleanupQueue(t, workQueue)
	t.Cleanup(func() { _ = broker.Close() })
	if broker.Client == nil {
		t.Fatal("RabbitMQ resource manager returned no client")
	}
	if err := broker.Initialization(s.ctx); err != nil {
		t.Fatalf("repeated RabbitMQ topology initialization: %v", err)
	}

	var migrationCount int
	var latest string
	if err := postgres.DB.QueryRowContext(s.ctx, `SELECT count(*), max(filename) FROM schema_migrations`).Scan(&migrationCount, &latest); err != nil {
		t.Fatal(err)
	}
	if migrationCount != 1 || latest != "20261010000000_tasks.sql" {
		t.Fatalf("migration ledger count=%d latest=%q", migrationCount, latest)
	}

	// Once sweeper/consumer declared the topology, the server publishes.
	created, err := svc.Submit(s.ctx, "", []byte(`{"source":"resources-manager"}`))
	if err != nil {
		t.Fatalf("task repository through managed DB: %v", err)
	}
	if created.Status != domain.Pending || !created.IsSubmitted {
		t.Fatalf("created task state=%s submitted=%v, want pending and submitted", created.Status, created.IsSubmitted)
	}
	if inspectQueue(s, workQueue) != 1 {
		t.Fatal("resource manager did not declare the configured RabbitMQ topology")
	}
	// Concurrent submissions share the two-channel pool on one connection.
	const concurrent = 12
	var wg sync.WaitGroup
	failures := make(chan error, concurrent)
	for range concurrent {
		wg.Go(func() {
			if item, err := svc.Submit(s.ctx, "", []byte("concurrent")); err != nil || !item.IsSubmitted {
				failures <- fmt.Errorf("concurrent submit=%+v,%v", item, err)
			}
		})
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	if count := inspectQueue(s, workQueue); count != 1+concurrent {
		t.Fatalf("queued messages=%d, want %d", count, 1+concurrent)
	}

	if err := broker.Close(); err != nil {
		t.Fatalf("close RabbitMQ resources: %v", err)
	}
	if err := postgres.Close(); err != nil {
		t.Fatalf("close PostgreSQL resources: %v", err)
	}
	if err := postgres.DB.PingContext(s.ctx); err == nil {
		t.Fatal("managed database remained open after Close")
	}
}
