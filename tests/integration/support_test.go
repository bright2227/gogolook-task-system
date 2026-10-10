//go:build integration

package integration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bright2227/gogolook-task-system/internal/usecase/port"

	"github.com/bright2227/gogolook-task-system/internal/configs"
	deliveryamqp "github.com/bright2227/gogolook-task-system/internal/delivery/amqp"
	"github.com/bright2227/gogolook-task-system/internal/domain"
	"github.com/bright2227/gogolook-task-system/internal/infrastructure/rabbitmq"
	"github.com/bright2227/gogolook-task-system/internal/migrations"
	"github.com/bright2227/gogolook-task-system/internal/repository/task"
	"github.com/bright2227/gogolook-task-system/internal/usecase"
	"github.com/docker/docker/api/types/container"
	_ "github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type integrationStack struct {
	ctx              context.Context
	db               *sql.DB
	repo             port.TaskRepository
	dbURL, brokerURL string
	queueSuffix      string
}

// retryDelays mirrors the source's default network retry policy.
var retryDelays = []time.Duration{time.Second, 10 * time.Second, time.Minute}

// republishAfter enables the optional submitted-task safety net
// (TASK_REPUBLISH_AFTER, disabled by default).
const republishAfter = 45 * time.Second

// staleAfter is shorter than the TASK_STALE_AFTER default to keep tests quick;
// it is still 2*PROCESS_TIMEOUT+10s for the default 10s PROCESS_TIMEOUT.
const staleAfter = 30 * time.Second

// The suite owns one disposable container per resource. Each test gets a fresh
// database and unique queues; no application state is shared across tests.
var suite struct {
	pgOnce, rabbitOnce   sync.Once
	pg, rabbit           testcontainers.Container
	pgErr, rabbitErr     error
	admin                *sql.DB
	pgAddress, brokerURL string
	pgURL                *url.URL
	ctx                  context.Context
}

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	suite.ctx = ctx
	code := m.Run()
	if code != 0 && suite.rabbit != nil {
		logsCtx, stop := context.WithTimeout(context.Background(), 3*time.Second)
		if stream, err := suite.rabbit.Logs(logsCtx); err == nil {
			body, _ := io.ReadAll(io.LimitReader(stream, 64<<10))
			_ = stream.Close()
			for _, line := range strings.Split(string(body), "\n") {
				lower := strings.ToLower(line)
				if strings.Contains(lower, "alarm") || strings.Contains(lower, "disk") || strings.Contains(lower, "watermark") || strings.Contains(lower, "blocked") || strings.Contains(lower, "error") {
					fmt.Fprintln(os.Stderr, line)
				}
			}
		}
		stop()
	}
	if suite.admin != nil {
		_ = suite.admin.Close()
	}
	if suite.rabbit != nil {
		_ = testcontainers.TerminateContainer(suite.rabbit)
	}
	if suite.pg != nil {
		_ = testcontainers.TerminateContainer(suite.pg)
	}
	cancel()
	os.Exit(code)
}
func initializeSuitePostgres() {
	if endpoint := os.Getenv("INTEGRATION_POSTGRES_URL"); endpoint != "" {
		target, err := url.Parse(endpoint)
		if err != nil {
			suite.pgErr = err
			return
		}
		suite.pgAddress = target.Host
		suite.pgURL = target
		suite.admin, suite.pgErr = sql.Open("postgres", endpoint)
		if suite.pgErr == nil {
			suite.admin.SetMaxOpenConns(2)
			suite.admin.SetMaxIdleConns(1)
			suite.pgErr = suite.admin.PingContext(suite.ctx)
		}
		return
	}
	suite.pg, suite.pgErr = testcontainers.Run(suite.ctx, "postgres:16-alpine",
		testcontainers.WithEnv(map[string]string{"POSTGRES_USER": "task", "POSTGRES_PASSWORD": "task", "POSTGRES_DB": "tasks"}),
		testcontainers.WithCmd("postgres", "-c", "shared_buffers=16MB", "-c", "max_connections=32", "-c", "work_mem=1MB", "-c", "maintenance_work_mem=16MB"),
		testcontainers.WithHostConfigModifier(func(c *container.HostConfig) {
			c.Memory = 192 << 20
			c.NanoCPUs = 1e9
			c.Tmpfs = map[string]string{"/var/lib/postgresql/data": "rw,size=96m"}
		}),
		testcontainers.WithExposedPorts("5432/tcp"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(time.Minute), wait.ForListeningPort("5432/tcp").SkipInternalCheck().WithStartupTimeout(time.Minute)))
	if suite.pgErr != nil {
		return
	}
	host, err := suite.pg.Host(suite.ctx)
	if err != nil {
		suite.pgErr = err
		return
	}
	port, err := suite.pg.MappedPort(suite.ctx, "5432/tcp")
	if err != nil {
		suite.pgErr = err
		return
	}
	suite.pgAddress = host + ":" + port.Port()
	suite.admin, suite.pgErr = sql.Open("postgres", "postgres://task:task@"+suite.pgAddress+"/tasks?sslmode=disable")
	if suite.pgErr == nil {
		suite.admin.SetMaxOpenConns(2)
		suite.admin.SetMaxIdleConns(1)
		suite.pgErr = suite.admin.PingContext(suite.ctx)
	}
}
func initializeSuiteRabbit() {
	if endpoint := os.Getenv("INTEGRATION_RABBITMQ_URL"); endpoint != "" {
		suite.brokerURL = endpoint
		return
	}
	suite.rabbit, suite.rabbitErr = testcontainers.Run(suite.ctx, "rabbitmq:3.13-alpine",
		testcontainers.WithEnv(map[string]string{"RABBITMQ_DEFAULT_USER": "task", "RABBITMQ_DEFAULT_PASS": "task", "RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS": "+S 1:1 +SDcpu 1 +SDio 1 +A 4"}),
		testcontainers.WithFiles(testcontainers.ContainerFile{Reader: strings.NewReader("vm_memory_high_watermark.absolute = 192MiB\ndisk_free_limit.absolute = 16MiB\n"), ContainerFilePath: "/etc/rabbitmq/conf.d/30-limits.conf", FileMode: 0644}, testcontainers.ContainerFile{Reader: strings.NewReader("[].\n"), ContainerFilePath: "/etc/rabbitmq/enabled_plugins", FileMode: 0644}),
		testcontainers.WithHostConfigModifier(func(c *container.HostConfig) {
			c.Memory = 256 << 20
			c.NanoCPUs = 1e9
			c.Tmpfs = map[string]string{"/var/lib/rabbitmq": "rw,size=64m"}
		}),
		testcontainers.WithExposedPorts("5672/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("Server startup complete;").WithStartupTimeout(time.Minute),
			wait.ForListeningPort("5672/tcp").SkipInternalCheck().WithStartupTimeout(time.Minute)))
	if suite.rabbitErr != nil {
		return
	}
	host, err := suite.rabbit.Host(suite.ctx)
	if err != nil {
		suite.rabbitErr = err
		return
	}
	port, err := suite.rabbit.MappedPort(suite.ctx, "5672/tcp")
	if err != nil {
		suite.rabbitErr = err
		return
	}
	suite.brokerURL = "amqp://task:task@" + host + ":" + port.Port() + "/"
}
func newPostgresStack(t *testing.T, applyMigrations bool) integrationStack {
	t.Helper()
	suite.pgOnce.Do(initializeSuitePostgres)
	if suite.pgErr != nil {
		t.Fatal(suite.pgErr)
	}
	ctx, cancel := context.WithTimeout(suite.ctx, 30*time.Second)
	t.Cleanup(cancel)
	database := "test_" + strings.ReplaceAll(string(domain.NewID()), "-", "")
	if _, err := suite.admin.ExecContext(ctx, "CREATE DATABASE "+database); err != nil {
		t.Fatal(err)
	}
	dbURL := "postgres://task:task@" + suite.pgAddress + "/" + database + "?sslmode=disable"
	if suite.pgURL != nil {
		target := *suite.pgURL
		target.Path = "/" + database
		target.RawPath = ""
		dbURL = target.String()
	}
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() {
		_ = db.Close()
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := suite.admin.ExecContext(cleanup, "DROP DATABASE "+database+" WITH (FORCE)"); err != nil {
			t.Errorf("delete test database %s: %v", database, err)
		}
	})
	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	if applyMigrations {
		if err := migrations.Apply(ctx, db); err != nil {
			t.Fatal(err)
		}
	}
	return integrationStack{ctx: ctx, db: db, repo: task.NewTaskRepository(db), dbURL: dbURL, queueSuffix: database}
}
func newIntegrationStack(t *testing.T) integrationStack {
	t.Helper()
	suite.rabbitOnce.Do(initializeSuiteRabbit)
	if suite.rabbitErr != nil {
		t.Fatal(suite.rabbitErr)
	}
	s := newPostgresStack(t, true)
	s.brokerURL = suite.brokerURL
	return s
}

// Broker construction and initialization are separate, matching application wiring.
type brokerFixture struct {
	*rabbitmq.TaskQueue
	broker *rabbitmq.Client
}

func (s integrationStack) client(t *testing.T, prefix string) *brokerFixture {
	t.Helper()
	return s.connect(t, s.queue(prefix+".tasks"), true)
}

// process models another command process on the same queue: it has its own
// client, connection and channels, so stopping one never affects the other.
func (s integrationStack) process(t *testing.T, of *brokerFixture) *brokerFixture {
	t.Helper()
	return s.connect(t, of.broker.Queue(), false)
}

func (s integrationStack) connect(t *testing.T, queue string, ownsQueue bool) *brokerFixture {
	t.Helper()
	resources := configs.NewRabbitMQResourcesManager(rabbitmq.Config{URL: s.brokerURL, Queue: queue, DialTimeout: time.Second})
	if err := resources.Initialization(s.ctx); err != nil {
		t.Fatalf("initialize broker %s: %v", queue, err)
	}
	if ownsQueue {
		s.cleanupQueue(t, queue)
	}
	t.Cleanup(func() { _ = resources.Close() })
	conn, err := resources.Client.Connection()
	if err != nil {
		t.Fatal(err)
	}
	// The fixture owns the connection; role adapters only operate their channels.
	stopConnection := rabbitmq.CloseConnectionOnCancel(s.ctx, conn)
	t.Cleanup(stopConnection)
	publishChannel, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	taskQueue := rabbitmq.NewTaskQueue(publishChannel, resources.Client.Queue())
	if err := taskQueue.Initialization(); err != nil {
		t.Fatal(err)
	}
	return &brokerFixture{TaskQueue: taskQueue, broker: resources.Client}
}

// consumer opens a channel on the fixture's current connection and initializes
// one AMQP delivery adapter on it, as the consumer command does per session.
func (c *brokerFixture) consumer(t *testing.T, ctx context.Context, svc *usecase.TaskUseCase, work usecase.Work, retryDelays []time.Duration) *deliveryamqp.Consumer {
	t.Helper()
	conn, err := c.broker.Connection()
	if err != nil {
		t.Fatal(err)
	}
	channel, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	worker := deliveryamqp.NewConsumer(channel, c.broker.Queue(), svc.Parallelism, svc, work, retryDelays)
	if err := worker.Initialization(); err != nil {
		t.Fatal(err)
	}
	return worker
}

// restartConnection models a stopped command releasing its connection. The
// replacement then initializes its own consumer before consuming deliveries.
func (c *brokerFixture) restartConnection(t *testing.T, ctx context.Context) {
	t.Helper()
	if err := c.broker.Close(); err != nil {
		t.Fatal(err)
	}
	conn, err := c.broker.Connection()
	if err != nil {
		t.Fatal(err)
	}
	stopConnection := rabbitmq.CloseConnectionOnCancel(ctx, conn)
	t.Cleanup(stopConnection)
}

func (s integrationStack) queue(name string) string { return name + "." + s.queueSuffix }

func (s integrationStack) cleanupQueue(t *testing.T, queue string) {
	t.Helper()
	t.Cleanup(func() {
		conn, err := amqp.DialConfig(s.brokerURL, amqp.Config{Dial: amqp.DefaultDial(time.Second)})
		if err != nil {
			t.Errorf("queue cleanup dial: %v", err)
			return
		}
		defer conn.CloseDeadline(time.Now().Add(time.Second))
		// Bound all AMQP RPCs, including a blocked QueueDelete or Channel.Close.
		timer := time.AfterFunc(2*time.Second, func() { _ = conn.CloseDeadline(time.Now()) })
		defer timer.Stop()
		ch, err := conn.Channel()
		if err == nil {
			_, err = ch.QueueDelete(queue, false, false, false)
		}
		if err != nil {
			t.Errorf("delete test queue %s: %v", queue, err)
		}
	})
}
func submitTask(t *testing.T, ctx context.Context, svc *usecase.TaskUseCase, raw string) *domain.Task {
	t.Helper()
	v, err := svc.Submit(ctx, "", []byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func waitForStatus(t *testing.T, parent context.Context, svc *usecase.TaskUseCase, id domain.ID, want domain.Status) *domain.Task {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		v, err := svc.Get(ctx, string(id))
		if err != nil {
			t.Fatal(err)
		}
		if v.Status == want {
			return v
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("task %s did not reach %s (status %s): %v", id, want, v.Status, ctx.Err())
		}
	}
}

type runningConsumer struct {
	cancel   context.CancelFunc
	done     chan struct{}
	stopOnce sync.Once
	err      error
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
		return errors.New("consumer did not stop")
	}
}
func inspectQueue(s integrationStack, queue string) int {
	conn, err := amqp.DialConfig(s.brokerURL, amqp.Config{Dial: amqp.DefaultDial(time.Second)})
	if err != nil {
		return -1
	}
	defer conn.CloseDeadline(time.Now())
	timer := time.AfterFunc(2*time.Second, func() { _ = conn.CloseDeadline(time.Now()) })
	defer timer.Stop()
	ch, err := conn.Channel()
	if err != nil {
		return -1
	}
	info, err := ch.QueueInspect(queue)
	if err != nil {
		return -1
	}
	return info.Messages
}
func assertQueueCount(t *testing.T, ctx context.Context, s integrationStack, queue string, want int) {
	t.Helper()
	if got := inspectQueue(s, queue); got != want {
		t.Fatalf("queue %s messages=%d, want %d", queue, got, want)
	}
}
func waitForQueueDrained(t *testing.T, parent context.Context, s integrationStack, queue string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, time.Second)
	defer cancel()
	// Ready counts exclude unacked deliveries. Allow the batch's ACK to reach
	// the broker, then callers close the owner connection and verify no requeue.
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	stable := 0
	for {
		if inspectQueue(s, queue) == 0 {
			stable++
			if stable == 3 {
				return
			}
		} else {
			stable = 0
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("queue %s did not drain: %v", queue, ctx.Err())
		}
	}
}

// startBatchConsumer loops the AMQP delivery adapter's ConsumeBatch. settled,
// when set, runs after each batch was processed and acknowledged.
func startBatchConsumer(t *testing.T, parent context.Context, client *brokerFixture, svc *usecase.TaskUseCase, work usecase.Work, retryDelays []time.Duration, settled func(context.Context) error) *runningConsumer {
	t.Helper()
	worker := client.consumer(t, parent, svc, work, retryDelays)
	return startConsumer(t, parent, client, func(ctx context.Context) error {
		if err := worker.ConsumeBatch(ctx); err != nil || settled == nil {
			return err
		}
		return settled(ctx)
	})
}
func startConsumer(t *testing.T, parent context.Context, client *brokerFixture, consume func(context.Context) error) *runningConsumer {
	t.Helper()
	ctx, cancel := context.WithCancel(parent)
	r := &runningConsumer{cancel: cancel, done: make(chan struct{})}
	go func() {
		conn, err := client.broker.Connection()
		if err != nil {
			r.err = err
			close(r.done)
			return
		}
		stopConnection := rabbitmq.CloseConnectionOnCancel(ctx, conn)
		for ctx.Err() == nil {
			if err := consume(ctx); err != nil {
				r.err = err
				break
			}
		}
		if r.err == nil {
			r.err = ctx.Err()
		}
		stopConnection()
		close(r.done)
	}()
	t.Cleanup(func() { _ = r.Stop() })
	return r
}

// sweeperOnly fails the Submit fast path, leaving new rows unsubmitted for the
// PublishPending sweeper or for tests that never publish.
var sweeperOnly usecase.TaskQueue = integrationPublisherFunc(func(context.Context, domain.ID) error {
	return errors.New("fast path disabled")
})

func newPublishingTaskUseCase(s integrationStack, client *brokerFixture, batch int) *usecase.TaskUseCase {
	svc := usecase.NewTaskUseCase(s.repo, client.TaskQueue, time.Now)
	svc.SweeperBatch = batch
	svc.RepublishAfter = republishAfter
	svc.StaleAfter = staleAfter
	svc.RetryDelays = retryDelays
	return svc
}
