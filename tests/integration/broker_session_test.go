//go:build integration

package integration

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/bright2227/gogolook-task-system/internal/domain"
	"github.com/bright2227/gogolook-task-system/internal/usecase"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Each role must restore its own topology. Running them together could hide a
// missing declaration in one command behind the other command's declaration.
func TestBrokerSessionsRestoreMissingTopology(t *testing.T) {
	for _, role := range []string{"sweeper", "consumer"} {
		t.Run(role, func(t *testing.T) {
			binary := buildService(t, role)
			s := newIntegrationStack(t)
			fixture := s.client(t, "integration.session."+role)
			conn, err := fixture.broker.Connection()
			if err != nil {
				t.Fatal(err)
			}
			// No method is blocked; this proxy is used to pause connections.
			proxy := newMethodBlockingProxy(t, s.brokerURL, 0, 0)
			process := startService(t, binary, s.dbURL, proxy.URL, fixture.broker.Queue())
			waitForService(t, s.ctx, process, "initial session", func() bool {
				log, _ := os.ReadFile(process.logPath)
				if !bytes.Contains(log, []byte("started")) {
					return false
				}
				if role == "consumer" {
					queue, err := sessionQueue(conn, fixture.broker.Queue())
					return err == nil && queue.Consumers == 1
				}
				return true
			})

			proxy.pause()
			control, err := conn.Channel()
			if err != nil {
				t.Fatal(err)
			}
			// Deleting this unique queue also removes its binding. The shared
			// exchange and all other test queues remain intact.
			if _, err := control.QueueDelete(fixture.broker.Queue(), false, false, false); err != nil {
				t.Fatal(err)
			}
			if err := control.Close(); err != nil {
				t.Fatal(err)
			}

			svc := usecase.NewTaskUseCase(s.repo, sweeperOnly, time.Now)
			created := submitTask(t, s.ctx, svc, "restored")
			if role == "sweeper" {
				// Make this persisted task eligible without waiting for the
				// initial-publication grace period.
				if _, err := s.db.ExecContext(s.ctx, `UPDATE tasks SET created_at=now()-interval '1 minute',updated_at=now()-interval '1 minute' WHERE id=$1`, created.ID); err != nil {
					t.Fatal(err)
				}
			}
			proxy.resume()

			if role == "sweeper" {
				waitForService(t, s.ctx, process, "publication after topology loss", func() bool {
					queue, err := sessionQueue(conn, fixture.broker.Queue())
					if err != nil || queue.Messages != 1 {
						return false
					}
					stored, err := s.repo.Get(s.ctx, created.ID)
					return err == nil && stored.IsSubmitted
				})
				channel, err := conn.Channel()
				if err != nil {
					t.Fatal(err)
				}
				defer channel.Close()
				message, ok, err := channel.Get(fixture.broker.Queue(), true)
				if err != nil || !ok || string(message.Body) != string(created.ID) {
					t.Fatalf("recovered publication: message=%q present=%v err=%v", message.Body, ok, err)
				}
			} else {
				waitForService(t, s.ctx, process, "consumer after topology loss", func() bool {
					queue, err := sessionQueue(conn, fixture.broker.Queue())
					return err == nil && queue.Consumers == 1
				})
				// Publish through the untouched fixture channel. A confirm
				// without a return proves the queue binding was restored too.
				publication, err := fixture.Enqueue(s.ctx, created.ID)
				if err != nil || publication.Err != nil {
					t.Fatalf("publication to recovered consumer: %+v err=%v", publication, err)
				}
				waitForService(t, s.ctx, process, "completion after topology loss", func() bool {
					stored, err := s.repo.Get(s.ctx, created.ID)
					return err == nil && stored.Status == domain.Completed && stored.Result != nil && *stored.Result == "8" && stored.Error == nil
				})
			}
			process.assertRunning(t)
		})
	}
}

func buildService(t *testing.T, role string) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), role)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "go", "build", "-race", "-o", binary, "./cmd/"+role)
	command.Dir = filepath.Join("..", "..")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v\n%s", role, err, output)
	}
	return binary
}

type serviceProcess struct {
	done    chan struct{}
	err     error
	logPath string
}

func startService(t *testing.T, binary, databaseURL, brokerURL, queue string) *serviceProcess {
	t.Helper()
	logPath := filepath.Join(t.TempDir(), "service.log")
	log, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(binary)
	command.Stdout, command.Stderr = log, log
	command.Env = append(os.Environ(),
		"DATABASE_URL="+databaseURL, "RABBITMQ_URL="+brokerURL, "TASK_QUEUE="+queue,
		"CONSUMERS=1", "PROCESS_TIMEOUT=5s", "TASK_STALE_AFTER=20s",
		"SWEEPER_INTERVAL=100ms", "TASK_RETRY_DELAYS=none", "TASK_REPUBLISH_AFTER=0")
	if err := command.Start(); err != nil {
		_ = log.Close()
		t.Fatal(err)
	}
	process := &serviceProcess{done: make(chan struct{}), logPath: logPath}
	go func() {
		process.err = command.Wait()
		close(process.done)
	}()
	t.Cleanup(func() {
		defer log.Close()
		_ = command.Process.Signal(syscall.SIGTERM)
		select {
		case <-process.done:
		case <-time.After(5 * time.Second):
			_ = command.Process.Kill()
			<-process.done
			t.Errorf("service did not stop after SIGTERM")
		}
		if process.err != nil {
			output, _ := os.ReadFile(process.logPath)
			t.Errorf("service exit: %v\n%s", process.err, output)
		}
	})
	return process
}

func (p *serviceProcess) assertRunning(t *testing.T) {
	t.Helper()
	select {
	case <-p.done:
		log, _ := os.ReadFile(p.logPath)
		t.Fatalf("service exited: %v\n%s", p.err, log)
	default:
	}
}

func waitForService(t *testing.T, parent context.Context, process *serviceProcess, phase string, ready func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		process.assertRunning(t)
		if ready() {
			return
		}
		select {
		case <-process.done:
			process.assertRunning(t)
		case <-ticker.C:
		case <-ctx.Done():
			log, _ := os.ReadFile(process.logPath)
			t.Fatalf("timed out waiting for %s: %v\n%s", phase, ctx.Err(), log)
		}
	}
}

func sessionQueue(conn *amqp.Connection, queue string) (amqp.Queue, error) {
	channel, err := conn.Channel()
	if err != nil {
		return amqp.Queue{}, err
	}
	defer channel.Close()
	return channel.QueueInspect(queue)
}
