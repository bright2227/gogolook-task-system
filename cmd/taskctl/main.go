// Command taskctl provides bounded, audited DLQ inspection and replay from a worker pod.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/bright2227/gogolook-task-system/internal/configs"
	"github.com/bright2227/gogolook-task-system/internal/delivery/rabbitmq"
	"github.com/bright2227/gogolook-task-system/internal/domain"
	"github.com/bright2227/gogolook-task-system/internal/repository/task/postgres"
	"github.com/bright2227/gogolook-task-system/internal/usecase"
)

const advisoryLockKey = "gogolook-task-system:taskctl:dlq"

type command struct {
	inspectLimit int
	replay       bool
	messageID    string
	operator     string
	reason       string
	scanLimit    int
	timeout      time.Duration
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, "taskctl:", err)
		os.Exit(1)
	}
}

func run(parent context.Context, args []string, stdout, stderr io.Writer) error {
	cmd, err := parseCommand(args, stderr)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, cmd.timeout)
	defer cancel()
	cfg, err := configs.Load()
	if err != nil {
		return fmt.Errorf("load worker configuration: %w", err)
	}
	store, err := postgres.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("connect to PostgreSQL: %w", err)
	}
	defer store.Close()
	broker := rabbitmq.New(cfg.Rabbit)
	defer broker.Close()
	unlock, err := acquireDLQLock(ctx, store.DB)
	if err != nil {
		return err
	}
	defer unlock()
	if cmd.replay {
		return replay(ctx, cmd, store, broker, stdout)
	}
	return inspect(ctx, cmd, broker, stdout)
}

func parseCommand(args []string, stderr io.Writer) (command, error) {
	if len(args) < 2 || args[0] != "dlq" {
		return command{}, errors.New("usage: taskctl dlq inspect|replay [flags]")
	}
	switch args[1] {
	case "inspect":
		fs := flag.NewFlagSet("taskctl dlq inspect", flag.ContinueOnError)
		fs.SetOutput(stderr)
		limit := fs.Int("limit", 20, "maximum messages to inspect (1-1000)")
		timeout := fs.Duration("timeout", 30*time.Second, "command deadline")
		if err := fs.Parse(args[2:]); err != nil {
			return command{}, err
		}
		if fs.NArg() != 0 || *limit < 1 || *limit > rabbitmq.MaxDeadLetterScan || *timeout <= 0 {
			return command{}, errors.New("inspect requires no positional arguments, --limit 1..1000, and a positive --timeout")
		}
		return command{inspectLimit: *limit, scanLimit: *limit, timeout: *timeout}, nil
	case "replay":
		fs := flag.NewFlagSet("taskctl dlq replay", flag.ContinueOnError)
		fs.SetOutput(stderr)
		messageID := fs.String("message-id", "", "stable DLQ message UUID")
		operator := fs.String("operator", "", "operator identity for the audit record")
		reason := fs.String("reason", "", "reason for the manual replay")
		scanLimit := fs.Int("scan-limit", rabbitmq.MaxDeadLetterScan, "maximum DLQ messages to scan (1-1000)")
		timeout := fs.Duration("timeout", 30*time.Second, "command deadline")
		if err := fs.Parse(args[2:]); err != nil {
			return command{}, err
		}
		if fs.NArg() != 0 || *scanLimit < 1 || *scanLimit > rabbitmq.MaxDeadLetterScan || *timeout <= 0 {
			return command{}, errors.New("replay requires no positional arguments, --scan-limit 1..1000, and a positive --timeout")
		}
		id, err := domain.ParseID(*messageID)
		if err != nil {
			return command{}, errors.New("replay requires --message-id with a valid UUID")
		}
		if strings.TrimSpace(*operator) == "" || len(*operator) > 128 {
			return command{}, errors.New("replay requires --operator with 1-128 bytes")
		}
		if strings.TrimSpace(*reason) == "" || len(*reason) > 512 {
			return command{}, errors.New("replay requires --reason with 1-512 bytes")
		}
		return command{
			replay: true, messageID: string(id), operator: strings.TrimSpace(*operator),
			reason: strings.TrimSpace(*reason), scanLimit: *scanLimit, timeout: *timeout,
		}, nil
	default:
		return command{}, errors.New("usage: taskctl dlq inspect|replay [flags]")
	}
}

func inspect(ctx context.Context, cmd command, broker *rabbitmq.Client, stdout io.Writer) error {
	enc := json.NewEncoder(stdout)
	enc.SetEscapeHTML(false)
	result, err := broker.ScanDeadLetters(ctx, cmd.inspectLimit, "", func(item rabbitmq.DeadLetterItem) error {
		view := map[string]any{
			"message_id": item.MessageID, "redelivered": item.Redelivered,
			"routing_key": item.RoutingKey,
		}
		failure, parseErr := rabbitmq.ParseFailureEnvelope(item.Body)
		if parseErr == nil {
			view["version"] = failure.Version
			view["failed_at"] = failure.FailedAt
			view["task_id"] = failure.TaskID
			view["event_id"] = failure.EventID
			view["execution_id"] = failure.ExecutionID
			original := failure.Failure.Original
			if len(original) > 256 {
				original = original[:256]
			}
			view["failure"] = map[string]any{
				"category": failure.Failure.Category, "code": failure.Failure.Code,
				"detail": failure.Failure.Detail, "worker_id": failure.Failure.WorkerID,
				"retry_count": failure.Failure.RetryCount, "replay_cycle": failure.Failure.ReplayCycle,
				"attempts": failure.Failure.Attempts, "failed_at": failure.Failure.FailedAt,
				"source_event_id":    failure.Failure.SourceEventID,
				"original_hash":      failure.Failure.OriginalHash,
				"original_truncated": failure.Failure.OriginalTruncated,
				"original_preview":   string(original),
			}
		} else {
			preview := item.Body
			if len(preview) > 256 {
				preview = preview[:256]
			}
			view["format_error"] = parseErr.Error()
			view["body_preview"] = string(preview)
		}
		return enc.Encode(view)
	})
	if err != nil {
		return fmt.Errorf("inspect DLQ: %w", err)
	}
	return enc.Encode(map[string]any{
		"scanned": result.Scanned, "partial": result.Partial,
		"note": "basic.get inspection preserves messages but can change queue order and redelivery flags",
	})
}

func replay(ctx context.Context, cmd command, store *postgres.Store, broker *rabbitmq.Client, stdout io.Writer) error {
	service := usecase.Tasks{Store: store, Clock: time.Now}
	var response usecase.ReplayResponse
	result, err := broker.ScanDeadLetters(ctx, cmd.scanLimit, cmd.messageID, func(item rabbitmq.DeadLetterItem) error {
		failure, err := rabbitmq.ParseFailureEnvelope(item.Body)
		if err != nil {
			return fmt.Errorf("message %s is not a replayable failure envelope: %w", item.MessageID, err)
		}
		if failure.MessageID != cmd.messageID || item.MessageID != cmd.messageID {
			return errors.New("DLQ message ID does not match the requested replay ID")
		}
		if failure.Failure.Category == "unknown_task" || failure.Failure.Category == "malformed_envelope" || failure.TaskID == "" || failure.ExecutionID == "" || failure.EventID == "" {
			return errors.New("malformed or unknown-task DLQ messages are inspect-only; no repair command is provided")
		}
		response, err = service.Replay(ctx, usecase.ReplayRequest{
			MessageID: cmd.messageID, Operator: cmd.operator, Reason: cmd.reason,
		})
		return err
	})
	if err != nil {
		return fmt.Errorf("replay DLQ message: %w", err)
	}
	if !result.Found {
		if result.Partial {
			return fmt.Errorf("message %s was not found in the scanned prefix of %d messages; increase --scan-limit up to %d", cmd.messageID, result.Scanned, rabbitmq.MaxDeadLetterScan)
		}
		return fmt.Errorf("message %s was not found in the DLQ", cmd.messageID)
	}
	return json.NewEncoder(stdout).Encode(map[string]any{
		"message_id": cmd.messageID, "task_id": response.TaskID,
		"execution_id": response.ExecutionID, "already_replayed": response.AlreadyReplayed,
		"acknowledged": true,
	})
}

func acquireDLQLock(ctx context.Context, db *sql.DB) (func(), error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("open advisory-lock connection: %w", err)
	}
	var acquired bool
	if err = conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock(hashtextextended($1, 0))`, advisoryLockKey).Scan(&acquired); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("acquire DLQ advisory lock: %w", err)
	}
	if !acquired {
		_ = conn.Close()
		return nil, errors.New("another taskctl DLQ operation is already running")
	}
	return func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		var unlocked bool
		_ = conn.QueryRowContext(unlockCtx, `SELECT pg_advisory_unlock(hashtextextended($1, 0))`, advisoryLockKey).Scan(&unlocked)
		_ = conn.Close()
	}, nil
}
