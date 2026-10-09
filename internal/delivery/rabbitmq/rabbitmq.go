// Package rabbitmq implements durable task delivery over RabbitMQ.
package rabbitmq

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/bright2227/gogolook-task-system/internal/domain"
	"github.com/bright2227/gogolook-task-system/internal/usecase"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	DefaultQueue           = "gogolook.tasks"
	DefaultRetryQueue      = "gogolook.tasks.retry"
	DefaultDeadQueue       = "gogolook.tasks.dlq"
	WorkEnvelopeVersion    = 1
	FailureEnvelopeVersion = 1
	MaxDeadLetterOriginal  = 1 << 20
	MaxDeadLetterScan      = 1000
)

const maxFailureDetail = 512

type Config struct {
	URL                          string
	Queue, RetryQueue, DeadQueue string
	RetryDelay                   time.Duration
	DialTimeout                  time.Duration
}

func (c Config) normalized() Config {
	if c.Queue == "" {
		c.Queue = DefaultQueue
	}
	if c.RetryQueue == "" {
		c.RetryQueue = DefaultRetryQueue
	}
	if c.DeadQueue == "" {
		c.DeadQueue = DefaultDeadQueue
	}
	if c.RetryDelay < time.Millisecond || c.RetryDelay.Milliseconds() > 2147483647 {
		c.RetryDelay = 2 * time.Second
	}
	if c.DialTimeout <= 0 {
		c.DialTimeout = 3 * time.Second
	}
	return c
}

type envelope struct {
	Version     int       `json:"version"`
	EventID     domain.ID `json:"event_id"`
	TaskID      domain.ID `json:"task_id"`
	ExecutionID domain.ID `json:"execution_id"`
}

// FailureEnvelope is the durable, inspectable DLQ wire format.
type FailureEnvelope struct {
	Version     int                   `json:"version"`
	MessageID   string                `json:"message_id"`
	EventID     domain.ID             `json:"event_id,omitempty"`
	TaskID      domain.ID             `json:"task_id,omitempty"`
	ExecutionID domain.ID             `json:"execution_id,omitempty"`
	FailedAt    time.Time             `json:"failed_at"`
	Failure     usecase.FailureRecord `json:"failure"`
}

// DeadLetterItem is a single unacknowledged message read during a bounded DLQ scan.
type DeadLetterItem struct {
	MessageID   string
	Body        []byte
	Redelivered bool
	RoutingKey  string
	Headers     amqp.Table
}

type DLQScanResult struct {
	Scanned int
	Found   bool
	Partial bool
}

func message(m usecase.Message) ([]byte, error) {
	eventID, err := domain.ParseID(string(m.EventID))
	if err != nil {
		return nil, fmt.Errorf("invalid event ID: %w", err)
	}
	taskID, err := domain.ParseID(string(m.TaskID))
	if err != nil {
		return nil, fmt.Errorf("invalid task ID: %w", err)
	}
	executionID, err := domain.ParseID(string(m.ExecutionID))
	if err != nil {
		return nil, fmt.Errorf("invalid execution ID: %w", err)
	}
	return json.Marshal(envelope{
		Version: WorkEnvelopeVersion, EventID: eventID, TaskID: taskID, ExecutionID: executionID,
	})
}

func decode(body []byte) (usecase.Message, error) {
	if len(body) > 1024 {
		return usecase.Message{}, errors.New("task envelope exceeds 1024 bytes")
	}
	var e envelope
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if err := d.Decode(&e); err != nil {
		return usecase.Message{}, err
	}
	var extra any
	if err := d.Decode(&extra); !errors.Is(err, io.EOF) {
		return usecase.Message{}, errors.New("expected one JSON value")
	}
	if e.Version != WorkEnvelopeVersion {
		return usecase.Message{}, fmt.Errorf("unsupported task envelope version %d", e.Version)
	}
	taskID, err := domain.ParseID(string(e.TaskID))
	if err != nil {
		return usecase.Message{}, fmt.Errorf("invalid task ID: %w", err)
	}
	eventID, err := domain.ParseID(string(e.EventID))
	if err != nil {
		return usecase.Message{}, fmt.Errorf("invalid event ID: %w", err)
	}
	executionID, err := domain.ParseID(string(e.ExecutionID))
	if err != nil {
		return usecase.Message{}, fmt.Errorf("invalid execution ID: %w", err)
	}
	return usecase.Message{TaskID: taskID, EventID: eventID, ExecutionID: executionID}, nil
}

// ParseFailureEnvelope validates a versioned DLQ message before inspection or replay.
func ParseFailureEnvelope(body []byte) (FailureEnvelope, error) {
	if len(body) > MaxDeadLetterOriginal*2 {
		return FailureEnvelope{}, errors.New("dead-letter envelope exceeds size limit")
	}
	var e FailureEnvelope
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if err := d.Decode(&e); err != nil {
		return FailureEnvelope{}, err
	}
	var extra any
	if err := d.Decode(&extra); !errors.Is(err, io.EOF) {
		return FailureEnvelope{}, errors.New("expected one JSON value")
	}
	if e.Version != FailureEnvelopeVersion {
		return FailureEnvelope{}, fmt.Errorf("unsupported dead-letter envelope version %d", e.Version)
	}
	messageID, err := domain.ParseID(e.MessageID)
	if err != nil {
		return FailureEnvelope{}, fmt.Errorf("invalid dead-letter message ID: %w", err)
	}
	e.MessageID = string(messageID)
	if e.EventID != "" {
		e.EventID, err = domain.ParseID(string(e.EventID))
		if err != nil {
			return FailureEnvelope{}, fmt.Errorf("invalid event ID: %w", err)
		}
	}
	if e.TaskID != "" {
		e.TaskID, err = domain.ParseID(string(e.TaskID))
		if err != nil {
			return FailureEnvelope{}, fmt.Errorf("invalid task ID: %w", err)
		}
	}
	if e.ExecutionID != "" {
		e.ExecutionID, err = domain.ParseID(string(e.ExecutionID))
		if err != nil {
			return FailureEnvelope{}, fmt.Errorf("invalid execution ID: %w", err)
		}
	}
	if e.FailedAt.IsZero() || e.Failure.Category == "" || e.Failure.Code == "" {
		return FailureEnvelope{}, errors.New("dead-letter envelope is missing failure metadata")
	}
	return e, nil
}

type Client struct {
	cfg       Config
	mu        sync.Mutex
	conn      *amqp.Connection
	poolMu    sync.Mutex
	pool      chan *publishChannel
	poolCount int
}

const (
	maxPublishChannels = 8
	confirmTimeout     = 5 * time.Second
)

type publishChannel struct {
	mu       sync.Mutex
	ch       *amqp.Channel
	confirms <-chan amqp.Confirmation
	returns  <-chan amqp.Return
}

func New(c Config) *Client {
	return &Client{cfg: c.normalized(), pool: make(chan *publishChannel, maxPublishChannels)}
}

func (c *Client) dial() (*amqp.Connection, error) {
	conn, _, err := c.dialWithRaw()
	return conn, err
}

func (c *Client) dialWithRaw() (*amqp.Connection, net.Conn, error) {
	var raw net.Conn
	conn, err := amqp.DialConfig(c.cfg.URL, amqp.Config{Heartbeat: 10 * time.Second, Dial: func(network, address string) (net.Conn, error) {
		dialer := net.Dialer{Timeout: c.cfg.DialTimeout}
		connected, dialErr := dialer.Dial(network, address)
		if dialErr != nil {
			return nil, dialErr
		}
		if dialErr = connected.SetDeadline(time.Now().Add(c.cfg.DialTimeout)); dialErr != nil {
			_ = connected.Close()
			return nil, dialErr
		}
		raw = connected
		return &boundedWriteConn{Conn: connected, timeout: c.cfg.DialTimeout}, nil
	}})
	if raw != nil {
		_ = raw.SetDeadline(time.Time{})
	}
	return conn, raw, err
}

type boundedWriteConn struct {
	net.Conn
	timeout time.Duration
}

func (c *boundedWriteConn) Write(p []byte) (int, error) {
	if err := c.Conn.SetWriteDeadline(time.Now().Add(c.timeout)); err != nil {
		return 0, err
	}
	return c.Conn.Write(p)
}

func (c *Client) connection() (*amqp.Connection, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil && !c.conn.IsClosed() {
		return c.conn, nil
	}
	conn, err := c.dial()
	if err != nil {
		return nil, err
	}
	c.conn = conn
	return conn, nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	return err
}

func (c *Client) topology(ch *amqp.Channel) error {
	if _, err := ch.QueueDeclare(c.cfg.Queue, true, false, false, false, nil); err != nil {
		return err
	}
	_, err := ch.QueueDeclare(c.cfg.DeadQueue, true, false, false, false, nil)
	return err
}

// Publish is synchronous: it succeeds only after a broker confirm and no
// mandatory return.
func (c *Client) Publish(ctx context.Context, m usecase.Message) error {
	switch m.Destination {
	case usecase.DestinationWork:
		return c.publishTo(ctx, c.cfg.Queue, m)
	case usecase.DestinationDeadLetter:
		return c.publishFailure(ctx, m)
	default:
		return fmt.Errorf("unsupported message destination %q", m.Destination)
	}
}

func (c *Client) Retry(ctx context.Context, m usecase.Message) error {
	// Retry timing now lives in PostgreSQL outbox.available_at. Keep this
	// compatibility method on the work queue so it cannot add a second TTL.
	return c.publishTo(ctx, c.cfg.Queue, m)
}

func (c *Client) DeadLetter(ctx context.Context, body []byte) error {
	return c.publishQuarantine(ctx, body, "operator", "manual dead-letter publication", nil, "")
}

func (c *Client) publishTo(ctx context.Context, queue string, m usecase.Message) error {
	body, err := message(m)
	if err != nil {
		return err
	}
	return c.publishRawMessage(ctx, queue, amqp.Publishing{
		ContentType: "application/json", DeliveryMode: amqp.Persistent,
		MessageId: string(m.EventID), Timestamp: time.Now().UTC(), Body: body,
	})
}

func (c *Client) publishFailure(ctx context.Context, m usecase.Message) error {
	e, err := failureEnvelope(m)
	if err != nil {
		return err
	}
	body, err := json.Marshal(e)
	if err != nil {
		return err
	}
	return c.publishRawMessage(ctx, c.cfg.DeadQueue, amqp.Publishing{
		ContentType: "application/json", DeliveryMode: amqp.Persistent,
		MessageId: e.MessageID, Timestamp: e.FailedAt, Body: body,
		Headers: amqp.Table{"x-gogolook-failure-category": e.Failure.Category, "x-gogolook-failure-code": e.Failure.Code},
	})
}

func failureEnvelope(m usecase.Message) (FailureEnvelope, error) {
	if m.Failure == nil {
		return FailureEnvelope{}, errors.New("dead-letter message requires failure metadata")
	}
	if m.Failure.Category == "" || m.Failure.Code == "" {
		return FailureEnvelope{}, errors.New("dead-letter failure requires category and code")
	}
	eventID, err := domain.ParseID(string(m.EventID))
	if err != nil {
		return FailureEnvelope{}, fmt.Errorf("invalid dead-letter event ID: %w", err)
	}
	taskID, err := domain.ParseID(string(m.TaskID))
	if err != nil {
		return FailureEnvelope{}, fmt.Errorf("invalid dead-letter task ID: %w", err)
	}
	executionID, err := domain.ParseID(string(m.ExecutionID))
	if err != nil {
		return FailureEnvelope{}, fmt.Errorf("invalid dead-letter execution ID: %w", err)
	}
	failedAt := m.Failure.FailedAt
	if failedAt.IsZero() {
		failedAt = m.AvailableAt
	}
	if failedAt.IsZero() {
		failedAt = time.Now().UTC()
	}
	e := FailureEnvelope{
		Version: FailureEnvelopeVersion, MessageID: string(eventID), EventID: eventID,
		TaskID: taskID, ExecutionID: executionID, FailedAt: failedAt,
		Failure: *m.Failure,
	}
	return e, nil
}

func (c *Client) publishRawMessage(ctx context.Context, queue string, publishing amqp.Publishing) error {
	var last error
	for attempt := 0; attempt < 3; attempt++ {
		pc, err := c.acquirePublisher(ctx)
		if err != nil {
			return err
		}
		err = c.publishOn(ctx, pc, queue, publishing)
		c.pool <- pc
		if err == nil {
			return nil
		}
		last = err
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempt < 2 {
			timer := time.NewTimer(time.Duration(attempt+1) * 200 * time.Millisecond)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			}
		}
	}
	return last
}

func (c *Client) publishQuarantine(ctx context.Context, original []byte, category, detail string, task *usecase.Message, sourceMessageID string) error {
	if category == "" {
		category = "malformed_envelope"
	}
	captured := original
	truncated := false
	if len(captured) > MaxDeadLetterOriginal {
		captured = captured[:MaxDeadLetterOriginal]
		truncated = true
	}
	hash := sha256.Sum256(original)
	workerID, _ := os.Hostname()
	messageID := sourceMessageID
	if id, err := domain.ParseID(messageID); err == nil {
		messageID = string(id)
	} else {
		messageID = string(domain.NewID())
	}
	failedAt := time.Now().UTC()
	e := FailureEnvelope{
		Version:   FailureEnvelopeVersion,
		MessageID: messageID,
		FailedAt:  failedAt,
		Failure: usecase.FailureRecord{
			Category: truncateUTF8(category, 96), Code: truncateUTF8(category, 96),
			Detail: truncateUTF8(detail, maxFailureDetail), WorkerID: workerID,
			FailedAt: failedAt,
			Original: append([]byte(nil), captured...), OriginalHash: hex.EncodeToString(hash[:]),
			OriginalTruncated: truncated,
		},
	}
	if task != nil {
		e.EventID, e.TaskID, e.ExecutionID = task.EventID, task.TaskID, task.ExecutionID
		e.Failure.SourceEventID = task.EventID
	}
	body, err := json.Marshal(e)
	if err != nil {
		return err
	}
	return c.publishRawMessage(ctx, c.cfg.DeadQueue, amqp.Publishing{
		ContentType: "application/json", DeliveryMode: amqp.Persistent, MessageId: messageID,
		Timestamp: e.FailedAt,
		Headers:   amqp.Table{"x-gogolook-failure-category": e.Failure.Category, "x-gogolook-failure-code": e.Failure.Code},
		Body:      body,
	})
}

func truncateUTF8(s string, max int) string {
	s = strings.ToValidUTF8(s, "�")
	if len(s) <= max {
		return s
	}
	for max > 0 && (s[max]&0xc0) == 0x80 {
		max--
	}
	return s[:max]
}

// ScanDeadLetters performs a bounded basic.get scan. It deliberately leaves
// inspected and unmatched deliveries unacknowledged; closing the channel requeues
// them. For a matching ID, visit runs before the only acknowledgement this method
// can issue, so callers can commit a replay receipt first.
func (c *Client) ScanDeadLetters(ctx context.Context, limit int, matchMessageID string, visit func(DeadLetterItem) error) (DLQScanResult, error) {
	if limit < 1 || limit > MaxDeadLetterScan {
		return DLQScanResult{}, fmt.Errorf("DLQ scan limit must be between 1 and %d", MaxDeadLetterScan)
	}
	if matchMessageID != "" {
		id, err := domain.ParseID(matchMessageID)
		if err != nil {
			return DLQScanResult{}, fmt.Errorf("invalid DLQ message ID: %w", err)
		}
		matchMessageID = string(id)
	}
	conn, raw, err := c.dialWithRaw()
	if err != nil {
		return DLQScanResult{}, err
	}
	defer conn.Close()
	stopOnCancel := context.AfterFunc(ctx, func() { _ = raw.Close() })
	defer stopOnCancel()
	ch, err := conn.Channel()
	if err != nil {
		return DLQScanResult{}, err
	}
	defer ch.Close()
	if _, err := ch.QueueDeclare(c.cfg.DeadQueue, true, false, false, false, nil); err != nil {
		return DLQScanResult{}, fmt.Errorf("declare DLQ for scan: %w", err)
	}
	result := DLQScanResult{}
	for result.Scanned < limit {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		d, ok, getErr := ch.Get(c.cfg.DeadQueue, false)
		if getErr != nil {
			return result, getErr
		}
		if !ok {
			return result, nil
		}
		result.Scanned++
		item := DeadLetterItem{
			MessageID: d.MessageId, Body: append([]byte(nil), d.Body...),
			Redelivered: d.Redelivered, RoutingKey: d.RoutingKey, Headers: d.Headers,
		}
		if matchMessageID == "" {
			if visit != nil {
				if err := visit(item); err != nil {
					return result, err
				}
			}
			continue
		}
		if item.MessageID != matchMessageID {
			continue
		}
		result.Found = true
		if visit == nil {
			return result, errors.New("DLQ replay requires a visitor")
		}
		if err := visit(item); err != nil {
			return result, err
		}
		if err := d.Ack(false); err != nil {
			return result, fmt.Errorf("acknowledge replayed DLQ message: %w", err)
		}
		return result, nil
	}
	if matchMessageID == "" || !result.Found {
		result.Partial = true
	}
	return result, nil
}

func (c *Client) acquirePublisher(ctx context.Context) (*publishChannel, error) {
	select {
	case pc := <-c.pool:
		return pc, nil
	default:
	}
	c.poolMu.Lock()
	if c.poolCount < maxPublishChannels {
		c.poolCount++
		c.poolMu.Unlock()
		return &publishChannel{}, nil
	}
	c.poolMu.Unlock()
	select {
	case pc := <-c.pool:
		return pc, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *Client) publishOn(ctx context.Context, pc *publishChannel, queue string, publishing amqp.Publishing) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if pc.ch == nil || pc.ch.IsClosed() {
		conn, err := c.connection()
		if err != nil {
			return err
		}
		ch, err := conn.Channel()
		if err != nil {
			return err
		}
		if err = c.topology(ch); err == nil {
			err = ch.Confirm(false)
		}
		if err != nil {
			_ = ch.Close()
			return err
		}
		pc.ch = ch
		pc.confirms = ch.NotifyPublish(make(chan amqp.Confirmation, 1))
		pc.returns = ch.NotifyReturn(make(chan amqp.Return, 1))
	}
	publishing.Body = append([]byte(nil), publishing.Body...)
	if publishing.ContentType == "" {
		publishing.ContentType = "application/json"
	}
	if publishing.DeliveryMode == 0 {
		publishing.DeliveryMode = amqp.Persistent
	}
	if publishing.Timestamp.IsZero() {
		publishing.Timestamp = time.Now().UTC()
	}
	if err := pc.ch.PublishWithContext(ctx, "", queue, true, false, publishing); err != nil {
		pc.invalidate()
		return err
	}
	timer := time.NewTimer(confirmTimeout)
	defer timer.Stop()
	select {
	case confirmation, ok := <-pc.confirms:
		if !ok {
			pc.invalidate()
			return errors.New("publisher confirm channel closed")
		}
		if !confirmation.Ack {
			pc.invalidate()
			return errors.New("publisher nack")
		}
		// The broker orders a mandatory return before this publication's confirm.
		select {
		case returned := <-pc.returns:
			pc.invalidate()
			return fmt.Errorf("message returned by broker: %s", returned.ReplyText)
		default:
		}
		return nil
	case <-ctx.Done():
		pc.invalidate()
		return ctx.Err()
	case <-timer.C:
		pc.invalidate()
		return errors.New("publisher confirm timed out")
	}
}

func (pc *publishChannel) invalidate() {
	if pc.ch != nil {
		_ = pc.ch.Close()
	}
	pc.ch = nil
	pc.confirms = nil
	pc.returns = nil
}

// Consume reconnects with bounded backoff until ctx is canceled. Each channel
// uses bounded prefetch and a fixed worker count. Uncommitted decisions remain
// unacknowledged so closing the channel returns them to the durable work queue.
func (c *Client) Consume(ctx context.Context, workers int, processor usecase.TaskProcessor) error {
	if workers < 1 {
		return errors.New("worker count must be positive")
	}
	backoff := 250 * time.Millisecond
	for ctx.Err() == nil {
		started := time.Now()
		err := c.consumeSession(ctx, workers, processor)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			slog.Warn("rabbitmq consumer disconnected; reconnecting", "error", err)
		}
		if !wait(ctx, jitter(backoff)) {
			return ctx.Err()
		}
		if time.Since(started) >= time.Minute {
			backoff = 250 * time.Millisecond
		} else if backoff < 30*time.Second {
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
	}
	return ctx.Err()
}

func jitter(d time.Duration) time.Duration {
	return d/2 + time.Duration(rand.Int64N(int64(d-d/2)))
}

func (c *Client) consumeSession(ctx context.Context, workers int, processor usecase.TaskProcessor) error {
	conn, err := c.connection()
	if err != nil {
		return err
	}
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	sessionCtx, cancelSession := context.WithCancel(ctx)
	var workersDone chan struct{}
	// This owner cleanup is installed as soon as the channel exists, so setup
	// failures also close it. Close before waiting to release blocked receivers.
	// In-flight Process calls keep the parent context: a broker/session failure
	// requeues unacked deliveries without canceling work already owned by a worker.
	// Parent shutdown still cancels those calls through ctx and leaves leases recoverable.
	defer func() {
		cancelSession()
		_ = ch.Close()
		if workersDone != nil {
			<-workersDone
		}
	}()
	if err = c.topology(ch); err != nil {
		return err
	}
	if err = ch.Qos(workers, 0, false); err != nil {
		return err
	}
	consumerTag := "gogolook-worker-" + string(domain.NewID())
	deliveries, err := ch.Consume(c.cfg.Queue, consumerTag, false, false, false, false, nil)
	if err != nil {
		return err
	}
	failed := make(chan error, 1)
	var wg sync.WaitGroup
	workersDone = make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-sessionCtx.Done():
					return
				case d, ok := <-deliveries:
					if !ok {
						select {
						case failed <- errors.New("rabbitmq consumer channel closed"):
						default:
						}
						return
					}
					if sessionCtx.Err() != nil {
						return
					}
					if handleErr := c.handle(ctx, d, processor); handleErr != nil {
						select {
						case failed <- handleErr:
						default:
						}
						return
					}
				}
			}
		}()
	}
	go func() { wg.Wait(); close(workersDone) }()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-failed:
		return err
	case <-workersDone:
		return errors.New("rabbitmq consumer channel closed")
	}
}

func (c *Client) handle(ctx context.Context, d amqp.Delivery, processor usecase.TaskProcessor) error {
	m, err := decode(d.Body)
	if err != nil {
		if publishErr := c.publishQuarantine(ctx, d.Body, "malformed_envelope", err.Error(), nil, d.MessageId); publishErr != nil {
			slog.Error("malformed task delivery could not be dead-lettered", "error", publishErr)
			return publishErr
		}
		if ackErr := d.Ack(false); ackErr != nil {
			slog.Error("dead-lettered delivery ack failed", "error", ackErr)
			return ackErr
		}
		slog.Warn("task delivery dead-lettered", "reason", "malformed", "error", err)
		return nil
	}
	err = processor.Process(ctx, usecase.ProcessTaskRequest{
		ID: string(m.TaskID), EventID: string(m.EventID), ExecutionID: string(m.ExecutionID),
		Original: append([]byte(nil), d.Body...),
	})
	if err == nil {
		if ackErr := d.Ack(false); ackErr != nil {
			slog.Error("processed task delivery ack failed", "task_id", m.TaskID, "error", ackErr)
			return ackErr
		}
		slog.Info("task delivery acknowledged", "task_id", m.TaskID, "event_id", m.EventID, "execution_id", m.ExecutionID)
		return nil
	}
	if errors.Is(err, usecase.ErrNotFound) {
		if publishErr := c.publishQuarantine(ctx, d.Body, "unknown_task", err.Error(), &m, d.MessageId); publishErr != nil {
			slog.Error("unknown task could not be dead-lettered", "task_id", m.TaskID, "event_id", m.EventID, "error", publishErr)
			return publishErr
		}
		if ackErr := d.Ack(false); ackErr != nil {
			slog.Error("dead-lettered unknown task ack failed", "task_id", m.TaskID, "event_id", m.EventID, "error", ackErr)
			return ackErr
		}
		slog.Warn("task delivery dead-lettered", "task_id", m.TaskID, "event_id", m.EventID, "reason", "unknown_task")
		return nil
	}
	slog.Error("task decision not committed; delivery remains unacknowledged",
		"task_id", m.TaskID, "event_id", m.EventID, "execution_id", m.ExecutionID, "error", err)
	return err
}

func wait(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
