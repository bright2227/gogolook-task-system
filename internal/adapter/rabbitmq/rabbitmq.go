// Package rabbitmq implements durable task delivery over RabbitMQ.
package rabbitmq

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/bright2227/vsx-task-system/internal/app"
	"github.com/bright2227/vsx-task-system/internal/task"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	DefaultQueue      = "vsx.tasks"
	DefaultRetryQueue = "vsx.tasks.retry"
	DefaultDeadQueue  = "vsx.tasks.dlq"
)

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
	if c.RetryDelay <= 0 || c.RetryDelay.Milliseconds() > 2147483647 {
		c.RetryDelay = 2 * time.Second
	}
	if c.DialTimeout <= 0 {
		c.DialTimeout = 3 * time.Second
	}
	return c
}

type envelope struct {
	EventID task.ID `json:"event_id"`
	TaskID  task.ID `json:"task_id"`
}

func message(m app.Message) ([]byte, error) {
	if _, err := task.ParseID(string(m.TaskID)); err != nil {
		return nil, fmt.Errorf("invalid task ID: %w", err)
	}
	if m.EventID != "" {
		if _, err := task.ParseID(string(m.EventID)); err != nil {
			return nil, fmt.Errorf("invalid event ID: %w", err)
		}
	}
	return json.Marshal(envelope{m.EventID, m.TaskID})
}

func decode(body []byte) (app.Message, error) {
	if len(body) > 1024 {
		return app.Message{}, errors.New("task envelope exceeds 1024 bytes")
	}
	var e envelope
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if err := d.Decode(&e); err != nil {
		return app.Message{}, err
	}
	var extra any
	if err := d.Decode(&extra); !errors.Is(err, io.EOF) {
		return app.Message{}, errors.New("expected one JSON value")
	}
	taskID, err := task.ParseID(string(e.TaskID))
	if err != nil {
		return app.Message{}, err
	}
	eventID := task.ID("")
	if e.EventID != "" {
		eventID, err = task.ParseID(string(e.EventID))
		if err != nil {
			return app.Message{}, err
		}
	}
	return app.Message{TaskID: taskID, EventID: eventID}, nil
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
	return conn, err
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
	if _, err := ch.QueueDeclare(c.cfg.RetryQueue, true, false, false, false, amqp.Table{
		"x-queue-type":              "quorum",
		"x-message-ttl":             int32(c.cfg.RetryDelay.Milliseconds()),
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": c.cfg.Queue,
		"x-dead-letter-strategy":    "at-least-once",
		"x-overflow":                "reject-publish",
	}); err != nil {
		return err
	}
	_, err := ch.QueueDeclare(c.cfg.DeadQueue, true, false, false, false, nil)
	return err
}

// Publish is synchronous: it succeeds only after a broker confirm and no
// mandatory return.
func (c *Client) Publish(ctx context.Context, m app.Message) error {
	return c.publishTo(ctx, c.cfg.Queue, m)
}
func (c *Client) Retry(ctx context.Context, m app.Message) error {
	return c.publishTo(ctx, c.cfg.RetryQueue, m)
}
func (c *Client) DeadLetter(ctx context.Context, body []byte) error {
	return c.publishRaw(ctx, c.cfg.DeadQueue, body)
}

func (c *Client) publishTo(ctx context.Context, queue string, m app.Message) error {
	body, err := message(m)
	if err != nil {
		return err
	}
	return c.publishRaw(ctx, queue, body)
}
func (c *Client) publishRaw(ctx context.Context, queue string, body []byte) error {
	var last error
	for attempt := 0; attempt < 3; attempt++ {
		pc, err := c.acquirePublisher(ctx)
		if err != nil {
			return err
		}
		err = c.publishOn(ctx, pc, queue, body)
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

func (c *Client) publishOn(ctx context.Context, pc *publishChannel, queue string, body []byte) error {
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
	if err := pc.ch.PublishWithContext(ctx, "", queue, true, false, amqp.Publishing{
		ContentType: "application/json", DeliveryMode: amqp.Persistent,
		Timestamp: time.Now().UTC(), Body: append([]byte(nil), body...),
	}); err != nil {
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

type Handler func(context.Context, task.ID) error

// Consume reconnects until ctx is canceled. Each channel uses bounded prefetch
// and a fixed worker count. A failed retry/DLQ publish leaves the delivery
// unacknowledged so closing the channel returns it to the durable source queue.
func (c *Client) Consume(ctx context.Context, workers int, handler Handler) error {
	if workers < 1 {
		return errors.New("worker count must be positive")
	}
	for ctx.Err() == nil {
		err := c.consumeSession(ctx, workers, handler)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			slog.Warn("rabbitmq consumer disconnected; reconnecting", "error", err)
		}
		if !wait(ctx, time.Second) {
			return ctx.Err()
		}
	}
	return ctx.Err()
}

func (c *Client) consumeSession(ctx context.Context, workers int, handler Handler) error {
	conn, err := c.connection()
	if err != nil {
		return err
	}
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	if err = c.topology(ch); err != nil {
		return err
	}
	if err = ch.Qos(workers, 0, false); err != nil {
		return err
	}
	consumerTag := "vsx-worker-" + string(task.NewID())
	deliveries, err := ch.Consume(c.cfg.Queue, consumerTag, false, false, false, false, nil)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for d := range deliveries {
				if ctx.Err() != nil {
					continue
				}
				c.handle(ctx, ch, d, handler)
			}
		}()
	}
	workersDone := make(chan struct{})
	go func() { wg.Wait(); close(workersDone) }()
	select {
	case <-ctx.Done():
		_ = ch.Cancel(consumerTag, false)
		<-workersDone
		return ctx.Err()
	case <-workersDone:
		return errors.New("rabbitmq consumer channel closed")
	}
}

func (c *Client) handle(ctx context.Context, ch *amqp.Channel, d amqp.Delivery, handler Handler) {
	m, err := decode(d.Body)
	if err != nil {
		if publishErr := c.DeadLetter(ctx, d.Body); publishErr != nil {
			slog.Error("malformed task delivery could not be dead-lettered", "error", publishErr)
			_ = ch.Close()
			return
		}
		if ackErr := d.Ack(false); ackErr != nil {
			slog.Error("dead-lettered delivery ack failed", "error", ackErr)
		}
		slog.Warn("task delivery dead-lettered", "reason", "malformed", "error", err)
		return
	}
	err = handler(ctx, m.TaskID)
	if err == nil {
		if ackErr := d.Ack(false); ackErr != nil {
			slog.Error("processed task delivery ack failed", "task_id", m.TaskID, "error", ackErr)
			return
		}
		slog.Info("task delivery acknowledged", "task_id", m.TaskID, "event_id", m.EventID)
		return
	}
	if errors.Is(err, app.ErrNotFound) {
		if publishErr := c.DeadLetter(ctx, d.Body); publishErr != nil {
			slog.Error("unknown task could not be dead-lettered", "task_id", m.TaskID, "event_id", m.EventID, "error", publishErr)
			_ = ch.Close()
			return
		}
		if ackErr := d.Ack(false); ackErr != nil {
			slog.Error("dead-lettered unknown task ack failed", "task_id", m.TaskID, "event_id", m.EventID, "error", ackErr)
		}
		slog.Warn("task delivery dead-lettered", "task_id", m.TaskID, "event_id", m.EventID, "reason", "unknown_task")
		return
	}
	if publishErr := c.Retry(ctx, m); publishErr == nil {
		if ackErr := d.Ack(false); ackErr != nil {
			slog.Error("delayed task delivery ack failed", "task_id", m.TaskID, "event_id", m.EventID, "error", ackErr)
			return
		}
		slog.Warn("task delivery scheduled for retry", "task_id", m.TaskID, "event_id", m.EventID, "error", err)
	} else {
		slog.Error("task delivery retry publish failed", "task_id", m.TaskID, "event_id", m.EventID, "error", err, "publish_error", publishErr)
		_ = ch.Close()
	}
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
