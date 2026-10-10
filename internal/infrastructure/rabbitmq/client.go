// Package rabbitmq provides broker connections, channel topology declarations
// and the outbound task queue adapter.
package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"syscall"
	"time"

	"github.com/bright2227/gogolook-task-system/internal/delivery"
	"github.com/bright2227/gogolook-task-system/internal/domain"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	DefaultDialTimeout = 3 * time.Second
	DefaultCooldown    = 5 * time.Second
	DefaultQueue       = "tasks"
	DefaultExchange    = "direct"
)

var errCoolingDown = fmt.Errorf("broker unavailable (cooling down): %w", domain.ErrNetwork)

type Config struct {
	URL         string
	Queue       string
	DialTimeout time.Duration
}

func (c Config) normalized() Config {
	if c.Queue == "" {
		c.Queue = DefaultQueue
	}
	if c.DialTimeout <= 0 {
		c.DialTimeout = DefaultDialTimeout
	}
	return c
}

type Client struct {
	cfg      Config
	mu       chan struct{}
	conn     *amqp.Connection
	retryAt  time.Time // guarded by mu
	Cooldown time.Duration
}

// New creates a broker resource without opening a connection.
func New(c Config) *Client {
	return &Client{cfg: c.normalized(), mu: make(chan struct{}, 1), Cooldown: DefaultCooldown}
}
func (c *Client) Queue() string { return c.cfg.Queue }

// dial bounds TCP, TLS and AMQP setup by both the caller and DialTimeout.
// The socket guard is removed before returning a live connection: subsequent
// operations own cancellation, and AMQP heartbeats own its read deadline.
func (c *Client) dial(ctx context.Context) (*amqp.Connection, error) {
	setup, cancel := context.WithTimeout(ctx, c.cfg.DialTimeout)
	defer cancel()
	var socket net.Conn
	var stop func() bool
	done := make(chan struct{})
	conn, err := amqp.DialConfig(c.cfg.URL, amqp.Config{Heartbeat: 10 * time.Second, Dial: func(network, address string) (net.Conn, error) {
		var err error
		socket, err = (&net.Dialer{}).DialContext(setup, network, address)
		if err != nil {
			return nil, err
		}
		stop = context.AfterFunc(setup, func() {
			defer close(done)
			_ = socket.Close()
		})
		return socket, nil
	}})
	if stop != nil && !stop() {
		<-done
	}
	if err != nil || setup.Err() != nil {
		if socket != nil {
			_ = socket.Close()
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if setup.Err() != nil {
			// Closing the socket can surface as "use of closed connection".
			// Preserve a dial timeout as a retryable transport failure.
			err = &net.OpError{Op: "dial", Net: "amqp", Err: syscall.ETIMEDOUT}
		}
		return nil, err
	}
	return conn, nil
}

func (c *Client) Connection() (*amqp.Connection, error) {
	return c.ConnectionContext(context.Background())
}

func (c *Client) ConnectionContext(ctx context.Context) (*amqp.Connection, error) {
	select {
	case c.mu <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-c.mu }()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.conn != nil && !c.conn.IsClosed() {
		return c.conn, nil
	}
	if time.Now().Before(c.retryAt) {
		return nil, errCoolingDown
	}
	conn, err := c.dial(ctx)
	if err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			c.retryAt = time.Now().Add(c.Cooldown)
		}
		return nil, err
	}
	c.conn = conn
	c.retryAt = time.Time{}
	return conn, nil
}

func (c *Client) Close() error {
	c.mu <- struct{}{}
	defer func() { <-c.mu }()
	if c.conn == nil {
		return nil
	}
	// Heartbeats can refresh deadlines, so keep a forced-close fallback.
	stop := forceCloseAfter(c.conn, c.cfg.DialTimeout)
	defer stop()
	err := c.conn.CloseDeadline(time.Now().Add(c.cfg.DialTimeout))
	c.conn = nil
	return err
}

func forceCloseAfter(conn *amqp.Connection, timeout time.Duration) func() {
	done := make(chan struct{})
	timer := time.AfterFunc(timeout, func() {
		defer close(done)
		_ = conn.CloseDeadline(time.Now())
	})
	return func() {
		if !timer.Stop() {
			<-done
		}
	}
}

func CloseConnectionOnCancel(ctx context.Context, conn *amqp.Connection) func() {
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(done)
		_ = conn.CloseDeadline(time.Now())
	})
	return func() {
		if !stop() {
			<-done
		} else if ctx.Err() != nil {
			// The operation may observe cancellation before AfterFunc starts.
			// Finish the owner's cleanup even when unregistering wins that race.
			_ = conn.CloseDeadline(time.Now())
		}
	}
}

func TransportError(err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if errors.Is(err, amqp.ErrClosed) || errors.Is(err, io.EOF) {
		return delivery.NetworkError(&net.OpError{Op: "publish", Net: "amqp", Err: io.EOF})
	}
	var brokerErr *amqp.Error
	if errors.As(err, &brokerErr) {
		if cause := transportCause(brokerErr); cause != nil {
			return delivery.NetworkError(&net.OpError{Op: "publish", Net: "amqp", Err: cause})
		}
		return err
	}
	if delivery.IsNetworkError(err) {
		return delivery.NetworkError(err)
	}
	return err
}

func transportCause(err *amqp.Error) error {
	if err.Code == 320 {
		return io.EOF
	}
	// The client reports socket read failures as AMQP FrameError (501). Only
	// normalize recognizable transport reasons; other frame errors are terminal.
	if err.Code != 501 {
		return nil
	}
	reason := strings.ToLower(err.Reason)
	switch {
	case strings.Contains(reason, "eof"):
		return io.EOF
	case strings.Contains(reason, "timeout"):
		return syscall.ETIMEDOUT
	case strings.Contains(reason, "connection reset by peer"):
		return syscall.ECONNRESET
	case strings.Contains(reason, "connection aborted"):
		return syscall.ECONNABORTED
	case strings.Contains(reason, "broken pipe"):
		return syscall.EPIPE
	case strings.Contains(reason, "network is unreachable"):
		return syscall.ENETUNREACH
	case strings.Contains(reason, "host is unreachable"), strings.Contains(reason, "no route to host"):
		return syscall.EHOSTUNREACH
	default:
		return nil
	}
}
