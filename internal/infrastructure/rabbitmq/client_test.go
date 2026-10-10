package rabbitmq

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/bright2227/gogolook-task-system/internal/delivery"
	"github.com/bright2227/gogolook-task-system/internal/domain"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestConnectionContextCancelsWhileAnotherHandshakeIsRunning(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		if conn, err := listener.Accept(); err == nil {
			accepted <- conn
		}
	}()
	client := New(Config{URL: "amqp://guest:guest@" + listener.Addr().String() + "/", DialTimeout: 2 * time.Second})
	first, cancelFirst := context.WithCancel(context.Background())
	defer cancelFirst()
	finished := make(chan error, 1)
	go func() {
		_, err := client.ConnectionContext(first)
		finished <- err
	}()
	select {
	case conn := <-accepted:
		defer conn.Close()
	case <-time.After(time.Second):
		t.Fatal("first handshake never connected")
	}
	second, cancelSecond := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelSecond()
	started := time.Now()
	_, err = client.ConnectionContext(second)
	elapsed := time.Since(started)
	cancelFirst()
	select {
	case firstErr := <-finished:
		if !errors.Is(firstErr, context.Canceled) {
			t.Errorf("first handshake=%v", firstErr)
		}
	case <-time.After(time.Second):
		t.Fatal("first handshake did not stop after cancellation")
	}
	if !errors.Is(err, context.DeadlineExceeded) || elapsed > 500*time.Millisecond {
		t.Fatalf("waiting connection=%v after %s", err, elapsed)
	}
}

func TestPublisherTransportErrorsAreClassifiedForPublicationRetry(t *testing.T) {
	for _, err := range []error{
		TransportError(io.EOF),
		TransportError(syscall.ETIMEDOUT),
		TransportError(amqp.ErrClosed),
		TransportError(&amqp.Error{Code: 320, Reason: "connection forced"}),
		TransportError(&amqp.Error{Code: 501, Reason: "read tcp: i/o timeout"}),
	} {
		if !delivery.IsNetworkError(err) || !errors.Is(err, domain.ErrNetwork) {
			t.Errorf("transport error %v is not retryable by submission recovery", err)
		}
	}
	for _, err := range []error{
		TransportError(context.DeadlineExceeded),
		TransportError(&amqp.Error{Code: 403, Reason: "access refused"}),
		TransportError(&amqp.Error{Code: 406, Reason: "precondition failed"}),
		TransportError(&amqp.Error{Code: 501, Reason: "frame could not be parsed"}),
	} {
		if delivery.IsNetworkError(err) {
			t.Errorf("terminal error %v was classified as transport", err)
		}
	}
}

func TestClientCancelsHandshake(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		if conn, err := listener.Accept(); err == nil {
			accepted <- conn
		}
	}()
	client := New(Config{URL: "amqp://guest:guest@" + listener.Addr().String() + "/", DialTimeout: 2 * time.Second})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	if _, err := client.ConnectionContext(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("handshake cancellation=%v", err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("canceled handshake took %s", elapsed)
	}
	if !client.retryAt.IsZero() || client.conn != nil {
		t.Fatal("canceled setup retained a cooldown or connection")
	}
	select {
	case conn := <-accepted:
		defer conn.Close()
		_ = conn.SetReadDeadline(time.Now().Add(time.Second))
		if _, err := io.Copy(io.Discard, conn); err != nil {
			t.Fatalf("canceled handshake left its socket open: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("handshake never connected")
	}
}

// A silent listener accepts TCP but never speaks AMQP, so each dial costs the
// full DialTimeout, like an unresponsive broker.
func TestClientCoolsDownAfterFailedDial(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var dials atomic.Int64
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			dials.Add(1)
			defer conn.Close()
		}
	}()
	const dialTimeout = 200 * time.Millisecond
	client := New(Config{URL: "amqp://guest:guest@" + listener.Addr().String() + "/", DialTimeout: dialTimeout})
	ctx := context.Background()
	if _, err := client.ConnectionContext(ctx); !delivery.IsNetworkError(err) {
		t.Fatalf("first dial=%v", err)
	}
	started := time.Now()
	_, err = client.ConnectionContext(ctx)
	if elapsed := time.Since(started); !delivery.IsNetworkError(err) || elapsed >= dialTimeout/2 || dials.Load() != 1 {
		t.Fatalf("cooling dial=%v after %s, dials=%d", err, elapsed, dials.Load())
	}
	client.retryAt = time.Now()
	if _, err := client.ConnectionContext(ctx); !delivery.IsNetworkError(err) || dials.Load() != 2 {
		t.Fatalf("dial after cooldown=%v, dials=%d", err, dials.Load())
	}
}
