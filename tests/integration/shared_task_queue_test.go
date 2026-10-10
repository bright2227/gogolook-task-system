//go:build integration

package integration

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bright2227/gogolook-task-system/internal/configs"
	"github.com/bright2227/gogolook-task-system/internal/domain"
	"github.com/bright2227/gogolook-task-system/internal/infrastructure/rabbitmq"
)

// methodBlockingProxy forwards the handshake and heartbeats, but can drop one
// kind of AMQP request. The broker stays alive while the client's RPC stalls,
// so only the operation's cancellation guard can release it.
type methodBlockingProxy struct {
	URL     string
	blocked atomic.Bool
	started chan struct{}
	pause   func()
	resume  func()
}

func newMethodBlockingProxy(t *testing.T, brokerURL string, class, method uint16) *methodBlockingProxy {
	t.Helper()
	target, err := url.Parse(brokerURL)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	upstream := target.Host
	target.Host = listener.Addr().String()
	proxy := &methodBlockingProxy{URL: target.String(), started: make(chan struct{}, 1)}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var sockets []net.Conn
	stopped := false
	paused := false
	// Pause drops active connections and rejects reconnects until the test has
	// removed topology. The listener stays available for the next session.
	proxy.pause = func() {
		mu.Lock()
		defer mu.Unlock()
		paused = true
		for _, socket := range sockets {
			_ = socket.Close()
		}
		sockets = nil
	}
	proxy.resume = func() {
		mu.Lock()
		defer mu.Unlock()
		paused = false
	}
	t.Cleanup(func() {
		mu.Lock()
		stopped = true
		_ = listener.Close()
		for _, socket := range sockets {
			_ = socket.Close()
		}
		mu.Unlock()
		wg.Wait()
	})
	wg.Go(func() {
		for {
			down, err := listener.Accept()
			if err != nil {
				return
			}
			up, err := net.DialTimeout("tcp", upstream, time.Second)
			if err != nil {
				_ = down.Close()
				return
			}
			mu.Lock()
			if stopped {
				_ = down.Close()
				_ = up.Close()
				mu.Unlock()
				return
			}
			if paused {
				_ = down.Close()
				_ = up.Close()
				mu.Unlock()
				continue
			}
			sockets = append(sockets, down, up)
			mu.Unlock()
			wg.Go(func() {
				defer down.Close()
				defer up.Close()
				_, _ = io.Copy(down, up)
			})
			wg.Go(func() {
				defer down.Close()
				defer up.Close()
				if _, err := io.CopyN(up, down, 8); err != nil { // AMQP protocol header
					return
				}
				for {
					var header [7]byte
					if _, err := io.ReadFull(down, header[:]); err != nil {
						return
					}
					body := make([]byte, int(binary.BigEndian.Uint32(header[3:]))+1)
					if _, err := io.ReadFull(down, body); err != nil {
						return
					}
					if header[0] == 1 && len(body) >= 5 && binary.BigEndian.Uint16(body[:2]) == class && binary.BigEndian.Uint16(body[2:4]) == method && proxy.blocked.Load() {
						select {
						case proxy.started <- struct{}{}:
						default:
						}
						// Preserve frame ordering: once the request is dropped, stop
						// forwarding client frames until its socket is closed.
						_, _ = io.Copy(io.Discard, down)
						return
					}
					if _, err := up.Write(append(header[:], body...)); err != nil {
						return
					}
				}
			})
		}
	})
	return proxy
}

func TestSharedTaskQueueBoundsSetupPublicationAndCleanup(t *testing.T) {
	s := newIntegrationStack(t)
	fixture := s.client(t, "integration.shared-deadline")
	for _, tc := range []struct {
		name          string
		class, method uint16
		warm          bool
	}{
		{name: "channel open", class: 20, method: 10},
		{name: "confirm setup", class: 85, method: 10},
		{name: "publication", class: 60, method: 40, warm: true},
		{name: "cleanup", class: 10, method: 50, warm: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proxy := newMethodBlockingProxy(t, s.brokerURL, tc.class, tc.method)
			pool := configs.NewSharedTaskQueueResources(rabbitmq.Config{URL: proxy.URL, Queue: fixture.broker.Queue(), DialTimeout: 250 * time.Millisecond}, 2)
			client := pool.Client
			t.Cleanup(func() { _ = pool.Close() })
			t.Cleanup(func() { _ = client.Close() })
			if tc.warm {
				ctx, cancel := context.WithCancel(s.ctx)
				publication, err := pool.Enqueue(ctx, domain.NewID())
				cancel() // A completed call must remove its connection guard.
				if err != nil || publication.Err != nil {
					t.Fatalf("warm publication=%+v,%v", publication, err)
				}
			}
			proxy.blocked.Store(true)
			ctx, cancel := context.WithTimeout(s.ctx, 500*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			started := time.Now()
			go func() {
				if tc.name == "cleanup" {
					err := client.Close()
					if closeErr := pool.Close(); err == nil {
						err = closeErr
					}
					done <- err
					return
				}
				_, err := pool.Enqueue(ctx, domain.NewID())
				done <- err
			}()
			select {
			case <-proxy.started:
			case err := <-done:
				t.Fatalf("operation ended before the blocked method: %v", err)
			case <-time.After(time.Second):
				t.Fatal("operation never reached the blocked method")
			}
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("blocked operation unexpectedly succeeded")
				}
				if tc.name == "publication" && !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("publication=%v, want its deadline", err)
				}
				if (tc.name == "channel open" || tc.name == "confirm setup") && !errors.Is(err, domain.ErrNetwork) {
					t.Fatalf("setup=%v, want the observed transport error", err)
				}
			case <-time.After(time.Second):
				t.Fatal("operation remained blocked after its deadline")
			}
			if elapsed := time.Since(started); elapsed > time.Second {
				t.Fatalf("operation exceeded its cleanup budget: %s", elapsed)
			}
			// Cancellation must terminate the affected connection, not just
			// abandon a channel. A later call redials without a caller cooldown.
			proxy.blocked.Store(false)
			if tc.name != "cleanup" {
				publication, err := pool.Enqueue(s.ctx, domain.NewID())
				if err != nil || publication.Err != nil {
					t.Fatalf("publication after canceled session=%+v,%v", publication, err)
				}
			}
		})
	}
}

func TestSharedTaskQueueCanceledWaitKeepsBorrowedConnection(t *testing.T) {
	s := newIntegrationStack(t)
	fixture := s.client(t, "integration.shared-wait")
	proxy := newMethodBlockingProxy(t, s.brokerURL, 60, 40)
	pool := configs.NewSharedTaskQueueResources(rabbitmq.Config{URL: proxy.URL, Queue: fixture.broker.Queue(), DialTimeout: 250 * time.Millisecond}, 1)
	client := pool.Client
	t.Cleanup(func() {
		_ = client.Close()
		_ = pool.Close()
	})
	if publication, err := pool.Enqueue(s.ctx, domain.NewID()); err != nil || publication.Err != nil {
		t.Fatalf("warm publication=%+v,%v", publication, err)
	}
	conn, err := client.Connection()
	if err != nil {
		t.Fatal(err)
	}
	proxy.blocked.Store(true)
	active, cancelActive := context.WithCancel(s.ctx)
	defer cancelActive()
	done := make(chan error, 1)
	go func() {
		_, err := pool.Enqueue(active, domain.NewID())
		done <- err
	}()
	select {
	case <-proxy.started:
	case err := <-done:
		t.Fatalf("active publication stopped before blocking: %v", err)
	case <-time.After(time.Second):
		t.Fatal("active publication never borrowed the channel")
	}
	waiting, cancelWaiting := context.WithTimeout(s.ctx, 50*time.Millisecond)
	defer cancelWaiting()
	if _, err := pool.Enqueue(waiting, domain.NewID()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("pool wait=%v, want its deadline", err)
	}
	if conn.IsClosed() {
		t.Fatal("canceled pool wait closed the active publication's connection")
	}
	select {
	case err := <-done:
		t.Fatalf("canceled pool wait interrupted the active publication: %v", err)
	default:
	}
	cancelActive()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || !conn.IsClosed() {
			t.Fatalf("active cancellation=%v, connection closed=%v", err, conn.IsClosed())
		}
	case <-time.After(time.Second):
		t.Fatal("active publication did not stop when its own context ended")
	}
	proxy.blocked.Store(false)
	if publication, err := pool.Enqueue(s.ctx, domain.NewID()); err != nil || publication.Err != nil {
		t.Fatalf("publication after cancellation=%+v,%v", publication, err)
	}
}
