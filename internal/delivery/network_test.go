package delivery_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"net"
	"os"
	"syscall"
	"testing"

	"github.com/bright2227/gogolook-task-system/internal/delivery"
	"github.com/bright2227/gogolook-task-system/internal/domain"
)

func TestIsNetworkErrorOnlyRecognizesTransportFailures(t *testing.T) {
	wrapped := func(err error) error { return errors.Join(errors.New("wrapper"), err) }
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"reset", wrapped(syscall.ECONNRESET), true},
		{"connection refused", &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}, true},
		{"driver bad connection", driver.ErrBadConn, true},
		{"closed SQL connection", sql.ErrConnDone, true},
		{"permission", wrapped(os.ErrPermission), false},
		{"socket permission", &net.OpError{Op: "dial", Net: "tcp", Err: syscall.EACCES}, false},
		{"wrapped bad input", wrapped(errors.New("invalid payload field")), false},
		{"network wrapper around bad input", &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("invalid address")}, false},
		{"DNS timeout", &net.DNSError{Err: "timeout", Name: "service", IsTimeout: true}, true},
		{"DNS not found", &net.DNSError{Err: "no such host", Name: "missing", IsNotFound: true}, false},
		{"JSON syntax", &json.SyntaxError{Offset: 2}, false},
		{"JSON type", &json.UnmarshalTypeError{Value: "string"}, false},
		{"deadline", context.DeadlineExceeded, false},
		{"canceled", context.Canceled, false},
		{"marked deadline", errors.Join(domain.ErrNetwork, context.DeadlineExceeded), false},
		{"marked permission", errors.Join(domain.ErrNetwork, os.ErrPermission), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := delivery.IsNetworkError(tc.err); got != tc.want {
				t.Fatalf("IsNetworkError(%v)=%v want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestNetworkErrorMarksOnlyTransportFailures(t *testing.T) {
	transport := &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNRESET}
	marked := delivery.NetworkError(transport)
	if !errors.Is(marked, domain.ErrNetwork) || !errors.Is(marked, syscall.ECONNRESET) {
		t.Fatalf("NetworkError(%v) = %v, want both network marker and cause", transport, marked)
	}
	if got := delivery.NetworkError(marked); got != marked {
		t.Fatal("NetworkError changed an already-marked error")
	}
	terminal := errors.New("invalid payload")
	if got := delivery.NetworkError(terminal); got != terminal {
		t.Fatal("NetworkError wrapped a terminal error")
	}
}
