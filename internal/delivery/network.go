package delivery

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"syscall"

	"github.com/bright2227/gogolook-task-system/internal/domain"
)

// IsNetworkError identifies transport failures that can be retried safely.
// Cancellation, permission failures, and malformed input are terminal.
func IsNetworkError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, os.ErrPermission) || errors.Is(err, syscall.EACCES) || errors.Is(err, syscall.EPERM) {
		return false
	}
	if errors.Is(err, domain.ErrNetwork) {
		return true
	}
	var syntax *json.SyntaxError
	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &syntax) || errors.As(err, &typeErr) {
		return false
	}
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, sql.ErrConnDone) {
		return true
	}
	var dns *net.DNSError
	if errors.As(err, &dns) {
		return !dns.IsNotFound && (dns.IsTimeout || dns.IsTemporary)
	}
	for _, target := range []error{syscall.ECONNRESET, syscall.ECONNREFUSED, syscall.EPIPE, syscall.ETIMEDOUT, syscall.EHOSTUNREACH, syscall.ENETUNREACH, syscall.ECONNABORTED} {
		if errors.Is(err, target) {
			return true
		}
	}
	var op *net.OpError
	if errors.As(err, &op) {
		if errors.Is(op.Err, io.EOF) || errors.Is(op.Err, io.ErrUnexpectedEOF) {
			return true
		}
		var netErr net.Error
		return errors.As(op.Err, &netErr) && (netErr.Timeout() || netErr.Temporary())
	}
	return false
}

// NetworkError marks a transport failure so domain use cases can retry it.
// Existing markers are preserved, and terminal errors pass through unchanged.
func NetworkError(err error) error {
	if err == nil || errors.Is(err, domain.ErrNetwork) || !IsNetworkError(err) {
		return err
	}
	return errors.Join(domain.ErrNetwork, err)
}
