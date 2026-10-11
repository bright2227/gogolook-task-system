package domain

import "errors"

var (
	ErrInvalidID  = errors.New("invalid task ID")
	ErrPayload    = errors.New("payload must be a UTF-8 string of at most 1024 bytes without NUL characters")
	ErrName       = errors.New("name must be a nonblank UTF-8 string of at most 256 bytes without NUL characters")
	ErrBusy       = errors.New("task execution is busy")
	ErrNotDue     = errors.New("task execution is not due")
	ErrNotFound   = errors.New("task not found")
	ErrConflict   = errors.New("concurrent task update")
	ErrNetwork    = errors.New("network error")
	ErrPagination = errors.New("invalid pagination")
)
