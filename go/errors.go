package rime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ErrorKind identifies an SDK failure. Use errors.Is(err, rime.ErrTimeout), for example.
type ErrorKind string

func (k ErrorKind) Error() string { return string(k) }

const (
	ErrAuthentication ErrorKind = "authentication"
	ErrPermission     ErrorKind = "permission"
	// ErrBilling indicates that the account has insufficient prepaid credits.
	ErrBilling       ErrorKind = "billing"
	ErrInput         ErrorKind = "input"
	ErrResourceLimit ErrorKind = "resource_limit"
	ErrUnavailable   ErrorKind = "unavailable"
	ErrTimeout       ErrorKind = "timeout"
	ErrAudioFormat   ErrorKind = "audio_format"
	ErrCancelled     ErrorKind = "cancelled"
	ErrStream        ErrorKind = "stream"

	// ErrRealtime is a structured Prism service fault. Inspect *RealtimeFault for details.
	ErrRealtime ErrorKind = "realtime"
)

// Error includes the service request ID when the service supplies one.
// Realtime service faults are available through errors.As with *RealtimeFault.
type Error struct {
	Kind      ErrorKind
	Message   string
	RequestID string
	Cause     error
}

func (e *Error) Error() string        { return fmt.Sprintf("rime: %s: %s", e.Kind, e.Message) }
func (e *Error) Unwrap() error        { return e.Cause }
func (e *Error) Is(target error) bool { return target == e.Kind }

// StatusCode returns the refused realtime HTTP upgrade status, such as 503.
// The second result is false when no HTTP refusal was received.
func (e *Error) StatusCode() (int, bool) {
	var details *connectionError
	if errors.As(e.Cause, &details) && details.statusCode != nil {
		return *details.statusCode, true
	}
	return 0, false
}

// ServerKind returns the structured service error kind, such as insufficient_credits.
// The second result is false when no structured reason was supplied.
func (e *Error) ServerKind() (string, bool) {
	var details *connectionError
	if errors.As(e.Cause, &details) && details.serverKind != "" {
		return details.serverKind, true
	}
	return "", false
}

// CloseCode returns a received WebSocket close code, such as 1013.
// The second result is false when no close code was received.
func (e *Error) CloseCode() (int, bool) {
	var details *connectionError
	if errors.As(e.Cause, &details) && details.closeCode != nil {
		return *details.closeCode, true
	}
	return 0, false
}

// RetryAfter returns the server-requested delay. Zero with true is valid.
// False means that Retry-After was absent, invalid, duplicated, or too large.
// This value does not cause retries or make replay of accepted requests safe.
func (e *Error) RetryAfter() (time.Duration, bool) {
	var details *connectionError
	if errors.As(e.Cause, &details) && details.retryAfter != nil {
		return *details.retryAfter, true
	}
	return 0, false
}

// Transport diagnostics stay in the error chain to preserve Error's public struct layout.
type connectionError struct {
	serverKind string
	cause      error
	statusCode *int
	closeCode  *int
	retryAfter *time.Duration
}

func (e *connectionError) Unwrap() error { return e.cause }

func (e *connectionError) Error() string {
	if e.statusCode != nil {
		return fmt.Sprintf("realtime connection refused (HTTP %d)", *e.statusCode)
	}
	if e.closeCode != nil {
		return fmt.Sprintf("realtime connection closed (WebSocket close %d)", *e.closeCode)
	}
	if e.cause != nil {
		return e.cause.Error()
	}
	return "realtime connection diagnostics"
}

func failure(kind ErrorKind, message string) *Error { return &Error{Kind: kind, Message: message} }

func operationError(err error, id string, headers ...metadata.MD) *Error {
	var existing *Error
	if errors.As(err, &existing) {
		result := *existing
		if result.RequestID == "" {
			result.RequestID = id
		}
		return &result
	}
	kind := ErrStream
	switch {
	case errors.Is(err, context.Canceled):
		kind = ErrCancelled
	case errors.Is(err, context.DeadlineExceeded):
		kind = ErrTimeout
	default:
		switch status.Code(err) {
		case codes.Unauthenticated:
			kind = ErrAuthentication
		case codes.PermissionDenied:
			kind = ErrPermission
		case codes.InvalidArgument:
			kind = ErrInput
		case codes.ResourceExhausted:
			kind = ErrResourceLimit
		case codes.Unavailable:
			kind = ErrUnavailable
		case codes.DeadlineExceeded:
			kind = ErrTimeout
		case codes.Canceled:
			kind = ErrCancelled
		}
	}
	var serverKind string
	for _, values := range headers {
		if kinds := values.Get("x-rime-error-kind"); len(kinds) > 0 && kinds[0] != "" {
			serverKind = kinds[0]
			break
		}
	}
	if kind == ErrPermission && serverKind == "insufficient_credits" {
		kind = ErrBilling
	}
	message := "operation failed"
	if serviceStatus, ok := status.FromError(err); ok && strings.TrimSpace(serviceStatus.Message()) != "" {
		message = serviceStatus.Message()
	}
	if serverKind != "" {
		err = &connectionError{serverKind: serverKind, cause: err}
	}
	return &Error{Kind: kind, Message: message, RequestID: id, Cause: err}
}
