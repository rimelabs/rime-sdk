package rime

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrorKind identifies an SDK failure. Use errors.Is(err, rime.ErrTimeout), for example.
type ErrorKind string

func (k ErrorKind) Error() string { return string(k) }

const (
	ErrAuthentication ErrorKind = "authentication"
	ErrPermission     ErrorKind = "permission"
	ErrInput          ErrorKind = "input"
	ErrResourceLimit  ErrorKind = "resource_limit"
	ErrUnavailable    ErrorKind = "unavailable"
	ErrTimeout        ErrorKind = "timeout"
	ErrAudioFormat    ErrorKind = "audio_format"
	ErrCancelled      ErrorKind = "cancelled"
	ErrStream         ErrorKind = "stream"
)

// Error includes the service request ID when the service supplies one.
type Error struct {
	Kind      ErrorKind
	Message   string
	RequestID string
	Cause     error
}

func (e *Error) Error() string        { return fmt.Sprintf("rime: %s: %s", e.Kind, e.Message) }
func (e *Error) Unwrap() error        { return e.Cause }
func (e *Error) Is(target error) bool { return target == e.Kind }

func failure(kind ErrorKind, message string) *Error { return &Error{Kind: kind, Message: message} }

func operationError(err error, id string) *Error {
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
	message := "operation failed"
	if serviceStatus, ok := status.FromError(err); ok && strings.TrimSpace(serviceStatus.Message()) != "" {
		message = serviceStatus.Message()
	}
	return &Error{Kind: kind, Message: message, RequestID: id, Cause: err}
}
