package rime

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestStreamPreservesContextCauseBeforeTransportFailure(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		t.Run(cause.Error(), func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			stream := &AudioStream{op: &operation{ctx: ctx, cancel: cancel}, queue: newAudioQueue()}
			cancel(cause)
			// Force the transport worker to report cancellation before watch or
			// the reader handles the context. gRPC status errors do not unwrap
			// to the original context error.
			stream.fail(status.Error(codes.Canceled, "transport cancelled"))
			_, err := stream.queue.get(ctx)
			kind := ErrCancelled
			if cause == context.DeadlineExceeded {
				kind = ErrTimeout
			}
			if !errors.Is(err, cause) || !errors.Is(err, kind) {
				t.Fatalf("context cause lost: got %v (cause %v), want %v and %v", err, errors.Unwrap(err), cause, kind)
			}
		})
	}
}

func TestStreamPreservesServiceFailureWithoutContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	stream := &AudioStream{op: &operation{ctx: ctx, cancel: cancel}, queue: newAudioQueue()}
	serviceErr := status.Error(codes.Unavailable, "service unavailable")
	stream.fail(serviceErr)
	// Cleanup cancellation must not overwrite a failure already reported.
	stream.fail(context.Canceled)
	_, err := stream.queue.get(ctx)
	if !errors.Is(err, serviceErr) || !errors.Is(err, ErrUnavailable) {
		t.Fatalf("service failure lost: %v", err)
	}
}
